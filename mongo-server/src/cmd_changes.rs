//! `aggregate` with `$changeStream` and the `getMore` that follows it.

use crate::changes::{Ev, Scope};
use crate::cmd::{int_arg, Server, R};
use crate::error::MongoError;
use bson::{doc, Bson, Document};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, PartialEq)]
pub enum FullDoc {
    Default,
    UpdateLookup,
    WhenAvailable,
    Required,
}

/// `fullDocumentBeforeChange`.
#[derive(Clone, Copy, PartialEq)]
pub enum BeforeDoc {
    Off,
    WhenAvailable,
    Required,
}

pub struct ChangeCursor {
    /// Last time the cursor was created or used by a getMore (idle cursors are closed, see `cmd::CURSOR_TIMEOUT_SECS`).
    pub used: Instant,
    pub scope: Scope,
    pub last: u64,
    pub rest: Vec<Bson>,
    pub full: FullDoc,
    pub before: BeforeDoc,
    /// `showExpandedEvents`: DDL events and the collection UUID.
    pub expanded: bool,
    pub ns: String,
    /// An `invalidate` event is due after the batch that holds the event that invalidated the stream.
    pub closed: bool,
}

impl Server {
    pub(crate) async fn open_change_stream(&self, db: &str, cmd: &Document, spec: &Document, rest: &[Bson]) -> R<Document> {
        let coll = match cmd.get("aggregate") {
            Some(Bson::String(c)) => Some(c.clone()),
            _ => None,
        };
        let cluster = matches!(spec.get("allChangesForCluster"), Some(Bson::Boolean(true)));
        if cluster && db != "admin" {
            return Err(MongoError::new(73, "InvalidNamespace", "$changeStream with 'allChangesForCluster:true' may only be opened on the 'admin' database, and with {aggregate: 1}".into()));
        }
        let scope = if cluster { Scope { db: None, coll: None } } else { Scope { db: Some(db.to_string()), coll: coll.clone() } };
        for k in spec.keys() {
            if !matches!(k.as_str(), "fullDocument" | "fullDocumentBeforeChange" | "resumeAfter" | "startAfter" | "startAtOperationTime" | "allChangesForCluster" | "showExpandedEvents" | "showSystemEvents" | "showMigrationEvents" | "allowToRunOnConfigDB" | "allowToRunOnSystemNS") {
                return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field '$changeStream.{k}' is an unknown field.")));
            }
        }
        let enum_err = |field: &str, v: &Bson| MongoError::bad_value(format!("Enumeration value '{}' for field '$changeStream.{field}' is not a valid value.", match v { Bson::String(s) => s.clone(), o => o.to_string() }));
        let full = match spec.get("fullDocument") {
            None => FullDoc::Default,
            Some(Bson::String(s)) if s == "default" => FullDoc::Default,
            Some(Bson::String(s)) if s == "updateLookup" => FullDoc::UpdateLookup,
            Some(Bson::String(s)) if s == "whenAvailable" => FullDoc::WhenAvailable,
            Some(Bson::String(s)) if s == "required" => FullDoc::Required,
            Some(other) => return Err(enum_err("fullDocument", other)),
        };
        let before = match spec.get("fullDocumentBeforeChange") {
            None => BeforeDoc::Off,
            Some(Bson::String(s)) if s == "off" => BeforeDoc::Off,
            Some(Bson::String(s)) if s == "whenAvailable" => BeforeDoc::WhenAvailable,
            Some(Bson::String(s)) if s == "required" => BeforeDoc::Required,
            Some(other) => return Err(enum_err("fullDocumentBeforeChange", other)),
        };
        let expanded = matches!(spec.get("showExpandedEvents"), Some(Bson::Boolean(true)));
        if expanded {
            crate::changes::WANTS_UUID.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let log = &self.dbs.changes;
        for f in ["resumeAfter", "startAfter"] {
            if let Some(v) = spec.get(f) {
                if !matches!(v, Bson::Document(_)) {
                    return Err(MongoError::type_mismatch(format!("BSON field '$changeStream.{f}' is the wrong type '{}', expected type 'object'", crate::update::type_name(v))));
                }
            }
        }
        let start = if let Some(Bson::Document(t)) = spec.get("resumeAfter").or_else(|| spec.get("startAfter")) {
            match t.get("_data") {
                Some(Bson::String(d)) => log.parse_token(d)?,
                _ => return Err(MongoError::new(40647, "Location40647", format!("Bad resume token: _data of missing or of wrong type. Expected string, got {}", crate::update::fmt_doc(t).replace("{ ", "{").replace(" }", "}")))),
            }
        } else if let Some(Bson::Timestamp(ts)) = spec.get("startAtOperationTime") {
            log.seq_at_time(*ts)
        } else {
            log.head()
        };
        let ns = match &coll {
            Some(c) => format!("{db}.{c}"),
            None => format!("{db}.$cmd.aggregate"),
        };
        let batch = cmd.get_document("cursor").ok().and_then(|c| int_arg(c.get("batchSize")));
        // `$changeStreamSplitLargeEvent` (last stage only) splits events over 16 MB; no event here is that large, so it passes events through
        let mut rest_stages = rest.to_vec();
        for (i, st) in rest.iter().enumerate() {
            if let Some(sp) = st.as_document().and_then(|d| d.get("$changeStreamSplitLargeEvent")) {
                if !matches!(sp, Bson::Document(d) if d.is_empty()) {
                    return Err(MongoError::new(7182800, "Location7182800", "$changeStreamSplitLargeEvent spec should be an empty object".into()));
                }
                if i + 1 != rest.len() {
                    return Err(MongoError::new(7182802, "Location7182802", "$changeStreamSplitLargeEvent must be the last stage in the pipeline".into()));
                }
                rest_stages.pop();
            }
        }
        let cur = ChangeCursor { used: Instant::now(), scope, last: start, rest: rest_stages, full, before, expanded, ns: ns.clone(), closed: false };
        let id = self.store_change_cursor(cur);
        let _ = batch;
        Ok(doc! { "cursor": { "firstBatch": Vec::<Bson>::new(), "postBatchResumeToken": { "_data": log.token(start) }, "id": id, "ns": ns }, "ok": 1.0 })
    }

    pub(crate) fn store_change_cursor(&self, c: ChangeCursor) -> i64 {
        let mut m = self.change_cursors.lock().unwrap();
        loop {
            let id = (rand::random::<i64>() & i64::MAX) | (1 << 41);
            if !m.contains_key(&id) {
                m.insert(id, c);
                return id;
            }
        }
    }

    /// The description of an event in the message of a `required` image that is missing.
    fn event_brief(e: &Ev) -> String {
        let s = &e.spec;
        let mut ns = format!("db: \"{}\"", s.db);
        if let Some(c) = &s.coll {
            ns.push_str(&format!(", coll: \"{c}\""));
        }
        format!("{{operationType: \"{}\", ns: {{{ns}}}, clusterTime: Timestamp({}, {})}}", s.op, e.time.time, e.time.increment)
    }

    /// The change event document, fields in the order of mongod. `lookup`: the `fullDocument` of an `updateLookup`.
    fn event_doc(&self, e: &Ev, cur: &ChangeCursor, lookup: Option<Option<Document>>, uuid: Option<Vec<u8>>) -> R<Document> {
        use crate::changes::EvSpec;
        let s: &EvSpec = &e.spec;
        let mut d = Document::new();
        let mut before_val: Option<Bson> = None;
        if matches!(s.op, "update" | "replace" | "delete") && cur.before != BeforeDoc::Off {
            match (&s.pre, cur.before) {
                (Some(p), _) => before_val = Some(Bson::Document(p.clone())),
                (None, BeforeDoc::Required) => {
                    return Err(MongoError::new(47, "NoMatchingDocument", format!("Executor error during getMore :: caused by :: Change stream was configured to require a pre-image for all update, delete and replace events, but the pre-image was not found for event: {}", Self::event_brief(e))));
                }
                (None, _) => before_val = Some(Bson::Null),
            }
        }
        if let Some((db, coll)) = &s.to {
            d.insert("to", doc! { "db": db.clone(), "coll": coll.clone() });
        }
        if let Some((n, lsid)) = &s.txn {
            d.insert("txnNumber", Bson::Int64(*n));
            d.insert("lsid", lsid.clone());
        }
        d.insert("_id", doc! { "_data": self.dbs.changes.token(e.seq) });
        d.insert("operationType", s.op);
        d.insert("clusterTime", Bson::Timestamp(e.time));
        if cur.expanded {
            if let Some(u) = s.uuid.clone().or(uuid) {
                d.insert("collectionUUID", Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: u }));
            }
        }
        d.insert("wallTime", e.wall);
        match s.op {
            "insert" | "replace" => {
                d.insert("fullDocument", s.full.clone().map(Bson::Document).unwrap_or(Bson::Null));
            }
            "update" => match cur.full {
                FullDoc::Default => {}
                FullDoc::UpdateLookup => {
                    d.insert("fullDocument", lookup.flatten().map(Bson::Document).unwrap_or(Bson::Null));
                }
                FullDoc::WhenAvailable => {
                    d.insert("fullDocument", s.post.clone().map(Bson::Document).unwrap_or(Bson::Null));
                }
                FullDoc::Required => match &s.post {
                    Some(p) => {
                        d.insert("fullDocument", p.clone());
                    }
                    None => {
                        return Err(MongoError::new(47, "NoMatchingDocument", format!("Executor error during getMore :: caused by :: Change stream was configured to require a post-image for all update events, but the post-image was not found for event: {}", Self::event_brief(e))));
                    }
                },
            },
            _ => {}
        }
        let mut ns = doc! { "db": s.db.clone() };
        if let Some(c) = &s.coll {
            ns.insert("coll", c.clone());
        }
        if s.op != "invalidate" {
            d.insert("ns", ns);
        }
        if let Some(k) = &s.doc_key {
            d.insert("documentKey", doc! { "_id": k.clone() });
        }
        if let Some(u) = &s.desc {
            let mut u = u.clone();
            if cur.expanded {
                u.insert("disambiguatedPaths", doc! {});
            }
            d.insert("updateDescription", u);
        }
        if cur.expanded {
            if let Some(o) = &s.op_desc {
                d.insert("operationDescription", o.clone());
            }
            if let Some(o) = &s.state_before {
                d.insert("stateBeforeChange", o.clone());
            }
        }
        if let Some(b) = before_val {
            d.insert("fullDocumentBeforeChange", b);
        }
        Ok(d)
    }

    /// `getMore` on a change-stream cursor: wait up to `maxTimeMS` for events.
    pub(crate) async fn change_get_more(&self, id: i64, cmd: &Document) -> R<Document> {
        let mut cur = self.change_cursors.lock().unwrap().remove(&id).ok_or_else(|| MongoError::cursor_not_found(id))?;
        let batch = int_arg(cmd.get("batchSize")).map(|b| b.max(1) as usize).unwrap_or(1000);
        let wait = Duration::from_millis(int_arg(cmd.get("maxTimeMS")).unwrap_or(0).max(0) as u64);
        let deadline = Instant::now() + wait;
        let log = Arc::clone(&self.dbs.changes);
        let (events, last) = loop {
            let notified = log.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (evs, last) = log.read(&cur.scope, cur.last, batch);
            let evs: Vec<Arc<Ev>> = evs.into_iter().filter(|e| cur.expanded || !e.spec.expanded_only).collect();
            if !evs.is_empty() || Instant::now() >= deadline {
                break (evs, last);
            }
            // (only events the stream cannot see were read: go on from there)
            cur.last = cur.last.max(last);
            let _ = tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), notified).await;
        };
        let mut docs = Vec::new();
        let mut closing = false;
        for e in &events {
            let lookup = if cur.full == FullDoc::UpdateLookup && e.spec.op == "update" {
                let mut g = self.dbs.lock(&e.spec.db).await;
                let coll = e.spec.coll.clone().unwrap_or_default();
                let found = match &e.spec.doc_key {
                    Some(k) => g.get_doc(&coll, k).await.map_err(MongoError::from)?,
                    None => None,
                };
                Some(found)
            } else {
                None
            };
            let uuid = if cur.expanded && e.spec.uuid.is_none() && matches!(e.spec.op, "insert" | "update" | "replace" | "delete") {
                let mut g = self.dbs.lock(&e.spec.db).await;
                match &e.spec.coll {
                    Some(c) if g.collections().await.map_err(MongoError::from)?.iter().any(|n| n == c) => Some(g.collection_uuid(c).await.map_err(MongoError::from)?),
                    _ => None,
                }
            } else {
                None
            };
            docs.push(self.event_doc(e, &cur, lookup, uuid)?);
            let same_coll = cur.scope.coll.is_some() && cur.scope.coll == e.spec.coll;
            let invalidates = (matches!(e.spec.op, "drop" | "rename") && same_coll)
                || (e.spec.op == "dropDatabase" && cur.scope.db.as_deref() == Some(e.spec.db.as_str()));
            if invalidates {
                let mut inv = doc! { "_id": { "_data": log.token(e.seq) }, "operationType": "invalidate", "clusterTime": Bson::Timestamp(e.time), "wallTime": e.wall };
                inv.insert("_id", doc! { "_data": log.token(e.seq) });
                docs.push(inv);
                closing = true;
                break;
            }
        }
        let n_scanned = if closing { events.last().map(|e| e.seq).unwrap_or(last) } else { last };
        let out = if cur.rest.is_empty() {
            docs
        } else {
            crate::agg::run_pipeline(docs, &cur.rest, &crate::agg::Foreign::new())?
        };
        cur.last = n_scanned;
        let token = log.token(n_scanned);
        let ns = cur.ns.clone();
        let next_id = if closing { 0 } else { id };
        if !closing {
            cur.used = Instant::now();
            self.change_cursors.lock().unwrap().insert(id, cur);
        }
        Ok(doc! { "cursor": { "nextBatch": out.into_iter().map(Bson::Document).collect::<Vec<_>>(), "postBatchResumeToken": { "_data": token }, "id": next_id, "ns": ns }, "ok": 1.0 })
    }
}
