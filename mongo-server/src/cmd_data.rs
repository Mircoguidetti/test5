//! Reads and writes: find, count, distinct, aggregate, insert, update, findAndModify, delete.
//! Documents are found through [`crate::coll::find_docs`] and written through [`crate::coll::Writes`].

use crate::agg::run_pipeline;
use crate::cmd::{bool_arg, collection_name, int_arg, write_error, Server, R};
use crate::coll::{find_docs, resolve_hint, FindOpts, Hint, Writes};
use crate::dbs::Entry;
use crate::txn::{attach, take_writes, tx_ov, Txn};
use crate::error::MongoError;
use crate::index::{self, IndexDef};
use crate::layout::{doc_prefix, encode_sortable};
use crate::query::{self, parse_projection, parse_sort, path_values, project, sort_docs};
use crate::update::Upd;
use bson::{doc, oid::ObjectId, Bson, Document};
use std::collections::VecDeque;

const DEFAULT_FIRST_BATCH_TAIL: usize = 101;

pub(crate) fn now_ms() -> i64 {
    bson::DateTime::now().timestamp_millis()
}

impl Server {
    /// Before a write to a collection with TTL indexes, remove its expired documents (at most once a second),
    /// so an expired document cannot hold an `_id` or a unique key.
    pub(crate) async fn presweep(&self, g: &mut Entry, coll: &str, indexes: &[IndexDef]) -> R<()> {
        if !indexes.iter().any(|i| i.ttl.is_some() && i.state == index::State::Ready) {
            return Ok(());
        }
        if g.swept.get(coll).map(|t| t.elapsed().as_millis() < 1000).unwrap_or(false) {
            return Ok(());
        }
        g.swept.insert(coll.to_string(), std::time::Instant::now());
        let n = crate::ttl::sweep_coll(g, coll, now_ms(), 1).await?;
        self.dbs.ttl.docs_expired.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Matching documents of `coll`: (docs, indexes) with TTL-expired documents hidden.
    pub(crate) async fn matching(&self, g: &mut impl crate::coll::Rd, db: &str, coll: &str, filter: &Document, hint: Option<&Bson>, ov: Option<&crate::coll::Overlay>) -> R<Vec<Document>> {
        let indexes = self.load_indexes(g, db, coll).await?;
        let hint = resolve_hint(&indexes, hint)?;
        let specs = index::ttl_specs(&indexes);
        let (docs, _, _) = find_docs(g, coll, &indexes, &FindOpts { filter, sort: &[], want: None, hint, ttl: ttl_ctx(&specs), ov }).await?;
        Ok(docs)
    }

    /// Every visible document of a collection (TTL-expired ones hidden).
    pub(crate) async fn scan_visible(&self, g: &mut impl crate::coll::Rd, db: &str, coll: &str) -> R<Vec<Document>> {
        self.matching(g, db, coll, &Document::new(), None, None).await
    }

    /// The documents a find reads (before sort / skip / projection), from a shared view or the exclusive entry.
    async fn find_part(&self, g: &mut impl crate::coll::Rd, db: &str, coll: &str, filter: &Document, sort: &[(String, bool)], want: Option<usize>, hint: Option<&Bson>, ov: Option<&crate::coll::Overlay>) -> R<Vec<Document>> {
        let indexes = self.load_indexes(g, db, coll).await?;
        let hint = resolve_hint(&indexes, hint)?;
        let specs = index::ttl_specs(&indexes);
        let (docs, _, _) = find_docs(g, coll, &indexes, &FindOpts { filter, sort, want, hint, ttl: ttl_ctx(&specs), ov }).await?;
        Ok(docs)
    }

    pub(crate) async fn find(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        let coll = collection_name(cmd, "find")?;
        let filter = cmd.get_document("filter").cloned().unwrap_or_default();
        let mut sort = match cmd.get_document("sort") {
            Ok(s) => parse_sort(s)?,
            Err(_) => vec![],
        };
        // sort({$natural: ±1}) is the natural-order scan, forward or backward
        let natural_hint = match sort.as_slice() {
            [(f, asc)] if f == "$natural" && cmd.get("hint").is_none() => Some(bson::Bson::Document(doc! { "$natural": if *asc { 1 } else { -1 } })),
            _ => None,
        };
        if natural_hint.is_some() {
            sort.clear();
        }
        let hint_arg = natural_hint.as_ref().or_else(|| cmd.get("hint"));
        crate::query::probe_filter(&filter)?;
        let ext = crate::find_ext::parse(cmd)?;
        let projection = match cmd.get_document("projection") {
            Ok(p) => crate::projfind::parse(p, &filter)?,
            Err(_) => None,
        };
        let skip = int_arg(cmd.get("skip")).unwrap_or(0).max(0) as usize;
        let limit = int_arg(cmd.get("limit")).unwrap_or(0);
        let batch = int_arg(cmd.get("batchSize")).map(|b| b.max(0) as usize);
        let single = bool_arg(cmd.get("singleBatch"), false);
        let want = if limit > 0 { Some(skip + limit as usize) } else { None };
        if (sort.iter().any(|(f, _)| f == crate::text::SCORE_KEY) || projection.as_ref().map(|p| p.needs_text_score()).unwrap_or(false)) && !crate::text::has_text(&filter) {
            return Err(MongoError::new(40218, "Location40218", "query requires text score metadata, but it is not available".into()));
        }
        if (sort.iter().any(|(f, _)| f == crate::geo::DIST_KEY) || projection.as_ref().map(|p| p.needs_geo_near()).unwrap_or(false)) && crate::geo_query::find_near(&filter)?.is_none() {
            return Err(MongoError::new(40218, "Location40218", "query requires geoNear metadata, but it is not available".into()));
        }
        if bool_arg(cmd.get("awaitData"), false) && !bool_arg(cmd.get("tailable"), false) {
            return Err(MongoError::new(9, "FailedToParse", "Cannot set 'awaitData' without also setting 'tailable'".into()));
        }
        if bool_arg(cmd.get("tailable"), false) {
            if cmd.get_document("sort").map(|s| !(s.len() == 1 && s.get("$natural").map(|v| crate::query::as_f64(v) == 1.0).unwrap_or(false))).unwrap_or(false) {
                return Err(MongoError::bad_value("cannot use tailable option with a sort other than {$natural: 1}".into()));
            }
            if single {
                return Err(MongoError::bad_value("cannot use tailable option with the 'singleBatch' option".into()));
            }
            return self.find_tailable(db, coll, &filter, projection.as_ref(), limit, skip, batch, bool_arg(cmd.get("awaitData"), false), tx).await;
        }
        let mut docs = if ext.active() {
            let (sort_doc, proj_doc) = (cmd.get_document("sort").ok(), cmd.get_document("projection").ok());
            if tx.is_none() {
                let mut g = self.dbs.read(db).await?;
                self.find_ext(&mut g, db, coll, &filter, &sort, hint_arg, sort_doc, proj_doc, None, &ext).await?
            } else {
                let mut g = self.dbs.lock(db).await;
                attach(&mut g, tx);
                self.find_ext(&mut *g, db, coll, &filter, &sort, hint_arg, sort_doc, proj_doc, tx_ov(tx), &ext).await?
            }
        } else if tx.is_none() {
            let mut g = self.dbs.read(db).await?;
            self.find_part(&mut g, db, coll, &filter, &sort, want, hint_arg, None).await.map_err(|e| crate::find_ext::hint_error(e, &format!("{db}.{coll}"), &filter, cmd.get_document("sort").ok(), cmd.get_document("projection").ok()))?
        } else {
            let mut g = self.dbs.lock(db).await;
            attach(&mut g, tx);
            self.find_part(&mut *g, db, coll, &filter, &sort, want, hint_arg, tx_ov(tx)).await.map_err(|e| crate::find_ext::hint_error(e, &format!("{db}.{coll}"), &filter, cmd.get_document("sort").ok(), cmd.get_document("projection").ok()))?
        };
        if sort.is_empty() && cmd.get("hint").map(|h| matches!(h, Bson::Document(d) if d.contains_key("$natural"))).unwrap_or(true) {
            if self.coll_kind(db, coll).await?.map(|k| k.clustered.is_some()).unwrap_or(false) {
                let rev = natural_hint.as_ref().or_else(|| cmd.get("hint")).and_then(|h| h.as_document()).and_then(|d| d.get("$natural")).map(|n| crate::query::as_f64(n) < 0.0).unwrap_or(false);
                crate::special_colls::sort_by_id(&mut docs, rev);
            }
        }
        sort_docs(&mut docs, &sort);
        let mut docs: VecDeque<Document> = docs.into_iter().skip(skip).collect();
        if limit > 0 {
            docs.truncate(limit as usize);
        }
        if let (Some(p), false) = (&projection, ext.return_key) {
            for d in docs.iter_mut() {
                let rid = d.remove("$recordId");
                *d = p.apply(d)?;
                if let Some(r) = rid {
                    d.insert("$recordId", r);
                }
            }
        }
        for d in docs.iter_mut() {
            crate::text::strip_hidden(d);
            if coll == "system.profile" {
                d.remove("_id");
            }
        }
        let limited = limit > 0 && docs.len() >= limit as usize;
        let reply = self.cursor_reply_open(format!("{db}.{coll}"), docs, batch, single, limited);
        if matches!(cmd.get("noCursorTimeout"), Some(Bson::Boolean(true))) {
            self.mark_no_cursor_timeout(&reply);
        }
        Ok(reply)
    }

    /// `find` with `tailable`: only a capped collection has a natural order that new documents are added to. The cursor
    /// stays open after the last document and `getMore` continues from there.
    #[allow(clippy::too_many_arguments)]
    async fn find_tailable(&self, db: &str, coll: &str, filter: &Document, projection: Option<&crate::projfind::FindProjection>, limit: i64, skip: usize, batch: Option<usize>, await_data: bool, tx: &mut Option<Txn>) -> R<Document> {
        let ns = format!("{db}.{coll}");
        let mut g = self.dbs.read(db).await?;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Ok(self.cursor_reply_open(ns, VecDeque::new(), batch, false, false));
        }
        let _ = tx;
        let capped = g.coll_opts(coll).await?.map(|o| o.cap.is_some()).unwrap_or(false);
        if !capped {
            let tree = if filter.is_empty() { "$and".to_string() } else { crate::update::fmt_doc(filter) };
            return Err(MongoError::bad_value(format!("error processing query: ns={ns}Tree: {tree}\nSort: {{}}\nProj: {{}}\n tailable cursor requested on non capped collection")));
        }
        let tail = crate::capped::Tail { db: db.to_string(), coll: coll.to_string(), filter: filter.clone(), await_data, last_seq: 0 };
        let first_n = batch.unwrap_or(DEFAULT_FIRST_BATCH_TAIL).min(if limit > 0 { limit as usize } else { usize::MAX });
        let (mut docs, last) = crate::capped::fetch_after(&mut g, &tail, first_n.saturating_add(skip)).await?;
        docs.drain(..skip.min(docs.len()));
        docs.truncate(first_n);
        drop(g);
        if let Some(p) = projection {
            for d in docs.iter_mut() {
                *d = p.apply(d)?;
            }
        }
        Ok(self.tail_reply(ns, docs, crate::capped::Tail { last_seq: last, ..tail }))
    }

    pub(crate) async fn count(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        let coll = collection_name(cmd, "count")?;
        let filter = cmd.get_document("query").cloned().unwrap_or_default();
        let skip = int_arg(cmd.get("skip")).unwrap_or(0).max(0) as usize;
        let limit = int_arg(cmd.get("limit")).unwrap_or(0).unsigned_abs() as usize;
        let n = if tx.is_none() {
            let mut g = self.dbs.read(db).await?;
            self.matching(&mut g, db, coll, &filter, cmd.get("hint"), None).await?.len()
        } else {
            let mut g = self.dbs.lock(db).await;
            attach(&mut g, tx);
            self.matching(&mut *g, db, coll, &filter, cmd.get("hint"), tx_ov(tx)).await?.len()
        };
        let mut n = n.saturating_sub(skip);
        if limit > 0 {
            n = n.min(limit);
        }
        if tx.is_none() && self.coll_kind(db, coll).await?.map(|k| k.series.is_some()).unwrap_or(false) {
            // counted by an aggregation on a time-series collection, which replies with an integer ok
            return Ok(doc! { "n": n as i32, "ok": 1 });
        }
        Ok(doc! { "n": n as i32, "ok": 1.0 })
    }

    pub(crate) async fn distinct(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        let coll = collection_name(cmd, "distinct")?;
        let key = cmd.get_str("key").map_err(|_| MongoError::new(40414, "IDLFailedToParse", "BSON field 'distinct.key' is missing but a required field".into()))?;
        let filter = cmd.get_document("query").cloned().unwrap_or_default();
        let docs = if tx.is_none() {
            let mut g = self.dbs.read(db).await?;
            self.matching(&mut g, db, coll, &filter, cmd.get("hint"), None).await?
        } else {
            let mut g = self.dbs.lock(db).await;
            attach(&mut g, tx);
            self.matching(&mut *g, db, coll, &filter, cmd.get("hint"), tx_ov(tx)).await?
        };
        let values = distinct_values(&docs, key);
        Ok(doc! { "values": values, "ok": 1.0 })
    }

    pub(crate) async fn aggregate(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        match cmd.get("explain") {
            None => {}
            Some(Bson::Boolean(false)) => {}
            Some(Bson::Boolean(true)) => {
                let mut inner = Document::new();
                for (k, v) in cmd {
                    if k != "explain" && !k.starts_with('$') && !matches!(k.as_str(), "lsid" | "txnNumber") {
                        inner.insert(k.clone(), v.clone());
                    }
                }
                if !inner.contains_key("cursor") {
                    inner.insert("cursor", doc! {});
                }
                let mut out = self.explain(db, &doc! { "explain": inner, "verbosity": "queryPlanner" }).await?;
                let mut echo = cmd.clone();
                echo.remove("$clusterTime");
                echo.remove("$readPreference");
                out.insert("command", echo);
                return Ok(out);
            }
            Some(_) => return Err(MongoError::new(14, "TypeMismatch", "explain must be a boolean".into())),
        }
        let pipeline = cmd.get_array("pipeline").map_err(|_| MongoError::new(40414, "IDLFailedToParse", "BSON field 'aggregate.pipeline' is missing but a required field".into()))?;
        if let Some(Bson::Document(spec)) = pipeline.first().and_then(|s| s.as_document()).and_then(|s| s.get("$changeStream")) {
            return self.open_change_stream(db, cmd, spec, &pipeline[1..]).await;
        }
        if matches!(cmd.get("aggregate"), Some(Bson::Int32(1) | Bson::Int64(1) | Bson::Double(1.0))) {
            return self.aggregate_database(db, cmd, pipeline);
        }
        let coll = collection_name(cmd, "aggregate")?;
        // empty leading $match stages are optimized away, which lets a following $geoNear be first
        let lead = pipeline.iter().take_while(|s| matches!(s.as_document().and_then(|d| d.get_document("$match").ok()), Some(m) if m.is_empty() && s.as_document().map(|d| d.len()) == Some(1))).count();
        let pipeline: &Vec<Bson> = &if lead > 0 && pipeline.get(lead).and_then(|s| s.as_document()).map(|d| d.contains_key("$geoNear")).unwrap_or(false) { pipeline[lead..].to_vec() } else { pipeline.clone() };
        let cursor = cmd.get_document("cursor").map_err(|_| MongoError::failed_to_parse("The 'cursor' option is required, except for aggregate with the explain argument".into()))?;
        let batch = int_arg(cursor.get("batchSize")).map(|b| b.max(0) as usize);
        let stage_name = |s: &Bson| s.as_document().and_then(|d| d.keys().next().cloned()).unwrap_or_default();
        if pipeline_match_has_near(pipeline) {
            return Err(MongoError::new(5626500, "Location5626500", "$geoNear, $near, and $nearSphere are not allowed in this context, as these operators require sorting geospatial data. If you do not need sort, consider using $geoWithin instead. Check out https://dochub.mongodb.org/core/near-aggregation for more details.".to_string()));
        }
        for (i, s) in pipeline.iter().enumerate() {
            let n = stage_name(s);
            if matches!(n.as_str(), "$search" | "$searchMeta") && i != 0 {
                return Err(MongoError::new(40602, "Location40602", format!("{n} is only valid as the first stage in a pipeline.")));
            }
            if n == "$geoNear" && i != 0 {
                return Err(MongoError::new(40603, "Location40603", "$geoNear was not the first stage in the pipeline".to_string()));
            }
            if (n == "$collStats" || n == "$indexStats" || n == "$listSearchIndexes") && i != 0 {
                return Err(MongoError::new(40602, "Location40602", format!("{n} is only valid as the first stage in a pipeline.")));
            }
            if (n == "$out" || n == "$merge") && i + 1 != pipeline.len() {
                return Err(MongoError::new(40601, "Location40601", format!("{n} can only be the final stage in the pipeline")));
            }
            if let Some(arg) = s.as_document().and_then(|d| d.get(n.as_str())).filter(|_| n == "$out" || n == "$merge") {
                if n == "$out" { crate::xdb::parse_out(arg, db)?; } else { crate::xdb::parse_merge(arg, db)?; }
            }
        }
        let (body, tail): (&[Bson], Option<&Bson>) = match pipeline.last() {
            Some(l) if matches!(stage_name(l).as_str(), "$out" | "$merge") => (&pipeline[..pipeline.len() - 1], Some(l)),
            _ => (&pipeline[..], None),
        };
        let first = body.first().and_then(|s| s.as_document());
        let docs = if let Some(spec) = first.and_then(|d| d.get("$collStats")) {
            let mut g = self.dbs.lock(db).await;
            attach(&mut g, tx);
            self.coll_stats_stage(&mut g, db, coll, spec).await?
        } else if first.map(|d| d.contains_key("$indexStats")).unwrap_or(false) {
            let mut g = self.dbs.lock(db).await;
            attach(&mut g, tx);
            self.index_stats_stage(&mut g, db, coll).await?
        } else if let Some(spec) = first.and_then(|d| d.get("$listSearchIndexes")) {
            let mut g = self.dbs.lock(db).await;
            attach(&mut g, tx);
            self.list_search_indexes_stage(&mut g, coll, spec).await?
        } else if first.map(|d| d.contains_key("$planCacheStats")).unwrap_or(false) {
            Vec::new()
        } else if let Some((sname, sspec)) = first.and_then(|d| d.iter().find(|(k, _)| matches!(k.as_str(), "$search" | "$searchMeta"))) {
            let all = if tx.is_none() {
                let mut g = self.dbs.read(db).await?;
                self.matching(&mut g, db, coll, &Document::new(), None, None).await?
            } else {
                let mut g = self.dbs.lock(db).await;
                attach(&mut g, tx);
                self.matching(&mut *g, db, coll, &Document::new(), None, tx_ov(tx)).await?
            };
            crate::search::run_stage(sname, sspec, all)?
        } else if let Some(spec) = first.and_then(|d| d.get("$geoNear")) {
            let (filter, st) = geo_near_filter(spec)?;
            let mut docs = if tx.is_none() {
                let mut g = self.dbs.read(db).await?;
                self.geo_near_docs(&mut g, db, coll, &filter, cmd.get("hint"), None, &st).await?
            } else {
                let mut g = self.dbs.lock(db).await;
                attach(&mut g, tx);
                self.geo_near_docs(&mut *g, db, coll, &filter, cmd.get("hint"), tx_ov(tx), &st).await?
            };
            apply_geo_near_fields(&mut docs, &st)?;
            docs
        } else {
            let first_match = first.and_then(|s| s.get_document("$match").ok()).cloned().unwrap_or_default();
            // The pipeline's own $match rejects $where; do not run it here as a plain find filter.
            let first_match = if format!("{first_match:?}").contains("$where") { Document::new() } else { first_match };
            if tx.is_none() {
                let mut g = self.dbs.read(db).await?;
                self.matching(&mut g, db, coll, &first_match, cmd.get("hint"), None).await?
            } else {
                let mut g = self.dbs.lock(db).await;
                attach(&mut g, tx);
                self.matching(&mut *g, db, coll, &first_match, cmd.get("hint"), tx_ov(tx)).await?
            }
        };
        let first_is_text_match = first.and_then(|d| d.get_document("$match").ok()).map(crate::text::has_text).unwrap_or(false);
        let first_is_text_match = first_is_text_match || first.map(|d| d.contains_key("$search") || d.contains_key("$searchMeta")).unwrap_or(false);
        let body_rest: &[Bson] = if first.map(|d| d.contains_key("$collStats") || d.contains_key("$indexStats") || d.contains_key("$listSearchIndexes") || d.contains_key("$geoNear") || d.contains_key("$planCacheStats")).unwrap_or(false) || first_is_text_match { &body[1..] } else { body };
        let mut foreign = crate::agg::Foreign::new();
        for from in crate::agg::lookup_collections(body_rest) {
            let fd = if let Some(v) = Box::pin(self.view_docs(db, &from, tx)).await? {
                v
            } else if tx.is_none() {
                let mut g = self.dbs.read(db).await?;
                self.scan_visible(&mut g, db, &from).await?
            } else {
                let mut g = self.dbs.lock(db).await;
                attach(&mut g, tx);
                self.scan_visible(&mut *g, db, &from).await?
            };
            let geo_meta: Vec<Document> = {
                let ix = if tx.is_none() {
                    let mut g = self.dbs.read(db).await?;
                    self.load_indexes(&mut g, db, &from).await?
                } else {
                    let mut g = self.dbs.lock(db).await;
                    attach(&mut g, tx);
                    self.load_indexes(&mut *g, db, &from).await?
                };
                ix.iter().filter_map(|i| match &i.kind { index::Kind::Geo(gs) if i.state == index::State::Ready => Some(doc! { "field": gs.field.clone(), "sphere": gs.sphere }), _ => None }).collect()
            };
            foreign.insert(format!("\u{0}geo:{from}"), geo_meta);
            foreign.insert(from, fd);
        }
        let mut out = run_pipeline(docs, body_rest, &foreign)?;
        for d in out.iter_mut() {
            crate::text::strip_hidden(d);
        }
        if let Some(t) = tail {
            self.agg_write_stage(db, t, out, tx).await?;
            return Ok(self.cursor_reply_open(format!("{db}.{coll}"), VecDeque::new(), batch, false, false));
        }
        Ok(self.cursor_reply_open(format!("{db}.{coll}"), out.into(), batch, false, false))
    }

    /// The documents of a `$collStats` stage.
    async fn coll_stats_stage(&self, g: &mut Entry, db: &str, coll: &str, spec: &Bson) -> R<Vec<Document>> {
        let spec = spec.as_document().ok_or_else(|| MongoError::new(5447000, "Location5447000", "$collStats must take a nested object but found".to_string()))?;
        for k in spec.keys() {
            if !matches!(k.as_str(), "latencyStats" | "storageStats" | "count" | "queryExecStats") {
                return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field '$collStats.{k}' is an unknown field.")));
            }
        }
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::new(26, "NamespaceNotFound", format!("Unable to retrieve storageStats in $collStats stage :: caused by :: Collection [{db}.{coll}] not found.")));
        }
        let mut d = doc! { "ns": format!("{db}.{coll}"), "host": "mongo-server", "localTime": bson::DateTime::now() };
        if spec.contains_key("latencyStats") {
            d.insert("latencyStats", doc! { "reads": { "latency": 0_i64, "ops": 0_i64 }, "writes": { "latency": 0_i64, "ops": 0_i64 }, "commands": { "latency": 0_i64, "ops": 0_i64 }, "transactions": { "latency": 0_i64, "ops": 0_i64 } });
        }
        if spec.contains_key("storageStats") {
            let scale = crate::stats_scale::scale_arg("$collStats.storageStats.scale", spec.get_document("storageStats").ok().and_then(|s| s.get("scale")), false)?;
            let mut st = self.storage_stats(g, db, coll).await?;
            crate::stats_scale::scale_collection(&mut st, scale);
            st.remove("nindexes_unused");
            d.insert("storageStats", st);
        }
        if spec.contains_key("count") {
            let n = self.storage_stats(g, db, coll).await?.get_i32("count").unwrap_or(0);
            d.insert("count", n);
        }
        Ok(vec![d])
    }

    async fn index_stats_stage(&self, g: &mut Entry, db: &str, coll: &str) -> R<Vec<Document>> {
        let _ = db;
        let indexes = g.indexes(coll).await?;
        let now = bson::DateTime::now();
        let mut out = vec![doc! { "name": "_id_", "key": { "_id": 1 }, "host": "mongo-server", "accesses": { "ops": 0_i64, "since": now }, "spec": { "v": 2, "key": { "_id": 1 }, "name": "_id_" } }];
        for i in indexes.iter().filter(|i| i.state == index::State::Ready) {
            out.push(doc! { "name": i.name.clone(), "key": i.key_pattern(), "host": "mongo-server", "accesses": { "ops": 0_i64, "since": now }, "spec": i.spec.clone() });
        }
        Ok(out)
    }

    /// `$out` / `$merge`: write the pipeline's output into a collection of the same database.
    pub(crate) async fn agg_write_stage(&self, db: &str, stage: &Bson, docs: Vec<Document>, tx: &mut Option<Txn>) -> R<()> {
        let sd = stage.as_document().unwrap();
        let (name, spec) = sd.iter().next().unwrap();
        if name == "$out" {
            let t = crate::xdb::parse_out(spec, db)?;
            let (tdb, target) = (t.db, t.coll);
            let mut g = self.dbs.lock(&tdb).await;
            if tdb == db {
                attach(&mut g, tx);
            }
            let ns_names = g.collections().await?;
            // (the dup-key messages of mongod name the temporary collection the results are first written to)
            let ns = format!("{tdb}.tmp.agg_out.{}", crate::cmd::fake_uuid());
            let mut indexes = self.load_indexes(&mut g, &tdb, &target).await?;
            let mut w = take_writes(tx);
            let existing = {
                let (docs, _, _) = find_docs(&mut g, &target, &indexes, &FindOpts { filter: &Document::new(), sort: &[], want: None, hint: Hint::None, ttl: None, ov: Some(&w.ov) }).await?;
                docs
            };
            for d in &existing {
                w.delete(&mut g, &tdb, &target, &indexes, d).await?;
            }
            for mut d in docs {
                if !d.contains_key("_id") {
                    let mut with_id = doc! { "_id": ObjectId::new() };
                    for (k, v) in d {
                        with_id.insert(k, v);
                    }
                    d = with_id;
                }
                w.put(&mut g, &target, &ns, &mut indexes, &d, None).await?;
            }
            let names = w.names.clone().unwrap_or(ns_names.clone());
            let new_coll = (!names.iter().any(|n| *n == target)).then(|| {
                let mut v = names.clone();
                v.push(target.clone());
                v
            });
            return self.commit(&mut g, &target, w, &indexes, new_coll, tx).await;
        }
        // $merge
        let m = crate::xdb::parse_merge(spec, db)?;
        let (tdb, into, on, when_matched, when_not) = (m.target.db, m.target.coll, m.on, m.when_matched, m.when_not_matched);
        let lets = m.lets;
        let mut g = self.dbs.lock(&tdb).await;
        if tdb == db {
            attach(&mut g, tx);
        }
        let ns_names = g.collections().await?;
        let ns = format!("{tdb}.{into}");
        let mut indexes = self.load_indexes(&mut g, &tdb, &into).await?;
        crate::xdb::check_merge_on(&on, ns_names.iter().any(|n| *n == into), &indexes, g.collation_of(&into).is_some())?;
        let mut w = take_writes(tx);
        // Merging and keep-existing are executed as `$set`-style upserts: new fields are applied in name order.
        let sorted_mode = on == ["_id".to_string()] && matches!(&when_matched, Bson::String(m) if m == "merge" || m == "keepExisting");
        for d in docs {
            let mut filter = Document::new();
            let mut no_match = false;
            for f in &on {
                match crate::expr::field_path(&d, f) {
                    Some(Bson::Array(_)) => return Err(MongoError::new(51185, "Location51185", format!("$merge write error: 'on' field '{f}' is an array"))),
                    Some(v) if !matches!(v, Bson::Null | Bson::Undefined) => {
                        filter.insert(f.clone(), v);
                    }
                    // a document without `_id` is inserted with a new one
                    None if f == "_id" => no_match = true,
                    _ => return Err(MongoError::new(51132, "Location51132", format!("$merge write error: 'on' field '{f}' cannot be missing, null, undefined or an array")))
                }
            }
            let found = if no_match { Vec::new() } else { find_docs(&mut g, &into, &indexes, &FindOpts { filter: &filter, sort: &[], want: Some(1), hint: Hint::None, ttl: None, ov: Some(&w.ov) }).await?.0 };
            match found.into_iter().next() {
                Some(old) => {
                    let new = match &when_matched {
                        Bson::String(m) if m == "merge" => {
                            let mut n = old.clone();
                            let mut keys: Vec<&String> = d.keys().filter(|k| *k != "_id").collect();
                            keys.sort();
                            for k in keys {
                                n.insert(k.clone(), d.get(k).cloned().unwrap());
                            }
                            n
                        }
                        Bson::String(m) if m == "replace" => {
                            let mut n = d.clone();
                            n.insert("_id", old.get("_id").cloned().unwrap_or(Bson::Null));
                            let mut ordered = doc! { "_id": old.get("_id").cloned().unwrap_or(Bson::Null) };
                            for (k, v) in n {
                                ordered.insert(k, v);
                            }
                            ordered
                        }
                        Bson::String(m) if m == "keepExisting" => continue,
                        Bson::String(m) if m == "fail" => {
                            let mut e = MongoError::duplicate_key_c(&ns, "_id_", &doc! { "_id": 1 }, &doc! { "_id": old.get("_id").cloned().unwrap_or(Bson::Null) }, g.collation_of(&into).as_deref());
                            e.msg = format!("$merge with whenMatched: fail found an existing document with the same values for the 'on' fields :: caused by :: {}", e.msg);
                            return Err(e);
                        }
                        Bson::Array(p) => {
                            let mut vars = vec![("new".to_string(), Bson::Document(d.clone()))];
                            if let Some(l) = &lets {
                                for (k, e) in l {
                                    let v = crate::expr::eval(e, &crate::expr::Scope::new(&d))?.unwrap_or(Bson::Null);
                                    vars.push((k.clone(), v));
                                }
                            }
                            let mut r = crate::expr::with_ambient(vars, || run_pipeline(vec![old.clone()], p, &crate::agg::Foreign::new()))?;
                            r.pop().unwrap_or_else(|| old.clone())
                        }
                        _ => return Err(MongoError::new(51181, "Location51181", "unsupported whenMatched mode".into())),
                    };
                    if new != old {
                        w.put(&mut g, &into, &ns, &mut indexes, &new, Some(&old)).await?;
                    }
                }
                None => match when_not.as_str() {
                    "insert" => {
                        // A $merge that creates the target collection inserts the documents with their top-level fields
                        // in name order, `_id` first; into an existing collection the source order is kept.
                        let mut n = d.clone();
                        if sorted_mode {
                            let mut keys: Vec<String> = d.keys().filter(|k| *k != "_id").cloned().collect();
                            keys.sort();
                            n = doc! { "_id": d.get("_id").cloned().unwrap_or_else(|| Bson::ObjectId(ObjectId::new())) };
                            for k in keys {
                                n.insert(k.clone(), d.get(&k).cloned().unwrap());
                            }
                        } else if !n.contains_key("_id") {
                            let mut with_id = doc! { "_id": ObjectId::new() };
                            for (k, v) in n {
                                with_id.insert(k, v);
                            }
                            n = with_id;
                        }
                        w.put(&mut g, &into, &ns, &mut indexes, &n, None).await?;
                    }
                    "discard" => {}
                    "fail" => return Err(MongoError::new(13113, "MergeStageNoMatchingDocument", "$merge could not find a matching document in the target collection for at least one document in the source collection".into())),
                    _ => return Err(MongoError::new(51181, "Location51181", "unsupported whenNotMatched mode".into())),
                },
            }
        }
        let names = w.names.clone().unwrap_or(ns_names.clone());
        // (a $merge that writes nothing, such as one that discards what does not match, does not create the target)
        let new_coll = (!names.iter().any(|n| *n == into) && !w.ops.is_empty()).then(|| {
            let mut v = names.clone();
            v.push(into.clone());
            v
        });
        self.commit(&mut g, &into, w, &indexes, new_coll, tx).await
    }

    // ---- writes ----------------------------------------------------------------------------

    /// Apply a write set (plus catalog changes) as one batch, or, inside a transaction, keep it in the transaction.
    pub(crate) async fn commit(&self, g: &mut Entry, coll: &str, mut w: Writes, indexes: &[IndexDef], new_coll: Option<Vec<String>>, tx: &mut Option<Txn>) -> R<()> {
        if w.indexes_dirty {
            w.meta.insert(coll.to_string(), indexes.to_vec());
            w.indexes_dirty = false;
        }
        let created = new_coll.is_some() && coll != "system.views";
        if new_coll.is_some() {
            w.names = new_coll;
        }
        if let Some(t) = tx {
            t.w = w;
            return Ok(());
        }
        let colls: Vec<String> = w.cap_delta.keys().cloned().collect();
        for c in colls {
            let ix = if c == coll { indexes.to_vec() } else { g.indexes(&c).await? };
            crate::capped::enforce(g, "", &mut w, &ix, &c).await?;
        }
        let mut events = std::mem::take(&mut w.events);
        if coll == "system.views" {
            events.clear();
        }
        let ops = w.final_ops()?;
        g.apply(ops).await?;
        if created {
            // a collection made by a write: its `create` event comes first
            let uuid = g.collection_uuid(coll).await?;
            let db = events.first().map(|e| e.db.clone());
            if let Some(db) = db {
                let id_index = doc! { "idIndex": { "v": 2, "key": { "_id": 1 }, "name": "_id_" } };
                events.insert(0, crate::changes::EvSpec { uuid: Some(uuid), op_desc: Some(id_index), expanded_only: true, ..crate::changes::EvSpec::on(&db, coll, "create") });
            }
        }
        self.dbs.changes.publish(events);
        Ok(())
    }

    pub(crate) async fn insert(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        let coll = collection_name(cmd, "insert")?;
        let docs = cmd.get_array("documents").map_err(|_| MongoError::new(40414, "IDLFailedToParse", "BSON field 'insert.documents' is missing but a required field".into()))?;
        let ordered = bool_arg(cmd.get("ordered"), true) || tx.is_some();
        let ns = format!("{db}.{coll}");
        let mut rr = self.retry_begin(db, cmd, docs.len(), "insert").await?;
        let mut g = self.dbs.lock(db).await;
        attach(&mut g, tx);
        let names = g.collections().await?;
        let mut indexes = self.load_indexes(&mut g, db, coll).await?;
        self.presweep(&mut g, coll, &indexes).await?;
        let mut w = take_writes(tx);
        w.bypass_validation = bool_arg(cmd.get("bypassDocumentValidation"), false);
        let names = w.names.clone().unwrap_or(names);
        let kind = g.coll_opts(coll).await?;
        let mut errors: Vec<Bson> = Vec::new();
        let mut n = 0;
        let mut replayed = 0;
        let mut ts_ordered_fail = false;
        let docs_len = docs.len();
        for (i, raw) in docs.iter().enumerate() {
            if let Some(b) = rr.as_mut() {
                if b.run.is_retried(i) {
                    b.run.stored(i);
                    replayed += 1;
                    continue;
                }
            }
            let Some(d) = raw.as_document() else {
                errors.push(Bson::Document(write_error(i, 14, "document to insert must be an object".into())));
                if ordered { break } else { continue }
            };
            let mut d = d.clone();
            if let Some((tf, _)) = kind.as_ref().and_then(|k| k.series.as_ref()) {
                if !matches!(d.get(tf.as_str()), Some(Bson::DateTime(_))) {
                    errors.push(Bson::Document(write_error(i, 2, format!("'{tf}' must be present and contain a valid BSON UTC datetime value"))));
                    if ordered { ts_ordered_fail = true; break } else { continue }
                }
                if d.get("_id").is_none() {
                    d.insert(crate::text::TS_AUTO, true);
                }
                d = crate::special_colls::series_order(d, kind.as_ref().and_then(|k| k.series.as_ref()).unwrap());
            }
            if d.get("_id").is_none() {
                let mut with_id = doc! { "_id": ObjectId::new() };
                for (k, v) in d {
                    with_id.insert(k, v);
                }
                d = with_id;
            } else if d.keys().next().map(String::as_str) != Some("_id") && !kind.as_ref().map(|k| k.series.is_some()).unwrap_or(false) {
                // MongoDB always stores `_id` as the first field.
                let id = d.remove("_id").unwrap();
                let mut with_id = doc! { "_id": id };
                for (k, v) in d {
                    with_id.insert(k, v);
                }
                d = with_id;
            }
            let id = d.get("_id").unwrap().clone();
            if matches!(id, Bson::Array(_) | Bson::RegularExpression(_) | Bson::Undefined) {
                errors.push(Bson::Document(write_error(i, 53, format!("The '_id' value cannot be of type {}", crate::update::type_name(&id)))));
                if ordered { break } else { continue }
            }
            // An expired-but-not-yet-removed document still holds its _id and its unique keys.
            if w.exists(&mut g, coll, &id).await? {
                let mut e = MongoError::duplicate_key_c(&ns, "_id_", &doc! { "_id": 1 }, &doc! { "_id": id.clone() }, g.collation_of(coll).as_deref());
                if kind.as_ref().map(|k| k.clustered.is_some()).unwrap_or(false) {
                    // (a clustered collection has no _id index to name)
                    e.msg = format!("E11000 duplicate key error collection: {ns} dup key: {} found value: {{}}", crate::update::fmt_doc(&doc! { "_id": id.clone() }));
                    if let Some(x) = e.extra.as_mut() {
                        x.insert("foundValue", doc! {});
                    }
                }
                errors.push(Bson::Document(e.to_write_error(i)));
                if ordered { break } else { continue }
            }
            if let Err(e) = w.put(&mut g, coll, &ns, &mut indexes, &d, None).await {
                if e.code == 112 {
                    return Err(e);
                }
                errors.push(Bson::Document(e.to_write_error(i)));
                if ordered { break } else { continue }
            }
            n += 1;
            if let Some(b) = rr.as_mut() {
                b.run.record_insert(i);
            }
        }
        let new_coll = (!names.iter().any(|x| x == coll) && n > 0).then(|| {
            let mut v = names.clone();
            v.push(coll.to_string());
            v
        });
        if let Some(b) = &rr {
            b.run.stage(&mut w, &b.key);
        }
        self.commit(&mut g, coll, w, &indexes, new_coll, tx).await?;
        if let Some(b) = rr.as_mut() {
            b.run.apply();
        }
        let mut reply = doc! { "n": if ts_ordered_fail { docs_len as i32 } else { (n + replayed) as i32 } };
        if !errors.is_empty() {
            reply.insert("writeErrors", errors);
        }
        if let Some(b) = &rr {
            if replayed > 0 {
                reply.insert("retriedStmtIds", b.run.retried_ids());
            }
        }
        reply.insert("ok", 1.0);
        Ok(reply)
    }

    pub(crate) async fn update(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        let coll = collection_name(cmd, "update")?;
        let specs = cmd.get_array("updates").map_err(|_| MongoError::new(40414, "IDLFailedToParse", "BSON field 'update.updates' is missing but a required field".into()))?;
        let ordered = bool_arg(cmd.get("ordered"), true) || tx.is_some();
        let ns = format!("{db}.{coll}");
        // The shape of a pipeline's stages is checked while parsing the command, before any statement runs.
        for spec in specs {
            if let Some(Bson::Array(stages)) = spec.as_document().and_then(|s| s.get("u")) {
                if stages.iter().any(|s| s.as_document().map(|d| d.len() != 1).unwrap_or(true)) {
                    return Err(MongoError::new(40323, "Location40323", "A pipeline stage specification object must contain exactly one field.".into()));
                }
            }
        }
        let cols = specs.iter().map(|s| match s.as_document() { Some(d) => crate::collation::of_statement("update", d), None => Ok(Ok(None)) }).collect::<R<Vec<_>>>()?;
        let mut rr = self.retry_begin(db, cmd, specs.len(), "update").await?;
        let mut g = self.dbs.lock(db).await;
        attach(&mut g, tx);
        let names = g.collections().await?;
        let mut indexes = self.load_indexes(&mut g, db, coll).await?;
        self.presweep(&mut g, coll, &indexes).await?;
        let ttl_specs = index::ttl_specs(&indexes);
        let series = g.coll_opts(coll).await?.and_then(|k| k.series.clone());
        let mut w = take_writes(tx);
        w.bypass_validation = bool_arg(cmd.get("bypassDocumentValidation"), false);
        let names = w.names.clone().unwrap_or(names);
        let mut acc = UpdAcc::default();
        let mut errors: Vec<Bson> = Vec::new();

        'specs: for (i, spec) in specs.iter().enumerate() {
            if let Some(b) = rr.as_mut() {
                if b.run.is_retried(i) {
                    let d = b.run.stored(i).unwrap_or_default();
                    acc.n += 1;
                    acc.n_modified += d.get_i32("m").unwrap_or(0);
                    if let Some(id) = d.get("up") {
                        acc.upserted.push(Bson::Document(doc! { "index": i as i32, "_id": id.clone() }));
                    }
                    continue;
                }
                if spec.as_document().and_then(|s| s.get("multi")) == Some(&Bson::Boolean(true)) {
                    errors.push(Bson::Document(write_error(i, 72, "Cannot use (or request) retryable writes with multi=true".into())));
                    if ordered {
                        break 'specs;
                    }
                    continue;
                }
            }
            let before = (acc.n, acc.n_modified, acc.upserted.len());
            let series_err = series.as_ref().and_then(|t| spec.as_document().and_then(|d| crate::special_colls::series_update_error(t, d)));
            let step: R<()> = if let Some(e) = series_err { Err(e) } else { match &cols[i] { Err(e) => Err(e.clone()), Ok(None) => update_stmt(&mut g, coll, &ns, &mut indexes, &ttl_specs, &mut w, spec, i, &mut acc).await, Ok(Some(c)) => crate::collation::scope(Some(c.clone()), update_stmt(&mut g, coll, &ns, &mut indexes, &ttl_specs, &mut w, spec, i, &mut acc)).await } };
            if step.is_ok() {
                if let Some(b) = rr.as_mut() {
                    let modified = acc.n_modified - before.1;
                    let up = (acc.upserted.len() > before.2).then(|| acc.upserted.last().and_then(|u| u.as_document()).and_then(|u| u.get("_id").cloned())).flatten();
                    if modified > 0 || up.is_some() {
                        let mut d = doc! { "t": "u", "m": modified };
                        if let Some(id) = up {
                            d.insert("up", id);
                        }
                        b.run.record(i, d);
                    }
                }
            }
            if let Err(e) = step {
                if e.code == 112 {
                    return Err(e);
                }
                // a statement that failed counts nothing
                acc.n = before.0;
                acc.n_modified = before.1;
                acc.upserted.truncate(before.2);
                errors.push(Bson::Document(e.to_write_error(i)));
                if ordered {
                    break 'specs;
                }
            }
        }
        let new_coll = (!acc.upserted.is_empty() && !names.iter().any(|x| x == coll)).then(|| {
            let mut v = names.clone();
            v.push(coll.to_string());
            v
        });
        if let Some(b) = &rr {
            b.run.stage(&mut w, &b.key);
        }
        self.commit(&mut g, coll, w, &indexes, new_coll, tx).await?;
        if let Some(b) = rr.as_mut() {
            b.run.apply();
        }
        let mut reply = doc! { "n": acc.n };
        if !errors.is_empty() {
            reply.insert("writeErrors", errors);
        }
        if let Some(b) = &rr {
            if !b.run.retried.is_empty() {
                reply.insert("retriedStmtIds", b.run.retried_ids());
            }
        }
        if !acc.upserted.is_empty() {
            reply.insert("upserted", acc.upserted);
        }
        reply.insert("nModified", acc.n_modified);
        reply.insert("ok", 1.0);
        Ok(reply)
    }

    pub(crate) async fn find_and_modify(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        self.find_and_modify_inner(db, cmd, tx).await.map_err(|mut e| {
            // (a document that fails the collection validator surfaces from the plan executor)
            if e.code == 121 && !e.msg.starts_with("Plan executor") {
                e.msg = format!("Plan executor error during findAndModify :: caused by :: {}", e.msg);
            }
            e
        })
    }

    async fn find_and_modify_inner(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        let key = cmd.keys().next().cloned().unwrap_or_default();
        let coll = collection_name(cmd, &key)?;
        let ns = format!("{db}.{coll}");
        let q = match cmd.get("query") {
            None | Some(Bson::Null) => Document::new(),
            Some(Bson::Document(d)) => d.clone(),
            Some(_) => return Err(MongoError::type_mismatch("BSON field 'findAndModify.query' is the wrong type, expected object".into())),
        };
        let sort = match cmd.get("sort") {
            Some(Bson::Document(s)) => parse_sort(s)?,
            _ => vec![],
        };
        let remove = bool_arg(cmd.get("remove"), false);
        let want_new = bool_arg(cmd.get("new"), false);
        let upsert = bool_arg(cmd.get("upsert"), false);
        let projection = match cmd.get("fields") {
            Some(Bson::Document(p)) => crate::projfind::parse(p, &q)?,
            _ => None,
        };
        if want_new && projection.as_ref().map(|p| p.has_positional()).unwrap_or(false) {
            return Err(MongoError::bad_value("cannot use a positional projection and return the new document".into()));
        }
        let u = match cmd.get("update") {
            None => None,
            Some(u @ (Bson::Document(_) | Bson::Array(_))) => Some(Upd::new(u, &q, cmd.get("arrayFilters"), cmd.get("c"))?),
            Some(_) => return Err(MongoError::new(14, "TypeMismatch", "BSON field 'findAndModify.update' is the wrong type, expected object or array".into())),
        };
        if remove && u.is_some() {
            return Err(MongoError::failed_to_parse("Cannot specify both an update and remove=true".into()));
        }
        if remove && upsert {
            return Err(MongoError::failed_to_parse("Cannot specify both upsert=true and remove=true".into()));
        }
        if remove && want_new {
            return Err(MongoError::failed_to_parse("Cannot specify both new=true and remove=true; 'remove' always returns the deleted document".into()));
        }
        if !remove && u.is_none() {
            return Err(MongoError::failed_to_parse("Either an update or remove=true must be specified".into()));
        }
        let shape = |d: Option<Document>| -> R<Bson> {
            Ok(match d {
                None => Bson::Null,
                Some(d) => Bson::Document(match &projection {
                    Some(p) => p.apply(&d)?,
                    None => d,
                }),
            })
        };
        let mut rr = self.retry_begin(db, cmd, 1, "findAndModify").await?;
        if let Some(b) = rr.as_mut() {
            if b.run.is_retried(0) {
                let id = b.run.id(0);
                let stored = b.run.stored(0).unwrap_or_default();
                let mut lo = stored.get_document("lo").cloned().unwrap_or_default();
                if lo.is_empty() {
                    lo = doc! { "n": 1 };
                }
                let image = match stored.get("v") {
                    Some(Bson::Document(d)) => Some(d.clone()),
                    _ => None,
                };
                return Ok(doc! { "lastErrorObject": lo, "value": image.map(Bson::Document).unwrap_or(Bson::Null), "retriedStmtId": id, "ok": 1.0 });
            }
        }
        let mut g = self.dbs.lock(db).await;
        attach(&mut g, tx);
        if g.coll_opts(coll).await?.map(|k| k.series.is_some()).unwrap_or(false) {
            return Err(MongoError::new(166, "CommandNotSupportedOnView", format!("Taking {ns} lock for timeseries is not allowed")));
        }
        let names = g.collections().await?;
        let mut indexes = self.load_indexes(&mut g, db, coll).await?;
        self.presweep(&mut g, coll, &indexes).await?;
        let ttl_specs = index::ttl_specs(&indexes);
        let hint = resolve_hint(&indexes, cmd.get("hint"))?;
        let mut w = take_writes(tx);
        w.bypass_validation = bool_arg(cmd.get("bypassDocumentValidation"), false);
        let names = w.names.clone().unwrap_or(names);
        let (mut matched, _, _) = find_docs(&mut g, coll, &indexes, &FindOpts { filter: &q, sort: &sort, want: Some(1), hint, ttl: ttl_ctx(&ttl_specs), ov: Some(&w.ov) }).await?;
        for d in matched.iter_mut() { crate::text::strip_scores(d); }
        sort_docs(&mut matched, &sort);
        let hit = matched.into_iter().next();
        if remove {
            return Ok(match hit {
                None => doc! { "lastErrorObject": { "n": 0 }, "value": Bson::Null, "ok": 1.0 },
                Some(old) => {
                    w.delete(&mut g, db, coll, &indexes, &old).await?;
                    if let Some(b) = rr.as_mut() {
                        b.run.record(0, doc! { "t": "f", "lo": { "n": 1 }, "v": old.clone() });
                        b.run.stage(&mut w, &b.key);
                    }
                    self.commit(&mut g, coll, w, &indexes, None, tx).await?;
                    if let Some(b) = rr.as_mut() {
                        b.run.apply();
                    }
                    doc! { "lastErrorObject": { "n": 1 }, "value": shape(Some(old))?, "ok": 1.0 }
                }
            });
        }
        let u = u.unwrap();
        match hit {
            Some(old) => {
                let new = u.apply(&old)?;
                if u.take_forced() || new.to_vec()? != old.to_vec()? {
                    w.pending_update = u.operators().cloned();
                    w.put(&mut g, coll, &ns, &mut indexes, &new, Some(&old)).await?;
                    if let Some(b) = rr.as_mut() {
                        b.run.record(0, doc! { "t": "f", "lo": { "n": 1, "updatedExisting": true }, "v": if want_new { new.clone() } else { old.clone() } });
                        b.run.stage(&mut w, &b.key);
                    }
                    self.commit(&mut g, coll, w, &indexes, None, tx).await?;
                    if let Some(b) = rr.as_mut() {
                        b.run.apply();
                    }
                }
                let value = shape(Some(if want_new { new } else { old }))?;
                Ok(doc! { "lastErrorObject": { "n": 1, "updatedExisting": true }, "value": value, "ok": 1.0 })
            }
            None if upsert => {
                let nd = u.upsert()?;
                let id = nd.get("_id").cloned().unwrap();
                if w.exists(&mut g, coll, &id).await? {
                    return Err(MongoError::duplicate_key_c(&ns, "_id_", &doc! { "_id": 1 }, &doc! { "_id": nd.get("_id").cloned().unwrap_or(Bson::Null) }, g.collation_of(coll).as_deref()));
                }
                w.put(&mut g, coll, &ns, &mut indexes, &nd, None).await?;
                let new_coll = (!names.iter().any(|x| x == coll)).then(|| {
                    let mut v = names.clone();
                    v.push(coll.to_string());
                    v
                });
                if let Some(b) = rr.as_mut() {
                    let mut d = doc! { "t": "f", "lo": { "n": 1, "updatedExisting": false, "upserted": id.clone() } };
                    if want_new {
                        d.insert("v", nd.clone());
                    }
                    b.run.record(0, d);
                    b.run.stage(&mut w, &b.key);
                }
                self.commit(&mut g, coll, w, &indexes, new_coll, tx).await?;
                if let Some(b) = rr.as_mut() {
                    b.run.apply();
                }
                let value = if want_new { shape(Some(nd))? } else { Bson::Null };
                Ok(doc! { "lastErrorObject": { "n": 1, "updatedExisting": false, "upserted": id }, "value": value, "ok": 1.0 })
            }
            None => Ok(doc! { "lastErrorObject": { "n": 0, "updatedExisting": false }, "value": Bson::Null, "ok": 1.0 }),
        }
    }

    pub(crate) async fn delete(&self, db: &str, cmd: &Document, tx: &mut Option<Txn>) -> R<Document> {
        let coll = collection_name(cmd, "delete")?;
        let specs = cmd.get_array("deletes").map_err(|_| MongoError::new(40414, "IDLFailedToParse", "BSON field 'delete.deletes' is missing but a required field".into()))?;
        let cols = specs.iter().map(|s| match s.as_document() { Some(d) => crate::collation::of_statement("delete", d), None => Ok(Ok(None)) }).collect::<R<Vec<_>>>()?;
        let mut rr = self.retry_begin(db, cmd, specs.len(), "delete").await?;
        let mut g = self.dbs.lock(db).await;
        attach(&mut g, tx);
        let indexes = self.load_indexes(&mut g, db, coll).await?;
        self.presweep(&mut g, coll, &indexes).await?;
        let ttl_specs = index::ttl_specs(&indexes);
        let mut w = take_writes(tx);
        let mut n = 0;
        let mut errors: Vec<Bson> = Vec::new();
        let ordered = bool_arg(cmd.get("ordered"), true) || tx.is_some();
        for (i, spec) in specs.iter().enumerate() {
            if let Some(b) = rr.as_mut() {
                if b.run.is_retried(i) {
                    b.run.stored(i);
                    n += 1;
                    continue;
                }
                if spec.as_document().and_then(|s| s.get("limit")).map(|l| matches!(l, Bson::Int32(0) | Bson::Int64(0)) || matches!(l, Bson::Double(f) if *f == 0.0)).unwrap_or(false) {
                    errors.push(Bson::Document(write_error(i, 72, "Cannot use (or request) retryable writes with limit=0".into())));
                    if ordered {
                        break;
                    }
                    continue;
                }
            }
            let n_before = n;
            let res: R<()> = match &cols[i] { Err(e) => Err(e.clone()), Ok(None) => delete_stmt(&mut g, db, coll, &indexes, &ttl_specs, &mut w, spec, &mut n).await, Ok(Some(c)) => crate::collation::scope(Some(c.clone()), delete_stmt(&mut g, db, coll, &indexes, &ttl_specs, &mut w, spec, &mut n)).await };
            if res.is_ok() && n > n_before {
                if let Some(b) = rr.as_mut() {
                    b.run.record(i, doc! { "t": "d" });
                }
            }
            if let Err(e) = res {
                if e.code == 112 {
                    return Err(e);
                }
                errors.push(Bson::Document(write_error(i, e.code, e.msg)));
                if ordered {
                    break;
                }
            }
        }
        if let Some(b) = &rr {
            b.run.stage(&mut w, &b.key);
        }
        self.commit(&mut g, coll, w, &indexes, None, tx).await?;
        if let Some(b) = rr.as_mut() {
            b.run.apply();
        }
        let mut reply = doc! { "n": n };
        if !errors.is_empty() {
            reply.insert("writeErrors", errors);
        }
        if let Some(b) = &rr {
            if !b.run.retried.is_empty() {
                reply.insert("retriedStmtIds", b.run.retried_ids());
            }
        }
        reply.insert("ok", 1.0);
        Ok(reply)
    }
}


#[derive(Default)]
struct UpdAcc {
    n: i32,
    n_modified: i32,
    upserted: Vec<Bson>,
}

#[allow(clippy::too_many_arguments)]
async fn update_stmt(g: &mut Entry, coll: &str, ns: &str, indexes: &mut Vec<IndexDef>, ttl_specs: &[index::TtlSpec], w: &mut Writes, spec: &Bson, i: usize, acc: &mut UpdAcc) -> R<()> {
                let series = g.coll_opts(coll).await?.and_then(|k| k.series.clone());
                let spec = spec.as_document().ok_or_else(|| MongoError::new(14, "TypeMismatch", "update statement must be an object".into()))?;
                let q = spec.get_document("q").map_err(|_| MongoError::new(40414, "IDLFailedToParse", "BSON field 'updates.q' is missing but a required field".into()))?;
                let u = match spec.get("u") {
                    Some(u @ (Bson::Document(_) | Bson::Array(_))) => Upd::new(u, q, spec.get("arrayFilters"), spec.get("c"))?,
                    _ => return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field 'updates.u' is missing but a required field".into())),
                };
                let multi = bool_arg(spec.get("multi"), false);
                let upsert = bool_arg(spec.get("upsert"), false);
                let hint = resolve_hint(indexes, spec.get("hint"))?;
                let (mut matched, _, _) = find_docs(g, coll, indexes, &FindOpts { filter: q, sort: &[], want: if multi { None } else { Some(1) }, hint, ttl: ttl_ctx(ttl_specs), ov: Some(&w.ov) }).await?;
        for d in matched.iter_mut() { crate::text::strip_scores(d); }
                if !multi {
                    matched.truncate(1);
                }
                if matched.is_empty() {
                    if !upsert {
                        return Ok(());
                    }
                    let nd = u.upsert()?;
                    let id = nd.get("_id").cloned().unwrap();
                    if w.exists(g, coll, &id).await? {
                        return Err(MongoError::duplicate_key_c(ns, "_id_", &doc! { "_id": 1 }, &doc! { "_id": id }, g.collation_of(coll).as_deref()));
                    }
                    w.put(g, coll, ns, indexes, &nd, None).await?;
                    acc.upserted.push(Bson::Document(doc! { "index": i as i32, "_id": id }));
                    acc.n += 1;
                    return Ok(());
                }
                for old in matched {
                    let mut new = u.apply(&old)?;
                    if let Some(t) = &series {
                        new = crate::special_colls::series_order(new, t);
                    }
                    acc.n += 1;
                    if !u.take_forced() && new.to_vec()? == old.to_vec()? {
                        continue;
                    }
                    w.pending_update = u.operators().cloned();
                    w.put(g, coll, ns, indexes, &new, Some(&old)).await?;
                    acc.n_modified += 1;
                }
                Ok(())
}


async fn delete_stmt(g: &mut Entry, db: &str, coll: &str, indexes: &[IndexDef], ttl_specs: &[index::TtlSpec], w: &mut Writes, spec: &Bson, n: &mut i32) -> R<()> {
                let spec = spec.as_document().ok_or_else(|| MongoError::new(14, "TypeMismatch", "delete statement must be an object".into()))?;
                let q = spec.get_document("q").map_err(|_| MongoError::new(40414, "IDLFailedToParse", "BSON field 'deletes.q' is missing but a required field".into()))?;
                let limit = int_arg(spec.get("limit")).unwrap_or(0);
                let hint = resolve_hint(indexes, spec.get("hint"))?;
                let ev_start = w.events.len();
                let (mut found, _, _) = find_docs(g, coll, indexes, &FindOpts { filter: q, sort: &[], want: if limit == 0 { None } else { Some(limit as usize) }, hint, ttl: ttl_ctx(ttl_specs), ov: Some(&w.ov) }).await?;
        for d in found.iter_mut() { crate::text::strip_scores(d); }
                if limit > 0 {
                    found.truncate(limit as usize);
                }
                for d in found {
                    w.delete(g, db, coll, indexes, &d).await?;
                    *n += 1;
                }
                // MongoDB's oplog (and so a change stream) lists the documents of one delete statement last-first.
                w.events[ev_start..].reverse();
                Ok(())
}

/// TTL filter argument for [`FindOpts`]: `None` when the collection has no TTL index.
pub(crate) fn ttl_ctx(specs: &[index::TtlSpec]) -> Option<(&[index::TtlSpec], i64)> {
    if specs.is_empty() {
        None
    } else {
        Some((specs, now_ms()))
    }
}

#[allow(dead_code)]
fn _unused(_: &Hint) {
    let _ = (doc_prefix, encode_sortable);
}

/// The parsed options of a `$geoNear` stage that apply after the search.
pub(crate) struct GeoNearStage {
    distance_field: String,
    include_locs: Option<String>,
    multiplier: f64,
    key: Option<String>,
    spherical: bool,
}

/// Validate a `$geoNear` stage and turn it into the equivalent `$near` / `$nearSphere` find filter.
pub(crate) fn geo_near_filter(spec: &Bson) -> R<(Document, GeoNearStage)> {
    let Bson::Document(s) = spec else {
        return Err(MongoError::new(10065, "Location10065", "invalid parameter: expected an object ($geoNear)".to_string()));
    };
    let num = |b: &Bson| match b {
        Bson::Double(x) => Some(*x),
        Bson::Int32(x) => Some(*x as f64),
        Bson::Int64(x) => Some(*x as f64),
        _ => None,
    };
    for k in ["limit", "num"] {
        if s.contains_key(k) {
            let c = if k == "limit" { 50858 } else { 50857 };
            return Err(MongoError::new(c, if c == 50858 { "Location50858" } else { "Location50857" }, format!("$geoNear no longer supports the '{k}' parameter. Use a $limit stage instead.")));
        }
    }
    let near = s.get("near").ok_or_else(|| MongoError::new(5860400, "Location5860400", "$geoNear requires a 'near' argument".to_string()))?;
    let distance_field = match s.get("distanceField") {
        Some(Bson::String(f)) => f.clone(),
        _ => return Err(MongoError::new(16606, "Location16606", "$geoNear requires a 'distanceField' option as a String".to_string())),
    };
    let near_geojson = match near {
        Bson::Document(_) => true,
        Bson::Array(_) => false,
        _ => return Err(MongoError::new(5860401, "Location5860401", "$geoNear requires near argument to be a GeoJSON object or a legacy point(array)".to_string())),
    };
    let spherical = matches!(s.get("spherical"), Some(Bson::Boolean(true)));
    let mut cond = Document::new();
    let mut inner = Document::new();
    for (k, op) in [("maxDistance", "$maxDistance"), ("minDistance", "$minDistance")] {
        if let Some(v) = s.get(k) {
            let n = num(v).ok_or_else(|| MongoError::bad_value(format!("{k} must be a number")))?;
            if n < 0.0 {
                return Err(MongoError::bad_value(format!("{k} must be nonnegative")));
            }
            inner.insert(op, v.clone());
        }
    }
    let name = if spherical || near_geojson { "$nearSphere" } else { "$near" };
    let arg = if near_geojson {
        let mut a = doc! { "$geometry": near.clone() };
        a.extend(inner);
        Bson::Document(a)
    } else {
        cond.extend(inner);
        near.clone()
    };
    let key = match s.get("key") {
        Some(Bson::String(k)) => Some(k.clone()),
        Some(_) => return Err(MongoError::new(14, "TypeMismatch", "$geoNear requires that 'key' option as a String".to_string())),
        None => None,
    };
    let multiplier = s.get("distanceMultiplier").and_then(num).unwrap_or(1.0);
    let include_locs = match s.get("includeLocs") {
        Some(Bson::String(l)) => Some(l.clone()),
        Some(_) => return Err(MongoError::new(16607, "Location16607", "$geoNear requires that 'includeLocs' option is a String".to_string())),
        None => None,
    };
    let query = match s.get("query") {
        None => Document::new(),
        Some(Bson::Document(q)) => q.clone(),
        Some(_) => return Err(MongoError::new(14, "TypeMismatch", "query must be an object".to_string())),
    };
    if crate::geo_query::contains_near(&query) {
        return Err(MongoError::bad_value("Too many geoNear expressions".to_string()));
    }
    cond.insert(name, arg);
    // the field is filled in by the caller once the index is known
    let mut f = Document::new();
    f.insert("\0field", cond);
    if !query.is_empty() {
        f.insert("\0query", query);
    }
    Ok((f, GeoNearStage { distance_field, include_locs, multiplier, key, spherical }))
}

impl Server {
    /// The documents of a `$geoNear` stage, nearest first, carrying the hidden distance / point fields.
    async fn geo_near_docs(&self, g: &mut impl crate::coll::Rd, db: &str, coll: &str, f: &Document, hint: Option<&Bson>, ov: Option<&crate::coll::Overlay>, st: &GeoNearStage) -> R<Vec<Document>> {
        let indexes = self.load_indexes(g, db, coll).await?;
        let geo: Vec<&crate::index::IndexDef> = indexes.iter().filter(|i| matches!(i.kind, crate::index::Kind::Geo(_)) && i.state == crate::index::State::Ready).collect();
        let field = match &st.key {
            Some(k) => k.clone(),
            None => match geo.as_slice() {
                [only] => match &only.kind {
                    crate::index::Kind::Geo(gs) => gs.field.clone(),
                    _ => unreachable!(),
                },
                [] => {
                    let any = self.scan_visible(g, db, coll).await?;
                    return Err(if any.is_empty() {
                        MongoError::new(26, "NamespaceNotFound", format!("$geoNear requires a geo index to run, but {db}.{coll} does not exist"))
                    } else {
                        MongoError::new(27, "IndexNotFound", "$geoNear requires a 2d or 2dsphere index, but none were found".to_string())
                    });
                }
                many => {
                    // several geo indexes: the kind the options ask for decides; two of the same kind are ambiguous
                    let sphere = st.spherical;
                    let same: Vec<&&crate::index::IndexDef> = many.iter().filter(|i| matches!(&i.kind, crate::index::Kind::Geo(gs) if gs.sphere == sphere)).collect();
                    match same.as_slice() {
                        [one] => match &one.kind {
                            crate::index::Kind::Geo(gs) => gs.field.clone(),
                            _ => unreachable!(),
                        },
                        _ => return Err(MongoError::new(27, "IndexNotFound", "There is more than one 2dsphere index or more than one 2d index, specify a key".to_string())),
                    }
                }
            },
        };
        let mut filter = Document::new();
        filter.insert(field, f.get("\0field").cloned().unwrap_or(Bson::Null));
        if let Some(q) = f.get_document("\0query").ok() {
            filter = doc! { "$and": [Bson::Document(q.clone()), Bson::Document(filter)] };
        }
        self.matching(g, db, coll, &filter, hint, ov).await
    }
}

/// Does a `$match` stage anywhere in the pipeline (including `$lookup` sub-pipelines) use `$near` / `$nearSphere`?
fn pipeline_match_has_near(stages: &[Bson]) -> bool {
    fn walk(b: &Bson) -> bool {
        match b {
            Bson::Document(d) => d.iter().any(|(k, v)| (k == "$match" && matches!(v, Bson::Document(m) if crate::geo_query::contains_near(m))) || walk(v)),
            Bson::Array(a) => a.iter().any(walk),
            _ => false,
        }
    }
    stages.iter().any(walk)
}

/// Replace the hidden distance / location of nearest-first documents by the stage's output fields.
pub(crate) fn apply_geo_near_fields(docs: &mut [Document], st: &GeoNearStage) -> R<()> {
    for d in docs.iter_mut() {
        let dist = d.remove(crate::geo::DIST_KEY).and_then(|b| match b { Bson::Double(x) => Some(x), _ => None }).unwrap_or(0.0);
        let loc = d.remove(crate::geo::POINT_KEY).unwrap_or(Bson::Null);
        let parts: Vec<&str> = st.distance_field.split('.').collect();
        crate::update::set_path(d, &parts, Bson::Double(dist * st.multiplier))?;
        if let Some(l) = &st.include_locs {
            let parts: Vec<&str> = l.split('.').collect();
            crate::update::set_path(d, &parts, loc)?;
        }
    }
    Ok(())
}

/// `$geoNear` over already-read documents (a `$lookup` sub-pipeline); `geo` lists the collection's `(field, spherical)` geo indexes.
pub(crate) fn geo_near_pure(docs: Vec<Document>, spec: &Bson, geo: &[(String, bool)]) -> R<Vec<Document>> {
    let (f, st) = geo_near_filter(spec)?;
    let no_plan = || MongoError::new(291, "NoQueryExecutionPlans", "error processing query: planner returned error :: caused by :: unable to find index for $geoNear query".to_string());
    let field = match &st.key {
        Some(k) => k.clone(),
        None => match geo {
            [] => return Err(MongoError::new(27, "IndexNotFound", "$geoNear requires a 2d or 2dsphere index, but none were found".to_string())),
            [only] => only.0.clone(),
            many => {
                let same: Vec<&(String, bool)> = many.iter().filter(|g| g.1 == st.spherical).collect();
                match same.as_slice() {
                    [one] => one.0.clone(),
                    _ => return Err(MongoError::new(27, "IndexNotFound", "There is more than one 2dsphere index or more than one 2d index, specify a key".to_string())),
                }
            }
        },
    };
    let mut filter = Document::new();
    filter.insert(field.clone(), f.get("\0field").cloned().unwrap_or(Bson::Null));
    if let Ok(q) = f.get_document("\0query") {
        filter = doc! { "$and": [Bson::Document(q.clone()), Bson::Document(filter)] };
    }
    crate::geo_query::validate_filter(&filter, true)?;
    let Some((path, n)) = crate::geo_query::find_near(&filter)? else { return Err(no_plan()) };
    let Some(g) = geo.iter().find(|g| g.0 == path) else { return Err(no_plan()) };
    if !(if n.from_geometry { g.1 } else { n.spherical || !g.1 }) {
        return Err(no_plan());
    }
    let mut hits = Vec::new();
    for d in docs {
        if query::matches(&d, &filter)? {
            hits.push(d);
        }
    }
    let mut out = crate::coll::order_by_near(hits, &path, &n, None);
    apply_geo_near_fields(&mut out, &st)?;
    Ok(out)
}

impl Server {
    /// `aggregate: 1`: a database-level pipeline, which has to start with `$documents`.
    fn aggregate_database(&self, db: &str, cmd: &Document, pipeline: &[Bson]) -> R<Document> {
        let cursor = cmd.get_document("cursor").map_err(|_| MongoError::failed_to_parse("The 'cursor' option is required, except for aggregate with the explain argument".into()))?;
        let batch = int_arg(cursor.get("batchSize")).map(|b| b.max(0) as usize);
        let first = pipeline.first().and_then(|s| s.as_document());
        let first_name = first.and_then(|d| d.keys().next().cloned());
        if let Some(name) = first_name.as_deref() {
            if let Some(source) = self.db_level_source(db, name, first.and_then(|d| d.get(name)).unwrap_or(&Bson::Null)) {
                let docs = source?;
                for s in pipeline.iter().skip(1) {
                    if let Some(n) = s.as_document().and_then(|d| d.keys().next()) {
                        if matches!(n.as_str(), "$currentOp" | "$listLocalSessions" | "$listSessions") {
                            return Err(MongoError::new(40602, "Location40602", format!("{n} is only valid as the first stage in a pipeline")));
                        }
                    }
                }
                let out = run_pipeline(docs, &pipeline[1..], &crate::agg::Foreign::new())?;
                return Ok(self.cursor_reply_open(format!("{db}.$cmd.aggregate"), out.into(), batch, false, false));
            }
        }
        let Some(spec) = first.and_then(|d| d.get("$documents")) else {
            return Err(crate::diag::db_level_error(first_name.as_deref()));
        };
        for (i, s) in pipeline.iter().enumerate().skip(1) {
            if s.as_document().map(|d| d.contains_key("$documents")).unwrap_or(false) {
                return Err(MongoError::new(40602, "Location40602", format!("$documents is only valid as the first stage in a pipeline.{i}")));
            }
        }
        let docs = match crate::expr::eval(spec, &crate::expr::Scope::new(&Document::new()))? {
            Some(Bson::Array(a)) => a.into_iter().map(|x| match x {
                Bson::Document(d) => Ok(d),
                other => Err(MongoError::new(40228, "Location40228", format!("$documents elements must be objects, found: {}", crate::update::type_name(&other)))),
            }).collect::<R<Vec<Document>>>()?,
            _ => return Err(MongoError::new(5858203, "Location5858203", "an array expression is expected".to_string())),
        };
        let out = run_pipeline(docs, &pipeline[1..], &crate::agg::Foreign::new())?;
        Ok(self.cursor_reply_open(format!("{db}.$cmd.aggregate"), out.into(), batch, false, false))
    }
}

/// The distinct values of `key` in `docs` (an array contributes its elements), in the order of the BSON comparison.
pub(crate) fn distinct_values(docs: &[Document], key: &str) -> Vec<Bson> {
    let mut values: Vec<Bson> = Vec::new();
    for d in docs {
        for v in path_values(d, key) {
            let items: Vec<&Bson> = match v {
                Bson::Array(a) => a.iter().collect(),
                other => vec![other],
            };
            for it in items {
                if !values.iter().any(|x| query::equal(x, it)) {
                    values.push(it.clone());
                }
            }
        }
    }
    values.sort_by(query::compare);
    values
}
