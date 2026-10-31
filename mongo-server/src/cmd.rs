//! Command dispatch: handshake commands, CRUD, cursors, collection and index management.

use crate::agg::run_pipeline;
use crate::dbs::{valid_db_name, Dbs, Entry};
use crate::error::MongoError;
use crate::layout::{doc_prefix, encode_sortable, index_key};
use crate::query::{self, id_equality, matches, parse_projection, parse_sort, path_values, project, sort_docs};
use crate::update::{apply_update, fmt_doc, upsert_document};
use bson::{doc, oid::ObjectId, Bson, DateTime, Document};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use tenant_bench::lifecycle::Op;

pub(crate) type R<T> = Result<T, MongoError>;

const MAX_WIRE_VERSION: i32 = 21;
const DEFAULT_FIRST_BATCH: usize = 101;

pub struct Server {
    pub dbs: Arc<Dbs>,
    pub(crate) builds: Arc<Mutex<HashSet<String>>>,
    pub(crate) sessions: crate::txn::Sessions,
    pub(crate) next_txn_id: std::sync::atomic::AtomicU64,
    /// Replica-set name and advertised address reported by `hello` (drivers allow transactions only on replica sets).
    pub(crate) rs: Mutex<Option<(String, String)>>,
    /// Print every command name and database (used to discover what drivers send on connect).
    pub log_commands: bool,
    cursors: Mutex<HashMap<i64, Cursor>>,
    pub(crate) change_cursors: Mutex<HashMap<i64, crate::cmd_changes::ChangeCursor>>,
    next_conn: AtomicI64,
    pub(crate) auth: crate::auth::AuthState,
    pub(crate) retry: crate::retry::Registry,
    pub(crate) diag: crate::diag::Diag,
}

/// Idle time after which a cursor is closed (`--cursor-timeout-secs`, mongod's `cursorTimeoutMillis`; 0 = never).
pub static CURSOR_TIMEOUT_SECS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(600);
/// Cursors closed because they were idle (serverStatus `metrics.cursor.timedOut`).
pub static CURSORS_TIMED_OUT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Idle time after which a `noCursorTimeout` cursor is closed anyway: mongod ties such a cursor to its logical session, which
/// expires after 30 minutes without use.
const NO_TIMEOUT_CURSOR_SECS: u64 = 30 * 60;

struct Cursor {
    /// Last time the cursor was created or used by a getMore.
    used: std::time::Instant,
    /// Opened with `noCursorTimeout`: closed after `NO_TIMEOUT_CURSOR_SECS` idle instead of `CURSOR_TIMEOUT_SECS`.
    no_timeout: bool,
    ns: String,
    docs: VecDeque<Document>,
    /// A tailable cursor: where it stands in its capped collection.
    tail: Option<crate::capped::Tail>,
}

pub(crate) fn ok() -> Document {
    doc! { "ok": 1.0 }
}

pub(crate) fn int_arg(v: Option<&Bson>) -> Option<i64> {
    match v? {
        Bson::Int32(i) => Some(*i as i64),
        Bson::Int64(i) => Some(*i),
        Bson::Double(f) if f.fract() == 0.0 => Some(*f as i64),
        _ => None,
    }
}

pub(crate) fn bool_arg(v: Option<&Bson>, default: bool) -> bool {
    match v {
        Some(Bson::Boolean(b)) => *b,
        Some(Bson::Int32(i)) => *i != 0,
        Some(Bson::Int64(i)) => *i != 0,
        Some(Bson::Double(f)) => *f != 0.0,
        _ => default,
    }
}

pub(crate) fn collection_name<'a>(cmd: &'a Document, key: &str) -> R<&'a str> {
    match cmd.get(key) {
        Some(Bson::String(s)) if !s.is_empty() && !s.contains('\0') && !s.contains('$') => Ok(s),
        Some(Bson::String(s)) => Err(MongoError::new(73, "InvalidNamespace", format!("Invalid collection name specified '{s}'"))),
        _ => Err(MongoError::new(2, "BadValue", format!("collection name has invalid type {}", cmd.get(key).map(crate::update::type_name).unwrap_or("missing")))),
    }
}

/// Random UUID-shaped string for messages that embed a build/collection id.
pub(crate) fn fake_uuid() -> String {
    let h: u128 = rand::random();
    let s = format!("{h:032x}");
    format!("{}-{}-{}-{}-{}", &s[..8], &s[8..12], &s[12..16], &s[16..20], &s[20..])
}

pub(crate) fn write_error(index: usize, code: i32, msg: String) -> Document {
    doc! { "index": index as i32, "code": code, "errmsg": msg }
}


/// Commands that write (they get a new cluster time when they published no change event).
fn is_write_command(name: &str) -> bool {
    matches!(
        name,
        "insert" | "update" | "delete" | "findAndModify" | "findandmodify" | "create" | "drop" | "dropDatabase" | "createIndexes" | "dropIndexes" | "collMod" | "renameCollection" | "convertToCapped" | "cloneCollectionAsCapped" | "mapReduce" | "mapreduce" | "createUser"
            | "updateUser" | "dropUser" | "createRole" | "updateRole" | "dropRole" | "commitTransaction"
    )
}

impl Server {
    pub fn new(dbs: Arc<Dbs>, log_commands: bool) -> Arc<Self> {
        let s = Arc::new(Self { dbs, builds: Arc::new(Mutex::new(HashSet::new())), sessions: Default::default(), next_txn_id: std::sync::atomic::AtomicU64::new(1), rs: Mutex::new(None), log_commands, cursors: Mutex::new(HashMap::new()), change_cursors: Mutex::new(HashMap::new()), next_conn: AtomicI64::new(1), auth: Default::default(), retry: Default::default(), diag: Default::default() });
        // Close idle cursors (see `CURSOR_TIMEOUT_SECS`): a cursor keeps the rest of its results in memory, and a client that
        // stops reading (or dies) without killCursors used to leave them there until the process stopped.
        if let Ok(h) = tokio::runtime::Handle::try_current() {
            let w = Arc::downgrade(&s);
            h.spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                    let Some(s) = w.upgrade() else { break };
                    s.expire_idle_cursors();
                }
            });
        }
        s
    }

    /// The cursor of a `find` reply was opened with `noCursorTimeout`.
    pub(crate) fn mark_no_cursor_timeout(&self, reply: &Document) {
        if let Ok(id) = reply.get_document("cursor").and_then(|c| c.get_i64("id")) {
            if let Some(c) = self.cursors.lock().unwrap().get_mut(&id) {
                c.no_timeout = true;
            }
        }
    }

    /// Close the cursors that no getMore used for `CURSOR_TIMEOUT_SECS`. Returns how many were closed.
    pub(crate) fn expire_idle_cursors(&self) -> usize {
        let secs = CURSOR_TIMEOUT_SECS.load(std::sync::atomic::Ordering::Relaxed);
        if secs == 0 {
            return 0;
        }
        let max = std::time::Duration::from_secs(secs);
        let max_no_timeout = std::time::Duration::from_secs(secs.max(NO_TIMEOUT_CURSOR_SECS));
        let mut n = 0;
        self.cursors.lock().unwrap().retain(|_, c| {
            let keep = c.used.elapsed() < if c.no_timeout { max_no_timeout } else { max };
            n += usize::from(!keep);
            keep
        });
        self.change_cursors.lock().unwrap().retain(|_, c| {
            let keep = c.used.elapsed() < max;
            n += usize::from(!keep);
            keep
        });
        CURSORS_TIMED_OUT.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        n
    }

    pub fn new_connection_id(&self) -> i64 {
        self.next_conn.fetch_add(1, Relaxed)
    }

    pub async fn handle(self: &Arc<Self>, db: &str, cmd: Document, conn_id: i64) -> Document {
        let Some(name) = cmd.keys().next().cloned() else {
            return MongoError::new(59, "CommandNotFound", "Empty command document".into()).to_reply();
        };
        if self.log_commands {
            eprintln!("CMD {name} db={db}");
        }
        // fair queue between databases under saturation, safety limits, per-database metrics (fair.rs)
        // Only commands that pass authentication and authorization take a place (and touch per-database counters); the
        // others go straight to `run_command`, which answers with the same error.
        let _permit = if crate::fair::exempt(&name) || self.authorize(conn_id, db, &name, &cmd).await.is_err() {
            None
        } else {
            match crate::fair::admit(db, !self.dbs.is_open_hint(db)).await {
                Ok(p) => Some(p),
                Err(reply) => return reply,
            }
        };
        let t0 = std::time::Instant::now();
        let (opid, kill) = self.diag.begin(conn_id, db, &name, &cmd);
        let killable = crate::diag::killable(&name);
        let is_rs = self.rs.lock().unwrap().is_some();
        if is_rs {
            if let Some(Bson::Timestamp(ts)) = cmd.get_document("$clusterTime").ok().and_then(|c| c.get("clusterTime")) {
                self.dbs.changes.advance(*ts);
            }
        }
        let (r, op_time) = crate::fair::timed(db, crate::changes::OP_TIME.scope(std::cell::Cell::new(None), async { let r = self.run_command(db, &name, &cmd, conn_id, opid, kill, killable).await; (r, crate::changes::OP_TIME.with(|c| c.get())) })).await;
        let r: R<Document> = r;
        let micros = t0.elapsed().as_micros() as i64;
        self.diag.end(opid, db, &name, &cmd, &r, micros);
        self.profile_record(db, &name, &cmd, &r, micros, conn_id).await;
        let ms = t0.elapsed().as_millis();
        if ms >= 500 {
            eprintln!("SLOW {name} db={db} ms={ms} ok={}", r.is_ok());
        }
        let mut reply = match r {
            Ok(d) => d,
            Err(e) => e.to_reply(),
        };
        if is_rs {
            let log = &self.dbs.changes;
            let op = match op_time {
                Some((t, i)) => bson::Timestamp { time: t, increment: i },
                None if is_write_command(&name) => log.tick(),
                None => log.cluster_now(),
            };
            let now = log.cluster_now();
            let now = if (now.time, now.increment) < (op.time, op.increment) { op } else { now };
            reply.insert("$clusterTime", doc! { "clusterTime": Bson::Timestamp(now), "signature": { "hash": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![0; 20] }), "keyId": 0_i64 } });
            reply.insert("operationTime", Bson::Timestamp(op));
        }
        reply
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_command(self: &Arc<Self>, db: &str, name: &str, cmd: &Document, conn_id: i64, opid: i64, kill: Arc<tokio::sync::Notify>, killable: bool) -> R<Document> {
        let name = name.to_string();
        let r = if crate::diag::needs_js(&name, &cmd) {
            // a command that runs JavaScript blocks its thread: all of them run on one thread of their own (the engines and their
            // per-database scopes are per thread), the connections that run no JavaScript go on
            let (me, db2, name2, cmd2) = (Arc::clone(self), db.to_string(), name.clone(), cmd.clone());
            crate::diag::run_js(async move { crate::diag::as_op(opid, me.diag.guarded(kill, killable, me.dispatch_durable(&db2, &name2, &cmd2, conn_id))).await }).await
        } else {
            crate::diag::as_op(opid, self.diag.guarded(kill, killable, self.dispatch_durable(db, &name, &cmd, conn_id))).await
        };
        r
    }

    /// Runs the command inside the JavaScript scope and the group-commit scopes (engine-cost): writes staged by the command become
    /// durable after its locks were released, and the reply waits for that. The scopes are task-locals, so they are opened here,
    /// in the task that runs the command (JavaScript commands run on a thread of their own, see `run_command`).
    async fn dispatch_durable(&self, db: &str, name: &str, cmd: &Document, conn_id: i64) -> R<Document> {
        crate::dbs::TOKENS
            .scope(Default::default(), async {
                let (mut r, commits, relax) = crate::dbs::RELAX
                    .scope(std::cell::Cell::new(false), crate::dbs::PENDING.scope(Default::default(), async {
                        let r = crate::js::scoped(db, self.dispatch(db, name, cmd, conn_id)).await;
                        (r, crate::dbs::PENDING.with(|p| std::mem::take(&mut *p.borrow_mut())), crate::dbs::RELAX.with(|c| c.get()))
                    }))
                    .await;
                // Later commits of a database flush after its earlier ones, so waiting for the last one of each database
                // covers all of them (a command can write to more than one database: $out, renameCollection).
                let mut last: Vec<tenant_bench::lifecycle::Commit> = Vec::new();
                for c in commits.into_iter().rev() {
                    if !last.iter().any(|l| l.tenant() == c.tenant()) {
                        last.push(c);
                    }
                }
                for c in last {
                    if relax && crate::ctl::durability::relaxed_ms(c.tenant()) > 0 && crate::locallog::get().is_none() {
                        c.relax();
                    } else if let Err(e) = c.wait().await {
                        r = Err(e.into());
                    }
                }
                r
            })
            .await
    }

    async fn dispatch(&self, db: &str, name: &str, cmd: &Document, conn_id: i64) -> R<Document> {
        self.authorize(conn_id, db, name, cmd).await?;
        crate::stable_api::check(name, cmd)?;
        if matches!(name, "commitTransaction" | "abortTransaction") && db != "admin" {
            return Err(MongoError::new(13, "Unauthorized", format!("{name} may only be run against the admin database.")));
        }
        crate::admin_cmds::check_max_time_ms(name, cmd)?;
        if let Some(r) = self.write_gate(db, name, cmd).await? {
            return Ok(r);
        }
        let wc = self.command_write_concern(name, cmd)?;
        if wc.as_ref().map_or(false, |w| w.relaxed()) && !cmd.contains_key("autocommit") {
            let _ = crate::dbs::RELAX.try_with(|c| c.set(true));
        }
        self.check_read_concern(name, cmd)?;
        let mut r = Box::pin(self.dispatch_inner(db, name, cmd, conn_id)).await?;
        if let Some(wc) = &wc {
            self.finish_write_concern(name, cmd, wc, &mut r)?;
        }
        if self.is_rs() && matches!(cmd.get_document("readConcern").ok().and_then(|rc| rc.get_str("level").ok()), Some("snapshot")) && !cmd.contains_key("autocommit") {
            // a snapshot read says the time it read at
            let at = self.dbs.changes.cluster_now();
            if let Ok(c) = r.get_document_mut("cursor") {
                c.insert("atClusterTime", Bson::Timestamp(at));
            } else if name == "distinct" {
                let ok = r.remove("ok");
                r.insert("atClusterTime", Bson::Timestamp(at));
                if let Some(ok) = ok {
                    r.insert("ok", ok);
                }
            }
        }
        r.remove("\u{0}ncolls");
        Ok(r)
    }

    async fn dispatch_inner(&self, db: &str, name: &str, cmd: &Document, conn_id: i64) -> R<Document> {
        if name == "shutdown" && db != "admin" {
            return Err(MongoError::new(13, "Unauthorized", "shutdown may only be run against the admin database.".into()));
        }
        crate::fields::check(name, cmd)?;
        if let Some(r) = self.diag_command(db, name, cmd).await {
            return r;
        }
        // the `let` option: variables every expression of the command can read
        let vars = match (name, cmd.get("let")) {
            ("find" | "aggregate" | "update" | "delete" | "findAndModify" | "findandmodify", Some(l)) => {
                let label = match name {
                    "find" => "FindCommandRequest",
                    "findAndModify" | "findandmodify" => "findAndModify",
                    other => other,
                };
                crate::expr::parse_let(label, l)?
            }
            _ => Vec::new(),
        };
        if vars.is_empty() {
            self.dispatch_commands(db, name, cmd, conn_id).await
        } else {
            crate::expr::CMD_LET.scope(vars, self.dispatch_commands(db, name, cmd, conn_id)).await
        }
    }

    async fn dispatch_commands(&self, db: &str, name: &str, cmd: &Document, conn_id: i64) -> R<Document> {
        // Commands that do not touch a tenant.
        match name {
            "replSetGetStatus" | "replSetGetConfig" => return self.repl_cmd(name, db),
            "shutdown" => return self.shutdown_cmd(db, cmd),
            "dropConnections" | "rotateCertificates" | "setUserWriteBlockMode" | "setFeatureCompatibilityVersion" => return self.admin_cmd(name, db, cmd).unwrap(),
            "hello" | "isMaster" | "ismaster" => {
                if let Ok(md) = cmd.get_document("client") {
                    self.diag.set_metadata(conn_id, md.clone());
                }
                // an awaitable hello (topologyVersion + maxAwaitTimeMS): nothing changes here, so wait out the time
                if let (Ok(tv), Some(ms)) = (cmd.get_document("topologyVersion"), crate::cmd::int_arg(cmd.get("maxAwaitTimeMS"))) {
                    if tv.get_i64("counter").unwrap_or(-1) == 0 && ms > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(ms.min(10_000) as u64)).await;
                    }
                }
                let mut d = self.hello(name, conn_id);
                if name != "hello" && matches!(cmd.get("helloOk"), Some(Bson::Boolean(true))) {
                    let ok = d.remove("ismaster");
                    let mut n = Document::new();
                    n.insert("helloOk", true);
                    n.insert("ismaster", ok.unwrap_or(Bson::Boolean(true)));
                    n.extend(d);
                    d = n;
                }
                if let Ok(list) = cmd.get_array("compression") {
                    let chosen: Vec<Bson> = list.iter().filter(|c| matches!(c, Bson::String(s) if crate::wire::COMPRESSORS.contains(&s.as_str()))).cloned().collect();
                    if !chosen.is_empty() {
                        let mut n = doc! { "compression": chosen };
                        n.extend(d);
                        d = n;
                    }
                }
                self.auth_hello(cmd, &mut d).await?;
                return Ok(d);
            }
            "saslStart" => return self.sasl_start(conn_id, db, cmd).await,
            "authenticate" => return self.authenticate(conn_id, db, cmd).await,
            "saslContinue" => return self.sasl_continue(conn_id, cmd).await,
            "logout" => return self.logout(conn_id, db),
            "createUser" => return self.create_user(db, cmd).await,
            "updateUser" => return self.update_user(db, cmd).await,
            "dropUser" => return self.drop_user(db, cmd).await,
            "dropAllUsersFromDatabase" => return self.drop_all_users(db).await,
            "grantRolesToUser" => return self.grant_roles_to_user(db, cmd, false).await,
            "revokeRolesFromUser" => return self.grant_roles_to_user(db, cmd, true).await,
            "usersInfo" => return self.users_info(db, cmd).await,
            "createRole" => return self.create_role(db, cmd).await,
            "updateRole" => return self.update_role(db, cmd).await,
            "dropRole" => return self.drop_role(db, cmd).await,
            "dropAllRolesFromDatabase" => return self.drop_all_roles(db).await,
            "grantRolesToRole" | "revokeRolesFromRole" | "grantPrivilegesToRole" | "revokePrivilegesFromRole" => return self.role_delta(db, cmd, name).await,
            "rolesInfo" => return self.roles_info(db, cmd).await,
            "invalidateUserCache" => {
                crate::fields::check("invalidateUserCache", cmd)?;
                return Ok(ok());
            }
            "ping" => return Ok(ok()),
            "buildInfo" | "buildinfo" => return Ok(build_info()),
            "getParameter" => return crate::params::get_parameter(cmd, self.is_rs()),
            "setParameter" => return crate::params::set_parameter(cmd, self.is_rs()),
            "endSessions" => return self.end_sessions(cmd).await,
            "killSessions" | "refreshSessions" => return Ok(ok()),
            "commitTransaction" | "abortTransaction" if db != "admin" => {
                return Err(MongoError::new(13, "Unauthorized", format!("{name} may only be run against the admin database.")));
            }
            "commitTransaction" => return self.commit_txn(cmd).await,
            "abortTransaction" => return self.abort_txn(cmd).await,
            "whatsmyuri" => return Ok(doc! { "you": "127.0.0.1:0", "ok": 1.0 }),
            "connectionStatus" => return self.connection_status(conn_id, cmd).await,
            "listDatabases" => return self.list_databases(db, cmd, conn_id).await,
            "tenantStats" => return Ok(self.tenant_stats()),
            "renameCollection" => return self.rename_collection(db, cmd).await,
            "ttlPass" => {
                let gap = int_arg(cmd.get("minGapSecs")).unwrap_or(0) as u64;
                self.dbs.ttl_pass(std::time::Duration::from_secs(gap), int_arg(cmd.get("maxWakes")).unwrap_or(1000) as usize).await?;
                return Ok(ok());
            }
            _ => {}
        }
        if !valid_db_name(db) {
            return Err(MongoError::new(73, "InvalidNamespace", format!("Invalid database name: '{db}'")));
        }
        crate::fields::check(name, cmd)?;
        self.check_txn_number(name, cmd)?;
        if let Some(tc) = crate::txn::parse(cmd)? {
            return self.run_in_txn(db, name, cmd, &tc).await;
        }
        let col = if matches!(name, "find" | "aggregate" | "count" | "distinct" | "findAndModify" | "findandmodify" | "mapReduce" | "mapreduce") {
            crate::collation::of_command(name, cmd)?
        } else if name == "explain" {
            // the collation of the command that is explained
            match cmd.get_document("explain").ok().and_then(|e| e.iter().next().map(|(k, _)| (k.clone(), e))) {
                Some((inner, e)) if matches!(inner.as_str(), "update" | "delete") => {
                    // the collation of the first statement
                    let stmts = e.get_array("updates").or_else(|_| e.get_array("deletes")).ok();
                    match stmts.and_then(|a| a.first()).and_then(|s| s.as_document()) {
                        Some(st) => crate::collation::of_statement(&inner, st)?.ok().flatten(),
                        None => None,
                    }
                }
                Some((inner, e)) => crate::collation::of_command(&inner, e)?,
                None => None,
            }
        } else {
            None
        };
        crate::collation::scope(col, self.route(db, name, cmd)).await
    }

    async fn route(&self, db: &str, name: &str, cmd: &Document) -> R<Document> {
        let mut no_tx: Option<crate::txn::Txn> = None;
        let tx = &mut no_tx;
        if let Some(d) = self.view_route(db, name, cmd, tx).await? {
            return Ok(d);
        }
        match name {
            "compactStructuredEncryptionData" => self.compact_structured_encryption_data(db, cmd).await,
            "createSearchIndexes" => self.create_search_indexes(db, cmd).await,
            "dropSearchIndex" => self.drop_search_index(db, cmd).await,
            "updateSearchIndex" => self.update_search_index(db, cmd).await,
            "insert" => self.insert(db, cmd, tx).await,
            "find" => self.find(db, cmd, tx).await,
            "getMore" => self.get_more(db, cmd).await,
            "killCursors" => self.kill_cursors(cmd),
            "update" => self.update(db, cmd, tx).await,
            "delete" => self.delete(db, cmd, tx).await,
            "findAndModify" | "findandmodify" => self.find_and_modify(db, cmd, tx).await,
            "count" => self.count(db, cmd, tx).await,
            "distinct" => self.distinct(db, cmd, tx).await,
            "aggregate" => self.aggregate(db, cmd, tx).await,
            "mapReduce" | "mapreduce" => self.map_reduce(db, cmd, tx).await,
            "drop" => self.drop_collection(db, cmd).await,
            "create" => self.create(db, cmd).await,
            "dropIndexes" => self.drop_indexes(db, cmd).await,
            "collMod" => self.coll_mod(db, cmd).await,
            "convertToCapped" => self.convert_to_capped(db, cmd).await,
            "cloneCollectionAsCapped" => self.clone_as_capped(db, cmd).await,
            "explain" => self.explain(db, cmd).await,
            "validate" => self.validate(db, cmd).await,
            "collStats" => self.coll_stats(db, cmd).await,
            "dbStats" => self.db_stats(db, cmd).await,
            "createIndexes" => self.create_indexes(db, cmd).await,
            "listIndexes" => self.list_indexes(db, cmd).await,
            "listCollections" => self.list_collections(db, cmd).await,
            "dropDatabase" => {
                let colls = self.dbs.lock(db).await.collections().await?;
                let n_colls = colls.len();
                self.dbs.lock(db).await.drop_database().await?;
                let mut evs: Vec<crate::changes::EvSpec> = colls.into_iter().map(|c| crate::changes::EvSpec { db: db.to_string(), coll: Some(c), op: "drop", doc_key: None, full: None, desc: None, txn: None, ..Default::default() }).collect();
                evs.push(crate::changes::EvSpec { db: db.to_string(), coll: None, op: "dropDatabase", doc_key: None, full: None, desc: None, txn: None, ..Default::default() });
                self.dbs.changes.publish(evs);
                Ok(doc! { "dropped": db, "\u{0}ncolls": n_colls as i32, "ok": 1.0 })
            }
            "tenantAdmin" => self.tenant_admin(db, cmd).await,
            "tenantTrace" => Box::pin(self.tenant_trace(db, cmd)).await,
            other => {
                eprintln!("UNKNOWN COMMAND {other:?} db={db}");
                Err(MongoError::command_not_found(other))
            }
        }
    }

    pub(crate) fn cursors_open(&self) -> usize {
        self.cursors.lock().unwrap().len() + self.change_cursors.lock().unwrap().len()
    }

    pub(crate) fn hello_doc(&self) -> Document {
        self.hello("hello", 0)
    }

    fn hello(&self, name: &str, conn_id: i64) -> Document {
        static PROCESS_ID: std::sync::OnceLock<ObjectId> = std::sync::OnceLock::new();
        let pid = *PROCESS_ID.get_or_init(ObjectId::new);
        let mut d = Document::new();
        let rs = self.rs.lock().unwrap().clone();
        let primary_flag = if name == "hello" { "isWritablePrimary" } else { "ismaster" };
        let topology = doc! { "processId": pid, "counter": 0_i64 };
        match &rs {
            Some((set, addr)) => {
                d.insert("topologyVersion", topology);
                d.insert("hosts", vec![Bson::String(addr.clone())]);
                d.insert("setName", set.clone());
                d.insert("setVersion", 1_i32);
                d.insert(primary_flag, true);
                d.insert("secondary", false);
                d.insert("primary", addr.clone());
                d.insert("me", addr.clone());
                d.insert("electionId", ObjectId::from_bytes([0x7f, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 1]));
                let ts = self.dbs.changes.cluster_now();
                let now = DateTime::now();
                d.insert("lastWrite", doc! { "opTime": { "ts": Bson::Timestamp(ts), "t": 1_i64 }, "lastWriteDate": now, "majorityOpTime": { "ts": Bson::Timestamp(ts), "t": 1_i64 }, "majorityWriteDate": now });
            }
            None => {
                d.insert(primary_flag, true);
                d.insert("topologyVersion", topology);
            }
        }
        d.insert("maxBsonObjectSize", 16_777_216_i32);
        d.insert("maxMessageSizeBytes", 48_000_000_i32);
        d.insert("maxWriteBatchSize", 100_000_i32);
        d.insert("localTime", Bson::DateTime(DateTime::now()));
        d.insert("logicalSessionTimeoutMinutes", 30_i32);
        d.insert("connectionId", conn_id as i32);
        d.insert("minWireVersion", 0_i32);
        d.insert("maxWireVersion", MAX_WIRE_VERSION);
        d.insert("readOnly", false);
        d.insert("ok", 1.0);
        d
    }

    // ---- databases and collections ---------------------------------------------------------

    async fn list_databases(&self, db: &str, cmd: &Document, conn_id: i64) -> R<Document> {
        if db != "admin" {
            return Err(MongoError::new(13, "Unauthorized", "listDatabases may only be run against the admin database.".into()));
        }
        let wrong = |f: &str, v: &Bson, want: &str| MongoError::type_mismatch(format!("BSON field 'listDatabases.{f}' is the wrong type '{}', expected {want}", crate::update::type_name(v)));
        for f in ["nameOnly", "authorizedDatabases"] {
            if let Some(v) = cmd.get(f) {
                if !matches!(v, Bson::Boolean(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_)) {
                    return Err(wrong(f, v, "types '[bool, long, int, decimal, double']"));
                }
            }
        }
        let filter = match cmd.get("filter") {
            Some(Bson::Document(f)) => f.clone(),
            Some(v) => return Err(wrong("filter", v, "type 'object'")),
            None => Document::new(),
        };
        crate::query::probe_filter(&filter)?;
        let name_only = bool_arg(cmd.get("nameOnly"), false);
        let mut names = self.dbs.list_databases().await?;
        // (mongod always has these)
        for n in ["admin", "config", "local"] {
            if !names.iter().any(|x| x == n) {
                names.push(n.to_string());
            }
        }
        names.sort();
        let names = self.visible_databases(conn_id, names).await?;
        let mut dbs: Vec<Bson> = Vec::new();
        for n in names {
            let full = doc! { "name": n, "sizeOnDisk": 0_i64, "empty": false };
            if !matches(&full, &filter)? {
                continue;
            }
            dbs.push(Bson::Document(if name_only { doc! { "name": full.get_str("name").unwrap_or_default() } } else { full }));
        }
        let mut d = doc! { "databases": dbs };
        if !name_only {
            d.insert("totalSize", 0_i64);
            d.insert("totalSizeMb", 0_i64);
        }
        d.insert("ok", 1.0);
        Ok(d)
    }

    async fn list_collections(&self, db: &str, cmd: &Document) -> R<Document> {
        let name_only = bool_arg(cmd.get("nameOnly"), false);
        let filter = cmd.get_document("filter").cloned().unwrap_or_default();
        let mut g = self.dbs.lock(db).await;
        let mut names = g.collections().await?;
        names.sort();
        let mut options_of: HashMap<String, Document> = HashMap::new();
        let mut uuids: HashMap<String, Vec<u8>> = HashMap::new();
        for n in &names {
            if let Some(o) = g.coll_opts(n).await? {
                options_of.insert(n.clone(), o.raw.clone());
            }
            if !name_only {
                uuids.insert(n.clone(), g.collection_uuid(n).await?);
            }
        }
        drop(g);
        let views = self.list_views(db).await?;
        let mut docs = VecDeque::new();
        let mut extra: Vec<Document> = Vec::new();
        let mut names: Vec<(String, Option<crate::views::ViewDef>)> = names.into_iter().map(|n| (n, None)).chain(views.into_iter().map(|(n, v)| (n, Some(v)))).collect();
        names.sort_by(|a, b| a.0.cmp(&b.0));
        for (n, view) in names {
            let d = if let Some(v) = view {
                if name_only { doc! { "name": n, "type": "view" } } else { doc! { "name": n, "type": "view", "options": v.options(), "info": { "readOnly": true } } }
            } else if name_only {
                if options_of.get(&n).map(|o| o.contains_key("timeseries")).unwrap_or(false) {
                    extra.push(doc! { "name": format!("system.buckets.{n}"), "type": "collection" });
                    doc! { "name": n, "type": "timeseries" }
                } else {
                    doc! { "name": n, "type": "collection" }
                }
            } else {
                let options = options_of.get(&n).cloned().unwrap_or_default();
                let mut id_index = doc! { "v": 2, "key": { "_id": 1 }, "name": "_id_" };
                if let Some(c) = options.get("collation") {
                    id_index.insert("collation", c.clone());
                }
                let uuid = Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: uuids.get(&n).cloned().unwrap_or_default() });
                if options.contains_key("timeseries") {
                    let companion = crate::special_colls::buckets_entry(&n, &options, uuid.clone());
                    if matches(&companion, &filter)? {
                        extra.push(companion);
                    }
                    doc! { "name": n, "type": "timeseries", "options": options.clone(), "info": { "readOnly": false } }
                } else if options.contains_key("clusteredIndex") {
                    doc! { "name": n, "type": "collection", "options": options.clone(), "info": { "readOnly": false, "uuid": uuid } }
                } else {
                    doc! { "name": n, "type": "collection", "options": options, "info": { "readOnly": false, "uuid": uuid }, "idIndex": id_index }
                }
            };
            if matches(&d, &filter)? {
                docs.push_back(d);
            }
        }
        docs.extend(extra);
        let batch = cmd.get_document("cursor").ok().and_then(|c| int_arg(c.get("batchSize"))).map(|b| b as usize);
        Ok(self.cursor_reply(format!("{db}.$cmd.listCollections"), docs, batch, false))
    }

    pub(crate) async fn create(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "create")?;
        if let Some(d) = self.create_view(db, coll, cmd).await? {
            return Ok(d);
        }
        let collation = match cmd.get("collation") {
            Some(v) => crate::collation::of_create(v)?,
            None => None,
        };
        let capped = crate::capped::parse_create(cmd)?;
        // the options as stored and listed: capped, size, max, then the collation
        let mut raw = Document::new();
        if let Some((size, max)) = capped.clone() {
            raw.extend(crate::capped::options_doc(size, max));
        }
        if let Some(c) = &collation {
            raw.insert("collation", c.to_doc());
        }
        let special = crate::special_colls::parse_create(&format!("{db}.{coll}"), cmd, capped.is_some())?;
        if special.options.contains_key("timeseries") { crate::special_colls::SERIES_SEEN.store(true, std::sync::atomic::Ordering::Relaxed); }
        raw.extend(special.options.clone());
        if let Some(v) = cmd.get("encryptedFields") {
            raw.insert("encryptedFields", crate::special_colls::encrypted_fields(coll, v, self.is_rs())?);
        }
        raw.extend(crate::collopts::plain_options("create", cmd)?);
        raw.extend(crate::collopts::validation_options("create", cmd)?);
        if let Some(i) = crate::collopts::images_option("create", cmd)? {
            // (a disabled option is not kept)
            if i.get_document("changeStreamPreAndPostImages").ok().and_then(|d| d.get_bool("enabled").ok()) == Some(true) {
                raw.extend(i);
            }
        }
        let raw = crate::collopts::canonical(&raw);
        let mut g = self.dbs.lock(db).await;
        let mut names = g.collections().await?;
        if names.iter().any(|n| n == coll) {
            // MongoDB 7.0: creating an existing collection with the same options succeeds.
            let existing = g.coll_opts(coll).await?.map(|o| o.raw.clone()).unwrap_or_default();
            let has_options = cmd.iter().any(|(k, v)| {
                !matches!(k.as_str(), "create" | "$db" | "lsid" | "$clusterTime" | "writeConcern" | "readConcern" | "comment" | "txnNumber" | "$readPreference" | "collation" | "capped" | "size" | "max" | "validator" | "validationLevel" | "validationAction" | "storageEngine" | "indexOptionDefaults")
                    && !(k == "capped" && matches!(v, Bson::Boolean(false)))
            });
            if has_options {
                return Err(MongoError::namespace_exists(format!("Collection already exists. NS: {db}.{coll}")));
            }
            if existing != raw {
                let mut shown = doc! { "uuid": "<uuid>" };
                shown.extend(existing);
                let body = crate::update::fmt_doc(&shown).replacen("uuid: \"<uuid>\"", &format!("uuid: UUID(\"{}\")", fake_uuid()), 1);
                return Err(MongoError::namespace_exists(format!("namespace {db}.{coll} already exists, but with different options: {body}")));
            }
            return Ok(ok());
        }
        names.push(coll.to_string());
        let mut ops = vec![Entry::collections_op(&names)?];
        let options = raw.clone();
        {
            let mut initial: Vec<crate::index::IndexDef> = Vec::new();
            if let Some(t) = special.options.get_document("timeseries").ok() {
                if let Ok(meta) = t.get_str("metaField") {
                    let tf = t.get_str("timeField").unwrap_or_default();
                    if let Some(ix) = crate::index::IndexDef::from_stored(&doc! { "v": 2, "key": { meta: 1, tf: 1 }, "name": format!("{meta}_1_{tf}_1") }) {
                        initial.push(ix);
                    }
                }
            }
            if let Some((field, secs)) = &special.ttl {
                initial.extend(crate::special_colls::ttl_index(field, *secs));
            }
            if !initial.is_empty() {
                ops.push(Entry::indexes_op(coll, &initial)?);
            }
        }
        if !raw.is_empty() {
            ops.push(Op::Put(crate::collopts::meta_key(coll), raw.to_vec()?));
            g.set_coll_opts(coll, Some(crate::collopts::CollOpts::parse(raw)));
        }
        if capped.is_some() {
            ops.push(Op::Put(crate::capped::stat_key(coll), doc! { "docs": 0_i64, "bytes": 0_i64 }.to_vec()?));
        }
        g.apply(ops).await?;
        let uuid = g.collection_uuid(coll).await?;
        drop(g);
        self.publish_create(db, coll, uuid, &options, false);
        Ok(ok())
    }

    /// Process-wide gauges and counters for the load tests: open tenants and every object-store request.
    fn tenant_stats(&self) -> Document {
        use std::sync::atomic::Ordering::Relaxed;
        let s = self.dbs.global.counters.snapshot();
        let ls = self.dbs.leases.stats.lock().unwrap().clone();
        doc! {
            "holder": self.dbs.leases.holder.clone(),
            "pid": std::process::id() as i64,
            "openReaders": self.dbs.readers.load(Relaxed),
            "openWriters": self.dbs.writers.load(Relaxed),
            "entries": self.dbs.entry_count() as i64,
            "leasesHeld": self.dbs.leases.held_count() as i64,
            "leases": { "acquired": ls.acquired as i64, "takeovers": ls.takeovers as i64, "refused": ls.refused as i64, "renewals": ls.renewals as i64, "renewFailures": ls.renew_failures as i64, "lost": ls.lost as i64 },
            "ttl": {
                "passes": self.dbs.ttl.passes.load(Relaxed) as i64, "hintsListed": self.dbs.ttl.hints_listed.load(Relaxed) as i64,
                "due": self.dbs.ttl.due.load(Relaxed) as i64, "skippedOpen": self.dbs.ttl.skipped_open.load(Relaxed) as i64,
                "skippedGap": self.dbs.ttl.skipped_gap.load(Relaxed) as i64, "wakes": self.dbs.ttl.wakes.load(Relaxed) as i64,
                "sessionSweeps": self.dbs.ttl.session_sweeps.load(Relaxed) as i64, "docsExpired": self.dbs.ttl.docs_expired.load(Relaxed) as i64,
            },
            "requests": {
                "put": (s.put + s.multipart_create + s.multipart_part + s.multipart_complete) as i64,
                "copy": s.copy as i64, "list": s.list as i64,
                "get": (s.get + s.get_range) as i64, "head": s.head as i64, "delete": s.delete as i64,
                "billedPutTier": s.billed_put_tier() as i64, "billedGetTier": s.billed_get_tier() as i64,
                "putBytes": self.dbs.global.counters.put_bytes.load(Relaxed) as i64, "getBytes": self.dbs.global.counters.get_bytes.load(Relaxed) as i64,
            },
            "groupCommit": { "commits": tenant_bench::lifecycle::GROUP_COMMITS.load(Relaxed) as i64, "flushes": tenant_bench::lifecycle::GROUP_FLUSHES.load(Relaxed) as i64, "windowUs": tenant_bench::lifecycle::GROUP_WINDOW_US.load(Relaxed) as i64, "adaptiveWaits": tenant_bench::lifecycle::ADAPTIVE_WAITS.load(Relaxed) as i64, "adaptiveWaitUs": tenant_bench::lifecycle::ADAPTIVE_WAIT_US.load(Relaxed) as i64, "relaxedAcks": tenant_bench::lifecycle::RELAXED_ACKS.load(Relaxed) as i64 },
            "localLog": { "records": crate::locallog::RECORDS.load(Relaxed) as i64, "fsyncs": crate::locallog::FSYNCS.load(Relaxed) as i64, "uploads": crate::locallog::UPLOADS.load(Relaxed) as i64, "replayed": crate::locallog::REPLAYED.load(Relaxed) as i64 },
            "walGc": { "passes": tenant_bench::lifecycle::WAL_GC_PASSES.load(Relaxed) as i64, "deleted": tenant_bench::lifecycle::WAL_GC_DELETED.load(Relaxed) as i64, "listUs": tenant_bench::lifecycle::WAL_GC_LIST_US.load(Relaxed) as i64, "deleteUs": tenant_bench::lifecycle::WAL_GC_DELETE_US.load(Relaxed) as i64, "totalUs": tenant_bench::lifecycle::WAL_GC_TOTAL_US.load(Relaxed) as i64, "noop": tenant_bench::lifecycle::WAL_GC_NOOP.load(Relaxed) as i64 },
            "sstGc": { "passes": tenant_bench::lifecycle::SST_GC_PASSES.load(Relaxed) as i64, "deletedSsts": tenant_bench::lifecycle::SST_GC_DELETED_SSTS.load(Relaxed) as i64, "deletedManifests": tenant_bench::lifecycle::SST_GC_DELETED_MANIFESTS.load(Relaxed) as i64, "noop": tenant_bench::lifecycle::SST_GC_NOOP.load(Relaxed) as i64, "closeLight": tenant_bench::lifecycle::CLOSE_GC_LIGHT.load(Relaxed) as i64 },
            "putByCategory": tenant_bench::counting::CATEGORIES.iter().enumerate().map(|(i, n)| (n.to_string(), Bson::Int64(self.dbs.global.counters.put_cat[i].load(Relaxed) as i64))).collect::<Document>(),
            "getByCategory": tenant_bench::counting::CATEGORIES.iter().enumerate().map(|(i, n)| (n.to_string(), Bson::Int64(self.dbs.global.counters.get_cat[i].load(Relaxed) as i64))).collect::<Document>(),
            "maintenance": {
                "compactRuns": tenant_bench::lifecycle::MAINT.compact_runs.load(Relaxed) as i64, "compactPuts": tenant_bench::lifecycle::MAINT.compact_puts.load(Relaxed) as i64,
                "compactGets": tenant_bench::lifecycle::MAINT.compact_gets.load(Relaxed) as i64, "gcRuns": tenant_bench::lifecycle::MAINT.gc_runs.load(Relaxed) as i64,
                "gcDeletes": tenant_bench::lifecycle::MAINT.gc_deletes.load(Relaxed) as i64, "gcLists": tenant_bench::lifecycle::MAINT.gc_lists.load(Relaxed) as i64,
                "gcGets": tenant_bench::lifecycle::MAINT.gc_gets.load(Relaxed) as i64,
            },
            "wakeQueue": { "queued": crate::dbs::WAKE_QUEUED.load(Relaxed), "maxQueued": crate::dbs::WAKE_MAX_QUEUED.load(Relaxed), "waits": crate::dbs::WAKE_WAITS.load(Relaxed) as i64, "waitMs": crate::dbs::WAKE_WAIT_MS.load(Relaxed) as i64 },
            "lat": {
                "put": self.dbs.global.counters.lat_put.snapshot().into_iter().map(|x| x as i64).collect::<Vec<_>>(),
                "get": self.dbs.global.counters.lat_get.snapshot().into_iter().map(|x| x as i64).collect::<Vec<_>>(),
            },
            "ok": 1.0
        }
    }

    /// Runs a command with object-store tracing on and returns every request it made, in order. For the wake
    /// breakdown: `cold` closes the tenant first, `marker` (`present` | `missing`) sets the clean marker so the wake
    /// takes the clean or the unclean path. Meant for a quiet server (the trace is process-wide).
    async fn tenant_trace(&self, db: &str, cmd: &Document) -> R<Document> {
        use object_store::ObjectStoreExt;
        let run = cmd.get_document("run").map_err(|_| MongoError::bad_value("tenantTrace needs a `run` command".into()))?.clone();
        let name = run.keys().next().cloned().unwrap_or_default();
        if bool_arg(cmd.get("cold"), true) {
            let mut g = self.dbs.lock(db).await;
            g.close().await?;
        }
        let marker = object_store::path::Path::from(format!("tenants/{db}/_clean"));
        match cmd.get_str("marker").unwrap_or("") {
            "present" => {
                self.dbs.global.put(&marker, object_store::PutPayload::from_static(b"clean")).await.map_err(|e| MongoError::internal(e.to_string()))?;
            }
            "missing" => {
                let _ = self.dbs.global.delete(&marker).await;
            }
            _ => {}
        }
        tenant_bench::counting::set_phase(0);
        self.dbs.global.trace.start();
        let t0 = std::time::Instant::now();
        let reply = self.dispatch(db, &name, &run, 0).await;
        let elapsed = t0.elapsed();
        let events = self.dbs.global.trace.take();
        Ok(doc! {
            "elapsedUs": elapsed.as_micros() as i64,
            "innerOk": reply.is_ok(),
            "events": events.into_iter().map(|e| Bson::Document(doc! { "op": e.op, "path": e.path, "phase": e.phase, "startUs": e.start_us as i64, "durUs": e.dur_us as i64, "bytes": e.bytes as i64 })).collect::<Vec<_>>(),
            "ok": 1.0,
        })
    }

    async fn tenant_admin(&self, db: &str, cmd: &Document) -> R<Document> {
        let mut g = self.dbs.lock(db).await;
        if bool_arg(cmd.get("close"), false) {
            g.close().await?;
        }
        let (put_tier, get_tier, deletes) = self.dbs.request_counts();
        Ok(doc! {
            "mode": g.mode(), "wakes": g.wakes as i64, "closes": g.closes as i64, "uncleanWake": g.unclean_wake().unwrap_or(false),
            "requests": { "putTier": put_tier as i64, "getTier": get_tier as i64, "delete": deletes as i64 },
            "ok": 1.0
        })
    }

    // ---- cursors ---------------------------------------------------------------------------

    pub(crate) fn cursor_reply(&self, ns: String, mut docs: VecDeque<Document>, batch: Option<usize>, single_batch: bool) -> Document {
        let n = batch.unwrap_or(DEFAULT_FIRST_BATCH).min(docs.len());
        let first: Vec<Bson> = docs.drain(..n).map(Bson::Document).collect();
        let id = if docs.is_empty() || single_batch { 0 } else { self.store_cursor(ns.clone(), docs) };
        // (the list commands put the cursor id and namespace first)
        doc! { "cursor": { "id": id, "ns": ns, "firstBatch": first }, "ok": 1.0 }
    }

    /// The reply of `find` and `aggregate`: like mongod, a batch that is exactly as big as asked leaves the cursor open (the
    /// next `getMore` finds out that it is exhausted), unless a `limit` was reached.
    pub(crate) fn cursor_reply_open(&self, ns: String, mut docs: VecDeque<Document>, batch: Option<usize>, single_batch: bool, limited: bool) -> Document {
        let want = batch.unwrap_or(DEFAULT_FIRST_BATCH);
        let n = want.min(docs.len());
        let first: Vec<Bson> = docs.drain(..n).map(Bson::Document).collect();
        let open = !single_batch && (!docs.is_empty() || (!limited && n > 0 && n == want));
        let id = if open { self.store_cursor(ns.clone(), docs) } else { 0 };
        doc! { "cursor": { "firstBatch": first, "id": id, "ns": ns }, "ok": 1.0 }
    }

    /// Register a tailable cursor (it stays open when it has caught up) and answer with its first batch.
    pub(crate) fn tail_reply(&self, ns: String, first: Vec<Document>, tail: crate::capped::Tail) -> Document {
        let mut c = self.cursors.lock().unwrap();
        let id = loop {
            let id = (rand::random::<i64>() & i64::MAX) | (1 << 40);
            if !c.contains_key(&id) {
                break id;
            }
        };
        c.insert(id, Cursor { used: std::time::Instant::now(), no_timeout: false, ns: ns.clone(), docs: VecDeque::new(), tail: Some(tail) });
        doc! { "cursor": { "firstBatch": first.into_iter().map(Bson::Document).collect::<Vec<_>>(), "id": id, "ns": ns }, "ok": 1.0 }
    }

    fn store_cursor(&self, ns: String, docs: VecDeque<Document>) -> i64 {
        let mut c = self.cursors.lock().unwrap();
        loop {
            let id = (rand::random::<i64>() & i64::MAX) | (1 << 40);
            if !c.contains_key(&id) {
                c.insert(id, Cursor { used: std::time::Instant::now(), no_timeout: false, ns, docs, tail: None });
                return id;
            }
        }
    }

    pub(crate) async fn get_more(&self, db: &str, cmd: &Document) -> R<Document> {
        let id = int_arg(cmd.get("getMore")).ok_or_else(|| MongoError::new(14, "TypeMismatch", "getMore's cursor id must be a long".into()))?;
        if self.change_cursors.lock().unwrap().contains_key(&id) {
            return self.change_get_more(id, cmd).await;
        }
        let batch = int_arg(cmd.get("batchSize")).map(|b| b as usize);
        let tail = {
            let cursors = self.cursors.lock().unwrap();
            cursors.get(&id).map(|c| c.tail.clone())
        };
        if let Some(Some(tail)) = tail {
            return self.tail_get_more(id, tail, batch, cmd).await;
        }
        if let Some(v) = cmd.get("maxTimeMS") {
            let m = match v {
                Bson::Int32(i) => *i as i64,
                Bson::Int64(i) => *i,
                Bson::Double(f) => *f as i64,
                other => return Err(MongoError::type_mismatch(format!("BSON field 'getMore.maxTimeMS' is the wrong type '{}', expected types '[long, int, decimal, double']", crate::update::type_name(other)))),
            };
            if !(0..=2147483647).contains(&m) {
                return Err(MongoError::bad_value(format!("{m} value for maxTimeMS is out of range [0, 2147483647]")));
            }
            if self.cursors.lock().unwrap().contains_key(&id) {
                return Err(MongoError::bad_value("cannot set maxTimeMS on getMore command for a non-awaitData cursor".into()));
            }
        }
        let mut cursors = self.cursors.lock().unwrap();
        let c = cursors.get_mut(&id).ok_or_else(|| MongoError::cursor_not_found(id))?;
        c.used = std::time::Instant::now();
        let n = batch.unwrap_or(usize::MAX).min(c.docs.len());
        let next: Vec<Bson> = c.docs.drain(..n).map(Bson::Document).collect();
        let ns = c.ns.clone();
        let done = c.docs.is_empty() && !(batch.is_some() && n > 0 && n == batch.unwrap_or(0));
        if done {
            cursors.remove(&id);
        }
        let _ = db;
        Ok(doc! { "cursor": { "nextBatch": next, "id": if done { 0 } else { id }, "ns": ns }, "ok": 1.0 })
    }

    /// `getMore` of a tailable cursor: the documents after the last one it looked at; an `awaitData` cursor waits for some.
    async fn tail_get_more(&self, id: i64, tail: crate::capped::Tail, batch: Option<usize>, cmd: &Document) -> R<Document> {
        let max_time = int_arg(cmd.get("maxTimeMS"));
        if max_time.is_some() && !tail.await_data {
            return Err(MongoError::bad_value("cannot set maxTimeMS on getMore command for a non-awaitData cursor".into()));
        }
        let wait = std::time::Duration::from_millis(max_time.filter(|m| *m > 0).unwrap_or(1000) as u64);
        let deadline = std::time::Instant::now() + wait;
        let limit = batch.filter(|b| *b > 0).unwrap_or(usize::MAX);
        let mut cur = tail;
        loop {
            let r = {
                let mut g = self.dbs.read(&cur.db).await?;
                crate::capped::fetch_after(&mut g, &cur, limit).await
            };
            let (docs, last) = match r {
                Ok(x) => x,
                Err(e) => {
                    self.cursors.lock().unwrap().remove(&id);
                    return Err(e);
                }
            };
            cur.last_seq = last;
            if !docs.is_empty() || !cur.await_data || std::time::Instant::now() >= deadline {
                let ns = format!("{}.{}", cur.db, cur.coll);
                match self.cursors.lock().unwrap().get_mut(&id) {
                    Some(c) => {
                        c.tail = Some(cur);
                        c.used = std::time::Instant::now();
                    }
                    None => return Err(MongoError::cursor_not_found(id)),
                }
                return Ok(doc! { "cursor": { "nextBatch": docs.into_iter().map(Bson::Document).collect::<Vec<_>>(), "id": id, "ns": ns }, "ok": 1.0 });
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    fn kill_cursors(&self, cmd: &Document) -> R<Document> {
        let ids = cmd.get_array("cursors").map_err(|_| MongoError::new(40414, "IDLFailedToParse", "BSON field 'killCursors.cursors' is missing but a required field".into()))?;
        let mut killed = Vec::new();
        let mut not_found = Vec::new();
        for (i, b) in ids.iter().enumerate() {
            if !matches!(b, Bson::Int64(_)) {
                return Err(MongoError::type_mismatch(format!("BSON field 'killCursors.cursors.{i}' is the wrong type '{}', expected type 'long'", crate::update::type_name(b))));
            }
        }
        let mut cursors = self.cursors.lock().unwrap();
        for id in ids.iter().filter_map(|b| int_arg(Some(b))) {
            if cursors.remove(&id).is_some() || self.change_cursors.lock().unwrap().remove(&id).is_some() { killed.push(Bson::Int64(id)) } else { not_found.push(Bson::Int64(id)) }
        }
        Ok(doc! { "cursorsKilled": killed, "cursorsNotFound": not_found, "cursorsAlive": [], "cursorsUnknown": [], "ok": 1.0 })
    }
}

fn build_info() -> Document {
    doc! {
        "version": "7.0.45",
        "gitVersion": "none",
        "modules": [],
        "allocator": "system",
        "javascriptEngine": "quickjs",
        "sysInfo": "deprecated",
        "versionArray": [7, 0, 45, 0],
        "openssl": { "running": "disabled", "compiled": "disabled" },
        "buildEnvironment": {},
        "bits": 64,
        "debug": false,
        "maxBsonObjectSize": 16_777_216_i32,
        "storageEngines": ["slatedb"],
        "ok": 1.0,
    }
}


