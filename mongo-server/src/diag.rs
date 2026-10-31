//! Diagnostics and administration commands that applications, drivers and tools send: `serverStatus`, `currentOp` / `killOp`,
//! `top`, `hostInfo`, `getCmdLineOpts`, `connPoolStats`, `listCommands`, `getLog`, `lockInfo`, the read/write concern defaults,
//! cluster parameters, `profile` (with `system.profile`), `dbHash`, `dataSize`, `validateDBMetadata`, `startSession`,
//! `killAllSessions`, the plan cache commands, `reIndex`, `compact`, `filemd5` and `setIndexCommitQuorum`.
//!
//! The shape of `serverStatus`, `hostInfo`, `connPoolStats` and `listCommands` comes from templates captured from mongod 7.0.45
//! (`src/diag/*.json`, canonical extended JSON): every number in them is zero until the live counter of this server replaces it.

use crate::cmd::{bool_arg, collection_name, int_arg, ok, Server, R};
use crate::error::MongoError;
use bson::{doc, Bson, Document};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::Notify;

const SS_SA: &str = include_str!("diag/serverStatus_sa.json");
const SS_RS: &str = include_str!("diag/serverStatus_rs.json");
const HOST_INFO: &str = include_str!("diag/hostInfo_sa.json");
const CONN_POOL: &str = include_str!("diag/connPoolStats_sa.json");
const LIST_COMMANDS: &str = include_str!("diag/listCommands.json");
const FEATURES: &str = include_str!("diag/features.json");

pub const VERSION: &str = "7.0.45";

fn template(src: &str) -> Document {
    match serde_json::from_str::<Bson>(src) {
        Ok(Bson::Document(d)) => d,
        _ => Document::new(),
    }
}

/// The template with every number set to zero (keeping its type).
fn zeroed(d: &Document) -> Document {
    d.iter()
        .map(|(k, v)| {
            let z = match v {
                Bson::Int32(_) => Bson::Int32(0),
                Bson::Int64(_) => Bson::Int64(0),
                Bson::Double(_) if k != "ok" => Bson::Double(0.0),
                Bson::Document(x) => Bson::Document(zeroed(x)),
                Bson::Array(a) => Bson::Array(a.iter().map(|e| if let Bson::Document(x) = e { Bson::Document(zeroed(x)) } else { e.clone() }).collect()),
                other => other.clone(),
            };
            (k.clone(), z)
        })
        .collect()
}

fn set_path(d: &mut Document, path: &str, v: Bson) {
    match path.split_once('.') {
        None => {
            d.insert(path, v);
        }
        Some((h, t)) => {
            if let Some(Bson::Document(x)) = d.get_mut(h) {
                set_path(x, t, v);
            }
        }
    }
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname").map(|s| s.trim().to_string()).ok().filter(|s| !s.is_empty()).unwrap_or_else(|| "localhost".into())
}

fn now_string() -> String {
    chrono_like(bson::DateTime::now())
}

fn chrono_like(t: bson::DateTime) -> String {
    t.try_to_rfc3339_string().map(|s| s.replace('Z', "+00:00")).unwrap_or_default()
}

struct ConnInfo {
    client: String,
    metadata: Option<Document>,
}

struct OpRec {
    conn: i64,
    db: String,
    name: String,
    cmd: Document,
    start: Instant,
    kill: Arc<Notify>,
}

#[derive(Default, Clone, Copy)]
struct TopCell {
    time: i64,
    count: i64,
}

#[derive(Default, Clone, Copy)]
struct TopNs {
    total: TopCell,
    read: TopCell,
    write: TopCell,
    queries: TopCell,
    getmore: TopCell,
    insert: TopCell,
    update: TopCell,
    remove: TopCell,
    commands: TopCell,
}

#[derive(Clone)]
pub struct Prof {
    level: i32,
    filter: Option<Document>,
}

impl Default for Prof {
    fn default() -> Self {
        Prof { level: 0, filter: None }
    }
}

pub struct Diag {
    pub(crate) start: Instant,
    /// insert, query, update, delete, getmore, command
    op: [AtomicU64; 6],
    /// inserted, deleted, updated, returned
    docs: [AtomicU64; 4],
    requests: AtomicU64,
    total_conns: AtomicU64,
    cmds: Mutex<BTreeMap<String, (u64, u64)>>,
    ops: Mutex<BTreeMap<i64, OpRec>>,
    next_op: AtomicI64,
    conns: Mutex<HashMap<i64, ConnInfo>>,
    seq: Mutex<HashMap<i64, Vec<String>>>,
    /// Logical sessions seen: lsid (hex) -> (the session id, last use).
    sessions: Mutex<HashMap<String, (Bson, bson::DateTime)>>,
    top: Mutex<BTreeMap<String, TopNs>>,
    prof: Mutex<HashMap<String, Prof>>,
    /// slowms and sampleRate are server-wide
    prof_global: Mutex<(i64, f64)>,
    pub filters: Mutex<HashMap<String, Vec<Document>>>,
    log: Mutex<(u64, VecDeque<String>)>,
    rwc: Mutex<Option<Document>>,
    cluster: Mutex<Option<Document>>,
    fsync_locks: AtomicI64,
}

impl Default for Diag {
    fn default() -> Self {
        let d = Diag {
            start: Instant::now(),
            op: Default::default(),
            docs: Default::default(),
            requests: AtomicU64::new(0),
            total_conns: AtomicU64::new(0),
            cmds: Default::default(),
            ops: Default::default(),
            next_op: AtomicI64::new(1000),
            conns: Default::default(),
            seq: Default::default(),
            sessions: Default::default(),
            top: Default::default(),
            prof: Default::default(),
            prof_global: Mutex::new((100, 1.0)),
            filters: Default::default(),
            log: Default::default(),
            rwc: Default::default(),
            cluster: Default::default(),
            fsync_locks: AtomicI64::new(0),
        };
        d.log_line("I", "CONTROL", 23403, "initandlisten", "Build Info", Some(doc! { "buildInfo": { "version": VERSION, "gitVersion": "none", "modules": [], "allocator": "system", "environment": { "distarch": "x86_64", "target_arch": "x86_64" } } }));
        d.log_line("I", "NETWORK", 23016, "listener", "Waiting for connections", Some(doc! { "port": 0, "ssl": "off" }));
        d
    }
}

impl Diag {
    pub(crate) fn fsync_lock_count(&self) -> i64 {
        self.fsync_locks.load(Relaxed)
    }

    pub fn log_line(&self, sev: &str, comp: &str, id: i32, ctx: &str, msg: &str, attr: Option<Document>) {
        let mut line = doc! { "t": { "$date": chrono_like(bson::DateTime::now()) }, "s": sev, "c": comp, "id": id, "ctx": ctx, "msg": msg };
        if let Some(a) = attr {
            line.insert("attr", a);
        }
        let s = serde_json::to_string(&Bson::Document(line).into_relaxed_extjson()).unwrap_or_default() + "\n";
        let mut l = self.log.lock().unwrap();
        l.0 += 1;
        l.1.push_back(s);
        while l.1.len() > 1024 {
            l.1.pop_front();
        }
    }

    pub fn conn_opened(&self, id: i64, peer: Option<std::net::SocketAddr>) {
        self.total_conns.fetch_add(1, Relaxed);
        self.conns.lock().unwrap().insert(id, ConnInfo { client: peer.map(|p| p.to_string()).unwrap_or_default(), metadata: None });
    }

    pub fn note_seq(&self, id: i64, keys: &[String]) {
        let mut m = self.seq.lock().unwrap();
        if keys.is_empty() {
            m.remove(&id);
        } else {
            m.insert(id, keys.to_vec());
        }
    }

    pub fn conn_closed(&self, id: i64) {
        self.conns.lock().unwrap().remove(&id);
        self.seq.lock().unwrap().remove(&id);
    }

    pub fn set_metadata(&self, id: i64, md: Document) {
        if let Some(c) = self.conns.lock().unwrap().get_mut(&id) {
            if c.metadata.is_none() {
                c.metadata = Some(md);
            }
        }
    }

    /// An operation starts: it is listed by `currentOp` until `end`.
    pub fn begin(&self, conn: i64, db: &str, name: &str, cmd: &Document) -> (i64, Arc<Notify>) {
        let id = self.next_op.fetch_add(1, Relaxed);
        if let Some(Bson::Binary(b)) = cmd.get_document("lsid").ok().and_then(|l| l.get("id")) {
            let key: String = b.bytes.iter().map(|x| format!("{x:02x}")).collect();
            self.sessions.lock().unwrap().insert(key, (Bson::Binary(b.clone()), bson::DateTime::now()));
        }
        let kill = Arc::new(Notify::new());
        self.requests.fetch_add(1, Relaxed);
        self.ops.lock().unwrap().insert(id, OpRec { conn, db: db.to_string(), name: name.to_string(), cmd: crate::auth::redact_command(name, cmd), start: Instant::now(), kill: Arc::clone(&kill) });
        (id, kill)
    }

    /// Record a finished operation: counters, `top`, and the reply's document counts.
    pub fn end(&self, id: i64, db: &str, name: &str, cmd: &Document, r: &R<Document>, micros: i64) {
        self.ops.lock().unwrap().remove(&id);
        KILLED.lock().unwrap().retain(|k| *k != id);
        let (cat, n) = match name {
            "insert" => (0, cmd.get_array("documents").map(|a| a.len()).unwrap_or(1)),
            "find" => (1, 1),
            "update" => (2, cmd.get_array("updates").map(|a| a.len()).unwrap_or(1)),
            "delete" => (3, cmd.get_array("deletes").map(|a| a.len()).unwrap_or(1)),
            "getMore" => (4, 1),
            _ => (5, 1),
        };
        self.op[cat].fetch_add(n as u64, Relaxed);
        {
            let mut c = self.cmds.lock().unwrap();
            let e = c.entry(name.to_string()).or_insert((0, 0));
            e.0 += 1;
            if r.is_err() {
                e.1 += 1;
            }
        }
        if let Ok(d) = r {
            let cnt = |k: &str| d.get(k).and_then(|v| int_arg(Some(v))).unwrap_or(0).max(0) as u64;
            match name {
                "insert" => {
                    self.docs[0].fetch_add(cnt("n"), Relaxed);
                }
                "delete" => {
                    self.docs[1].fetch_add(cnt("n"), Relaxed);
                }
                "update" => {
                    self.docs[2].fetch_add(cnt("nModified") + d.get_array("upserted").map(|a| a.len() as u64).unwrap_or(0), Relaxed);
                }
                "find" | "getMore" | "aggregate" => {
                    if let Ok(c) = d.get_document("cursor") {
                        let n = c.get_array("firstBatch").or_else(|_| c.get_array("nextBatch")).map(|a| a.len()).unwrap_or(0);
                        self.docs[3].fetch_add(n as u64, Relaxed);
                    }
                }
                _ => {}
            }
        }
        let coll = match name {
            "find" | "insert" | "update" | "delete" | "count" | "distinct" | "aggregate" | "findAndModify" | "findandmodify" | "mapReduce" | "createIndexes" | "drop" | "listIndexes" | "create" | "collMod" | "dropIndexes" | "getMore" | "validate" | "collStats" => cmd.get(name).and_then(|v| v.as_str()).or_else(|| cmd.get("collection").and_then(|v| v.as_str())),
            _ => None,
        };
        if let Some(c) = coll {
            let ns = format!("{db}.{c}");
            let mut t = self.top.lock().unwrap();
            let e = t.entry(ns).or_default();
            let bump = |c: &mut TopCell| {
                c.time += micros;
                c.count += 1;
            };
            bump(&mut e.total);
            let (is_write, cell): (bool, fn(&mut TopNs) -> &mut TopCell) = match name {
                "find" => (false, |e| &mut e.queries),
                "getMore" => (false, |e| &mut e.getmore),
                "insert" => (true, |e| &mut e.insert),
                "update" => (true, |e| &mut e.update),
                "delete" => (true, |e| &mut e.remove),
                "findAndModify" | "findandmodify" => (true, |e| &mut e.commands),
                _ => (false, |e| &mut e.commands),
            };
            bump(cell(e));
            if is_write || matches!(name, "createIndexes" | "drop" | "create" | "collMod" | "dropIndexes") {
                bump(&mut e.write);
            } else {
                bump(&mut e.read);
            }
        }
    }

    /// Run `fut`; when `killable` and `killOp` names this operation first, stop it with Interrupted.
    pub async fn guarded<F: std::future::Future<Output = R<Document>>>(&self, kill: Arc<Notify>, killable: bool, fut: F) -> R<Document> {
        if !killable {
            return fut.await;
        }
        tokio::select! {
            r = fut => r,
            _ = kill.notified() => Err(MongoError::new(11601, "Interrupted", "operation was interrupted".into())),
        }
    }

    fn prof_of(&self, db: &str) -> Prof {
        self.prof.lock().unwrap().get(db).cloned().unwrap_or_default()
    }
}

tokio::task_local! {
    /// The id `currentOp` shows for the command the task runs.
    static CUR_OP: i64;
}

static KILLED: Mutex<Vec<i64>> = Mutex::new(Vec::new());

/// Run a command as operation `id`.
pub async fn as_op<F: std::future::Future>(id: i64, fut: F) -> F::Output {
    CUR_OP.scope(id, fut).await
}

/// Has `killOp` stopped the operation running on this task? (JavaScript checks this while it runs.)
pub fn current_killed() -> bool {
    CUR_OP.try_with(|id| KILLED.lock().map(|k| k.contains(id)).unwrap_or(false)).unwrap_or(false)
}

/// Does the command run JavaScript? Such a command blocks its thread, so it gets one of its own.
pub fn needs_js(name: &str, cmd: &Document) -> bool {
    fn has(d: &Document) -> bool {
        d.iter().any(|(k, v)| {
            matches!(k.as_str(), "$where" | "$function" | "$accumulator") || match v {
                Bson::Document(x) => has(x),
                Bson::Array(a) => a.iter().any(|e| matches!(e, Bson::Document(x) if has(x))),
                _ => false,
            }
        })
    }
    matches!(name, "mapReduce" | "mapreduce") || has(cmd)
}

/// `$queryStats`: parses the stage; it only runs as the first stage of `{aggregate: 1}` on `admin` (`db` is `None` for a collection
/// pipeline). Query statistics are not collected, so the stage yields nothing.
pub fn query_stats_stage(spec: &Bson, db: Option<&str>) -> R<()> {
    let Bson::Document(o) = spec else {
        return Err(MongoError::new(9, "FailedToParse", format!("$queryStats value must be an object. Found: {}", crate::update::type_name(spec))));
    };
    for (k, v) in o {
        match k.as_str() {
            "transformIdentifiers" => {
                let Bson::Document(t) = v else {
                    return Err(MongoError::type_mismatch(format!("BSON field '$queryStats.transformIdentifiers' is the wrong type '{}', expected type 'object'", crate::update::type_name(v))));
                };
                let mut hmac = false;
                for (tk, tv) in t {
                    match (tk.as_str(), tv) {
                        ("algorithm", Bson::String(a)) if a == "hmac-sha-256" => hmac = true,
                        ("algorithm", Bson::String(a)) => return Err(MongoError::new(2, "BadValue", format!("Enumeration value '{a}' for field '$queryStats.transformIdentifiers.algorithm' is not a valid value."))),
                        ("algorithm", x) => return Err(MongoError::type_mismatch(format!("BSON field '$queryStats.transformIdentifiers.algorithm' is the wrong type '{}', expected type 'string'", crate::update::type_name(x)))),
                        ("hmacKey", Bson::Binary(b)) => {
                            use bson::spec::BinarySubtype as B;
                            if !matches!(b.subtype, B::Sensitive) {
                                let n = match b.subtype {
                                    B::Generic => "general",
                                    B::Function => "function",
                                    B::BinaryOld => "byteArrayDeprecated",
                                    B::UuidOld => "UUIDOld",
                                    B::Uuid => "UUID",
                                    B::Md5 => "MD5",
                                    B::Encrypted => "encrypt",
                                    B::Column => "bsonColumn",
                                    B::UserDefined(_) => "Custom",
                                    _ => "invalid",
                                };
                                return Err(MongoError::type_mismatch(format!("BSON field '$queryStats.transformIdentifiers.hmacKey' is the wrong binData type '{n}', expected type 'sensitive'")));
                            }
                            if b.bytes.len() < 32 {
                                return Err(MongoError::bad_value(format!("hmacKey must be greater than or equal to 32 bytes, found length: {}", b.bytes.len())));
                            }
                        }
                        ("hmacKey", x) => return Err(MongoError::type_mismatch(format!("BSON field '$queryStats.transformIdentifiers.hmacKey' is the wrong type '{}', expected type 'binData'", crate::update::type_name(x)))),
                        (other, _) => return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field '$queryStats.transformIdentifiers.{other}' is an unknown field."))),
                    }
                }
                if !t.contains_key("algorithm") {
                    return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field '$queryStats.transformIdentifiers.algorithm' is missing but a required field".into()));
                }
                if hmac && !t.contains_key("hmacKey") {
                    return Err(MongoError::new(9, "FailedToParse", "The 'hmacKey' parameter of the $queryStats stage must be specified when applying the hmac-sha-256 algorithm".into()));
                }
            }
            _ => return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field '$queryStats.{k}' is an unknown field."))),
        }
    }
    if db != Some("admin") {
        return Err(MongoError::new(73, "InvalidNamespace", "$queryStats must be run against the 'admin' database with {aggregate: 1}".into()));
    }
    Ok(())
}

/// The error of `{aggregate: 1}` when the pipeline does not start with a stage that needs no collection.
pub fn db_level_error(first: Option<&str>) -> MongoError {
    const NEEDS_COLLECTION: &[&str] = &[
        "$match", "$group", "$project", "$sort", "$limit", "$skip", "$unwind", "$lookup", "$facet", "$sample", "$collStats", "$indexStats", "$count", "$addFields", "$set", "$unset", "$replaceRoot", "$replaceWith", "$bucket", "$bucketAuto", "$graphLookup", "$redact",
        "$sortByCount", "$out", "$merge", "$geoNear", "$unionWith", "$setWindowFields", "$densify", "$fill", "$planCacheStats", "$documents",
    ];
    match first {
        None => MongoError::new(73, "InvalidNamespace", "{aggregate: 1} is not valid for an empty pipeline.".into()),
        Some(n) if NEEDS_COLLECTION.contains(&n) => MongoError::new(73, "InvalidNamespace", format!("{{aggregate: 1}} is not valid for '{n}'; a collection is required.")),
        Some("$search") | Some("$listSearchIndexes") | Some("$searchMeta") | Some("$vectorSearch") => MongoError::new(6047401, "Location6047401", format!("{} stage is only allowed on MongoDB Atlas", first.unwrap())),
        Some(n) => MongoError::new(40324, "Location40324", format!("Unrecognized pipeline stage name: '{n}'")),
    }
}

type Job = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

static JS_THREAD: std::sync::OnceLock<std::sync::mpsc::Sender<Job>> = std::sync::OnceLock::new();

/// Run a command that executes JavaScript on the thread that does all of it.
pub async fn run_js<F: std::future::Future<Output = R<Document>> + Send + 'static>(fut: F) -> R<Document> {
    let tx = JS_THREAD.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel::<Job>();
        let handle = tokio::runtime::Handle::current();
        std::thread::Builder::new()
            .name("js".into())
            .stack_size(8 << 20)
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    handle.block_on(job);
                }
            })
            .expect("js thread");
        tx
    });
    let (rtx, rrx) = tokio::sync::oneshot::channel();
    let job: Job = Box::pin(async move {
        let _ = rtx.send(fut.await);
    });
    if tx.send(job).is_err() {
        return Err(MongoError::new(1, "InternalError", "the JavaScript thread is gone".into()));
    }
    rrx.await.unwrap_or_else(|_| Err(MongoError::new(1, "InternalError", "the JavaScript thread dropped the command".into())))
}

/// Commands the engine can stop with `killOp` (a write is never torn: it is applied by one atomic batch).
pub fn killable(name: &str) -> bool {
    matches!(name, "find" | "aggregate" | "count" | "distinct" | "mapReduce" | "mapreduce" | "getMore")
}

fn admin_only(db: &str, name: &str) -> R<()> {
    if db != "admin" {
        return Err(MongoError::new(13, "Unauthorized", format!("{name} may only be run against the admin database.")));
    }
    Ok(())
}

fn bad_type(v: &Bson, field: &str, want: &str) -> MongoError {
    MongoError::new(14, "TypeMismatch", format!("BSON field '{field}' is the wrong type '{}', expected type '{want}'", crate::update::type_name(v)))
}

fn md5_hex(data: &[u8]) -> String {
    use md5::{Digest, Md5};
    let mut h = Md5::new();
    h.update(data);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

impl Server {
    /// Diagnostic and administration commands; `None` when `name` is not one of them.
    pub(crate) async fn diag_command(&self, db: &str, name: &str, cmd: &Document) -> Option<R<Document>> {
        Some(match name {
            "serverStatus" => self.server_status(cmd),
            "hostInfo" => Ok(self.host_info()),
            "currentOp" => self.current_op(db, cmd).await,
            "killOp" => self.kill_op(db, cmd),
            "top" => self.top(db),
            "getCmdLineOpts" => admin_only(db, name).map(|_| self.cmd_line_opts()),
            "connPoolStats" => Ok(self.conn_pool_stats()),
            "listCommands" => Ok(self.list_commands()),
            "features" => Ok(template(FEATURES)),
            "lockInfo" => admin_only(db, name).map(|_| doc! { "lockInfo": [], "ok": 1.0 }),
            "getLog" => self.get_log(db, cmd),
            "getDefaultRWConcern" => self.get_default_rwc(db, cmd),
            "setDefaultRWConcern" => self.set_default_rwc(db, cmd),
            "getClusterParameter" => self.get_cluster_parameter(cmd),
            "setClusterParameter" => self.set_cluster_parameter(cmd),
            "profile" => self.profile(db, cmd),
            "dbHash" => self.db_hash(db, cmd).await,
            "dataSize" => self.data_size(cmd).await,
            "validateDBMetadata" => self.validate_db_metadata(cmd).await,
            "startSession" => {
                let r = start_session();
                if let Some(Bson::Binary(b)) = r.get_document("id").ok().and_then(|i| i.get("id").cloned()) {
                    let key: String = b.bytes.iter().map(|x| format!("{x:02x}")).collect();
                    self.diag.sessions.lock().unwrap().insert(key, (Bson::Binary(b), bson::DateTime::now()));
                }
                Ok(r)
            }
            "killAllSessions" | "killAllSessionsByPattern" => kill_all_sessions(name, cmd),
            "planCacheListFilters" | "planCacheSetFilter" | "planCacheClearFilters" | "planCacheClear" => self.plan_cache(db, name, cmd).await,
            "reIndex" => self.re_index(db, cmd).await,
            "compact" => self.compact(db, cmd).await,
            "filemd5" => self.filemd5(db, cmd).await,
            "setIndexCommitQuorum" => self.set_index_commit_quorum(db, cmd).await,
            "logRotate" => crate::admin_cmds::log_rotate(db, cmd),
            "fsync" => self.fsync(db, cmd),
            "fsyncUnlock" => self.fsync_unlock(db),
            "cursorInfo" => Err(MongoError::command_not_found(name)),
            _ => return None,
        })
    }

    /// The documents of a stage that starts a database-level pipeline (`{aggregate: 1}`); `None` for any other stage.
    pub(crate) fn db_level_source(&self, db: &str, name: &str, spec: &Bson) -> Option<R<Vec<Document>>> {
        match name {
            "$currentOp" => Some((|| {
                if db != "admin" {
                    return Err(MongoError::new(73, "InvalidNamespace", "$currentOp must be run against the 'admin' database with {aggregate: 1}".into()));
                }
                let Bson::Document(o) = spec else {
                    return Err(MongoError::new(14, "TypeMismatch", format!("$currentOp options must be specified in an object, but found: {}", crate::update::type_name(spec))));
                };
                for k in o.keys() {
                    if !matches!(k.as_str(), "allUsers" | "idleConnections" | "idleCursors" | "idleSessions" | "localOps" | "backtrace") {
                        return Err(MongoError::new(9, "FailedToParse", format!("Unrecognized option '{k}' in $currentOp stage.")));
                    }
                }
                let mut docs = self.current_op_docs(None, false);
                if bool_arg(o.get("idleConnections"), false) {
                    let busy: Vec<i64> = self.diag.ops.lock().unwrap().values().map(|r| r.conn).collect();
                    let conns = self.diag.conns.lock().unwrap();
                    let mut ids: Vec<&i64> = conns.keys().filter(|c| !busy.contains(c)).collect();
                    ids.sort();
                    for id in ids {
                        let c = &conns[id];
                        let mut d = doc! { "type": "idleConnection", "host": self.host_port(), "desc": format!("conn{id}"), "connectionId": *id as i32, "client": c.client.clone() };
                        if let Some(m) = &c.metadata {
                            d.insert("clientMetadata", m.clone());
                        }
                        d.insert("active", false);
                        d.insert("currentOpTime", now_string());
                        d.insert("threaded", true);
                        docs.push(d);
                    }
                }
                Ok(docs)
            })()),
            "$listLocalSessions" => Some((|| {
                if let Bson::Document(o) = spec {
                    if let Some(k) = o.keys().find(|k| !matches!(k.as_str(), "allUsers" | "users")) {
                        return Err(MongoError::new(40415, "IDLUnknownField", format!("BSON field '$listLocalSessions.{k}' is an unknown field.")));
                    }
                }
                let users_only = matches!(spec, Bson::Document(o) if o.get_array("users").map(|u| !u.is_empty()).unwrap_or(false) && !bool_arg(o.get("allUsers"), false));
                if users_only {
                    return Ok(vec![]);
                }
                let mut out: Vec<Document> = Vec::new();
                let cutoff = bson::DateTime::from_millis(bson::DateTime::now().timestamp_millis() - 30 * 60 * 1000);
                self.diag.sessions.lock().unwrap().retain(|_, (_, last)| *last >= cutoff);
                for (_, (id, last)) in self.diag.sessions.lock().unwrap().iter() {
                    out.push(doc! { "_id": { "id": id.clone(), "uid": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: sha_empty() }) }, "lastUse": Bson::DateTime(*last) });
                }
                Ok(out)
            })()),
            "$queryStats" => Some(query_stats_stage(spec, Some(db)).map(|_| Vec::new())),
            "$listSessions" => Some(Err(MongoError::new(73, "InvalidNamespace", "$listSessions may only be run against config.system.sessions".into()))),
            "$listCachedAndActiveUsers" => Some(Err(MongoError::new(224, "QueryFeatureNotAllowed", "$listCachedAndActiveUsers is not allowed with the current configuration. You may need to enable the corresponding feature flag".into()))),
            _ => None,
        }
    }

    pub(crate) fn is_rs(&self) -> bool {
        self.rs.lock().unwrap().is_some()
    }

    pub(crate) fn host_port_name(&self) -> String {
        hostname()
    }

    pub(crate) fn listen_port(&self) -> i32 {
        let args: Vec<String> = std::env::args().collect();
        args.iter().position(|a| a == "--listen").and_then(|i| args.get(i + 1)).and_then(|l| l.rsplit(':').next()).and_then(|p| p.parse().ok()).unwrap_or(27017)
    }

    fn host_port(&self) -> String {
        match &*self.rs.lock().unwrap() {
            Some((_, addr)) => format!("{}:{}", hostname(), addr.rsplit(':').next().unwrap_or("27017")),
            None => hostname(),
        }
    }

    pub(crate) fn server_status(&self, cmd: &Document) -> R<Document> {
        let rs = self.is_rs();
        let t = template(if rs { SS_RS } else { SS_SA });
        let mut d = zeroed(&t);
        let dg = &self.diag;
        let up = dg.start.elapsed();
        d.insert("host", self.host_port());
        d.insert("version", VERSION);
        d.insert("pid", Bson::Int64(std::process::id() as i64));
        d.insert("uptime", up.as_secs_f64().floor());
        d.insert("uptimeMillis", Bson::Int64(up.as_millis() as i64));
        d.insert("uptimeEstimate", Bson::Int64(up.as_secs() as i64));
        d.insert("localTime", Bson::DateTime(bson::DateTime::now()));
        let conns = dg.conns.lock().unwrap().len() as i32;
        let active = dg.ops.lock().unwrap().len() as i32;
        set_path(&mut d, "connections.current", Bson::Int32(conns));
        set_path(&mut d, "connections.available", Bson::Int32((51200 - conns).max(0)));
        set_path(&mut d, "connections.totalCreated", Bson::Int32(dg.total_conns.load(Relaxed) as i32));
        set_path(&mut d, "connections.active", Bson::Int32(active));
        set_path(&mut d, "connections.threaded", Bson::Int32(conns));
        set_path(&mut d, "flowControl.targetRateLimit", Bson::Int32(1000000000));
        for (i, k) in ["insert", "query", "update", "delete", "getmore", "command"].iter().enumerate() {
            set_path(&mut d, &format!("opcounters.{k}"), Bson::Int64(dg.op[i].load(Relaxed) as i64));
        }
        set_path(&mut d, "network.numRequests", Bson::Int64(dg.requests.load(Relaxed) as i64));
        for (i, k) in ["inserted", "deleted", "updated", "returned"].iter().enumerate() {
            set_path(&mut d, &format!("metrics.document.{k}"), Bson::Int64(dg.docs[i].load(Relaxed) as i64));
        }
        let mut cm = Document::new();
        for (k, (total, failed)) in dg.cmds.lock().unwrap().iter() {
            cm.insert(k.clone(), doc! { "failed": Bson::Int64(*failed as i64), "total": Bson::Int64(*total as i64) });
        }
        set_path(&mut d, "metrics.commands", Bson::Document(cm));
        let open = self.cursors_open() as i64;
        set_path(&mut d, "metrics.cursor.open.total", Bson::Int64(open));
        set_path(&mut d, "metrics.cursor.timedOut", Bson::Int64(crate::cmd::CURSORS_TIMED_OUT.load(Relaxed) as i64));
        // Live heap and what the allocator holds (jemalloc), and the memtable flushes started by age (mem-growth branch).
        let (alloc, resident) = crate::heap_stats();
        set_path(&mut d, "mem.heapAllocatedBytes", Bson::Int64(alloc as i64));
        set_path(&mut d, "mem.heapResidentBytes", Bson::Int64(resident as i64));
        set_path(&mut d, "mem.memtableAgeFlushes", Bson::Int64(tenant_bench::lifecycle::MEMTABLE_AGE_FLUSHES.load(Relaxed) as i64));
        set_path(&mut d, "mem.bits", Bson::Int32(64));
        if let Ok(s) = std::fs::read_to_string("/proc/self/statm") {
            let mut it = s.split_whitespace().filter_map(|x| x.parse::<i64>().ok());
            let (virt, res) = (it.next().unwrap_or(0), it.next().unwrap_or(0));
            set_path(&mut d, "mem.resident", Bson::Int32((res * 4096 / 1048576) as i32));
            set_path(&mut d, "mem.virtual", Bson::Int32((virt * 4096 / 1048576) as i32));
        }
        set_path(&mut d, "mem.supported", Bson::Boolean(true));
        set_path(&mut d, "featureCompatibilityVersion.major", Bson::Int32(7));
        if rs {
            let h = self.hello_doc();
            if let Some(Bson::Document(r)) = d.get_mut("repl") {
                for k in ["hosts", "setName", "setVersion", "primary", "me", "electionId"] {
                    if let Some(v) = h.get(k) {
                        r.insert(k, v.clone());
                    }
                }
            }
        }
        if crate::special_colls::SERIES_SEEN.load(Relaxed) {
            let mut out = Document::new();
            for (k, v) in std::mem::take(&mut d) {
                if k == "catalogStats" {
                    out.insert("bucketCatalog", crate::special_colls::bucket_catalog());
                }
                out.insert(k, v);
            }
            d = out;
        }
        // `section: 0` leaves a section out
        for (k, v) in cmd.iter().skip(1) {
            if matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)) || matches!(v, Bson::Double(f) if *f == 0.0) {
                d.remove(k);
            }
        }
        d.insert("ok", 1.0);
        Ok(d)
    }

    fn host_info(&self) -> Document {
        let mut d = template(HOST_INFO);
        if let Some(Bson::Document(s)) = d.get_mut("system") {
            s.insert("currentTime", Bson::DateTime(bson::DateTime::now()));
            s.insert("hostname", self.host_port());
        }
        d
    }

    fn cmd_line_opts(&self) -> Document {
        let argv: Vec<Bson> = std::env::args().map(Bson::String).collect();
        let args: Vec<String> = std::env::args().collect();
        let val = |k: &str| args.iter().position(|a| a == k).and_then(|i| args.get(i + 1)).cloned();
        let mut parsed = Document::new();
        if let Some(l) = val("--listen") {
            let (ip, port) = l.rsplit_once(':').unwrap_or(("127.0.0.1", "27017"));
            parsed.insert("net", doc! { "bindIp": ip, "port": port.parse::<i32>().unwrap_or(27017) });
        }
        if let Some(r) = val("--replica-set") {
            parsed.insert("replication", doc! { "replSet": r });
        }
        doc! { "argv": argv, "parsed": parsed, "ok": 1.0 }
    }

    fn conn_pool_stats(&self) -> Document {
        let mut d = zeroed(&template(CONN_POOL));
        d.insert("numClientConnections", self.diag.conns.lock().unwrap().len() as i32);
        d
    }

    fn list_commands(&self) -> Document {
        let t = template(LIST_COMMANDS);
        let mut cmds = Document::new();
        if let Ok(all) = t.get_document("commands") {
            for (k, v) in all {
                if IMPLEMENTED.contains(&k.as_str()) {
                    cmds.insert(k.clone(), v.clone());
                }
            }
        }
        doc! { "commands": cmds, "ok": 1.0 }
    }

    fn get_log(&self, db: &str, cmd: &Document) -> R<Document> {
        admin_only(db, "getLog")?;
        match cmd.get("getLog") {
            Some(Bson::String(s)) if s == "*" => Ok(doc! { "names": ["global", "startupWarnings"], "ok": 1.0 }),
            Some(Bson::String(s)) if s == "global" || s == "startupWarnings" => {
                let l = self.diag.log.lock().unwrap();
                let lines: Vec<Bson> = if s == "global" { l.1.iter().cloned().map(Bson::String).collect() } else { vec![] };
                Ok(doc! { "totalLinesWritten": if s == "global" { l.0 as i32 } else { 0 }, "log": lines, "ok": 1.0 })
            }
            Some(Bson::String(s)) => Err(MongoError::new(96, "OperationFailed", format!("No log named '{s}'"))),
            Some(v) => Err(bad_type(v, "getLog.getLog", "string")),
            None => Err(MongoError::new(2, "BadValue", "getLog".into())),
        }
    }

    // ---- currentOp / killOp / top ---------------------------------------------------------------

    pub(crate) fn current_op_docs(&self, own: Option<i64>, all: bool) -> Vec<Document> {
        let _ = own;
        let host = self.host_port();
        let ops = self.diag.ops.lock().unwrap();
        let conns = self.diag.conns.lock().unwrap();
        let mut out = Vec::new();
        for (id, o) in ops.iter() {
            let mut d = doc! { "type": "op", "host": host.clone(), "desc": format!("conn{}", o.conn), "connectionId": o.conn as i32 };
            if let Some(c) = conns.get(&o.conn) {
                d.insert("client", c.client.clone());
                if let Some(m) = &c.metadata {
                    d.insert("clientMetadata", m.clone());
                }
            }
            let run = o.start.elapsed();
            d.insert("active", true);
            d.insert("currentOpTime", now_string());
            d.insert("threaded", true);
            d.insert("opid", *id as i32);
            let mut command = o.cmd.clone();
            command.remove("$clusterTime");
            command.remove("$readPreference");
            command.remove("txnNumber");
            command.remove("writeConcern");
            command.remove("readConcern");
            if let Some(Bson::Document(l)) = command.get_mut("lsid") {
                l.remove("uid");
            }
            if let Some(Bson::Document(l)) = o.cmd.get("lsid") {
                let mut l = l.clone();
                if !l.contains_key("uid") {
                    l.insert("uid", Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: sha_empty() }));
                }
                d.insert("lsid", l);
            }
            d.insert("secs_running", Bson::Int64(run.as_secs() as i64));
            d.insert("microsecs_running", Bson::Int64(run.as_micros() as i64));
            let (op, ns) = op_kind(&o.name, &o.db, &o.cmd);
            d.insert("op", op);
            d.insert("ns", ns);
            d.insert("redacted", false);
            command.insert("$db", o.db.clone());
            d.insert("command", command);
            if matches!(o.name.as_str(), "find" | "aggregate" | "count" | "distinct" | "currentOp") {
                d.insert("queryFramework", "classic");
            }
            d.insert("numYields", 0);
            d.insert("locks", doc! {});
            d.insert("waitingForLock", false);
            d.insert("lockStats", doc! {});
            d.insert("waitingForFlowControl", false);
            d.insert("flowControlStats", doc! {});
            out.push(d);
        }
        if all {
            out.push(doc! { "type": "op", "host": host.clone(), "desc": "SessionKiller", "active": true, "currentOpTime": now_string() });
        }
        out
    }

    async fn current_op(&self, db: &str, cmd: &Document) -> R<Document> {
        admin_only(db, "currentOp")?;
        let all = bool_arg(cmd.get("$all"), false);
        let mut filter = Document::new();
        for (k, v) in cmd.iter().skip(1) {
            if !k.starts_with('$') && !matches!(k.as_str(), "lsid" | "txnNumber" | "comment" | "maxTimeMS" | "readConcern" | "writeConcern" | "apiVersion" | "apiStrict" | "apiDeprecationErrors") {
                filter.insert(k.clone(), v.clone());
            }
        }
        let mut inprog = Vec::new();
        for d in self.current_op_docs(None, all) {
            if crate::query::matches(&d, &filter)? {
                inprog.push(Bson::Document(d));
            }
        }
        Ok(doc! { "inprog": inprog, "ok": 1.0 })
    }

    fn kill_op(&self, db: &str, cmd: &Document) -> R<Document> {
        admin_only(db, "killOp")?;
        let Some(op) = cmd.get("op") else { return Err(MongoError::new(4, "NoSuchKey", "Missing expected field \"op\"".into())) };
        let id = match op {
            Bson::Int32(i) => *i as i64,
            Bson::Int64(i) => *i,
            Bson::Double(f) => *f as i64,
            _ => return Err(MongoError::new(14, "TypeMismatch", "Expected field \"op\" to have numeric type, but found string".replace("string", crate::update::type_name(op)))),
        };
        if let Some(o) = self.diag.ops.lock().unwrap().get(&id) {
            KILLED.lock().unwrap().push(id);
            o.kill.notify_one();
        }
        Ok(doc! { "info": "attempting to kill op", "ok": 1.0 })
    }

    fn top(&self, db: &str) -> R<Document> {
        admin_only(db, "top")?;
        let mut totals = doc! { "note": "all times in microseconds" };
        for (ns, t) in self.diag.top.lock().unwrap().iter() {
            let c = |c: &TopCell| Bson::Document(doc! { "time": c.time as i32, "count": c.count as i32 });
            totals.insert(ns.clone(), doc! { "total": c(&t.total), "readLock": c(&t.read), "writeLock": c(&t.write), "queries": c(&t.queries), "getmore": c(&t.getmore), "insert": c(&t.insert), "update": c(&t.update), "remove": c(&t.remove), "commands": c(&t.commands) });
        }
        Ok(doc! { "totals": totals, "ok": 1.0 })
    }

    // ---- read / write concern defaults, cluster parameters ---------------------------------------

    /// The write concern that `setDefaultRWConcern` set, if any.
    pub(crate) fn default_write_concern(&self) -> Option<Document> {
        self.diag.rwc.lock().unwrap().as_ref().and_then(|s| s.get_document("defaultWriteConcern").ok().cloned())
    }

    fn get_default_rwc(&self, _db: &str, cmd: &Document) -> R<Document> {
        if !self.is_rs() {
            return Err(MongoError::new(51300, "Location51300", "'getDefaultRWConcern' is not supported on standalone nodes.".into()));
        }
        let mut d = doc! { "defaultReadConcern": { "level": "local" }, "defaultWriteConcern": { "w": "majority", "wtimeout": 0 } };
        let stored = self.diag.rwc.lock().unwrap().clone();
        let mut wsrc = "implicit";
        let mut rsrc = "implicit";
        if let Some(s) = &stored {
            if let Some(v) = s.get("defaultWriteConcern") {
                d.insert("defaultWriteConcern", v.clone());
                wsrc = "global";
            }
            if let Some(v) = s.get("defaultReadConcern") {
                d.insert("defaultReadConcern", v.clone());
                rsrc = "global";
            }
            for k in ["updateOpTime", "updateWallClockTime"] {
                if let Some(v) = s.get(k) {
                    d.insert(k, v.clone());
                }
            }
        }
        d.insert("defaultWriteConcernSource", wsrc);
        d.insert("defaultReadConcernSource", rsrc);
        if bool_arg(cmd.get("inMemory"), false) {
            d.insert("inMemory", true);
        }
        d.insert("localUpdateWallClockTime", Bson::DateTime(bson::DateTime::now()));
        d.insert("ok", 1.0);
        Ok(d)
    }

    fn set_default_rwc(&self, db: &str, cmd: &Document) -> R<Document> {
        let _ = db;
        if !self.is_rs() {
            return Err(MongoError::new(51300, "Location51300", "'setDefaultRWConcern' is not supported on standalone nodes.".into()));
        }
        let wc = cmd.get("defaultWriteConcern");
        let rc = cmd.get("defaultReadConcern");
        if wc.is_none() && rc.is_none() {
            return Err(MongoError::new(2, "BadValue", "At least one of the \"defaultReadConcern\" or \"defaultWriteConcern\" fields must be present".into()));
        }
        let mut cur = self.diag.rwc.lock().unwrap().clone().unwrap_or_default();
        if let Some(w) = wc {
            let Bson::Document(w) = w else { return Err(bad_type(w, "setDefaultRWConcern.defaultWriteConcern", "object")) };
            if w.is_empty() {
                if cur.contains_key("defaultWriteConcern") {
                    return Err(MongoError::new(20, "IllegalOperation", "The global default write concern cannot be unset once it is set.".into()));
                }
            } else {
                match w.get("w") {
                    Some(Bson::Int32(0)) | Some(Bson::Int64(0)) => return Err(MongoError::new(2, "BadValue", "Unacknowledged write concern is not suitable for the default write concern".into())),
                    None => return Err(MongoError::new(2, "BadValue", "Default write concern must have 'w' field.".into())),
                    _ => {}
                }
                let mut w2 = w.clone();
                if !w2.contains_key("wtimeout") {
                    w2.insert("wtimeout", 0);
                }
                cur.insert("defaultWriteConcern", w2);
            }
        }
        if let Some(r) = rc {
            let Bson::Document(r) = r else { return Err(bad_type(r, "setDefaultRWConcern.defaultReadConcern", "object")) };
            if r.is_empty() {
                cur.remove("defaultReadConcern");
            } else {
                match r.get("level") {
                    Some(Bson::String(l)) if matches!(l.as_str(), "local" | "available" | "majority") => {}
                    Some(Bson::String(l)) => return Err(MongoError::new(2, "BadValue", format!("level: '{l}' is not suitable for the default read concern"))),
                    _ => {
                        let k = r.keys().next().cloned().unwrap_or_default();
                        return Err(MongoError::new(72, "InvalidOptions", format!("Unrecognized option in readConcern: {k}")));
                    }
                }
                cur.insert("defaultReadConcern", r.clone());
            }
        }
        let ts = bson::Timestamp { time: (bson::DateTime::now().timestamp_millis() / 1000) as u32, increment: 1 };
        cur.insert("updateOpTime", Bson::Timestamp(ts));
        cur.insert("updateWallClockTime", Bson::DateTime(bson::DateTime::now()));
        *self.diag.rwc.lock().unwrap() = Some(cur);
        self.get_default_rwc("admin", &doc! {})
    }

    fn get_cluster_parameter(&self, cmd: &Document) -> R<Document> {
        if !self.is_rs() {
            return Err(MongoError::new(20, "IllegalOperation", "getClusterParameter cannot be run on standalones".into()));
        }
        let names: Vec<String> = match cmd.get("getClusterParameter") {
            Some(Bson::String(s)) if s == "*" => vec!["changeStreamOptions".into()],
            Some(Bson::String(s)) => vec![s.clone()],
            Some(Bson::Array(a)) => a.iter().filter_map(|b| b.as_str().map(str::to_string)).collect(),
            Some(v) => return Err(bad_type(v, "getClusterParameter.getClusterParameter", "string or array")),
            None => vec![],
        };
        let mut out = Vec::new();
        for n in names {
            if n != "changeStreamOptions" {
                return Err(MongoError::new(4, "NoSuchKey", format!("Unknown server parameter: {n}")));
            }
            let mut p = doc! { "_id": "changeStreamOptions", "clusterParameterTime": Bson::Timestamp(bson::Timestamp { time: 0, increment: 0 }), "preAndPostImages": { "expireAfterSeconds": "off" } };
            if let Some(c) = self.diag.cluster.lock().unwrap().as_ref() {
                for (k, v) in c {
                    p.insert(k.clone(), v.clone());
                }
            }
            out.push(Bson::Document(p));
        }
        Ok(doc! { "clusterParameters": out, "ok": 1.0 })
    }

    fn set_cluster_parameter(&self, cmd: &Document) -> R<Document> {
        if !self.is_rs() {
            return Err(MongoError::new(20, "IllegalOperation", "setClusterParameter cannot be run on standalones".into()));
        }
        let Some(Bson::Document(p)) = cmd.get("setClusterParameter") else {
            return Err(MongoError::new(14, "TypeMismatch", "BSON field 'setClusterParameter' is the wrong type, expected type 'object'".into()));
        };
        let Some((name, val)) = p.iter().next() else { return Err(MongoError::new(2, "BadValue", "Cluster parameter name is missing".into())) };
        if name != "changeStreamOptions" {
            return Err(MongoError::new(4, "NoSuchKey", format!("Unknown server parameter: {name}")));
        }
        let Bson::Document(v) = val else { return Err(MongoError::new(2, "BadValue", "Cluster parameter value must be an object".into())) };
        let mut st = doc! { "clusterParameterTime": Bson::Timestamp(bson::Timestamp { time: (bson::DateTime::now().timestamp_millis() / 1000) as u32, increment: 1 }) };
        if let Some(Bson::Document(i)) = v.get("preAndPostImages") {
            let mut i = i.clone();
            if let Some(n) = i.get("expireAfterSeconds").and_then(|x| int_arg(Some(x))) {
                i.insert("expireAfterSeconds", Bson::Int64(n));
            }
            st.insert("preAndPostImages", i);
        }
        *self.diag.cluster.lock().unwrap() = Some(st);
        Ok(ok())
    }

    // ---- profiler --------------------------------------------------------------------------------

    fn profile(&self, db: &str, cmd: &Document) -> R<Document> {
        let wrong = |field: &str, v: &Bson| MongoError::new(14, "TypeMismatch", format!("BSON field 'profile.{field}' is the wrong type '{}', expected types '[long, int, decimal, double']", crate::update::type_name(v)));
        let level = match cmd.get("profile") {
            Some(Bson::Int32(i)) => *i as i64,
            Some(Bson::Int64(i)) => *i,
            Some(Bson::Double(f)) => *f as i64,
            Some(v) => return Err(wrong("profile", v)),
            None => -1,
        };
        let mut g = self.diag.prof_global.lock().unwrap();
        let mut p = self.diag.prof.lock().unwrap();
        let cur = p.entry(db.to_string()).or_default();
        let before = (cur.clone(), g.0, g.1);
        if let Some(v) = cmd.get("slowms") {
            g.0 = match v {
                Bson::Int32(i) => *i as i64,
                Bson::Int64(i) => *i,
                Bson::Double(f) => *f as i64,
                other => return Err(wrong("slowms", other)),
            };
        }
        if let Some(v) = cmd.get("sampleRate") {
            let f = match v {
                Bson::Double(f) => *f,
                Bson::Int32(i) => *i as f64,
                Bson::Int64(i) => *i as f64,
                other => return Err(wrong("sampleRate", other)),
            };
            if !(0.0..=1.0).contains(&f) {
                return Err(MongoError::new(2, "BadValue", "'sampleRate' must be between 0.0 and 1.0 inclusive".to_string()));
            }
            g.1 = f;
        }
        match cmd.get("filter") {
            Some(Bson::Document(f)) => {
                crate::query::probe_filter(f)?;
                cur.filter = Some(f.clone());
            }
            Some(Bson::String(s)) if s == "unset" => cur.filter = None,
            Some(_) => return Err(MongoError::new(2, "BadValue", "Expected an object, or the string 'unset'.".to_string())),
            None => {}
        }
        let mut reply = doc! { "was": before.0.level, "slowms": before.1 as i32, "sampleRate": before.2 };
        if let Some(f) = &before.0.filter {
            reply.insert("filter", f.clone());
        }
        if (0..=2).contains(&level) {
            cur.level = level as i32;
        }
        if before.0.filter.is_some() || cmd.contains_key("filter") {
            reply.insert("note", "When a filter expression is set, slowms and sampleRate are not used for profiling and slow-query log lines.");
        }
        reply.insert("ok", 1.0);
        Ok(reply)
    }

    /// After a command ran: write its profile entry when the database profiles.
    pub(crate) async fn profile_record(&self, db: &str, name: &str, cmd: &Document, r: &R<Document>, micros: i64, conn: i64) {
        let p = self.diag.prof_of(db);
        if p.level == 0 || db == "admin" && name == "profile" || matches!(name, "profile" | "hello" | "isMaster" | "ismaster" | "ping" | "saslStart" | "saslContinue" | "endSessions") {
            return;
        }
        let ns_target = cmd.get(name).and_then(|v| v.as_str()).unwrap_or("");
        if ns_target == "system.profile" || (name == "getMore" && cmd.get_str("collection").ok() == Some("system.profile")) {
            return;
        }
        let millis = micros / 1000;
        let (slowms, sample) = *self.diag.prof_global.lock().unwrap();
        if p.level == 1 && p.filter.is_none() && millis < slowms {
            return;
        }
        let (op, ns) = op_kind(name, db, cmd);
        let reply = r.as_ref().ok();
        let count = |k: &str| reply.and_then(|d| d.get(k)).and_then(|v| int_arg(Some(v))).unwrap_or(0);
        let batch_len = reply.and_then(|d| d.get_document("cursor").ok()).map(|c| c.get_array("firstBatch").or_else(|_| c.get_array("nextBatch")).map(|a| a.len() as i64).unwrap_or(0));
        let nreturned = batch_len.unwrap_or_else(|| count("n"));
        let mut command = crate::auth::redact_command(name, cmd);
        command.remove("$clusterTime");
        command.remove("$readPreference");
        command.insert("$db", db);
        // document sequences are not shown by the profiler
        for k in self.diag.seq.lock().unwrap().get(&conn).cloned().unwrap_or_default() {
            if k == "documents" {
                command.remove(&k);
            }
        }
        if matches!(name, "update" | "delete") {
            // the profiler shows the statement, not the command
            command = cmd.get_array(if name == "update" { "updates" } else { "deletes" }).ok().and_then(|a| a.first()).and_then(|b| b.as_document()).cloned().unwrap_or_default();
            if name == "update" {
                for k in ["multi", "upsert"] {
                    if !command.contains_key(k) {
                        command.insert(k, false);
                    }
                }
            }
        }
        let client = self.diag.conns.lock().unwrap().get(&conn).map(|c| c.client.clone()).unwrap_or_default();
        let stmt_filter = |d: &Document| d.get_document("q").cloned().unwrap_or_default();
        let filter = match name {
            "find" => cmd.get_document("filter").cloned().unwrap_or_default(),
            "update" | "delete" => cmd.get_array(if name == "update" { "updates" } else { "deletes" }).ok().and_then(|a| a.first()).and_then(|b| b.as_document()).map(stmt_filter).unwrap_or_default(),
            _ => Document::new(),
        };
        let plan = if filter.len() == 1 && filter.get("_id").map(|v| !matches!(v, Bson::Document(_))).unwrap_or(false) { "IDHACK" } else { "COLLSCAN" };
        let hash = |salt: &str| format!("{:08X}", md5_hex(format!("{salt}{}", crate::update::fmt_doc(&filter)).as_bytes())[..8].chars().fold(0_u32, |a, c| a.wrapping_mul(31).wrapping_add(c as u32)));
        let exhausted = reply.and_then(|d| d.get_document("cursor").ok()).map(|c| matches!(c.get("id"), Some(Bson::Int64(0)))).unwrap_or(true);
        let locks_r = || Bson::Document(doc! { "FeatureCompatibilityVersion": { "acquireCount": { "r": Bson::Int64(1) } }, "Global": { "acquireCount": { "r": Bson::Int64(1) } } });
        let locks_w = || Bson::Document(doc! { "ParallelBatchWriterMode": { "acquireCount": { "r": Bson::Int64(1) } }, "FeatureCompatibilityVersion": { "acquireCount": { "w": Bson::Int64(1) } }, "ReplicationStateTransition": { "acquireCount": { "w": Bson::Int64(1) } }, "Global": { "acquireCount": { "w": Bson::Int64(1) } }, "Database": { "acquireCount": { "w": Bson::Int64(1) } }, "Collection": { "acquireCount": { "w": Bson::Int64(1) } } });
        let exec = |stage: &str, n: i64| Bson::Document(doc! { "stage": stage, "nReturned": Bson::Int64(n), "executionTimeMillisEstimate": Bson::Int64(0), "works": Bson::Int64(n + 1), "advanced": Bson::Int64(n), "needTime": Bson::Int64(0), "needYield": Bson::Int64(0), "saveState": 0, "restoreState": 0, "isEOF": 1 });
        let resp_len = reply.map(|d| d.to_vec().map(|v| v.len()).unwrap_or(0)).unwrap_or(0) as i32;
        let mut e = doc! { "op": op, "ns": ns, "command": command };
        match (op, name) {
            ("query", _) => e.extend(doc! { "keysExamined": 0_i64, "docsExamined": nreturned, "nBatches": 1, "cursorExhausted": exhausted, "numYield": 0, "nreturned": nreturned as i32, "queryHash": hash("q"), "planCacheKey": hash("p"), "queryFramework": "classic", "locks": locks_r(), "flowControl": {}, "responseLength": resp_len, "protocol": "op_msg", "cpuNanos": Bson::Int64(micros * 1000), "millis": millis as i32, "planSummary": plan, "planningTimeMicros": Bson::Int64(0), "execStats": exec(if plan == "IDHACK" { "IDHACK" } else { "COLLSCAN" }, nreturned) }),
            ("insert", _) => e.extend(doc! { "ninserted": count("n") as i32, "keysInserted": Bson::Int64(count("n") * 2), "numYield": 0, "locks": locks_w(), "flowControl": { "acquireCount": Bson::Int64(1) }, "responseLength": resp_len, "protocol": "op_msg", "cpuNanos": Bson::Int64(micros * 1000), "millis": millis as i32 }),
            ("update", _) => e.extend(doc! { "keysExamined": 0_i64, "docsExamined": count("n"), "nMatched": count("n") as i32, "nModified": count("nModified") as i32, "nUpserted": reply.and_then(|d| d.get_array("upserted").ok()).map(|a| a.len()).unwrap_or(0) as i32, "keysInserted": 0, "keysDeleted": 0, "numYield": 0, "locks": locks_w(), "flowControl": { "acquireCount": Bson::Int64(1) }, "cpuNanos": Bson::Int64(micros * 1000), "millis": millis as i32, "planSummary": plan, "planningTimeMicros": Bson::Int64(0), "execStats": exec("UPDATE", 0) }),
            ("remove", _) => e.extend(doc! { "keysExamined": 0_i64, "docsExamined": count("n"), "ndeleted": count("n") as i32, "keysDeleted": Bson::Int64(count("n") * 2), "numYield": 0, "locks": locks_w(), "flowControl": { "acquireCount": Bson::Int64(1) }, "cpuNanos": Bson::Int64(micros * 1000), "millis": millis as i32, "planSummary": plan, "planningTimeMicros": Bson::Int64(0), "execStats": exec("DELETE", 0) }),
            _ if batch_len.is_some() || matches!(name, "count" | "distinct") => e.extend(doc! { "keysExamined": 0_i64, "docsExamined": nreturned, "cursorExhausted": exhausted, "numYield": 0, "nreturned": nreturned as i32, "queryHash": hash("q"), "planCacheKey": hash("p"), "queryFramework": "classic", "locks": locks_r(), "flowControl": {}, "responseLength": resp_len, "protocol": "op_msg", "cpuNanos": Bson::Int64(micros * 1000), "millis": millis as i32, "planSummary": plan, "planningTimeMicros": Bson::Int64(0) }),
            _ => e.extend(doc! { "numYield": 0, "locks": locks_w(), "flowControl": { "acquireCount": Bson::Int64(1) }, "responseLength": resp_len, "protocol": "op_msg", "cpuNanos": Bson::Int64(micros * 1000), "millis": millis as i32 }),
        }
        e.insert("ts", Bson::DateTime(bson::DateTime::now()));
        e.insert("client", client);
        e.insert("allUsers", Vec::<Bson>::new());
        e.insert("user", "");
        if let Err(err) = r {
            e.insert("ok", 0);
            e.insert("errMsg", err.msg.clone());
            e.insert("errName", err.name.to_string());
            e.insert("errCode", err.code);
        }
        if let Some(f) = &p.filter {
            let mut probe = e.clone();
            probe.insert("millis", millis as i32);
            if !crate::query::matches(&probe, f).unwrap_or(false) {
                return;
            }
        } else if sample < 1.0 && rand::random::<f64>() >= sample {
            return;
        }
        let _ = self.write_profile(db, e).await;
    }

    async fn write_profile(&self, db: &str, entry: Document) -> R<()> {
        let names = self.dbs.lock(db).await.collections().await?;
        if !names.iter().any(|n| n == "system.profile") {
            self.create(db, &doc! { "create": "system.profile", "capped": true, "size": 1048576 }).await?;
        }
        let mut none = None;
        self.insert(db, &doc! { "insert": "system.profile", "documents": [entry] }, &mut none).await?;
        Ok(())
    }

    // ---- dbHash / dataSize / validateDBMetadata -----------------------------------------------------

    async fn db_hash(&self, db: &str, cmd: &Document) -> R<Document> {
        let started = Instant::now();
        let want: Option<Vec<String>> = match cmd.get("collections") {
            None => None,
            Some(Bson::Array(a)) => Some(a.iter().filter_map(|b| b.as_str().map(str::to_string)).collect()),
            Some(_) => None,
        };
        let mut g = self.dbs.lock(db).await;
        let mut names = g.collections().await?;
        names.sort();
        let mut colls = Document::new();
        let mut uuids = Document::new();
        let mut capped = Vec::new();
        let mut all = String::new();
        for n in names {
            if let Some(w) = &want {
                if !w.contains(&n) {
                    continue;
                }
            }
            if n == "system.profile" {
                continue;
            }
            let cap = g.coll_opts(&n).await?.map(|o| o.cap.is_some()).unwrap_or(false);
            let mut buf: Vec<u8> = Vec::new();
            if cap {
                // a capped collection is hashed in natural order
                let np = crate::natural::prefix(&n);
                let end = crate::keyenc::prefix_end(&np);
                for (_, id_enc) in g.scan_range(&np, end.as_deref(), false, None, 1 << 20).await? {
                    let mut k = crate::layout::doc_prefix(&n);
                    k.extend(id_enc.iter());
                    if let Some(v) = g.get_live(&k).await? {
                        buf.extend(crate::natural::bson_part(&v));
                    }
                }
                capped.push(Bson::String(n.clone()));
            } else {
                for (_, v) in g.scan(&crate::layout::doc_prefix(&n)).await? {
                    buf.extend(crate::natural::bson_part(&v));
                }
            }
            let h = md5_hex(&buf);
            all.push_str(&h);
            colls.insert(n.clone(), h);
            uuids.insert(n.clone(), Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: g.collection_uuid(&n).await? }));
        }
        Ok(doc! { "host": self.host_port(), "collections": colls, "capped": capped, "uuids": uuids, "md5": md5_hex(all.as_bytes()), "timeMillis": started.elapsed().as_millis() as i32, "ok": 1.0 })
    }

    async fn data_size(&self, cmd: &Document) -> R<Document> {
        let started = Instant::now();
        let ns = match cmd.get("dataSize") {
            Some(Bson::String(s)) => s.clone(),
            Some(v) => return Err(bad_type(v, "dataSize", "string")),
            None => return Err(MongoError::new(2, "BadValue", "no ns".into())),
        };
        let Some((db, coll)) = ns.split_once('.') else {
            return Err(MongoError::new(73, "InvalidNamespace", format!("Namespace {ns} is not a valid collection name")));
        };
        let estimate = bool_arg(cmd.get("estimate"), false);
        let (min, max) = (cmd.get("min"), cmd.get("max"));
        if min.is_some() && max.is_none() {
            return Err(MongoError::new(2, "BadValue", "Max must be set if min is set.".into()));
        }
        let g = self.dbs.read(db).await?;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Ok(doc! { "size": Bson::Int64(0), "numObjects": Bson::Int64(0), "millis": started.elapsed().as_millis() as i32, "ok": 1.0 });
        }
        let mut range: Option<(Vec<String>, Document, Document)> = None;
        if let (Some(Bson::Document(lo)), Some(Bson::Document(hi))) = (min, max) {
            let kp: Document = match cmd.get("keyPattern") {
                Some(Bson::Document(k)) => k.clone(),
                _ => lo.keys().map(|k| (k.clone(), Bson::Int32(1))).collect(),
            };
            let indexes = g.indexes(coll).await?;
            let fields: Vec<String> = kp.keys().cloned().collect();
            let has = fields == ["_id"] || indexes.iter().any(|i| i.spec.get_document("key").map(|k| k.keys().cloned().collect::<Vec<_>>() == fields).unwrap_or(false));
            if !has {
                return Err(MongoError::new(96, "OperationFailed", "Couldn't find valid index containing key pattern".into()));
            }
            range = Some((fields, lo.clone(), hi.clone()));
        }
        let (mut size, mut n) = (0_i64, 0_i64);
        for (_, v) in g.scan(&crate::layout::doc_prefix(coll)).await? {
            let raw = crate::natural::bson_part(&v);
            if let Some((fields, lo, hi)) = &range {
                let d = Document::from_reader(raw)?;
                let key = |f: &str| crate::query::path_values(&d, f).into_iter().next().cloned().unwrap_or(Bson::Null);
                let bound = |b: &Document| -> Vec<Bson> { fields.iter().map(|f| b.get(f).cloned().unwrap_or(Bson::Null)).collect() };
                let k: Vec<Bson> = fields.iter().map(|f| key(f)).collect();
                let cmp = |a: &[Bson], b: &[Bson]| {
                    for (x, y) in a.iter().zip(b) {
                        let o = crate::query::compare(x, y);
                        if o != std::cmp::Ordering::Equal {
                            return o;
                        }
                    }
                    std::cmp::Ordering::Equal
                };
                if cmp(&k, &bound(lo)) == std::cmp::Ordering::Less || cmp(&k, &bound(hi)) != std::cmp::Ordering::Less {
                    continue;
                }
            }
            size += raw.len() as i64;
            n += 1;
        }
        Ok(doc! { "size": Bson::Int64(size), "numObjects": Bson::Int64(n), "millis": started.elapsed().as_millis() as i32, "estimate": estimate, "ok": 1.0 })
    }

    async fn validate_db_metadata(&self, cmd: &Document) -> R<Document> {
        let Some(Bson::Document(api)) = cmd.get("apiParameters") else {
            return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field 'validateDBMetadata.apiParameters' is missing but a required field".into()));
        };
        if !api.contains_key("version") {
            return Err(MongoError::new(40414, "IDLFailedToParse", "BSON field 'validateDBMetadata.apiParameters.version' is missing but a required field".into()));
        }
        let strict = match api.get("strict") {
            None => false,
            Some(Bson::Boolean(b)) => *b,
            Some(v) => return Err(MongoError::new(14, "TypeMismatch", format!("Field 'strict' should be a boolean value, but found: {}", crate::update::type_name(v)))),
        };
        let only_db = cmd.get_str("db").ok().map(str::to_string);
        let only_coll = cmd.get_str("collection").ok().map(str::to_string);
        let dbs: Vec<String> = match &only_db {
            Some(d) => vec![d.clone()],
            None => self.dbs.list_databases().await?,
        };
        let mut errs = Vec::new();
        if strict {
            for d in dbs {
                let g = self.dbs.read(&d).await?;
                let mut names = g.collections().await?;
                names.sort();
                for c in names {
                    if only_coll.as_deref().map(|o| o != c).unwrap_or(false) {
                        continue;
                    }
                    for i in g.indexes(&c).await? {
                        let disallowed = i.spec.get_bool("sparse").unwrap_or(false) || i.spec.get_document("key").map(|k| k.values().any(|v| v.as_str() == Some("text"))).unwrap_or(false);
                        if disallowed {
                            errs.push(Bson::Document(doc! { "ns": format!("{d}.{c}"), "code": 323, "codeName": "APIStrictError", "errmsg": format!("The index with name {} is not allowed in API version 1.", i.name) }));
                        }
                    }
                }
            }
        }
        Ok(doc! { "apiVersionErrors": errs, "ok": 1.0 })
    }

    // ---- plan cache, reIndex, compact, filemd5, setIndexCommitQuorum --------------------------------------

    async fn plan_cache(&self, db: &str, name: &str, cmd: &Document) -> R<Document> {
        let coll = match cmd.get(name) {
            Some(Bson::String(s)) if !s.is_empty() => s.as_str(),
            Some(v) => return Err(MongoError::new(73, "InvalidNamespace", format!("collection name has invalid type {}", crate::update::type_name(v)))),
            None => return Err(MongoError::new(73, "InvalidNamespace", "no collection".into())),
        };
        let key = format!("{db}.{coll}");
        let exists = self.dbs.lock(db).await.collections().await?.iter().any(|n| n == coll);
        match name {
            "planCacheListFilters" => {
                let f = self.diag.filters.lock().unwrap().get(&key).cloned().unwrap_or_default();
                Ok(doc! { "filters": f.into_iter().map(Bson::Document).collect::<Vec<_>>(), "ok": 1.0 })
            }
            "planCacheSetFilter" => {
                let Some(Bson::Document(q)) = cmd.get("query") else { return Err(MongoError::new(2, "BadValue", "required field query missing".into())) };
                let Some(Bson::Array(ix)) = cmd.get("indexes") else { return Err(MongoError::new(2, "BadValue", "required field indexes missing".into())) };
                if ix.is_empty() {
                    return Err(MongoError::new(2, "BadValue", "required field indexes must contain at least one index".into()));
                }
                if !exists {
                    return Err(MongoError::new(2, "BadValue", "no such collection".into()));
                }
                let entry = doc! { "query": q.clone(), "sort": cmd.get_document("sort").cloned().unwrap_or_default(), "projection": cmd.get_document("projection").cloned().unwrap_or_default(), "indexes": ix.clone() };
                let mut m = self.diag.filters.lock().unwrap();
                let l = m.entry(key).or_default();
                l.retain(|e| e.get("query") != entry.get("query") || e.get("sort") != entry.get("sort") || e.get("projection") != entry.get("projection"));
                l.push(entry);
                Ok(ok())
            }
            "planCacheClearFilters" => {
                let mut m = self.diag.filters.lock().unwrap();
                match cmd.get("query") {
                    Some(Bson::Document(q)) => {
                        if let Some(l) = m.get_mut(&key) {
                            l.retain(|e| e.get("query") != Some(&Bson::Document(q.clone())));
                        }
                    }
                    _ => {
                        m.remove(&key);
                    }
                }
                Ok(ok())
            }
            _ => Ok(ok()),
        }
    }

    async fn re_index(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "reIndex")?;
        if self.is_rs() {
            return Err(MongoError::new(20, "IllegalOperation", format!("reIndex is only allowed on a standalone mongod instance. Cannot reIndex '{db}.{coll}' while replication is active")));
        }
        let mut g = self.dbs.lock(db).await;
        if !g.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found("collection does not exist".into()));
        }
        let indexes = self.load_indexes(&mut g, db, coll).await?;
        let mut specs = vec![Bson::Document(doc! { "v": 2, "key": { "_id": 1 }, "name": "_id_" })];
        for i in indexes.iter().filter(|i| i.state == crate::index::State::Ready) {
            specs.push(Bson::Document(i.spec.clone()));
        }
        let n = specs.len() as i32;
        Ok(doc! { "nIndexesWas": n, "nIndexes": n, "indexes": specs, "ok": 1.0 })
    }

    async fn compact(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "compact")?;
        if self.is_rs() && !bool_arg(cmd.get("force"), false) {
            return Err(MongoError::new(20, "IllegalOperation", "will not run compact on an active replica set primary as this will slow down other running operations. use force:true to force".into()));
        }
        if !self.dbs.lock(db).await.collections().await?.iter().any(|n| n == coll) {
            return Err(MongoError::namespace_not_found("collection does not exist".into()));
        }
        Ok(doc! { "bytesFreed": 0, "ok": 1.0 })
    }

    async fn filemd5(&self, db: &str, cmd: &Document) -> R<Document> {
        let root = cmd.get_str("root").unwrap_or("fs");
        let chunks = format!("{root}.chunks");
        let Some(files_id) = cmd.get("filemd5") else { return Err(MongoError::new(2, "BadValue", "filemd5 requires an _id".into())) };
        let mut g = self.dbs.lock(db).await;
        let mut n = 0;
        let mut buf: Vec<u8> = Vec::new();
        if g.collections().await?.iter().any(|c| *c == chunks) {
            let mut docs = Vec::new();
            for (_, v) in g.scan(&crate::layout::doc_prefix(&chunks)).await? {
                let d = Document::from_reader(crate::natural::bson_part(&v))?;
                if d.get("files_id") == Some(files_id) {
                    docs.push(d);
                }
            }
            docs.sort_by_key(|d| d.get("n").and_then(|v| int_arg(Some(v))).unwrap_or(0));
            for d in docs {
                if let Some(Bson::Binary(b)) = d.get("data") {
                    buf.extend(&b.bytes);
                }
                n += 1;
            }
        }
        Ok(doc! { "numChunks": n, "md5": md5_hex(&buf), "ok": 1.0 })
    }

    async fn set_index_commit_quorum(&self, db: &str, cmd: &Document) -> R<Document> {
        let coll = collection_name(cmd, "setIndexCommitQuorum")?;
        let names: Vec<String> = cmd.get_array("indexNames").map(|a| a.iter().filter_map(|b| b.as_str().map(str::to_string)).collect()).unwrap_or_default();
        let _ = db;
        let _ = names;
        Err(MongoError::new(27, "IndexNotFound", format!("Cannot find an index build on collection '{db}.{coll}' with the provided index names")))
    }

    fn fsync(&self, db: &str, cmd: &Document) -> R<Document> {
        admin_only(db, "fsync")?;
        if cmd.get("lock").map(|v| !matches!(v, Bson::Boolean(false) | Bson::Null | Bson::Undefined | Bson::Int32(0) | Bson::Int64(0)) && !matches!(v, Bson::Double(f) if *f == 0.0)).unwrap_or(false) {
            let n = self.diag.fsync_locks.fetch_add(1, Relaxed) + 1;
            return Ok(doc! { "info": "now locked against writes, use db.fsyncUnlock() to unlock", "lockCount": Bson::Int64(n), "seeAlso": "http://dochub.mongodb.org/core/fsynccommand", "ok": 1.0 });
        }
        Ok(doc! { "numFiles": 1, "ok": 1.0 })
    }

    fn fsync_unlock(&self, db: &str) -> R<Document> {
        admin_only(db, "fsyncUnlock")?;
        let cur = self.diag.fsync_locks.load(Relaxed);
        if cur <= 0 {
            return Err(MongoError::new(20, "IllegalOperation", "fsyncUnlock called when not locked".into()));
        }
        let n = self.diag.fsync_locks.fetch_sub(1, Relaxed) - 1;
        Ok(doc! { "info": "fsyncUnlock completed", "lockCount": Bson::Int64(n), "ok": 1.0 })
    }
}

fn sha_empty() -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(b"").to_vec()
}

fn op_kind(name: &str, db: &str, cmd: &Document) -> (&'static str, String) {
    let coll = cmd.get(name).and_then(|v| v.as_str()).map(str::to_string);
    let ns_cmd = format!("{db}.$cmd");
    match name {
        "insert" => ("insert", format!("{db}.{}", coll.unwrap_or_default())),
        "update" => ("update", format!("{db}.{}", coll.unwrap_or_default())),
        "delete" => ("remove", format!("{db}.{}", coll.unwrap_or_default())),
        "find" => ("query", format!("{db}.{}", coll.unwrap_or_default())),
        "getMore" => ("getmore", format!("{db}.{}", cmd.get_str("collection").unwrap_or_default())),
        "currentOp" => ("command", "admin.$cmd.aggregate".to_string()),
        "aggregate" if !matches!(cmd.get("aggregate"), Some(Bson::String(_))) => ("command", format!("{db}.$cmd.aggregate")),
        _ => match coll {
            Some(c) => ("command", format!("{db}.{c}")),
            None => ("command", ns_cmd),
        },
    }
}

fn start_session() -> Document {
    let id: [u8; 16] = rand::random();
    let mut b = id.to_vec();
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    doc! { "id": { "id": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: b }) }, "timeoutMinutes": 30, "ok": 1.0 }
}

fn kill_all_sessions(name: &str, cmd: &Document) -> R<Document> {
    match cmd.get(name) {
        Some(Bson::Array(_)) => Ok(ok()),
        Some(v) => Err(MongoError::new(14, "TypeMismatch", format!("BSON field 'KillAllSessionsCmd.{name}' is the wrong type '{}', expected type 'array'", crate::update::type_name(v)).replace("KillAllSessionsCmd", if name == "killAllSessions" { "KillAllSessionsCmd" } else { "KillAllSessionsByPatternCmd" }))),
        None => Ok(ok()),
    }
}

/// The commands this server runs (what `listCommands` lists).
pub const IMPLEMENTED: &[&str] = &[
    "abortTransaction", "aggregate", "authenticate", "buildInfo", "buildinfo", "cloneCollectionAsCapped", "collMod", "collStats", "commitTransaction", "compact", "connPoolStats", "connectionStatus", "convertToCapped", "count", "create", "createIndexes", "createRole", "createUser",
    "currentOp", "dataSize", "dbHash", "dbStats", "delete", "distinct", "drop", "dropAllRolesFromDatabase", "dropAllUsersFromDatabase", "dropDatabase", "dropIndexes", "dropRole", "dropUser", "endSessions", "explain", "features", "filemd5", "find", "findAndModify", "findandmodify", "fsync",
    "fsyncUnlock", "getCmdLineOpts", "getDefaultRWConcern", "getClusterParameter", "getLog", "getMore", "getParameter", "grantPrivilegesToRole", "grantRolesToRole", "grantRolesToUser", "hello", "hostInfo", "insert", "invalidateUserCache", "isMaster", "killAllSessions",
    "killAllSessionsByPattern", "killCursors", "killOp", "killSessions", "listCollections", "listCommands", "listDatabases", "listIndexes", "lockInfo", "logRotate", "logout", "mapReduce", "ping", "planCacheClear", "planCacheClearFilters", "planCacheListFilters", "planCacheSetFilter",
    "profile", "reIndex", "refreshSessions", "renameCollection", "revokePrivilegesFromRole", "revokeRolesFromRole", "revokeRolesFromUser", "rolesInfo", "saslContinue", "saslStart", "serverStatus", "setClusterParameter", "setDefaultRWConcern", "setIndexCommitQuorum", "setParameter",
    "startSession", "top", "update", "updateRole", "updateUser", "usersInfo", "validate", "validateDBMetadata", "whatsmyuri",
];
