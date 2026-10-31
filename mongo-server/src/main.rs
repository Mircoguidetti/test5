//! A MongoDB wire-protocol server whose databases are slatedb tenants on object storage.
//!
//! Each MongoDB database is one tenant at `tenants/{dbname}` managed by the results3 lifecycle
//! (reader wake, upgrade on first write, clean close after an idle period, `_clean` marker,
//! standalone compaction and GC).

/// jemalloc instead of the system allocator (mem-growth branch). Measured with 50 databases at 2 writes/s on MinIO: glibc's
/// malloc held ~5.5 KB of RSS per write for ~1.5 KB of live heap and gave nothing back when the databases closed;
/// jemalloc held ~1.9 KB per write. `background_thread` returns freed pages to the system while the process is idle
/// (without it, pages were only purged by later allocations).
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: &[u8] = b"background_thread:true\0";

mod agg;
mod auth;
mod authz;
mod scram;
mod tls;
mod agg_ext;
mod agg_more;
mod json_schema;
mod capped;
mod capped_cmds;
mod changes;
mod cmd;
mod cmd_changes;
mod collation;
mod collmod_index;
mod collopts;
mod coll;
mod cmd_data;
mod cmd_index;
mod cmd_mapreduce;
mod ctl;
mod cs_events;
mod dbs;
mod diag;
mod fields;
mod find_ext;
mod dsum;
mod error;
mod fair;
mod expr;
mod index;
mod js;
mod keyenc;
mod layout;
mod lease;
mod localcas;
mod faults;
mod locallog;
mod natural;
mod netguard;
mod params;
mod projfind;
mod query;
mod geo;
mod geo_query;
mod rc;
mod regex;
mod retry;
mod search;
mod special_colls;
mod stable_api;
mod stats_scale;
mod search_more;
mod text;
mod wild;
mod ttl;
mod txn;
mod update;
mod validation;
mod views;
mod admin_cmds;
mod repl_cmds;
mod search_idx;
mod wc;
mod wire;
mod xdb;

use anyhow::Result;
use clap::Parser;
use cmd::Server;
use dbs::Dbs;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;
use tenant_bench::counting::CountingStore;
use tenant_bench::lifecycle::{CompactionQueue, Lifecycle, Maintenance};
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};

#[derive(Parser, Debug)]
struct Args {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:27018")]
    listen: String,
    /// Close a database's tenant after this many idle seconds.
    #[arg(long, default_value_t = 60)]
    idle_secs: u64,
    /// S3 endpoint URL, only for S3-compatible services such as MinIO (also read from AWS_ENDPOINT). Empty: AWS itself.
    /// An `http://` endpoint switches plain HTTP on.
    #[arg(long, default_value = "")]
    endpoint: String,
    /// S3 region (also read from AWS_REGION or AWS_DEFAULT_REGION). With `--store s3` one of the three must be set: the engine
    /// has no region of its own.
    #[arg(long)]
    region: Option<String>,
    #[arg(long, default_value = "bench")]
    bucket: String,
    /// Do not run the background compaction and GC loops (also disables the L0 guard).
    #[arg(long)]
    no_maintenance: bool,
    /// Tenant lease time-to-live; leases of open tenants are renewed every third of it, and a crashed
    /// process's tenants can be taken over once it has passed.
    #[arg(long, default_value_t = 30)]
    lease_ttl_secs: u64,
    /// Lease holder id (default: a unique id per process start).
    #[arg(long)]
    holder: Option<String>,
    /// Remove expired TTL documents of tenants that are open as writers this often.
    #[arg(long, default_value_t = 60)]
    ttl_sweep_secs: u64,
    /// Wake sleeping tenants to remove their expired documents (off by default). Off: expired documents are hidden on
    /// every read and removed at the tenant's next natural writer session; nothing is woken, no expiry hint is
    /// written at close and no maintenance pass lists anything. On: a maintenance pass (one LIST per pass) wakes
    /// tenants whose documents are due, each at most once per `--ttl-min-gap-secs`.
    #[arg(long)]
    ttl_wake_sleeping: bool,
    /// With `--ttl-wake-sleeping`: every this often, look for sleeping tenants with expired documents and wake due ones.
    #[arg(long, default_value_t = 60)]
    ttl_maintenance_secs: u64,
    /// Do not wake the same sleeping tenant for expiry more often than this.
    #[arg(long, default_value_t = 3600)]
    ttl_min_gap_secs: u64,
    /// At most this many sleeping tenants are woken for expiry per maintenance pass.
    #[arg(long, default_value_t = 20)]
    ttl_max_wakes: usize,
    /// Replica-set name reported by `hello`. Drivers allow multi-document transactions and change streams only on
    /// replica sets, so set it (with `--advertise`) when an application needs them; empty (default) is a standalone
    /// server, which is what clients that connect by another host name or address expect.
    #[arg(long, default_value = "")]
    replica_set: String,
    /// Address advertised in `hello.hosts` / `me`; must match the host:port clients connect to
    /// (default `localhost:<listen port>`).
    #[arg(long)]
    advertise: Option<String>,
    /// Do not run the TTL sweeps (expired documents stay hidden but are not removed).
    #[arg(long)]
    no_ttl: bool,
    /// WAL flush interval in milliseconds. 0 (default) keeps the baseline: slatedb's 100 ms periodic flush plus an
    /// explicit flush after every write batch, so a write is acknowledged as soon as its WAL object is durable. A
    /// value above 0 sets the periodic interval and drops the explicit flush: a write is acknowledged (durable) at
    /// the next flush, up to one interval later, and concurrent writes to a tenant share one WAL object.
    #[arg(long, default_value_t = 0)]
    wal_flush_ms: u64,
    /// Acknowledge writes before they are durable (they become durable at the next periodic flush). Unsafe: a crash
    /// can lose acknowledged writes. For measuring the durability trade-off only; use with --wal-flush-ms.
    #[arg(long)]
    ack_before_durable: bool,
    /// Object store: `s3` (the endpoint above), `local` (a directory, see --local-dir) or `memory`.
    #[arg(long, default_value = "s3")]
    store: String,
    #[arg(long, default_value = "/tmp/mongo-server-store")]
    local_dir: String,
    /// Cold wakes allowed at once; more queue (0 = unlimited). Measured with 500 databases waking together after a restart
    /// (MinIO, 2 CPUs, results-round3.md): first request p50/p99 2.7/16.4 s with 8, 3.9/12.1 s with 32, 6.1/8.6 s with 128,
    /// 6.3/9.5 s without a limit. 128 gives the shortest tail, which is what a customer waits for.
    #[arg(long, default_value_t = 128)]
    max_concurrent_wakes: usize,
    /// Memtable size in MiB (slatedb's own estimate of the data) at which a writer flushes it to an L0 SST (slatedb's default
    /// is 64 MiB per database). The process memory a memtable holds is about 4 times this estimate for 1 KB documents
    /// (results-round3.md). 2 bounded the memory better but, on real S3, a database at 100 writes/s then flushed L0 so
    /// often that its writes stalled on compaction (p99 2-3 s, request cost doubled); 8 had no such stall (results-round3.md,
    /// part E). 0 = slatedb's default.
    #[arg(long, default_value_t = 8)]
    memtable_max_mb: u64,
    /// A writer flushes its memtable to an L0 SST once its oldest unflushed write is this old, in seconds (0 = only when the
    /// memtable is full or after slatedb's 4096 WAL flushes). Measured (mem-growth branch, 50 databases at 2 writes/s): the
    /// memtables of slowly written databases were most of the memory that grew with the writes. A database written fast
    /// fills its memtable before this age and is not affected; a slow one gets at most one more L0 SST per interval.
    #[arg(long, default_value_t = 600)]
    memtable_max_age_secs: u64,
    /// Also keep the data blocks of the L0 SSTs a writer flushes in the shared block cache (slatedb's default). Off: those
    /// blocks filled the cache with data nobody had read and, after the database closed, could never be read again (new
    /// cache scope per open). Blocks are still cached when read.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    cache_flushed_blocks: bool,
    /// A cursor (find, aggregate, tailable, change stream) that no getMore used for this long is closed and its buffered
    /// results freed, as mongod's `cursorTimeoutMillis` (10 minutes). Before, an abandoned cursor stayed until the process
    /// stopped. 0 = never.
    #[arg(long, default_value_t = 600)]
    cursor_timeout_secs: u64,
    /// One block cache shared by every open database, in MiB (slatedb's default is a 512 MiB cache per open database, with no
    /// limit across databases: the memory then grew with the data read by each database).
    #[arg(long, default_value_t = 256)]
    block_cache_mb: u64,
    /// One metadata cache (SST indexes and filters) shared by every open database, in MiB (slatedb's default: 128 MiB per database).
    #[arg(long, default_value_t = 64)]
    meta_cache_mb: u64,
    /// Memory the change-stream event log may hold, in MiB (estimated). Older events are dropped first; a resume token older than
    /// the oldest event kept fails with ChangeStreamHistoryLost. The log keeps whole documents, so without this limit every
    /// write grew the process by ~4 KB until 100 000 events were held.
    #[arg(long, default_value_t = 32)]
    change_log_max_mb: usize,
    /// Relaxed durability for databases without their own setting (`PUT /v1/databases/{db}/durability`), in milliseconds:
    /// 0 (default) = off, otherwise 1000 to 10000. A write with write concern `{w: 1, j: false}` to such a database is
    /// acknowledged before it is durable and becomes durable within this interval (lost if the process dies first);
    /// `j: true`, `w: "majority"` and writes without a write concern stay durable before the acknowledgement.
    #[arg(long, default_value_t = 0)]
    relaxed_flush_ms: u64,
    /// Adaptive group commit: the first write of a group waits a short window (half the database's average flush time, at
    /// most --adaptive-group-commit-max-us) when more writes of the same database are expected within it, so they share
    /// one WAL object. A database written rarely never waits.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    adaptive_group_commit: bool,
    #[arg(long, default_value_t = 5000)]
    adaptive_group_commit_max_us: u64,
    /// PROTOTYPE, off by default: a durable log in this local directory. Writes are acknowledged after the log's fsync and
    /// reach object storage in blocks every --local-log-upload-secs; a restart applies the records not yet uploaded. Losing
    /// the disk loses up to that interval of acknowledged writes of every database. See locallog.rs.
    #[arg(long, default_value = "")]
    local_log_dir: String,
    #[arg(long, default_value_t = 30)]
    local_log_upload_secs: u64,
    /// Local log: the restart gives up on replaying one database after this long (its records are kept for the next
    /// restart) so that one stuck database (its S3 requests hanging or retried) does not keep the whole server down.
    #[arg(long, default_value_t = 120)]
    local_log_replay_timeout_secs: u64,
    /// Fair queue between databases, only while the server is saturated: when the runtime runs a 10 ms timer late by more
    /// than this (moving average, ms), new commands wait in their database's queue and start in turn (wakes first) as
    /// others finish; below it every command starts at once. 0 = off. See fair.rs.
    #[arg(long, default_value_t = 10.0)]
    fair_lag_ms: f64,
    /// While saturated (see --fair-lag-ms), at most this many commands run at once; the others wait in turn (0 = 4 per CPU).
    #[arg(long, default_value_t = 0)]
    fair_max_inflight: u64,
    /// Safety limit per database, only against runaway clients: commands running or queued at once (0 = none).
    #[arg(long, default_value_t = 10_000)]
    db_max_inflight: u64,
    /// Safety limit per database, only against runaway clients: commands in one second (0 = none).
    #[arg(long, default_value_t = 100_000)]
    db_max_rate: u64,
    /// Test control only: never treat a lease as expired or lost. Demonstrates what the ownership checks prevent.
    #[arg(long)]
    unsafe_skip_ownership_checks: bool,
    /// Run reads of one tenant one at a time like writes (the behaviour before concurrent reads). By default reads of
    /// one tenant share the entry lock and run concurrently; writes and tenant state changes are exclusive.
    #[arg(long)]
    serialize_reads: bool,
    /// Do not use leases (single process only; for stores without conditional update, e.g. --store local).
    #[arg(long)]
    no_leases: bool,
    /// Wait for each write's own flush while holding the tenant lock (the behaviour before group commit: one WAL
    /// object per write). By default writes arriving while a flush is in flight share the next WAL object.
    #[arg(long)]
    no_group_commit: bool,
    /// Seconds between manifest reads of an open writer (it learns of compactions, and retries a memtable flush that
    /// stalled on a full L0, at this pace). One LIST and one GET per open writer per interval: 60 s is ~86 000 requests
    /// a month per database open for writing (30 s, the old default, twice that). Must stay below --sst-gc-min-unref-secs.
    #[arg(long, default_value_t = 60)]
    writer_poll_secs: u64,
    /// One lease per process (`processes/{holder}/_lease`, renewed every third of the ttl) plus a per-database owner
    /// record written when the database is opened, instead of one lease object renewed per open database (6 PUT per minute
    /// for every open database with the default ttl): an open database then costs no lease request. On by default;
    /// `--process-lease false` goes back to one lease per database. Every process sharing the bucket should use the same
    /// setting (a mix is safe, see lease.rs: each mode refuses the other's live records).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    process_lease: bool,
    /// Seconds between WAL garbage-collection passes over an open writer that is being written (0 = off, WAL objects
    /// then stay until the tenant has been idle and is collected as a whole).
    #[arg(long, default_value_t = 20)]
    wal_gc_secs: u64,
    /// Group commit window in milliseconds (fractions allowed): the first write of a group waits this long before
    /// flushing so that more writes can share the WAL object. 0 (default) flushes at once.
    #[arg(long, default_value_t = 0.0)]
    group_commit_window_ms: f64,
    /// Reads see only durable data (default true). `--durable-reads false`: reads may see writes that are staged
    /// and waiting for their group flush (they can disappear if the process dies before the flush).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    durable_reads: bool,
    /// Test control: slatedb's `l0_max_ssts` for writers (default 8, 0 keeps it). Lowering it makes the stalled-flush
    /// shutdown fallback reachable with a modest number of writes.
    #[arg(long, default_value_t = 0)]
    l0_max_ssts: usize,
    /// How the WAL cleanup of an open writer works: `bulk` (default: remembers where it stopped, bulk deletes),
    /// `single` (the same with one DELETE per object), `slatedb` (slatedb's own pass: lists the whole WAL directory,
    /// one DELETE per object) or `list-only` (that listing without deleting; profiling only, bounds nothing).
    #[arg(long, default_value = "bulk")]
    wal_gc_mode: String,
    /// Seconds between cleanups of the compacted SSTs and old manifests of an open writer (0 = off: they stay until
    /// the tenant closes).
    #[arg(long, default_value_t = 60)]
    sst_gc_secs: u64,
    /// Seconds an SST or manifest must have been seen unreferenced before it is deleted (must exceed
    /// --writer-poll-secs, the staleness of a live writer's view of the manifest).
    #[arg(long, default_value_t = 120)]
    sst_gc_min_unref_secs: u64,
    /// Test control, unsafe: the SST cleanup also deletes the oldest SST the manifest still references.
    #[arg(long)]
    gc_control_delete_live: bool,
    /// Test control: memtable size at which a writer flushes it to an L0 SST (slatedb default 64 MiB; 0 keeps it).
    #[arg(long, default_value_t = 0)]
    l0_sst_bytes: u64,
    /// With --group-commit-window-ms: wait for the window only while another write of the tenant is still on its way
    /// to the flush; flush at once when none is (so a lone writer never waits). Without it the window is always waited.
    #[arg(long)]
    group_commit_adaptive: bool,
    /// Log every command name and database to stderr.
    #[arg(long)]
    log_commands: bool,
    #[command(flatten)]
    ctl: ctl::CtlArgs,
    #[command(flatten)]
    net: netguard::NetArgs,
    /// Require authentication and authorization for every command (mongod's --auth).
    #[arg(long)]
    auth: bool,
    /// With --auth: the only roles a caller that holds neither root nor clusterAdmin (a provisioner, a customer's user
    /// administrator) may hand out with createUser, updateUser and grantRolesToUser, comma-separated, e.g. `readWrite,read`.
    /// When set, such a caller may manage users only on customer databases (never admin, local, config, $external), may
    /// grant only the listed built-in roles of the database the user is defined on, never one of its own roles, and may not
    /// create or change roles. Only built-in roles of one database without user administration are accepted (read,
    /// readWrite, dbAdmin). Empty: no restriction (mongod's rules).
    #[arg(long, default_value = "")]
    grantable_roles: String,
    /// TLS on the client port: disabled, allowTLS, preferTLS or requireTLS (mongod's net.tls.mode).
    #[arg(long, default_value = "disabled")]
    tls_mode: String,
    /// PEM file with the server certificate chain and its private key (net.tls.certificateKeyFile).
    #[arg(long, default_value = "")]
    tls_certificate_key_file: String,
    /// PEM file with the CA certificates client certificates must chain to (net.tls.CAFile).
    #[arg(long, default_value = "")]
    tls_ca_file: String,
    /// With --tls-ca-file: accept clients that present no certificate (net.tls.allowConnectionsWithoutCertificates).
    #[arg(long)]
    tls_allow_connections_without_certificates: bool,
}

async fn handle_connection(server: Arc<Server>, tls: Arc<tls::TlsConfig>, tcp: TcpStream, conn_id: i64) {
    // The client address is the TCP peer: nothing in front of the engine rewrites it, and nothing the client sends changes it.
    let guard = netguard::get();
    let peer = tcp.peer_addr().ok().map(|a| netguard::canonical_ip(a.ip()));
    let Some(_slot) = guard.open_connection(peer) else {
        return; // over the per-address limit: closed at once
    };
    let deadline = guard.handshake_timeout().map(|t| tokio::time::Instant::now() + t);
    let accepted = match deadline {
        Some(d) => match tokio::time::timeout_at(d, tls.accept(tcp)).await {
            Ok(r) => r,
            Err(_) => {
                guard.totals.handshake_timeouts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                eprintln!("conn {conn_id}: closed: TLS handshake not done within the handshake timeout (peer {})", peer.map(|p| p.to_string()).unwrap_or_default());
                return;
            }
        },
        None => tls.accept(tcp).await,
    };
    let stream = match accepted {
        Ok(s) => s,
        Err(e) => {
            eprintln!("conn {conn_id}: TLS handshake failed: {e:#}");
            return;
        }
    };
    stream.set_nodelay(true);
    server.diag.conn_opened(conn_id, stream.peer_addr().ok().map(|a| std::net::SocketAddr::new(a.ip(), a.port())));
    server.auth_connection_opened(conn_id, stream.peer_addr().ok().map(|a| a.ip()), stream.local_addr().ok().map(|a| a.ip()), stream.client_subject());
    handle_connection_inner(&server, stream, conn_id, deadline, peer).await;
    server.auth_connection_closed(conn_id);
    server.diag.conn_closed(conn_id);
}

async fn handle_connection_inner(server: &Arc<Server>, stream: tls::Stream, conn_id: i64, first_deadline: Option<tokio::time::Instant>, peer: Option<std::net::IpAddr>) {
    let guard = netguard::get();
    let (r, mut w) = tokio::io::split(stream);
    let mut r = BufReader::new(r);
    let mut response_id = 1;
    let mut first = true;
    loop {
        // The first message must arrive whole before the handshake deadline. With --auth, until the connection has
        // authenticated, a message must arrive whole within the handshake timeout once it started, and may not be larger than
        // the pre-authentication limit. Idle time between messages is not limited (a driver's monitoring connection never
        // authenticates and waits between its hello commands).
        let pre_auth = server.auth.enabled.load(std::sync::atomic::Ordering::Relaxed) && !server.conn_authenticated(conn_id);
        let (first_by, rest_within) = match (first, first_deadline) {
            (true, Some(d)) => (Some(d), Some(d.saturating_duration_since(tokio::time::Instant::now()))),
            _ => (None, if pre_auth { guard.handshake_timeout() } else { None }),
        };
        let max_len = if pre_auth { guard.pre_auth_max() } else { usize::MAX };
        first = false;
        match wire::read_request_bounded(&mut r, max_len, first_by, rest_within).await {
            Ok(Some(req)) => {
                let probe = ctl::usage::before(&req.db, &req.command).await;
                server.diag.note_seq(conn_id, &req.seq_keys);
                let reply = server.handle(&req.db, req.command.clone(), conn_id).await;
                ctl::usage::after(probe).await;
                if reply.contains_key("$shutdown") {
                    // `shutdown`: no reply; the connection closes and the process stops
                    repl_cmds::SHUTDOWN.notify_one();
                    break;
                }
                if req.more_to_come {
                    continue;
                }
                response_id += 1;
                if let Err(e) = wire::write_reply(&mut w, &req, &reply, response_id).await {
                    eprintln!("conn {conn_id}: write failed: {e:#}");
                    break;
                }
            }
            Ok(None) => break,
            Err(e) => {
                let shown = peer.map(|p| p.to_string()).unwrap_or_default();
                if e.downcast_ref::<wire::ReadTimeout>().is_some() {
                    guard.totals.handshake_timeouts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    eprintln!("conn {conn_id}: closed: message not received within the handshake timeout (peer {shown})");
                } else if let Some(t) = e.downcast_ref::<wire::TooLarge>() {
                    guard.totals.oversized_before_auth.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    eprintln!("conn {conn_id}: closed: {t} before authentication (peer {shown})");
                } else {
                    eprintln!("conn {conn_id}: {e:#}");
                }
                break;
            }
        }
    }
}

/// Measurement builds (`--features heap-prof`): every `HEAP_PROF_DUMP_SECS` seconds, print jemalloc's live heap
/// (`allocated`) next to what the allocator holds (`resident`) and, with `prof:true`, write a heap profile
/// (`HEAP_PROF_PREFIX.N.heap`) for `jeprof --base`.
#[cfg(feature = "heap-prof")]
fn heap_prof_thread() {
    let Some(secs) = std::env::var("HEAP_PROF_DUMP_SECS").ok().and_then(|v| v.parse::<u64>().ok()) else { return };
    let prefix = std::env::var("HEAP_PROF_PREFIX").unwrap_or_else(|_| "/tmp/heap".into());
    std::thread::spawn(move || {
        for n in 0u64.. {
            std::thread::sleep(std::time::Duration::from_secs(secs));
            let (a, r) = heap_stats();
            eprintln!("HEAPPROF n={n} allocated={a} resident={r}");
            let path = std::ffi::CString::new(format!("{prefix}.{n:04}.heap")).unwrap();
            unsafe {
                let _ = tikv_jemalloc_ctl::raw::write(b"prof.dump\0", path.as_ptr());
            }
        }
    });
}

/// jemalloc's live heap (`stats.allocated`) and the memory it holds (`stats.resident`), in bytes.
pub(crate) fn heap_stats() -> (u64, u64) {
    use tikv_jemalloc_ctl::{epoch, stats};
    let _ = epoch::advance();
    (stats::allocated::read().unwrap_or(0) as u64, stats::resident::read().unwrap_or(0) as u64)
}

fn main() -> Result<()> {
    #[cfg(feature = "heap-prof")]
    heap_prof_thread();
    // The futures of the command handlers are large: the workers get stacks that are not the 2 MiB default.
    tokio::runtime::Builder::new_multi_thread().enable_all().thread_stack_size(64 << 20).build()?.block_on(run())
}

async fn run() -> Result<()> {
    let args = Args::parse();
    // The SST cleanup deletes an SST this long after it left the manifest; a reader that polls the manifest less often
    // would read SSTs that are already gone.
    anyhow::ensure!(args.writer_poll_secs < args.sst_gc_min_unref_secs, "--writer-poll-secs ({}) must be below --sst-gc-min-unref-secs ({})", args.writer_poll_secs, args.sst_gc_min_unref_secs);
    anyhow::ensure!(
        !args.unsafe_skip_ownership_checks || std::env::var("MONGO_ALLOW_UNSAFE_TEST_FLAGS").map_or(false, |v| v == "1"),
        "--unsafe-skip-ownership-checks is a test control that lets two processes serve one database; it needs MONGO_ALLOW_UNSAFE_TEST_FLAGS=1"
    );
    netguard::install(&args.net)?;
    let grantable = authz::parse_grantable(&args.grantable_roles).map_err(|e| anyhow::anyhow!(e))?;
    let s3: Arc<dyn ObjectStore> = match args.store.as_str() {
        "s3" => {
            // The credentials are the standard AWS ones, none is built in: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
            // AWS_SESSION_TOKEN, a web identity token (AWS_WEB_IDENTITY_TOKEN_FILE + AWS_ROLE_ARN), the container credentials
            // endpoint, or, with none of those set, the instance role through the metadata service (IMDSv2). Region and endpoint
            // come from the flags or from AWS_REGION / AWS_DEFAULT_REGION / AWS_ENDPOINT. Credentials are never on the command line.
            let mut b = AmazonS3Builder::from_env().with_bucket_name(&args.bucket);
            if let Some(r) = &args.region {
                b = b.with_region(r);
            }
            let region_set = args.region.is_some() || ["AWS_REGION", "AWS_DEFAULT_REGION"].iter().any(|k| std::env::var(k).map(|v| !v.is_empty()).unwrap_or(false));
            anyhow::ensure!(region_set, "no S3 region: pass --region or set AWS_REGION (the engine has no built-in region)");
            if !args.endpoint.is_empty() {
                b = b.with_endpoint(&args.endpoint);
                if args.endpoint.starts_with("http://") {
                    b = b.with_allow_http(true);
                }
            }
            // S3 lists keys in order: the WAL cleanup finds the oldest WAL object with one LIST.
            tenant_bench::lifecycle::LIST_SORTED.store(true, std::sync::atomic::Ordering::Relaxed);
            Arc::new(b.build()?)
        }
        "local" => {
            std::fs::create_dir_all(&args.local_dir)?;
            // LocalFileSystem has no conditional overwrite (PutMode::Update); the wrapper adds it for one process.
            Arc::new(localcas::LocalCas::new(Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&args.local_dir)?)))
        }
        "memory" => Arc::new(object_store::memory::InMemory::new()),
        other => anyhow::bail!("unknown --store {other}"),
    };
    let s3 = faults::wrap(s3);
    tenant_bench::lifecycle::FLUSH_INTERVAL_MS.store(args.wal_flush_ms, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::ACK_BEFORE_DURABLE.store(args.ack_before_durable, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::EXPLICIT_FLUSH.store(args.wal_flush_ms == 0, std::sync::atomic::Ordering::Relaxed);
    dbs::set_wake_limit(args.max_concurrent_wakes);
    tenant_bench::lifecycle::MEMTABLE_MAX_BYTES.store(args.memtable_max_mb << 20, std::sync::atomic::Ordering::Relaxed);
    if args.block_cache_mb > 0 || args.meta_cache_mb > 0 {
        tenant_bench::lifecycle::install_shared_cache(args.block_cache_mb << 20, args.meta_cache_mb << 20);
    }
    changes::MAX_BYTES.store(args.change_log_max_mb << 20, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::MEMTABLE_MAX_AGE_SECS.store(args.memtable_max_age_secs, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::CACHE_FLUSHED_BLOCKS.store(args.cache_flushed_blocks, std::sync::atomic::Ordering::Relaxed);
    cmd::CURSOR_TIMEOUT_SECS.store(args.cursor_timeout_secs, std::sync::atomic::Ordering::Relaxed);
    ctl::durability::install(args.relaxed_flush_ms)?;
    fair::install(args.fair_lag_ms, args.db_max_inflight, args.db_max_rate, args.fair_max_inflight);
    let local_log_old = if args.local_log_dir.is_empty() { None } else { Some(locallog::install(&args.local_log_dir, args.local_log_upload_secs)?) };
    tenant_bench::lifecycle::ADAPTIVE.store(args.adaptive_group_commit, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::ADAPTIVE_MAX_US.store(args.adaptive_group_commit_max_us, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::WRITER_POLL_SECS.store(args.writer_poll_secs, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::HOT_GC_EVERY_SECS.store(args.wal_gc_secs, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::GROUP_WINDOW_US.store((args.group_commit_window_ms * 1000.0) as u64, std::sync::atomic::Ordering::Relaxed);
    dbs::DURABLE_READS.store(args.durable_reads, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::L0_MAX_SSTS_OVERRIDE.store(args.l0_max_ssts, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::WAL_GC_MODE.store(
        match args.wal_gc_mode.as_str() { "slatedb" => 0, "list-only" => 1, "single" => 2, "bulk" => 3, other => anyhow::bail!("unknown --wal-gc-mode {other}") },
        std::sync::atomic::Ordering::Relaxed,
    );
    tenant_bench::lifecycle::SST_GC_EVERY_SECS.store(args.sst_gc_secs, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::SST_GC_MIN_UNREF_SECS.store(args.sst_gc_min_unref_secs, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::GC_CONTROL_DELETE_LIVE.store(args.gc_control_delete_live, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::L0_SST_BYTES_OVERRIDE.store(args.l0_sst_bytes, std::sync::atomic::Ordering::Relaxed);
    tenant_bench::lifecycle::GROUP_ADAPTIVE.store(args.group_commit_adaptive, std::sync::atomic::Ordering::Relaxed);
    dbs::GROUP_COMMIT.store(!args.no_group_commit, std::sync::atomic::Ordering::Relaxed);
    dbs::SHARED_READS.store(!args.serialize_reads, std::sync::atomic::Ordering::Relaxed);
    // Requests are attributed to databases by object path (control layer, usage per database).
    let usage = ctl::usage::Usage::new();
    let s3 = ctl::usage::wrap(s3, Arc::clone(&usage));
    // One counter for every request this process makes (lifecycle, leases, maintenance, listings).
    let global = Arc::new(CountingStore::new(s3));
    let s3: Arc<dyn ObjectStore> = global.clone();
    let counting = Arc::new(CountingStore::new(Arc::clone(&s3)));
    let (queue, rx) = CompactionQueue::new();
    let lc = if args.no_maintenance {
        Lifecycle::with_guard(counting, None, false)
    } else {
        Lifecycle::new(counting, Some(Arc::clone(&queue)))
    };
    let maintenance = (!args.no_maintenance).then(|| {
        Maintenance::start(Arc::clone(&s3), Arc::clone(&s3), queue, rx, Arc::clone(&lc.touched), Arc::clone(&lc.maint_locks), 2, Duration::from_secs(30), Duration::from_secs(15))
    });
    let holder = args.holder.clone().unwrap_or_else(|| format!("{}-{:08x}", std::process::id(), rand::random::<u32>()));
    let leases = lease::Leases::new(Arc::clone(&lc.store), holder.clone(), Duration::from_secs(args.lease_ttl_secs));
    leases.process_mode.store(args.process_lease, std::sync::atomic::Ordering::Relaxed);
    leases.disabled.store(args.no_leases, std::sync::atomic::Ordering::Relaxed);
    leases.unsafe_skip.store(args.unsafe_skip_ownership_checks, std::sync::atomic::Ordering::Relaxed);
    {
        // The hot WAL cleanup of an open writer runs only while this process owns the tenant.
        let l = Arc::clone(&leases);
        let _ = tenant_bench::lifecycle::OWNER_VALID.set(Box::new(move |id: &str| l.disabled.load(std::sync::atomic::Ordering::Relaxed) || l.valid(id)));
    }
    let lc_for_ctl = Arc::clone(&lc);
    let dbs = Dbs::new(lc, Arc::clone(&s3), Duration::from_secs(args.idle_secs), Arc::clone(&leases), global);
    dbs.spawn_reaper();
    let (lost_tx, lost_rx) = tokio::sync::mpsc::unbounded_channel();
    leases.spawn_renewer(lost_tx);
    dbs.spawn_lost_handler(lost_rx);
    if !args.no_ttl {
        dbs.spawn_ttl_session_sweeper(Duration::from_secs(args.ttl_sweep_secs));
        if args.ttl_wake_sleeping {
            ttl::WAKE_SLEEPING.store(true, std::sync::atomic::Ordering::Relaxed);
            dbs.spawn_ttl_maintenance(Duration::from_secs(args.ttl_maintenance_secs), Duration::from_secs(args.ttl_min_gap_secs), args.ttl_max_wakes);
        }
    }
    let server = Server::new(Arc::clone(&dbs), args.log_commands);
    server.auth.enabled.store(args.auth, std::sync::atomic::Ordering::Relaxed);
    if let Some(g) = grantable {
        eprintln!("grantable roles (callers without root or clusterAdmin): {}", g.join(","));
        let _ = server.auth.grantable.set(g);
    }
    if !args.replica_set.is_empty() {
        let port = args.listen.rsplit(':').next().unwrap_or("27017");
        *server.rs.lock().unwrap() = Some((args.replica_set.clone(), args.advertise.clone().unwrap_or_else(|| format!("localhost:{port}"))));
    }
    server.spawn_txn_janitor();
    // The local log is applied before the control layer starts: no erase, restore or fork (resumed or new) may run on a
    // database whose acknowledged writes are still only in the log, and an erase's drop record is then always seen first.
    if let Some(old) = local_log_old {
        let t = std::time::Instant::now();
        let (n, recs) = locallog::recover(&dbs, old, Duration::from_secs(args.local_log_replay_timeout_secs)).await?;
        eprintln!("local log: restart applied {recs} records to {n} databases in {:.1} s", t.elapsed().as_secs_f64());
        locallog::get().unwrap().spawn_uploader();
    }
    ctl::start(Arc::clone(&dbs), Arc::clone(&server), lc_for_ctl, holder.clone(), args.ctl.clone(), usage).await?;
    let tls_cfg = Arc::new(tls::load(&tls::Options { mode: &args.tls_mode, certificate_key_file: &args.tls_certificate_key_file, ca_file: &args.tls_ca_file, allow_without_certificates: args.tls_allow_connections_without_certificates })?);
    tls::install(Arc::clone(&tls_cfg));
    let listener = TcpListener::bind(&args.listen).await?;
    eprintln!("mongo-server listening on {} (idle close after {} s, holder {holder}, lease ttl {} s)", args.listen, args.idle_secs, args.lease_ttl_secs);
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let server = Arc::clone(&server);
                let id = server.new_connection_id();
                tokio::spawn(handle_connection(server, Arc::clone(&tls_cfg), stream, id));
            }
            _ = tokio::signal::ctrl_c() => break,
            _ = term.recv() => break,
            _ = repl_cmds::SHUTDOWN.notified() => break,
        }
    }
    eprintln!("shutting down: closing tenants cleanly");
    ctl::stop_agents().await;
    dbs.close_all().await;
    ctl::shutdown().await;
    leases.release_process().await;
    eprintln!("tenants closed; stopping maintenance");
    if let Some(m) = maintenance {
        m.shutdown().await;
    }
    eprintln!("maintenance stopped");
    Ok(())
}
