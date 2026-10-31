# test5

## Engine (`mongo-server`): access control and network limits

### Who may run the administration commands (with `--auth`)

| command | who may run it |
|---|---|
| `rotateCertificates` | the cluster action `rotateCertificates`, as in mongod: `root`, `hostManager`, `clusterAdmin`, or a custom role that carries the action. With TLS on, the certificate, key and CA files are read again and used for every new connection. |
| `tenantStats` (S3 request counters of the whole process) | `root` only (or a role that inherits `root`). Never the roles of one database, nor the other cluster roles. |
| `tenantAdmin` on the data port | nobody (root included). The control layer is the admin port, with its token. |

### Roles a provisioner may hand out: `--grantable-roles`

By default (flag empty) the engine follows mongod: whoever has `grantRole` on a database may hand out any role of that
database, so a provisioner with user administration on every database can grant `dbOwner` to a customer, `userAdmin` on
`admin`, or create a copy of itself.

Set the flag to the roles the control plane really needs:

```
mongo-server --auth --grantable-roles readWrite,read ...
```

With the flag set, a caller that holds neither `root` nor `clusterAdmin` (directly or through a role that inherits them):

* may create users and hand out roles (`createUser`, `updateUser` with `roles`, `grantRolesToUser`) **only** with the roles
  of the list, as built-in roles of the database the user is defined on (the customer database);
* may never manage users on `admin`, `local`, `config` or `$external` (no second provisioner, no change to the password,
  roles or existence of the users of `admin`);
* may never hand out one of its own roles, a role that can manage users or roles, a role of another database, or a custom
  role (even one that includes only `read`);
* may not create, change or drop roles at all;
* may still change passwords and custom data, revoke roles, list and drop the users of customer databases.

The refusal is mongod's `Unauthorized` (code 13). `root` and `clusterAdmin` keep mongod's rules. The rule applies to every
caller without those roles, including a customer's own user administrator.

Accepted values: comma-separated names of built-in roles of one database without user administration: `read`,
`readWrite`, `dbAdmin`. Anything else (`dbOwner`, `userAdmin`, admin or cluster roles, unknown names, empty items, spaces,
`role@db`) stops the engine at startup with a message that names the flag.

The control plane's flows keep working with `--grantable-roles readWrite,read`: customer users with `readWrite` or `read`
on their database, and the internal read-only user of each database `cp_<id>` (role `read`), **created on the customer
database itself** (a user defined on `admin` with `read` on the customer database would be refused).

### Authentication blocks and connection limits

All counters and blocks are in memory, per process. Trusted networks (`--trusted-networks`) are never counted, blocked or
limited. The values in force are shown by `GET /v1/network` on the admin port (`config`), with the blocked addresses;
`DELETE /v1/network/blocks/{ip}` lifts every block of an address and forgets its counts (the address-wide block and the
blocks of every database: the operator's tool). `DELETE /v1/network/blocks/{ip}?database=<db>` lifts **only** the block
of the pair (address, database) and forgets that pair's counts: the address-wide block and the other databases' blocks
stay. The reply says `{"ip", "database", "unblocked"}`, where `unblocked` is false when the pair had nothing tracked; an
empty `database` is a 400.

| flag | default | effect |
|---|---|---|
| `--auth-fail-max` | 100 | failed authentications from one address within the window that block the address on every database (the 100th failure starts the block) |
| `--auth-fail-max-per-db` | 5 | failed authentications from one address on one database within the window that block the address on that database only |
| `--auth-fail-window-secs` | 60 | sliding window of both thresholds |
| `--auth-block-secs` | 300 | first block; each new block of the same key doubles it |
| `--auth-block-max-secs` | 3600 | longest block |
| `--auth-block-forget-secs` | 86400 | without a block for this long, the doubling starts again |
| `--max-conns-per-ip` | 200 | connections at once per address (0: no limit) |
| `--handshake-timeout-secs` | 10 | TLS handshake and first message; before authentication, each message once started (0: off) |
| `--pre-auth-max-message-bytes` | 65536 | largest message before authentication |
| `--ip-allowlist-refresh-secs` | 5 | re-reading of a database's allow list |
| `--ipv6-group-prefix` | 64 | IPv6 addresses are counted per prefix |
| `--netguard-max-tracked` | 100000 | addresses tracked at once (memory bound) |
| `--netguard-max-dbs-per-ip` | 1000 | databases tracked at once per address (memory bound) |

Why 100 per address: a platform with one database per customer connects from few addresses; after a wrong password
rotation, 10 failures would block every database of the platform for up to an hour. The per-database threshold (5) still
stops guessing on one database.

## Engine: request cost, memory and durability modes (round3)

Details and measurements: `results-round3.md`.

### Defaults that changed

| flag | before | now | why |
|---|---|---|---|
| `--process-lease` | off (one lease per open database, renewed every ttl/3: 6 PUT/min each) | **on** (one lease per process; `--process-lease false` goes back) | an open database costs no lease request; safety with two processes: `ctl-tests/test_20_process_lease.py` |
| `--max-concurrent-wakes` | 0 (unlimited) | **128** | a restart wakes every database at once; with 500 databases on 2 CPUs, 128 gave the shortest tail (first request p99 8.6 s against 9.5 s unlimited and 16.4 s with 8) |
| `--writer-poll-secs` | 30 | **60** | one LIST + one GET per open writer per poll |
| `--usage-flush-secs` | 60 | **300** | one PUT per active database per flush. A crash loses the usage counters (billing) of up to the last 300 s of every database |
| `--memtable-max-mb` (new) | slatedb's 64 MiB per database | **8** | memory held by unflushed writes (measured: ~4.3 KB of process memory per 1 KB unflushed write). 2 held less memory but stalled a database at 100 writes/s on real S3 (p99 2-3 s). It also sets slatedb's `max_unflushed_bytes` to 4 × that, 32 MiB per database: a bulk load hits write backpressure much earlier than with slatedb's defaults |
| `--block-cache-mb`, `--meta-cache-mb` (new) | slatedb's 512 MiB + 128 MiB **per open database** | **256 + 64 MiB shared by all databases** | memory grew with the data read and written |
| `--change-log-max-mb` (new) | 100 000 events with whole documents | **32 MiB** (and still at most 100 000 events) | ~2.5 KB of memory per write |

The WAL cleanup of open writers (`--wal-gc-mode bulk`, the default) no longer falls back to slatedb's pass when a database has a
retention window: it deletes the WAL objects below the oldest checkpoint (restore points are checkpoints) and below the
writer's replay boundary, using the writer's own view of the manifest. A pass with nothing to delete makes no request.

### Memory per write (mem-growth)

Details and measurements: `results-mem-growth.md`. Four changes, each measured on MinIO with 50 databases at 2 writes/s:

* **jemalloc** is the allocator (with `background_thread`, so freed pages go back to the system while the process is idle).
  With the system allocator the process held ~4.5-5.5 KB of RSS per 1 KB write for ~1.5 KB of live heap, and gave nothing
  back when the databases closed. `serverStatus` reports the live heap as `mem.heapAllocatedBytes` and what the allocator
  holds as `mem.heapResidentBytes`.
* `--memtable-max-age-secs` (new, default **600**): a writer flushes its memtable to an L0 SST once its oldest unflushed
  write is that old (each database between 3/4 of the age and the age, so databases opened together do not flush at the
  same moment). slatedb flushed a memtable only when full or after 4096 WAL flushes (~34 minutes at 2 writes/s). A database
  written fast fills its memtable first and is not affected; a slow one gets at most one more L0 SST per interval. 0 = off.
* `--cache-flushed-blocks` (new, default **false**): the data blocks of the L0 SSTs a writer flushes are no longer put in
  the shared block cache (index and filter blocks still are). They filled the cache with data nobody had read, and after
  the database closed they could never be read again (new cache scope per open). Blocks are still cached when read.
  `true` goes back to slatedb's behaviour.
* `--cursor-timeout-secs` (new, default **600**, mongod's `cursorTimeoutMillis`): a cursor that no `getMore` used for that
  long is closed and its buffered results freed (`serverStatus` `metrics.cursor.timedOut`). Before, an abandoned cursor stayed
  until the process stopped. A `find` with `noCursorTimeout: true` is closed after 30 minutes idle (mongod's session timeout).
  0 = never.

Regression test: `ctl-tests/test_27_memory_per_write.py` (fails when the live heap or the RSS grows by more than 0.5 / 0.8 KB
per write once the bounded structures are full, when the heap does not come back after the databases close, or when idle
cursors are not closed). Measurement scripts: `tools/mem/memcheck.py` (memory per write and after the close),
`tools/mem/perdb.py` (memory per open database). Measurement build with jemalloc's heap profiler:
`cargo build --release -p mongo-server --features heap-prof`, then `_RJEM_MALLOC_CONF=prof:true HEAP_PROF_DUMP_SECS=60
HEAP_PROF_PREFIX=/tmp/h` and `jeprof --base`.

### Open files: `LimitNOFILE`

Each client connection is one file descriptor; the engine itself holds a few dozen (listeners, the S3 connection pool, the
local log if on). Measured in `results-round3.md` (restart with 500 databases, client pools of up to 64 connections per
database): 6000 to 7900 descriptors open at the peak, about one per client connection. Rule: `LimitNOFILE` >= (client
connections the server may hold) + 1024. `LimitNOFILE=65536` (the systemd unit's `[Service]` section) covers that test with a
wide margin; the number of connections real drivers keep is not measured.

### Fair queue and safety limits (noisy neighbour)

No quota slows or rejects a database's normal traffic. When the server is saturated (the scheduler lag, sampled by a 10 ms
timer, is above `--fair-lag-ms`, default 10), at most `--fair-max-inflight` commands run at once (default 0 = 4 per CPU) and
the others wait in a queue that serves wakes of closed databases first and then one command per database in turn. Very high
safety limits per database, `--db-max-inflight 10000` running commands and `--db-max-rate 100000` requests/s, answer with the
MongoDB error 462 `IngressRequestRateLimitExceeded` (label `SystemOverloadedError`) and count a rejection.
`GET /v1/metrics/databases` on the admin port gives per database: requests and req/s, command CPU, estimated memory, queue
waits (count, total, max) and rejections.

### Lease modes and rollback

`--process-lease true` (the default) writes per-database owner records that never expire by themselves: the owner's
process lease decides. A process started with `--process-lease false` (going back to per-database leases) also honours
those records: it takes such a database over once the owner's process lease expired (`ctl-tests/test_25_review_fixes.py`).
Every process of a bucket should still use the same mode. A takeover revokes the old process lease, so a process whose
renewal was late or whose clock is off drops its databases at its next renewal (`ctl-tests/test_23_process_lease_late.py`);
lease deadlines use CLOCK_BOOTTIME, which keeps counting while a VM is suspended.

**Clock requirement.** The processes that share a bucket must keep their wall clocks synchronised (NTP or an equivalent
service). Whether an old owner's lease has expired is decided by comparing the expiry it wrote (its own wall clock) with
the wall clock of the process that wants to take over; CLOCK_BOOTTIME only protects each process's own view of its
leases. A process takes a database over only once the owner's lease expired more than 1.5 s ago (skew allowance), and an
owner stops serving `ttl/6` before its lease expires by its own clock. So the takeover is safe while the clock offset
between any two processes stays below **1.5 s + ttl/6** (6.5 s with the default `--lease-ttl-secs 30`, 2.2 s with 4 s);
keep it well below 1.5 s.

What protects what:
- **Two writers on the WAL:** slatedb's fencing (writer epoch). A writer that another process replaced gets `Fenced` on
  its next WAL write or manifest update, whatever the clocks say, so a write acknowledged after a durable WAL write
  (the default, immediate mode) is never lost to a second owner.
- **Modes that acknowledge before the WAL write** (the local log, relaxed durability): only the lease. The old owner
  acknowledges a write when its own lease is still valid by its own clock; its later upload is fenced. If the clocks are
  off by more than the allowance above, the old owner can acknowledge writes after the takeover, and they are lost
  (`ctl-tests/test_23_process_lease_late.py`, local log: offset 0 and 1.5 s lose nothing; offset 20 s, outside the
  requirement, lost every write the old owner acknowledged in the window).

Test-only switches: `--unsafe-skip-ownership-checks` starts only with `MONGO_ALLOW_UNSAFE_TEST_FLAGS=1`;
`MONGO_TEST_FAULTS_FILE`, `MONGO_TEST_WALL_CLOCK_OFFSET_MS` and `MONGO_TEST_LOCALLOG_FAIL_AT` inject faults. None is ever set
in production.

### Write modes

| mode | how to switch it on | acknowledged when | lost if the process dies (kill -9) | lost if the disk is lost |
|---|---|---|---|---|
| immediate (default) | - | the write's WAL object is in S3 | nothing | nothing |
| adaptive group commit | `--adaptive-group-commit true` | same as immediate; the first write of a group may wait up to half a flush time (max 5 ms) when more writes of the same database are coming, so they share one PUT | nothing | nothing |
| relaxed (per database) | `PUT /v1/databases/{db}/durability {"relaxed_flush_ms": 1000..10000}` (0 = off), or `--relaxed-flush-ms` for every database | a write with write concern `{w: 1, j: false}` (or `w: 0`): once applied in memory. Every other write: as immediate | the relaxed writes of the last `relaxed_flush_ms` (+ one PUT) | the same |
| local log (PROTOTYPE) | `--local-log-dir DIR --local-log-upload-secs 30..60` | the record is fsynced on the local disk | nothing (the restart applies the log) | up to `--local-log-upload-secs` of writes of every database of the process |

**Relaxed durability, exact semantics.** Only a write command (insert, update, delete, findAndModify, bulk writes) whose write
concern is `{w: 1, j: false}`, `{j: false}` or `{w: 0}` (without `j: true`), outside a transaction, to a database whose
`relaxed_flush_ms` is not 0, is acknowledged before it is durable. It is visible at once to every reader of that server (reads of
such a database also see writes that are staged and waiting for their flush, as mongod's `j: false`). The database's writer
flushes its WAL to S3 on its own every `relaxed_flush_ms`, so the write is durable within that time plus one PUT. `j: true`,
`w: "majority"`, a write without any write concern (the drivers' default) and a transaction commit stay durable before the
acknowledgement, and they make durable every relaxed write staged before them. An idle close or a clean shutdown flushes. If
the process dies before the flush, the relaxed writes of the interval are lost, and a reader may have seen them. Setting the
value closes the database if it is open (its writer reopens with the new flush interval).

**Local log (prototype, not a default).** Writes are acknowledged after a group fsync of a log file in `--local-log-dir`;
every `--local-log-upload-secs` each written database flushes its WAL to S3 in one PUT. At start, before accepting
connections, the engine applies again the records not yet in S3 (exactly once: every batch carries a marker with the
record's number). The databases of a process in this mode are tied to its disk: until the restart has run on the same
disk, another process that takes a database over (after the lease expired) does not see the writes that were only in
the log, and losing the disk loses them. All processes of a bucket should use the same mode. See `mongo-server/src/locallog.rs`.

Local log guarantees and their limits (`ctl-tests/test_24_local_log.py`):
- sequence numbers carry a boot number kept in the log directory, so records of a later run are never taken for uploaded;
- a failed write or fsync of the log stops the process (fail-stop): nothing more is acknowledged, nothing more is uploaded,
  and the restart applies every record before the failure. Until the process stops, readers of that process may have seen
  the failed batch (it is staged in the memtable before the log write): it is never acknowledged and never made durable;
- a restart refuses the records of a database that another writer opened since (newer slatedb writer epoch);
- a write is not acknowledged if ownership was lost by the time its record is on disk;
- the restart applies the log before the admin interface starts, and a database whose replay fails or makes no progress for
  `--local-log-replay-timeout-secs` keeps its records while the others go on. Until they are applied that database is
  refused to every client (error 134 `DatabaseRecoveryPending`, retryable), so nothing reads it without them or writes
  over them; a background task retries the replay (after 1 s, doubling up to 30 s) and then opens it again. If the process
  restarts first, the next restart applies them. A drop or erase of the database discards them;
- a slow database delays the restart: the restart replays up to 32 databases at once and waits for all of them, so one
  database whose replay hangs keeps the restart (and the admin interface) waiting up to `--local-log-replay-timeout-secs`
  (default 120 s). No write is lost; the other databases are replayed meanwhile;
- when ownership is lost (lease lapsed, another process took the database), the writes of that database that were only in
  the local log are lost: the old owner can no longer upload them (it is fenced), and a drop record makes the restart skip them.
