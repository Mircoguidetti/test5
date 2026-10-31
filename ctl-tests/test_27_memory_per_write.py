"""Memory per write and idle cursors (mem-growth branch).

1. test_memory_per_write_is_bounded: 20 databases written at 10 inserts/s each (~1 KB documents) for a warm-up and then a
   measured window. Fails when the engine's memory grows with the writes faster than a threshold:
     - live heap (serverStatus mem.heapAllocatedBytes, jemalloc) per write in the window above HEAP_KB_PER_WRITE;
     - RSS per write in the window above RSS_KB_PER_WRITE;
   and, after the writes stop and every database closed (idle), when the live heap is still more than RETURN_SLACK_MB above
   where it was before the writes.
   The server runs with a short memtable age (--memtable-max-age-secs 20) and a small change-stream log (--change-log-max-mb 4)
   so that both reach their bound within the warm-up: what still grows after that grows with every write.
   Measured (MinIO): with the fix ~0.24 KB of RSS and ~0.19 KB of live heap per write; with the old behaviour (flags below)
   ~1.7 and ~1.6 KB, and main with the system allocator ~4.5 KB of RSS per write and nothing given back after the close.
   CTL_MEM_EXTRA adds server flags (e.g. "--memtable-max-age-secs 0 --cache-flushed-blocks true" to see the test fail).
2. test_idle_cursors_expire: cursors that no getMore uses are closed after --cursor-timeout-secs and their buffered results
   freed; a cursor in use stays open, and so does a noCursorTimeout cursor (closed after 30 minutes idle, as mongod's session).
   Measured before the fix: 40 abandoned cursors still open and usable after 700 s, ~27 MB of RSS held.
"""
import os
import threading
import time

import pytest
from pymongo.errors import OperationFailure

from lib import Server, make_bucket

N_DB = 20
RATE = 10.0          # inserts per second per database
WARM = 60            # seconds before the measured window
WINDOW = 120         # measured window, seconds
HEAP_KB_PER_WRITE = 0.5
RSS_KB_PER_WRITE = 0.8
RETURN_SLACK_MB = 16
PAD = "y" * 900


def rss_mb(pid):
    with open(f"/proc/{pid}/statm") as f:
        return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 2**20


def heap_mb(client):
    m = client.admin.command("serverStatus").get("mem", {})
    v = m.get("heapAllocatedBytes")
    return v / 2**20 if v is not None else None


def slope(xs, ys):
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    den = sum((x - mx) ** 2 for x in xs)
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den if den else 0.0


def test_memory_per_write_is_bounded():
    bucket = make_bucket("mem")
    flags = {"--memtable-max-age-secs": "20", "--change-log-max-mb": "4"}
    more = os.environ.get("CTL_MEM_EXTRA", "").split()
    flags.update(zip(more[::2], more[1::2]))          # CTL_MEM_EXTRA overrides the values above
    extra = [x for kv in flags.items() for x in kv]
    srv = Server(bucket, "mem", idle=30, extra=extra).start()
    try:
        c = srv.client()
        dbs = [f"w{i:02d}" for i in range(N_DB)]
        for d in dbs:
            c[d].t.insert_one({"_id": "first"})
        time.sleep(2)
        heap_before = heap_mb(c)
        writes = [0]
        lock = threading.Lock()
        stop = threading.Event()
        errors = []

        def writer(db):
            cl = srv.client(maxPoolSize=2)
            coll = cl[db].t
            nxt, k = time.time(), 0
            while not stop.is_set():
                nxt += 1 / RATE
                try:
                    coll.insert_one({"_id": k, "t": time.time(), "pad": PAD})
                    with lock:
                        writes[0] += 1
                except Exception as e:  # recorded, the test fails below
                    errors.append(repr(e)[:200])
                k += 1
                w = nxt - time.time()
                if w > 0:
                    time.sleep(w)
            cl.close()

        threads = [threading.Thread(target=writer, args=(d,), daemon=True) for d in dbs]
        t0 = time.time()
        for t in threads:
            t.start()
        samples = []          # (t, writes, rss, heap)
        while time.time() - t0 < WARM + WINDOW:
            time.sleep(5)
            with lock:
                w = writes[0]
            samples.append((time.time() - t0, w, rss_mb(srv.proc.pid), heap_mb(c)))
        stop.set()
        for t in threads:
            t.join(30)
        win = [s for s in samples if s[0] >= WARM]
        n_win = win[-1][1] - win[0][1]
        rss_kb = slope([s[1] for s in win], [s[2] for s in win]) * 1024
        heap_kb = slope([s[1] for s in win], [s[3] for s in win]) * 1024 if win[0][3] is not None else None
        # every database closes after --idle-secs without commands
        time.sleep(srv.idle + 30)
        heap_after = heap_mb(c)
        rss_after = rss_mb(srv.proc.pid)
        report = (f"writes in window {n_win}, RSS {rss_kb:.2f} KB/write, heap {heap_kb if heap_kb is None else round(heap_kb, 3)} KB/write, "
                  f"RSS end of writes {samples[-1][2]:.1f} MB, after close {rss_after:.1f} MB, heap before {heap_before} MB, "
                  f"end of writes {samples[-1][3]} MB, after close {heap_after} MB")
        print(report)
        with open(os.path.join(os.path.dirname(srv.log_path), "test_27_memory.txt"), "a") as f:
            f.write(f"{time.strftime('%F %T')} extra={extra} {report}\n")
        assert not errors, errors[:5]
        assert n_win > 0.8 * N_DB * RATE * (samples[-1][0] - win[0][0]), f"load did not reach its rate: {n_win} writes"
        assert rss_kb < RSS_KB_PER_WRITE, report
        if heap_kb is not None:
            assert heap_kb < HEAP_KB_PER_WRITE, report
            assert heap_after < heap_before + RETURN_SLACK_MB, report
    finally:
        srv.term()


def test_idle_cursors_expire():
    bucket = make_bucket("curs")
    srv = Server(bucket, "curs", idle=300, extra=["--cursor-timeout-secs", "6"]).start()
    try:
        c = srv.client()
        c.cdb.t.insert_many([{"_id": i, "pad": PAD} for i in range(500)])

        def status():
            s = c.admin.command("serverStatus")
            return s["metrics"]["cursor"]["open"]["total"], s["metrics"]["cursor"].get("timedOut"), heap_mb(c)

        open0, _, heap0 = status()
        # raw commands: the driver does not know these cursors and never kills them (an abandoned cursor)
        ids = [c.cdb.command("find", "t", batchSize=1)["cursor"]["id"] for _ in range(40)]
        assert all(ids)
        kept = c.cdb.command("find", "t", batchSize=1)["cursor"]["id"]
        # noCursorTimeout: kept for mongod's session timeout (30 minutes) instead of --cursor-timeout-secs
        no_timeout = c.cdb.command("find", "t", batchSize=1, noCursorTimeout=True)["cursor"]["id"]
        open1, _, heap1 = status()
        assert open1 == open0 + 42
        t0 = time.time()
        while time.time() - t0 < 16:                     # the kept cursor is used every 2 s, the others never
            c.cdb.command("getMore", kept, collection="t", batchSize=1)
            time.sleep(2)
        open2, timed_out, heap2 = status()
        print(f"cursors open {open0} -> {open1} -> {open2}, timed out {timed_out}, heap MB {heap0} -> {heap1} -> {heap2}")
        assert open2 == open0 + 2, (open0, open1, open2)
        assert timed_out == 40
        with pytest.raises(OperationFailure) as e:
            c.cdb.command("getMore", ids[0], collection="t")
        assert e.value.code == 43                        # CursorNotFound
        assert c.cdb.command("getMore", kept, collection="t", batchSize=1)["cursor"]["id"] == kept
        assert c.cdb.command("getMore", no_timeout, collection="t", batchSize=1)["cursor"]["id"] == no_timeout
        if heap1 is not None:
            # 40 cursors held ~40 x 499 documents of ~1 KB; closing them frees that
            assert heap2 < heap1 - 10, (heap1, heap2)
    finally:
        srv.term()
