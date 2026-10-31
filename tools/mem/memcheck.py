"""Memory per write, and whether it comes back (mem-growth branch).

N databases on MinIO, RATE inserts/s each of ~1 KB (tools/round3/load.py), RSS every 5 s. Then the writes stop; the
engine closes the idle databases after --idle-secs (each close flushes the memtable to an L0 SST and frees it) and the
script keeps sampling for IDLE seconds.

Reported:
  kb_per_write        slope of RSS over the write phase (least squares, after the first WARM seconds) / writes per second
  rss_peak_mb         highest RSS
  rss_after_close_mb  RSS at the end of the idle phase (every database closed)
  returned_pct        share of the growth of the write phase given back after the close

usage: memcheck.py TAG [--n 50] [--rate 2] [--secs 300] [--idle 120] [--idle-close 30] [--bin B] [-- engine flags...]
The engine's environment is the script's (MALLOC_ARENA_MAX, _RJEM_MALLOC_CONF... pass through)."""
import argparse
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "round3"))
from common import WORK, dump, proc_stat  # noqa: E402
from engine import Engine, make_bucket  # noqa: E402


def slope(points):
    """Least-squares slope of [(t, y)]."""
    n = len(points)
    mt = sum(t for t, _ in points) / n
    my = sum(y for _, y in points) / n
    den = sum((t - mt) ** 2 for t, _ in points)
    return sum((t - mt) * (y - my) for t, y in points) / den if den else 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("tag")
    ap.add_argument("--n", type=int, default=50)
    ap.add_argument("--rate", type=float, default=2)
    ap.add_argument("--secs", type=float, default=300)
    ap.add_argument("--warm", type=float, default=60)
    ap.add_argument("--idle", type=float, default=120)
    ap.add_argument("--idle-close", type=int, default=30)
    ap.add_argument("--bin", default=None)
    ap.add_argument("--mix", default="write", choices=["write", "half", "read"])
    ap.add_argument("--prefill", type=int, default=0, help="documents of ~1 KB put in `items` of every database first (reads use them)")
    ap.add_argument("--out", default=None)
    ap.add_argument("flags", nargs="*")
    a = ap.parse_args()
    bucket = make_bucket(f"memc-{a.tag}".lower().replace("_", "-"))
    eng = Engine(a.tag, bucket, ["--idle-secs", str(a.idle_close)] + a.flags, bin=a.bin).start()
    dbs = [f"m{i:03d}" for i in range(a.n)]
    c = eng.client()
    if a.prefill:
        from engine import prefill
        prefill(eng, dbs, a.prefill)
    else:
        for d in dbs:
            c[d].items.insert_one({"_id": 0})
    samples = []
    # the engine's own request counters (MinIO's are shared by every process that uses it)
    req0 = eng.stats().get("requests")
    t0 = time.time()
    load_out = os.path.join(WORK, f"memc-{a.tag}-load.json")
    load = subprocess.Popen([sys.executable, os.path.join(HERE, "..", "round3", "load.py"), "--port", str(eng.port), "--dbs", ",".join(dbs),
                             "--rate", str(a.rate), "--mix", a.mix, "--secs", str(a.secs), "--out", load_out,
                             "--procs", "2", "--t-start", str(t0 + 2)] + (["--prefill", str(a.prefill)] if a.prefill else []))

    def sample(phase):
        s = proc_stat(eng.pid)
        s["t"] = round(time.time() - t0, 1)
        s["phase"] = phase
        samples.append(s)
        print(json.dumps(s), flush=True)

    while load.poll() is None:
        sample("write")
        time.sleep(5)
    t_stop = time.time()
    req = None
    req1 = eng.stats().get("requests")
    if req0 and req1:
        n_w = json.load(open(load_out))["n_write"]
        put, get = req1["billedPutTier"] - req0["billedPutTier"], req1["billedGetTier"] - req0["billedGetTier"]
        req = {"put_tier": put, "get_tier": get, "put_per_write": round(put / max(1, n_w), 4), "get_per_write": round(get / max(1, n_w), 4)}
    while time.time() - t_stop < a.idle:
        sample("idle")
        time.sleep(5)
    sample("idle")
    status, metrics = eng.admin("GET", "/v1/metrics/databases")
    eng.stop()
    ld = json.load(open(load_out))
    writes = ld["n_write"]
    wps = writes / a.secs
    pts = [(s["t"], s["rss_mb"]) for s in samples if s["phase"] == "write" and s["t"] >= a.warm]
    kb = slope(pts) * 1024 / wps if len(pts) > 3 and wps else None
    base = samples[0]["rss_mb"]
    peak = max(s["rss_mb"] for s in samples)
    end_w = [s for s in samples if s["phase"] == "write"][-1]["rss_mb"]
    after = samples[-1]["rss_mb"]
    res = {"tag": a.tag, "flags": a.flags, "n": a.n, "rate": a.rate, "secs": a.secs, "writes": writes,
           "write_p50_ms": ld.get("write_p50_ms"), "write_p99_ms": ld.get("write_p99_ms"), "errors": ld.get("errors"),
           "kb_per_write": round(kb, 2) if kb is not None else None, "rss_start_mb": round(base, 1),
           "rss_end_of_writes_mb": round(end_w, 1), "rss_peak_mb": round(peak, 1), "rss_after_close_mb": round(after, 1),
           "returned_pct": round(100 * (end_w - after) / (end_w - base), 1) if end_w > base else None,
           "requests_write_phase": req, "metrics_status": status, "samples": samples}
    print("RESULT", json.dumps({k: v for k, v in res.items() if k != "samples"}))
    dump(a.out or os.path.join(WORK, f"memc-{a.tag}.json"), res)


if __name__ == "__main__":
    main()
