#!/usr/bin/env python3
"""Runs the whole matrix: server (mongod, fenvix) x database cores (1, 2) x layout (1 db x 200 000 docs,
100 dbs x 2 000 docs) x workload (read, rw, insert).

For every cell:
  1. client sweep with short runs (10 s warm-up, 20 s measured) over 1, 2, 4, ... clients, stopped once the
     server's CPU is >= 95% of its cores and the throughput no longer grows by 5%, or once it falls;
  2. the saturation point = the smallest client count whose throughput is within 3% of the best of the sweep;
  3. three full runs at that point (60 s warm-up, 180 s measured).

Each server starts on an empty data directory for every (server, cores, layout); the workloads run in the order
read, rw, insert (insert grows the data, so it runs last). Results are appended to results/raw.jsonl; a cell whose
three full runs are already there is skipped, so the script can be stopped and started again.

Usage: run_matrix.py [--servers mongod,fenvix] [--cores 1,2] [--layouts 1,100] [--workloads read,rw,insert]
                     [--warmup 60] [--duration 180] [--reps 3]
"""
import argparse, json, os, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.environ.get("BENCH_BIN", os.path.join(HERE, ".bin"))
RES = os.environ.get("BENCH_RESULTS", os.path.join(HERE, "results"))
RAW = os.path.join(RES, "raw.jsonl")
NCPU = os.cpu_count()
LAYOUTS = {1: (1, 200_000), 100: (100, 2_000)}
SWEEP = [1, 2, 4, 8, 16, 32, 64, 128, 256]


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def records():
    if not os.path.exists(RAW):
        return []
    with open(RAW) as f:
        return [json.loads(l) for l in f if l.strip()]


def append(rec):
    os.makedirs(RES, exist_ok=True)
    with open(RAW, "a") as f:
        f.write(json.dumps(rec) + "\n")


def gen_cpus(db_cores):
    return f"{db_cores}-{NCPU - 1}"


def loadgen(args, db_cores):
    env = dict(os.environ, GOGC="400")
    cmd = ["taskset", "-c", gen_cpus(db_cores), os.path.join(BIN, "loadgen")] + args
    out = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if out.returncode != 0:
        raise RuntimeError(f"loadgen failed: {out.stderr[-2000:]}")
    if out.stderr.strip():
        log("loadgen stderr:", out.stderr.strip()[-500:])
    return json.loads(out.stdout.strip().splitlines()[-1])


def server(cmd, name, cpus=""):
    out = subprocess.run([os.path.join(HERE, "server.sh"), cmd, name] + ([cpus] if cpus else []),
                         capture_output=True, text=True, check=True)
    return out.stdout.strip()


def one_run(base, cell, clients, warmup, duration, phase, rep):
    seq = len(records()) + 1
    r = loadgen(["-mode", "run", "-workload", cell["workload"], "-dbs", str(cell["dbs"]),
                 "-docs", str(cell["docs"]), "-clients", str(clients), "-warmup", str(warmup),
                 "-duration", str(duration), "-pid", str(cell["pid"]),
                 "-insert-base", str(seq * 100_000_000_000)], cell["cores"])
    r.update({k: cell[k] for k in ("server", "cores", "layout", "workload")}, phase=phase, rep=rep,
             ts=time.strftime("%Y-%m-%dT%H:%M:%S"))
    append(r)
    log(f"{phase:5} {cell['server']:6} cores={cell['cores']} layout={cell['layout']:3} {cell['workload']:6} "
        f"clients={clients:3} ops/s={r['ops_per_s']:9.0f} srv_cores={r['server_cores_used']:.2f} "
        f"gen_cores={r['gen_cores_used']:.2f} p50={r['p50_ms']:.2f} p99={r['p99_ms']:.2f} err={r['errors_measured']}")
    return r


def sweep(cell):
    best, prev, pts = None, None, []
    for c in SWEEP:
        r = one_run(None, cell, c, 10, 20, "sweep", 0)
        pts.append(r)
        if best is None or r["ops_per_s"] > best["ops_per_s"]:
            best = r
        saturated = r["server_cores_used"] >= 0.95 * cell["cores"]
        if prev is not None:
            gain = r["ops_per_s"] / max(prev["ops_per_s"], 1e-9)
            if gain < 0.97 or (saturated and gain < 1.05):
                break
        prev = r
    chosen = min((p for p in pts if p["ops_per_s"] >= 0.97 * best["ops_per_s"]), key=lambda p: p["clients"])
    return chosen["clients"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--servers", default="mongod,fenvix")
    ap.add_argument("--cores", default="1,2")
    ap.add_argument("--layouts", default="1,100")
    ap.add_argument("--workloads", default="read,rw,insert")
    ap.add_argument("--warmup", type=int, default=60)
    ap.add_argument("--duration", type=int, default=180)
    ap.add_argument("--reps", type=int, default=3)
    a = ap.parse_args()
    for srv in a.servers.split(","):
        for cores in map(int, a.cores.split(",")):
            for layout in map(int, a.layouts.split(",")):
                def done(w):
                    return sum(1 for r in records() if r["phase"] == "full" and r["server"] == srv and
                               r["cores"] == cores and r["layout"] == layout and r["workload"] == w) >= a.reps
                todo = [w for w in a.workloads.split(",") if not done(w)]
                if not todo:
                    continue
                dbs, docs = LAYOUTS[layout]
                cpus = "0" if cores == 1 else f"0-{cores - 1}"
                pid = server("start", srv, cpus)
                log(f"started {srv} on cpus {cpus}, pid {pid}")
                try:
                    ld = loadgen(["-mode", "load", "-dbs", str(dbs), "-docs", str(docs), "-clients", "16"], cores)
                    ld.update(server=srv, cores=cores, layout=layout, phase="load")
                    append(ld)
                    log(f"loaded {dbs} x {docs} in {ld['seconds']:.1f} s")
                    for w in todo:
                        cell = dict(server=srv, cores=cores, layout=layout, workload=w, dbs=dbs, docs=docs, pid=pid)
                        clients = sweep(cell)
                        log(f"saturation point: {clients} clients")
                        for rep in range(1, a.reps + 1):
                            one_run(None, cell, clients, a.warmup, a.duration, "full", rep)
                finally:
                    server("stop", srv)


if __name__ == "__main__":
    sys.exit(main())
