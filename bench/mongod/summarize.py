#!/usr/bin/env python3
"""Reads results/raw.jsonl and prints, per cell, the median of the full runs (and the sweep), as plain text."""
import json, os, statistics as st, sys
from collections import defaultdict
RAW = os.path.join(os.environ.get("BENCH_RESULTS", os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")), "raw.jsonl")
recs = [json.loads(l) for l in open(RAW) if l.strip()]
full, sweep = defaultdict(list), defaultdict(list)
for r in recs:
    if r.get("phase") in ("full", "sweep"):
        k = (r["workload"], r["layout"], r["cores"], r["server"])
        (full if r["phase"] == "full" else sweep)[k].append(r)
med = {}
for k, rs in sorted(full.items()):
    m = {f: st.median(r[f] for r in rs) for f in ("ops_per_s", "p50_ms", "p99_ms", "server_cores_used", "gen_cores_used")}
    m["ops_per_cpu_s"] = st.median(r["ops_per_s"] / r["server_cores_used"] for r in rs)
    m["ops_per_core"] = m["ops_per_s"] / k[2]
    m["clients"], m["n"] = rs[0]["clients"], len(rs)
    m["spread"] = (max(r["ops_per_s"] for r in rs) - min(r["ops_per_s"] for r in rs)) / m["ops_per_s"]
    med[k] = m
json.dump({"|".join(map(str, k)): v for k, v in med.items()}, open(RAW.replace("raw.jsonl", "medians.json"), "w"), indent=1)
for k, m in med.items():
    w, layout, cores, srv = k
    print(f"{w} {layout}db {cores}core {srv}: {m['ops_per_s']:.0f} op/s, {m['ops_per_core']:.0f} op/s per core assigned, "
          f"{m['ops_per_cpu_s']:.0f} op per CPU-second used, CPU used {m['server_cores_used']:.2f} cores, "
          f"p50 {m['p50_ms']:.2f} ms, p99 {m['p99_ms']:.2f} ms, clients {m['clients']}, generator {m['gen_cores_used']:.2f} cores, "
          f"runs {m['n']}, spread {100*m['spread']:.1f}%")
print()
for (w, layout, cores, srv), m in med.items():
    if srv != "fenvix":
        continue
    o = med.get((w, layout, cores, "mongod"))
    if o:
        print(f"ratio fenvix/mongod {w} {layout}db {cores}core: per core assigned {m['ops_per_core']/o['ops_per_core']:.2f}, "
              f"per CPU-second used {m['ops_per_cpu_s']/o['ops_per_cpu_s']:.2f}")
print()
for k, rs in sorted(sweep.items()):
    print(" ".join(map(str, k)), "sweep:", ", ".join(f"{r['clients']}c={r['ops_per_s']:.0f}/{r['server_cores_used']:.2f}cpu" for r in rs))
