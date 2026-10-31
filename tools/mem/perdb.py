"""Memory per open database (mem-growth branch): open N databases with one insert each (they stay open as writers),
measure RSS (and the live heap when serverStatus reports it) before and after, report MB per open database.
usage: perdb.py TAG [--n 200] [--bin B] [-- engine flags...]"""
import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "round3"))
from common import proc_stat  # noqa: E402
from engine import Engine, make_bucket  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("tag")
ap.add_argument("--n", type=int, default=200)
ap.add_argument("--bin", default=None)
ap.add_argument("flags", nargs="*")
a = ap.parse_args()
eng = Engine(a.tag, make_bucket(f"perdb-{a.tag}".lower()), ["--idle-secs", "3600"] + a.flags, bin=a.bin).start()
c = eng.client()


def heap():
    return c.admin.command("serverStatus").get("mem", {}).get("heapAllocatedBytes")


c.admin.command("ping")
c.warm.t.insert_one({"_id": 0})
time.sleep(3)
r0, h0 = proc_stat(eng.pid)["rss_mb"], heap()
with ThreadPoolExecutor(16) as ex:
    list(ex.map(lambda i: c[f"p{i:04d}"].t.insert_one({"_id": 0, "pad": "x" * 900}), range(a.n)))
time.sleep(20)
r1, h1 = proc_stat(eng.pid)["rss_mb"], heap()
eng.stop()
res = {"tag": a.tag, "n": a.n, "rss_before_mb": round(r0, 1), "rss_after_mb": round(r1, 1), "rss_mb_per_db": round((r1 - r0) / a.n, 3),
       "heap_mb_per_db": round((h1 - h0) / 2**20 / a.n, 3) if h0 is not None else None}
print("RESULT", json.dumps(res))
