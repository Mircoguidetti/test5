#!/usr/bin/env bash
# profile.sh WORKLOAD LAYOUT CLIENTS [SECONDS]
# CPU profile of the engine (1 core) under one workload: perf samples the engine process at 499 Hz while the load
# generator runs; prints the functions with the most self time and writes perf.data / a folded stack file in results/.
# Runs on the release binary as built (symbols are present, no debug info: inlined functions are counted in their caller).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; BIN="${BENCH_BIN:-$HERE/.bin}"; OUT="$HERE/results"; mkdir -p "$OUT"
PERF="${PERF:-$(ls /usr/lib/linux-tools/*/perf 2>/dev/null | head -1)}"
w=$1; layout=$2; clients=$3; secs=${4:-60}
if [ "$layout" = 100 ]; then dbs=100; docs=2000; else dbs=1; docs=200000; fi
pid=$("$HERE/server.sh" start fenvix 0)
taskset -c 1-3 "$BIN/loadgen" -mode load -dbs $dbs -docs $docs -clients 16 >/dev/null
taskset -c 1-3 "$BIN/loadgen" -mode run -workload "$w" -dbs $dbs -docs $docs -clients "$clients" -warmup $((secs + 40)) -duration 5 -pid "$pid" >/dev/null &
lg=$!
sleep 30
data="$OUT/perf-$w-$layout.data"
taskset -c 3 "$PERF" record -F 499 -g -p "$pid" -o "$data" -- sleep "$secs" 2>/dev/null
wait $lg || true
"$HERE/server.sh" stop fenvix
taskset -c 3 "$PERF" report -i "$data" --no-children --sort symbol --stdio 2>/dev/null | grep -E '^ +[0-9]' | head -25 \
  | tee "$OUT/perf-$w-$layout-top.txt"
# self time grouped by crate/module prefix (first path segment of the demangled name)
taskset -c 3 "$PERF" report -i "$data" --no-children --sort dso --stdio 2>/dev/null | grep -E '^ +[0-9]' | head -10 \
  > "$OUT/perf-$w-$layout-dso.txt"
taskset -c 3 "$PERF" script -i "$data" 2>/dev/null | gzip > "$OUT/perf-$w-$layout.script.gz"
