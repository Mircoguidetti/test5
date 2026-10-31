#!/usr/bin/env bash
# server.sh start mongod|fenvix CPUS   -> starts the server pinned to CPUS (e.g. 0 or 0-1) on an empty data directory,
#                                          prints its process id on stdout
# server.sh stop  mongod|fenvix
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="${BENCH_BIN:-$HERE/.bin}"; DATA="${BENCH_DATA:-$HOME/benchdata}"
MONGO_IMAGE="${MONGO_IMAGE:-mongo:8.0}"; PORT=27017
cmd=$1; what=$2; cpus=${3:-0}
port_free() { ! (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; }
wait_port_free() {
  pkill -x mongo-server 2>/dev/null || true; docker rm -f bench-mongod >/dev/null 2>&1 || true
  for _ in $(seq 1 120); do port_free && return 0; sleep 1; done
  echo "port $PORT still busy" >&2; exit 1
}
case "$cmd/$what" in
start/mongod)
  wait_port_free
  rm -rf "$DATA/mongod"; mkdir -p "$DATA/mongod"
  # Journaling is always on in mongod 8 (it cannot be turned off for WiredTiger). Defaults otherwise: WiredTiger cache
  # 50% of (RAM - 1 GB), snappy block compression, journal commit interval 100 ms (j:true writes force a journal flush).
  docker run -d --name bench-mongod --network host --cpuset-cpus "$cpus" -v "$DATA/mongod:/data/db" "$MONGO_IMAGE" \
    mongod --bind_ip 127.0.0.1 --port $PORT >/dev/null
  for _ in $(seq 1 60); do
    docker exec bench-mongod mongosh --quiet --port $PORT --eval 'db.runCommand({ping:1}).ok' >/dev/null 2>&1 && break; sleep 1
  done
  p=$(docker inspect -f '{{.State.Pid}}' bench-mongod)
  [ "$p" -gt 0 ] || { docker logs bench-mongod 2>&1 | tail -5 >&2; exit 1; }
  echo "$p" ;;
stop/mongod)
  docker rm -f bench-mongod >/dev/null 2>&1 || true ;;
start/fenvix)
  wait_port_free
  rm -rf "$DATA/fenvix"; mkdir -p "$DATA/fenvix/store"
  # Default mode of main except the object store: `--store local` (a directory on the local disk) instead of S3.
  # Durability default: immediate (a write is acknowledged once its WAL object is written to the store).
  taskset -c "$cpus" "$BIN/mongo-server" --listen 127.0.0.1:$PORT --store local --local-dir "$DATA/fenvix/store" \
    >"$DATA/fenvix/server.log" 2>&1 &
  p=$!
  for _ in $(seq 1 60); do grep -q "listening on" "$DATA/fenvix/server.log" && break; sleep 0.5; done
  grep -q "listening on" "$DATA/fenvix/server.log" || { cat "$DATA/fenvix/server.log" >&2; exit 1; }
  echo $p ;;
stop/fenvix)
  wait_port_free ;;
*) echo "usage: $0 start|stop mongod|fenvix [cpus]" >&2; exit 2 ;;
esac
