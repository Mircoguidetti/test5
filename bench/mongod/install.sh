#!/usr/bin/env bash
# Installs what the benchmark needs on one Linux machine: mongod 8.x (Docker image), the engine (release build of
# this checkout) and the load generator. Nothing is modified in the engine sources.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; ROOT="$(cd "$HERE/../.." && pwd)"
BIN="${BENCH_BIN:-$HERE/.bin}"; mkdir -p "$BIN"
MONGO_IMAGE="${MONGO_IMAGE:-mongo:8.0}"

# 1. mongod. Order asked: official apt repository, Docker, mongodb-memory-server (npm). On the machine of the first run the
#    first and third are blocked (repo.mongodb.org / fastdl.mongodb.org answer 403 through the network proxy), so Docker.
if ! docker info >/dev/null 2>&1; then
  echo "starting dockerd"; (dockerd >"$BIN/dockerd.log" 2>&1 &)
  for _ in $(seq 1 30); do docker info >/dev/null 2>&1 && break; sleep 1; done
fi
for i in 1 2 3 4 5; do docker pull "$MONGO_IMAGE" && break; echo "pull failed (attempt $i), retrying"; sleep $((i*10)); done
docker run --rm "$MONGO_IMAGE" mongod --version | head -1

# 2. engine, release build of main (the profile of the workspace Cargo.toml, unchanged).
(cd "$ROOT" && cargo build --release -p mongo-server)
cp "$ROOT/target/release/mongo-server" "$BIN/mongo-server"

# 3. load generator (official MongoDB Go driver v2).
(cd "$HERE/loadgen" && go build -o "$BIN/loadgen" .)
echo "ok: $BIN"
