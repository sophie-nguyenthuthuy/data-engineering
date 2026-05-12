#!/usr/bin/env bash
# Start a 3-node cluster locally for development/testing.
set -e

DATA_DIR="${DATA_DIR:-/tmp/raft-metadata}"
PORT_BASE="${PORT_BASE:-8001}"

p1=$PORT_BASE
p2=$((PORT_BASE + 1))
p3=$((PORT_BASE + 2))

echo "Starting node1 on :$p1"
python server.py \
  --id node1 \
  --port "$p1" \
  --peers "node2=localhost:$p2,node3=localhost:$p3" \
  --data-dir "$DATA_DIR" &
PID1=$!

echo "Starting node2 on :$p2"
python server.py \
  --id node2 \
  --port "$p2" \
  --peers "node1=localhost:$p1,node3=localhost:$p3" \
  --data-dir "$DATA_DIR" &
PID2=$!

echo "Starting node3 on :$p3"
python server.py \
  --id node3 \
  --port "$p3" \
  --peers "node1=localhost:$p1,node2=localhost:$p2" \
  --data-dir "$DATA_DIR" &
PID3=$!

echo "Cluster started. PIDs: $PID1 $PID2 $PID3"
echo "Press Ctrl-C to stop."

trap "kill $PID1 $PID2 $PID3 2>/dev/null; echo Stopped." INT TERM
wait
