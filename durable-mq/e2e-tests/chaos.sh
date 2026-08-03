#!/usr/bin/env bash
# Builds durable-mq once, then repeatedly starts it, lets it run for a
# random 1-20s, SIGKILLs it (a real crash, not a graceful shutdown), waits
# a random 1-5s, and restarts — forever. Run your e2e publisher/subscriber
# against it in another terminal to exercise crash recovery under load.
#
# durable-wal/ is left alone between restarts on purpose — that's the
# whole point, recovering from whatever was on disk when it got killed.
#
# Override defaults via env vars, e.g.:
#   MIN_UP=5 MAX_UP=30 ./e2e-tests/chaos.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_PATH="$SCRIPT_DIR/durable-mq-server"
SERVER_LOG="$SCRIPT_DIR/chaos-server.log"

MIN_UP="${MIN_UP:-1}"
MAX_UP="${MAX_UP:-20}"
MIN_DOWN="${MIN_DOWN:-1}"
MAX_DOWN="${MAX_DOWN:-5}"

cd "$ROOT_DIR"

echo "Building durable-mq -> $BIN_PATH"
go build -o "$BIN_PATH" . || exit 1

pid=""
cleanup() {
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        echo "cleanup: killing durable-mq (pid $pid)"
        kill -9 "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null
    fi
}
trap cleanup EXIT
trap 'exit 0' INT TERM

rand_between() {
    echo $(( RANDOM % ($2 - $1 + 1) + $1 ))
}

echo "Chaos loop starting. Server output -> $SERVER_LOG"
echo "Press Ctrl+C to stop."

iteration=0
while true; do
    iteration=$((iteration + 1))
    up_time=$(rand_between "$MIN_UP" "$MAX_UP")
    down_time=$(rand_between "$MIN_DOWN" "$MAX_DOWN")

    echo "=== iteration $iteration: launching ($(date +%H:%M:%S)) ===" >>"$SERVER_LOG"
    "$BIN_PATH" >>"$SERVER_LOG" 2>&1 &
    pid=$!

    sleep 0.3
    if ! kill -0 "$pid" 2>/dev/null; then
        echo "[$iteration] durable-mq exited immediately — check $SERVER_LOG — retrying in 2s"
        sleep 2
        continue
    fi

    echo "[$iteration] up (pid $pid) for ${up_time}s"
    sleep "$up_time"

    if kill -0 "$pid" 2>/dev/null; then
        echo "[$iteration] crashing (pid $pid)"
        kill -9 "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null
    else
        echo "[$iteration] durable-mq already exited on its own before the crash — check $SERVER_LOG"
    fi

    echo "[$iteration] down for ${down_time}s"
    sleep "$down_time"
done
