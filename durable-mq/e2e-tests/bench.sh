#!/usr/bin/env bash
#
# Runs the full performance comparison and prints a markdown summary.
#
#   push-mq         no WAL, separate implementation
#   durable-off     durable-mq with -wal-mode off      (no log written)
#   durable-nosync  durable-mq with -wal-mode nosync   (log written, no fsync)
#   durable-sync    durable-mq with -wal-mode sync     (fsync per append)
#
# durable-off -> durable-sync is the cost of durability. nosync -> sync is the
# cost of fsync alone. push-mq -> durable-off is implementation drift between
# the two codebases and is not a durability result.
#
# Arms are interleaved rather than run in blocks
#
# Local usage (broker, load generator and sink all on this machine — fine for
# a smoke test, but the three compete for CPU, so see REMOTE below for real
# numbers):
#
#   ./e2e-tests/bench.sh
#
# Split usage, which is what real runs should use. On the broker machine start
# the server yourself for one arm at a time; on the load machine run:
#
#   BROKER=http://192.168.1.50:8081 SINK_ADVERTISE=http://192.168.1.60 \
#   MANAGE_BROKER=0 ARMS=durable-sync REPS=10 ./e2e-tests/bench.sh
#
# Tunables:
#   ARMS REPS DURATION WARMUP RATE QUEUES PUBS_PER_QUEUE SUBS_PER_QUEUE
#   PAYLOAD BROKER SINK_ADVERTISE SINK_BASE_PORT MANAGE_BROKER OUT_DIR
#   MAX_SEG_SIZE CKPT_THRESHOLD
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_DIR="$(cd "$ROOT_DIR/.." && pwd)"
BIN_DIR="$SCRIPT_DIR/bin"

ARMS="${ARMS:-push-mq durable-off durable-nosync durable-sync}"
REPS="${REPS:-10}"
DURATION="${DURATION:-30s}"
WARMUP="${WARMUP:-5s}"
RATE="${RATE:-200}"
QUEUES="${QUEUES:-3}"
PUBS_PER_QUEUE="${PUBS_PER_QUEUE:-1}"
SUBS_PER_QUEUE="${SUBS_PER_QUEUE:-1}"
PAYLOAD="${PAYLOAD:-64}"

# WAL geometry. Empty/0 means the broker's production defaults (128MB
# segments, checkpoint every 4). Turn both down to make the checkpoint path
# actually run inside a 30s benchmark instead of needing ~384MB of log.
MAX_SEG_SIZE="${MAX_SEG_SIZE:-}"
CKPT_THRESHOLD="${CKPT_THRESHOLD:-0}"

BROKER="${BROKER:-http://localhost:8081}"
SINK_ADVERTISE="${SINK_ADVERTISE:-http://localhost}"
SINK_BASE_PORT="${SINK_BASE_PORT:-9090}"
# 1: this script starts and stops the broker between arms. 0: the broker is
# somewhere else and you restart it yourself between arms.
MANAGE_BROKER="${MANAGE_BROKER:-1}"
OUT_DIR="${OUT_DIR:-$SCRIPT_DIR/results}"

WAL_DIR="$SCRIPT_DIR/bench-wal"
SINK_URL="$SINK_ADVERTISE:$SINK_BASE_PORT"

broker_pid=""
sink_pid=""

cleanup() {
    [[ -n "$sink_pid" ]] && kill "$sink_pid" 2>/dev/null
    [[ -n "$broker_pid" ]] && kill -9 "$broker_pid" 2>/dev/null
    wait 2>/dev/null
    return 0
}
trap cleanup EXIT
trap 'echo; echo "interrupted"; exit 130' INT TERM

die() { echo "error: $*" >&2; exit 1; }

# wait_port polls until something answers on host:port. Guessing with sleep is
# how a slow bind turns into a run of silently failed requests that looks
# exactly like the broker dropping messages.
wait_port() {
    local url="$1" tries=200
    for ((i = 0; i < tries; i++)); do
        if [[ "$(curl -s -o /dev/null -w '%{http_code}' "$url" 2>/dev/null)" != "000" ]]; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

build() {
    echo "building..."
    mkdir -p "$BIN_DIR"
    (cd "$ROOT_DIR" && go build -o "$BIN_DIR/durable-mq" .) || die "building durable-mq"
    (cd "$ROOT_DIR" && go build -o "$BIN_DIR/loadgen" ./e2e-tests/loadgen) || die "building loadgen"
    (cd "$ROOT_DIR" && go build -o "$BIN_DIR/subscriber" ./e2e-tests/subscriber) || die "building subscriber"
    (cd "$ROOT_DIR" && go build -o "$BIN_DIR/aggregate" ./e2e-tests/aggregate) || die "building aggregate"
    if [[ "$ARMS" == *push-mq* ]]; then
        [[ -d "$REPO_DIR/push-mq" ]] || die "push-mq not found at $REPO_DIR/push-mq"
        (cd "$REPO_DIR/push-mq" && go build -o "$BIN_DIR/push-mq" .) || die "building push-mq"
    fi
}

start_broker() {
    local arm="$1"
    [[ "$MANAGE_BROKER" == "1" ]] || return 0

    # Every repetition starts from an empty log. Carrying segments across runs
    # would let one arm inherit another's checkpoint state and quietly change
    # what the next measurement means.
    rm -rf "$WAL_DIR"

    case "$arm" in
        push-mq)
            "$BIN_DIR/push-mq" >"$SCRIPT_DIR/broker.log" 2>&1 &
            ;;
        durable-off|durable-nosync|durable-sync)
            local mode="${arm#durable-}"
            local extra=()
            [[ -n "$MAX_SEG_SIZE" ]] && extra+=(-max-seg-size "$MAX_SEG_SIZE")
            [[ "$CKPT_THRESHOLD" != "0" ]] && extra+=(-checkpoint-threshold "$CKPT_THRESHOLD")
            # ${a[@]+"${a[@]}"} rather than "${a[@]}": under `set -u`, bash 3.2
            # (which is what macOS ships) treats an empty array expansion as an
            # unbound variable, and empty is the default path here.
            "$BIN_DIR/durable-mq" -port 8081 -wal-dir "$WAL_DIR" -wal-mode "$mode" \
                ${extra[@]+"${extra[@]}"} \
                >"$SCRIPT_DIR/broker.log" 2>&1 &
            ;;
        *)
            die "unknown arm: $arm"
            ;;
    esac
    broker_pid=$!
    wait_port "$BROKER/queues" || die "broker for arm $arm never came up; see $SCRIPT_DIR/broker.log"
}

# await_remote_broker handles the arm switch when the broker lives on another
# machine. Interleaving still matters there — arguably more, since a manual run
# is spread over a longer wall-clock window — so rather than forcing one arm
# per invocation this pauses for the restart and carries on.
await_remote_broker() {
    local arm="$1"
    if [[ -t 0 ]]; then
        echo
        echo "  >> on the broker machine: stop it, wipe its WAL dir, restart for arm '$arm'"
        echo "  >> then press Enter here"
        read -r </dev/tty
        printf '     resuming ... '
    fi
    wait_port "$BROKER/queues" ||
        die "nothing answering at $BROKER — check the broker is running and reachable from this machine"
}

stop_broker() {
    [[ "$MANAGE_BROKER" == "1" ]] || return 0
    [[ -n "$broker_pid" ]] || return 0
    kill -9 "$broker_pid" 2>/dev/null
    wait "$broker_pid" 2>/dev/null
    broker_pid=""
    rm -rf "$WAL_DIR"
}

main() {
    command -v go >/dev/null || die "go not found in PATH"
    command -v curl >/dev/null || die "curl not found in PATH"
    build

    # Clear only the arms about to be measured. A split run against a remote
    # broker does one arm per invocation, and wiping the whole directory would
    # destroy the arms already collected.
    mkdir -p "$OUT_DIR"
    for arm in $ARMS; do
        rm -f "$OUT_DIR/${arm}-"*.json
    done
    rm -f "$OUT_DIR/summary.md"

    # One sink process serves every arm and repetition; loadgen zeroes its
    # counters at the start of each run.
    "$BIN_DIR/subscriber" -base-port "$SINK_BASE_PORT" -n "$SUBS_PER_QUEUE" -mode count \
        >"$SCRIPT_DIR/sink.log" 2>&1 &
    sink_pid=$!
    wait_port "http://localhost:$SINK_BASE_PORT/stats" || die "sink never came up; see $SCRIPT_DIR/sink.log"

    local total=$((REPS * $(wc -w <<<"$ARMS")))
    local n=0
    echo "running $total measurements: $(wc -w <<<"$ARMS") arms x $REPS reps, ${DURATION} each (${WARMUP} warm-up)"
    echo

    for ((rep = 1; rep <= REPS; rep++)); do
        for arm in $ARMS; do
            n=$((n + 1))
            printf '[%d/%d] %-16s rep %d ... ' "$n" "$total" "$arm" "$rep"

            if [[ "$MANAGE_BROKER" == "1" ]]; then
                start_broker "$arm"
            else
                await_remote_broker "$arm"
            fi
            "$BIN_DIR/loadgen" \
                -broker "$BROKER" -sink "$SINK_URL" \
                -arm "$arm" -rep "$rep" \
                -queues "$QUEUES" -publishers-per-queue "$PUBS_PER_QUEUE" \
                -subscribers-per-queue "$SUBS_PER_QUEUE" \
                -rate "$RATE" -payload "$PAYLOAD" \
                -duration "$DURATION" -warmup "$WARMUP" \
                -out "$OUT_DIR/${arm}-${rep}.json" \
                >"$SCRIPT_DIR/loadgen.log" 2>&1
            local rc=$?
            stop_broker

            if [[ $rc -ne 0 ]]; then
                echo "FAILED (see $SCRIPT_DIR/loadgen.log)"
                tail -3 "$SCRIPT_DIR/loadgen.log" >&2
            else
                echo "ok"
            fi
        done
    done

    echo
    "$BIN_DIR/aggregate" -dir "$OUT_DIR" | tee "$OUT_DIR/summary.md"
    echo
    echo "per-run JSON in $OUT_DIR, summary in $OUT_DIR/summary.md"
}

main "$@"
