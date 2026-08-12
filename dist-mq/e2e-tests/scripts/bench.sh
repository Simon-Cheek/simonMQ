#!/usr/bin/env bash
#
# Runs the benchmark across cluster sizes and prints a markdown summary.
#
#   ./e2e-tests/scripts/bench.sh                       # arms 1 3 5, 3 reps each
#   ARMS="1 3" REPS=5 ./e2e-tests/scripts/bench.sh
#
# An arm is a cluster size, so switching arms means redeploying. That is why
# reps are interleaved rather than run in blocks: a benchmark spread over
# fifteen minutes drifts as the machine warms, other work lands on it, and
# page cache fills, and running all of one arm before any of the next would
# charge that drift entirely to whichever arm went last.
#
# Deploying an arm wipes the previous cluster's volumes, so every measurement
# starts from an empty log. Carrying raft state across would let one arm
# inherit another's snapshot position.
#
# Tunables: ARMS REPS NAMESPACE IMAGE QUEUES PUBS SUBSCRIBERS RATE PAYLOAD
#           DURATION WARMUP OUT_DIR SKIP_BUILD CTR
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" # e2e-tests/scripts
E2E_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"                    # e2e-tests
ROOT_DIR="$(cd "$E2E_DIR/.." && pwd)"                      # dist-mq

ARMS="${ARMS:-1 3 5}"
REPS="${REPS:-3}"
NAMESPACE="${NAMESPACE:-dist-mq}"
IMAGE="${IMAGE:-dist-mq-e2e:local}"
QUEUES="${QUEUES:-3}"
PUBS="${PUBS:-1}"
SUBSCRIBERS="${SUBSCRIBERS:-1}"
RATE="${RATE:-200}"
PAYLOAD="${PAYLOAD:-64}"
DURATION="${DURATION:-30s}"
WARMUP="${WARMUP:-5s}"
OUT_DIR="${OUT_DIR:-$E2E_DIR/results}"
JOB_TIMEOUT="${JOB_TIMEOUT:-600}"
SKIP_BUILD="${SKIP_BUILD:-0}"
CTR="${CTR:-sudo k3s ctr}"

die() { echo "error: $*" >&2; exit 1; }

render() {
    local file="$1" arm="$2" rep="$3"
    local out
    out="$(cat "$file")"
    out="${out//__NAMESPACE__/$NAMESPACE}"
    out="${out//__IMAGE__/$IMAGE}"
    out="${out//__ARM__/$arm}"
    out="${out//__REP__/$rep}"
    out="${out//__QUEUES__/$QUEUES}"
    out="${out//__PUBS__/$PUBS}"
    out="${out//__SUBSCRIBERS__/$SUBSCRIBERS}"
    out="${out//__RATE__/$RATE}"
    out="${out//__PAYLOAD__/$PAYLOAD}"
    out="${out//__DURATION__/$DURATION}"
    out="${out//__WARMUP__/$WARMUP}"
    # Tallies only. Recording every token at benchmark rates would put the
    # sink's allocator inside the measurement.
    out="${out//__MODE__/count}"

    if leftover="$(grep -o '__[A-Z_]*__' <<<"$out" | sort -u)" && [[ -n "$leftover" ]]; then
        die "unsubstituted placeholder(s) in $file: $(tr '\n' ' ' <<<"$leftover")"
    fi
    printf '%s\n' "$out"
}

command -v kubectl >/dev/null || die "kubectl not found in PATH"
command -v docker >/dev/null || die "docker not found in PATH"

if [[ "$SKIP_BUILD" != "1" ]]; then
    echo "building $IMAGE"
    docker build -f "$E2E_DIR/Dockerfile" -t "$IMAGE" "$ROOT_DIR" || die "docker build failed"
    docker save "$IMAGE" | $CTR images import - >/dev/null || die "image import failed"
fi

mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR"/*.json "$OUT_DIR/summary.md"

total=$((REPS * $(wc -w <<<"$ARMS")))
n=0

for ((rep = 1; rep <= REPS; rep++)); do
    for arm in $ARMS; do
        n=$((n + 1))
        printf '[%d/%d] %s-node rep %d ... ' "$n" "$total" "$arm" "$rep"

        # WIPE because the replica count is changing; deploy.sh refuses a
        # resize against live volumes for exactly the reason it matters here.
        if ! WIPE=1 SKIP_BUILD=1 NAMESPACE="$NAMESPACE" "$ROOT_DIR/k8s/deploy.sh" "$arm" >/dev/null 2>&1; then
            echo "FAILED to deploy $arm-node cluster"
            continue
        fi

        render "$E2E_DIR/k8s/sink.template.yaml" "$arm" "$rep" | kubectl apply -f - >/dev/null
        kubectl rollout status deployment/dist-mq-sink -n "$NAMESPACE" --timeout=120s >/dev/null ||
            { echo "FAILED: sink not ready"; continue; }

        kubectl delete job dist-mq-bench -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null
        render "$E2E_DIR/k8s/bench-job.template.yaml" "$arm-node" "$rep" | kubectl apply -f - >/dev/null

        verdict=""
        for ((i = 0; i < JOB_TIMEOUT; i++)); do
            if [[ "$(kubectl get job dist-mq-bench -n "$NAMESPACE" -o jsonpath='{.status.succeeded}' 2>/dev/null)" == "1" ]]; then
                verdict="ok"
                break
            fi
            if [[ -n "$(kubectl get job dist-mq-bench -n "$NAMESPACE" -o jsonpath='{.status.failed}' 2>/dev/null)" ]]; then
                verdict="failed"
                break
            fi
            sleep 1
        done

        if [[ "$verdict" != "ok" ]]; then
            echo "FAILED (${verdict:-timeout})"
            kubectl logs -n "$NAMESPACE" -l app=dist-mq-bench --tail=5 2>/dev/null
            continue
        fi

        # The pod log carries progress lines as well as the result, so the
        # JSON is lifted out from between its markers.
        kubectl logs -n "$NAMESPACE" -l app=dist-mq-bench --tail=-1 2>/dev/null |
            sed -n '/---BENCH-JSON-BEGIN---/,/---BENCH-JSON-END---/p' |
            sed '1d;$d' >"$OUT_DIR/${arm}-node-${rep}.json"

        if [[ ! -s "$OUT_DIR/${arm}-node-${rep}.json" ]]; then
            echo "FAILED: no result JSON in pod log"
            rm -f "$OUT_DIR/${arm}-node-${rep}.json"
            continue
        fi
        echo "ok"
    done
done

echo
# aggregate runs from the image so the host needs no Go toolchain.
docker run --rm -v "$OUT_DIR:/results" --entrypoint /app/aggregate "$IMAGE" -dir /results |
    tee "$OUT_DIR/summary.md"
echo
echo "per-run JSON in $OUT_DIR, summary in $OUT_DIR/summary.md"
