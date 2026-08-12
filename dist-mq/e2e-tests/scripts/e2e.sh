#!/usr/bin/env bash
#
# Builds the e2e image, deploys the sink, runs the durability suite as a Job
# and reports its verdict. Assumes ./k8s/deploy.sh has already put a cluster up
# in the same namespace.
#
#   ./e2e-tests/scripts/e2e.sh                   # against whatever is deployed
#   MESSAGES=2000 ./e2e-tests/scripts/e2e.sh     # longer run
#
# To exercise failover, start chaos.sh in another terminal first and leave it
# running. The suite asserts the same thing either way — that is the point of
# keeping chaos out of it.
#
# Tunables: NAMESPACE IMAGE QUEUES SUBSCRIBERS MESSAGES SKIP_BUILD CTR
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" # e2e-tests/scripts
E2E_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"                    # e2e-tests
ROOT_DIR="$(cd "$E2E_DIR/.." && pwd)"                      # dist-mq — docker build context

NAMESPACE="${NAMESPACE:-dist-mq}"
IMAGE="${IMAGE:-dist-mq-e2e:local}"
QUEUES="${QUEUES:-2}"
SUBSCRIBERS="${SUBSCRIBERS:-2}"
MESSAGES="${MESSAGES:-500}"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-120s}"
TEST_TIMEOUT="${TEST_TIMEOUT:-15m}"
JOB_TIMEOUT="${JOB_TIMEOUT:-900}" # seconds to wait for the Job to settle
SKIP_BUILD="${SKIP_BUILD:-0}"
CTR="${CTR:-sudo k3s ctr}"

die() { echo "error: $*" >&2; exit 1; }

render() {
    local file="$1"
    local out
    out="$(cat "$file")"
    out="${out//__NAMESPACE__/$NAMESPACE}"
    out="${out//__IMAGE__/$IMAGE}"
    out="${out//__QUEUES__/$QUEUES}"
    out="${out//__SUBSCRIBERS__/$SUBSCRIBERS}"
    out="${out//__MESSAGES__/$MESSAGES}"
    out="${out//__DRAIN_TIMEOUT__/$DRAIN_TIMEOUT}"
    out="${out//__TEST_TIMEOUT__/$TEST_TIMEOUT}"

    if leftover="$(grep -o '__[A-Z_]*__' <<<"$out" | sort -u)" && [[ -n "$leftover" ]]; then
        die "unsubstituted placeholder(s) in $file: $(tr '\n' ' ' <<<"$leftover")"
    fi
    printf '%s\n' "$out"
}

command -v kubectl >/dev/null || die "kubectl not found in PATH"
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 ||
    die "namespace $NAMESPACE does not exist — run ./k8s/deploy.sh first"

if [[ "$SKIP_BUILD" != "1" ]]; then
    command -v docker >/dev/null || die "docker not found; rerun with SKIP_BUILD=1"
    echo "building $IMAGE"
    docker build -f "$E2E_DIR/Dockerfile" -t "$IMAGE" "$ROOT_DIR" || die "docker build failed"
    echo "importing $IMAGE into k3s"
    docker save "$IMAGE" | $CTR images import - >/dev/null || die "image import failed"
fi

echo "deploying sink ($SUBSCRIBERS subscriber(s))"
render "$E2E_DIR/k8s/sink.template.yaml" | kubectl apply -f - >/dev/null
kubectl rollout status deployment/dist-mq-sink -n "$NAMESPACE" --timeout=120s ||
    die "sink did not become ready"

# The Job's pod spec is immutable, so a rerun replaces it rather than patching.
kubectl delete job dist-mq-durability -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null

echo "running durability: $MESSAGES message(s), $QUEUES queue(s), $SUBSCRIBERS subscriber(s)"
render "$E2E_DIR/k8s/durability-job.template.yaml" | kubectl apply -f - >/dev/null

# kubectl wait can only watch for one condition at a time, and waiting for
# "complete" on a run that fails would just burn the timeout.
verdict=""
for ((i = 0; i < JOB_TIMEOUT; i++)); do
    if [[ "$(kubectl get job dist-mq-durability -n "$NAMESPACE" -o jsonpath='{.status.succeeded}' 2>/dev/null)" == "1" ]]; then
        verdict="passed"
        break
    fi
    if [[ -n "$(kubectl get job dist-mq-durability -n "$NAMESPACE" -o jsonpath='{.status.failed}' 2>/dev/null)" ]]; then
        verdict="failed"
        break
    fi
    sleep 1
done

echo
kubectl logs -n "$NAMESPACE" -l app=dist-mq-durability --tail=-1 2>/dev/null || true
echo

case "$verdict" in
    passed) echo "durability PASSED" ;;
    failed) echo "durability FAILED" >&2; exit 1 ;;
    *) die "job did not settle within ${JOB_TIMEOUT}s — kubectl describe job dist-mq-durability -n $NAMESPACE" ;;
esac
