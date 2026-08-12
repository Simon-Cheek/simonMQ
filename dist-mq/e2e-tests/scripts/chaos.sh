#!/usr/bin/env bash
#
# Kills dist-mq pods at random, forever. Run it in one terminal and e2e.sh in
# another: the durability suite asserts the same thing either way, which is why
# chaos lives out here instead of inside it.
#
#   ./e2e-tests/scripts/chaos.sh
#   MIN_UP=2 MAX_UP=8 ./e2e-tests/scripts/chaos.sh
#
# Pods are deleted with grace period 0, so the process is killed rather than
# asked to stop — recovering from whatever was on disk at that instant is the
# thing worth testing.
#
# One pod at a time, and the next kill waits for the last one to be Ready
# again. That keeps a quorum alive on any cluster of 3 or more, so writes stay
# available and the durability assertion has something to assert. On a
# single-node cluster the same loop is a crash-recovery test instead: writes
# are refused while it is down, which the suite records as rejected and makes
# no claim about.
#
# Tunables: NAMESPACE MIN_UP MAX_UP READY_TIMEOUT
set -uo pipefail

NAMESPACE="${NAMESPACE:-dist-mq}"
MIN_UP="${MIN_UP:-5}"
MAX_UP="${MAX_UP:-20}"
READY_TIMEOUT="${READY_TIMEOUT:-120s}"

die() { echo "error: $*" >&2; exit 1; }

command -v kubectl >/dev/null || die "kubectl not found in PATH"
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || die "namespace $NAMESPACE does not exist"

trap 'echo; echo "chaos stopped"; exit 0' INT TERM

echo "chaos: killing one dist-mq pod every ${MIN_UP}-${MAX_UP}s in namespace $NAMESPACE"
echo "Press Ctrl+C to stop."

iteration=0
while true; do
    iteration=$((iteration + 1))

    # Read loop rather than mapfile, which bash 3.2 does not have and which is
    # what macOS ships.
    pods=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && pods+=("$line")
    done < <(kubectl get pods -n "$NAMESPACE" -l app=dist-mq -o name 2>/dev/null)

    if ((${#pods[@]} == 0)); then
        echo "[$iteration] no dist-mq pods found, retrying in 5s"
        sleep 5
        continue
    fi

    victim="${pods[RANDOM % ${#pods[@]}]}"
    echo "[$iteration] killing $victim ($(date +%H:%M:%S))"
    kubectl delete "$victim" -n "$NAMESPACE" --grace-period=0 --force >/dev/null 2>&1

    # Waiting for the replacement before killing again is what bounds this to
    # one node down at a time.
    if ! kubectl wait --for=condition=Ready pod -n "$NAMESPACE" -l app=dist-mq \
        --timeout="$READY_TIMEOUT" >/dev/null 2>&1; then
        echo "[$iteration] cluster did not return to Ready within $READY_TIMEOUT — pausing 15s"
        sleep 15
        continue
    fi

    up=$((RANDOM % (MAX_UP - MIN_UP + 1) + MIN_UP))
    echo "[$iteration] cluster healthy again, next kill in ${up}s"
    sleep "$up"
done
