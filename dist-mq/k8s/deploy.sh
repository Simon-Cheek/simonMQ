#!/usr/bin/env bash
#
# Builds the image, imports it into k3s, renders the manifest for the requested
# cluster size and applies it. Run this on the VM.
#
#   ./k8s/deploy.sh 3           # three-node cluster
#   ./k8s/deploy.sh 1           # single node, same manifest
#   WIPE=1 ./k8s/deploy.sh 5    # tear down state first
#
# WIPE deletes the namespace, and with it the PersistentVolumeClaims. That is
# required when changing cluster size: a pod that already has raft state skips
# bootstrap entirely, so an old volume would carry the previous cluster's
# membership into the new one and the node would sit waiting on a quorum of
# peers that no longer exist. The script refuses a size change without it.
#
# Tunables: NAMESPACE IMAGE SKIP_BUILD WIPE CTR plus everything render.sh takes
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" # k8s
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"                   # dist-mq — the docker build context

REPLICAS="${1:-${REPLICAS:-3}}"
NAMESPACE="${NAMESPACE:-dist-mq}"
IMAGE="${IMAGE:-dist-mq:local}"
SKIP_BUILD="${SKIP_BUILD:-0}"
WIPE="${WIPE:-0}"
CTR="${CTR:-sudo k3s ctr}"

die() { echo "error: $*" >&2; exit 1; }

command -v kubectl >/dev/null || die "kubectl not found in PATH"

if [[ "$SKIP_BUILD" != "1" ]]; then
    command -v docker >/dev/null || die "docker not found; build elsewhere and rerun with SKIP_BUILD=1"
    echo "building $IMAGE"
    docker build -t "$IMAGE" "$ROOT_DIR" || die "docker build failed"

    # k3s runs containerd, which does not read the docker daemon's image store.
    echo "importing $IMAGE into k3s"
    docker save "$IMAGE" | $CTR images import - >/dev/null || die "importing image into k3s failed"
fi

# A resize against live volumes is the one way to get a cluster that comes up
# healthy-looking and never elects, so catch it here rather than in a test.
current="$(kubectl get statefulset dist-mq -n "$NAMESPACE" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
if [[ -n "$current" && "$current" != "$REPLICAS" && "$WIPE" != "1" ]]; then
    die "cluster is deployed at $current node(s), requested $REPLICAS.
  Changing size needs the old raft state gone: WIPE=1 $0 $REPLICAS"
fi

if [[ "$WIPE" == "1" ]]; then
    echo "wiping namespace $NAMESPACE"
    kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=true
fi

echo "deploying $REPLICAS node(s) to namespace $NAMESPACE"
REPLICAS="$REPLICAS" NAMESPACE="$NAMESPACE" IMAGE="$IMAGE" "$SCRIPT_DIR/render.sh" "$REPLICAS" |
    kubectl apply -f - || die "kubectl apply failed"

echo "waiting for rollout"
kubectl rollout status statefulset/dist-mq -n "$NAMESPACE" --timeout=180s ||
    die "rollout did not complete; try: kubectl describe pod -n $NAMESPACE -l app=dist-mq"

# Readiness is a TCP check, so it says the listener is up and nothing about
# consensus. An election is the first thing that proves the peers actually
# found each other, which makes it the only smoke test worth running here.
echo -n "waiting for a leader "
leader=""
for _ in $(seq 1 60); do
    leader="$(kubectl logs -n "$NAMESPACE" -l app=dist-mq --tail=-1 2>/dev/null |
        grep -o 'entering leader state' | head -1 || true)"
    [[ -n "$leader" ]] && break
    echo -n "."
    sleep 1
done
echo

if [[ -z "$leader" ]]; then
    echo "warning: no node reported entering leader state within 60s" >&2
    echo "  kubectl logs -n $NAMESPACE -l app=dist-mq --tail=50" >&2
    exit 1
fi

kubectl get pods -n "$NAMESPACE" -l app=dist-mq
echo
echo "cluster up. from inside the cluster: http://dist-mq.$NAMESPACE.svc.cluster.local:8080"
echo "from this machine:  kubectl port-forward -n $NAMESPACE svc/dist-mq 8080:8080"
