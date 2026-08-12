#!/usr/bin/env bash
#
# Fills dist-mq.template.yaml and writes the result to stdout. Cluster size is
# the only thing that varies, and it has to reach both the replica count and
# the -peers list, so it is computed here rather than kept in sync across
# checked-in overlays.
#
#   ./k8s/render.sh 3 | kubectl apply -f -
#   ./k8s/render.sh 1 > /tmp/single.yaml
#
# REPLICAS=1 is the single-node case. It is the same manifest with one pod, not
# a separate deployment path, so the e2e suite exercises identical wiring
# either way.
#
# Substitution is bash parameter expansion rather than sed or envsubst: the
# peers string is full of slashes and colons, and gettext is not a dependency
# worth taking on for this.
#
# Tunables: NAMESPACE IMAGE PULL_POLICY STORAGE_CLASS STORAGE_SIZE
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE="$SCRIPT_DIR/dist-mq.template.yaml"

REPLICAS="${1:-${REPLICAS:-3}}"
NAMESPACE="${NAMESPACE:-dist-mq}"
IMAGE="${IMAGE:-dist-mq:local}"
PULL_POLICY="${PULL_POLICY:-IfNotPresent}"
# Named rather than left to the cluster default, so the template can hold a
# plain value instead of a whole optional line. Omitting the field means "use
# the default class" while setting it to "" means "no class, do not provision",
# and a placeholder that has to render as either a key-value pair or nothing at
# all is what stops the template being readable as YAML on its own.
STORAGE_CLASS="${STORAGE_CLASS:-local-path}"
STORAGE_SIZE="${STORAGE_SIZE:-1Gi}"

# Fixed, and literal in the template too. They live here only because -peers
# has to embed them; assert_matches_template below keeps the two copies honest.
HTTP_PORT=8080
RAFT_PORT=9000
RAFT_SVC=dist-mq-raft

die() { echo "error: $*" >&2; exit 1; }

[[ -f "$TEMPLATE" ]] || die "template not found at $TEMPLATE"
[[ "$REPLICAS" =~ ^[0-9]+$ ]] && ((REPLICAS >= 1)) ||
    die "replicas must be a positive integer, got '$REPLICAS'"
if ((REPLICAS % 2 == 0)); then
    echo "warning: $REPLICAS is an even cluster size — it tolerates the same" \
        "number of failures as $((REPLICAS - 1)) while needing one more node to" \
        "form a quorum" >&2
fi

# Every node is given the same -peers string, and each entry is that pod's
# stable DNS name rather than its address, because the address changes on every
# reschedule and the raft configuration is written once at bootstrap.
peers=""
for ((i = 0; i < REPLICAS; i++)); do
    host="dist-mq-$i.$RAFT_SVC.$NAMESPACE.svc.cluster.local"
    peers="${peers:+$peers,}dist-mq-$i=$host:$RAFT_PORT=http://$host:$HTTP_PORT"
done

out="$(cat "$TEMPLATE")"
out="${out//__NAMESPACE__/$NAMESPACE}"
out="${out//__RAFT_SVC__/$RAFT_SVC}"
out="${out//__REPLICAS__/$REPLICAS}"
out="${out//__IMAGE__/$IMAGE}"
out="${out//__PULL_POLICY__/$PULL_POLICY}"
out="${out//__HTTP_PORT__/$HTTP_PORT}"
out="${out//__RAFT_PORT__/$RAFT_PORT}"
out="${out//__PEERS__/$peers}"
out="${out//__STORAGE_CLASS__/$STORAGE_CLASS}"
out="${out//__STORAGE_SIZE__/$STORAGE_SIZE}"

# A placeholder surviving substitution renders a manifest that applies cleanly
# and behaves nothing like it reads.
if leftover="$(grep -o '__[A-Z_]*__' <<<"$out" | sort -u)" && [[ -n "$leftover" ]]; then
    die "unsubstituted placeholder(s) in $TEMPLATE: $(tr '\n' ' ' <<<"$leftover")"
fi

# The ports and the headless service name are literal in the template but are
# also baked into -peers, so the two files each hold a copy. Drift between them
# gives a cluster that applies cleanly, comes up healthy-looking, and never
# forms a quorum because every peer is being dialled at the wrong port.
assert_matches_template() {
    grep -qF -- "$1" <<<"$out" || die "$TEMPLATE has drifted from render.sh: expected to find '$1'"
}
assert_matches_template "-http-addr=0.0.0.0:$HTTP_PORT"
assert_matches_template "-raft-addr=0.0.0.0:$RAFT_PORT"
assert_matches_template "name: $RAFT_SVC"
assert_matches_template "serviceName: $RAFT_SVC"

printf '%s\n' "$out"
