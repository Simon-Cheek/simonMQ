# simonMQ - Dist MQ

`distMQ` is a message queue that behaves the same way as `pushMQ` and `durableMQ` from the outside, with one core difference: it runs as a cluster rather than a single process. State is replicated across every node by consensus, so the queue survives losing a machine outright rather than merely surviving a restart.

Where `durableMQ` achieves durability with a Write-Ahead Log written to one disk, `distMQ` achieves it with a replicated log written to a quorum of disks. The tradeoff is deliberate: a write now costs a network round trip and several fsyncs instead of one, and in exchange a node can be destroyed permanently without losing a message or taking the queue offline.

Consensus is provided by `hashicorp/raft`. This project does not implement the Raft algorithm — it implements the state machine that sits behind it, which is where all of the queue-specific design lives.

## The Replicated State Machine Model

Everything in `distMQ` follows from one rule: **the log is the truth, and all other state is derived from it.**

Every mutation is serialized into a command, appended to the replicated log, and only takes effect once a majority of nodes have persisted it. Each node then applies committed commands, in identical order, to its own local copy of the state. Because every node applies the same commands in the same sequence, every node arrives at the same state.

Two constraints follow, and they shape most of the codebase:

- **Applying a command must be deterministic.** Anything that could produce a different answer on different nodes — UUID generation, clock reads, map iteration order affecting state — has to be resolved by the leader *before* the command is proposed, and carried in the command itself. Message IDs and a message's subscriber list are both handled this way.
- **Applying a command must have no side effects.** Apply runs on every node, including followers, and again on every node during log replay after a restart. Delivering a message from inside Apply would mean every follower POSTing to every subscriber, repeatedly.

That second constraint is why delivery is a leader-only concern layered on top of the state machine rather than part of it.

## General Structure

`distMQ` is split into packages that act as hierarchical layers:

- **Model**
  - Shared, serializable data types used by every layer above — subscriber policies, queue state, and message state. One shared definition means the command encoder, the state machine, the delivery layer, and the HTTP layer all agree on what a "subscriber policy" or "pending message" is.
- **Command**
  - The wire format for log entries. Six command types (`CreateQueue`, `DeleteQueue`, `PutSubPolicy`, `DeleteSubPolicy`, `Enqueue`, `Ack`) and the encode/decode for them. Validation happens on both encode and decode, so a malformed command fails on the leader rather than becoming a committed entry every node has to cope with forever.
- **Storage**
  - The replicated state itself: which queues exist, who subscribes to them, and which messages still have subscribers owed a delivery. Defined as an interface with an in-memory implementation, so a durable backend could be swapped in without touching anything above it.
- **FSM**
  - Implements Raft's state machine interface. Decodes a committed log entry, dispatches it to the matching Storage mutation, and returns the result. Also handles snapshot and restore. Deliberately thin — it is glue, not logic.
- **Node**
  - Owns the Raft handle and is the only package that touches the Raft API. Wires up the transport, the log store, the snapshot store, and the state machine, and exposes a typed method per command type. Every write in the system goes through here.
- **Delivery**
  - The leader-only machinery: per-queue in-memory buffers, the workers that deliver over HTTP, retry handling, and the lifecycle that builds and destroys all of it as leadership moves. Holds no state that cannot be rebuilt from Storage.
- **Server**
  - The HTTP layer. Routes match `pushMQ` and `durableMQ` exactly, plus leadership middleware that redirects writes to the current leader.

## Storage Layout

Two distinct things are persisted, in the same directory, for entirely different reasons:

- **`raft.db`** — the replicated log plus Raft's own election metadata (current term, and who this node voted for). This is the durability. It is written and fsynced before an entry counts as replicated.
- **`snapshots/`** — periodic captures of the state machine, written so the log can be truncated. A snapshot contains current state only, never history: a message that has been fully acknowledged appears in no snapshot and, once compaction catches up, in no log either.

The state machine itself is held in memory. This is not a durability compromise — a committed entry is already quorum-durable in the log, and state is reconstructed on startup from the newest snapshot plus the entries after it. Keeping it in memory avoids a second fsync per write on every node for data that is, by construction, reproducible.

Because a message is deleted from the state machine the moment its last subscriber settles, the state machine is sized by **outstanding work** rather than by throughput. A queue moving high volume with subscribers keeping up holds almost nothing.

## Message Lifecycle

1. A publisher POSTs to the leader. The handler reads the queue's current subscriber list and generates a message ID.
2. The handler proposes an `Enqueue` command carrying the ID, the payload, and the subscriber snapshot. It blocks until a quorum has persisted the entry and the leader has applied it.
3. Every node applies the entry to its own state machine. The leader additionally receives the resolved message back and hands it to the delivery layer.
4. The queue's worker pops the message and fans out one goroutine per pending subscriber.
5. The worker collects every result, then proposes a single `Ack` command naming every subscriber that settled in that pass — whether by acknowledging or by exhausting its retries.
6. Once the ack commits, subscribers still outstanding cause the message to be requeued to the tail. If none remain, the message is deleted from replicated state entirely.

Step 5 batching one pass into one command matters: a message with five subscribers costs two consensus round trips, not six.

## Leadership and Delivery

Only the leader delivers. Followers hold identical state and can answer reads, but run no workers and hold no message buffers.

The delivery layer is constructed on promotion and destroyed on demotion:

- **On promotion**, the node first waits until its state machine has applied everything its predecessor committed. Being elected does not mean being caught up — the two are separate events, and reading state between them would silently skip part of the backlog. It then rebuilds its queues by scanning replicated state for anything still owed a delivery.
- **On demotion**, everything is torn down immediately. In-flight HTTP deliveries are not waited on; their results are discarded rather than recorded. A demoted leader must never report an ack, because "gave up after N retries" is a decision that only ever existed in that node's memory, and applying it in a later term would suppress delivery attempts the new leader has not made.

Work enters the delivery layer through exactly one function, fed from two directions: directly after a commit (the fast path), and by a periodic sweep of replicated state (the safety net, catching anything that committed but never got scheduled). Both are deduplicated against the same set, so the two can race freely without producing a double delivery.

Everything the delivery layer holds — buffers, retry counts, in-flight bookkeeping — is disposable. That is what makes failover work: the new leader does not recover the old leader's delivery state, it recomputes it from replicated state.

## Delivery Model & Policies

Identical to `pushMQ` and `durableMQ`. Each queue can have any number of subscribers, each described by a **SubPolicy**:

- **SubName** — the subscriber's unique identifier within the queue.
- **SubURL** — the base URL delivery attempts are POSTed to.
- **NumberOfRetries** — how many attempts are made before the subscriber is given up on.

Delivery is **at-least-once** per subscriber. The set of subscribers registered when a message is enqueued is captured at that moment and replicated with the message, so subscribers added afterward never receive it and subscribers removed afterward do not block it. Each subscriber is delivered to independently — a slow or failing subscriber never blocks the others on the same message.

A subscriber that exhausts its retries is treated as settled even though delivery never succeeded. There is no dead-letter queue; this is a known gap rather than a design decision.

## Guarantees

- **A write is durable once acknowledged.** A 202 means a majority of nodes have persisted the entry. Losing any minority of the cluster — including permanently — loses nothing.
- **State survives total cluster restart**, provided the Raft directory does. Every node rebuilds from its snapshot plus its log.
- **Writes require a quorum.** With a majority unreachable, the cluster rejects writes rather than accepting ones it cannot guarantee. Reads continue to be served.
- **Delivery is at-least-once.** Failover, a partitioned leader, and a client retry after an ambiguous failure can each produce a duplicate. Subscribers are expected to tolerate that.
- **Reads are not linearizable.** `GET /queues` is served from local state on whichever node receives it, and a follower may be behind. This is acceptable for an inspection endpoint and is not used for anything the system's correctness depends on.
- **Ordering is best-effort.** Messages are delivered in enqueue order per queue, but a retried message is requeued to the tail and therefore falls behind messages enqueued after it. This matches `pushMQ` and `durableMQ`.

## Client Contract

Writes must reach the leader. A node that is not the leader rejects them, and the distinction between the two rejection cases matters:

- **421** — this node is not the leader, and the current leader's address is supplied. The client should retry there and cache the address for subsequent requests, so the redirect costs one extra hop per leadership term rather than one per request.
- **503** — there is no leader at the moment, meaning an election is in progress. The client should back off and retry rather than shopping around, since no other node will accept the write either.

`GET /queues` is unrestricted and answerable by any node.

## Configuration

| flag | purpose | scope |
| --- | --- | --- |
| `-id` | Raft server identity, persisted in cluster configuration | unique per node, stable across restarts |
| `-raft-dir` | holds `raft.db` and `snapshots/` | unique per node, must be persistent |
| `-raft-addr` | node-to-node RPC bind address | unique per node |
| `-raft-advertise` | address peers use to reach this node | required when binding a wildcard address |
| `-http-addr` | client-facing HTTP listener | unique per node |
| `-peers` | cluster membership as `id=raftAddr=httpBaseURL` | identical on every node |
| `-bootstrap` | write initial cluster configuration on first boot | identical on every node |
| `-reconcile` | delivery sweep interval | free |

Each node listens on two ports for two different audiences. The Raft port carries only cluster traffic and should never be reachable by clients or placed behind a load balancer — peers address each other by identity, and round-robining between them breaks replication. The HTTP port is the opposite: a plain round-robin service is fine, because the leader redirect handles landing on a follower.

`-peers` is parsed once into both the Raft membership and the address mapping used for redirects, so the two cannot drift apart.

## Future Work

- **Dynamic cluster membership.** Membership is static and set at bootstrap. Growing or shrinking a running cluster requires adding and removing voters at runtime, which is not yet wired up.
- **No dead-letter queue**, carried over from `pushMQ` and `durableMQ`.
- **Retry counts are leader-local** and reset on failover, so a message can receive more total attempts than its policy configures. Replicating them would cost a consensus round trip per attempt to record something only the current leader uses.
- **Commands are JSON-encoded**, which is readable in a log dump but larger on disk and on the wire than a binary framing. The codec is isolated behind one encode/decode pair, so this is a contained change if throughput demands it.
- **No idempotency keys.** A publisher that retries after an ambiguous failure produces a second message with a new ID. Accepting a client-supplied ID would make retries safe, since a repeated ID is already handled as a no-op.
- **Batching is per-message.** Acks from one delivery pass share a command, but commands from separate messages are not coalesced. Batching across messages would cut consensus round trips further at the cost of latency and complexity.
- **Kubernetes deployment** requires a StatefulSet for stable identity and persistent volumes, plus a headless service for peer DNS. Anti-affinity matters more than node count — three replicas in one failure domain provide no more safety than one.
