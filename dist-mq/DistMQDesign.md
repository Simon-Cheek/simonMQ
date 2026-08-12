# simonMQ - Dist MQ

`distMQ` is a message queue that behaves the same way as `pushMQ` and `durableMQ` except that it is distributed using Raft. State is replicated across every node by consensus, so the queue survives losing a machine outright rather than merely surviving a restart.

The durability of `distMq` is achieved through a persistent log and state snapshot (through BoltDB), with a replicated In Memory FSM that can reference the durable log as needed.

## General Structure

`distMQ` is split into packages that act as hierarchical layers:

- **Model**
  - Shared, serializable data types used by every layer above
- **Command**
  - The wire format for log entries. Six command types (`CreateQueue`, `DeleteQueue`, `PutSubPolicy`, `DeleteSubPolicy`, `Enqueue`, `Ack`) and the encode/decode for them.
- **Storage**
  - The replicated state itself: which queues exist, who subscribes to them, and which messages still have subscribers owed a delivery. Defined as an interface with an in-memory implementation, so a durable backend could be swapped in without touching anything above it.
- **FSM**
  - Implements Raft's state machine interface. Decodes a committed log entry, dispatches it to the matching Storage mutation, and returns the result. Also handles snapshot and restore.
- **Node**
  - Owns the Raft handle and is the only package that touches the Raft API. Wires up the transport, the log store, the snapshot store, and the state machine, and exposes a typed method per command type.
- **Delivery**
  - The leader-only logic: per-queue in-memory buffers, the workers that deliver over HTTP, retry handling, and the lifecycle that builds and destroys all of it as leadership moves. Holds no state that cannot be rebuilt from Storage.
- **Server**
  - The HTTP layer. Routes match `pushMQ` and `durableMQ` exactly, plus leadership middleware that redirects writes to the current leader.

## Storage Layout

Two distinct things are persisted, in the same directory, for entirely different reasons:

- **`raft.db`** — the replicated log plus Raft's own election metadata (current term, and who this node voted for). This is the durability. It is written and fsynced before an entry counts as replicated.
- **`snapshots/`** — periodic captures of the state machine, written so the log can be truncated. A snapshot contains current state only, never history: a message that has been fully acknowledged appears in no snapshot and, once compaction catches up, in no log either.

The state machine itself is held in memory. This is for performance reasons and is also justified by the fact that the FSM can be rebuilt from log / snapshot data, which is on disk.

Because a message is deleted from the state machine the moment its last subscriber settles, the state machine is sized by **outstanding work** rather than by throughput. A queue moving high volume with subscribers keeping up holds almost nothing.
(A struggling subscriber may potentially hold up delivery traffic for that queue though; messages are currently delivered by per-queue workers).

## Message Lifecycle

1. A publisher POSTs to the leader. The handler reads the queue's current subscriber list and generates a message ID.
2. The handler proposes an `Enqueue` command carrying the ID, the payload, and the subscriber snapshot. It blocks until a quorum has persisted the entry and the leader has applied it.
3. Every node applies the entry to its own state machine. The leader additionally receives the resolved message back and hands it to the delivery layer.
4. The queue's worker pops the message and fans out one goroutine per pending subscriber.
5. The worker collects every result, then proposes a single `Ack` command naming every subscriber that settled in that pass — whether by acknowledging or by exhausting its retries.
6. Once the ack commits, subscribers still outstanding cause the message to be requeued to the tail. If none remain, the message is deleted from replicated state entirely.

## Leadership and Delivery

Only the leader delivers. Followers hold identical state and can answer reads, but run no workers and hold no message buffers.

The delivery layer is constructed on promotion and destroyed on demotion:

- **On promotion**, the node first waits until its state machine has applied everything its predecessor committed. It then rebuilds its queues by scanning replicated state for anything still owed a delivery.
- **On demotion**, everything is torn down immediately. In-flight HTTP deliveries are not waited on; their results are discarded rather than recorded. A demoted leader must never report an ack, because "gave up after N retries" is a decision that only ever existed in that node's memory, and applying it in a later term would suppress delivery attempts the new leader has not made.

Work enters the delivery layer through exactly one function, fed from two directions: directly after a commit (the fast path), and by a periodic sweep of replicated state (the safety net, catching anything that committed but never got scheduled). Both are deduplicated against the same set, so the two can race freely without producing a double delivery.

Everything the delivery layer holds is disposable. That is what makes failover work: the new leader does not recover the old leader's delivery state, it recomputes it from replicated state.

## Delivery Model & Policies

Identical to `pushMQ` and `durableMQ`. Each queue can have any number of subscribers, each described by a **SubPolicy**:

- **SubName** — the subscriber's unique identifier within the queue.
- **SubURL** — the base URL delivery attempts are POSTed to.
- **NumberOfRetries** — how many attempts are made before the subscriber is given up on.

Delivery is **at-least-once** per subscriber. The set of subscribers registered when a message is enqueued is captured at that moment and replicated with the message, so subscribers added afterward never receive it and subscribers removed afterward do not block it. Each subscriber is delivered to independently — a slow or failing subscriber never blocks the others on the same message.

A subscriber that exhausts its retries is treated as settled even though delivery never succeeded. DLQ is reserved for future work.

## Guarantees

- **A write is durable once acknowledged.** A 202 means a majority of nodes have persisted the entry. Losing any minority of the cluster — including permanently — loses nothing.
- **State survives total cluster restart**, provided the Raft directory does. Every node rebuilds from its snapshot plus its log.
- **Writes require a quorum.** With a majority unreachable, the cluster rejects writes rather than accepting ones it cannot guarantee. Reads continue to be served.
- **Delivery is at-least-once.** Failover, a partitioned leader, and a client retry after an ambiguous failure can each produce a duplicate. Subscribers are expected to tolerate that.
- **Reads are not linearizable.** `GET /queues` is served from local state on whichever node receives it, and a follower may be behind. Queue / Subscriber state is not considered essential enough to justify linearizability.
- **Ordering is best-effort.** Messages are delivered in enqueue order per queue, but a retried message is requeued to the tail and therefore falls behind messages enqueued after it. In general, ordering is not a priority.

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

`-peers` is parsed once into both the Raft membership and the mapping used for redirects, so the two cannot drift apart.

The redirect mapping is keyed by **server id**, not by Raft address. Raft reports the leader as both, but only the id is stable: the address is whatever that node advertised, and under an orchestrator it changes every time the process is rescheduled. Keying on the address would mean a node that moved could win an election and then be un-redirectable, because the address followers report for it no longer matches any entry in the map. The id never moves.

## Future Work

- **Dynamic cluster membership.** Membership is static and set at bootstrap. Growing or shrinking a running cluster requires adding and removing voters at runtime, which is not yet wired up.
- **No dead-letter queue**, yet.
- **Retry counts are leader-local** and reset on failover, so a message can receive more total attempts than its policy configures. Replicating them would cost a consensus round trip per attempt to record something only the current leader uses.
- **Commands are JSON-encoded**, which is readable in a log dump but larger on disk and on the wire than a binary framing. `durable-mq` has a byte-encoded version of records, which could be applied here.
- **No idempotency keys.** A publisher that retries after an ambiguous failure produces a second message with a new ID. Accepting a client-supplied ID would make retries safe, since a repeated ID is already handled as a no-op.
- **Batching is per-message.** Acks from one delivery pass share a command, but commands from separate messages are not coalesced. Batching across messages would cut consensus round trips further at the cost of latency and complexity.
- **Kubernetes deployment** along with performance and durability tests.
