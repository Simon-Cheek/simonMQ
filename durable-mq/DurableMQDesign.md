# simonMQ - Durable MQ

`durableMQ` is a message queue designed to work the same way as `pushMq`, with one key difference: the application can be stopped, crashed, or restarted at any time without losing state. Every action that changes the state of the system (creating a queue, enqueuing a message, registering a subscriber, acknowledging delivery, etc) is written to disk before it takes effect, and the entire system can be reconstructed from that record alone.

This is achieved with a Write-Ahead Log (WAL). Because the queue's job is fundamentally simple (receive a message, forward it to subscribers, track subscriber acks) the WAL is relatively simple. The only real complexity involves in-flight messages: tracking which subscribers have already received a message and which have not.

As the WAL grows, replaying it from the very beginning on every restart becomes increasingly expensive. Checkpointing solves this by periodically compacting the log's history into a compact snapshot, so recovery only has to replay the portion of the log since the last checkpoint rather than the system's entire history. Full detail on both the WAL and the checkpointing system that runs on top of it is in `./WalDesign.md`.

## General Structure

`durable-MQ` is split into packages that act as hierarchical layers, each responsible for a different concern:

- **Record**
  - The lowest layer. Handles serializing individual log entries to and from bytes, including their checksums.
- **Model**
  - Shared, serializable data types used across every layer above Record — subscriber policies, enqueue payloads, ack payloads, and checkpoint metadata. Having one shared definition for these means the WAL, catalog, delivery, and coordinator layers all agree on exactly what a "subscriber policy" or "enqueued message" looks like on disk.
- **Wal**
  - Handles reading and writing the log files themselves, including segment file lifecycle (rolling to a new file once a segment grows large enough) and checkpoint file lifecycle (writing new checkpoint files and cleaning up ones that are no longer needed).
- **Coordinator**
  - Orchestrates all of the above into a single API surface for the rest of the system. Owns the WAL and drives recovery from it at startup.
  - Also owns the periodic checkpointing process — deciding when a checkpoint is due, deriving the compacted snapshot, and writing it out — while live traffic continues uninterrupted.
  - Stores the canonical, in-memory Queue and Subscriber state, reconstructed by replaying the WAL. This is handled by the `catalog` package.
  - A separate `delivery` package reconstructs per-message delivery state (which subscribers a message was addressed to, and who has already acknowledged it) — used only during the one-time startup replay, then discarded once live serving begins.
- **Queue**
  - Owns the live, running queues: the in-memory message buffers, the workers that deliver messages to subscribers over HTTP, and retry handling. Bundles this live serving logic together with the durable state from Coordinator into one stable API for the server.
- **Server** (not a subpackage)
  - Runs the HTTP server and exposes the system's routes to the network — creating and deleting queues, enqueuing messages, managing subscribers, and inspecting current queue/subscriber state.

## Delivery Model & Policies

Each queue can have any number of subscribers, each described by a **SubPolicy**:

- **SubName** — the subscriber's unique identifier within the queue.
- **SubURL** — the base URL delivery attempts are POSTed to.
- **NumberOfRetries** — how many delivery attempts are made to this subscriber before it's given up on.

Delivery is **at-least-once** per subscriber. When a message is enqueued, the set of subscribers currently registered on that queue is captured as a snapshot at that moment — subscribers added afterward do not receive messages that were already in flight, and subscribers removed afterward don't block a message that was already addressed to them. Each subscriber is delivered to independently: a slow or failing subscriber never blocks delivery to the others on the same message.

On a failed delivery attempt, the subscriber is retried up to its configured `NumberOfRetries`. Once that limit is reached, the subscriber is treated as settled for that message (acknowledged) even though delivery never actually succeeded — there is currently no dead-letter queue or other mechanism to surface permanently-failed deliveries; this is a known gap, not an intentional design choice. A message is considered fully complete only once every subscriber in its original snapshot has either acknowledged it or exhausted its retries.

## Durability & Recovery Guarantees

- No action is considered to have happened until it has been durably appended to the WAL — the in-memory system state is always a derivative of the log, never the other way around.
- A restart (whether graceful or a hard crash) replays the WAL to reconstruct exact pre-crash state: which queues and subscribers exist, which messages are still in flight, and which subscribers have already acknowledged which messages. No message is lost, and no subscriber that already acknowledged a message is redelivered to after a restart.
- Checkpointing periodically compacts this history so that recovery time stays bounded by the size of the log since the last checkpoint, not the system's entire lifetime — without changing any of the above guarantees.

## Future Work

- No dead-letter queue: durable (and configurable) logic for messages that exceed the retry limit.
- Retry-attempt counts are not preserved across a restart — after a crash, a subscriber's retry count for any in-flight message resets, even if it had already used some of its attempts beforehand.
- More granular locking in `catalog.go`
- Message IDs are UUID-based, which is simple but not performant (this bottlenecked `simple-mq`)
