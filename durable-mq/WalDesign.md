# Write Ahead Log (WAL) Design

## Architecture

### Log File Format

Every action in the system is recorded as one record in the WAL:

`LSN (uint64) - length (uint32) - opType (uint8) - queueNameLength (uint32) - queueName (string) - payload (string) - CRC checksum (uint32)`

The LSN (Log Sequence Number) is a globally unique, monotonically increasing identifier assigned in write order. The CRC checksum lets replay detect a corrupted or torn record — e.g. from a crash mid-write — and stop cleanly rather than trust it.

Records are grouped into segment files capped at a configured size; once full, writing rolls to a new segment. Segments are read back in order, transparently spanning file boundaries, so the log behaves as one continuous stream regardless of how many files it's split across.

Optypes:
- ENQUEUE (1)
- ACK (2)
- CREATE_QUEUE (3)
- DELETE_QUEUE (4)
- UPDATE_SUB_POLICY (5)
- DELETE_SUB_POLICY (6)
- BEGIN_CHECKPOINT (7)
- END_CHECKPOINT (8)

### Enqueue Records Carry Their Own Subscriber Snapshot

An ENQUEUE record's payload embeds the resolved list of subscriber policies at the moment it was enqueued, not just the message content — replay never needs to consult separate, ambient policy state to know who a message was addressed to. This keeps a message's subscriber list a true point-in-time snapshot, and it's what makes checkpointing possible: a bounded window of the log can be replayed correctly without the full history of policy changes that preceded it.

## Checkpoint Design

As the WAL grows, replaying it in full on every restart gets increasingly expensive. Checkpointing periodically compacts the log's history into a snapshot, so recovery only needs to replay since the most recent checkpoint, not the system's entire lifetime.

### Trigger

A checkpoint is due once the WAL exceeds a configured number of segment files, checked after every write. Only one runs at a time: if one is already in progress when the threshold is crossed again, the check is a no-op until it finishes, cleanup included. Checkpointing runs in the background; normal reads and writes aren't blocked while it's derived.

### Flow of Control

1. A `BEGIN_CHECKPOINT` record is appended to the live WAL, marking this checkpoint's snapshot boundary LSN — one of only two points where a checkpoint briefly holds the same lock ordinary writes use.
2. Starting from the last valid checkpoint's boundary, the log is replayed forward through this new `BEGIN_CHECKPOINT`'s LSN, reconstructing queue, subscriber, and in-flight message state as of that point. Live writes continue during this step — the replay range is bounded and unaffected by anything appended after the boundary.
3. That state is compacted in memory:
   - Queue and subscriber-policy history collapses to just the final state — deleted queues and policies simply don't appear.
   - Fully acknowledged messages (every subscriber in their snapshot has settled) are dropped, along with their ack records.
   - Messages still awaiting acknowledgement are kept, along with whichever acks they've already received — so no subscriber that already acked before the checkpoint gets redelivered to afterward.
   - Anything appended after the `BEGIN_CHECKPOINT` boundary is left alone (only items prior to the append are compacted)
4. The compacted result is written to a new checkpoint file, uniquely identified and using the same on-disk record format as the WAL. The write goes through a temp-file-then-atomic-rename sequence, so the file only ever appears fully intact under its permanent name; a crash mid-write can never leave a readable but incomplete file.
5. A checksum of the complete checkpoint file is computed.
6. An `END_CHECKPOINT` record is appended to the live WAL, naming the checkpoint file and its checksum — the second and last point a checkpoint briefly holds the ordinary-write lock. From here, the checkpoint is valid and durable.
7. Superseded WAL segments and checkpoint files are cleaned up. This is best-effort — if it doesn't fully complete (e.g. a crash), nothing is lost; the same files are recognized as safe to remove on the next checkpoint or at startup.

### Replay Algorithm

- A `BEGIN_CHECKPOINT` record is noted but otherwise has no effect on its own.
- An `END_CHECKPOINT` record names a checkpoint file and its expected checksum. That file's actual checksum is recomputed and compared:
  - If they match, everything accumulated so far is discarded and replaced with the checkpoint file's own compacted contents.
  - If the file is missing or its checksum doesn't match, the `END_CHECKPOINT` is discarded and the associated file (if exists) is deleted.
- Scanning replays in forward order cover the potential edge case where multiple valid checkpoint files exist at once.
