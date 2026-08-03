# Write Ahead Log (WAL) Design

## Architecture

### Log File Format
LSN (uint64) - length (uint32) - opType (uint8) - queueNameLength (uint32) - queueName (string) - payload (string) - CRC checksum (uint32)

Optypes:
- ENQUEUE (1)
- ACK (2)
- CREATE_QUEUE (3)
- DELETE_QUEUE (4)
- UPDATE_SUB_POLICY (5)
- DELETE_SUB_POLICY (6)
- BEGIN_CHECKPOINT (7)
- END_CHECKPOINT (8)

## Checkpoint Design

### Flow of Control
- `BEGIN_CHECKPOINT` is logged to the WAL
- Everything prior to this log is aggregated in memory
  - Enqueues are deleted if fully acked (subscriber state on enqueue is stored with the enqueue log)
  - All acks for messages that are fully acked are also removed
  - Current queue / subscriber state as of the start of the checkpoint process is compressed into creation logs
- Checkpoint log is written out to disk
- `END_CHECKPOINT` is logged to the WAL pointing to the LSN of the earlier `BEGIN_CHECKPOINT`
  - Contains the name of the checkpoint file as well as the checksum
- Old Checkpoint files and WAL files prior to the corresponding `BEGIN_CHECKPOINT` log are removed (once checksums are verified)

### Replay WAL Algorithm
- Most recent checkpoint file is read after verifying its presence in the log (END_CHECKPOINT) and checksums validated
- If not valid, the checkpoint file is removed and the log replayed as normal
- If valid, the log is discarded up until the final `BEGIN_CHECKPOINT` associated with the final valid `END_CHECKPOINT`
  - Everything past this point is appended to the checkpoint log
- Files found with a final LSN previous to the end of the checkpoint log are deleted


### Todo Checklist for Implementing Checkpointing
- [x] Change Enqueues to embed SubPolicy at time of enqueue (alter app logic to use this as well)
  - Retry-attempt tracking (how many attempts each SubPolicy has remaining) is deferred to a separate task — it needs a new record type and isn't required for checkpointing itself, which only needs acked/not-acked status.
- [x] Change reading from the WAL to ignore anything prior to the last valid `Begin_Checkpoint` log
  - Valid means there is an associated `End_Checkpoint` and the checksum + checkpoint file is intact
- [x] Implement rest of "Replay WAL Algorithm" defined above
- [ ] Start implementing actual checkpointing logic
  - Define and implement trigger to begin checkpointing
  - Start checkpointing method that receives full log into memory
  - Dedups / Compacts as needed and as defined above into MEMORY
  - Once final compaction is set, streams to disk (define file naming convention)
    - `checkpoint-<beginCheckpointLSN>.ckpt`
  - Obtains checksum, writes END_CHECKPOINT to WAL