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