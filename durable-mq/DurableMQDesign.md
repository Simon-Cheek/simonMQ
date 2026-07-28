# simonMQ - Durable MQ

### Log File Format
LSN (uint64) - length (uint32) - opType (uint8) - queueNameLength (uint32) - queueName (string) - payload (string) - CRC checksum (uint32)

Optypes:
- ENQUEUE (1)
- ACK (2)
- CREATE_QUEUE (3)
- DELETE_QUEUE (4)
- UPDATE_SUB_POLICY (5)
- DELETE_SUB_POLICY (6)

### Tradeoffs and Scope
Work Todo: Eventual Features
- WAL handles corrupted records in real time
- Dedup logic producer-side (make messages retried externally idempotent)
- Preserve retry attempt #s for unACKed subscribers on crash + restart
  - Currently only preserves which subscribers have ACKed

### Structure
- Record
  - Handles serializing log records into bytes
- Wal
  - Handles reading and writing to the log files
  - Handles management of log files + compaction
- Delivery
  - Handles rebuilding application state from WAL
    - Mainly messages in flight + ACK status
- Catalog
  - Handles queue registry and subscriber policy state
- Coordinator
  - Manages shared state between Delivery / Catalog
- Queue
  - Actual in memory queue impl