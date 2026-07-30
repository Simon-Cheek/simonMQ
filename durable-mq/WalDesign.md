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

## Tradeoffs

## Flow of Control