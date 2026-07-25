# simonMQ - Durable MQ

### Log File Format
LSN (uint64) - length (uint32) - opType (uint8) - queueID (TBD) - payload - CRC checksum (uint32)

Optypes:
- ENQUEUE (1)
- ACK (2)
- CREATE_QUEUE (3)
- DELETE_QUEUE (4)
- UPDATE_SUB_POLICY (5)
- DELETE_SUB_POLICY (6)