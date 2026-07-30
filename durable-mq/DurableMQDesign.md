# simonMQ - Durable MQ

`durableMQ` is designed to work the same way as `pushMq`, with the exception that the application can be stopped and restarted at any time while still preserving state.
This requires actions being written to disk. A Write-Ahead-Log (WAL) is a common way to record actions that need to be done by the queue so that they can be replayed in the event of a crash or restart.
Since the queue simply receives messages and forwards them, a WAL custom-built is fairly simple in scope. Actions such as queueing a message or adding a subscriber are simply logged in the WAL and replayed on restart.
There is some intermediate state, such as a message which has been acknowledged by one subscriber but not another.

More detailed information on the Write Ahead Log (WAL) can be found in `./WalDesign.md`.

## General Structure

`durable-MQ` is split into various packages which act as hierarchical layers that each manage a different aspect of the system.

- Record
  - The lowest layer, handles serializing log records into bytes
- Wal
  - Handles reading and writing to the log files
  - Handles creation and management (lifecycle) of log files
- Coordinator
  - Handles recovery of data from WAL
    - `delivery` package exists to replay and process Enqueue and Ack events
  - Stores canonical Queue and Subscriber state
    - `catalog` stores this info and generates it upon WAL replay
  - Orchestration layer (API) for the WAL
- Queue
  - Handles live queues and their associated workers
  - Bundles live running queue logic and permenent state into one stable API for the server
- Server (not a subpackage)
  - Runs the HTTP server, exposes routes to network

### Upcoming Features (todo)
- Double check graceful handling of corrupted records in WAL
- More granular locks in `catalog` for greater performance
- New msgID system (UUID is not performant)
- Preserve retry attempt #s for unACKed subscribers on crash + restart
  - Currently only preserves which subscribers have ACKed
- Checkpointing system in `segment-mgr` (WAL does not currently checkpoint / compact)

### Structure
- Record
  - Handles serializing log records into bytes
- Wal
  - Handles reading and writing to the log files
  - Handles management of log files + compaction
- Delivery
  - Handles rebuilding application state from WAL
    - Mainly Messages in flight + ACK status
- Catalog
  - Handles queue registry and subscriber policy state
- Coordinator
  - Manages shared state between Delivery / Catalog
- Queue
  - Actual in memory queue impl