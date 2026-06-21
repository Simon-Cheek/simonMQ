# simonMQ

This repository serves as design practice for Message Queues of varying designs. Each one will be documented here.

## Simple-MQ
Simple-MQ serves as simple practice for constructing a message queue and contains the following features:
- HTTP Server that accepts `POST /queues/{queueName}/messages` to enqueue
  - If `queueName` has not been used previously, it will be created
- Consumers receive messages by simple polling at `GET /queues/{queueName}/messages/next`
  - 204 Response if no messages are currently in the queue
- Message delivery policy is "At Most Once", meaning that messages are deleted as soon as they are consumed once
- Any user can consume messages from any queue
- Queue is entirely in memory, meaning that in-flight messages are lost if the queue crashes

## Simple-MQ-2
Same as Simple-MQ with concurrency optimizations such as queue specific locks and better internal data structures
- Used to demonstrate throughput improvements with more efficient concurrency mgmt
- Uses Ring Buffer Queue Implementation

## Push-MQ
Pub/Sub model concurrently sending messages to each queue's configured subscribers
- Consumers attach server locations to call a `POST /queue/message` method on for messages
  - Push-MQ automatically calls all consumers and retries if not given a 200 response ("At Least Once" policy)
- Call `POST /queues/{queueName}/subscribers/{subName}` to register
- Call `PUT /queues/{queueName}/subscribers/{subName}` to configure individual subscriber policies
- Call `DELETE /queues/{queueName}/subscribers/{subName}`
- Policies
  - Fully in memory (no persistence yet)
  - No Dead Letter System (yet)
  - Configurable retry system per subscriber (max attempts, retry frequency, etc)
  - NOT FIFO (messages can deliver out of order even within a queue)
  - At-least-once policy (guaranteed message transfer preferred over preventing duplicates)

## Performance-Tests
Used to understand the performance differences between the various implementations.

### Simple-MQ vs Simple-MQ-2
- Simple-MQ and Simple-MQ-2 had near identical throughput in various environments
  - CPU (Unit) Bound tests
    - Near identical throughput, unless at high contention levels (8+ cores contending for same queues)
    - Throughput was bottlenecked by UUID generation more than anything
  - External (HTTP) tests
    - Near identical throughput regardless of contention levels
    - Possibly HTTP/JSON bottlenecked? Could try gRPC implementation next