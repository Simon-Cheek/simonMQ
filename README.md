# simonMQ

This repository serves as design practice for Message Queues of varying designs. Each one will be documented here.

## Simple-MQ
Simple-MQ serves as simple practice for constructing a message queue and contains the following features:
- HTTP Server that accepts `POST /Queues/{queueName}/Messages` to enqueue
  - If `queueName` has not been used previously, it will be created
- Consumers receive Messages by simple polling at `GET /Queues/{queueName}/Messages/next`
  - 204 Response if no Messages are currently in the queue
- Message delivery policy is "At Most Once", meaning that Messages are deleted as soon as they are consumed once
- Any user can consume Messages from any queue
- Queue is entirely in memory, meaning that in-flight Messages are lost if the queue crashes

## Simple-MQ-2
Same as Simple-MQ with concurrency optimizations such as queue specific locks and better internal data structures
- Used to demonstrate throughput improvements with more efficient concurrency mgmt
- Uses Ring Buffer Queue Implementation

## Push-MQ
Pub/Sub model concurrently sending Messages to each queue's configured subscribers
- Consumers attach server locations to call a `POST /queue/message` method on for Messages
  - Push-MQ automatically calls all consumers and retries if not given a 200 response ("At Least Once" policy)
- Call `POST /Queues/{queueName}/subscribers/{SubName}` to register
- Call `PUT /Queues/{queueName}/subscribers/{SubName}` to configure individual subscriber policies
- Call `DELETE /Queues/{queueName}/subscribers/{SubName}`
- Policies
  - Fully in memory (no persistence yet)
  - No Dead Letter System (yet)
  - Configurable number of retries per Subscriber per Queue
  - NOT FIFO (Messages can deliver out of order even within a queue)
  - At-least-once policy (guaranteed message transfer preferred over preventing duplicates)
  - Subscriber policy changes dont affect Messages actively in flight
    - Once they are taken off of the queue, the policy is frozen

## Durable-MQ
Pub/Sub model identical in functionality to Push-MQ with one core distinction: durable storage.
- Write Ahead Log (WAL) which is used to rebuild queue and subscriber state after a crash
- Much more detailed documentation can be found within `/durable-mq/DurableMQDesign.md`
- WAL implementation info can be found in `/durable-mq/WalDesign.md`
- Performance tests comparing `push-mq` and `durable-mq` can be found in `/durable-mq/Performance.md`

## Dist-MQ
Pub/Sub model identical in functionality to Push-MQ, running as a cluster rather than a single process.
Multiple instances of `Dist-MQ` form a quorum, replicating all state by consensus using Raft (`hashicorp/raft`).
Where `durable-mq` survives a restart, `dist-mq` survives losing a machine outright.
- Detailed documentation can be found within `/dist-mq/DistMQDesign.md`
- Every state change is replicated to a majority of nodes before it takes effect
  - A write is only acknowledged once it is durable on a quorum of disks
  - Any minority of the cluster can be lost permanently without losing a message or taking the queue offline
- Replicated log and Raft metadata persist to bolt-db; state snapshots persist as files
  - Queue and subscriber state is held in memory and rebuilt from the log on startup
- All writes require the leader node
  - Followers redirect writes with a `421` carrying the leader's address, or a `503` while an election is in progress
  - Queue state is readable from any node, though a follower may be slightly behind
- Message delivery is leader-only, and is rebuilt from replicated state whenever leadership moves
- Same at-least-once policy as Push-MQ, with duplicates additionally possible across a leadership change