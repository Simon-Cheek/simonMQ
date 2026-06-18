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
