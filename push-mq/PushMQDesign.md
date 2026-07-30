# PushMQ — Design

## Architecture
One broker manages a collection of Queues (max 128). Each queue has its own independent buffer, concurrency, subscriber policies, and worker.
- Having separate Queues allows for further logic later related to queue management (compared to if all Queues shared the same data structure).

The limit on virtual Queues allows for one worker per queue compared to a threadpool of workers managing many Queues (would require fairness mechanisms).

## Message Lifecycle
1. Message is inserted into the ring buffer, then non-blocking-signals the queue's worker if idle.
2. Worker pops the message, fans out one goroutine per pending (unacked, retries remaining) subscriber to attempt delivery.
3. Worker waits for all goroutines to finish, then updates ack/retry state.
4. All subscribers acked or exhausted → discard (no DLQ). Else → requeue to the tail.

## Delivery
- Each worker fans out requests to all unacked subscribers with remaining retries, sends results back to shared channel.
  - Default HTTP Timeout to stop long-running requests from blocking Queues for long periods of time (15 seconds)
- If there are any remaining unacked subscribers with retries, the message is re-queued for later attempts.
  - This means that there is no configurable delay on retries, instead it is determined inherently by the traffic levels of that queue.
  - Additional logic could define a minimum wait period to retry a request (requeue the msg if the wait period has not expired yet)

## Worker Behavior
- Each worker continually pulls off Messages from the queue until there are none, then sleeps.
- When a new message arrives on the queue, the specific worker assigned to the queue is signaled via channel.

## Non-Features (for later iterations)
- Dead Letter Queue (DLQ)
- Permanent Storage (only in memory at the moment)