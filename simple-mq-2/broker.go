package main

import "sync"
import "github.com/google/uuid"

type Broker struct {
	queues map[string]*Queue
	mu     sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		queues: make(map[string]*Queue),
	}
}

func (b *Broker) enqueue(name string, payload string) *QueueMsg {

	msg := &QueueMsg{
		MsgId:   name + "/" + uuid.New().String(),
		Payload: payload,
	}

	queue := b.getOrCreateQueue(name)
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.messages = append(queue.messages, msg)

	return msg
}

func (b *Broker) dequeue(name string) *QueueMsg {

	queue := b.getOrCreateQueue(name)
	queue.mu.Lock()
	defer queue.mu.Unlock()

	if len(queue.messages) > 0 {
		msg := queue.messages[0]
		queue.messages = queue.messages[1:]
		return msg
	}
	return nil
}

func (b *Broker) getOrCreateQueue(name string) *Queue {
	b.mu.Lock()
	defer b.mu.Unlock()

	q, exists := b.queues[name]
	if !exists {
		q = &Queue{name: name}
		b.queues[name] = q
	}
	return q
}
