package main

import "sync"

type Broker struct {
	queues map[string]*Queue
	mu     sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		queues: make(map[string]*Queue),
	}
}

func (b *Broker) Enqueue(name string, payload string) *QueueMsg {

	msg := &QueueMsg{
		MsgId:   name + "/", //+ uuid.New().String(), // Todo: Replace with non UUID string
		Payload: payload,
	}

	queue := b.getOrCreateQueue(name)
	queue.Add(msg)
	return msg
}

func (b *Broker) Dequeue(name string) *QueueMsg {

	queue := b.getOrCreateQueue(name)
	msg := queue.Pop()
	return msg
}

func (b *Broker) getOrCreateQueue(name string) *Queue {
	b.mu.Lock()
	defer b.mu.Unlock()

	q, exists := b.queues[name]
	if !exists {
		q = newQueue(name)
		b.queues[name] = q
	}
	return q
}
