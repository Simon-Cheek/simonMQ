package main

import "sync"

type Broker struct {
	queues map[string]*Queue
	mu     sync.Mutex
}

type Queue struct {
	name string
	// TODO: Refactor to use more efficient data structure
	messages []*QueueMsg
}

type QueueMsg struct {
	MsgId   string
	Payload string
}

func NewBroker() *Broker {
	return &Broker{
		queues: make(map[string]*Queue),
	}
}

func (b *Broker) enqueue(name string, payload string) *QueueMsg {

	msg := &QueueMsg{
		MsgId:   name + "/", // + uuid.New().String(), Todo: Remove UUID Generation. bottlenecks throughput
		Payload: payload,
	}

	// TODO: Lock by Queue instead of globally
	b.mu.Lock()
	defer b.mu.Unlock()

	val, exists := b.queues[name]
	if exists {
		val.messages = append(val.messages, msg)
	} else {
		b.queues[name] = &Queue{
			name:     name,
			messages: []*QueueMsg{msg},
		}
	}

	return msg
}

func (b *Broker) dequeue(name string) *QueueMsg {

	// TODO: Lock by Queue instead of globally
	b.mu.Lock()
	defer b.mu.Unlock()

	val, exists := b.queues[name]
	if exists && len(val.messages) > 0 {
		msg := val.messages[0]
		val.messages = val.messages[1:]
		return msg
	}
	return nil
}
