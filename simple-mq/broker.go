package main

import "sync"
import "github.com/google/uuid"

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

func (b *Broker) enqueue(name string, payload string) *QueueMsg {

	msg := &QueueMsg{
		MsgId:   name + "/" + uuid.New().String(),
		Payload: payload,
	}

	// TODO: Lock by Queue instead of globally
	b.mu.Lock()
	defer b.mu.Unlock()

	val, exists := b.queues[name]
	if exists && len(val.messages) > 0 {
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
	if exists {
		msg := val.messages[0]
		val.messages = val.messages[1:]
		return msg
	} else {
		return nil
	}
}
