package main

import (
	"errors"
	"fmt"
	"sync"
)

type Broker struct {
	queues map[string]*Queue

	// Protects the list of Queues, use whenever accessing the queues
	mu sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		queues: make(map[string]*Queue),
	}
}

func (b *Broker) Enqueue(name string, payload string) (error, *QueueMsg) {
	b.mu.Lock()
	defer b.mu.Unlock()

	msg := &QueueMsg{
		MsgId:   name + "/", //+ uuid.New().String(), // Todo: Replace with non UUID string
		Payload: payload,
	}
	err, queue := b.getQueue(name)
	if err != nil {
		return err, nil
	}
	queue.Add(msg)
	return nil, msg
}

func (b *Broker) Dequeue(name string) (error, *QueueMsg) {
	b.mu.Lock()
	defer b.mu.Unlock()

	err, queue := b.getQueue(name)
	if err != nil {
		return err, nil
	}
	msg := queue.Pop()
	return nil, msg
}

func (b *Broker) AddSubscriber(metadata SubMetadata, queueName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	err, queue := b.getQueue(queueName)
	if err != nil {
		return err
	}

	err = queue.addSubscriber(metadata)
	if err != nil {
		return err
	}
	return nil
}

func (b *Broker) UpdateSubscriber(metadata SubMetadata, queueName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	err, queue := b.getQueue(queueName)
	if err != nil {
		return err
	}
	err = queue.updateSubscriber(metadata)
	return nil
}

func (b *Broker) RemoveSubscriber(subName string, queueName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	err, queue := b.getQueue(queueName)
	if err != nil {
		return err
	}

	queue.removeSubscriber(subName)
	return nil
}

func (b *Broker) getQueue(name string) (error, *Queue) {
	q, exists := b.queues[name]
	if !exists {
		return errors.New(fmt.Sprintf("queue %s does not exist", name)), nil
	}
	return nil, q
}
