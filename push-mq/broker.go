package main

import (
	"errors"
	"fmt"
	"sync"
)

type Broker struct {
	queues    map[string]*Queue
	numQueues int

	// Protects the list of Queues, use whenever accessing the queues
	mu sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		queues:    make(map[string]*Queue),
		numQueues: 0,
	}
}

func (b *Broker) CreateQueue(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.queues[name]; ok {
		return errors.New(fmt.Sprintf("queue %s already exists", name))
	}
	if b.numQueues >= 128 {
		return errors.New(fmt.Sprintf("too many queues %d", b.numQueues))
	}

	newQ := NewQueue(name)
	b.queues[name] = newQ
	b.numQueues++

	// Fire off attached worker
	go newQ.RunQueueWorker()

	return nil
}

func (b *Broker) DeleteQueue(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	queue, ok := b.queues[name]
	if ok {
		delete(b.queues, name)
		b.numQueues--
		close(queue.isClosed)
	}
}

func (b *Broker) Enqueue(name string, payload string) (error, *QueueMsg) {
	b.mu.Lock()
	defer b.mu.Unlock()

	msg := NewQueueMsg(name, payload)
	err, queue := b.getQueue(name)
	if err != nil {
		return err, nil
	}
	queue.Add(msg)
	return nil, msg
}

func (b *Broker) AddSubscriber(metadata SubPolicy, queueName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	err, queue := b.getQueue(queueName)
	if err != nil {
		return err
	}

	err = queue.AddSubscriber(metadata)
	if err != nil {
		return err
	}
	return nil
}

func (b *Broker) UpdateSubscriber(metadata SubPolicy, queueName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	err, queue := b.getQueue(queueName)
	if err != nil {
		return err
	}
	err = queue.UpdateSubscriber(metadata)
	return nil
}

func (b *Broker) RemoveSubscriber(subName string, queueName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	err, queue := b.getQueue(queueName)
	if err != nil {
		return err
	}

	queue.RemoveSubscriber(subName)
	return nil
}

func (b *Broker) getQueue(name string) (error, *Queue) {
	q, exists := b.queues[name]
	if !exists {
		return errors.New(fmt.Sprintf("queue %s does not exist", name)), nil
	}
	return nil, q
}
