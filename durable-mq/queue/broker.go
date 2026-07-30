package main

import (
	"durable-mq/catalog"
	"durable-mq/coordinator"
	"fmt"
	"sync"
)

type Broker struct {
	queues    map[string]*Queue
	numQueues int
	cord      *coordinator.Coordinator

	// Protects the list of Queues, use whenever accessing the queues
	mu sync.Mutex
}

func NewBroker() *Broker {
	c, err := coordinator.NewCoordinator()
	if err != nil {
		panic(err)
	}
	return &Broker{
		queues:    make(map[string]*Queue),
		numQueues: 0,
		cord:      c,
	}
}

func (b *Broker) RestoreWAL() error {
	
	qs, dInfo, err := b.cord.ReplayLog()
	if err != nil {
		return err
	}
	for _, qName := range qs {
		err = b.CreateQueue(qName, true)
		if err != nil {
			continue
		}
	}
	for qName, di := range dInfo {
		if _, ok := b.queues[qName]; !ok {
			continue
		}
		for _, msgInfo := range di.Messages {
			content := msgInfo.Content
			subList := msgInfo.SubList
			ackedSubs := msgInfo.AckedSubs

			// Don't queue msg if all subs have acked
			hasUnackedSubs := false
			for _, sub := range subList {
				if _, ok := ackedSubs[sub.SubName]; !ok {
					hasUnackedSubs = true
					break
				}
			}
			if !hasUnackedSubs {
				continue
			}
			b.EnqueueFromWAL(qName, content, subList)
		}
	}
	return nil
}

func (b *Broker) CreateQueue(name string, fromWAL bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.queues[name]; ok {
		return fmt.Errorf("queue %s already exists", name)
	}
	if b.numQueues >= 128 {
		return fmt.Errorf("too many queues %d", b.numQueues)
	}

	newQ := NewQueue(name)
	b.queues[name] = newQ
	b.numQueues++

	// Fire off attached worker
	go newQ.RunQueueWorker()

	if !fromWAL {
		// b.cord. CREATE QUEUE
	}

	return nil
}

func (b *Broker) DeleteQueue(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.queues[name]; ok {
		delete(b.queues, name)
	}
	b.cord.DeleteQueue(name)
}

func (b *Broker) Enqueue(queueName string, payload string) error {
}

func (b *Broker) EnqueueFromWAL(queueName string, payload string, subPolicies map[string]catalog.SubPolicy) {
}

func (b *Broker) AddSubscriber(metadata catalog.SubPolicy, queueName string) error {
}

func (b *Broker) UpdateSubscriber(metadata catalog.SubPolicy, queueName string) error {
}

func (b *Broker) RemoveSubscriber(subName string, queueName string) error {
}
