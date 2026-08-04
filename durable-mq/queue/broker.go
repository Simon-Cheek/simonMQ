package queue

import (
	"durable-mq/catalog"
	"durable-mq/coordinator"
	"durable-mq/model"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type Broker struct {
	queues    map[string]*Queue
	numQueues int
	cord      *coordinator.Coordinator

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
		if err := b.CreateQueue(qName, true); err != nil {
			continue
		}
	}
	for qName, di := range dInfo {
		if _, ok := b.queues[qName]; !ok {
			continue
		}
		for msgId, msgInfo := range di.Messages {
			content := msgInfo.Content
			subList := msgInfo.SubList
			ackedSubs := msgInfo.AckedSubs

			hasUnackedSubs := false
			for subName := range subList {
				if _, ok := ackedSubs[subName]; !ok {
					hasUnackedSubs = true
					break
				}
			}
			if !hasUnackedSubs {
				continue
			}
			b.EnqueueFromWAL(qName, msgId, content, subList, ackedSubs)
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

	if !fromWAL {
		if err := b.cord.CreateQueue(name); err != nil {
			return err
		}
	}

	newQ := NewQueue(name, func(msgId, subName string) error {
		return b.cord.Ack(name, msgId, subName)
	})
	b.queues[name] = newQ
	b.numQueues++

	go newQ.RunQueueWorker()

	return nil
}

func (b *Broker) DeleteQueue(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	q, ok := b.queues[name]
	if !ok {
		return fmt.Errorf("queue %s not found", name)
	}

	if err := b.cord.DeleteQueue(name); err != nil {
		return err
	}

	delete(b.queues, name)
	b.numQueues--
	close(q.isClosed) // stop the worker goroutine

	return nil
}

// Close stops every queue worker and releases the WAL
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, q := range b.queues {
		close(q.isClosed) // stop the worker goroutine
	}
	b.queues = make(map[string]*Queue)
	b.numQueues = 0

	return b.cord.Close()
}

func (b *Broker) Enqueue(queueName string, payload string) error {
	b.mu.Lock()
	q, ok := b.queues[queueName]
	b.mu.Unlock()
	if !ok {
		return fmt.Errorf("queue %s not found", queueName)
	}

	msgId := queueName + "-" + uuid.New().String()

	subPolicies, err := b.cord.Enqueue(queueName, msgId, payload)
	if err != nil {
		return err
	}

	msg := NewQueueMsg(msgId, payload, subPolicies, nil)
	q.Add(msg)
	return nil
}

func (b *Broker) EnqueueFromWAL(queueName string, msgId string, payload string,
	subPolicies map[string]model.SubPolicy, ackedSubs map[string]struct{}) {
	b.mu.Lock()
	q, ok := b.queues[queueName]
	b.mu.Unlock()
	if !ok {
		return
	}
	msg := NewQueueMsg(msgId, payload, subPolicies, ackedSubs)
	q.Add(msg)
}

func (b *Broker) AllQueueInfo() []catalog.QueueInfo {
	return b.cord.AllQueueInfo()
}

func (b *Broker) AddSubscriber(metadata model.SubPolicy, queueName string) error {
	return b.cord.UpdateSubPolicy(queueName, metadata)
}

func (b *Broker) UpdateSubscriber(metadata model.SubPolicy, queueName string) error {
	return b.cord.UpdateSubPolicy(queueName, metadata)
}

func (b *Broker) RemoveSubscriber(subName string, queueName string) error {
	return b.cord.DeleteSubPolicy(queueName, subName)
}
