package catalog

import (
	"durable-mq/record"
	"fmt"
	"sync"
)

type Catalog struct {
	queues map[string]*QueueInfo
	mu     sync.Mutex
}

type QueueInfo struct {
	name        string
	subPolicies map[string]SubPolicy
}

func NewQueueInfo(name string) *QueueInfo {
	return &QueueInfo{
		name:        name,
		subPolicies: make(map[string]SubPolicy),
	}
}

func (c *Catalog) ProcessRecord(rec record.Record) error {
	optype := rec.OpType

	switch optype {
	case record.OpCreateQueue:
		c.createQueue(rec.QueueName)
		return nil
	case record.OpDeleteQueue:
		c.removeQueue(rec.QueueName)
		return nil
	case record.OpUpdateSubPolicy:
		return c.updateSubPolicy(rec.QueueName, rec.Payload)
	case record.OpDeleteSubPolicy:
		return c.removeSubPolicy(rec.QueueName, rec.Payload)
	default:
		return fmt.Errorf("invalid optype passed to ProcessRecord: %v", optype)

	}

}

func (c *Catalog) createQueue(queueName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queues[queueName] = NewQueueInfo(queueName) // Force refresh even if already exists
}

func (c *Catalog) removeQueue(queueName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.queues, queueName)
}

func (c *Catalog) updateSubPolicy(queueName string, payload []byte) error {
	subPolicy, err := decodeSubPolicy(payload)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	queue, ok := c.queues[queueName]
	if !ok {
		return fmt.Errorf("queue %s not found", queueName)
	}
	queue.subPolicies[subPolicy.SubName] = subPolicy
	return nil
}

func (c *Catalog) removeSubPolicy(queueName string, payload []byte) error {
	subPolicy, err := decodeSubPolicy(payload)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	queue, ok := c.queues[queueName]
	if !ok {
		return fmt.Errorf("queue %s not found", queueName)
	}
	delete(queue.subPolicies, subPolicy.SubName)
	return nil
}
