package catalog

import (
	"durable-mq/record"
	"fmt"
	"sync"
)

// TODO: Consider more granular locking for performance
type Catalog struct {
	queues map[string]*QueueInfo
	mu     sync.Mutex
}

func NewCatalog() *Catalog {
	return &Catalog{queues: make(map[string]*QueueInfo)}
}

type QueueInfo struct {
	Name        string
	SubPolicies map[string]SubPolicy
}

func NewQueueInfo(name string) *QueueInfo {
	return &QueueInfo{
		Name:        name,
		SubPolicies: make(map[string]SubPolicy),
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

// ReturnQueueNames returns the set of currently known queue names
func (c *Catalog) ReturnQueueNames() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	names := make([]string, 0, len(c.queues))
	for queueName := range c.queues {
		names = append(names, queueName)
	}
	return names
}

func (c *Catalog) QueueExists(queueName string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.queues[queueName]
	return ok
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

func (c *Catalog) FetchQueueSubList(queueName string) (map[string]SubPolicy, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	q, ok := c.queues[queueName]
	if !ok {
		return nil, false
	}
	policies := make(map[string]SubPolicy, len(q.SubPolicies))
	for name, policy := range q.SubPolicies {
		policies[name] = policy
	}
	return policies, true
}

func (c *Catalog) updateSubPolicy(queueName string, payload []byte) error {
	subPolicy, err := DecodeSubPolicy(payload)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	queue, ok := c.queues[queueName]
	if !ok {
		return fmt.Errorf("queue %s not found", queueName)
	}
	queue.SubPolicies[subPolicy.SubName] = subPolicy
	return nil
}

func (c *Catalog) removeSubPolicy(queueName string, payload []byte) error {
	subPolicy, err := DecodeSubPolicy(payload)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	queue, ok := c.queues[queueName]
	if !ok {
		return fmt.Errorf("queue %s not found", queueName)
	}
	delete(queue.SubPolicies, subPolicy.SubName)
	return nil
}
