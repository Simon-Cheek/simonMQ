package coordinator

import (
	"durable-mq/catalog"
	"durable-mq/delivery"
	"durable-mq/record"
	"durable-mq/wal"
	"fmt"
	"sync"
)

// Coordinator manages the initial replay of the WAL
// Acts as a wrapper of WAL for writes during queue runtime
// Acts as a wrapper of Catalog as well, coordinating Queue and Sub Metadata
type Coordinator struct {
	log  *wal.WAL
	cat  *catalog.Catalog
	deli *delivery.Delivery
	mu   sync.Mutex // Prevent Append operations from happening while replay occurs
}

// WAL Config
const walDirectory = ""
const maxLogSize = 16 * 8 * 1024 * 1024 // 16MB

func NewCoordinator() (*Coordinator, error) {
	waLog, err := wal.Open(walDirectory, maxLogSize)
	if err != nil {
		return nil, err
	}
	return &Coordinator{
		cat:  catalog.NewCatalog(),
		deli: delivery.NewDelivery(),
		log:  waLog,
	}, nil
}

func (c *Coordinator) ReplayLog() ([]string,
	map[string]*delivery.DeliveryQueueInfo, error) {

	// Don't allow any modifications while WAL is being replayed
	c.mu.Lock()
	defer c.mu.Unlock()

	records, err := c.log.ReadAll()
	if err != nil {
		return nil, nil, err
	}

	for _, rec := range records {
		opType := rec.OpType

		switch opType {
		case record.OpEnqueue:
			err = c.handleEnqueue(*rec)
			if err != nil {
				return nil, nil, err
			}
		case record.OpAck:
			err = c.deli.ProcessAck(*rec)
			if err != nil {
				return nil, nil, err
			}
		default:
			err = c.cat.ProcessRecord(*rec)
			if err != nil {
				return nil, nil, err
			}
		}

		// Delivery needs to be informed if a queue is removed
		if opType == record.OpDeleteQueue {
			c.deli.DeleteQueueMessages(rec.QueueName)
		}

	}
	catalogSnapshot := c.cat.ReturnQueueNames()
	deliveryResults := c.deli.YieldDeliveryData()
	c.deli = nil
	return catalogSnapshot, deliveryResults, nil
}

func (c *Coordinator) handleEnqueue(rec record.Record) error {
	queueName := rec.QueueName
	subList, ok := c.cat.FetchQueueSubList(queueName) // Capture sublist at time of enqueue
	if !ok {
		return fmt.Errorf("queue %s not found", queueName)
	}
	return c.deli.ProcessEnqueue(rec, subList)
}

func (c *Coordinator) CreateQueue(queueName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	rec := &record.Record{
		OpType:    record.OpCreateQueue,
		QueueName: queueName,
	}
	if _, err := c.log.Append(rec); err != nil {
		return err
	}
	return c.cat.ProcessRecord(*rec)
}

func (c *Coordinator) DeleteQueue(queueName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.cat.QueueExists(queueName) {
		return fmt.Errorf("queue %s not found", queueName)
	}

	rec := &record.Record{
		OpType:    record.OpDeleteQueue,
		QueueName: queueName,
	}
	if _, err := c.log.Append(rec); err != nil {
		return err
	}
	return c.cat.ProcessRecord(*rec)
}

func (c *Coordinator) UpdateSubPolicy(queueName string, policy catalog.SubPolicy) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.cat.QueueExists(queueName) {
		return fmt.Errorf("queue %s not found", queueName)
	}

	payload, err := catalog.EncodeSubPolicy(policy)
	if err != nil {
		return err
	}
	rec := &record.Record{
		OpType:    record.OpUpdateSubPolicy,
		QueueName: queueName,
		Payload:   payload,
	}
	if _, err := c.log.Append(rec); err != nil {
		return err
	}
	return c.cat.ProcessRecord(*rec)
}

func (c *Coordinator) DeleteSubPolicy(queueName string, subName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.cat.QueueExists(queueName) {
		return fmt.Errorf("queue %s not found", queueName)
	}

	payload, err := catalog.EncodeSubPolicy(catalog.SubPolicy{SubName: subName})
	if err != nil {
		return err
	}
	rec := &record.Record{
		OpType:    record.OpDeleteSubPolicy,
		QueueName: queueName,
		Payload:   payload,
	}
	if _, err := c.log.Append(rec); err != nil {
		return err
	}
	return c.cat.ProcessRecord(*rec)
}
