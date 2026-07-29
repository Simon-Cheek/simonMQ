package coordinator

import (
	"durable-mq/catalog"
	"durable-mq/delivery"
	"durable-mq/record"
	"durable-mq/wal"
	"fmt"
	"sync"
)

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

func (c *Coordinator) ReplayLog() (map[string]catalog.QueueInfo,
	map[string]*delivery.DeliveryQueueInfo, error) {
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

	}
	catalogSnapshot := c.cat.ReturnQueueResults()
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
