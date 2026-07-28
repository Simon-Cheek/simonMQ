package delivery

import (
	"durable-mq/record"
	"sync"
)

type Delivery struct {
	mu     sync.Mutex
	queues map[string]*DeliveryQueueInfo
}

type DeliveryQueueInfo struct {
	messages map[string]*DeliveryMessageInfo // Maps msgID to info
}

type DeliveryMessageInfo struct {
	content   string
	subList   map[string]struct{}
	ackedSubs map[string]struct{}
}

func NewDelivery() *Delivery {
	return &Delivery{
		queues: make(map[string]*DeliveryQueueInfo),
	}
}

func (d *Delivery) ProcessEnqueue(rec record.Record) error {}

func (d *Delivery) ProcessAck(rec record.Record) error {}
