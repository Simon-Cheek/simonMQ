package delivery

import (
	"durable-mq/record"
	"fmt"
)

// Delivery is only used for startup by a single thread to refresh WAL data
// So it has no need for concurrency mgmt
// It has no live state mgmt of the queue
type Delivery struct {
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

func NewDeliveryQueueInfo() *DeliveryQueueInfo {
	return &DeliveryQueueInfo{
		messages: make(map[string]*DeliveryMessageInfo),
	}
}

func NewDeliveryMessageInfo() *DeliveryMessageInfo {
	return &DeliveryMessageInfo{
		subList:   make(map[string]struct{}),
		ackedSubs: make(map[string]struct{}),
	}
}

// ProcessEnqueue adds the msg AFTER queue has already been verified by coordinator
// Assumes that msgId collision will not occur (overwrites if so)
func (d *Delivery) ProcessEnqueue(rec record.Record, subList []string) error {
	optype := rec.OpType
	if !(optype == record.OpEnqueue) {
		return fmt.Errorf("invalid op type: %v", optype)
	}
	payload := rec.Payload
	enq, err := decodeEnqueue(payload)
	if err != nil {
		return err
	}
	msgId, content := enq.MsgId, enq.MsgContent
	queueName := rec.QueueName

	if _, ok := d.queues[queueName]; !ok {
		d.queues[queueName] = NewDeliveryQueueInfo()
	}

	msgInfo := NewDeliveryMessageInfo()
	msgInfo.content = content
	for _, sub := range subList {
		msgInfo.subList[sub] = struct{}{}
	}
	d.queues[queueName].messages[msgId] = msgInfo

	return nil
}

func (d *Delivery) ProcessAck(rec record.Record) error {
	optype := rec.OpType
	if !(optype == record.OpAck) {
		return fmt.Errorf("invalid op type: %v", optype)
	}
	payload := rec.Payload
	ack, err := decodeAck(payload)
	if err != nil {
		return err
	}
	msgId, subName := ack.MsgId, ack.SubName
	queueName := rec.QueueName

	// Queue and Msg should already exist: if not, error
	if _, ok := d.queues[queueName]; !ok {
		return fmt.Errorf("message not found queueName: %v", queueName)
	}
	msgInfo, ok := d.queues[queueName].messages[msgId]
	if !ok {
		return fmt.Errorf("message not found msgId: %v", msgId)
	}
	_, ok = msgInfo.subList[subName]
	if !ok {
		return nil // Subname not in subList, so no need to document
	}
	msgInfo.ackedSubs[subName] = struct{}{}
	return nil
}

func (d *Delivery) YieldDeliveryData() map[string]*DeliveryQueueInfo {
	return d.queues
}
