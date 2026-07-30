package delivery

import (
	"durable-mq/catalog"
	"durable-mq/record"
	"fmt"
)

// Delivery is only used for startup by a single thread to refresh WAL data
// So it has no need for concurrency mgmt
// It has no live state mgmt of the queue
type Delivery struct {
	Queues map[string]*DeliveryQueueInfo // Maps queueName to QueueInfo
}

type DeliveryQueueInfo struct {
	Messages map[string]*DeliveryMessageInfo // Maps msgID to info
}

type DeliveryMessageInfo struct {
	Content   string
	SubList   map[string]catalog.SubPolicy
	AckedSubs map[string]struct{}
}

func NewDelivery() *Delivery {
	return &Delivery{
		Queues: make(map[string]*DeliveryQueueInfo),
	}
}

func NewDeliveryQueueInfo() *DeliveryQueueInfo {
	return &DeliveryQueueInfo{
		Messages: make(map[string]*DeliveryMessageInfo),
	}
}

func NewDeliveryMessageInfo() *DeliveryMessageInfo {
	return &DeliveryMessageInfo{
		SubList:   make(map[string]catalog.SubPolicy),
		AckedSubs: make(map[string]struct{}),
	}
}

// ProcessEnqueue adds the msg AFTER queue has already been verified by coordinator
// Assumes that msgId collision will not occur (overwrites if so)
func (d *Delivery) ProcessEnqueue(rec record.Record, subList []catalog.SubPolicy) error {
	optype := rec.OpType
	if !(optype == record.OpEnqueue) {
		return fmt.Errorf("invalid op type: %v", optype)
	}
	payload := rec.Payload
	enq, err := DecodeEnqueue(payload)
	if err != nil {
		return err
	}
	msgId, content := enq.MsgId, enq.MsgContent
	queueName := rec.QueueName

	if _, ok := d.Queues[queueName]; !ok {
		d.Queues[queueName] = NewDeliveryQueueInfo()
	}

	msgInfo := NewDeliveryMessageInfo()
	msgInfo.Content = content
	for _, sub := range subList {
		msgInfo.SubList[sub.SubName] = sub
	}
	d.Queues[queueName].Messages[msgId] = msgInfo

	return nil
}

func (d *Delivery) ProcessAck(rec record.Record) error {
	optype := rec.OpType
	if !(optype == record.OpAck) {
		return fmt.Errorf("invalid op type: %v", optype)
	}
	payload := rec.Payload
	ack, err := DecodeAck(payload)
	if err != nil {
		return err
	}
	msgId, subName := ack.MsgId, ack.SubName
	queueName := rec.QueueName

	// Queue and Msg should already exist: if not, error
	if _, ok := d.Queues[queueName]; !ok {
		return fmt.Errorf("message not found queueName: %v", queueName)
	}
	msgInfo, ok := d.Queues[queueName].Messages[msgId]
	if !ok {
		return fmt.Errorf("message not found msgId: %v", msgId)
	}
	_, ok = msgInfo.SubList[subName]
	if !ok {
		return nil // Subname not in SubList, so no need to document
	}
	msgInfo.AckedSubs[subName] = struct{}{}
	return nil
}

func (d *Delivery) DeleteQueueMessages(queueName string) {
	delete(d.Queues, queueName)
}

func (d *Delivery) YieldDeliveryData() map[string]*DeliveryQueueInfo {
	return d.Queues
}
