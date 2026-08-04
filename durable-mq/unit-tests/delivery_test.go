package unit_tests

import (
	"testing"

	"durable-mq/delivery"
	"durable-mq/model"
	"durable-mq/record"
)

func enqueueRec(t *testing.T, queueName, msgId, content string, subs map[string]model.SubPolicy) record.Record {
	t.Helper()
	payload, err := model.EncodeEnqueue(model.Enqueue{MsgId: msgId, MsgContent: content, SubList: subs})
	if err != nil {
		t.Fatalf("EncodeEnqueue returned error: %v", err)
	}
	return record.Record{OpType: record.OpEnqueue, QueueName: queueName, Payload: payload}
}

func ackRec(t *testing.T, queueName, msgId, subName string) record.Record {
	t.Helper()
	payload, err := model.EncodeAck(model.Ack{MsgId: msgId, SubName: subName})
	if err != nil {
		t.Fatalf("EncodeAck returned error: %v", err)
	}
	return record.Record{OpType: record.OpAck, QueueName: queueName, Payload: payload}
}

func twoSubs() map[string]model.SubPolicy {
	return map[string]model.SubPolicy{
		"sub1": {SubName: "sub1", SubURL: "http://a.com", NumberOfRetries: 3},
		"sub2": {SubName: "sub2", SubURL: "http://b.com", NumberOfRetries: 5},
	}
}

func TestDeliveryProcessEnqueue(t *testing.T) {
	d := delivery.NewDelivery()
	subs := twoSubs()

	if err := d.ProcessEnqueue(enqueueRec(t, "orders", "msg-1", "hello", subs)); err != nil {
		t.Fatalf("ProcessEnqueue returned error: %v", err)
	}

	data := d.YieldDeliveryData()
	qInfo, ok := data["orders"]
	if !ok {
		t.Fatal("queue missing from delivery data")
	}
	msg, ok := qInfo.Messages["msg-1"]
	if !ok {
		t.Fatal("message missing from delivery data")
	}
	if msg.Content != "hello" {
		t.Errorf("content = %q, want %q", msg.Content, "hello")
	}
	if len(msg.SubList) != 2 {
		t.Errorf("sub list has %d entries, want 2", len(msg.SubList))
	}
	if msg.SubList["sub1"] != subs["sub1"] || msg.SubList["sub2"] != subs["sub2"] {
		t.Errorf("sub list = %+v, want %+v", msg.SubList, subs)
	}
	if len(msg.AckedSubs) != 0 {
		t.Errorf("a freshly enqueued message already has %d acked subs", len(msg.AckedSubs))
	}
}

func TestDeliveryProcessEnqueueRejectsWrongOpType(t *testing.T) {
	d := delivery.NewDelivery()

	rec := enqueueRec(t, "orders", "msg-1", "hello", twoSubs())
	rec.OpType = record.OpCreateQueue

	if err := d.ProcessEnqueue(rec); err == nil {
		t.Error("ProcessEnqueue accepted a non-ENQUEUE record")
	}
}

func TestDeliveryProcessEnqueueRejectsCorruptPayload(t *testing.T) {
	d := delivery.NewDelivery()

	rec := record.Record{OpType: record.OpEnqueue, QueueName: "orders", Payload: []byte("not json")}
	if err := d.ProcessEnqueue(rec); err == nil {
		t.Error("ProcessEnqueue accepted an undecodable payload")
	}
}

func TestDeliveryPartialAck(t *testing.T) {
	d := delivery.NewDelivery()
	if err := d.ProcessEnqueue(enqueueRec(t, "orders", "msg-1", "hello", twoSubs())); err != nil {
		t.Fatalf("ProcessEnqueue returned error: %v", err)
	}

	if err := d.ProcessAck(ackRec(t, "orders", "msg-1", "sub1")); err != nil {
		t.Fatalf("ProcessAck returned error: %v", err)
	}

	msg := d.YieldDeliveryData()["orders"].Messages["msg-1"]
	if _, acked := msg.AckedSubs["sub1"]; !acked {
		t.Error("sub1 is not marked acked")
	}
	if _, acked := msg.AckedSubs["sub2"]; acked {
		t.Error("sub2 is marked acked but was never acked")
	}
	// The full sub list must survive an ack — it's what tells recovery who
	// still needs delivery.
	if len(msg.SubList) != 2 {
		t.Errorf("sub list has %d entries after an ack, want 2", len(msg.SubList))
	}
}

func TestDeliveryFullAck(t *testing.T) {
	d := delivery.NewDelivery()
	if err := d.ProcessEnqueue(enqueueRec(t, "orders", "msg-1", "hello", twoSubs())); err != nil {
		t.Fatalf("ProcessEnqueue returned error: %v", err)
	}

	for _, sub := range []string{"sub1", "sub2"} {
		if err := d.ProcessAck(ackRec(t, "orders", "msg-1", sub)); err != nil {
			t.Fatalf("ProcessAck(%s) returned error: %v", sub, err)
		}
	}

	msg := d.YieldDeliveryData()["orders"].Messages["msg-1"]
	if len(msg.AckedSubs) != 2 {
		t.Errorf("got %d acked subs, want 2", len(msg.AckedSubs))
	}
}

func TestDeliveryDuplicateAckIsIdempotent(t *testing.T) {
	d := delivery.NewDelivery()
	if err := d.ProcessEnqueue(enqueueRec(t, "orders", "msg-1", "hello", twoSubs())); err != nil {
		t.Fatalf("ProcessEnqueue returned error: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := d.ProcessAck(ackRec(t, "orders", "msg-1", "sub1")); err != nil {
			t.Fatalf("ProcessAck attempt %d returned error: %v", i, err)
		}
	}

	msg := d.YieldDeliveryData()["orders"].Messages["msg-1"]
	if len(msg.AckedSubs) != 1 {
		t.Errorf("got %d acked subs after repeated acks of the same sub, want 1", len(msg.AckedSubs))
	}
}

func TestDeliveryProcessAckRejectsWrongOpType(t *testing.T) {
	d := delivery.NewDelivery()

	rec := ackRec(t, "orders", "msg-1", "sub1")
	rec.OpType = record.OpEnqueue

	if err := d.ProcessAck(rec); err == nil {
		t.Error("ProcessAck accepted a non-ACK record")
	}
}

func TestDeliveryProcessAckRejectsCorruptPayload(t *testing.T) {
	d := delivery.NewDelivery()

	rec := record.Record{OpType: record.OpAck, QueueName: "orders", Payload: []byte("not json")}
	if err := d.ProcessAck(rec); err == nil {
		t.Error("ProcessAck accepted an undecodable payload")
	}
}

func TestDeliveryProcessAckUnknownQueueOrMessage(t *testing.T) {
	d := delivery.NewDelivery()

	if err := d.ProcessAck(ackRec(t, "ghost", "msg-1", "sub1")); err == nil {
		t.Error("ProcessAck against an unknown queue returned nil error")
	}

	if err := d.ProcessEnqueue(enqueueRec(t, "orders", "msg-1", "hello", twoSubs())); err != nil {
		t.Fatalf("ProcessEnqueue returned error: %v", err)
	}
	if err := d.ProcessAck(ackRec(t, "orders", "msg-does-not-exist", "sub1")); err == nil {
		t.Error("ProcessAck against an unknown message returned nil error")
	}
}

func TestDeliveryProcessAckIgnoresSubOutsideSnapshot(t *testing.T) {
	d := delivery.NewDelivery()
	if err := d.ProcessEnqueue(enqueueRec(t, "orders", "msg-1", "hello", twoSubs())); err != nil {
		t.Fatalf("ProcessEnqueue returned error: %v", err)
	}

	// A subscriber added after this message was enqueued isn't in its
	// snapshot — acking it is a no-op, not an error.
	if err := d.ProcessAck(ackRec(t, "orders", "msg-1", "sub-added-later")); err != nil {
		t.Fatalf("ProcessAck returned error: %v", err)
	}

	msg := d.YieldDeliveryData()["orders"].Messages["msg-1"]
	if _, recorded := msg.AckedSubs["sub-added-later"]; recorded {
		t.Error("an ack from a subscriber outside the message's snapshot was recorded")
	}
	if len(msg.AckedSubs) != 0 {
		t.Errorf("got %d acked subs, want 0", len(msg.AckedSubs))
	}
}

func TestDeliveryDeleteQueueMessages(t *testing.T) {
	d := delivery.NewDelivery()
	if err := d.ProcessEnqueue(enqueueRec(t, "orders", "msg-1", "hello", twoSubs())); err != nil {
		t.Fatalf("ProcessEnqueue returned error: %v", err)
	}
	if err := d.ProcessEnqueue(enqueueRec(t, "events", "msg-2", "world", twoSubs())); err != nil {
		t.Fatalf("ProcessEnqueue returned error: %v", err)
	}

	d.DeleteQueueMessages("orders")

	data := d.YieldDeliveryData()
	if _, ok := data["orders"]; ok {
		t.Error("orders queue still present after DeleteQueueMessages")
	}
	if _, ok := data["events"]; !ok {
		t.Error("events queue was removed but shouldn't have been")
	}
}

func TestDeliveryMultipleQueuesAndMessagesStayIsolated(t *testing.T) {
	d := delivery.NewDelivery()

	if err := d.ProcessEnqueue(enqueueRec(t, "orders", "msg-1", "a", twoSubs())); err != nil {
		t.Fatalf("ProcessEnqueue returned error: %v", err)
	}
	if err := d.ProcessEnqueue(enqueueRec(t, "orders", "msg-2", "b", twoSubs())); err != nil {
		t.Fatalf("ProcessEnqueue returned error: %v", err)
	}
	if err := d.ProcessEnqueue(enqueueRec(t, "events", "msg-3", "c", twoSubs())); err != nil {
		t.Fatalf("ProcessEnqueue returned error: %v", err)
	}

	if err := d.ProcessAck(ackRec(t, "orders", "msg-1", "sub1")); err != nil {
		t.Fatalf("ProcessAck returned error: %v", err)
	}

	data := d.YieldDeliveryData()
	if len(data["orders"].Messages) != 2 {
		t.Errorf("orders has %d messages, want 2", len(data["orders"].Messages))
	}
	if len(data["events"].Messages) != 1 {
		t.Errorf("events has %d messages, want 1", len(data["events"].Messages))
	}
	if len(data["orders"].Messages["msg-1"].AckedSubs) != 1 {
		t.Error("msg-1 should have exactly one acked sub")
	}
	if len(data["orders"].Messages["msg-2"].AckedSubs) != 0 {
		t.Error("msg-2 was acked but the ack was addressed to msg-1")
	}
	if len(data["events"].Messages["msg-3"].AckedSubs) != 0 {
		t.Error("msg-3 was acked but the ack was addressed to a different queue")
	}
}

func TestDeliveryEnqueueWithNoSubscribers(t *testing.T) {
	d := delivery.NewDelivery()

	// A queue with no registered subscribers still produces a valid record;
	// it just has nobody to deliver to.
	if err := d.ProcessEnqueue(enqueueRec(t, "orders", "msg-1", "hello", map[string]model.SubPolicy{})); err != nil {
		t.Fatalf("ProcessEnqueue returned error: %v", err)
	}

	msg := d.YieldDeliveryData()["orders"].Messages["msg-1"]
	if len(msg.SubList) != 0 {
		t.Errorf("sub list has %d entries, want 0", len(msg.SubList))
	}

	// Acking against an empty snapshot is a no-op rather than a crash.
	if err := d.ProcessAck(ackRec(t, "orders", "msg-1", "whoever")); err != nil {
		t.Fatalf("ProcessAck returned error: %v", err)
	}
}
