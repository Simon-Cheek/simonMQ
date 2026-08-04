package unit_tests

import (
	"fmt"
	"slices"
	"sort"
	"testing"

	"durable-mq/catalog"
	"durable-mq/coordinator"
	"durable-mq/delivery"
	"durable-mq/model"
	"durable-mq/record"
)

// recordLog builds a WAL-shaped record slice with sequential LSNs, standing in
// for the bounded window CompactRecords receives during checkpoint derivation.
type recordLog []*record.Record

func (l *recordLog) add(rec record.Record) {
	rec.LSN = uint64(len(*l) + 1)
	*l = append(*l, &rec)
}

func (l *recordLog) createQueue(name string) {
	l.add(record.Record{OpType: record.OpCreateQueue, QueueName: name})
}

func (l *recordLog) deleteQueue(name string) {
	l.add(record.Record{OpType: record.OpDeleteQueue, QueueName: name})
}

func (l *recordLog) updateSub(t *testing.T, queueName string, p model.SubPolicy) {
	t.Helper()
	l.add(subPolicyRec(t, record.OpUpdateSubPolicy, queueName, p))
}

func (l *recordLog) deleteSub(t *testing.T, queueName, subName string) {
	t.Helper()
	l.add(subPolicyRec(t, record.OpDeleteSubPolicy, queueName, model.SubPolicy{SubName: subName}))
}

func (l *recordLog) enqueue(t *testing.T, queueName, msgId, content string, subs map[string]model.SubPolicy) {
	t.Helper()
	l.add(enqueueRec(t, queueName, msgId, content, subs))
}

func (l *recordLog) ack(t *testing.T, queueName, msgId, subName string) {
	t.Helper()
	l.add(ackRec(t, queueName, msgId, subName))
}

func (l *recordLog) beginCheckpoint() {
	l.add(record.Record{OpType: record.OpBeginCheckpoint})
}

func countOps(recs []*record.Record, op record.OpType) int {
	n := 0
	for _, rec := range recs {
		if rec.OpType == op {
			n++
		}
	}
	return n
}

// replayRecords applies recs exactly the way Coordinator.ReplayLog does, so a
// compacted stream can be checked against the state it actually reconstructs.
func replayRecords(t *testing.T, recs []*record.Record) (*catalog.Catalog, *delivery.Delivery) {
	t.Helper()
	cat := catalog.NewCatalog()
	deli := delivery.NewDelivery()

	for i, rec := range recs {
		var err error
		switch rec.OpType {
		case record.OpEnqueue:
			err = deli.ProcessEnqueue(*rec)
		case record.OpAck:
			err = deli.ProcessAck(*rec)
		default:
			err = cat.ProcessRecord(*rec)
		}
		if err != nil {
			t.Fatalf("replaying record %d (op %v, queue %q) returned error: %v", i, rec.OpType, rec.QueueName, err)
		}
		if rec.OpType == record.OpDeleteQueue {
			deli.DeleteQueueMessages(rec.QueueName)
		}
	}
	return cat, deli
}

// liveState renders the state a Broker would actually restore: queues, their
// policies, and only those messages still owed to at least one subscriber
// (Broker.RestoreWAL drops the rest). Compaction discards fully-acked
// messages that a full replay still carries, so this is the level at which
// the two must agree.
func liveState(cat *catalog.Catalog, deli *delivery.Delivery) []string {
	var lines []string

	for _, qi := range cat.AllQueueInfo() {
		lines = append(lines, fmt.Sprintf("queue %s", qi.Name))
		for subName, p := range qi.SubPolicies {
			lines = append(lines, fmt.Sprintf("sub %s/%s url=%s retries=%d", qi.Name, subName, p.SubURL, p.NumberOfRetries))
		}
	}

	for qName, qInfo := range deli.YieldDeliveryData() {
		for msgId, msg := range qInfo.Messages {
			outstanding := false
			for subName := range msg.SubList {
				if _, acked := msg.AckedSubs[subName]; !acked {
					outstanding = true
					break
				}
			}
			if !outstanding {
				continue
			}

			var subs, acked []string
			for s := range msg.SubList {
				subs = append(subs, s)
			}
			for s := range msg.AckedSubs {
				acked = append(acked, s)
			}
			sort.Strings(subs)
			sort.Strings(acked)
			lines = append(lines, fmt.Sprintf("msg %s/%s content=%q subs=%v acked=%v", qName, msgId, msg.Content, subs, acked))
		}
	}

	sort.Strings(lines)
	return lines
}

func TestCompactDropsFullyAckedMessages(t *testing.T) {
	var log recordLog
	log.createQueue("orders")
	log.updateSub(t, "orders", subPolicy("sub1"))
	log.updateSub(t, "orders", subPolicy("sub2"))
	log.enqueue(t, "orders", "msg-1", "settled", twoSubs())
	log.ack(t, "orders", "msg-1", "sub1")
	log.ack(t, "orders", "msg-1", "sub2")

	out := coordinator.CompactRecords(log)

	if n := countOps(out, record.OpEnqueue); n != 0 {
		t.Errorf("got %d ENQUEUE records, want 0 — a fully acked message should be dropped", n)
	}
	if n := countOps(out, record.OpAck); n != 0 {
		t.Errorf("got %d ACK records, want 0 — acks for a dropped message should go too", n)
	}
	if n := countOps(out, record.OpCreateQueue); n != 1 {
		t.Errorf("got %d CREATE_QUEUE records, want 1", n)
	}
}

func TestCompactKeepsPartiallyAckedMessages(t *testing.T) {
	var log recordLog
	log.createQueue("orders")
	log.updateSub(t, "orders", subPolicy("sub1"))
	log.updateSub(t, "orders", subPolicy("sub2"))
	log.enqueue(t, "orders", "msg-1", "in flight", twoSubs())
	log.ack(t, "orders", "msg-1", "sub1")

	out := coordinator.CompactRecords(log)

	if n := countOps(out, record.OpEnqueue); n != 1 {
		t.Errorf("got %d ENQUEUE records, want 1 — a partially acked message must survive", n)
	}
	if n := countOps(out, record.OpAck); n != 1 {
		t.Errorf("got %d ACK records, want 1 — the ack already received must survive so sub1 isn't redelivered", n)
	}
}

func TestCompactCollapsesSubPolicyHistory(t *testing.T) {
	var log recordLog
	log.createQueue("orders")
	log.updateSub(t, "orders", model.SubPolicy{SubName: "sub1", SubURL: "http://v1.com", NumberOfRetries: 1})
	log.updateSub(t, "orders", model.SubPolicy{SubName: "sub1", SubURL: "http://v2.com", NumberOfRetries: 2})
	log.updateSub(t, "orders", model.SubPolicy{SubName: "sub1", SubURL: "http://v3.com", NumberOfRetries: 3})
	log.updateSub(t, "orders", subPolicy("doomed"))
	log.deleteSub(t, "orders", "doomed")

	out := coordinator.CompactRecords(log)

	if n := countOps(out, record.OpUpdateSubPolicy); n != 1 {
		t.Fatalf("got %d UPDATE_SUB_POLICY records, want 1 (only the surviving final policy)", n)
	}
	if n := countOps(out, record.OpDeleteSubPolicy); n != 0 {
		t.Errorf("got %d DELETE_SUB_POLICY records, want 0 — a deleted policy should just be absent", n)
	}

	cat, _ := replayRecords(t, out)
	subs, ok := cat.FetchQueueSubList("orders")
	if !ok {
		t.Fatal("orders queue missing after replaying compacted output")
	}
	if len(subs) != 1 || subs["sub1"].SubURL != "http://v3.com" || subs["sub1"].NumberOfRetries != 3 {
		t.Errorf("replayed policies = %+v, want only sub1 at its final v3 values", subs)
	}
}

func TestCompactDropsDeletedQueuesEntirely(t *testing.T) {
	var log recordLog
	log.createQueue("orders")
	log.updateSub(t, "orders", subPolicy("sub1"))
	log.enqueue(t, "orders", "msg-1", "doomed", twoSubs())
	log.deleteQueue("orders")
	log.createQueue("survivor")

	out := coordinator.CompactRecords(log)

	for _, rec := range out {
		if rec.QueueName == "orders" {
			t.Errorf("record for the deleted queue survived compaction: op %v", rec.OpType)
		}
	}
	if n := countOps(out, record.OpDeleteQueue); n != 0 {
		t.Errorf("got %d DELETE_QUEUE records, want 0 — a deleted queue should just be absent", n)
	}
	if n := countOps(out, record.OpCreateQueue); n != 1 {
		t.Errorf("got %d CREATE_QUEUE records, want 1 (survivor only)", n)
	}
}

func TestCompactRecreatedQueueDoesNotInheritOldSubs(t *testing.T) {
	var log recordLog
	log.createQueue("orders")
	log.updateSub(t, "orders", subPolicy("old-sub"))
	log.deleteQueue("orders")
	log.createQueue("orders")
	log.updateSub(t, "orders", subPolicy("new-sub"))

	out := coordinator.CompactRecords(log)

	cat, _ := replayRecords(t, out)
	subs, ok := cat.FetchQueueSubList("orders")
	if !ok {
		t.Fatal("orders queue missing after replaying compacted output")
	}
	if _, leaked := subs["old-sub"]; leaked {
		t.Error("a subscriber from before the queue was deleted survived into the re-created queue")
	}
	if _, ok := subs["new-sub"]; !ok {
		t.Error("the subscriber added after re-creation is missing")
	}
}

func TestCompactDropsCheckpointMarkers(t *testing.T) {
	var log recordLog
	log.createQueue("orders")
	log.beginCheckpoint()
	endPayload, err := model.EncodeEndCheckpoint(model.EndCheckpoint{FileName: "x.ckpt", FileChecksum: "abcd1234"})
	if err != nil {
		t.Fatalf("EncodeEndCheckpoint returned error: %v", err)
	}
	log.add(record.Record{OpType: record.OpEndCheckpoint, Payload: endPayload})

	out := coordinator.CompactRecords(log)

	// Markers describe log structure, not state — a checkpoint file that
	// carried them would nest checkpoints inside checkpoints.
	if n := countOps(out, record.OpBeginCheckpoint); n != 0 {
		t.Errorf("got %d BEGIN_CHECKPOINT records in compacted output, want 0", n)
	}
	if n := countOps(out, record.OpEndCheckpoint); n != 0 {
		t.Errorf("got %d END_CHECKPOINT records in compacted output, want 0", n)
	}
}

func TestCompactSkipsEnqueueForUnknownQueue(t *testing.T) {
	var log recordLog
	// An enqueue whose queue was never created (or was already deleted) has
	// nowhere to land — replaying it would fail catalog/delivery lookups.
	log.enqueue(t, "ghost", "msg-1", "orphan", twoSubs())
	log.createQueue("orders")

	out := coordinator.CompactRecords(log)

	if n := countOps(out, record.OpEnqueue); n != 0 {
		t.Errorf("got %d ENQUEUE records, want 0 — an enqueue for an unknown queue should be skipped", n)
	}
	replayRecords(t, out) // must not error
}

func TestCompactSkipsCorruptPayloads(t *testing.T) {
	var log recordLog
	log.createQueue("orders")
	log.add(record.Record{OpType: record.OpEnqueue, QueueName: "orders", Payload: []byte("not json")})
	log.add(record.Record{OpType: record.OpUpdateSubPolicy, QueueName: "orders", Payload: []byte("not json")})
	log.add(record.Record{OpType: record.OpAck, QueueName: "orders", Payload: []byte("not json")})
	log.updateSub(t, "orders", subPolicy("sub1"))

	// Undecodable records are skipped rather than aborting the checkpoint.
	out := coordinator.CompactRecords(log)

	if n := countOps(out, record.OpEnqueue); n != 0 {
		t.Errorf("got %d ENQUEUE records, want 0", n)
	}
	if n := countOps(out, record.OpUpdateSubPolicy); n != 1 {
		t.Errorf("got %d UPDATE_SUB_POLICY records, want 1 (the decodable one)", n)
	}
	replayRecords(t, out) // must not error
}

func TestCompactOutputOrderingIsReplayable(t *testing.T) {
	var log recordLog
	log.createQueue("orders")
	log.createQueue("events")
	log.updateSub(t, "orders", subPolicy("sub1"))
	log.updateSub(t, "orders", subPolicy("sub2"))
	log.updateSub(t, "events", subPolicy("sub3"))
	log.enqueue(t, "orders", "msg-1", "a", twoSubs())
	log.enqueue(t, "events", "msg-2", "b", twoSubs())
	log.ack(t, "orders", "msg-1", "sub1")

	out := coordinator.CompactRecords(log)

	// Replay rejects a policy before its queue and an ack before its message,
	// so ordering here isn't cosmetic — these indexes must hold.
	firstQueueSeen := map[string]int{}
	firstMsgSeen := map[string]int{}
	for i, rec := range out {
		switch rec.OpType {
		case record.OpCreateQueue:
			if _, ok := firstQueueSeen[rec.QueueName]; !ok {
				firstQueueSeen[rec.QueueName] = i
			}
		case record.OpUpdateSubPolicy:
			created, ok := firstQueueSeen[rec.QueueName]
			if !ok || created > i {
				t.Errorf("UPDATE_SUB_POLICY at index %d precedes CREATE_QUEUE for %q", i, rec.QueueName)
			}
		case record.OpEnqueue:
			enq, err := model.DecodeEnqueue(rec.Payload)
			if err != nil {
				t.Fatalf("DecodeEnqueue returned error: %v", err)
			}
			if _, ok := firstMsgSeen[enq.MsgId]; !ok {
				firstMsgSeen[enq.MsgId] = i
			}
		case record.OpAck:
			ack, err := model.DecodeAck(rec.Payload)
			if err != nil {
				t.Fatalf("DecodeAck returned error: %v", err)
			}
			enqueued, ok := firstMsgSeen[ack.MsgId]
			if !ok || enqueued > i {
				t.Errorf("ACK at index %d precedes the ENQUEUE for message %q", i, ack.MsgId)
			}
		}
	}

	replayRecords(t, out) // the real proof: replay must not error
}

func TestCompactEmptyInput(t *testing.T) {
	out := coordinator.CompactRecords(nil)
	if len(out) != 0 {
		t.Errorf("got %d records from empty input, want 0", len(out))
	}
	replayRecords(t, out)
}

func TestCompactPreservesLiveState(t *testing.T) {
	cases := []struct {
		name  string
		build func(t *testing.T, log *recordLog)
	}{
		{
			name: "queues and policies only",
			build: func(t *testing.T, log *recordLog) {
				log.createQueue("orders")
				log.createQueue("events")
				log.updateSub(t, "orders", subPolicy("sub1"))
				log.updateSub(t, "events", subPolicy("sub2"))
			},
		},
		{
			name: "policy churn",
			build: func(t *testing.T, log *recordLog) {
				log.createQueue("orders")
				log.updateSub(t, "orders", model.SubPolicy{SubName: "sub1", SubURL: "http://v1.com", NumberOfRetries: 1})
				log.updateSub(t, "orders", model.SubPolicy{SubName: "sub1", SubURL: "http://v2.com", NumberOfRetries: 4})
				log.updateSub(t, "orders", subPolicy("temp"))
				log.deleteSub(t, "orders", "temp")
			},
		},
		{
			name: "mixed acked and unacked messages",
			build: func(t *testing.T, log *recordLog) {
				log.createQueue("orders")
				log.updateSub(t, "orders", subPolicy("sub1"))
				log.updateSub(t, "orders", subPolicy("sub2"))
				log.enqueue(t, "orders", "settled", "gone", twoSubs())
				log.ack(t, "orders", "settled", "sub1")
				log.ack(t, "orders", "settled", "sub2")
				log.enqueue(t, "orders", "partial", "half", twoSubs())
				log.ack(t, "orders", "partial", "sub1")
				log.enqueue(t, "orders", "untouched", "fresh", twoSubs())
			},
		},
		{
			name: "deleted queue with in-flight messages",
			build: func(t *testing.T, log *recordLog) {
				log.createQueue("orders")
				log.createQueue("keeper")
				log.updateSub(t, "orders", subPolicy("sub1"))
				log.updateSub(t, "keeper", subPolicy("sub1"))
				log.enqueue(t, "orders", "msg-1", "doomed", twoSubs())
				log.enqueue(t, "keeper", "msg-2", "safe", twoSubs())
				log.deleteQueue("orders")
			},
		},
		{
			name: "queue deleted and recreated",
			build: func(t *testing.T, log *recordLog) {
				log.createQueue("orders")
				log.updateSub(t, "orders", subPolicy("old"))
				log.enqueue(t, "orders", "msg-old", "before", twoSubs())
				log.deleteQueue("orders")
				log.createQueue("orders")
				log.updateSub(t, "orders", subPolicy("new"))
				log.enqueue(t, "orders", "msg-new", "after", twoSubs())
			},
		},
		{
			name: "checkpoint markers interleaved",
			build: func(t *testing.T, log *recordLog) {
				log.createQueue("orders")
				log.updateSub(t, "orders", subPolicy("sub1"))
				log.beginCheckpoint()
				log.enqueue(t, "orders", "msg-1", "after marker", twoSubs())
			},
		},
		{
			name: "repeated create for the same queue",
			build: func(t *testing.T, log *recordLog) {
				log.createQueue("orders")
				log.updateSub(t, "orders", subPolicy("sub1"))
				log.createQueue("orders")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var log recordLog
			tc.build(t, &log)

			fullCat, fullDeli := replayRecords(t, log)
			want := liveState(fullCat, fullDeli)

			compactCat, compactDeli := replayRecords(t, coordinator.CompactRecords(log))
			got := liveState(compactCat, compactDeli)

			if !slices.Equal(got, want) {
				t.Errorf("compacted replay diverges from full replay\n full:     %v\n compacted: %v", want, got)
			}
		})
	}
}

func TestCompactIsIdempotent(t *testing.T) {
	var log recordLog
	log.createQueue("orders")
	log.updateSub(t, "orders", subPolicy("sub1"))
	log.updateSub(t, "orders", subPolicy("sub2"))
	log.enqueue(t, "orders", "msg-1", "in flight", twoSubs())
	log.ack(t, "orders", "msg-1", "sub1")

	once := coordinator.CompactRecords(log)
	twice := coordinator.CompactRecords(once)

	// Compacting an already-compacted stream must be a no-op; a checkpoint
	// derived from a previous checkpoint's records feeds exactly this path.
	cat1, deli1 := replayRecords(t, once)
	cat2, deli2 := replayRecords(t, twice)

	if !slices.Equal(liveState(cat2, deli2), liveState(cat1, deli1)) {
		t.Errorf("re-compacting changed the resulting state\n once:  %v\n twice: %v",
			liveState(cat1, deli1), liveState(cat2, deli2))
	}
	if len(twice) != len(once) {
		t.Errorf("re-compacting changed the record count: %d then %d", len(once), len(twice))
	}
}
