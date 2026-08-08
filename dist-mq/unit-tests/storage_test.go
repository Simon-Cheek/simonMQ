package unit_tests

import (
	"errors"
	"testing"

	"dist-mq/model"
	"dist-mq/storage"
)

func newStore(t *testing.T, queues ...string) *storage.InMemoryStorage {
	t.Helper()
	s := storage.NewInMemoryStorage()
	for _, q := range queues {
		if err := s.CreateQueue(q); err != nil {
			t.Fatalf("CreateQueue(%q) returned error: %v", q, err)
		}
	}
	return s
}

func addSub(t *testing.T, s *storage.InMemoryStorage, queueName, subName string) {
	t.Helper()
	policy := model.SubPolicy{SubName: subName, SubURL: "http://" + subName, NumberOfRetries: 3}
	if err := s.PutSubPolicy(queueName, policy); err != nil {
		t.Fatalf("PutSubPolicy(%q, %q) returned error: %v", queueName, subName, err)
	}
}

// Mirrors the real call path: the leader reads the sub list, then that list
// travels in the command and arrives back here as a parameter.
func mustEnqueue(t *testing.T, s *storage.InMemoryStorage, queueName, msgID, payload string) model.MessageInfo {
	t.Helper()
	subs, _ := s.FetchSubList(queueName)
	msg, err := s.Enqueue(queueName, msgID, payload, subs)
	if err != nil {
		t.Fatalf("Enqueue(%q, %q) returned error: %v", queueName, msgID, err)
	}
	return msg
}

func pendingIDs(t *testing.T, s *storage.InMemoryStorage, queueName string) []string {
	t.Helper()
	msgs, err := s.PendingMessages(queueName)
	if err != nil {
		t.Fatalf("PendingMessages(%q) returned error: %v", queueName, err)
	}
	ids := make([]string, 0, len(msgs))
	for _, m := range msgs {
		ids = append(ids, m.MsgID)
	}
	return ids
}

func TestCreateQueueRejectsDuplicate(t *testing.T) {
	s := newStore(t, "orders")

	// Deterministic across nodes and implies no mutation, so it is safe to
	// surface from Apply and lets the leader answer 400 without a pre-check.
	if err := s.CreateQueue("orders"); !errors.Is(err, storage.ErrQueueExists) {
		t.Fatalf("duplicate CreateQueue: got %v, want ErrQueueExists", err)
	}
}

func TestDuplicateCreateLeavesSubscribersIntact(t *testing.T) {
	s := newStore(t, "orders")
	addSub(t, s, "orders", "billing")

	_ = s.CreateQueue("orders")

	subs, ok := s.FetchSubList("orders")
	if !ok {
		t.Fatal("FetchSubList: queue missing after duplicate create")
	}
	if _, ok := subs["billing"]; !ok {
		t.Fatal("duplicate CreateQueue wiped the subscriber list")
	}
}

func TestDeleteQueueRemovesMessagesAndSubs(t *testing.T) {
	s := newStore(t, "orders")
	addSub(t, s, "orders", "billing")
	mustEnqueue(t, s, "orders", "m1", "hello")

	if err := s.DeleteQueue("orders"); err != nil {
		t.Fatalf("DeleteQueue returned error: %v", err)
	}
	if _, ok := s.FetchSubList("orders"); ok {
		t.Fatal("FetchSubList still resolves after DeleteQueue")
	}
	if _, err := s.PendingMessages("orders"); !errors.Is(err, storage.ErrQueueNotFound) {
		t.Fatalf("PendingMessages after delete: got %v, want ErrQueueNotFound", err)
	}
}

func TestEnqueueUnknownQueue(t *testing.T) {
	s := newStore(t)
	if _, err := s.Enqueue("nope", "m1", "hello", nil); !errors.Is(err, storage.ErrQueueNotFound) {
		t.Fatalf("Enqueue to unknown queue: got %v, want ErrQueueNotFound", err)
	}
}

// The list in the command wins over whatever this node currently holds, so a
// node whose sub state has drifted still stores the same subscribers as
// everyone else.
func TestEnqueueUsesSuppliedSubListNotLocalState(t *testing.T) {
	s := newStore(t, "orders")
	addSub(t, s, "orders", "local-only")

	supplied := map[string]model.SubPolicy{
		"billing": {SubName: "billing", SubURL: "http://billing", NumberOfRetries: 3},
	}
	msg, err := s.Enqueue("orders", "m1", "hello", supplied)
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	if _, ok := msg.SubList["local-only"]; ok {
		t.Fatal("local sub state leaked into the message")
	}
	if _, ok := msg.SubList["billing"]; !ok {
		t.Fatalf("supplied sub list was not used: %v", msg.SubList)
	}

	// Mutating the caller's map must not reach into stored state.
	supplied["injected"] = model.SubPolicy{SubName: "injected"}
	stored, err := s.PendingMessages("orders")
	if err != nil {
		t.Fatalf("PendingMessages returned error: %v", err)
	}
	if _, ok := stored[0].SubList["injected"]; ok {
		t.Fatal("stored message aliased the caller's sub list")
	}
}

func TestEnqueueSnapshotsSubscribersAtEnqueueTime(t *testing.T) {
	s := newStore(t, "orders")
	addSub(t, s, "orders", "billing")
	mustEnqueue(t, s, "orders", "m1", "hello")

	// Registering later must not retroactively owe delivery of an older message.
	addSub(t, s, "orders", "audit")

	msgs, err := s.PendingMessages("orders")
	if err != nil {
		t.Fatalf("PendingMessages returned error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 pending message, got %d", len(msgs))
	}
	if _, ok := msgs[0].SubList["audit"]; ok {
		t.Fatal("subscriber registered after enqueue leaked into the message snapshot")
	}
}

func TestEnqueueWithNoSubscribersIsNotStored(t *testing.T) {
	s := newStore(t, "orders")
	mustEnqueue(t, s, "orders", "m1", "hello")

	if ids := pendingIDs(t, s, "orders"); len(ids) != 0 {
		t.Fatalf("message with no subscribers was stored: %v", ids)
	}
}

// Replay is the raft-specific hazard: after a snapshot restore, committed
// entries get reapplied against state that may already reflect them.
func TestEnqueueReplayDoesNotResurrectAcks(t *testing.T) {
	s := newStore(t, "orders")
	addSub(t, s, "orders", "billing")
	addSub(t, s, "orders", "audit")
	mustEnqueue(t, s, "orders", "m1", "hello")

	if err := s.Ack("orders", "m1", []string{"billing"}); err != nil {
		t.Fatalf("Ack returned error: %v", err)
	}

	// Same entry applied a second time.
	replayed := mustEnqueue(t, s, "orders", "m1", "hello")

	if len(replayed.AckedSubs) != 1 || replayed.AckedSubs[0] != "billing" {
		t.Fatalf("replayed enqueue lost the ack: AckedSubs=%v", replayed.AckedSubs)
	}
	if pending := replayed.PendingSubs(); len(pending) != 1 {
		t.Fatalf("expected 1 pending sub after replay, got %d", len(pending))
	}
}

func TestAckIsIdempotentAndDropsCompletedMessages(t *testing.T) {
	s := newStore(t, "orders")
	addSub(t, s, "orders", "billing")
	addSub(t, s, "orders", "audit")
	mustEnqueue(t, s, "orders", "m1", "hello")

	if err := s.Ack("orders", "m1", []string{"billing", "billing"}); err != nil {
		t.Fatalf("Ack returned error: %v", err)
	}
	if ids := pendingIDs(t, s, "orders"); len(ids) != 1 {
		t.Fatalf("message dropped while a subscriber was still owed: %v", ids)
	}

	if err := s.Ack("orders", "m1", []string{"audit"}); err != nil {
		t.Fatalf("Ack returned error: %v", err)
	}
	if ids := pendingIDs(t, s, "orders"); len(ids) != 0 {
		t.Fatalf("fully acked message was retained: %v", ids)
	}

	// Replayed ack against a message already dropped — normal, not an error.
	if err := s.Ack("orders", "m1", []string{"audit"}); err != nil {
		t.Fatalf("Ack on dropped message returned error: %v", err)
	}
	if err := s.Ack("gone", "m1", []string{"audit"}); err != nil {
		t.Fatalf("Ack on missing queue returned error: %v", err)
	}
}

func TestAckIgnoresSubscribersOutsideSnapshot(t *testing.T) {
	s := newStore(t, "orders")
	addSub(t, s, "orders", "billing")
	mustEnqueue(t, s, "orders", "m1", "hello")

	if err := s.Ack("orders", "m1", []string{"stranger"}); err != nil {
		t.Fatalf("Ack returned error: %v", err)
	}
	// Must not count toward completion, or the message vanishes unacked.
	if ids := pendingIDs(t, s, "orders"); len(ids) != 1 {
		t.Fatalf("ack from an unlisted subscriber completed the message: %v", ids)
	}
}

// FIFO has to survive a leadership change, and map iteration alone would not
// give it — this is what the sequence counter buys.
func TestPendingMessagesAreOldestFirst(t *testing.T) {
	s := newStore(t, "orders")
	addSub(t, s, "orders", "billing")
	for _, id := range []string{"m1", "m2", "m3", "m4", "m5"} {
		mustEnqueue(t, s, "orders", id, id)
	}

	got := pendingIDs(t, s, "orders")
	want := []string{"m1", "m2", "m3", "m4", "m5"}
	if len(got) != len(want) {
		t.Fatalf("got %d pending messages, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pending order = %v, want %v", got, want)
		}
	}
}

// Reads hand out state that outlives the lock — the delivery layer holds a
// MessageInfo across an HTTP round trip while Apply keeps mutating.
func TestReadsReturnCopies(t *testing.T) {
	s := newStore(t, "orders")
	addSub(t, s, "orders", "billing")
	mustEnqueue(t, s, "orders", "m1", "hello")

	subs, _ := s.FetchSubList("orders")
	delete(subs, "billing")
	subs["injected"] = model.SubPolicy{SubName: "injected"}

	msgs, err := s.PendingMessages("orders")
	if err != nil {
		t.Fatalf("PendingMessages returned error: %v", err)
	}
	msgs[0].SubList["injected"] = model.SubPolicy{SubName: "injected"}

	fresh, _ := s.FetchSubList("orders")
	if _, ok := fresh["billing"]; !ok {
		t.Fatal("mutating a returned sub list deleted from the store")
	}
	if _, ok := fresh["injected"]; ok {
		t.Fatal("mutating a returned sub list wrote into the store")
	}

	freshMsgs, _ := s.PendingMessages("orders")
	if _, ok := freshMsgs[0].SubList["injected"]; ok {
		t.Fatal("mutating a returned message wrote into the store")
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	s := newStore(t, "orders", "events")
	addSub(t, s, "orders", "billing")
	addSub(t, s, "orders", "audit")
	addSub(t, s, "events", "analytics")
	mustEnqueue(t, s, "orders", "m1", "one")
	mustEnqueue(t, s, "orders", "m2", "two")
	mustEnqueue(t, s, "events", "e1", "evt")
	if err := s.Ack("orders", "m1", []string{"billing"}); err != nil {
		t.Fatalf("Ack returned error: %v", err)
	}

	data, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot returned error: %v", err)
	}

	restored := storage.NewInMemoryStorage()
	if err := restored.Restore(data); err != nil {
		t.Fatalf("Restore returned error: %v", err)
	}

	msgs, err := restored.PendingMessages("orders")
	if err != nil {
		t.Fatalf("PendingMessages returned error: %v", err)
	}
	if len(msgs) != 2 || msgs[0].MsgID != "m1" || msgs[1].MsgID != "m2" {
		t.Fatalf("restored messages wrong or out of order: %+v", msgs)
	}
	if pending := msgs[0].PendingSubs(); len(pending) != 1 {
		t.Fatalf("restored m1 should owe exactly audit, got %v", pending)
	}
	if _, ok := msgs[0].PendingSubs()["audit"]; !ok {
		t.Fatal("restored m1 lost its outstanding subscriber")
	}

	// nextSeq must survive, or post-restore enqueues sort before existing ones.
	mustEnqueue(t, restored, "orders", "m3", "three")
	got := pendingIDs(t, restored, "orders")
	want := []string{"m1", "m2", "m3"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order after restore = %v, want %v", got, want)
		}
	}
}

// Restore replaces, it does not merge. Absence in the payload means deleted —
// a follower catching up via InstallSnapshot must not keep work the snapshot
// has already completed.
func TestRestoreReplacesRatherThanMerges(t *testing.T) {
	source := newStore(t, "orders")
	addSub(t, source, "orders", "billing")
	data, err := source.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot returned error: %v", err)
	}

	stale := newStore(t, "orders", "ghost")
	addSub(t, stale, "orders", "billing")
	addSub(t, stale, "ghost", "listener")
	mustEnqueue(t, stale, "orders", "old", "stale message")
	mustEnqueue(t, stale, "ghost", "g1", "stale message")

	if err := stale.Restore(data); err != nil {
		t.Fatalf("Restore returned error: %v", err)
	}

	if ids := pendingIDs(t, stale, "orders"); len(ids) != 0 {
		t.Fatalf("message absent from the snapshot survived restore: %v", ids)
	}
	if _, ok := stale.FetchSubList("ghost"); ok {
		t.Fatal("queue absent from the snapshot survived restore")
	}
}

func TestRestoreRejectsMalformedPayloadWithoutLosingState(t *testing.T) {
	s := newStore(t, "orders")
	addSub(t, s, "orders", "billing")
	mustEnqueue(t, s, "orders", "m1", "hello")

	if err := s.Restore([]byte("{not json")); err == nil {
		t.Fatal("Restore accepted a malformed payload")
	}
	if ids := pendingIDs(t, s, "orders"); len(ids) != 1 {
		t.Fatalf("failed Restore clobbered existing state: %v", ids)
	}
}
