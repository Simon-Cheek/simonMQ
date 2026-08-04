package unit_tests

import (
	"testing"

	"durable-mq/coordinator"
	"durable-mq/delivery"
	"durable-mq/model"
)

// newTestCoordinator points NewCoordinator's hardcoded relative WAL directory
// at a fresh temp dir for this test. t.Chdir changes the process working
// directory, so these tests must not call t.Parallel.
func newTestCoordinator(t *testing.T) *coordinator.Coordinator {
	t.Helper()
	t.Chdir(t.TempDir())
	return reopenCoordinator(t)
}

// reopenCoordinator builds a second Coordinator over the WAL directory the
// current test is already isolated onto — the stand-in for a process restart.
func reopenCoordinator(t *testing.T) *coordinator.Coordinator {
	t.Helper()
	c, err := coordinator.NewCoordinator()
	if err != nil {
		t.Fatalf("NewCoordinator returned error: %v", err)
	}
	return c
}

func subPolicy(name string) model.SubPolicy {
	return model.SubPolicy{SubName: name, SubURL: "http://" + name + ".example.com", NumberOfRetries: 3}
}

func msgInfo(t *testing.T, data map[string]*delivery.DeliveryQueueInfo, queueName, msgId string) *delivery.DeliveryMessageInfo {
	t.Helper()
	qInfo, ok := data[queueName]
	if !ok {
		t.Fatalf("queue %q missing from replayed delivery data", queueName)
	}
	msg, ok := qInfo.Messages[msgId]
	if !ok {
		t.Fatalf("message %q missing from replayed delivery data", msgId)
	}
	return msg
}

func TestCoordinatorCreateQueueAndAllQueueInfo(t *testing.T) {
	c := newTestCoordinator(t)

	if err := c.CreateQueue("orders"); err != nil {
		t.Fatalf("CreateQueue returned error: %v", err)
	}

	all := c.AllQueueInfo()
	if len(all) != 1 || all[0].Name != "orders" {
		t.Fatalf("AllQueueInfo = %+v, want a single queue named orders", all)
	}
}

func TestCoordinatorRejectsOperationsOnMissingQueue(t *testing.T) {
	c := newTestCoordinator(t)

	if err := c.DeleteQueue("ghost"); err == nil {
		t.Error("DeleteQueue on a nonexistent queue returned nil error")
	}
	if err := c.UpdateSubPolicy("ghost", subPolicy("sub1")); err == nil {
		t.Error("UpdateSubPolicy on a nonexistent queue returned nil error")
	}
	if err := c.DeleteSubPolicy("ghost", "sub1"); err == nil {
		t.Error("DeleteSubPolicy on a nonexistent queue returned nil error")
	}
	if _, err := c.Enqueue("ghost", "msg-1", "hello"); err == nil {
		t.Error("Enqueue on a nonexistent queue returned nil error")
	}
}

func TestCoordinatorEnqueueReturnsCurrentSubList(t *testing.T) {
	c := newTestCoordinator(t)

	if err := c.CreateQueue("orders"); err != nil {
		t.Fatalf("CreateQueue returned error: %v", err)
	}

	subs, err := c.Enqueue("orders", "msg-0", "before any subscribers")
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	if len(subs) != 0 {
		t.Errorf("got %d subscribers on a queue with none registered, want 0", len(subs))
	}

	if err := c.UpdateSubPolicy("orders", subPolicy("sub1")); err != nil {
		t.Fatalf("UpdateSubPolicy returned error: %v", err)
	}
	if err := c.UpdateSubPolicy("orders", subPolicy("sub2")); err != nil {
		t.Fatalf("UpdateSubPolicy returned error: %v", err)
	}

	subs, err = c.Enqueue("orders", "msg-1", "hello")
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	if len(subs) != 2 || subs["sub1"] != subPolicy("sub1") || subs["sub2"] != subPolicy("sub2") {
		t.Errorf("Enqueue returned sub list %+v, want sub1 and sub2", subs)
	}
}

func TestCoordinatorReplayEmptyLog(t *testing.T) {
	c := newTestCoordinator(t)

	queues, deliveryData, err := c.ReplayLog()
	if err != nil {
		t.Fatalf("ReplayLog returned error: %v", err)
	}
	if len(queues) != 0 {
		t.Errorf("got %d queues from an empty log, want 0", len(queues))
	}
	if len(deliveryData) != 0 {
		t.Errorf("got %d queues of delivery data from an empty log, want 0", len(deliveryData))
	}
}

func TestCoordinatorReplayRestoresQueuesAndSubPolicies(t *testing.T) {
	c := newTestCoordinator(t)

	if err := c.CreateQueue("orders"); err != nil {
		t.Fatalf("CreateQueue returned error: %v", err)
	}
	if err := c.CreateQueue("events"); err != nil {
		t.Fatalf("CreateQueue returned error: %v", err)
	}
	if err := c.UpdateSubPolicy("orders", subPolicy("sub1")); err != nil {
		t.Fatalf("UpdateSubPolicy returned error: %v", err)
	}
	if err := c.CreateQueue("temporary"); err != nil {
		t.Fatalf("CreateQueue returned error: %v", err)
	}
	if err := c.DeleteQueue("temporary"); err != nil {
		t.Fatalf("DeleteQueue returned error: %v", err)
	}

	restarted := reopenCoordinator(t)
	queues, _, err := restarted.ReplayLog()
	if err != nil {
		t.Fatalf("ReplayLog returned error: %v", err)
	}

	got := map[string]bool{}
	for _, q := range queues {
		got[q] = true
	}
	if !got["orders"] || !got["events"] {
		t.Errorf("replayed queues = %v, want orders and events", queues)
	}
	if got["temporary"] {
		t.Error("a deleted queue came back after replay")
	}

	all := restarted.AllQueueInfo()
	var orders *model.SubPolicy
	for _, qi := range all {
		if qi.Name == "orders" {
			if p, ok := qi.SubPolicies["sub1"]; ok {
				orders = &p
			}
		}
	}
	if orders == nil {
		t.Fatal("sub1 policy missing from the orders queue after replay")
	}
	if *orders != subPolicy("sub1") {
		t.Errorf("replayed policy = %+v, want %+v", *orders, subPolicy("sub1"))
	}
}

func TestCoordinatorReplayRestoresPartialAckState(t *testing.T) {
	c := newTestCoordinator(t)

	if err := c.CreateQueue("orders"); err != nil {
		t.Fatalf("CreateQueue returned error: %v", err)
	}
	if err := c.UpdateSubPolicy("orders", subPolicy("sub1")); err != nil {
		t.Fatalf("UpdateSubPolicy returned error: %v", err)
	}
	if err := c.UpdateSubPolicy("orders", subPolicy("sub2")); err != nil {
		t.Fatalf("UpdateSubPolicy returned error: %v", err)
	}
	if _, err := c.Enqueue("orders", "msg-1", "hello"); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	if err := c.Ack("orders", "msg-1", "sub1"); err != nil {
		t.Fatalf("Ack returned error: %v", err)
	}

	restarted := reopenCoordinator(t)
	_, deliveryData, err := restarted.ReplayLog()
	if err != nil {
		t.Fatalf("ReplayLog returned error: %v", err)
	}

	msg := msgInfo(t, deliveryData, "orders", "msg-1")
	if msg.Content != "hello" {
		t.Errorf("content = %q, want %q", msg.Content, "hello")
	}
	if len(msg.SubList) != 2 {
		t.Errorf("sub list has %d entries, want 2", len(msg.SubList))
	}
	if _, acked := msg.AckedSubs["sub1"]; !acked {
		t.Error("sub1 acked before the restart but is not acked after replay")
	}
	if _, acked := msg.AckedSubs["sub2"]; acked {
		t.Error("sub2 never acked but shows as acked after replay")
	}
}

func TestCoordinatorReplayUsesEnqueueTimeSubListSnapshot(t *testing.T) {
	c := newTestCoordinator(t)

	if err := c.CreateQueue("orders"); err != nil {
		t.Fatalf("CreateQueue returned error: %v", err)
	}
	if err := c.UpdateSubPolicy("orders", subPolicy("sub1")); err != nil {
		t.Fatalf("UpdateSubPolicy returned error: %v", err)
	}
	if err := c.UpdateSubPolicy("orders", subPolicy("sub2")); err != nil {
		t.Fatalf("UpdateSubPolicy returned error: %v", err)
	}
	if _, err := c.Enqueue("orders", "msg-1", "hello"); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	// Policy churn after the enqueue must not retroactively rewrite who that
	// message was addressed to — the snapshot rides on the ENQUEUE record.
	if err := c.DeleteSubPolicy("orders", "sub2"); err != nil {
		t.Fatalf("DeleteSubPolicy returned error: %v", err)
	}
	if err := c.UpdateSubPolicy("orders", subPolicy("sub3")); err != nil {
		t.Fatalf("UpdateSubPolicy returned error: %v", err)
	}

	restarted := reopenCoordinator(t)
	_, deliveryData, err := restarted.ReplayLog()
	if err != nil {
		t.Fatalf("ReplayLog returned error: %v", err)
	}

	msg := msgInfo(t, deliveryData, "orders", "msg-1")
	if len(msg.SubList) != 2 {
		t.Fatalf("sub list has %d entries, want the 2 present at enqueue time: %+v", len(msg.SubList), msg.SubList)
	}
	if _, ok := msg.SubList["sub2"]; !ok {
		t.Error("sub2 was removed from the queue after the enqueue but must remain in that message's snapshot")
	}
	if _, ok := msg.SubList["sub3"]; ok {
		t.Error("sub3 was added after the enqueue and must not appear in that message's snapshot")
	}

	// The catalog, unlike the message snapshot, does reflect the churn.
	for _, qi := range restarted.AllQueueInfo() {
		if qi.Name != "orders" {
			continue
		}
		if _, ok := qi.SubPolicies["sub2"]; ok {
			t.Error("sub2 was deleted but still appears in the catalog")
		}
		if _, ok := qi.SubPolicies["sub3"]; !ok {
			t.Error("sub3 was added but is missing from the catalog")
		}
	}
}

func TestCoordinatorReplayDropsDeletedQueueMessages(t *testing.T) {
	c := newTestCoordinator(t)

	if err := c.CreateQueue("orders"); err != nil {
		t.Fatalf("CreateQueue returned error: %v", err)
	}
	if err := c.UpdateSubPolicy("orders", subPolicy("sub1")); err != nil {
		t.Fatalf("UpdateSubPolicy returned error: %v", err)
	}
	if _, err := c.Enqueue("orders", "msg-1", "hello"); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	if err := c.DeleteQueue("orders"); err != nil {
		t.Fatalf("DeleteQueue returned error: %v", err)
	}

	restarted := reopenCoordinator(t)
	queues, deliveryData, err := restarted.ReplayLog()
	if err != nil {
		t.Fatalf("ReplayLog returned error: %v", err)
	}

	if len(queues) != 0 {
		t.Errorf("replayed queues = %v, want none", queues)
	}
	if _, ok := deliveryData["orders"]; ok {
		t.Error("messages for a deleted queue survived replay")
	}
}

func TestCoordinatorReplayHandlesRecreatedQueue(t *testing.T) {
	c := newTestCoordinator(t)

	if err := c.CreateQueue("orders"); err != nil {
		t.Fatalf("CreateQueue returned error: %v", err)
	}
	if err := c.UpdateSubPolicy("orders", subPolicy("old-sub")); err != nil {
		t.Fatalf("UpdateSubPolicy returned error: %v", err)
	}
	if err := c.DeleteQueue("orders"); err != nil {
		t.Fatalf("DeleteQueue returned error: %v", err)
	}
	if err := c.CreateQueue("orders"); err != nil {
		t.Fatalf("re-CreateQueue returned error: %v", err)
	}
	if err := c.UpdateSubPolicy("orders", subPolicy("new-sub")); err != nil {
		t.Fatalf("UpdateSubPolicy returned error: %v", err)
	}

	restarted := reopenCoordinator(t)
	if _, _, err := restarted.ReplayLog(); err != nil {
		t.Fatalf("ReplayLog returned error: %v", err)
	}

	for _, qi := range restarted.AllQueueInfo() {
		if qi.Name != "orders" {
			continue
		}
		if _, ok := qi.SubPolicies["old-sub"]; ok {
			t.Error("a subscriber from before the queue was deleted survived the re-creation")
		}
		if _, ok := qi.SubPolicies["new-sub"]; !ok {
			t.Error("the subscriber added after re-creation is missing")
		}
	}
}

func TestCoordinatorReplayPreservesMultipleMessages(t *testing.T) {
	c := newTestCoordinator(t)

	if err := c.CreateQueue("orders"); err != nil {
		t.Fatalf("CreateQueue returned error: %v", err)
	}
	if err := c.UpdateSubPolicy("orders", subPolicy("sub1")); err != nil {
		t.Fatalf("UpdateSubPolicy returned error: %v", err)
	}

	const n = 25
	for i := 0; i < n; i++ {
		msgId := "msg-" + string(rune('A'+i%26)) + string(rune('0'+i/26))
		if _, err := c.Enqueue("orders", msgId, "payload"); err != nil {
			t.Fatalf("Enqueue %d returned error: %v", i, err)
		}
	}

	restarted := reopenCoordinator(t)
	_, deliveryData, err := restarted.ReplayLog()
	if err != nil {
		t.Fatalf("ReplayLog returned error: %v", err)
	}

	qInfo, ok := deliveryData["orders"]
	if !ok {
		t.Fatal("orders queue missing after replay")
	}
	if len(qInfo.Messages) != n {
		t.Errorf("got %d messages after replay, want %d", len(qInfo.Messages), n)
	}
}
