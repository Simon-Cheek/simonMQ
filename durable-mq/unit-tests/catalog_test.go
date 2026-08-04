package unit_tests

import (
	"testing"

	"durable-mq/catalog"
	"durable-mq/model"
	"durable-mq/record"
)

func subPolicyRec(t *testing.T, op record.OpType, queueName string, p model.SubPolicy) record.Record {
	t.Helper()
	payload, err := model.EncodeSubPolicy(p)
	if err != nil {
		t.Fatalf("EncodeSubPolicy returned error: %v", err)
	}
	return record.Record{OpType: op, QueueName: queueName, Payload: payload}
}

func mustProcess(t *testing.T, c *catalog.Catalog, rec record.Record) {
	t.Helper()
	if err := c.ProcessRecord(rec); err != nil {
		t.Fatalf("ProcessRecord(%v) returned error: %v", rec.OpType, err)
	}
}

func TestCatalogCreateAndDeleteQueue(t *testing.T) {
	c := catalog.NewCatalog()

	if c.QueueExists("orders") {
		t.Fatal("queue exists in a brand new catalog")
	}

	mustProcess(t, c, record.Record{OpType: record.OpCreateQueue, QueueName: "orders"})
	if !c.QueueExists("orders") {
		t.Error("queue does not exist after CREATE_QUEUE")
	}

	mustProcess(t, c, record.Record{OpType: record.OpDeleteQueue, QueueName: "orders"})
	if c.QueueExists("orders") {
		t.Error("queue still exists after DELETE_QUEUE")
	}
}

func TestCatalogReturnQueueNames(t *testing.T) {
	c := catalog.NewCatalog()

	if names := c.ReturnQueueNames(); len(names) != 0 {
		t.Fatalf("new catalog returned %d queue names, want 0", len(names))
	}

	for _, name := range []string{"a", "b", "c"} {
		mustProcess(t, c, record.Record{OpType: record.OpCreateQueue, QueueName: name})
	}
	mustProcess(t, c, record.Record{OpType: record.OpDeleteQueue, QueueName: "b"})

	names := c.ReturnQueueNames()
	if len(names) != 2 {
		t.Fatalf("got %d queue names, want 2", len(names))
	}
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	if !got["a"] || !got["c"] || got["b"] {
		t.Errorf("queue names = %v, want exactly a and c", names)
	}
}

func TestCatalogSubPolicyLifecycle(t *testing.T) {
	c := catalog.NewCatalog()
	mustProcess(t, c, record.Record{OpType: record.OpCreateQueue, QueueName: "orders"})

	policy := model.SubPolicy{SubName: "sub1", SubURL: "http://example.com", NumberOfRetries: 3}
	mustProcess(t, c, subPolicyRec(t, record.OpUpdateSubPolicy, "orders", policy))

	subs, ok := c.FetchQueueSubList("orders")
	if !ok {
		t.Fatal("FetchQueueSubList reported the queue as missing")
	}
	if len(subs) != 1 || subs["sub1"] != policy {
		t.Fatalf("sub list = %+v, want exactly %+v", subs, policy)
	}

	// UPDATE on an existing sub name replaces rather than duplicates.
	updated := model.SubPolicy{SubName: "sub1", SubURL: "http://changed.com", NumberOfRetries: 9}
	mustProcess(t, c, subPolicyRec(t, record.OpUpdateSubPolicy, "orders", updated))

	subs, _ = c.FetchQueueSubList("orders")
	if len(subs) != 1 {
		t.Fatalf("got %d subs after update, want 1 (update should replace, not append)", len(subs))
	}
	if subs["sub1"] != updated {
		t.Errorf("sub1 = %+v, want %+v", subs["sub1"], updated)
	}

	// DELETE_SUB_POLICY only needs the name to match.
	mustProcess(t, c, subPolicyRec(t, record.OpDeleteSubPolicy, "orders", model.SubPolicy{SubName: "sub1"}))
	subs, _ = c.FetchQueueSubList("orders")
	if len(subs) != 0 {
		t.Errorf("got %d subs after delete, want 0", len(subs))
	}
}

func TestCatalogCreateQueueResetsExistingSubPolicies(t *testing.T) {
	c := catalog.NewCatalog()
	mustProcess(t, c, record.Record{OpType: record.OpCreateQueue, QueueName: "orders"})
	mustProcess(t, c, subPolicyRec(t, record.OpUpdateSubPolicy, "orders", model.SubPolicy{SubName: "sub1"}))

	// Re-creating a queue under the same name is a deliberate force-refresh:
	// compaction relies on a recreated queue starting with no inherited subs.
	mustProcess(t, c, record.Record{OpType: record.OpCreateQueue, QueueName: "orders"})

	subs, ok := c.FetchQueueSubList("orders")
	if !ok {
		t.Fatal("queue missing after re-creation")
	}
	if len(subs) != 0 {
		t.Errorf("got %d subs after re-creating the queue, want 0", len(subs))
	}
}

func TestCatalogSubPolicyOnMissingQueueErrors(t *testing.T) {
	c := catalog.NewCatalog()

	updateErr := c.ProcessRecord(subPolicyRec(t, record.OpUpdateSubPolicy, "ghost", model.SubPolicy{SubName: "s"}))
	if updateErr == nil {
		t.Error("UPDATE_SUB_POLICY against a nonexistent queue returned nil error")
	}

	deleteErr := c.ProcessRecord(subPolicyRec(t, record.OpDeleteSubPolicy, "ghost", model.SubPolicy{SubName: "s"}))
	if deleteErr == nil {
		t.Error("DELETE_SUB_POLICY against a nonexistent queue returned nil error")
	}
}

func TestCatalogRejectsCorruptSubPolicyPayload(t *testing.T) {
	c := catalog.NewCatalog()
	mustProcess(t, c, record.Record{OpType: record.OpCreateQueue, QueueName: "orders"})

	corrupt := record.Record{
		OpType:    record.OpUpdateSubPolicy,
		QueueName: "orders",
		Payload:   []byte("not valid json"),
	}
	if err := c.ProcessRecord(corrupt); err == nil {
		t.Error("expected an error for an undecodable sub policy payload, got nil")
	}
}

func TestCatalogCheckpointMarkersAreNoOps(t *testing.T) {
	c := catalog.NewCatalog()
	mustProcess(t, c, record.Record{OpType: record.OpCreateQueue, QueueName: "orders"})

	// Checkpoint markers are log structure, not catalog state — replay feeds
	// them through ProcessRecord and they must not error or mutate anything.
	for _, op := range []record.OpType{record.OpBeginCheckpoint, record.OpEndCheckpoint} {
		if err := c.ProcessRecord(record.Record{OpType: op}); err != nil {
			t.Errorf("ProcessRecord(%v) returned error: %v", op, err)
		}
	}

	if names := c.ReturnQueueNames(); len(names) != 1 || names[0] != "orders" {
		t.Errorf("catalog state changed after checkpoint markers: %v", names)
	}
}

func TestCatalogRejectsDeliveryOpTypes(t *testing.T) {
	c := catalog.NewCatalog()

	// ENQUEUE/ACK belong to delivery — the catalog should reject them rather
	// than silently ignore a record it was never meant to handle.
	for _, op := range []record.OpType{record.OpEnqueue, record.OpAck} {
		if err := c.ProcessRecord(record.Record{OpType: op, QueueName: "orders"}); err == nil {
			t.Errorf("ProcessRecord(%v) returned nil error, want an error", op)
		}
	}
}

func TestCatalogFetchQueueSubListMissingQueue(t *testing.T) {
	c := catalog.NewCatalog()
	subs, ok := c.FetchQueueSubList("ghost")
	if ok {
		t.Error("FetchQueueSubList reported ok for a nonexistent queue")
	}
	if subs != nil {
		t.Errorf("sub list = %v, want nil", subs)
	}
}

func TestCatalogFetchQueueSubListReturnsCopy(t *testing.T) {
	c := catalog.NewCatalog()
	mustProcess(t, c, record.Record{OpType: record.OpCreateQueue, QueueName: "orders"})
	mustProcess(t, c, subPolicyRec(t, record.OpUpdateSubPolicy, "orders", model.SubPolicy{SubName: "sub1"}))

	subs, _ := c.FetchQueueSubList("orders")
	subs["injected"] = model.SubPolicy{SubName: "injected"}
	delete(subs, "sub1")

	fresh, _ := c.FetchQueueSubList("orders")
	if _, leaked := fresh["injected"]; leaked {
		t.Error("mutating the returned sub list leaked into the catalog")
	}
	if _, ok := fresh["sub1"]; !ok {
		t.Error("deleting from the returned sub list removed the catalog's own entry")
	}
}

func TestCatalogAllQueueInfo(t *testing.T) {
	c := catalog.NewCatalog()
	mustProcess(t, c, record.Record{OpType: record.OpCreateQueue, QueueName: "orders"})
	mustProcess(t, c, record.Record{OpType: record.OpCreateQueue, QueueName: "events"})

	p1 := model.SubPolicy{SubName: "sub1", SubURL: "http://a.com", NumberOfRetries: 2}
	p2 := model.SubPolicy{SubName: "sub2", SubURL: "http://b.com", NumberOfRetries: 5}
	mustProcess(t, c, subPolicyRec(t, record.OpUpdateSubPolicy, "orders", p1))
	mustProcess(t, c, subPolicyRec(t, record.OpUpdateSubPolicy, "orders", p2))

	all := c.AllQueueInfo()
	if len(all) != 2 {
		t.Fatalf("got %d queues, want 2", len(all))
	}

	byName := map[string]catalog.QueueInfo{}
	for _, qi := range all {
		byName[qi.Name] = qi
	}

	orders, ok := byName["orders"]
	if !ok {
		t.Fatal("orders queue missing from AllQueueInfo")
	}
	if len(orders.SubPolicies) != 2 || orders.SubPolicies["sub1"] != p1 || orders.SubPolicies["sub2"] != p2 {
		t.Errorf("orders sub policies = %+v, want sub1=%+v sub2=%+v", orders.SubPolicies, p1, p2)
	}

	events, ok := byName["events"]
	if !ok {
		t.Fatal("events queue missing from AllQueueInfo")
	}
	if len(events.SubPolicies) != 0 {
		t.Errorf("events sub policies = %+v, want empty", events.SubPolicies)
	}
}

func TestCatalogAllQueueInfoReturnsCopy(t *testing.T) {
	c := catalog.NewCatalog()
	mustProcess(t, c, record.Record{OpType: record.OpCreateQueue, QueueName: "orders"})
	mustProcess(t, c, subPolicyRec(t, record.OpUpdateSubPolicy, "orders", model.SubPolicy{SubName: "sub1"}))

	all := c.AllQueueInfo()
	all[0].SubPolicies["injected"] = model.SubPolicy{SubName: "injected"}

	fresh, _ := c.FetchQueueSubList("orders")
	if _, leaked := fresh["injected"]; leaked {
		t.Error("mutating AllQueueInfo's returned policies leaked into the catalog")
	}
}
