package unit_tests

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"dist-mq/model"
	"dist-mq/node"
	"dist-mq/storage"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer l.Close()
	return fmt.Sprintf("127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port)
}

func startNode(t *testing.T, dir, bind string, store storage.Storage) *node.Node {
	t.Helper()
	n, err := node.New(node.Config{
		ID:        "node-0",
		Dir:       dir,
		BindAddr:  bind,
		Bootstrap: true,
		LogLevel:  "ERROR",
	}, store)
	if err != nil {
		t.Fatalf("node.New returned error: %v", err)
	}
	return n
}

// Leadership lands before the FSM has applied the committed backlog, so the
// barrier is what makes "ready to serve" true rather than just "elected".
func awaitLeadership(t *testing.T, n *node.Node) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if n.IsLeader() {
			if err := n.Barrier(10 * time.Second); err != nil {
				t.Fatalf("Barrier returned error: %v", err)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("node never became leader")
}

// Exercises the full path a write takes: encode, replicate, commit, apply on
// the FSM, and hand the result back through ApplyFuture.Response().
func TestSingleNodeClusterEndToEnd(t *testing.T) {
	store := storage.NewInMemoryStorage()
	n := startNode(t, t.TempDir(), freePort(t), store)
	defer n.Shutdown()
	awaitLeadership(t, n)

	if err := n.CreateQueue("orders"); err != nil {
		t.Fatalf("CreateQueue returned error: %v", err)
	}
	policy := model.SubPolicy{SubName: "billing", SubURL: "http://billing", NumberOfRetries: 3}
	if err := n.PutSubPolicy("orders", policy); err != nil {
		t.Fatalf("PutSubPolicy returned error: %v", err)
	}

	subs, ok := store.FetchSubList("orders")
	if !ok {
		t.Fatal("queue missing from storage after CreateQueue committed")
	}

	msg, err := n.Enqueue("orders", "orders-1", "hello", subs)
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	if msg.MsgID != "orders-1" || msg.Payload != "hello" {
		t.Fatalf("returned message = %+v", msg)
	}
	if _, owed := msg.PendingSubs()["billing"]; !owed {
		t.Fatalf("returned message does not owe billing: %+v", msg)
	}

	if err := n.Ack("orders", "orders-1", []string{"billing"}); err != nil {
		t.Fatalf("Ack returned error: %v", err)
	}
	pending, err := store.PendingMessages("orders")
	if err != nil {
		t.Fatalf("PendingMessages returned error: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("fully acked message still pending: %v", pending)
	}
}

// Storage errors come back as the FSM's return value rather than as the
// future's error, so this pins that they survive the unwrap in propose.
func TestStorageErrorsPropagateThroughApplyFuture(t *testing.T) {
	store := storage.NewInMemoryStorage()
	n := startNode(t, t.TempDir(), freePort(t), store)
	defer n.Shutdown()
	awaitLeadership(t, n)

	if err := n.CreateQueue("orders"); err != nil {
		t.Fatalf("CreateQueue returned error: %v", err)
	}
	if err := n.CreateQueue("orders"); !errors.Is(err, storage.ErrQueueExists) {
		t.Fatalf("duplicate CreateQueue: got %v, want ErrQueueExists", err)
	}
	if _, err := n.Enqueue("nope", "m1", "hi", nil); !errors.Is(err, storage.ErrQueueNotFound) {
		t.Fatalf("Enqueue to unknown queue: got %v, want ErrQueueNotFound", err)
	}
}

// The whole durability claim in one test: state lives only in memory, so a
// restart that recovers it proves recovery came from the raft log on disk.
func TestStateSurvivesRestartViaLogReplay(t *testing.T) {
	dir := t.TempDir()
	bind := freePort(t)

	store := storage.NewInMemoryStorage()
	n := startNode(t, dir, bind, store)
	awaitLeadership(t, n)

	if err := n.CreateQueue("orders"); err != nil {
		t.Fatalf("CreateQueue returned error: %v", err)
	}
	policy := model.SubPolicy{SubName: "billing", SubURL: "http://billing", NumberOfRetries: 3}
	if err := n.PutSubPolicy("orders", policy); err != nil {
		t.Fatalf("PutSubPolicy returned error: %v", err)
	}
	subs, _ := store.FetchSubList("orders")
	for _, id := range []string{"m1", "m2", "m3"} {
		if _, err := n.Enqueue("orders", id, "payload-"+id, subs); err != nil {
			t.Fatalf("Enqueue(%s) returned error: %v", id, err)
		}
	}
	if err := n.Ack("orders", "m2", []string{"billing"}); err != nil {
		t.Fatalf("Ack returned error: %v", err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}

	revived := storage.NewInMemoryStorage()
	n2 := startNode(t, dir, bind, revived)
	defer n2.Shutdown()
	awaitLeadership(t, n2)

	pending, err := revived.PendingMessages("orders")
	if err != nil {
		t.Fatalf("PendingMessages after restart returned error: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("after restart got %d pending messages, want 2 (m2 was acked): %+v", len(pending), pending)
	}
	if pending[0].MsgID != "m1" || pending[1].MsgID != "m3" {
		t.Fatalf("after restart pending = %s, %s; want m1, m3", pending[0].MsgID, pending[1].MsgID)
	}
	if pending[0].Payload != "payload-m1" {
		t.Fatalf("payload lost across restart: %q", pending[0].Payload)
	}

	restored, ok := revived.FetchSubList("orders")
	if !ok {
		t.Fatal("queue lost across restart")
	}
	if restored["billing"] != policy {
		t.Fatalf("sub policy lost across restart: %+v", restored)
	}
}

func TestLeaderIDResolvesOnceElected(t *testing.T) {
	store := storage.NewInMemoryStorage()
	n := startNode(t, t.TempDir(), freePort(t), store)
	defer n.Shutdown()
	awaitLeadership(t, n)

	id, ok := n.LeaderID()
	if !ok {
		t.Fatal("LeaderID reported no leader after election")
	}
	if id != "node-0" {
		t.Fatalf("leader id = %q, want %q", id, "node-0")
	}
}
