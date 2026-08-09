package unit_tests

import (
	"errors"
	"sync"
	"testing"
	"time"

	"dist-mq/delivery"
	"dist-mq/model"
	"dist-mq/storage"
)

// fakeCluster stands in for node.Node so the whole promotion lifecycle runs
// without a raft cluster, ports, or elections.
type fakeCluster struct {
	leaderCh chan bool

	mu         sync.Mutex
	leader     bool
	barrierErr error
	ackErr     error
	acks       [][]string
	barriers   int
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{leaderCh: make(chan bool, 1)}
}

func (f *fakeCluster) LeaderCh() <-chan bool { return f.leaderCh }

func (f *fakeCluster) IsLeader() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leader
}

func (f *fakeCluster) Barrier(time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.barriers++
	return f.barrierErr
}

func (f *fakeCluster) Ack(_, _ string, subNames []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ackErr != nil {
		return f.ackErr
	}
	names := make([]string, len(subNames))
	copy(names, subNames)
	f.acks = append(f.acks, names)
	return nil
}

func (f *fakeCluster) elect(isLeader bool) {
	f.mu.Lock()
	f.leader = isLeader
	f.mu.Unlock()
	f.leaderCh <- isLeader
}

func (f *fakeCluster) ackCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.acks)
}

func (f *fakeCluster) barrierCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.barriers
}

func (f *fakeCluster) setBarrierErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.barrierErr = err
}

// storeWithPending builds committed state holding one pending message per id.
func storeWithPending(t *testing.T, queueName string, sub model.SubPolicy, msgIDs ...string) *storage.InMemoryStorage {
	t.Helper()
	s := storage.NewInMemoryStorage()
	if err := s.CreateQueue(queueName); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if err := s.PutSubPolicy(queueName, sub); err != nil {
		t.Fatalf("PutSubPolicy: %v", err)
	}
	subs, _ := s.FetchSubList(queueName)
	for _, id := range msgIDs {
		if _, err := s.Enqueue(queueName, id, "payload-"+id, subs); err != nil {
			t.Fatalf("Enqueue(%s): %v", id, err)
		}
	}
	return s
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Promotion has to pick up work the previous leader committed — this node never
// saw those enqueues, so nothing but the sweep can find them.
func TestPromotionSweepsInheritedWork(t *testing.T) {
	billing := newSubscriber(t)
	store := storeWithPending(t, "orders", billing.policy("billing", 3), "m1", "m2", "m3")
	cluster := newFakeCluster()

	m := delivery.NewManager(cluster, store, time.Hour) // reconcile off; test the promotion sweep
	go m.Run()
	defer m.Stop()

	cluster.elect(true)

	eventually(t, "inherited messages delivered", func() bool { return billing.calls() == 3 })
	eventually(t, "acks proposed", func() bool { return cluster.ackCount() == 3 })
}

// Nothing may be delivered before leadership arrives.
func TestFollowerDeliversNothing(t *testing.T) {
	billing := newSubscriber(t)
	store := storeWithPending(t, "orders", billing.policy("billing", 3), "m1")
	cluster := newFakeCluster()

	m := delivery.NewManager(cluster, store, 10*time.Millisecond)
	go m.Run()
	defer m.Stop()

	time.Sleep(100 * time.Millisecond)
	if billing.calls() != 0 {
		t.Fatalf("follower delivered %d messages", billing.calls())
	}
}

func TestScheduleIsDroppedWhileNotLeading(t *testing.T) {
	billing := newSubscriber(t)
	store := storeWithPending(t, "orders", billing.policy("billing", 3))
	cluster := newFakeCluster()

	m := delivery.NewManager(cluster, store, time.Hour)
	go m.Run()
	defer m.Stop()

	m.Schedule("orders", model.MessageInfo{
		MsgID:   "m1",
		Payload: "hello",
		SubList: map[string]model.SubPolicy{"billing": billing.policy("billing", 3)},
	})

	time.Sleep(100 * time.Millisecond)
	if billing.calls() != 0 {
		t.Fatalf("delivered %d messages while not leading", billing.calls())
	}
}

// The post-commit feed and the reconcile sweep both present messages that are
// still in flight — the ack has not committed, so storage still reports them
// pending. Only one delivery may result.
func TestPostCommitAndSweepDoNotDoubleDeliver(t *testing.T) {
	billing := newSubscriber(t)
	sub := billing.policy("billing", 3)
	store := storeWithPending(t, "orders", sub, "m1")
	cluster := newFakeCluster()

	// Hold the delivery open so the message stays in flight for the whole test,
	// which is exactly the window where storage still calls it pending.
	billing.holdRequests()

	m := delivery.NewManager(cluster, store, 10*time.Millisecond)
	go m.Run()
	defer m.Stop()

	cluster.elect(true)
	eventually(t, "first delivery to start", func() bool { return billing.calls() >= 1 })

	// Both feeds hammer the same in-flight message: the sweep on its ticker,
	// and the post-commit path directly.
	info := model.MessageInfo{MsgID: "m1", Payload: "payload-m1", SubList: map[string]model.SubPolicy{"billing": sub}}
	for range 20 {
		m.Schedule("orders", info)
	}
	time.Sleep(200 * time.Millisecond)

	if billing.calls() != 1 {
		t.Fatalf("delivered %d times while in flight, want exactly 1", billing.calls())
	}

	billing.release()
	eventually(t, "ack after release", func() bool { return cluster.ackCount() == 1 })
}

// Demotion must stop delivery, and a message committed afterwards must not be
// picked up until the node leads again.
func TestDemotionStopsDelivery(t *testing.T) {
	billing := newSubscriber(t)
	sub := billing.policy("billing", 3)
	store := storeWithPending(t, "orders", sub, "m1")
	cluster := newFakeCluster()

	m := delivery.NewManager(cluster, store, time.Hour)
	go m.Run()
	defer m.Stop()

	cluster.elect(true)
	eventually(t, "initial delivery", func() bool { return billing.calls() == 1 })

	cluster.elect(false)
	time.Sleep(50 * time.Millisecond)
	before := billing.calls()

	m.Schedule("orders", model.MessageInfo{
		MsgID:   "m2",
		Payload: "after demotion",
		SubList: map[string]model.SubPolicy{"billing": sub},
	})

	time.Sleep(150 * time.Millisecond)
	if billing.calls() != before {
		t.Fatalf("delivered %d messages after demotion", billing.calls()-before)
	}
}

// LeaderCh drops signals, so true can arrive when already leading. That must
// not leave two workers per queue delivering the same messages.
func TestRepeatedPromotionDoesNotDoubleDeliver(t *testing.T) {
	billing := newSubscriber(t)
	store := storeWithPending(t, "orders", billing.policy("billing", 3), "m1", "m2")
	cluster := newFakeCluster()

	m := delivery.NewManager(cluster, store, time.Hour)
	go m.Run()
	defer m.Stop()

	for range 3 {
		cluster.elect(true)
	}

	eventually(t, "messages delivered", func() bool { return billing.calls() >= 2 })
	time.Sleep(300 * time.Millisecond)

	if billing.calls() != 2 {
		t.Fatalf("delivered %d times across repeated promotions, want 2", billing.calls())
	}
}

// A message that committed but never reached Schedule is invisible until the
// reconcile sweep finds it in storage.
func TestReconcileSweepPicksUpOrphanedWork(t *testing.T) {
	billing := newSubscriber(t)
	sub := billing.policy("billing", 3)
	store := storeWithPending(t, "orders", sub)
	cluster := newFakeCluster()

	m := delivery.NewManager(cluster, store, 25*time.Millisecond)
	go m.Run()
	defer m.Stop()

	cluster.elect(true)
	eventually(t, "promotion", func() bool { return cluster.barrierCount() >= 1 })

	// Commit without telling the manager — the crash-between-commit-and-schedule case.
	subs, _ := store.FetchSubList("orders")
	if _, err := store.Enqueue("orders", "orphan", "never scheduled", subs); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	eventually(t, "orphan delivered by reconcile", func() bool { return billing.calls() == 1 })
}

// A barrier failure while leadership holds produces no further LeaderCh signal,
// so promotion has to retry itself or the node leads with delivery stopped.
func TestPromotionRetriesWhenBarrierFails(t *testing.T) {
	billing := newSubscriber(t)
	store := storeWithPending(t, "orders", billing.policy("billing", 3), "m1")
	cluster := newFakeCluster()
	cluster.setBarrierErr(errors.New("barrier timed out"))

	m := delivery.NewManager(cluster, store, time.Hour)
	go m.Run()
	defer m.Stop()

	cluster.elect(true)
	eventually(t, "barrier retried", func() bool { return cluster.barrierCount() >= 2 })

	cluster.setBarrierErr(nil)
	eventually(t, "delivery after barrier recovers", func() bool { return billing.calls() == 1 })
}

// Losing leadership mid-retry should stop the attempts rather than burning the
// full backoff — a LeaderCh signal is already on its way.
func TestPromotionStopsRetryingAfterLosingLeadership(t *testing.T) {
	billing := newSubscriber(t)
	store := storeWithPending(t, "orders", billing.policy("billing", 3), "m1")
	cluster := newFakeCluster()
	cluster.setBarrierErr(errors.New("barrier timed out"))

	m := delivery.NewManager(cluster, store, time.Hour)
	go m.Run()
	defer m.Stop()

	cluster.elect(true)
	eventually(t, "first barrier", func() bool { return cluster.barrierCount() >= 1 })

	cluster.mu.Lock()
	cluster.leader = false
	cluster.mu.Unlock()

	time.Sleep(300 * time.Millisecond)
	settled := cluster.barrierCount()
	time.Sleep(300 * time.Millisecond)

	if cluster.barrierCount() != settled {
		t.Fatalf("kept retrying after losing leadership: %d then %d", settled, cluster.barrierCount())
	}
	if billing.calls() != 0 {
		t.Fatalf("delivered %d messages without ever completing promotion", billing.calls())
	}
}
