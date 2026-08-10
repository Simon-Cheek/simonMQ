package delivery

import (
	"net/http"
	"sync"
	"time"

	"dist-mq/model"
	"dist-mq/storage"
)

const (
	defaultReconcileInterval = 30 * time.Second
	barrierTimeout           = 10 * time.Second
	promoteAttempts          = 4
	promoteBackoff           = 250 * time.Millisecond
)

// Cluster is what delivery needs from the raft layer
// Defined as an interface for testability
type Cluster interface {
	LeaderCh() <-chan bool
	IsLeader() bool
	Barrier(timeout time.Duration) error
	Ack(queueName, msgID string, subNames []string) error
}

// Manager owns the leader-local delivery machinery: the in-memory queues, their
// workers, and the lifecycle that builds and destroys both around leadership.
// Everything it holds is rebuildable from storage.
type Manager struct {
	cluster  Cluster
	store    storage.Storage
	interval time.Duration // Duration in which FSM is swept for orphaned messages
	client   *http.Client  // shared by every worker so subscriber connections pool

	mu           sync.Mutex
	queues       map[string]*Queue
	inFlightMsgs map[string]struct{} // msgIDs already scheduled
	stopCh       chan struct{}       // closed on demotion; nil when not running
}

func NewManager(cluster Cluster, store storage.Storage, interval time.Duration) *Manager {
	if interval <= 0 {
		interval = defaultReconcileInterval
	}
	return &Manager{
		cluster:      cluster,
		store:        store,
		interval:     interval,
		client:       NewDeliveryClient(),
		queues:       make(map[string]*Queue),
		inFlightMsgs: make(map[string]struct{}),
	}
}

// Run consumes leadership transitions for the life of the process, building the
// delivery machinery on promotion and destroying it on demotion.
func (m *Manager) Run() {
	for {
		leader, ok := <-m.cluster.LeaderCh()
		if !ok {
			return
		}
		if leader {
			m.promoteWithRetry()
		} else {
			m.demote()
		}
	}
}

// Schedule is the single way work enters delivery, called both by the server
// after a commit and by sweep. Drops anything it should not act on: not
// leading, already scheduled, or owed to nobody.
func (m *Manager) Schedule(queueName string, msg model.MessageInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopCh == nil {
		return
	}
	if _, ok := m.inFlightMsgs[msg.MsgID]; ok {
		return
	}
	if len(msg.PendingSubs()) == 0 {
		return
	}

	m.inFlightMsgs[msg.MsgID] = struct{}{}
	q := m.queueFor(queueName)
	q.Add(NewQueueMsg(msg))
}

// Stop tears delivery down at process shutdown.
func (m *Manager) Stop() { m.demote() }

// promoteWithRetry covers a barrier that fails while leadership holds. LeaderCh
// only fires on transitions, so without a retry that leaves the node leading
// with no delivery running and nothing to wake it.
func (m *Manager) promoteWithRetry() {
	for attempts := range promoteAttempts {
		if !m.cluster.IsLeader() {
			return
		}
		err := m.promote()
		if err != nil {
			if attempts+1 >= promoteAttempts {
				return
			}
			time.Sleep(promoteBackoff << attempts)
		} else {
			return
		}
	}
}

// promote takes over: reconcile any existing state, wait until this node has
// applied what its predecessor committed, then rebuild queues from storage and
// start the reconcile ticker
func (m *Manager) promote() error {
	m.demote()

	// Make sure in flight logs are first persisted to FSM
	err := m.cluster.Barrier(barrierTimeout)
	if err != nil {
		return err
	}

	// Create stop channel to inform workers / sweepers of demotion
	m.mu.Lock()
	m.stopCh = make(chan struct{})
	curStopCh := m.stopCh // Capture within lock for local use
	m.mu.Unlock()

	// Fetch all in flight msgs and Schedule
	m.sweep()

	// Initiate reconcile sweeper
	go m.reconcileLoop(curStopCh)

	return nil
}

// demote tears down by closing the one channel every worker and the reconcile
// loop is selecting on. Must be idempotent and must not wait for anything.
func (m *Manager) demote() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopCh == nil {
		return
	}
	close(m.stopCh)
	m.stopCh = nil
	m.queues = make(map[string]*Queue)
	m.inFlightMsgs = make(map[string]struct{})
}

// sweep schedules everything storage still owes a delivery. Runs once at
// promotion to pick up the predecessor's work, then periodically to catch
// anything that committed but never reached Schedule.
func (m *Manager) sweep() {
	storageData := m.store.QueueNames()
	for _, q := range storageData {
		msgs, _ := m.store.PendingMessages(q)
		for _, msg := range msgs {
			m.Schedule(q, msg)
		}
	}
}

// stopCh is passed rather than read from the field so a loop from a previous
// term exits instead of latching onto the next term's channel.
func (m *Manager) reconcileLoop(stopCh <-chan struct{}) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			m.sweep()
		}
	}
}

// forget drops a completed message from the dedupe set. Safe only because
// storage deletes fully acked messages, so no sweep can re-present it.
func (m *Manager) forget(msgID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inFlightMsgs, msgID)
}

// queueFor requires m.mu. Queues are created on first use rather than on
// CreateQueue, which applies on every node and must not start anything.
func (m *Manager) queueFor(queueName string) *Queue {
	if q, ok := m.queues[queueName]; ok {
		return q
	}

	q := NewQueue(queueName)
	m.queues[queueName] = q

	// The closure captures queueName so the worker can ack against the right
	// queue without holding a cluster reference itself.
	worker := NewWorker(q, m.stopCh, m.client,
		func(msgID string, subNames []string) error {
			return m.cluster.Ack(queueName, msgID, subNames)
		},
		m.forget,
	)
	go worker.Run()

	return q
}
