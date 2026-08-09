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

// Run initializes the data plane upon leader promotion
func (m *Manager) Run() {
	for isLeader := range m.cluster.LeaderCh() {
		if isLeader {
			m.promoteWithRetry()
		} else {
			m.demote()
		}
	}
}

// Schedule places a message on the physical queue after commit
func (m *Manager) Schedule(queueName string, msg model.MessageInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopCh == nil {
		return // not leading; the next promotion sweeps this out of storage
	}
	if _, ok := m.inFlightMsgs[msg.MsgID]; ok {
		return // the other feed got here first
	}
	if len(msg.PendingSubs()) == 0 {
		return
	}

	m.inFlightMsgs[msg.MsgID] = struct{}{}
	m.queueFor(queueName).Add(NewQueueMsg(msg))
}

// Stop gracefully shuts down queue operations after demotion
func (m *Manager) Stop() { m.demote() }

// promoteWithRetry covers a barrier that fails while leadership holds. LeaderCh
// only fires on transitions, so without a retry that leaves the node leading
// with no delivery running and nothing to wake it.
func (m *Manager) promoteWithRetry() {
	for attempt := range promoteAttempts {
		if err := m.promote(); err == nil {
			return
		}
		if !m.cluster.IsLeader() {
			return // leadership genuinely moved; a signal is already queued
		}
		time.Sleep(promoteBackoff << attempt) // Each further attempt doubles in delay (bit shift)
	}
}

func (m *Manager) promote() error {
	m.demote() // LeaderCh drops signals, so we may already be running

	// Makes sure that msgs are fully in the FSM before loading into live queue
	if err := m.cluster.Barrier(barrierTimeout); err != nil {
		return err
	}

	m.mu.Lock()
	m.stopCh = make(chan struct{})
	stopCh := m.stopCh // this term's channel, captured before anything can replace it
	m.mu.Unlock()

	m.sweep()
	go m.reconcileLoop(stopCh)
	return nil
}

// demote tears down by closing the one channel that each worker checks on
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
	for _, queueName := range m.store.QueueNames() {
		msgs, err := m.store.PendingMessages(queueName)
		if err != nil {
			continue // deleted between the two reads; nothing owed either way
		}
		for _, msg := range msgs {
			m.Schedule(queueName, msg)
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

	// Start worker lazily (only starts when an actual message is pushed onto queue)
	worker := NewWorker(q, m.stopCh, m.client,
		func(msgID string, subNames []string) error {
			return m.cluster.Ack(queueName, msgID, subNames)
		},
		m.forget,
	)
	go worker.Run()

	return q
}
