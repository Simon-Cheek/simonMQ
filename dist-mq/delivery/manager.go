package delivery

import (
	"sync"
	"time"

	"dist-mq/model"
	"dist-mq/storage"
)

const defaultReconcileInterval = 30 * time.Second

// Cluster is what delivery needs from the raft layer
// Defined as an interface for testability
type Cluster interface {
	LeaderCh() <-chan bool
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

	mu           sync.Mutex
	queues       map[string]*Queue
	inFlightMsgs map[string]struct{} // msgIDs already scheduled, deduping the two feeds
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
		queues:       make(map[string]*Queue),
		inFlightMsgs: make(map[string]struct{}),
	}
}

// Run initializes the data plane upon leader promotion
func (m *Manager) Run() {}

// Schedule places a message on the physical queue after commit
func (m *Manager) Schedule(queueName string, msg model.MessageInfo) {}

// Stop gracefully shuts down queue operations after demotion
func (m *Manager) Stop() {}
