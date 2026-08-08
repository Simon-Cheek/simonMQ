package storage

import (
	"errors"

	"dist-mq/model"
)

var (
	ErrQueueNotFound = errors.New("queue not found")
	ErrQueueExists   = errors.New("queue already exists")
)

// Storage holds the replicated state of the broker: which queues exist, who
// subscribes to them, and which messages still have subscribers owed a
// delivery. It is the state machine behind raft — never the durability
// mechanism. Interfaced in case this ever needs to be implemented by durable
// storage.
type Storage interface {
	// Mutations. Called only from fsm.Apply, on every node, in log order.
	CreateQueue(name string) error
	DeleteQueue(name string) error
	PutSubPolicy(queueName string, policy model.SubPolicy) error
	DeleteSubPolicy(queueName, subName string) error
	Enqueue(queueName, msgID, payload string, subList map[string]model.SubPolicy) (model.MessageInfo, error)
	Ack(queueName, msgID string, subNames []string) error

	// Reads. Servable by any node at any time, leader or follower. Not linearizable
	QueueNames() []string
	AllQueueInfo() []model.QueueInfo
	FetchSubList(queueName string) (map[string]model.SubPolicy, bool)
	PendingMessages(queueName string) ([]model.MessageInfo, error)

	// Snapshot plumbing for raft.FSM. Restore replaces all state wholesale —
	// an absent queue or message in the payload means deleted, not unchanged.
	Snapshot() ([]byte, error)
	Restore(data []byte) error

	Close() error
}
