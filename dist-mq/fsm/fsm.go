package fsm

import (
	"fmt"
	"io"

	"github.com/hashicorp/raft"

	"dist-mq/command"
	"dist-mq/storage"
)

type FSM struct {
	store storage.Storage
}

var _ raft.FSM = (*FSM)(nil)

func New(store storage.Storage) *FSM {
	return &FSM{store: store}
}

func (f *FSM) Apply(log *raft.Log) any {
	cmd, err := command.Decode(log.Data)
	if err != nil {
		// Decode is deterministic, so every node rejects this identically and
		// nobody mutates — returning beats panicking, which would take down
		// the cluster and then panic again on replay.
		return fmt.Errorf("apply index %d: %w", log.Index, err)
	}

	switch cmd.Type {
	case command.CreateQueue:
		return f.store.CreateQueue(cmd.QueueName)
	case command.DeleteQueue:
		return f.store.DeleteQueue(cmd.QueueName)
	case command.PutSubPolicy:
		return f.store.PutSubPolicy(cmd.QueueName, cmd.Policy)
	case command.DeleteSubPolicy:
		return f.store.DeleteSubPolicy(cmd.QueueName, cmd.SubName)
	case command.Ack:
		return f.store.Ack(cmd.QueueName, cmd.MsgID, cmd.SubNames)
	case command.Enqueue:
		msg, err := f.store.Enqueue(cmd.QueueName, cmd.MsgID, cmd.Payload, cmd.SubList)
		if err != nil {
			return err
		}
		return msg
	default:
		return fmt.Errorf("apply index %d: unhandled command type %s", log.Index, cmd.Type)
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	data, err := f.store.Snapshot()
	if err != nil {
		return nil, err
	}
	return &snapshot{data: data}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	return f.store.Restore(data)
}

// snapshot holds bytes already captured under the store's lock, so Persist —
// which raft runs on its own goroutine alongside ongoing Applies — does pure
// I/O and blocks nothing.
type snapshot struct {
	data []byte
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
