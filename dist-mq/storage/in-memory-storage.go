package storage

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"dist-mq/model"
)

// InMemoryStorage keeps replicated state in plain maps. Losing it on restart is
// fine — raft rebuilds it from the newest snapshot plus the log entries after it.
type InMemoryStorage struct {
	mu sync.RWMutex

	queues map[string]*queueState

	// useful if we decide we want strict FIFO later on
	nextSeq uint64
}

type queueState struct {
	name     string
	subs     map[string]model.SubPolicy
	messages map[string]*messageState
}

type messageState struct {
	seq     uint64
	msgID   string
	payload string

	// subList is snapshotted at enqueue time, so a policy added later never
	// retroactively owes delivery of an older message
	subList   map[string]model.SubPolicy
	ackedSubs map[string]struct{}
}

var _ Storage = (*InMemoryStorage)(nil)

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{queues: make(map[string]*queueState)}
}

func (s *InMemoryStorage) CreateQueue(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.queues[name]; ok {
		return ErrQueueExists
	}
	s.queues[name] = &queueState{
		name:     name,
		subs:     make(map[string]model.SubPolicy),
		messages: make(map[string]*messageState),
	}
	return nil
}

func (s *InMemoryStorage) DeleteQueue(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.queues[name]; !ok {
		return ErrQueueNotFound
	}
	delete(s.queues, name)
	return nil
}

func (s *InMemoryStorage) PutSubPolicy(queueName string, policy model.SubPolicy) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	q, ok := s.queues[queueName]
	if !ok {
		return ErrQueueNotFound
	}
	q.subs[policy.SubName] = policy
	return nil
}

func (s *InMemoryStorage) DeleteSubPolicy(queueName, subName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	q, ok := s.queues[queueName]
	if !ok {
		return ErrQueueNotFound
	}
	delete(q.subs, subName)
	return nil
}

func (s *InMemoryStorage) Enqueue(queueName, msgID, payload string) (model.MessageInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	q, ok := s.queues[queueName]
	if !ok {
		return model.MessageInfo{}, ErrQueueNotFound
	}
	if existing, ok := q.messages[msgID]; ok {
		return toMessageInfo(existing), nil
	}

	msg := &messageState{
		seq:       s.nextSeq,
		msgID:     msgID,
		payload:   payload,
		subList:   copySubs(q.subs),
		ackedSubs: make(map[string]struct{}),
	}
	s.nextSeq++

	// Nothing subscribes, so nothing is owed a delivery and the message is
	// already complete. Storing it would leave a row that the reconcile sweep
	// revisits forever. durable-mq drops these in the worker instead; same
	// outcome, one step earlier.
	if len(msg.subList) > 0 {
		q.messages[msgID] = msg
	}
	return toMessageInfo(msg), nil
}

func (s *InMemoryStorage) Ack(queueName, msgID string, subNames []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	q, ok := s.queues[queueName]
	if !ok {
		return nil
	}
	msg, ok := q.messages[msgID]
	if !ok {
		return nil
	}

	for _, subName := range subNames {
		// Ignore names outside the snapshot: a subscriber registered after
		// this message was enqueued was never owed it in the first place.
		if _, owed := msg.subList[subName]; !owed {
			continue
		}
		msg.ackedSubs[subName] = struct{}{}
	}

	// Dropping completed messages is what keeps state proportional to
	// outstanding work rather than to history — it bounds memory, keeps
	// snapshots small, and keeps the reconcile sweep scanning only live work.
	if len(msg.ackedSubs) >= len(msg.subList) {
		delete(q.messages, msgID)
	}
	return nil
}

func (s *InMemoryStorage) QueueNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.queues))
	for name := range s.queues {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *InMemoryStorage) AllQueueInfo() []model.QueueInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	all := make([]model.QueueInfo, 0, len(s.queues))
	for _, q := range s.queues {
		all = append(all, model.QueueInfo{
			Name:        q.name,
			SubPolicies: copySubs(q.subs),
			Messages:    messagesInOrder(q),
		})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })
	return all
}

func (s *InMemoryStorage) FetchSubList(queueName string) (map[string]model.SubPolicy, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	q, ok := s.queues[queueName]
	if !ok {
		return nil, false
	}
	return copySubs(q.subs), true
}

func (s *InMemoryStorage) PendingMessages(queueName string) ([]model.MessageInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	q, ok := s.queues[queueName]
	if !ok {
		return nil, ErrQueueNotFound
	}
	return messagesInOrder(q), nil
}

type snapshotState struct {
	NextSeq uint64          `json:"NextSeq"`
	Queues  []snapshotQueue `json:"Queues"`
}

type snapshotQueue struct {
	Name     string                     `json:"Name"`
	Subs     map[string]model.SubPolicy `json:"Subs"`
	Messages []snapshotMessage          `json:"Messages"`
}

type snapshotMessage struct {
	Seq       uint64                     `json:"Seq"`
	MsgID     string                     `json:"MsgId"`
	Payload   string                     `json:"Payload"`
	SubList   map[string]model.SubPolicy `json:"SubList"`
	AckedSubs []string                   `json:"AckedSubs"`
}

func (s *InMemoryStorage) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := snapshotState{
		NextSeq: s.nextSeq,
		Queues:  make([]snapshotQueue, 0, len(s.queues)),
	}
	for _, q := range s.queues {
		msgs := make([]snapshotMessage, 0, len(q.messages))
		for _, msg := range q.messages {
			msgs = append(msgs, snapshotMessage{
				Seq:       msg.seq,
				MsgID:     msg.msgID,
				Payload:   msg.payload,
				SubList:   copySubs(msg.subList),
				AckedSubs: sortedKeys(msg.ackedSubs),
			})
		}
		sort.Slice(msgs, func(i, j int) bool { return msgs[i].Seq < msgs[j].Seq })

		snap.Queues = append(snap.Queues, snapshotQueue{
			Name:     q.name,
			Subs:     copySubs(q.subs),
			Messages: msgs,
		})
	}
	sort.Slice(snap.Queues, func(i, j int) bool { return snap.Queues[i].Name < snap.Queues[j].Name })

	return json.Marshal(snap)
}

func (s *InMemoryStorage) Restore(data []byte) error {
	var snap snapshotState
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}

	queues := make(map[string]*queueState, len(snap.Queues))
	for _, sq := range snap.Queues {
		q := &queueState{
			name:     sq.Name,
			subs:     copySubs(sq.Subs),
			messages: make(map[string]*messageState, len(sq.Messages)),
		}
		for _, sm := range sq.Messages {
			acked := make(map[string]struct{}, len(sm.AckedSubs))
			for _, subName := range sm.AckedSubs {
				acked[subName] = struct{}{}
			}
			q.messages[sm.MsgID] = &messageState{
				seq:       sm.Seq,
				msgID:     sm.MsgID,
				payload:   sm.Payload,
				subList:   copySubs(sm.SubList),
				ackedSubs: acked,
			}
		}
		queues[sq.Name] = q
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues = queues
	s.nextSeq = snap.NextSeq
	return nil
}

// Close exists for the interface — an implementation backed by a file has one
// to honor. Nothing to release here.
func (s *InMemoryStorage) Close() error { return nil }

// --- helpers ---------------------------------------------------------------

func messagesInOrder(q *queueState) []model.MessageInfo {
	ordered := make([]*messageState, 0, len(q.messages))
	for _, msg := range q.messages {
		ordered = append(ordered, msg)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].seq < ordered[j].seq })

	out := make([]model.MessageInfo, 0, len(ordered))
	for _, msg := range ordered {
		out = append(out, toMessageInfo(msg))
	}
	return out
}

func toMessageInfo(msg *messageState) model.MessageInfo {
	return model.MessageInfo{
		MsgID:     msg.msgID,
		Payload:   msg.payload,
		SubList:   copySubs(msg.subList),
		AckedSubs: sortedKeys(msg.ackedSubs),
	}
}

func copySubs(in map[string]model.SubPolicy) map[string]model.SubPolicy {
	out := make(map[string]model.SubPolicy, len(in))
	for name, policy := range in {
		out[name] = policy
	}
	return out
}

func sortedKeys(in map[string]struct{}) []string {
	out := make([]string, 0, len(in))
	for k := range in {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
