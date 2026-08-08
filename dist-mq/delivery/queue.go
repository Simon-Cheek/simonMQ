package delivery

import (
	"fmt"
	"sync"

	"dist-mq/model"
)

const initialCapacity = 10

// Queue is a leader-local scheduling buffer, not state. Everything in it is
// rebuildable from storage, which is what lets the manager throw it away on
// demotion and rebuild it on promotion.
type Queue struct {
	name string

	mu    sync.Mutex
	buf   []*QueueMsg
	head  int
	count int

	hasWork chan struct{}
}

func NewQueue(name string) *Queue {
	return &Queue{
		name:    name,
		buf:     make([]*QueueMsg, initialCapacity),
		hasWork: make(chan struct{}, 1),
	}
}

type QueueMsg struct {
	MsgID   string
	Payload string

	// SubList is a snapshot taken at enqueue time and replicated with the
	// command, so it never shifts under a message already in flight.
	SubList   map[string]model.SubPolicy
	AckedSubs map[string]struct{}

	// RetryMap is leader-local and deliberately not replicated — a failover
	// resets a message's retry budget rather than costing a quorum round trip
	// per attempt.
	RetryMap map[string]int
}

func NewQueueMsg(info model.MessageInfo) *QueueMsg {
	acked := make(map[string]struct{}, len(info.AckedSubs))
	for _, subName := range info.AckedSubs {
		acked[subName] = struct{}{}
	}

	subs := make(map[string]model.SubPolicy, len(info.SubList))
	for name, policy := range info.SubList {
		subs[name] = policy
	}

	return &QueueMsg{
		MsgID:     info.MsgID,
		Payload:   info.Payload,
		SubList:   subs,
		AckedSubs: acked,
		RetryMap:  make(map[string]int),
	}
}

// PendingSubs is recomputed per pass because AckedSubs grows as deliveries land.
func (m *QueueMsg) PendingSubs() map[string]model.SubPolicy {
	pending := make(map[string]model.SubPolicy, len(m.SubList))
	for name, policy := range m.SubList {
		if _, done := m.AckedSubs[name]; done {
			continue
		}
		pending[name] = policy
	}
	return pending
}

func (m *QueueMsg) Done() bool {
	return len(m.AckedSubs) >= len(m.SubList)
}

func (q *Queue) Name() string { return q.name }

// HasWork signals a sleeping worker. Buffered at one and sent non-blocking, so
// a burst of Adds collapses into a single wakeup.
func (q *Queue) HasWork() <-chan struct{} { return q.hasWork }

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
}

func (q *Queue) Add(msg *QueueMsg) {
	q.mu.Lock()
	ind := (q.head + q.count) % len(q.buf)
	q.buf[ind] = msg
	q.count++

	if q.count >= len(q.buf) {
		q.grow()
	}
	q.mu.Unlock()

	select {
	case q.hasWork <- struct{}{}:
	default:
	}
}

func (q *Queue) Pop() *QueueMsg {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.count == 0 {
		return nil
	}

	msg := q.buf[q.head]
	q.buf[q.head] = nil // drop the reference so a popped message can be collected
	q.head = (q.head + 1) % len(q.buf)
	q.count--

	if q.count*4 <= len(q.buf) {
		q.compact()
	}
	return msg
}

func (q *Queue) grow() {
	q.copyOver(make([]*QueueMsg, len(q.buf)*2))
}

func (q *Queue) compact() {
	q.copyOver(make([]*QueueMsg, max(len(q.buf)/2, initialCapacity)))
}

func (q *Queue) copyOver(newBuf []*QueueMsg) {
	if q.count > len(newBuf) {
		panic(fmt.Sprintf("copyOver: count %d exceeds newBuf capacity %d", q.count, len(newBuf)))
	}

	copy(newBuf, q.buf[q.head:])
	nItemsCopied := len(q.buf[q.head:])

	if remaining := q.count - nItemsCopied; remaining > 0 {
		copy(newBuf[nItemsCopied:], q.buf[:remaining])
	}
	q.buf = newBuf
	q.head = 0
}
