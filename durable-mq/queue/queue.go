package queue

import (
	"durable-mq/model"
	"fmt"
	"sync"
)

const initialCapacity = 10

type Queue struct {
	name  string
	count int
	head  int
	buf   []*QueueMsg
	mu    sync.Mutex

	hasWork  chan struct{}
	isClosed chan struct{}

	onAck func(msgId, subName string) error // logs an ack durably via Coordinator
}

func NewQueue(name string, onAck func(msgId, subName string) error) *Queue {
	return &Queue{
		name:     name,
		count:    0,
		head:     0,
		buf:      make([]*QueueMsg, initialCapacity),
		hasWork:  make(chan struct{}, 1),
		isClosed: make(chan struct{}),
		onAck:    onAck,
	}
}

type QueueMsg struct {
	MsgId             string
	Payload           string
	AckedSubs         map[string]struct{} // Acked or Ran out of Retries (No DLQ yet)
	RetryMap          map[string]int      // Maps Subscriber names to their retry count
	SubPolicySnapshot map[string]model.SubPolicy
}

func NewQueueMsg(msgId string, payload string, subPolicySnapshot map[string]model.SubPolicy, ackedSubs map[string]struct{}) *QueueMsg {
	if ackedSubs == nil {
		ackedSubs = make(map[string]struct{})
	}
	return &QueueMsg{
		MsgId:             msgId,
		Payload:           payload,
		AckedSubs:         ackedSubs,
		RetryMap:          make(map[string]int),
		SubPolicySnapshot: subPolicySnapshot,
	}
}

func (q *Queue) Add(msg *QueueMsg) {
	q.mu.Lock()
	defer q.mu.Unlock()

	ind := (q.head + q.count) % len(q.buf)
	q.buf[ind] = msg
	q.count++

	if q.count >= len(q.buf) {
		q.grow()
	}

	// Notify associated worker if sleeping
	select {
	case q.hasWork <- struct{}{}:
	default: // Move on if it would block
	}

}

func (q *Queue) Pop() *QueueMsg {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.count == 0 {
		return nil
	}

	msg := q.buf[q.head]
	q.head = (q.head + 1) % len(q.buf)
	q.count--

	if q.count*4 <= len(q.buf) {
		q.compact()
	}

	return msg
}

func (q *Queue) grow() {
	newBuf := make([]*QueueMsg, len(q.buf)*2)
	q.copyOver(newBuf)
}

func (q *Queue) compact() {
	newBuf := make([]*QueueMsg, max(len(q.buf)/2, initialCapacity))
	q.copyOver(newBuf)
}

func (q *Queue) copyOver(newBuf []*QueueMsg) {
	if q.count > len(newBuf) {
		panic(fmt.Sprintf("copyOver: count %d exceeds newBuf capacity %d", q.count, len(newBuf)))
	}

	copy(newBuf, q.buf[q.head:])
	nItemsCopied := len(q.buf[q.head:])
	remainingItemsToCopy := q.count - nItemsCopied

	// Wrap Around Case
	if remainingItemsToCopy > 0 {
		copy(newBuf[nItemsCopied:], q.buf[:remainingItemsToCopy])
	}
	q.buf = newBuf
	q.head = 0
}
