package main

import (
	"errors"
	"fmt"
	"sync"
)

const initialCapacity = 10

type Queue struct {
	name        string
	count       int                   // Total Unread Msgs
	head        int                   // Location of last unread msg
	buf         []*QueueMsg           // Ring buffer
	mu          sync.Mutex            // Protects structs internal to the queue (including SubPolicy)
	SubPolicies map[string]*SubPolicy // Map of sub names to Metadata

	// Worker Management
	hasWork  chan struct{}
	isClosed chan struct{}
}

type QueueMsg struct {
	MsgId     string
	Payload   string
	AckedSubs map[string]struct{} // Acked or Ran out of Retries (No DLQ yet)
	RetryMap  map[string]int      // Maps Subscriber names to their retry count
}

func NewQueue(name string) *Queue {
	return &Queue{
		name:        name,
		count:       0,
		head:        0,
		buf:         make([]*QueueMsg, initialCapacity),
		SubPolicies: make(map[string]*SubPolicy),
		hasWork:     make(chan struct{}, 1), // Single Value Channel meant to notify the associated worker
		isClosed:    make(chan struct{}),
	}
}

func NewQueueMsg(queueName string, payload string) *QueueMsg {
	return &QueueMsg{
		MsgId:     queueName + "/", //+ uuid.New().String(), // Todo: Replace with non UUID string
		Payload:   payload,
		AckedSubs: make(map[string]struct{}),
		RetryMap:  make(map[string]int),
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

func (q *Queue) AddSubscriber(metadata SubPolicy) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !isValidSubMetadata(metadata) {
		return errors.New("invalid subscriber metadata")
	}
	initializeDefaultSubMetadataFields(&metadata)
	subMetaList := q.SubPolicies
	subMetaList[metadata.SubName] = &metadata
	return nil
}

// Check with Workers on concurrency if this ever modifies in place
func (q *Queue) UpdateSubscriber(metadata SubPolicy) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !isValidSubMetadata(metadata) {
		return errors.New("invalid subscriber metadata")
	}
	initializeDefaultSubMetadataFields(&metadata)
	subMetaList := q.SubPolicies
	subMetaList[metadata.SubName] = &metadata
	return nil
}

func (q *Queue) RemoveSubscriber(subName string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	subMetaList := q.SubPolicies
	delete(subMetaList, subName)
}

func initializeDefaultSubMetadataFields(metadata *SubPolicy) {
	if metadata.NumberOfRetries < 0 {
		metadata.NumberOfRetries = 3
	}
}

func isValidSubMetadata(metadata SubPolicy) bool {
	if metadata.SubName == "" {
		return false
	}
	if metadata.SubURL == "" {
		return false
	}
	if metadata.NumberOfRetries > 100 {
		return false
	}
	return true
}
