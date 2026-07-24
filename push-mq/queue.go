package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const initialCapacity = 10

type Queue struct {
	name    string
	count   int                     // Total Unread Msgs
	head    int                     // Location of last unread msg
	buf     []*QueueMsg             // Ring buffer
	mu      sync.Mutex              // Manages Queue Internals
	SubMeta map[string]*SubMetadata // Map of sub names to Metadata
}

type QueueMsg struct {
	MsgId     string
	Payload   string
	ackedSubs map[string]struct{}                  // Acked or Ran out of Retries (No DLQ yet)
	metadata  map[string]*QueueMsgDeliveryMetadata // Maps Subscriber names to their metadata
}

type QueueMsgDeliveryMetadata struct {
	retryCount            int
	lastRetry             time.Time
	deliverySuccessStatus bool // Remains false if retries are exceeded
}

func newQueue(name string) *Queue {
	return &Queue{
		name:    name,
		count:   0,
		head:    0,
		buf:     make([]*QueueMsg, initialCapacity),
		SubMeta: make(map[string]*SubMetadata),
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

func (q *Queue) addSubscriber(metadata SubMetadata) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !isValidSubMetadata(metadata) {
		return errors.New("invalid subscriber metadata")
	}
	initializeDefaultSubMetadataFields(&metadata)
	subMetaList := q.SubMeta
	subMetaList[metadata.subName] = &metadata
	return nil
}

func (q *Queue) updateSubscriber(metadata SubMetadata) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !isValidSubMetadata(metadata) {
		return errors.New("invalid subscriber metadata")
	}
	initializeDefaultSubMetadataFields(&metadata)
	subMetaList := q.SubMeta
	subMetaList[metadata.subName] = &metadata
	return nil
}

func (q *Queue) removeSubscriber(subName string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	subMetaList := q.SubMeta
	delete(subMetaList, subName)
}

func initializeDefaultSubMetadataFields(metadata *SubMetadata) {
	if metadata.numberOfRetries < 0 {
		metadata.numberOfRetries = 3
	}
	if metadata.retryPolicy == "" {
		metadata.retryPolicy = "fixed"
	}
	if metadata.initialDelay < 1 {
		metadata.initialDelay = 25
	}
}

func isValidSubMetadata(metadata SubMetadata) bool {
	if metadata.subName == "" {
		return false
	}
	if metadata.retryPolicy != "fixed" && metadata.retryPolicy != "exponential" && metadata.retryPolicy != "" {
		return false
	}

	if metadata.subURL == "" {
		return false
	}

	// No Initial Delays longer than a minute
	if metadata.initialDelay > 60000 {
		return false
	}

	if metadata.numberOfRetries > 100 {
		return false
	}

	return true
}
