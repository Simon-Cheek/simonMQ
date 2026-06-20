package main

import (
	"fmt"
	"sync"
)

const initialCapacity = 10

type Queue struct {
	name  string
	count int         // Total Unread Msgs
	head  int         // Location of last unread msg
	buf   []*QueueMsg // Ring buffer
	mu    sync.Mutex
}

type QueueMsg struct {
	MsgId   string
	Payload string
}

func newQueue(name string) *Queue {
	return &Queue{
		name:  name,
		count: 0,
		head:  0,
		buf:   make([]*QueueMsg, initialCapacity),
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
