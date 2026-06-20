package main

import "sync"

type Queue struct {
	name  string
	count int // Total Unread Msgs
	head  int // Location of last unread msg
	buf   []*QueueMsg
	mu    sync.Mutex
}

type QueueMsg struct {
	MsgId   string
	Payload string
}

func (q *Queue) add(msg *QueueMsg) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head+q.count >= len(q.buf) {
		q.grow()
	}
	q.buf[q.head+q.count] = msg
	q.count++
}

func (q *Queue) pop() *QueueMsg {
	q.mu.Lock()
	defer q.mu.Unlock()

	msg := q.buf[q.head]
	q.head++

	if q.count*3 < len(q.buf) && len(q.buf) > 100 {
		q.compact()
	}

	return msg

}

func (q *Queue) grow() {
	newBuf := make([]*QueueMsg, len(q.buf)*2)
	copy(newBuf, q.buf[:q.head])
	q.buf = newBuf
	q.head = 0
}

func (q *Queue) compact() {
	newBuf := make([]*QueueMsg, q.count*2)
	copy(newBuf, q.buf[:q.head])
	q.buf = newBuf
	q.head = 0
}
