package delivery

// Internal test: worker and process are unexported, so this cannot live in
// unit-tests alongside the rest.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"dist-mq/model"
)

type subscriber struct {
	server *httptest.Server

	mu       sync.Mutex
	bodies   []string
	paths    []string
	failNext int  // number of upcoming requests to fail
	failAll  bool // fail everything
}

func newSubscriber(t *testing.T) *subscriber {
	t.Helper()
	s := &subscriber{}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		s.mu.Lock()
		s.bodies = append(s.bodies, string(body))
		s.paths = append(s.paths, r.URL.Path)
		fail := s.failAll || s.failNext > 0
		if s.failNext > 0 {
			s.failNext--
		}
		s.mu.Unlock()

		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(s.server.Close)
	return s
}

func (s *subscriber) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.bodies)
}

func (s *subscriber) policy(name string, retries int) model.SubPolicy {
	return model.SubPolicy{SubName: name, SubURL: s.server.URL, NumberOfRetries: retries}
}

type recorder struct {
	mu     sync.Mutex
	acks   [][]string
	done   []string
	ackErr error
}

func (r *recorder) onAck(_ string, subNames []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ackErr != nil {
		return r.ackErr
	}
	names := make([]string, len(subNames))
	copy(names, subNames)
	r.acks = append(r.acks, names)
	return nil
}

func (r *recorder) onDone(msgID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.done = append(r.done, msgID)
}

func newTestWorker(t *testing.T, stopCh <-chan struct{}) (*worker, *Queue, *recorder) {
	t.Helper()
	if stopCh == nil {
		stopCh = make(chan struct{})
	}
	q := NewQueue("orders")
	rec := &recorder{}
	return newWorker(q, stopCh, NewDeliveryClient(), rec.onAck, rec.onDone), q, rec
}

func testMsg(subs ...model.SubPolicy) *QueueMsg {
	list := make(map[string]model.SubPolicy, len(subs))
	for _, s := range subs {
		list[s.SubName] = s
	}
	return NewQueueMsg(model.MessageInfo{MsgID: "m1", Payload: "hello", SubList: list})
}

// One pass over N subscribers produces exactly one Ack command, not N.
func TestProcessBatchesAcksIntoOneCommand(t *testing.T) {
	billing, audit := newSubscriber(t), newSubscriber(t)
	w, q, rec := newTestWorker(t, nil)

	w.process(testMsg(billing.policy("billing", 3), audit.policy("audit", 3)))

	if len(rec.acks) != 1 {
		t.Fatalf("got %d ack commands, want 1: %v", len(rec.acks), rec.acks)
	}
	if len(rec.acks[0]) != 2 || rec.acks[0][0] != "audit" || rec.acks[0][1] != "billing" {
		t.Fatalf("ack batch = %v, want [audit billing]", rec.acks[0])
	}
	if len(rec.done) != 1 {
		t.Fatalf("fully acked message not reported done: %v", rec.done)
	}
	if q.Len() != 0 {
		t.Fatal("fully acked message was requeued")
	}
}

func TestProcessDeliversPayloadToSubscriberPath(t *testing.T) {
	billing := newSubscriber(t)
	w, _, _ := newTestWorker(t, nil)

	w.process(testMsg(billing.policy("billing", 3)))

	billing.mu.Lock()
	defer billing.mu.Unlock()
	if len(billing.bodies) != 1 || billing.bodies[0] != "hello" {
		t.Fatalf("delivered bodies = %v, want [hello]", billing.bodies)
	}
	if billing.paths[0] != deliveryPath {
		t.Fatalf("delivered to %q, want %q", billing.paths[0], deliveryPath)
	}
}

// A failing subscriber with retries left is not acked, and the message goes
// back on the queue so the next pass retries only that subscriber.
func TestProcessRequeuesWhenASubscriberFails(t *testing.T) {
	billing, audit := newSubscriber(t), newSubscriber(t)
	audit.mu.Lock()
	audit.failAll = true
	audit.mu.Unlock()

	w, q, rec := newTestWorker(t, nil)
	msg := testMsg(billing.policy("billing", 3), audit.policy("audit", 3))
	w.process(msg)

	if len(rec.acks) != 1 || len(rec.acks[0]) != 1 || rec.acks[0][0] != "billing" {
		t.Fatalf("ack batch = %v, want [billing] only", rec.acks)
	}
	if q.Len() != 1 {
		t.Fatal("message with an outstanding subscriber was not requeued")
	}
	if len(rec.done) != 0 {
		t.Fatalf("incomplete message reported done: %v", rec.done)
	}

	// Second pass must skip the subscriber that already acked.
	w.process(q.Pop())
	if billing.calls() != 1 {
		t.Fatalf("billing was delivered %d times, want 1", billing.calls())
	}
}

// Exhausting retries is also "stop delivering", so it rides the same command.
func TestProcessAcksSubscriberThatRanOutOfRetries(t *testing.T) {
	audit := newSubscriber(t)
	audit.mu.Lock()
	audit.failAll = true
	audit.mu.Unlock()

	w, q, rec := newTestWorker(t, nil)
	msg := testMsg(audit.policy("audit", 2))

	w.process(msg)
	if len(rec.acks) != 0 {
		t.Fatalf("acked before retries were exhausted: %v", rec.acks)
	}
	if q.Len() != 1 {
		t.Fatal("message not requeued after first failure")
	}

	w.process(q.Pop())
	if len(rec.acks) != 1 || rec.acks[0][0] != "audit" {
		t.Fatalf("ack batch after exhausting retries = %v, want [audit]", rec.acks)
	}
	if len(rec.done) != 1 {
		t.Fatalf("message not reported done after giving up: %v", rec.done)
	}
	if q.Len() != 0 {
		t.Fatal("message requeued after giving up on every subscriber")
	}
}

// Nothing finished, so proposing an empty Ack would burn a log entry for a
// no-op on every node.
func TestProcessProposesNothingWhenNoSubscriberFinished(t *testing.T) {
	audit := newSubscriber(t)
	audit.mu.Lock()
	audit.failAll = true
	audit.mu.Unlock()

	w, q, rec := newTestWorker(t, nil)
	w.process(testMsg(audit.policy("audit", 5)))

	if len(rec.acks) != 0 {
		t.Fatalf("proposed an ack with no finished subscribers: %v", rec.acks)
	}
	if q.Len() != 1 {
		t.Fatal("message not requeued")
	}
}

// Never record an ack in memory that the replicated log does not have.
func TestProcessDoesNotMarkAcksWhenProposeFails(t *testing.T) {
	billing := newSubscriber(t)
	w, q, rec := newTestWorker(t, nil)
	rec.ackErr = errProposeFailed

	msg := testMsg(billing.policy("billing", 3))
	w.process(msg)

	if len(msg.AckedSubs) != 0 {
		t.Fatalf("marked acks in memory despite a failed propose: %v", msg.AckedSubs)
	}
	if q.Len() != 1 {
		t.Fatal("message not requeued after a failed propose")
	}
	if len(rec.done) != 0 {
		t.Fatalf("message reported done despite a failed propose: %v", rec.done)
	}
}

// A demoted leader must discard results rather than proposing them — a stale
// give-up ack landing in the next term would suppress delivery the new leader
// has not attempted.
func TestProcessDiscardsResultsAfterDemotion(t *testing.T) {
	billing := newSubscriber(t)
	stopCh := make(chan struct{})
	w, _, rec := newTestWorker(t, stopCh)

	close(stopCh)
	w.process(testMsg(billing.policy("billing", 3)))

	if len(rec.acks) != 0 {
		t.Fatalf("proposed an ack after demotion: %v", rec.acks)
	}
	if len(rec.done) != 0 {
		t.Fatalf("reported done after demotion: %v", rec.done)
	}
}

func TestRunExitsOnStop(t *testing.T) {
	stopCh := make(chan struct{})
	w, _, _ := newTestWorker(t, stopCh)

	exited := make(chan struct{})
	go func() {
		w.run()
		close(exited)
	}()

	close(stopCh)
	<-exited
}

// A backlog must not have to drain before the worker notices demotion.
func TestRunStopsMidBacklog(t *testing.T) {
	billing := newSubscriber(t)
	stopCh := make(chan struct{})
	w, q, _ := newTestWorker(t, stopCh)

	for i := range 200 {
		msg := testMsg(billing.policy("billing", 3))
		msg.MsgID = string(rune('a' + i%26))
		q.Add(msg)
	}
	close(stopCh)

	exited := make(chan struct{})
	go func() {
		w.run()
		close(exited)
	}()
	<-exited

	if billing.calls() != 0 {
		t.Fatalf("delivered %d messages after demotion, want 0", billing.calls())
	}
}

var errProposeFailed = &proposeError{}

type proposeError struct{}

func (e *proposeError) Error() string { return "propose failed" }
