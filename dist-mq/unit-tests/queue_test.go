package unit_tests

import (
	"fmt"
	"sync"
	"testing"

	"dist-mq/delivery"
	"dist-mq/model"
)

func msgInfo(msgID string, subNames ...string) model.MessageInfo {
	subs := make(map[string]model.SubPolicy, len(subNames))
	for _, name := range subNames {
		subs[name] = model.SubPolicy{SubName: name, SubURL: "http://" + name, NumberOfRetries: 3}
	}
	return model.MessageInfo{MsgID: msgID, Payload: "payload-" + msgID, SubList: subs}
}

func drain(q *delivery.Queue) []string {
	var ids []string
	for {
		msg := q.Pop()
		if msg == nil {
			return ids
		}
		ids = append(ids, msg.MsgID)
	}
}

func TestQueuePopsInFIFOOrder(t *testing.T) {
	q := delivery.NewQueue("orders")
	for i := range 5 {
		q.Add(delivery.NewQueueMsg(msgInfo(fmt.Sprintf("m%d", i))))
	}

	got := drain(q)
	for i, id := range got {
		if want := fmt.Sprintf("m%d", i); id != want {
			t.Fatalf("pop order = %v, want m0..m4", got)
		}
	}
}

func TestQueuePopOnEmptyReturnsNil(t *testing.T) {
	q := delivery.NewQueue("orders")
	if msg := q.Pop(); msg != nil {
		t.Fatalf("Pop on empty queue = %+v, want nil", msg)
	}
}

// Grow has to preserve order across the wraparound, which is where an
// off-by-one in copyOver would show up.
func TestQueueGrowsPreservingOrder(t *testing.T) {
	q := delivery.NewQueue("orders")
	const n = 500
	for i := range n {
		q.Add(delivery.NewQueueMsg(msgInfo(fmt.Sprintf("m%d", i))))
	}
	if q.Len() != n {
		t.Fatalf("Len = %d, want %d", q.Len(), n)
	}

	got := drain(q)
	if len(got) != n {
		t.Fatalf("drained %d messages, want %d", len(got), n)
	}
	for i, id := range got {
		if want := fmt.Sprintf("m%d", i); id != want {
			t.Fatalf("message %d = %q, want %q", i, id, want)
		}
	}
}

// Interleaving Add and Pop walks head around the buffer, so this exercises the
// wraparound branch of copyOver in both grow and compact.
func TestQueueWrapsAroundUnderInterleavedAddPop(t *testing.T) {
	q := delivery.NewQueue("orders")
	next, popped := 0, 0

	for round := range 200 {
		for range 3 {
			q.Add(delivery.NewQueueMsg(msgInfo(fmt.Sprintf("m%d", next))))
			next++
		}
		for range 2 {
			msg := q.Pop()
			if msg == nil {
				t.Fatalf("round %d: unexpected empty queue", round)
			}
			if want := fmt.Sprintf("m%d", popped); msg.MsgID != want {
				t.Fatalf("round %d: popped %q, want %q", round, msg.MsgID, want)
			}
			popped++
		}
	}

	for _, id := range drain(q) {
		if want := fmt.Sprintf("m%d", popped); id != want {
			t.Fatalf("tail drain: popped %q, want %q", id, want)
		}
		popped++
	}
	if popped != next {
		t.Fatalf("popped %d of %d added", popped, next)
	}
}

func TestQueueSignalsWork(t *testing.T) {
	q := delivery.NewQueue("orders")
	q.Add(delivery.NewQueueMsg(msgInfo("m1")))

	select {
	case <-q.HasWork():
	default:
		t.Fatal("Add did not signal HasWork")
	}
}

// The signal is buffered at one and sent non-blocking, so a burst collapses
// into a single wakeup rather than blocking the adder.
func TestQueueWorkSignalCollapsesAndNeverBlocks(t *testing.T) {
	q := delivery.NewQueue("orders")
	for i := range 100 {
		q.Add(delivery.NewQueueMsg(msgInfo(fmt.Sprintf("m%d", i))))
	}

	select {
	case <-q.HasWork():
	default:
		t.Fatal("no work signal after 100 adds")
	}
	select {
	case <-q.HasWork():
		t.Fatal("work signal was queued more than once")
	default:
	}
}

func TestQueueMsgTracksPendingSubs(t *testing.T) {
	msg := delivery.NewQueueMsg(msgInfo("m1", "billing", "audit"))

	if len(msg.PendingSubs()) != 2 {
		t.Fatalf("PendingSubs = %v, want both", msg.PendingSubs())
	}
	if msg.Done() {
		t.Fatal("Done reported true with both subscribers outstanding")
	}

	msg.AckedSubs["billing"] = struct{}{}
	pending := msg.PendingSubs()
	if len(pending) != 1 {
		t.Fatalf("PendingSubs after one ack = %v, want just audit", pending)
	}
	if _, ok := pending["audit"]; !ok {
		t.Fatalf("PendingSubs = %v, want audit", pending)
	}

	msg.AckedSubs["audit"] = struct{}{}
	if !msg.Done() {
		t.Fatal("Done reported false with every subscriber acked")
	}
}

// A message restored from storage carries the acks that already landed, so a
// promoted leader does not redeliver to subscribers that are already done.
func TestQueueMsgCarriesAcksFromStorage(t *testing.T) {
	info := msgInfo("m1", "billing", "audit")
	info.AckedSubs = []string{"billing"}

	msg := delivery.NewQueueMsg(info)
	pending := msg.PendingSubs()
	if len(pending) != 1 {
		t.Fatalf("PendingSubs = %v, want only audit", pending)
	}
	if _, ok := pending["audit"]; !ok {
		t.Fatalf("PendingSubs = %v, want audit", pending)
	}
}

func TestQueueMsgDoesNotAliasSource(t *testing.T) {
	info := msgInfo("m1", "billing")
	msg := delivery.NewQueueMsg(info)

	info.SubList["injected"] = model.SubPolicy{SubName: "injected"}
	if _, ok := msg.SubList["injected"]; ok {
		t.Fatal("QueueMsg aliased the source SubList")
	}
}

func TestQueueIsSafeUnderConcurrentAddAndPop(t *testing.T) {
	q := delivery.NewQueue("orders")
	const producers, perProducer = 8, 250

	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := range perProducer {
				q.Add(delivery.NewQueueMsg(msgInfo(fmt.Sprintf("p%d-m%d", p, i))))
			}
		}(p)
	}

	var mu sync.Mutex
	seen := make(map[string]struct{})
	var consumers sync.WaitGroup
	done := make(chan struct{})
	for range 4 {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			for {
				if msg := q.Pop(); msg != nil {
					mu.Lock()
					if _, dup := seen[msg.MsgID]; dup {
						mu.Unlock()
						panic("message popped twice: " + msg.MsgID)
					}
					seen[msg.MsgID] = struct{}{}
					mu.Unlock()
					continue
				}
				select {
				case <-done:
					return
				default:
				}
			}
		}()
	}

	wg.Wait()
	close(done)
	consumers.Wait()

	for _, msg := range drain(q) {
		seen[msg] = struct{}{}
	}
	if len(seen) != producers*perProducer {
		t.Fatalf("saw %d distinct messages, want %d", len(seen), producers*perProducer)
	}
}
