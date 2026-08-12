package client_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"dist-mq/e2e-tests/client"
	"dist-mq/server"
)

// fakeNode is one broker: a handler that can be switched between leader,
// follower and election behaviour while a test runs.
type fakeNode struct {
	srv    *httptest.Server
	role   atomic.Value // "leader", "follower", "election"
	leader atomic.Value // URL advertised on a 421; empty means none is named
	hits   atomic.Int64
}

func newFakeNode(t *testing.T) *fakeNode {
	t.Helper()
	n := &fakeNode{}
	n.role.Store("follower")
	n.leader.Store("")

	n.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.hits.Add(1)
		switch n.role.Load().(string) {
		case "leader":
			w.WriteHeader(http.StatusAccepted)
		case "election":
			w.Header().Set("Retry-After", "1")
			http.Error(w, "no leader elected", http.StatusServiceUnavailable)
		default:
			if l := n.leader.Load().(string); l != "" {
				w.Header().Set(server.LeaderHeader, l)
			}
			http.Error(w, "not leader", http.StatusMisdirectedRequest)
		}
	}))
	t.Cleanup(n.srv.Close)
	return n
}

func (n *fakeNode) become(role string) { n.role.Store(role) }
func (n *fakeNode) points(at string)   { n.leader.Store(at) }
func (n *fakeNode) url() string        { return n.srv.URL }

func newClient(t *testing.T, nodes []string, attempts int) *client.Client {
	t.Helper()
	c, err := client.New(client.Config{
		Nodes:       nodes,
		MaxAttempts: attempts,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
		HTTPTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	return c
}

func TestFollowsRedirectToLeader(t *testing.T) {
	leader, follower := newFakeNode(t), newFakeNode(t)
	leader.become("leader")
	follower.points(leader.url())

	c := newClient(t, []string{follower.url(), leader.url()}, 5)
	res := c.Enqueue(context.Background(), "q", []byte("x"))

	if res.Outcome != client.Accepted {
		t.Fatalf("outcome = %v (status %d, err %v), want accepted", res.Outcome, res.Status, res.Err)
	}
	if c.Leader() != leader.url() {
		t.Fatalf("cached leader = %q, want %q", c.Leader(), leader.url())
	}
}

// The redirect is only worth having if it is remembered; otherwise every
// publish pays for the hop and a benchmark measures the wasted round trip.
func TestLeaderIsCachedAcrossWrites(t *testing.T) {
	leader, follower := newFakeNode(t), newFakeNode(t)
	leader.become("leader")
	follower.points(leader.url())

	c := newClient(t, []string{follower.url(), leader.url()}, 5)
	for i := 0; i < 5; i++ {
		if res := c.Enqueue(context.Background(), "q", []byte("x")); res.Outcome != client.Accepted {
			t.Fatalf("write %d: outcome = %v", i, res.Outcome)
		}
	}

	if got := follower.hits.Load(); got != 1 {
		t.Fatalf("follower was asked %d times, want 1 — leader is not being cached", got)
	}
}

// A follower that cannot name the leader still has to be survivable, because
// that is exactly what a node reports when the leader has moved.
func TestSweepsWhenRedirectNamesNobody(t *testing.T) {
	leader, follower := newFakeNode(t), newFakeNode(t)
	leader.become("leader")
	follower.points("") // 421 with no X-Dist-MQ-Leader

	c := newClient(t, []string{follower.url(), leader.url()}, 5)
	res := c.Enqueue(context.Background(), "q", []byte("x"))

	if res.Outcome != client.Accepted {
		t.Fatalf("outcome = %v (status %d), want accepted", res.Outcome, res.Status)
	}
}

func TestBacksOffThroughElectionThenSucceeds(t *testing.T) {
	node := newFakeNode(t)
	node.become("election")

	c := newClient(t, []string{node.url()}, 10)
	go func() {
		time.Sleep(20 * time.Millisecond)
		node.become("leader")
	}()

	res := c.Enqueue(context.Background(), "q", []byte("x"))
	if res.Outcome != client.Accepted {
		t.Fatalf("outcome = %v (status %d), want accepted", res.Outcome, res.Status)
	}
	if res.Attempts < 2 {
		t.Fatalf("attempts = %d, want the election to have been retried", res.Attempts)
	}
}

// A dead node must not be mistaken for an ambiguous write. Nothing was sent,
// so nothing can have committed, and a durability test is entitled to say so.
func TestDeadNodeFailsOverAndIsNotAmbiguous(t *testing.T) {
	dead := newFakeNode(t)
	deadURL := dead.url()
	dead.srv.Close()

	leader := newFakeNode(t)
	leader.become("leader")

	c := newClient(t, []string{deadURL, leader.url()}, 5)
	res := c.Enqueue(context.Background(), "q", []byte("x"))

	if res.Outcome != client.Accepted {
		t.Fatalf("outcome = %v (err %v), want accepted after failover", res.Outcome, res.Err)
	}
}

func TestEveryNodeDownIsRejectedNotAmbiguous(t *testing.T) {
	a, b := newFakeNode(t), newFakeNode(t)
	urls := []string{a.url(), b.url()}
	a.srv.Close()
	b.srv.Close()

	c := newClient(t, urls, 3)
	res := c.Enqueue(context.Background(), "q", []byte("x"))

	if res.Outcome != client.Rejected {
		t.Fatalf("outcome = %v, want rejected — nothing was ever sent", res.Outcome)
	}
}

// A node that is down is usually a node that is restarting. Burning the whole
// attempt budget in microseconds rejects writes that had only to wait.
func TestDownClusterBacksOffRatherThanSpinning(t *testing.T) {
	node := newFakeNode(t)
	urls := []string{node.url()}
	node.srv.Close()

	c, err := client.New(client.Config{
		Nodes:       urls,
		MaxAttempts: 4,
		BaseBackoff: 20 * time.Millisecond,
		MaxBackoff:  50 * time.Millisecond,
		HTTPTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}

	start := time.Now()
	res := c.Enqueue(context.Background(), "q", []byte("x"))
	elapsed := time.Since(start)

	if res.Outcome != client.Rejected {
		t.Fatalf("outcome = %v, want rejected", res.Outcome)
	}
	if elapsed < 30*time.Millisecond {
		t.Fatalf("gave up in %s across %d attempts — it is not backing off", elapsed, res.Attempts)
	}
}

// The case the whole classification exists for: the request went out and the
// answer did not come back. It may have committed.
func TestLostAnswerIsAmbiguous(t *testing.T) {
	stall := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-stall // accept the request, never answer
	}))
	// Cleanups run LIFO, and srv.Close waits on outstanding handlers, so the
	// release has to be registered after it in order to run before it.
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(stall) })

	c, err := client.New(client.Config{
		Nodes:       []string{srv.URL},
		MaxAttempts: 2,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  2 * time.Millisecond,
		HTTPTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}

	res := c.Enqueue(context.Background(), "q", []byte("x"))
	if res.Outcome != client.Ambiguous {
		t.Fatalf("outcome = %v (err %v), want ambiguous", res.Outcome, res.Err)
	}
}

// A 421 is decided by requireLeader before the handler runs, so nothing was
// proposed. Giving up after only redirects is a clean rejection, and calling
// it ambiguous would forfeit an assertion the test is entitled to make.
func TestRedirectsThatNeverLandAreRejected(t *testing.T) {
	follower := newFakeNode(t)
	follower.points("") // 421, names nobody, forever

	c := newClient(t, []string{follower.url()}, 3)
	res := c.Enqueue(context.Background(), "q", []byte("x"))

	if res.Outcome != client.Rejected {
		t.Fatalf("outcome = %v, want rejected — a 421 proves nothing committed", res.Outcome)
	}
}

// A 503 is the opposite case. writeError returns one when a propose fails
// after leadership moved, and that entry may still commit in the new term, so
// the write has to stay unassertable.
func TestUnresolvedElectionIsAmbiguous(t *testing.T) {
	node := newFakeNode(t)
	node.become("election") // 503 forever

	c := newClient(t, []string{node.url()}, 3)
	res := c.Enqueue(context.Background(), "q", []byte("x"))

	if res.Outcome != client.Ambiguous {
		t.Fatalf("outcome = %v, want ambiguous — a 503 may follow a propose that commits", res.Outcome)
	}
}

func TestRejectionIsNotRetried(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, "queue not found", http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	c := newClient(t, []string{srv.URL}, 5)
	res := c.Enqueue(context.Background(), "q", []byte("x"))

	if res.Outcome != client.Rejected {
		t.Fatalf("outcome = %v, want rejected", res.Outcome)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("hits = %d, want 1 — a 404 is an answer, not a reason to retry", got)
	}
}

// Single node and multi node have to behave identically from up here, since
// that is what lets one suite cover both deployments.
func TestSingleNodeNeedsNoRedirect(t *testing.T) {
	only := newFakeNode(t)
	only.become("leader")

	c := newClient(t, []string{only.url()}, 5)
	res := c.Enqueue(context.Background(), "q", []byte("x"))

	if res.Outcome != client.Accepted {
		t.Fatalf("outcome = %v, want accepted", res.Outcome)
	}
	if res.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", res.Attempts)
	}
}
