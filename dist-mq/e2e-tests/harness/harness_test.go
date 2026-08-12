package harness_test

import (
	"context"
	"testing"
	"time"

	"dist-mq/e2e-tests/client"
	"dist-mq/e2e-tests/harness"
	"dist-mq/model"
)

func newClient(t *testing.T, nodes []string) *client.Client {
	t.Helper()
	c, err := client.New(client.Config{
		Nodes:       nodes,
		MaxAttempts: 5,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
		HTTPTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	return c
}

func setup(t *testing.T, size int, queues []string) (*clusterState, *harness.Harness, *client.Client) {
	t.Helper()
	state, urls := newCluster(t, size)
	c := newClient(t, urls)

	h, err := harness.Setup(context.Background(), harness.Config{
		Client:      c,
		Queues:      queues,
		SinkBase:    "http://sink:9090",
		Subscribers: []string{"sub-0", "sub-1"},
		Retries:     7,
	})
	if err != nil {
		t.Fatalf("harness.Setup: %v", err)
	}
	return state, h, c
}

func TestSetupCreatesQueuesAndSubscribers(t *testing.T) {
	state, _, _ := setup(t, 3, []string{"bench-q0", "bench-q1"})

	for _, queue := range []string{"bench-q0", "bench-q1"} {
		if !state.hasQueue(queue) {
			t.Fatalf("queue %s was not created", queue)
		}
		subs := state.subs(queue)
		if len(subs) != 2 {
			t.Fatalf("queue %s has %d subscriber(s), want 2", queue, len(subs))
		}
		// The SubURL is what the broker will actually POST to, so a wrong
		// prefix here is a run that delivers nowhere and verifies nothing.
		want := model.SubPolicy{SubName: "sub-0", SubURL: "http://sink:9090/sub-0", NumberOfRetries: 7}
		if subs["sub-0"] != want {
			t.Fatalf("sub-0 policy = %+v, want %+v", subs["sub-0"], want)
		}
	}
}

// Setup runs against a cluster that may already have been set up, and refusing
// to start would make a rerun a manual cleanup exercise.
func TestSetupIsRepeatable(t *testing.T) {
	state, urls := newCluster(t, 3)
	c := newClient(t, urls)

	cfg := harness.Config{
		Client:      c,
		Queues:      []string{"bench-q0"},
		SinkBase:    "http://sink:9090",
		Subscribers: []string{"sub-0"},
		Retries:     3,
	}
	for i := 0; i < 3; i++ {
		if _, err := harness.Setup(context.Background(), cfg); err != nil {
			t.Fatalf("Setup run %d: %v", i, err)
		}
	}
	if len(state.subs("bench-q0")) != 1 {
		t.Fatalf("subscribers = %d, want 1", len(state.subs("bench-q0")))
	}
}

// The barrier a TCP readiness probe cannot give: the port answers long before
// the cluster can take a write.
func TestWaitForLeaderFindsLeaderThroughRedirect(t *testing.T) {
	state, urls := newCluster(t, 3)
	state.setLeader(nodeID(2)) // not the node the client will try first
	c := newClient(t, urls)

	h, err := harness.Setup(context.Background(), harness.Config{
		Client: c, Queues: []string{"q"}, SinkBase: "http://sink:9090",
	})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}

	leader, err := h.WaitForLeader(context.Background(), 5*time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	if leader != urls[2] {
		t.Fatalf("leader = %q, want %q", leader, urls[2])
	}
}

func TestWaitForLeaderTimesOutDuringElection(t *testing.T) {
	state, urls := newCluster(t, 3)
	c := newClient(t, urls)
	h, err := harness.Setup(context.Background(), harness.Config{
		Client: c, Queues: []string{"q"}, SinkBase: "http://sink:9090",
	})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}

	state.setLeader("") // election in progress; every node answers 503
	if _, err := h.WaitForLeader(context.Background(), 300*time.Millisecond); err == nil {
		t.Fatal("WaitForLeader returned no error while no leader existed")
	}
}

func TestWaitForLeaderRecoversAfterElection(t *testing.T) {
	state, urls := newCluster(t, 3)
	c := newClient(t, urls)
	h, err := harness.Setup(context.Background(), harness.Config{
		Client: c, Queues: []string{"q"}, SinkBase: "http://sink:9090",
	})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}

	state.setLeader("")
	go func() {
		time.Sleep(100 * time.Millisecond)
		state.setLeader(nodeID(1))
	}()

	leader, err := h.WaitForLeader(context.Background(), 5*time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	if leader != urls[1] {
		t.Fatalf("leader = %q, want %q", leader, urls[1])
	}
}

func TestOutstandingCountsOnlyHarnessQueues(t *testing.T) {
	state, h, c := setup(t, 3, []string{"bench-q0"})

	// A queue the harness does not own, which must not be counted: the probe
	// queue is exactly such a queue in every real run.
	if res := c.CreateQueue(context.Background(), "someone-elses"); res.Outcome != client.Accepted {
		t.Fatalf("creating foreign queue: %v", res.Outcome)
	}
	state.addMessage("someone-elses", model.MessageInfo{MsgID: "x"})
	state.addMessage("bench-q0", model.MessageInfo{MsgID: "a"})
	state.addMessage("bench-q0", model.MessageInfo{MsgID: "b"})

	leader, err := h.WaitForLeader(context.Background(), 5*time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	got, err := h.Outstanding(context.Background(), leader)
	if err != nil {
		t.Fatalf("Outstanding: %v", err)
	}
	if got != 2 {
		t.Fatalf("outstanding = %d, want 2", got)
	}
}

func TestWaitDrainedBlocksUntilQueueEmpties(t *testing.T) {
	state, h, _ := setup(t, 3, []string{"bench-q0"})
	state.addMessage("bench-q0", model.MessageInfo{MsgID: "a"})

	go func() {
		time.Sleep(150 * time.Millisecond)
		state.clearMessages("bench-q0")
	}()

	if err := h.WaitDrained(context.Background(), 5*time.Second); err != nil {
		t.Fatalf("WaitDrained: %v", err)
	}
}

func TestWaitDrainedFailsWhenNothingDrains(t *testing.T) {
	state, h, _ := setup(t, 3, []string{"bench-q0"})
	state.addMessage("bench-q0", model.MessageInfo{MsgID: "stuck"})

	if err := h.WaitDrained(context.Background(), 400*time.Millisecond); err == nil {
		t.Fatal("WaitDrained returned no error with a message still outstanding")
	}
}

// The probe queue is harness litter, and leaving it behind would make the next
// run's queue listing wrong.
func TestTeardownRemovesQueuesAndProbe(t *testing.T) {
	state, h, _ := setup(t, 3, []string{"bench-q0", "bench-q1"})

	if !state.hasQueue(client.ProbeQueue) {
		t.Fatal("probe queue was never created — leader discovery did not run")
	}
	if err := h.Teardown(context.Background()); err != nil {
		t.Fatalf("Teardown: %v", err)
	}

	for _, queue := range []string{"bench-q0", "bench-q1", client.ProbeQueue} {
		if state.hasQueue(queue) {
			t.Errorf("queue %s survived teardown", queue)
		}
	}
}

func TestTeardownIsRepeatable(t *testing.T) {
	_, h, _ := setup(t, 3, []string{"bench-q0"})
	for i := 0; i < 2; i++ {
		if err := h.Teardown(context.Background()); err != nil {
			t.Fatalf("Teardown run %d: %v", i, err)
		}
	}
}

// Single node and multi node have to be the same code path from here up.
func TestSingleNodeBehavesIdentically(t *testing.T) {
	state, h, _ := setup(t, 1, []string{"bench-q0"})

	leader, err := h.WaitForLeader(context.Background(), 5*time.Second)
	if err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	if leader == "" {
		t.Fatal("no leader reported on a single-node cluster")
	}
	if !state.hasQueue("bench-q0") {
		t.Fatal("queue was not created")
	}
	if err := h.Teardown(context.Background()); err != nil {
		t.Fatalf("Teardown: %v", err)
	}
}

func TestSetupRejectsBadConfig(t *testing.T) {
	_, urls := newCluster(t, 1)
	c := newClient(t, urls)

	cases := map[string]harness.Config{
		"nil client":  {Queues: []string{"q"}},
		"no queues":   {Client: c},
		"no sinkbase": {Client: c, Queues: []string{"q"}, Subscribers: []string{"sub-0"}},
	}
	for name, cfg := range cases {
		if _, err := harness.Setup(context.Background(), cfg); err == nil {
			t.Errorf("%s: Setup returned no error", name)
		}
	}
}

func TestDiscoverRejectsEmptyService(t *testing.T) {
	if _, err := harness.Discover(context.Background(), "  ", 8080); err == nil {
		t.Fatal("Discover returned no error for an empty service name")
	}
}
