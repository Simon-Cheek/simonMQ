// Package harness builds and dismantles the state an e2e run needs: queues,
// subscriber policies pointing at the sink, and the barriers that say when the
// cluster is actually ready to be measured or asserted against.
//
// The barriers are the reason this exists as a package rather than a few lines
// in each test. A dist-mq node serves HTTP well before it has a leader, so
// "the port answers" and "the cluster accepts writes" are different facts, and
// a test that confuses them fails intermittently in a way that looks like a
// durability bug.
package harness

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"dist-mq/e2e-tests/client"
	"dist-mq/model"
)

const (
	pollInterval    = 500 * time.Millisecond
	defaultDeadline = 60 * time.Second
)

type Config struct {
	Client *client.Client

	// Queues to create. One queue is enough to assert durability; several are
	// what make a benchmark's per-queue workers do any work.
	Queues []string

	// SinkBase is the address the cluster reaches the sink on — an in-cluster
	// Service URL, not wherever the test process happens to be running.
	SinkBase string

	// Subscribers are registered on every queue, each pointed at its own path
	// prefix under SinkBase.
	Subscribers []string

	// Retries per subscriber. Durability runs want this high: the sink never
	// fails, so the only way to exhaust retries is a delivery the cluster
	// could not complete, and giving up would record a settled message that
	// was never delivered.
	Retries int
}

type Harness struct {
	cfg Config
}

func Setup(ctx context.Context, cfg Config) (*Harness, error) {
	if cfg.Client == nil {
		return nil, errors.New("harness: nil client")
	}
	if len(cfg.Queues) == 0 {
		return nil, errors.New("harness: no queues")
	}
	if len(cfg.Subscribers) > 0 && cfg.SinkBase == "" {
		return nil, errors.New("harness: subscribers configured with no sink base URL")
	}
	if cfg.Retries <= 0 {
		cfg.Retries = 10
	}

	h := &Harness{cfg: cfg}
	if _, err := h.WaitForLeader(ctx, defaultDeadline); err != nil {
		return nil, fmt.Errorf("harness: setup: %w", err)
	}

	for _, queue := range cfg.Queues {
		if err := h.createQueue(ctx, queue); err != nil {
			return nil, err
		}
		for _, sub := range cfg.Subscribers {
			policy := model.SubPolicy{
				SubName:         sub,
				SubURL:          cfg.SinkBase + "/" + sub,
				NumberOfRetries: cfg.Retries,
			}
			res := cfg.Client.PutSubPolicy(ctx, queue, policy)
			if res.Outcome != client.Accepted {
				return nil, fmt.Errorf("harness: registering %s on %s: %v (status %d): %w",
					sub, queue, res.Outcome, res.Status, res.Err)
			}
		}
	}
	return h, nil
}

func (h *Harness) createQueue(ctx context.Context, queue string) error {
	res := h.cfg.Client.CreateQueue(ctx, queue)
	// A rerun against a live cluster is normal, so an existing queue is a
	// success rather than a reason to refuse to start.
	if res.Outcome == client.Accepted || res.Status == http.StatusConflict {
		return nil
	}
	return fmt.Errorf("harness: creating queue %s: %v (status %d): %w",
		queue, res.Outcome, res.Status, res.Err)
}

// Teardown removes what Setup made. Failures are collected rather than
// returned early: a half-deleted cluster is worse than a fully attempted one.
func (h *Harness) Teardown(ctx context.Context) error {
	var errs []error
	for _, queue := range append(append([]string{}, h.cfg.Queues...), client.ProbeQueue) {
		res := h.cfg.Client.DeleteQueue(ctx, queue)
		if res.Outcome != client.Accepted && res.Status != http.StatusNotFound {
			errs = append(errs, fmt.Errorf("deleting queue %s: %v (status %d)", queue, res.Outcome, res.Status))
		}
	}
	return errors.Join(errs...)
}

// WaitForLeader blocks until some node reports a leader, and returns its base
// URL. This is the barrier a TCP readiness probe cannot provide.
//
// Nodes are swept rather than asked in parallel because a follower answers for
// free while the leader answers with a raft proposal, so the cheap answer is
// worth finding first.
func (h *Harness) WaitForLeader(ctx context.Context, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for {
		for _, node := range h.cfg.Client.Nodes() {
			probe, err := h.cfg.Client.ProbeLeader(ctx, node)
			if err != nil {
				lastErr = err // a down node is expected under chaos
				continue
			}
			if probe.HasLeader && probe.Leader != "" {
				return probe.Leader, nil
			}
			// A leader exists that this node cannot name. Keep sweeping: some
			// other node, or the leader itself, will identify it.
			if probe.HasLeader {
				lastErr = fmt.Errorf("leader exists but %s cannot name it", node)
			}
		}

		if time.Now().After(deadline) {
			if lastErr != nil {
				return "", fmt.Errorf("no leader within %s: %w", timeout, lastErr)
			}
			return "", fmt.Errorf("no leader within %s", timeout)
		}
		if err := sleep(ctx, pollInterval); err != nil {
			return "", err
		}
	}
}

// Outstanding counts messages still owed a delivery across the harness's
// queues. Reads are not linearizable, so this is a lower bound from one node's
// point of view — which is why it is a drain signal and never an assertion.
func (h *Harness) Outstanding(ctx context.Context, node string) (int, error) {
	queues, err := h.cfg.Client.ListQueues(ctx, node)
	if err != nil {
		return 0, err
	}

	wanted := make(map[string]struct{}, len(h.cfg.Queues))
	for _, q := range h.cfg.Queues {
		wanted[q] = struct{}{}
	}

	total := 0
	for _, q := range queues {
		if _, ok := wanted[q.Name]; ok {
			total += len(q.Messages)
		}
	}
	return total, nil
}

// WaitDrained blocks until no message in the harness's queues is still owed a
// delivery. It asks the leader, since a follower can be behind and report an
// empty queue the leader is still working through.
func (h *Harness) WaitDrained(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last int

	for {
		leader, err := h.WaitForLeader(ctx, time.Until(deadline))
		if err != nil {
			return fmt.Errorf("harness: draining: %w", err)
		}

		outstanding, err := h.Outstanding(ctx, leader)
		if err == nil {
			if outstanding == 0 {
				return nil
			}
			last = outstanding
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("harness: %d message(s) still outstanding after %s", last, timeout)
		}
		if err := sleep(ctx, pollInterval); err != nil {
			return err
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
