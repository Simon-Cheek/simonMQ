// Package client is the cluster-aware HTTP client the e2e harness drives
// dist-mq with. It is what makes every test above it indifferent to cluster
// size: against one node the leader is whichever node you called, against five
// it is found by following a 421, and callers write the same code either way.
//
// The outcome classification matters more than the retrying. Under chaos a
// write can fail in two very different ways — one where the request provably
// never reached raft, and one where it may have committed before the answer
// was lost — and a durability assertion that treats those alike is either
// vacuous or wrong. See Outcome.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"dist-mq/model"
	"dist-mq/server"
)

// Outcome is what a caller is allowed to conclude about a write.
type Outcome int

const (
	// Accepted: the cluster returned 2xx, so the entry is on a quorum of disks.
	// A durability test may assert delivery for these and nothing else.
	Accepted Outcome = iota

	// Rejected: the cluster answered, and the answer was no (404, 409, ...).
	// Nothing was committed and retrying would not change that.
	Rejected

	// Ambiguous: every attempt ended without an answer, after the request had
	// gone out. It may have committed. Assertions must stay silent on these —
	// counting one as lost invents a durability bug, counting one as delivered
	// hides a real one.
	Ambiguous
)

func (o Outcome) String() string {
	switch o {
	case Accepted:
		return "accepted"
	case Rejected:
		return "rejected"
	case Ambiguous:
		return "ambiguous"
	}
	return "unknown"
}

type Result struct {
	Outcome  Outcome
	Status   int // 0 when no response was ever received
	Attempts int
	Node     string // whoever answered, or was tried last
	Body     []byte
	Err      error
}

type Config struct {
	// Nodes are the HTTP base URLs of every node, e.g.
	// http://dist-mq-0.dist-mq-raft.dist-mq.svc.cluster.local:8080
	Nodes []string

	// MaxAttempts bounds one logical write. Benchmarks want this low: an
	// open-loop generator that retries is quietly offering more load than it
	// reports. Durability runs want it generous.
	MaxAttempts int

	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	HTTPTimeout time.Duration

	// MaxConns sizes the connection pool. The default transport keeps two idle
	// connections per host, which under load measures TCP handshakes.
	MaxConns int
}

type Client struct {
	cfg    Config
	http   *http.Client
	rand   *rand.Rand
	randMu sync.Mutex

	// leader is the cached redirect target. Caching is what keeps the redirect
	// at one extra hop per leadership term rather than one per request, which
	// is the difference between a benchmark measuring dist-mq and measuring
	// dist-mq plus a wasted round trip.
	leader atomic.Pointer[string]
	next   atomic.Uint64
}

func New(cfg Config) (*Client, error) {
	if len(cfg.Nodes) == 0 {
		return nil, errors.New("client: no nodes configured")
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 5
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 25 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 2 * time.Second
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 10 * time.Second
	}
	if cfg.MaxConns < 64 {
		cfg.MaxConns = 64
	}

	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = cfg.MaxConns
	t.MaxIdleConnsPerHost = cfg.MaxConns
	t.IdleConnTimeout = 90 * time.Second

	return &Client{
		cfg:  cfg,
		http: &http.Client{Transport: t, Timeout: cfg.HTTPTimeout},
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

func (c *Client) Nodes() []string { return c.cfg.Nodes }

// Leader reports the cached leader, which is only ever learned from a redirect
// and so is empty until the first write.
func (c *Client) Leader() string {
	if p := c.leader.Load(); p != nil {
		return *p
	}
	return ""
}

func (c *Client) setLeader(url string) { c.leader.Store(&url) }

func (c *Client) clearLeader() { c.leader.Store(nil) }

// --- write path ------------------------------------------------------------

func (c *Client) CreateQueue(ctx context.Context, queue string) Result {
	return c.Write(ctx, http.MethodPost, "/queues/"+queue, nil)
}

func (c *Client) DeleteQueue(ctx context.Context, queue string) Result {
	return c.Write(ctx, http.MethodDelete, "/queues/"+queue, nil)
}

func (c *Client) PutSubPolicy(ctx context.Context, queue string, p model.SubPolicy) Result {
	body, err := model.EncodeSubPolicy(p)
	if err != nil {
		return Result{Outcome: Rejected, Err: err}
	}
	return c.Write(ctx, http.MethodPost, "/queues/"+queue+"/subscribers", body)
}

func (c *Client) Enqueue(ctx context.Context, queue string, payload []byte) Result {
	return c.Write(ctx, http.MethodPost, "/queues/"+queue+"/messages", payload)
}

// Write drives one logical write to completion, following redirects and
// retrying elections. The node it starts on is the cached leader if there is
// one and an arbitrary node otherwise.
func (c *Client) Write(ctx context.Context, method, path string, body []byte) Result {
	target := c.Leader()
	if target == "" {
		target = c.cfg.Nodes[int(c.next.Add(1)-1)%len(c.cfg.Nodes)]
	}

	res := Result{Outcome: Ambiguous}
	// mayHaveCommitted tracks whether any attempt could have reached raft. A
	// 421 is not one of those: requireLeader rejects before the handler runs,
	// so nothing was proposed. Only a lost answer or a 503 qualifies, and the
	// 503 does because writeError returns one when a propose fails after
	// leadership moved — that entry may still commit in the new term.
	mayHaveCommitted := false

	for attempt := 1; attempt <= c.cfg.MaxAttempts; attempt++ {
		res.Attempts = attempt
		res.Node = target

		status, hdr, respBody, err := c.send(ctx, method, target, path, body)
		if err != nil {
			res.Err = err
			if dialFailed(err) {
				// Never left this machine. Another node may still be leader,
				// and this one is plainly down, so stop favouring it.
				if target == c.Leader() {
					c.clearLeader()
				}
				target = c.otherThan(target)
				// Backing off matters most when there is nowhere else to go:
				// a single-node cluster restarting is reachable again in a
				// second or two, and spinning through the attempt budget in
				// microseconds would reject writes that had only to wait.
				if err := c.pause(ctx, attempt); err != nil {
					break
				}
				continue
			}
			// Sent, then lost: a timeout or a reset mid-flight. This is the
			// case that makes a write ambiguous rather than failed.
			mayHaveCommitted = true
			c.clearLeader()
			if err := c.pause(ctx, attempt); err != nil {
				break
			}
			target = c.otherThan(target)
			continue
		}

		res.Status = status
		res.Body = respBody
		res.Err = nil

		switch {
		case status >= 200 && status < 300:
			// Only a leader answers 2xx on this path, so this is also how the
			// cache is populated when the first node tried happened to be it.
			c.setLeader(target)
			res.Outcome = Accepted
			return res

		case status == http.StatusMisdirectedRequest: // 421
			if leader := hdr.Get(server.LeaderHeader); leader != "" {
				c.setLeader(leader)
				target = leader
				continue
			}
			// A leader exists but this node cannot name it. Sweeping is the
			// documented fallback and costs one pass per leadership term.
			c.clearLeader()
			target = c.otherThan(target)
			continue

		case status == http.StatusServiceUnavailable: // 503
			// An election, or leadership lost between the middleware check and
			// the propose landing. The contract says back off here rather than
			// shop around: no other node will take the write either.
			//
			// The second case is why this counts as maybe-committed: the
			// propose may already have been replicated when leadership moved.
			mayHaveCommitted = true
			c.clearLeader()
			if err := c.pause(ctx, attempt); err != nil {
				return c.finish(res, mayHaveCommitted)
			}
			continue

		default:
			res.Outcome = Rejected
			return res
		}
	}

	return c.finish(res, mayHaveCommitted)
}

// finish classifies a write that ran out of attempts. Nothing that could have
// reached raft means a clean rejection; anything that might have is not.
func (c *Client) finish(res Result, mayHaveCommitted bool) Result {
	if mayHaveCommitted {
		res.Outcome = Ambiguous
	} else {
		res.Outcome = Rejected
	}
	if res.Err == nil {
		res.Err = fmt.Errorf("gave up after %d attempt(s), last status %d", res.Attempts, res.Status)
	}
	return res
}

// --- leadership ------------------------------------------------------------

// ProbeQueue is the queue ProbeLeader writes to. Creating it is a no-op after
// the first time, so repeated probes change nothing observable.
const ProbeQueue = "__dist-mq-probe__"

// Probe is one node's answer to "who is in charge".
type Probe struct {
	HasLeader bool
	IsLeader  bool   // the probed node is itself the leader
	Leader    string // base URL, empty when the answering node cannot name it
}

// ProbeLeader asks one node who the leader is. The server exposes no read-only
// way to ask, so this attempts a write and reads the rejection instead.
//
// On a follower it is free: requireLeader answers 421 before the handler runs,
// so nothing reaches raft. On the leader it is a real proposal — hence the
// dedicated queue, whose creation is a no-op once it exists — and callers
// should cache the answer rather than poll tightly.
func (c *Client) ProbeLeader(ctx context.Context, node string) (Probe, error) {
	status, hdr, _, err := c.send(ctx, http.MethodPost, node, "/queues/"+ProbeQueue, nil)
	if err != nil {
		return Probe{}, err
	}

	switch {
	case status == http.StatusServiceUnavailable:
		return Probe{}, nil // election in progress; nobody is in charge
	case status == http.StatusMisdirectedRequest:
		return Probe{HasLeader: true, Leader: hdr.Get(server.LeaderHeader)}, nil
	default:
		// Anything the handler itself produced — 201, or 409 because the probe
		// queue is already there — means this node took the write.
		return Probe{HasLeader: true, IsLeader: true, Leader: node}, nil
	}
}

// --- read path -------------------------------------------------------------

// ListQueues reads from a given node, or any node when base is empty. Reads
// are explicitly not linearizable: a follower answers with whatever it has
// applied, which is the point of asking a specific node.
func (c *Client) ListQueues(ctx context.Context, base string) ([]model.QueueInfo, error) {
	if base == "" {
		base = c.cfg.Nodes[int(c.next.Add(1)-1)%len(c.cfg.Nodes)]
	}
	status, _, body, err := c.send(ctx, http.MethodGet, base, "/queues", nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("GET %s/queues: status %d", base, status)
	}
	var out []model.QueueInfo
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("GET %s/queues: %w", base, err)
	}
	return out, nil
}

// --- plumbing --------------------------------------------------------------

func (c *Client) send(ctx context.Context, method, base, path string, body []byte) (int, http.Header, []byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, rdr)
	if err != nil {
		return 0, nil, nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()

	// Drained rather than abandoned so the connection returns to the pool.
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, resp.Header, nil, err
	}
	return resp.StatusCode, resp.Header, respBody, nil
}

// otherThan advances to a different node, so a wedged one is not retried in
// place. With a single node there is nowhere else to go, which is correct:
// that node is the whole cluster.
func (c *Client) otherThan(current string) string {
	if len(c.cfg.Nodes) == 1 {
		return c.cfg.Nodes[0]
	}
	for i := 0; i < len(c.cfg.Nodes); i++ {
		next := c.cfg.Nodes[int(c.next.Add(1)-1)%len(c.cfg.Nodes)]
		if next != current {
			return next
		}
	}
	return current
}

// pause is exponential with full jitter. The server's Retry-After is a flat
// one second, which is an order of magnitude longer than an uncontested
// election and would dominate the run.
func (c *Client) pause(ctx context.Context, attempt int) error {
	backoff := c.cfg.BaseBackoff << (attempt - 1)
	if backoff > c.cfg.MaxBackoff || backoff <= 0 {
		backoff = c.cfg.MaxBackoff
	}

	c.randMu.Lock()
	d := time.Duration(c.rand.Int63n(int64(backoff) + 1))
	c.randMu.Unlock()

	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// dialFailed reports whether the request provably never went out. A refused or
// unreachable dial is the pod being gone; anything later — a timeout, a reset —
// leaves open the possibility that raft committed and only the answer was lost.
func dialFailed(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Op == "dial"
	}
	return false
}
