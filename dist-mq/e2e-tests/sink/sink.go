// Package sink is the delivery endpoint dist-mq pushes to during e2e runs.
//
// One process serves every subscriber in a run, distinguished by a path prefix
// rather than by a port. dist-mq appends delivery.DeliveryPath to whatever
// SubURL a policy carries, so a SubURL of http://sink:9090/sub-0 arrives here
// as /sub-0/queue/message. That keeps the whole fan-out behind a single
// Service port, and keeps verification reading from one place instead of
// stitching together per-pod tallies.
//
// It always answers 2xx. That is a correctness requirement, not laziness: a
// subscriber that exhausts its retries is recorded as settled even though it
// never received anything, which makes "settled but undelivered" and "lost by
// the cluster" indistinguishable from the outside. A sink that never fails is
// what makes accepted-implies-delivered a sound assertion.
package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"dist-mq/e2e-tests/payload"
)

// maxBody caps a single delivery. Payload size is a benchmark knob and this is
// only here so a misdirected request cannot exhaust memory.
const maxBody = 4 << 20

type Mode string

const (
	// ModeCount tallies and discards. Benchmarks use this: retaining every
	// token would make the sink's allocator part of the measurement.
	ModeCount Mode = "count"

	// ModeRecord remembers which tokens arrived, which is what durability
	// verification reads.
	ModeRecord Mode = "record"
)

type Config struct {
	// Addr is the listen address. ":0" picks a free port, which is what
	// in-process tests want.
	Addr string

	// Subscribers are the names this sink answers for. A delivery to any other
	// prefix is refused rather than silently recorded, so a mistyped SubURL
	// fails loudly instead of producing a run that verifies nothing.
	Subscribers []string

	Mode Mode
}

type subscriber struct {
	name      string
	delivered atomic.Uint64

	mu         sync.Mutex
	seen       map[string]int // nil in count mode
	duplicates uint64
}

func (s *subscriber) record(mode Mode, body []byte) {
	s.delivered.Add(1)
	if mode != ModeRecord {
		return
	}

	token := payload.Token(body)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen[token]++
	if s.seen[token] > 1 {
		s.duplicates++
	}
}

func (s *subscriber) stats() SubStats {
	st := SubStats{Name: s.name, Delivered: s.delivered.Load()}
	s.mu.Lock()
	defer s.mu.Unlock()
	st.Unique = uint64(len(s.seen))
	st.Duplicates = s.duplicates
	return st
}

func (s *subscriber) reset() {
	s.delivered.Store(0)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen != nil {
		s.seen = make(map[string]int)
	}
	s.duplicates = 0
}

type SubStats struct {
	Name      string `json:"Name"`
	Delivered uint64 `json:"Delivered"` // every POST, duplicates included
	Unique    uint64 `json:"Unique"`    // distinct tokens; 0 in count mode
	// Duplicates is reported, never failed on. Delivery is at-least-once and a
	// leadership change is entitled to produce them.
	Duplicates uint64 `json:"Duplicates"`
}

type Stats struct {
	Mode        Mode       `json:"Mode"`
	Delivered   uint64     `json:"Delivered"`
	Unknown     uint64     `json:"Unknown"` // deliveries to an unconfigured prefix
	Subscribers []SubStats `json:"Subscribers"`
}

// Records is the delivery evidence a durability run verifies against: for each
// subscriber, every token it saw and how many times.
type Records struct {
	Mode        Mode                      `json:"Mode"`
	Subscribers map[string]map[string]int `json:"Subscribers"`
}

type Server struct {
	mode    Mode
	subs    map[string]*subscriber
	order   []string
	unknown atomic.Uint64

	ln   net.Listener
	srv  *http.Server
	done chan struct{}
}

func New(cfg Config) (*Server, error) {
	if len(cfg.Subscribers) == 0 {
		return nil, errors.New("sink: no subscribers configured")
	}
	if cfg.Mode == "" {
		cfg.Mode = ModeCount
	}
	if cfg.Mode != ModeCount && cfg.Mode != ModeRecord {
		return nil, fmt.Errorf("sink: unknown mode %q", cfg.Mode)
	}

	s := &Server{
		mode: cfg.Mode,
		subs: make(map[string]*subscriber, len(cfg.Subscribers)),
		done: make(chan struct{}),
	}
	for _, name := range cfg.Subscribers {
		if name == "" {
			return nil, errors.New("sink: empty subscriber name")
		}
		if _, dup := s.subs[name]; dup {
			return nil, fmt.Errorf("sink: duplicate subscriber %q", name)
		}
		sub := &subscriber{name: name}
		if cfg.Mode == ModeRecord {
			sub.seen = make(map[string]int)
		}
		s.subs[name] = sub
		s.order = append(s.order, name)
	}

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("sink: listen on %q: %w", cfg.Addr, err)
	}
	s.ln = ln
	s.srv = &http.Server{Handler: s.routes()}
	return s, nil
}

// Addr is the resolved listen address, which matters when Config.Addr asked
// for port 0.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// SubURL is what belongs in a SubPolicy for the named subscriber. base is the
// address the cluster reaches this process on, which is not necessarily the
// address it bound.
func (s *Server) SubURL(base, name string) string { return base + "/" + name }

func (s *Server) Serve() error {
	defer close(s.done)
	if err := s.srv.Serve(s.ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	err := s.srv.Shutdown(ctx)
	select {
	case <-s.done:
	case <-time.After(time.Second):
	}
	return err
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /{sub}/queue/message", s.handleDelivery)
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.HandleFunc("GET /records", s.handleRecords)
	mux.HandleFunc("POST /reset", s.handleReset)
	return mux
}

func (s *Server) handleDelivery(w http.ResponseWriter, r *http.Request) {
	sub, ok := s.subs[r.PathValue("sub")]
	if !ok {
		s.unknown.Add(1)
		http.Error(w, "unknown subscriber", http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	sub.record(s.mode, body)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	st := Stats{Mode: s.mode, Unknown: s.unknown.Load()}
	for _, name := range s.order {
		sub := s.subs[name].stats()
		st.Delivered += sub.Delivered
		st.Subscribers = append(st.Subscribers, sub)
	}
	writeJSON(w, st)
}

func (s *Server) handleRecords(w http.ResponseWriter, r *http.Request) {
	rec := Records{Mode: s.mode, Subscribers: make(map[string]map[string]int, len(s.subs))}
	for _, name := range s.order {
		sub := s.subs[name]
		sub.mu.Lock()
		seen := make(map[string]int, len(sub.seen))
		for token, n := range sub.seen {
			seen[token] = n
		}
		sub.mu.Unlock()
		rec.Subscribers[name] = seen
	}
	writeJSON(w, rec)
}

// handleReset zeroes every tally. One sink process serves many repetitions of
// a benchmark, and a run has to be able to say what it alone delivered.
func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	for _, sub := range s.subs {
		sub.reset()
	}
	s.unknown.Store(0)
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// Names returns the configured subscriber names in order.
func Names(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("sub-%d", i)
	}
	return names
}

// SortedTokens is a convenience for tests comparing record sets.
func SortedTokens(seen map[string]int) []string {
	out := make([]string, 0, len(seen))
	for token := range seen {
		out = append(out, token)
	}
	sort.Strings(out)
	return out
}
