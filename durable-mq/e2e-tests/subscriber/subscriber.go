// Command subscriber is the delivery sink the broker pushes messages to.
//
// It runs in one of two modes:
//
//   - count (default): messages are tallied in memory and discarded. This is
//     the mode benchmarks use. The old behaviour — open, write, close the
//     output file on every single message — made the sink itself the
//     bottleneck long before the broker was, so a perf run measured this
//     program's syscall rate rather than durable-mq's.
//   - record: message bodies are appended to a file for the correctness runs
//     that `verify` consumes. Still buffered and flushed periodically rather
//     than reopening the file per message.
//
// One process can serve several subscriber endpoints on consecutive ports
// (-n), so a fan-out benchmark doesn't need N processes babysat by the
// orchestrator. Every endpoint also serves GET /stats, which reports the
// tallies for all endpoints in the process.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const flushInterval = 200 * time.Millisecond

type endpoint struct {
	Port  int
	count atomic.Uint64
	rec   *recorder // nil in count mode
}

// recorder appends message bodies to a file behind a buffer. The buffer is
// flushed on a timer and at shutdown; a hard kill can lose the last
// flushInterval of lines, which is fine because the process under test in a
// crash run is the broker, never this one.
type recorder struct {
	mu   sync.Mutex
	f    *os.File
	buf  []byte
	path string
}

func newRecorder(path string) (*recorder, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &recorder{f: f, path: path}, nil
}

func (r *recorder) write(body []byte) {
	r.mu.Lock()
	r.buf = append(r.buf, body...)
	r.buf = append(r.buf, '\n')
	r.mu.Unlock()
}

func (r *recorder) flush() error {
	r.mu.Lock()
	if len(r.buf) == 0 {
		r.mu.Unlock()
		return nil
	}
	out := r.buf
	r.buf = nil
	r.mu.Unlock()

	if _, err := r.f.Write(out); err != nil {
		return err
	}
	return nil
}

func (r *recorder) close() error {
	if err := r.flush(); err != nil {
		r.f.Close()
		return err
	}
	return r.f.Close()
}

func main() {
	basePort := flag.Int("base-port", 9090, "first port to listen on")
	legacyPort := flag.String("p", "", "alias for -base-port (kept for existing scripts)")
	n := flag.Int("n", 1, "number of subscriber endpoints, on consecutive ports from -base-port")
	out := flag.String("o", "", "record message bodies to this file (implies -mode record)")
	mode := flag.String("mode", "count", "count (tally in memory) or record (append bodies to -o)")
	flag.Parse()

	port := *basePort
	if *legacyPort != "" {
		if _, err := fmt.Sscanf(*legacyPort, "%d", &port); err != nil {
			log.Fatalf("invalid -p: %v", err)
		}
	}
	// -o is what the old invocation passed, so honour it as a mode switch
	// rather than making existing scripts learn a new flag.
	if *out != "" {
		*mode = "record"
	}
	if *mode != "count" && *mode != "record" {
		log.Fatalf("invalid -mode %q (want count or record)", *mode)
	}
	if *mode == "record" && *out == "" {
		log.Fatal("-mode record requires -o")
	}
	if *n < 1 {
		log.Fatal("-n must be at least 1")
	}

	eps := make([]*endpoint, *n)
	for i := range eps {
		ep := &endpoint{Port: port + i}
		if *mode == "record" {
			rec, err := newRecorder(recordPath(*out, ep.Port, *n))
			if err != nil {
				log.Fatalf("opening record file: %v", err)
			}
			ep.rec = rec
		}
		eps[i] = ep
	}

	servers := make([]*http.Server, len(eps))
	for i, ep := range eps {
		mux := http.NewServeMux()
		mux.HandleFunc("POST /queue/message", ep.handleMessage)
		mux.HandleFunc("GET /stats", statsHandler(eps))
		mux.HandleFunc("POST /stats/reset", resetHandler(eps))
		srv := &http.Server{Addr: fmt.Sprintf(":%d", ep.Port), Handler: mux}
		servers[i] = srv

		go func(srv *http.Server, port int) {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("listener on :%d: %v", port, err)
			}
		}(srv, ep.Port)
	}

	log.Printf("subscriber sink: mode=%s endpoints=%d ports=%d-%d",
		*mode, len(eps), eps[0].Port, eps[len(eps)-1].Port)

	stop := make(chan struct{})
	if *mode == "record" {
		go func() {
			t := time.NewTicker(flushInterval)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					for _, ep := range eps {
						if err := ep.rec.flush(); err != nil {
							log.Printf("flush %s: %v", ep.rec.path, err)
						}
					}
				case <-stop:
					return
				}
			}
		}()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	<-ctx.Done()
	close(stop)

	for _, srv := range servers {
		srv.Close()
	}
	total := uint64(0)
	for _, ep := range eps {
		total += ep.count.Load()
		if ep.rec != nil {
			if err := ep.rec.close(); err != nil {
				log.Printf("closing %s: %v", ep.rec.path, err)
			}
		}
	}
	log.Printf("shutting down, received %d messages total", total)
}

func (e *endpoint) handleMessage(w http.ResponseWriter, r *http.Request) {
	if e.rec == nil {
		// Drain and discard. The body still has to be read so the connection
		// can be reused; not reading it would force the broker to open a new
		// one per message and quietly change what we're measuring.
		io.Copy(io.Discard, r.Body)
	} else {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		e.rec.write(body)
	}
	e.count.Add(1)
	w.WriteHeader(http.StatusAccepted)
}

type statsResponse struct {
	Total     uint64            `json:"Total"`
	ByPort    map[string]uint64 `json:"ByPort"`
	Timestamp time.Time         `json:"Timestamp"`
}

func statsHandler(eps []*endpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp := statsResponse{ByPort: make(map[string]uint64, len(eps)), Timestamp: time.Now()}
		for _, ep := range eps {
			c := ep.count.Load()
			resp.ByPort[fmt.Sprintf("%d", ep.Port)] = c
			resp.Total += c
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

// resetHandler zeroes the tallies so one long-lived sink can serve many
// benchmark repetitions without a restart between them.
func resetHandler(eps []*endpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		for _, ep := range eps {
			ep.count.Store(0)
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// recordPath keeps a single-endpoint run writing exactly to -o, and
// disambiguates by port only when there is more than one endpoint.
func recordPath(out string, port, n int) string {
	if n == 1 {
		return out
	}
	ext := filepath.Ext(out)
	return fmt.Sprintf("%s-%d%s", strings.TrimSuffix(out, ext), port, ext)
}
