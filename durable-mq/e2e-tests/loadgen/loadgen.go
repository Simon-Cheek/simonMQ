// Command loadgen drives one benchmark run against a broker and writes the
// result out as JSON.
//
// It is open-loop on purpose. A closed-loop generator — send, wait for the
// response, send again — cannot offer more load than the broker will take, so
// when the broker stalls the generator politely stalls with it and the
// recorded latencies stay flat. That hides exactly the behaviour a queue
// benchmark exists to expose. Here every publish has a fixed scheduled time
// derived from the target rate, and latency is measured from that scheduled
// time rather than from when the request actually went out. If the broker
// falls behind, the queueing delay lands in the numbers where it belongs.
// (This is the "coordinated omission" correction.)
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {
	broker := flag.String("broker", "http://localhost:8081", "base URL of the broker under test")
	sink := flag.String("sink", "http://localhost:9090", "base URL of the first subscriber endpoint; further endpoints are consecutive ports")
	arm := flag.String("arm", "unnamed", "name of the configuration being measured, e.g. durable-sync")
	rep := flag.Int("rep", 0, "repetition number for this arm")
	outPath := flag.String("out", "", "write the result JSON here (default stdout)")

	queues := flag.Int("queues", 3, "number of queues")
	pubsPerQueue := flag.Int("publishers-per-queue", 1, "concurrent publishers per queue")
	subsPerQueue := flag.Int("subscribers-per-queue", 1, "subscribers registered on each queue (fan-out)")
	rate := flag.Float64("rate", 200, "target publishes per second per publisher")
	payload := flag.Int("payload", 64, "message payload size in bytes")
	retries := flag.Int("retries", 3, "NumberOfRetries on each registered subscriber")

	duration := flag.Duration("duration", 30*time.Second, "total run length including warm-up")
	warmup := flag.Duration("warmup", 5*time.Second, "leading period excluded from the results")
	maxInFlight := flag.Int("max-in-flight", 512, "per-publisher cap on outstanding requests")
	flag.Parse()

	if *warmup >= *duration {
		log.Fatalf("-warmup (%s) must be shorter than -duration (%s)", *warmup, *duration)
	}
	if *queues < 1 || *pubsPerQueue < 1 || *subsPerQueue < 1 {
		log.Fatal("-queues, -publishers-per-queue and -subscribers-per-queue must all be at least 1")
	}

	client := newClient(*queues * *pubsPerQueue * *maxInFlight)

	queueNames := make([]string, *queues)
	for i := range queueNames {
		queueNames[i] = fmt.Sprintf("bench-q%d", i)
	}

	if err := setup(client, *broker, *sink, queueNames, *subsPerQueue, *retries); err != nil {
		log.Fatalf("setup: %v", err)
	}
	// Zero the sink's tallies after setup so the delivered count covers this
	// run only, even when one sink process serves many repetitions.
	resetSink(client, *sink, *subsPerQueue)

	body := bytes.Repeat([]byte("x"), *payload)
	start := time.Now()

	var wg sync.WaitGroup
	perPub := make([][]sample, *queues**pubsPerQueue)
	for qi, qName := range queueNames {
		for pi := 0; pi < *pubsPerQueue; pi++ {
			idx := qi**pubsPerQueue + pi
			wg.Add(1)
			go func(idx int, qName string) {
				defer wg.Done()
				perPub[idx] = publish(client, *broker, qName, body, start, *rate, *duration, *maxInFlight)
			}(idx, qName)
		}
	}

	// Snapshot the sink at the warm-up boundary so the delivered count covers
	// the same window the accepted count does. Without this, delivered spans
	// the whole run while accepted skips the warm-up, and every arm reports a
	// delivered/accepted ratio inflated by exactly duration/(duration-warmup).
	if d := time.Until(start.Add(*warmup)); d > 0 {
		time.Sleep(d)
	}
	deliveredAtWarmup, _ := sinkTotal(client, *sink)

	wg.Wait()

	var samples []sample
	for _, s := range perPub {
		samples = append(samples, s...)
	}

	sum := summarize(samples, *warmup, *duration)
	host, _ := os.Hostname()

	deliveredAtEnd, deliveredKnown := sinkTotal(client, *sink)
	var delivered uint64
	if deliveredAtEnd >= deliveredAtWarmup {
		delivered = deliveredAtEnd - deliveredAtWarmup
	}

	res := Result{
		Arm:                 *arm,
		Rep:                 *rep,
		Host:                host,
		StartedAt:           start,
		Broker:              *broker,
		Queues:              *queues,
		PublishersPerQueue:  *pubsPerQueue,
		SubscribersPerQueue: *subsPerQueue,
		TargetRatePerPub:    *rate,
		OfferedRate:         *rate * float64(*queues**pubsPerQueue),
		PayloadBytes:        *payload,
		DurationSec:         duration.Seconds(),
		WarmupSec:           warmup.Seconds(),
		Attempted:           sum.Attempted,
		Accepted:            sum.Accepted,
		Failed:              sum.Attempted - sum.Accepted,
		CompletedInWindow:   sum.CompletedInWindow,
		AcceptedPerSec:      sum.AcceptedPerSec,
		ElapsedSec:          sum.ElapsedSec,
		Latency:             sum.Latency,
		Delivered:           delivered,
		DeliveredKnown:      deliveredKnown,
	}

	out, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		log.Fatalf("encoding result: %v", err)
	}
	if *outPath == "" {
		fmt.Println(string(out))
	} else if err := os.WriteFile(*outPath, out, 0644); err != nil {
		log.Fatalf("writing %s: %v", *outPath, err)
	}

	drain := ""
	if res.ElapsedSec > res.DurationSec*1.05 {
		drain = fmt.Sprintf(", drained for %.1fs past the run", res.ElapsedSec-res.DurationSec)
	}
	log.Printf("%s rep=%d: %.0f msg/s completed (offered %.0f), p50=%.2fms p99=%.2fms, failed=%d%s",
		*arm, *rep, res.AcceptedPerSec, res.OfferedRate, sum.Latency.P50, sum.Latency.P99, res.Failed, drain)
}

// newClient returns a client with a connection pool large enough for the
// whole run. The default transport keeps only 2 idle connections per host,
// which would make the generator reconnect constantly and measure TCP
// handshakes instead of the broker.
func newClient(conns int) *http.Client {
	if conns < 64 {
		conns = 64
	}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = conns
	t.MaxIdleConnsPerHost = conns
	t.MaxConnsPerHost = 0
	t.IdleConnTimeout = 90 * time.Second
	return &http.Client{Transport: t, Timeout: 30 * time.Second}
}

// publish runs one publisher against one queue for the whole duration and
// returns its samples. Sends are issued from a bounded pool so a stalled
// broker cannot grow goroutines without limit; when the pool is full the
// publisher blocks, falls behind its schedule, and the resulting delay shows
// up in the latencies rather than being silently skipped.
func publish(client *http.Client, broker, queue string, body []byte,
	start time.Time, rate float64, duration time.Duration, maxInFlight int) []sample {

	url := fmt.Sprintf("%s/queues/%s/messages", broker, queue)
	interval := time.Duration(float64(time.Second) / rate)
	estimate := int(rate*duration.Seconds()) + 16
	samples := make([]sample, 0, estimate)

	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxInFlight)

	for i := 0; ; i++ {
		offset := time.Duration(i) * interval
		if offset >= duration {
			break
		}
		if d := time.Until(start.Add(offset)); d > 0 {
			time.Sleep(d)
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(offset time.Duration) {
			defer wg.Done()
			defer func() { <-sem }()

			ok := post(client, url, body)
			// Measured from the scheduled time, not from the actual send.
			lat := time.Since(start) - offset

			mu.Lock()
			samples = append(samples, sample{scheduled: offset, latency: lat, ok: ok})
			mu.Unlock()
		}(offset)
	}

	wg.Wait()
	return samples
}

func post(client *http.Client, url string, body []byte) bool {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "text/plain")
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	// Drain before closing so the connection returns to the idle pool
	// instead of being torn down.
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode == http.StatusAccepted
}

// setup creates every queue and registers every subscriber. Each repetition
// runs against a freshly started broker, so this has to happen every time.
func setup(client *http.Client, broker, sink string, queues []string, subsPerQueue, retries int) error {
	base, port, err := splitSink(sink)
	if err != nil {
		return err
	}
	for _, q := range queues {
		if err := requireStatus(client, http.MethodPost,
			fmt.Sprintf("%s/queues/%s", broker, q), nil, http.StatusCreated); err != nil {
			return fmt.Errorf("creating queue %s: %w", q, err)
		}
		for s := 0; s < subsPerQueue; s++ {
			policy := map[string]any{
				"SubName":         fmt.Sprintf("sub%d", s),
				"SubURL":          fmt.Sprintf("%s:%d", base, port+s),
				"NumberOfRetries": retries,
			}
			payload, err := json.Marshal(policy)
			if err != nil {
				return err
			}
			if err := requireStatus(client, http.MethodPost,
				fmt.Sprintf("%s/queues/%s/subscribers", broker, q), payload, http.StatusCreated); err != nil {
				return fmt.Errorf("registering sub%d on %s: %w", s, q, err)
			}
		}
	}
	return nil
}

func requireStatus(client *http.Client, method, url string, body []byte, want int) error {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != want {
		return fmt.Errorf("%s %s: got %d, want %d: %s", method, url, resp.StatusCode, want, strings.TrimSpace(string(msg)))
	}
	return nil
}

// splitSink pulls the trailing port off a sink base URL so consecutive
// endpoints can be addressed.
func splitSink(sink string) (string, int, error) {
	i := strings.LastIndex(sink, ":")
	if i < 0 {
		return "", 0, fmt.Errorf("sink %q must include a port", sink)
	}
	var port int
	if _, err := fmt.Sscanf(sink[i+1:], "%d", &port); err != nil {
		return "", 0, fmt.Errorf("sink %q must end in a port: %w", sink, err)
	}
	return sink[:i], port, nil
}

func sinkTotal(client *http.Client, sink string) (uint64, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sink+"/stats", nil)
	if err != nil {
		return 0, false
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	var s struct {
		Total uint64
	}
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return 0, false
	}
	return s.Total, true
}

func resetSink(client *http.Client, sink string, subsPerQueue int) {
	base, port, err := splitSink(sink)
	if err != nil {
		return
	}
	for s := 0; s < subsPerQueue; s++ {
		req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s:%d/stats/reset", base, port+s), nil)
		if err != nil {
			continue
		}
		if resp, err := client.Do(req); err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}
}
