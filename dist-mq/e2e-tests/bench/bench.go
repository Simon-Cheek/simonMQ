// Package bench measures dist-mq under a fixed offered load.
//
// The generator is open-loop on purpose. A closed-loop one — send, wait for
// the reply, send again — cannot offer more load than the broker will take, so
// when the broker stalls the generator politely stalls with it and the recorded
// latencies stay flat. That hides precisely the behaviour a queue benchmark
// exists to expose. Here every publish has a scheduled time derived from the
// target rate, and latency is measured from that scheduled time rather than
// from when the request actually went out, so queueing delay lands in the
// numbers instead of vanishing. (This is the coordinated-omission correction.)
package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"dist-mq/e2e-tests/client"
	"dist-mq/e2e-tests/payload"
	"dist-mq/e2e-tests/sink"
)

type Config struct {
	Client *client.Client
	Sink   string // base URL, used to read delivery tallies

	Arm string // what is being varied; here, the cluster size
	Rep int

	Queues              []string
	PublishersPerQueue  int
	SubscribersPerQueue int

	RatePerPublisher float64
	PayloadBytes     int
	Duration         time.Duration
	Warmup           time.Duration
	MaxInFlight      int
}

type Result struct {
	Arm       string    `json:"Arm"`
	Rep       int       `json:"Rep"`
	StartedAt time.Time `json:"StartedAt"`

	ClusterSize         int     `json:"ClusterSize"`
	Queues              int     `json:"Queues"`
	PublishersPerQueue  int     `json:"PublishersPerQueue"`
	SubscribersPerQueue int     `json:"SubscribersPerQueue"`
	PayloadBytes        int     `json:"PayloadBytes"`
	OfferedRate         float64 `json:"OfferedRatePerSec"`
	DurationSec         float64 `json:"DurationSec"`
	WarmupSec           float64 `json:"WarmupSec"`

	Attempted      int     `json:"Attempted"`
	Accepted       int     `json:"Accepted"`
	Ambiguous      int     `json:"Ambiguous"`
	Rejected       int     `json:"Rejected"`
	AcceptedPerSec float64 `json:"AcceptedPerSec"`
	ElapsedSec     float64 `json:"ElapsedSec"`
	Latency        Latency `json:"Latency"`

	Delivered       uint64  `json:"Delivered"`
	DeliveredPerSec float64 `json:"DeliveredPerSec"`
}

// Run executes one measurement and returns its result.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 512
	}
	if cfg.Warmup >= cfg.Duration {
		return Result{}, fmt.Errorf("bench: warmup (%s) must be shorter than duration (%s)", cfg.Warmup, cfg.Duration)
	}

	if err := resetSink(ctx, cfg.Sink); err != nil {
		return Result{}, err
	}

	body := payload.Encode("bench", cfg.PayloadBytes)
	start := time.Now()

	publishers := len(cfg.Queues) * cfg.PublishersPerQueue
	perPub := make([][]sample, publishers)

	var wg sync.WaitGroup
	for qi, queue := range cfg.Queues {
		for pi := 0; pi < cfg.PublishersPerQueue; pi++ {
			idx := qi*cfg.PublishersPerQueue + pi
			wg.Add(1)
			go func(idx int, queue string) {
				defer wg.Done()
				perPub[idx] = publish(ctx, cfg, queue, body, start)
			}(idx, queue)
		}
	}

	// Snapshot the sink at the warm-up boundary so the delivered count covers
	// the same window the accepted count does. Without this, delivered spans
	// the whole run while accepted skips the warm-up, and every arm reports a
	// ratio inflated by exactly duration/(duration-warmup).
	if d := time.Until(start.Add(cfg.Warmup)); d > 0 {
		time.Sleep(d)
	}
	deliveredAtWarmup, _ := sinkDelivered(ctx, cfg.Sink)

	wg.Wait()

	var samples []sample
	for _, s := range perPub {
		samples = append(samples, s...)
	}
	sum := summarize(samples, cfg.Warmup, cfg.Duration)

	// Sampled at the end of the publish window and not a moment later. Letting
	// the tail drain first would count deliveries that happened outside the
	// window their rate is then divided by, overstating it by exactly the
	// drain time. Deliveries for messages accepted near the end are missed as
	// a result, which is the honest trade: over a steady window this is the
	// delivery rate, and comparing it to AcceptedPerSec is what shows whether
	// the cluster is keeping up or accumulating backlog.
	deliveredAtEnd, _ := sinkDelivered(ctx, cfg.Sink)

	var delivered uint64
	if deliveredAtEnd >= deliveredAtWarmup {
		delivered = deliveredAtEnd - deliveredAtWarmup
	}

	res := Result{
		Arm:                 cfg.Arm,
		Rep:                 cfg.Rep,
		StartedAt:           start,
		ClusterSize:         len(cfg.Client.Nodes()),
		Queues:              len(cfg.Queues),
		PublishersPerQueue:  cfg.PublishersPerQueue,
		SubscribersPerQueue: cfg.SubscribersPerQueue,
		PayloadBytes:        cfg.PayloadBytes,
		OfferedRate:         cfg.RatePerPublisher * float64(publishers),
		DurationSec:         cfg.Duration.Seconds(),
		WarmupSec:           cfg.Warmup.Seconds(),
		Attempted:           sum.Attempted,
		Accepted:            sum.Accepted,
		Ambiguous:           sum.Ambiguous,
		Rejected:            sum.Rejected,
		AcceptedPerSec:      sum.AcceptedPerSec,
		ElapsedSec:          sum.ElapsedSec,
		Latency:             sum.Latency,
		Delivered:           delivered,
	}
	if res.ElapsedSec > 0 {
		res.DeliveredPerSec = float64(delivered) / res.ElapsedSec
	}
	return res, nil
}

// publish runs one publisher against one queue for the whole duration.
//
// Sends go out from a bounded pool so a stalled broker cannot grow goroutines
// without limit. When the pool is full the publisher blocks and falls behind
// its schedule, and because latency is measured from the scheduled time, that
// delay shows up in the numbers rather than being silently skipped.
func publish(ctx context.Context, cfg Config, queue string, body []byte, start time.Time) []sample {
	interval := time.Duration(float64(time.Second) / cfg.RatePerPublisher)
	samples := make([]sample, 0, int(cfg.RatePerPublisher*cfg.Duration.Seconds())+16)

	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, cfg.MaxInFlight)

	for i := 0; ; i++ {
		offset := time.Duration(i) * interval
		if offset >= cfg.Duration {
			break
		}
		if d := time.Until(start.Add(offset)); d > 0 {
			time.Sleep(d)
		}
		if ctx.Err() != nil {
			break
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(offset time.Duration) {
			defer wg.Done()
			defer func() { <-sem }()

			res := cfg.Client.Enqueue(ctx, queue, body)
			// From the scheduled time, not from the send: this is the whole
			// correction, and measuring from the send would report the broker
			// as fast right at the moment it stopped keeping up.
			latency := time.Since(start.Add(offset))

			mu.Lock()
			samples = append(samples, sample{offset: offset, latency: latency, outcome: res.Outcome})
			mu.Unlock()
		}(offset)
	}

	wg.Wait()
	return samples
}

func resetSink(ctx context.Context, base string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/reset", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("bench: resetting sink: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return nil
}

func sinkDelivered(ctx context.Context, base string) (uint64, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/stats", nil)
	if err != nil {
		return 0, false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()

	var st sink.Stats
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return 0, false
	}
	return st.Delivered, true
}

// WriteJSON emits the result. Markers bracket it so a run's numbers can be
// lifted out of a pod's log, which also carries progress lines.
func WriteJSON(w io.Writer, res Result) error {
	out, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "---BENCH-JSON-BEGIN---\n%s\n---BENCH-JSON-END---\n", out)
	return err
}
