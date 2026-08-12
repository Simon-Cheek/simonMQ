// Command bench runs one benchmark measurement against a deployed cluster and
// prints the result as JSON. One process per measurement: scripts/bench.sh
// interleaves arms and repetitions around it.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"dist-mq/e2e-tests/bench"
	"dist-mq/e2e-tests/client"
	"dist-mq/e2e-tests/harness"
	"dist-mq/e2e-tests/sink"
)

func main() {
	nodesFlag := flag.String("nodes", "", "comma-separated node base URLs; overrides -service")
	service := flag.String("service", "", "headless service to discover nodes from")
	httpPort := flag.Int("http-port", 8080, "node HTTP port, used with -service")
	sinkBase := flag.String("sink", "", "sink base URL as the cluster reaches it (required)")

	arm := flag.String("arm", "", "name of the configuration being measured; defaults to the cluster size")
	rep := flag.Int("rep", 1, "repetition number")

	queues := flag.Int("queues", 3, "number of queues")
	pubsPerQueue := flag.Int("publishers-per-queue", 1, "concurrent publishers per queue")
	subsPerQueue := flag.Int("subscribers-per-queue", 1, "subscribers registered on each queue")
	rate := flag.Float64("rate", 200, "target publishes per second per publisher")
	payloadLen := flag.Int("payload", 64, "message payload size in bytes")
	retries := flag.Int("retries", 3, "NumberOfRetries on each registered subscriber")

	duration := flag.Duration("duration", 30*time.Second, "total run length including warm-up")
	warmup := flag.Duration("warmup", 5*time.Second, "leading period excluded from the results")
	maxInFlight := flag.Int("max-in-flight", 512, "per-publisher cap on outstanding requests")
	setupTimeout := flag.Duration("setup-timeout", 90*time.Second, "how long to wait for a leader")
	flag.Parse()

	if *sinkBase == "" {
		log.Fatal("-sink is required")
	}

	nodes, err := resolveNodes(*nodesFlag, *service, *httpPort)
	if err != nil {
		log.Fatalf("resolving nodes: %v", err)
	}
	if len(nodes) == 0 {
		log.Fatal("no nodes: pass -nodes or -service")
	}

	// Two attempts, not the default eight. A benchmark's retries are load it
	// offers but does not report, so the budget is only large enough to follow
	// one redirect — and with the leader cached, steady state is one attempt.
	c, err := client.New(client.Config{
		Nodes:       nodes,
		MaxAttempts: 2,
		BaseBackoff: 10 * time.Millisecond,
		MaxBackoff:  100 * time.Millisecond,
		HTTPTimeout: 15 * time.Second,
		MaxConns:    *queues * *pubsPerQueue * *maxInFlight,
	})
	if err != nil {
		log.Fatalf("client: %v", err)
	}

	base := strings.TrimSuffix(*sinkBase, "/")
	queueNames := make([]string, *queues)
	for i := range queueNames {
		queueNames[i] = fmt.Sprintf("bench-q%d", i)
	}

	setupCtx, cancel := context.WithTimeout(context.Background(), *setupTimeout)
	defer cancel()

	h, err := harness.Setup(setupCtx, harness.Config{
		Client:      c,
		Queues:      queueNames,
		SinkBase:    base,
		Subscribers: sink.Names(*subsPerQueue),
		Retries:     *retries,
	})
	if err != nil {
		log.Fatalf("setup: %v", err)
	}

	name := *arm
	if name == "" {
		name = fmt.Sprintf("%d-node", len(nodes))
	}

	fmt.Fprintf(os.Stderr, "bench: arm=%s rep=%d nodes=%d offered=%.0f/s payload=%dB for %s\n",
		name, *rep, len(nodes), *rate*float64(*queues**pubsPerQueue), *payloadLen, *duration)

	res, err := bench.Run(context.Background(), bench.Config{
		Client:              c,
		Sink:                base,
		Arm:                 name,
		Rep:                 *rep,
		Queues:              queueNames,
		PublishersPerQueue:  *pubsPerQueue,
		SubscribersPerQueue: *subsPerQueue,
		RatePerPublisher:    *rate,
		PayloadBytes:        *payloadLen,
		Duration:            *duration,
		Warmup:              *warmup,
		MaxInFlight:         *maxInFlight,
	})
	if err != nil {
		log.Fatalf("bench: %v", err)
	}

	fmt.Fprintf(os.Stderr, "bench: %.0f accepted/s (offered %.0f), p50=%.2fms p99=%.2fms, delivered=%d\n",
		res.AcceptedPerSec, res.OfferedRate, res.Latency.P50, res.Latency.P99, res.Delivered)

	if err := bench.WriteJSON(os.Stdout, res); err != nil {
		log.Fatalf("writing result: %v", err)
	}

	// Queues are left in place between repetitions on purpose — recreating
	// them would put a burst of consensus traffic at the head of every run.
	// Only the final repetition cleans up, which bench.sh signals.
	if os.Getenv("BENCH_TEARDOWN") == "1" {
		teardownCtx, teardownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer teardownCancel()
		if err := h.Teardown(teardownCtx); err != nil {
			fmt.Fprintf(os.Stderr, "bench: teardown: %v\n", err)
		}
	}
}

func resolveNodes(nodesFlag, service string, port int) ([]string, error) {
	if nodesFlag != "" {
		var nodes []string
		for _, n := range strings.Split(nodesFlag, ",") {
			if n = strings.TrimSpace(strings.TrimSuffix(n, "/")); n != "" {
				nodes = append(nodes, n)
			}
		}
		return nodes, nil
	}
	if service == "" {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return harness.Discover(ctx, service, port)
}
