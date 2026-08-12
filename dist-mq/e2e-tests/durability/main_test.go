// Package durability is the e2e correctness suite. It is compiled with
// `go test -c`, shipped in the image and run as a Kubernetes Job, and it works
// unchanged against one node or five: cluster size is discovered, never
// configured, and every assertion is phrased in terms the delivery contract
// actually guarantees.
//
// With no -nodes or -service it skips rather than fails, so `go test ./...` on
// a laptop stays green without a cluster in reach.
package durability

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"dist-mq/e2e-tests/client"
	"dist-mq/e2e-tests/harness"
	"dist-mq/e2e-tests/sink"
)

var (
	nodesFlag = flag.String("nodes", "", "comma-separated node base URLs; overrides -service")
	service   = flag.String("service", "", "headless service to discover nodes from, e.g. dist-mq-raft.dist-mq.svc.cluster.local")
	httpPort  = flag.Int("http-port", 8080, "node HTTP port, used with -service")
	sinkBase  = flag.String("sink", "", "sink base URL as the cluster reaches it, e.g. http://dist-mq-sink.dist-mq.svc.cluster.local:9090")

	queueCount = flag.Int("queues", 2, "queues to run against")
	subCount   = flag.Int("subscribers", 2, "subscribers registered per queue")
	msgCount   = flag.Int("messages", 200, "messages published per durability run")
	payloadLen = flag.Int("payload", 64, "message payload size in bytes")
	retries    = flag.Int("retries", 20, "NumberOfRetries per subscriber")

	setupTimeout = flag.Duration("setup-timeout", 90*time.Second, "how long to wait for a leader before giving up")
	drainTimeout = flag.Duration("drain-timeout", 90*time.Second, "how long to wait for delivery to finish")
)

// env is everything a test needs, built once and shared.
type env struct {
	client *client.Client
	h      *harness.Harness
	queues []string
	subs   []string
	sink   string
}

var shared *env

func TestMain(m *testing.M) {
	flag.Parse()

	nodes, err := resolveNodes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "durability: %v\n", err)
		os.Exit(1)
	}
	if len(nodes) == 0 {
		fmt.Println("durability: no -nodes or -service given, skipping")
		os.Exit(0)
	}
	if *sinkBase == "" {
		fmt.Fprintln(os.Stderr, "durability: -sink is required when a cluster is configured")
		os.Exit(1)
	}

	code, err := run(m, nodes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "durability: %v\n", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func run(m *testing.M, nodes []string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), *setupTimeout)
	defer cancel()

	c, err := client.New(client.Config{
		Nodes: nodes,
		// Generous: under chaos a write should exhaust real recovery options
		// before being called ambiguous, or the run reports far more
		// unassertable writes than the cluster actually produced.
		MaxAttempts: 8,
		BaseBackoff: 50 * time.Millisecond,
		MaxBackoff:  2 * time.Second,
		HTTPTimeout: 10 * time.Second,
	})
	if err != nil {
		return 0, err
	}

	queues := names("dur-q", *queueCount)
	subs := sink.Names(*subCount)

	h, err := harness.Setup(ctx, harness.Config{
		Client:      c,
		Queues:      queues,
		SinkBase:    strings.TrimSuffix(*sinkBase, "/"),
		Subscribers: subs,
		Retries:     *retries,
	})
	if err != nil {
		return 0, fmt.Errorf("setup: %w", err)
	}

	fmt.Printf("durability: %d node(s), %d queue(s), %d subscriber(s), sink %s\n",
		len(nodes), len(queues), len(subs), *sinkBase)

	shared = &env{client: c, h: h, queues: queues, subs: subs, sink: strings.TrimSuffix(*sinkBase, "/")}
	code := m.Run()

	// Teardown failures are reported but do not overturn the result: a passing
	// run with litter left behind is still a passing run.
	teardownCtx, teardownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer teardownCancel()
	if err := h.Teardown(teardownCtx); err != nil {
		fmt.Fprintf(os.Stderr, "durability: teardown: %v\n", err)
	}
	return code, nil
}

func resolveNodes() ([]string, error) {
	if *nodesFlag != "" {
		var nodes []string
		for _, n := range strings.Split(*nodesFlag, ",") {
			if n = strings.TrimSpace(strings.TrimSuffix(n, "/")); n != "" {
				nodes = append(nodes, n)
			}
		}
		return nodes, nil
	}
	if *service == "" {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return harness.Discover(ctx, *service, *httpPort)
}

func names(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return out
}
