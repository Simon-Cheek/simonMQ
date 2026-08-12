// Command sink runs the e2e delivery endpoint as a standalone process, which
// is how it is deployed in-cluster. The same package runs in-process under go
// test, so a local run and a Kubernetes run exercise identical code.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"dist-mq/e2e-tests/sink"
)

const shutdownTimeout = 5 * time.Second

func main() {
	addr := flag.String("addr", ":9090", "listen address")
	n := flag.Int("n", 1, "number of subscriber endpoints, named sub-0 upward")
	mode := flag.String("mode", "count", "count (tally only) or record (remember tokens)")
	flag.Parse()

	if *n < 1 {
		log.Fatal("-n must be at least 1")
	}

	s, err := sink.New(sink.Config{
		Addr:        *addr,
		Subscribers: sink.Names(*n),
		Mode:        sink.Mode(*mode),
	})
	if err != nil {
		log.Fatalf("sink: %v", err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- s.Serve() }()

	fmt.Printf("sink listening on %s — %d subscriber(s), mode %s\n", s.Addr(), *n, *mode)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		if err != nil {
			log.Fatalf("sink: serve: %v", err)
		}
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := s.Shutdown(shutdownCtx); err != nil {
		log.Printf("sink: shutdown: %v", err)
	}
}
