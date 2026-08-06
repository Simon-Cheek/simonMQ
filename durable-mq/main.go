package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"durable-mq/coordinator"
	"durable-mq/queue"
	"durable-mq/server"
	"durable-mq/wal"
)

const shutdownTimeout = 10 * time.Second

func main() {
	port := flag.String("port", "8081", "port to listen on")
	walDir := flag.String("wal-dir", "", "WAL directory (default durable-wal)")
	walMode := flag.String("wal-mode", "sync",
		"durability mode: sync (fsync every append) | nosync (no fsync) | off (no log at all). "+
			"nosync and off are benchmark-only and are NOT durable.")
	maxSegSize := flag.String("max-seg-size", "",
		"segment roll threshold, e.g. 1MB or 512KB (default 128MB)")
	ckptThreshold := flag.Int("checkpoint-threshold", 0,
		"segments that must accumulate before a checkpoint runs (default 4). "+
			"Lower this with -max-seg-size to exercise checkpointing without writing hundreds of MB.")
	flag.Parse()

	mode, err := wal.ParseMode(*walMode)
	if err != nil {
		log.Fatalf("invalid -wal-mode: %v", err)
	}

	var segSize int64
	if *maxSegSize != "" {
		if segSize, err = wal.ParseSize(*maxSegSize); err != nil {
			log.Fatalf("invalid -max-seg-size: %v", err)
		}
	}

	b := queue.NewBrokerWithConfig(coordinator.Config{
		Dir:                 *walDir,
		Mode:                mode,
		MaxSegSize:          segSize,
		CheckpointThreshold: *ckptThreshold,
	})

	// Timed because recovery duration as a function of log size is the whole
	// justification for checkpointing, and it can't be measured from outside
	// the process without also counting process start and listener setup.
	restoreStart := time.Now()
	if err := b.RestoreWAL(); err != nil {
		log.Fatalf("failed to restore from WAL: %v", err)
	}
	restoreTook := time.Since(restoreStart)

	// Announced on every boot so a benchmark run can never be quietly
	// non-durable and later be reported as if it were.
	if mode.Durable() {
		fmt.Printf("Durability: sync (fsync per append) — restored WAL in %s\n", restoreTook)
	} else {
		fmt.Printf("Durability: %s — WARNING: NOT DURABLE, benchmark use only (restored in %s)\n",
			mode, restoreTook)
	}

	srv := &http.Server{
		Addr:    ":" + *port,
		Handler: server.NewServer(b).Routes(),
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server error: %v", err)
		}
	}()
	fmt.Println("Listening on :" + *port)

	// A hard kill skips all of this, which is fine — every append is fsynced
	// before it's acknowledged, so recovery handles an abrupt exit the same
	// way it handles a clean one.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	fmt.Println("Shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	if err := b.Close(); err != nil {
		log.Printf("broker close: %v", err)
	}
}
