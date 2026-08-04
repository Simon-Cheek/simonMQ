package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"durable-mq/queue"
	"durable-mq/server"
)

const port = "8081"

const shutdownTimeout = 10 * time.Second

func main() {
	b := queue.NewBroker()

	if err := b.RestoreWAL(); err != nil {
		log.Fatalf("failed to restore from WAL: %v", err)
	}
	fmt.Println("Successfully restored WAL")

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: server.NewServer(b).Routes(),
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server error: %v", err)
		}
	}()
	fmt.Println("Listening on :" + port)

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
