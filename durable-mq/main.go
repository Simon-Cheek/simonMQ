package main

import (
	"fmt"
	"log"
	"net/http"

	"durable-mq/queue"
	"durable-mq/server"
)

const port = "8081"

func main() {
	b := queue.NewBroker()

	if err := b.RestoreWAL(); err != nil {
		log.Fatalf("failed to restore from WAL: %v", err)
	}
	fmt.Println("Successfully restored WAL")

	srv := server.NewServer(b)
	log.Fatal(http.ListenAndServe(":"+port, srv.Routes()))
}
