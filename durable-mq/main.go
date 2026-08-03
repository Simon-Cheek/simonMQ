package main

import (
	"fmt"
	"log"
	"net/http"

	"durable-mq/queue"
)

const port = "8081"

func main() {
	b := queue.NewBroker()

	if err := b.RestoreWAL(); err != nil {
		log.Fatalf("failed to restore from WAL: %v", err)
	}
	fmt.Println("Successfully restored WAL")

	srv := NewServer(b)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /queues", srv.HandleListQueues)
	mux.HandleFunc("POST /queues/{queueName}/messages", srv.HandleEnqueue)
	mux.HandleFunc("POST /queues/{queueName}", srv.HandleQueueCreation)
	mux.HandleFunc("DELETE /queues/{queueName}", srv.HandleQueueDeletion)
	mux.HandleFunc("POST /queues/{queueName}/subscribers", srv.HandleSubPolicyCreation)
	mux.HandleFunc("PUT /queues/{queueName}/subscribers/{SubName}", srv.HandleSubPolicyUpdate)
	mux.HandleFunc("DELETE /queues/{queueName}/subscribers/{SubName}", srv.HandleSubPolicyDeletion)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
