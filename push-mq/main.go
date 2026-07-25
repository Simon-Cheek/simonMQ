package main

import (
	"log"
	"net/http"
)

const port = "8081"

func main() {
	b := NewBroker()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /queues/{queueName}/messages", b.HandleEnqueue)
	mux.HandleFunc("POST /queues/{queueName}", b.HandleQueueCreation)
	mux.HandleFunc("DELETE /queues/{queueName}", b.HandleQueueDeletion)
	mux.HandleFunc("POST /queues/{queueName}/subscribers", b.HandleSubPolicyCreation)
	mux.HandleFunc("PUT /queues/{queueName}/subscribers/{SubName}", b.HandleSubPolicyUpdate)
	mux.HandleFunc("DELETE /queues/{queueName}/subscribers/{SubName}", b.HandleSubPolicyDeletion)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
