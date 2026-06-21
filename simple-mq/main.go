package main

import (
	"log"
	"net/http"
)

const port = "8080"

func main() {
	b := NewBroker()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /queues/{queueName}/messages", b.handleEnqueue)
	mux.HandleFunc("GET /queues/{queueName}/messages/next", b.handleDequeue)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
