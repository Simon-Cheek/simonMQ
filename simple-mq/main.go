package main

import (
	"log"
	"net/http"
)

func main() {
	b := Broker{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /queues/{queueName}/messages", b.handleEnqueue)
	mux.HandleFunc("GET /queues/{queueName}/messages/next", b.handleDequeue)
	log.Fatal(http.ListenAndServe(":8080", mux))
}
