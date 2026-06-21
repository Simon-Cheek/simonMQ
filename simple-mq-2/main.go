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
	mux.HandleFunc("GET /queues/{queueName}/messages/next", b.HandleDequeue)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
