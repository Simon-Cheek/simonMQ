package main

import (
	"log"
	"net/http"
)

const port = "8081"

func main() {
	b := NewBroker()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /queues/{queueName}/messages", nil)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
