package main

import (
	"encoding/json"
	"io"
	"net/http"
)

func (b *Broker) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("queueName")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	b.enqueue(name, string(body))
	w.WriteHeader(http.StatusAccepted)
}

func (b *Broker) handleDequeue(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("queueName")
	msg := b.dequeue(name)
	if msg == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err := json.NewEncoder(w).Encode(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
