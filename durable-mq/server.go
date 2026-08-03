package main

import (
	"durable-mq/model"
	"encoding/json"
	"io"
	"net/http"

	"durable-mq/queue"
)

// Server is the HTTP layer. It holds a reference to a Broker and translates
// requests into Broker calls — it does not own or extend Broker's state.
type Server struct {
	broker *queue.Broker
}

// NewServer wires an HTTP layer around an existing Broker.
func NewServer(b *queue.Broker) *Server {
	return &Server{broker: b}
}

// HandleQueueCreation handles queue creation.
// POST /queues/{queueName}
func (s *Server) HandleQueueCreation(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("queueName")
	if err := s.broker.CreateQueue(name, false); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// HandleQueueDeletion handles queue deletion.
// DELETE /queues/{queueName}
func (s *Server) HandleQueueDeletion(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("queueName")
	if err := s.broker.DeleteQueue(name); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HandleEnqueue handles new message submissions.
// POST /queues/{queueName}/messages
func (s *Server) HandleEnqueue(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("queueName")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.broker.Enqueue(name, string(body)); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// HandleSubPolicyCreation handles SubPolicy creation.
// POST /queues/{queueName}/subscribers
// Body contains SubPolicy
func (s *Server) HandleSubPolicyCreation(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var policy model.SubPolicy
	if err := json.Unmarshal(body, &policy); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.broker.AddSubscriber(policy, queueName); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// HandleSubPolicyUpdate handles SubPolicy updates.
// PUT /queues/{queueName}/subscribers/{SubName}
// Body contains SubPolicy
func (s *Server) HandleSubPolicyUpdate(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")
	subName := r.PathValue("SubName")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var policy model.SubPolicy
	if err := json.Unmarshal(body, &policy); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	policy.SubName = subName // path is authoritative over body for identity

	if err := s.broker.UpdateSubscriber(policy, queueName); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// HandleSubPolicyDeletion handles SubPolicy deletion.
// DELETE /queues/{queueName}/subscribers/{SubName}
func (s *Server) HandleSubPolicyDeletion(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")
	subName := r.PathValue("SubName")

	if err := s.broker.RemoveSubscriber(subName, queueName); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
