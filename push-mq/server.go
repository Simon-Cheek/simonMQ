package main

import (
	"encoding/json"
	"io"
	"net/http"
)

// HandleQueueCreation handles queue creation.
// POST /queues/{queueName}
func (b *Broker) HandleQueueCreation(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("queueName")
	err := b.CreateQueue(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// HandleQueueDeletion handles queue deletion.
// DELETE /queues/{queueName}
func (b *Broker) HandleQueueDeletion(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("queueName")
	b.DeleteQueue(name)
	w.WriteHeader(http.StatusNoContent)
}

// HandleEnqueue handles new message submissions.
// POST /queues/{queueName}/messages
func (b *Broker) HandleEnqueue(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("queueName")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = b.Enqueue(name, string(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// HandleSubPolicyCreation handles SubPolicy creation.
// POST /queues/{queueName}/subscribers
// Body contains SubPolicy
func (b *Broker) HandleSubPolicyCreation(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var policy SubPolicy
	if err := json.Unmarshal(body, &policy); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := b.AddSubscriber(policy, queueName); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// HandleSubPolicyUpdate handles SubPolicy updates.
// PUT /queues/{queueName}/subscribers/{SubName}
// Body contains SubPolicy
func (b *Broker) HandleSubPolicyUpdate(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")
	subName := r.PathValue("SubName")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var policy SubPolicy
	if err := json.Unmarshal(body, &policy); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	policy.SubName = subName // path is authoritative over body for identity

	if err := b.UpdateSubscriber(policy, queueName); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// HandleSubPolicyDeletion handles SubPolicy deletion.
// DELETE /queues/{queueName}/subscribers/{SubName}
func (b *Broker) HandleSubPolicyDeletion(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")
	subName := r.PathValue("SubName")

	if err := b.RemoveSubscriber(subName, queueName); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
