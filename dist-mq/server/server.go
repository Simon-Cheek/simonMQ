package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/google/uuid"

	"dist-mq/model"
	"dist-mq/storage"
)

const (
	// LeaderHeader carries the current leader's HTTP base URL on a 421 so a
	// client can retry there instead of guessing.
	LeaderHeader = "X-Dist-MQ-Leader"
	retryAfter   = 1 // seconds, advertised on 503 during an election
)

// Cluster is the write path: everything here goes through raft.
type Cluster interface {
	IsLeader() bool
	LeaderID() (string, bool)
	CreateQueue(queueName string) error
	DeleteQueue(queueName string) error
	PutSubPolicy(queueName string, policy model.SubPolicy) error
	DeleteSubPolicy(queueName, subName string) error
	Enqueue(queueName, msgID, payload string, subList map[string]model.SubPolicy) (model.MessageInfo, error)
}

// StateReader is the read path, served from local committed state on any node.
type StateReader interface {
	AllQueueInfo() []model.QueueInfo
	FetchSubList(queueName string) (map[string]model.SubPolicy, bool)
}

// Scheduler receives committed messages so the leader can deliver them.
type Scheduler interface {
	Schedule(queueName string, msg model.MessageInfo)
}

type Config struct {
	// PeerHTTP maps a raft server id to that node's HTTP base URL.
	// Raft only knows the former, and a client can only use the latter.
	PeerHTTP map[string]string
}

type Server struct {
	cluster   Cluster
	state     StateReader
	scheduler Scheduler
	peerHTTP  map[string]string
}

func NewServer(cluster Cluster, state StateReader, scheduler Scheduler, cfg Config) *Server {
	return &Server{
		cluster:   cluster,
		state:     state,
		scheduler: scheduler,
		peerHTTP:  cfg.PeerHTTP,
	}
}

// Routes encapsulates the handling of routes within server.go
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /queues", s.HandleListQueues)

	mux.Handle("POST /queues/{queueName}", s.requireLeader(s.HandleQueueCreation))
	mux.Handle("DELETE /queues/{queueName}", s.requireLeader(s.HandleQueueDeletion))
	mux.Handle("POST /queues/{queueName}/messages", s.requireLeader(s.HandleEnqueue))
	mux.Handle("POST /queues/{queueName}/subscribers", s.requireLeader(s.HandleSubPolicyCreation))
	mux.Handle("PUT /queues/{queueName}/subscribers/{SubName}", s.requireLeader(s.HandleSubPolicyUpdate))
	mux.Handle("DELETE /queues/{queueName}/subscribers/{SubName}", s.requireLeader(s.HandleSubPolicyDeletion))

	return mux
}

// requireLeader distinguishes "someone else is in charge" from "nobody is".
// 421 is a redirect the client should follow; 503 means an election is running
// and the client should back off rather than shop around.
func (s *Server) requireLeader(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cluster.IsLeader() {
			next(w, r)
			return
		}

		leaderID, ok := s.cluster.LeaderID()
		if !ok {
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			http.Error(w, "no leader elected", http.StatusServiceUnavailable)
			return
		}

		// A leader exists but has no configured HTTP mapping: still worth a 421
		// so the client moves on, just without somewhere specific to go.
		if httpAddr, ok := s.peerHTTP[leaderID]; ok {
			w.Header().Set(LeaderHeader, httpAddr)
		}
		http.Error(w, "not leader", http.StatusMisdirectedRequest)
	})
}

// HandleListQueues returns every queue with its subscribers and outstanding
// messages. Served from local state on any node and explicitly not
// linearizable — a follower answers with whatever it has applied so far.
func (s *Server) HandleListQueues(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(s.state.AllQueueInfo()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) HandleQueueCreation(w http.ResponseWriter, r *http.Request) {
	if err := s.cluster.CreateQueue(r.PathValue("queueName")); err != nil {
		s.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) HandleQueueDeletion(w http.ResponseWriter, r *http.Request) {
	if err := s.cluster.DeleteQueue(r.PathValue("queueName")); err != nil {
		s.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HandleEnqueue commits the message, then hands the committed result to the
// delivery layer. Scheduling happens here rather than in the FSM because the
// FSM runs on every node and must never deliver.
func (s *Server) HandleEnqueue(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queueName")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	subList, ok := s.state.FetchSubList(queueName)
	if !ok {
		s.writeError(w, storage.ErrQueueNotFound)
		return
	}

	// Generated before proposing: uuid.New() inside Apply would mint a
	// different id on every node.
	msgID := queueName + "-" + uuid.New().String()

	msg, err := s.cluster.Enqueue(queueName, msgID, string(body), subList)
	if err != nil {
		s.writeError(w, err)
		return
	}

	s.scheduler.Schedule(queueName, msg)
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) HandleSubPolicyCreation(w http.ResponseWriter, r *http.Request) {
	policy, err := decodeSubPolicy(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.cluster.PutSubPolicy(r.PathValue("queueName"), policy); err != nil {
		s.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) HandleSubPolicyUpdate(w http.ResponseWriter, r *http.Request) {
	policy, err := decodeSubPolicy(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	policy.SubName = r.PathValue("SubName") // path is authoritative over body for identity

	if err := s.cluster.PutSubPolicy(r.PathValue("queueName"), policy); err != nil {
		s.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) HandleSubPolicyDeletion(w http.ResponseWriter, r *http.Request) {
	if err := s.cluster.DeleteSubPolicy(r.PathValue("queueName"), r.PathValue("SubName")); err != nil {
		s.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func decodeSubPolicy(r *http.Request) (model.SubPolicy, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return model.SubPolicy{}, err
	}
	return model.DecodeSubPolicy(body)
}

// writeError maps state-machine errors to status codes. Leadership can be lost
// between the middleware check and the propose landing, which surfaces as a
// propose failure rather than a clean redirect — 503 tells the client to retry
// rather than to trust a leader address that is already stale.
func (s *Server) writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, storage.ErrQueueNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, storage.ErrQueueExists):
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	}
}
