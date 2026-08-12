package harness_test

import (
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"

	"dist-mq/model"
	"dist-mq/server"
	"dist-mq/storage"
)

// The fakes below back the real server.NewServer rather than replacing it, so
// the harness is exercised against the actual routes and the actual
// requireLeader middleware. The 421-versus-503 distinction is the whole basis
// of leader discovery, and a hand-rolled stub would be free to get it wrong in
// exactly the way that matters.

type clusterState struct {
	mu       sync.Mutex
	leaderID string // empty means an election is in progress
	queues   map[string]*queueState
}

type queueState struct {
	subs     map[string]model.SubPolicy
	messages []model.MessageInfo
}

func newClusterState() *clusterState {
	return &clusterState{queues: make(map[string]*queueState)}
}

func (s *clusterState) setLeader(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leaderID = id
}

func (s *clusterState) addMessage(queue string, msg model.MessageInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if q, ok := s.queues[queue]; ok {
		q.messages = append(q.messages, msg)
	}
}

func (s *clusterState) clearMessages(queue string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if q, ok := s.queues[queue]; ok {
		q.messages = nil
	}
}

func (s *clusterState) hasQueue(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.queues[name]
	return ok
}

func (s *clusterState) subs(queue string) map[string]model.SubPolicy {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[queue]
	if !ok {
		return nil
	}
	out := make(map[string]model.SubPolicy, len(q.subs))
	for k, v := range q.subs {
		out[k] = v
	}
	return out
}

// node is one broker: a view of the shared state plus its own identity.
type node struct {
	id    string
	state *clusterState
	srv   *httptest.Server
}

func (n *node) IsLeader() bool {
	n.state.mu.Lock()
	defer n.state.mu.Unlock()
	return n.state.leaderID == n.id
}

func (n *node) LeaderID() (string, bool) {
	n.state.mu.Lock()
	defer n.state.mu.Unlock()
	if n.state.leaderID == "" {
		return "", false
	}
	return n.state.leaderID, true
}

func (n *node) CreateQueue(name string) error {
	n.state.mu.Lock()
	defer n.state.mu.Unlock()
	if _, exists := n.state.queues[name]; exists {
		return storage.ErrQueueExists
	}
	n.state.queues[name] = &queueState{subs: make(map[string]model.SubPolicy)}
	return nil
}

func (n *node) DeleteQueue(name string) error {
	n.state.mu.Lock()
	defer n.state.mu.Unlock()
	if _, ok := n.state.queues[name]; !ok {
		return storage.ErrQueueNotFound
	}
	delete(n.state.queues, name)
	return nil
}

func (n *node) PutSubPolicy(queue string, policy model.SubPolicy) error {
	n.state.mu.Lock()
	defer n.state.mu.Unlock()
	q, ok := n.state.queues[queue]
	if !ok {
		return storage.ErrQueueNotFound
	}
	q.subs[policy.SubName] = policy
	return nil
}

func (n *node) DeleteSubPolicy(queue, sub string) error {
	n.state.mu.Lock()
	defer n.state.mu.Unlock()
	q, ok := n.state.queues[queue]
	if !ok {
		return storage.ErrQueueNotFound
	}
	delete(q.subs, sub)
	return nil
}

func (n *node) Enqueue(queue, msgID, body string, subList map[string]model.SubPolicy) (model.MessageInfo, error) {
	n.state.mu.Lock()
	defer n.state.mu.Unlock()
	q, ok := n.state.queues[queue]
	if !ok {
		return model.MessageInfo{}, storage.ErrQueueNotFound
	}
	msg := model.MessageInfo{MsgID: msgID, Payload: body, SubList: subList}
	q.messages = append(q.messages, msg)
	return msg, nil
}

func (n *node) AllQueueInfo() []model.QueueInfo {
	n.state.mu.Lock()
	defer n.state.mu.Unlock()
	out := make([]model.QueueInfo, 0, len(n.state.queues))
	for name, q := range n.state.queues {
		out = append(out, model.QueueInfo{
			Name:        name,
			SubPolicies: q.subs,
			Messages:    append([]model.MessageInfo{}, q.messages...),
		})
	}
	return out
}

func (n *node) FetchSubList(queue string) (map[string]model.SubPolicy, bool) {
	n.state.mu.Lock()
	defer n.state.mu.Unlock()
	q, ok := n.state.queues[queue]
	if !ok {
		return nil, false
	}
	return q.subs, true
}

func (n *node) Schedule(string, model.MessageInfo) {}

// newCluster wires size nodes onto shared state and returns their base URLs.
func newCluster(t *testing.T, size int) (*clusterState, []string) {
	t.Helper()
	state := newClusterState()

	// The redirect map is filled after the servers have addresses, and is the
	// same map the servers already hold, which is how a 421 learns a URL that
	// did not exist when the server was built.
	peerHTTP := make(map[string]string, size)
	nodes := make([]*node, size)
	urls := make([]string, size)

	for i := 0; i < size; i++ {
		n := &node{id: nodeID(i), state: state}
		n.srv = httptest.NewServer(server.NewServer(n, n, n, server.Config{PeerHTTP: peerHTTP}).Routes())
		t.Cleanup(n.srv.Close)
		nodes[i] = n
		urls[i] = n.srv.URL
	}
	for i, n := range nodes {
		peerHTTP[n.id] = urls[i]
	}

	state.setLeader(nodeID(0))
	return state, urls
}

func nodeID(i int) string { return fmt.Sprintf("dist-mq-%d", i) }
