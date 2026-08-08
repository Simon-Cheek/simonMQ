package node

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"dist-mq/command"
	"dist-mq/fsm"
	"dist-mq/model"
	"dist-mq/storage"
)

const (
	applyTimeout        = 10 * time.Second
	retainSnapshotCount = 2
	logCacheSize        = 512
	transportMaxPool    = 3
	transportTimeout    = 10 * time.Second
)

var ErrNotLeader = raft.ErrNotLeader

type Peer struct {
	ID      string
	Address string
}

type Config struct {
	ID            string
	Dir           string
	BindAddr      string
	AdvertiseAddr string // defaults to BindAddr; k8s needs these to differ

	// Peers is the full membership for a static bootstrap. Every node may
	// bootstrap with the same list. Empty means bootstrap alone.
	Peers     []Peer
	Bootstrap bool

	LogOutput io.Writer
	LogLevel  string
}

// Node owns the raft handle and is the only place the raft API is touched.
type Node struct {
	raft       *raft.Raft
	appStorage storage.Storage
	boltDB     *raftboltdb.BoltStore
}

func New(cfg Config, store storage.Storage) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("node: empty ID")
	}
	if cfg.BindAddr == "" {
		return nil, errors.New("node: empty BindAddr")
	}
	if cfg.LogOutput == nil {
		cfg.LogOutput = os.Stderr
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "INFO"
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.ID)
	raftCfg.LogOutput = cfg.LogOutput
	raftCfg.LogLevel = cfg.LogLevel

	transport, err := newTransport(cfg)
	if err != nil {
		return nil, err
	}

	snapshots, logStore, boltDB, err := newStores(cfg)
	if err != nil {
		return nil, err
	}

	ra, err := raft.NewRaft(raftCfg, fsm.New(store), logStore, boltDB, snapshots, transport)
	if err != nil {
		_ = boltDB.Close()
		return nil, fmt.Errorf("node: new raft: %w", err)
	}

	// Bootstrapping a node that already has state would discard the
	// configuration raft persisted, so it is strictly a first-boot action.
	if cfg.Bootstrap {
		hasState, err := raft.HasExistingState(logStore, boltDB, snapshots)
		if err != nil {
			return nil, fmt.Errorf("node: check existing state: %w", err)
		}
		if !hasState {
			if err := ra.BootstrapCluster(bootstrapConfig(cfg, transport)).Error(); err != nil {
				return nil, fmt.Errorf("node: bootstrap: %w", err)
			}
		}
	}

	return &Node{raft: ra, appStorage: store, boltDB: boltDB}, nil
}

func newTransport(cfg Config) (raft.Transport, error) {
	advertise := cfg.AdvertiseAddr
	if advertise == "" {
		advertise = cfg.BindAddr
	}
	addr, err := net.ResolveTCPAddr("tcp", advertise)
	if err != nil {
		return nil, fmt.Errorf("node: resolve advertise address %q: %w", advertise, err)
	}

	transport, err := raft.NewTCPTransport(cfg.BindAddr, addr, transportMaxPool, transportTimeout, cfg.LogOutput)
	if err != nil {
		return nil, fmt.Errorf("node: tcp transport: %w", err)
	}
	return transport, nil
}

// boltDB backs both the log and the stable store; logs is a cache in front of
// it so replication does not hit disk for recent entries.
func newStores(cfg Config) (snapshots raft.SnapshotStore, logs raft.LogStore, boltDB *raftboltdb.BoltStore, err error) {

	// Verify Directory for Raft File Storage
	if cfg.Dir == "" {
		return nil, nil, nil, errors.New("node: empty Dir")
	}
	if err := os.MkdirAll(cfg.Dir, 0o750); err != nil {
		return nil, nil, nil, fmt.Errorf("node: create raft dir: %w", err)
	}

	// Create FileSnapshot Storage
	snapshots, err = raft.NewFileSnapshotStore(cfg.Dir, retainSnapshotCount, cfg.LogOutput)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("node: file snapshot store: %w", err)
	}

	// Create BoltDB Instance for Log Storage
	boltDB, err = raftboltdb.New(raftboltdb.Options{Path: filepath.Join(cfg.Dir, "raft.db")})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("node: bolt store: %w", err)
	}

	// Create Log Cache using BoltDB
	logs, err = raft.NewLogCache(logCacheSize, boltDB)
	if err != nil {
		_ = boltDB.Close()
		return nil, nil, nil, fmt.Errorf("node: log cache: %w", err)
	}
	return snapshots, logs, boltDB, nil
}

func bootstrapConfig(cfg Config, transport raft.Transport) raft.Configuration {
	if len(cfg.Peers) == 0 {
		return raft.Configuration{Servers: []raft.Server{{
			ID:      raft.ServerID(cfg.ID),
			Address: transport.LocalAddr(),
		}}}
	}

	servers := make([]raft.Server, 0, len(cfg.Peers))
	for _, peer := range cfg.Peers {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(peer.ID),
			Address: raft.ServerAddress(peer.Address),
		})
	}
	return raft.Configuration{Servers: servers}
}

// --- proposing -------------------------------------------------------------

// propose blocks until the entry is committed on a quorum and applied locally,
// then unwraps whatever the FSM returned. Storage errors travel back as the
// response value, not as the future's error.
func (n *Node) propose(cmd command.Command) (any, error) {
	data, err := command.Encode(cmd)
	if err != nil {
		return nil, err
	}

	future := n.raft.Apply(data, applyTimeout)
	if err := future.Error(); err != nil {
		return nil, err
	}
	if err, ok := future.Response().(error); ok {
		return nil, err
	}
	return future.Response(), nil
}

func (n *Node) CreateQueue(queueName string) error {
	_, err := n.propose(command.NewCreateQueue(queueName))
	return err
}

func (n *Node) DeleteQueue(queueName string) error {
	_, err := n.propose(command.NewDeleteQueue(queueName))
	return err
}

func (n *Node) PutSubPolicy(queueName string, policy model.SubPolicy) error {
	_, err := n.propose(command.NewPutSubPolicy(queueName, policy))
	return err
}

func (n *Node) DeleteSubPolicy(queueName, subName string) error {
	_, err := n.propose(command.NewDeleteSubPolicy(queueName, subName))
	return err
}

// Enqueue returns the resolved message so the caller can schedule delivery
// without re-reading state. The subscriber list travels in the command, so
// every node stores the same one.
func (n *Node) Enqueue(queueName, msgID, payload string, subList map[string]model.SubPolicy) (model.MessageInfo, error) {
	resp, err := n.propose(command.NewEnqueue(queueName, msgID, payload, subList))
	if err != nil {
		return model.MessageInfo{}, err
	}
	msg, ok := resp.(model.MessageInfo)
	if !ok {
		return model.MessageInfo{}, fmt.Errorf("enqueue: unexpected response %T", resp)
	}
	return msg, nil
}

func (n *Node) Ack(queueName, msgID string, subNames []string) error {
	_, err := n.propose(command.NewAck(queueName, msgID, subNames))
	return err
}

// --- leadership ------------------------------------------------------------

func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// Barrier blocks until every entry committed before this call has been applied
// locally. Leadership arrives before the FSM has caught up, so a promoted
// leader must barrier before reading state to rebuild delivery from — without
// it the sweep sees a partially applied backlog.
func (n *Node) Barrier(timeout time.Duration) error {
	return n.raft.Barrier(timeout).Error()
}

// LeaderAddress reports where writes should go. The false case is a live
// election, which is a backoff-and-retry for the client, not a redirect.
func (n *Node) LeaderAddress() (string, bool) {
	addr, _ := n.raft.LeaderWithID()
	if addr == "" {
		return "", false
	}
	return string(addr), true
}

// LeaderCh drops signals when the receiver is busy, and two consecutive true
// values mean leadership was lost and regained in between. Consumers must
// reconcile to the current state rather than toggling, and must never block.
func (n *Node) LeaderCh() <-chan bool {
	return n.raft.LeaderCh()
}

func (n *Node) Store() storage.Storage {
	return n.appStorage
}

func (n *Node) Stats() map[string]string {
	return n.raft.Stats()
}

func (n *Node) Shutdown() error {
	var errs []error
	if err := n.raft.Shutdown().Error(); err != nil {
		errs = append(errs, fmt.Errorf("raft shutdown: %w", err))
	}
	if err := n.boltDB.Close(); err != nil {
		errs = append(errs, fmt.Errorf("bolt close: %w", err))
	}
	if err := n.appStorage.Close(); err != nil {
		errs = append(errs, fmt.Errorf("store close: %w", err))
	}
	return errors.Join(errs...)
}
