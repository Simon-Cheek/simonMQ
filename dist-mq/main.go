package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"dist-mq/delivery"
	"dist-mq/node"
	"dist-mq/server"
	"dist-mq/storage"
)

const shutdownTimeout = 10 * time.Second

func main() {
	id := flag.String("id", "node-0", "raft server id; must be stable across restarts")
	raftDir := flag.String("raft-dir", "dist-raft", "directory holding raft.db and snapshots")
	raftAddr := flag.String("raft-addr", "127.0.0.1:9000", "raft transport bind address")
	raftAdvertise := flag.String("raft-advertise", "", "address peers reach this node on (default -raft-addr)")
	httpAddr := flag.String("http-addr", "127.0.0.1:8080", "HTTP listen address")
	peerList := flag.String("peers", "",
		"cluster members as id=raftAddr=httpBaseURL, comma separated. Empty means single node.")
	bootstrap := flag.Bool("bootstrap", true,
		"bootstrap the cluster on first boot. Ignored once raft has persisted state.")
	reconcile := flag.Duration("reconcile", 0, "delivery reconcile sweep interval (0 uses the default)")
	logLevel := flag.String("log-level", "INFO", "raft log level")
	flag.Parse()

	// K8s Override Config
	if *raftAdvertise == "" {
		_, raftP, err := net.SplitHostPort(*raftAddr)
		if err != nil {
			log.Fatal(err)
		}
		podIp := os.Getenv("POD_IP") + ":" + raftP // Match Raft defined port
		raftAdvertise = &podIp
	}

	peers, peerHTTP, err := parsePeers(*peerList)
	if err != nil {
		log.Fatalf("invalid -peers: %v", err)
	}

	// One store, three references: node hands it to the FSM, the manager sweeps
	// it, the server reads it. A second instance would silently split state.
	store := storage.NewInMemoryStorage()

	n, err := node.New(node.Config{
		ID:            *id,
		Dir:           *raftDir,
		BindAddr:      *raftAddr,
		AdvertiseAddr: *raftAdvertise,
		Peers:         peers,
		Bootstrap:     *bootstrap,
		LogLevel:      *logLevel,
	}, store)
	if err != nil {
		log.Fatalf("start node: %v", err)
	}

	mgr := delivery.NewManager(n, store, *reconcile)
	go mgr.Run()

	srv := &http.Server{
		Addr:    *httpAddr,
		Handler: server.NewServer(n, store, mgr, server.Config{PeerHTTP: peerHTTP}).Routes(),
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	fmt.Printf("dist-mq %s — raft %s, http %s, %d configured peer(s)\n",
		*id, *raftAddr, *httpAddr, len(peers))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	// Reverse of construction: stop accepting work, stop doing work, then tear
	// down what both were using.
	fmt.Println("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	mgr.Stop()
	if err := n.Shutdown(); err != nil {
		log.Printf("node shutdown: %v", err)
	}
}

// parsePeers turns one flag into the two structures that need it: raft's
// membership, and the raft-address to HTTP-address mapping a 421 redirect
// needs. They come from one source so they cannot drift apart.
func parsePeers(raw string) ([]node.Peer, map[string]string, error) {
	peerHTTP := make(map[string]string)
	if strings.TrimSpace(raw) == "" {
		return nil, peerHTTP, nil
	}

	var peers []node.Peer
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		parts := strings.Split(entry, "=")
		if len(parts) != 3 {
			return nil, nil, fmt.Errorf("peer %q: want id=raftAddr=httpBaseURL", entry)
		}
		peers = append(peers, node.Peer{ID: parts[0], Address: parts[1]})
		peerHTTP[parts[1]] = parts[2]
	}
	return peers, peerHTTP, nil
}
