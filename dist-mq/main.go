package main

import (
	"dist-mq/delivery"
	"dist-mq/node"
	"dist-mq/server"
	"dist-mq/storage"
	"log"
	"net/http"
)

func main() {

	// Instantiate Dependencies
	// Node satisfies interfaces required by Manager and Server
	store := storage.NewInMemoryStorage() // In Memory FSM
	nCfg := node.Config{
		ID:            "ID",
		Dir:           "Dir",
		BindAddr:      ":9000",
		AdvertiseAddr: ":9001",
		Peers:         nil,
		Bootstrap:     false,
		LogOutput:     nil,
		LogLevel:      "INFO",
	} // TODO: Add real fields to nCfg
	n, err := node.New(nCfg, store)
	if err != nil {
		log.Fatal(err)
	}

	mgr := delivery.NewManager(n, store, 0) // Default reconcile interval
	serv := server.NewServer(n, store, mgr, server.Config{
		PeerHTTP: nil, // TODO: Fix?
	})

	go mgr.Run()
	err = http.ListenAndServe(":9000", serv.Routes())
	if err != nil {
		log.Fatal(err)
	}
}
