package main

import (
	"dist-mq/delivery"
	"dist-mq/node"
	"dist-mq/storage"
	"log"
)

func main() {

	// Instantiate Dependencies
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
}
