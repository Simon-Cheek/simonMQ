package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"time"
)

func main() {
	port := flag.String("port", "9091", "port to listen on")
	name := flag.String("name", "subscriber", "subscriber name, used in log lines")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /queue/message", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		log.Printf("[%s] received message (%d bytes): %s", *name, len(body), body)
		w.WriteHeader(http.StatusOK)
	})

	addr := ":" + *port
	log.Printf("[%s] listening on %s", *name, addr)
	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
