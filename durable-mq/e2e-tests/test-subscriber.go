package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"os"
)

func main() {
	outputFile := flag.String("o", "received.txt", "file to append received message bodies to")
	port := flag.String("p", "9090", "port to listen on")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /queue/message", func(w http.ResponseWriter, r *http.Request) {
		handleMessage(w, r, *outputFile)
	})

	log.Printf("e2e test server listening on :%s, appending to %s", *port, *outputFile)
	log.Fatal(http.ListenAndServe(":"+*port, mux))
}

func handleMessage(w http.ResponseWriter, r *http.Request, outputFile string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	if _, err := f.Write(append(body, '\n')); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
