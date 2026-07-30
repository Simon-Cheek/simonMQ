package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	queue := flag.String("queue", "", "queue name to publish to (required)")
	broker := flag.String("broker", "http://localhost:8081", "base URL of the broker")
	outFile := flag.String("out", "published.txt", "file to append accepted message contents to")
	rate := flag.Float64("rate", 1, "messages per second")
	flag.Parse()

	if *queue == "" {
		fmt.Fprintln(os.Stderr, "missing required -queue flag")
		os.Exit(1)
	}
	if *rate <= 0 {
		fmt.Fprintln(os.Stderr, "-rate must be > 0")
		os.Exit(1)
	}

	f, err := os.OpenFile(*outFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open output file: %v", err)
	}
	defer f.Close()

	url := fmt.Sprintf("%s/queues/%s/messages", *broker, *queue)
	interval := time.Duration(float64(time.Second) / *rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("publishing to %s at %.2f msg/s, logging accepted messages to %s", url, *rate, *outFile)

	var counter uint64
	for range ticker.C {
		body := fmt.Sprintf("%d", counter)
		counter++

		resp, err := http.Post(url, "text/plain", bytes.NewBufferString(body))
		if err != nil {
			log.Printf("publish failed for msg %s: %v", body, err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusAccepted {
			log.Printf("msg %s got unexpected status %d", body, resp.StatusCode)
			continue
		}

		if _, err := f.WriteString(body + "\n"); err != nil {
			log.Printf("failed to record accepted msg %s: %v", body, err)
		}
	}
}
