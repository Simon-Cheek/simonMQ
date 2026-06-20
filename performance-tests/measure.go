package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// N workers post to N queues, N workers receive from N queues (2N workers in total)
func measure(seconds int, numQueues int, port string) (int, int) {
	var totalSent int64
	var totalReceived int64
	var noContentRes int64

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        2*numQueues + 10, // total idle conns kept around
			MaxIdleConnsPerHost: 2*numQueues + 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < numQueues; i++ {
		queueURL := fmt.Sprintf("http://localhost:"+port+"/queues/queue-%d", i) // base URL — decide host/port

		wg.Add(1)
		go func(qURL string) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// build payload, call doPost, decide what counts as "sent"
				payload, _ := json.Marshal(map[string]string{"payload": uuid.NewString()})
				status, err := doPost(client, qURL+"/messages", payload)
				if err == nil && status == http.StatusAccepted {
					atomic.AddInt64(&totalSent, 1)
				} else {
					fmt.Printf("Invalid response with status %d from queue %s: %s", status, qURL, err)
				}
			}
		}(queueURL)

		wg.Add(1)
		go func(qURL string) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// call doGet, branch on status, decide what counts as "received"
				status, err := doGet(client, qURL+"/messages/next")
				if err == nil && status == http.StatusOK {
					atomic.AddInt64(&totalReceived, 1)
				} else if err != nil {
					fmt.Printf("Invalid response with status %d from queue %s: %s", status, qURL, err)
				} else if status == http.StatusNoContent {
					atomic.AddInt64(&noContentRes, 1)
				}
			}
		}(queueURL)
	}

	time.Sleep(time.Duration(seconds) * time.Second)
	close(stop)
	wg.Wait()

	return int(atomic.LoadInt64(&totalSent) + atomic.LoadInt64(&totalReceived)), int(atomic.LoadInt64(&noContentRes))
}

func doPost(client *http.Client, url string, body []byte) (status int, err error) {
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) // drain body so the connection can be reused
	return resp.StatusCode, nil
}

func doGet(client *http.Client, url string) (status int, err error) {
	resp, err := client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}
