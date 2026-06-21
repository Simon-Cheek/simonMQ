package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// fillThenDrain enqueues as many items as possible against a single queue for
// `seconds`, then measures how long it takes a single consumer to fully drain it.
func fillThenDrain(seconds int, numProducers int, port string) (enqueued int, drainDuration time.Duration) {
	queueURL := fmt.Sprintf("http://localhost:%s/queues/queue-0/messages", port)

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        numProducers + 10,
			MaxIdleConnsPerHost: numProducers + 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	var totalSent int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				payload, _ := json.Marshal(map[string]string{"payload": uuid.NewString()})
				status, err := doPost(client, queueURL, payload)
				if err == nil && status == http.StatusAccepted {
					atomic.AddInt64(&totalSent, 1)
				} else {
					fmt.Printf("enqueue error: status=%d err=%v\n", status, err)
				}
			}
		}()
	}

	time.Sleep(time.Duration(seconds) * time.Second)
	close(stop)
	wg.Wait()

	enqueued = int(atomic.LoadInt64(&totalSent))

	nextURL := fmt.Sprintf("http://localhost:%s/queues/queue-0/messages/next", port)

	start := time.Now()
	drained := 0
	for {
		status, err := doGet(client, nextURL)
		if err != nil {
			fmt.Printf("dequeue error: %v\n", err)
			continue
		}
		if status == http.StatusNoContent {
			break
		}
		if status == http.StatusOK {
			drained++
		}
	}
	drainDuration = time.Since(start)
	return enqueued, drainDuration
}

// Similar to FillThenDrain() but randomly multiplexes across numQueues
func fillThenDrainContention(seconds int, numProducers int, numQueues int, port string) (totalEnqueued int, totalDrainDuration time.Duration) {
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        numProducers + 10,
			MaxIdleConnsPerHost: numProducers + 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	enqueuedCounts := make([]int64, numQueues) // one atomic counter per queue

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}

				qIndex := rand.Intn(numQueues)
				url := fmt.Sprintf("http://localhost:%s/queues/queue-%d/messages", port, qIndex)

				payload, _ := json.Marshal(map[string]string{"payload": uuid.NewString()})
				status, err := doPost(client, url, payload)
				if err == nil && status == http.StatusAccepted {
					atomic.AddInt64(&enqueuedCounts[qIndex], 1)
				} else {
					fmt.Printf("enqueue error: status=%d err=%v\n", status, err)
				}
			}
		}()
	}

	time.Sleep(time.Duration(seconds) * time.Second)
	close(stop)
	wg.Wait()

	totalEnqueued = 0
	for i := range enqueuedCounts {
		totalEnqueued += int(atomic.LoadInt64(&enqueuedCounts[i]))
	}

	// --- Drain phase: sequential, one queue at a time ---
	for i := 0; i < numQueues; i++ {
		nextURL := fmt.Sprintf("http://localhost:%s/queues/queue-%d/messages/next", port, i)

		start := time.Now()
		drained := 0
		for {
			status, err := doGet(client, nextURL)
			if err != nil {
				fmt.Printf("dequeue error on queue-%d: %v\n", i, err)
				continue
			}
			if status == http.StatusNoContent {
				break
			}
			if status == http.StatusOK {
				drained++
			}
		}
		queueDrainTime := time.Since(start)
		totalDrainDuration += queueDrainTime
	}
	return
}

// N workers post to N queues, N workers receive from N queues (2N workers in total)
func measureThroughput(seconds int, numQueues int, port string) (int, int) {
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
