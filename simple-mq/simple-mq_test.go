package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
)

// Requires local server to be running
const baseURL = "http://localhost:8080"

// Simple test verifying 1 Enqueue and 1 Dequeue
func TestEnqueueThenDequeue(t *testing.T) {
	queue := "myqueue"
	payload := "payload"
	err := enqueue(queue, payload)
	if err != nil {
		t.Error(err)
	}
	res, err := dequeue(queue)
	if err != nil {
		t.Error(err)
	}
	if res != payload {
		t.Errorf("got %s, want %s", res, payload)
	}
}

// Testing concurrent enqueues to different queues
func TestConcurrentEnqueueToDifferentQueues(t *testing.T) {
	// Test Setup
	drainQueueContents("q1")
	drainQueueContents("q2")
	drainQueueContents("q3")
	queue1Payloads := []string{"Q1payload1", "Q1payload2", "Q1payload3", "Q1payload4", "Q1payload5", "Q1payload6", "Q1payload7", "Q1payload8"}
	queue2Payloads := []string{"Q2payload1", "Q2payload2", "Q2payload3", "Q2payload4", "Q2payload5", "Q2payload6", "Q2payload7", "Q2payload8"}
	queue3Payloads := []string{"Q3payload1", "Q3payload2", "Q3payload3", "Q3payload4", "Q3payload5", "Q3payload6", "Q3payload7", "Q3payload8"}

	// Load in separate queues concurrently
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		for _, payload := range queue1Payloads {
			err := enqueue("q1", payload)
			if err != nil {
				t.Error(err)
			}
		}
		wg.Done()
	}()

	go func() {
		for _, payload := range queue2Payloads {
			err := enqueue("q2", payload)
			if err != nil {
				t.Error(err)
			}
		}
		wg.Done()
	}()

	go func() {
		for _, payload := range queue3Payloads {
			err := enqueue("q3", payload)
			if err != nil {
				t.Error(err)
			}
		}
		wg.Done()
	}()
	wg.Wait()

	// Verify all 3 queues filled in correct order
	q1Contents, err := drainQueueContents("q1")
	if err != nil {
		t.Error(err)
	}
	if !slices.Equal(q1Contents, queue1Payloads) {
		t.Errorf("got %s, want %s", q1Contents, queue2Payloads)
	}
	q2Contents, err := drainQueueContents("q2")
	if err != nil {
		t.Error(err)
	}
	if !slices.Equal(q2Contents, queue2Payloads) {
		t.Errorf("got %s, want %s", q1Contents, queue2Payloads)
	}
	q3Contents, err := drainQueueContents("q3")
	if err != nil {
		t.Error(err)
	}
	if !slices.Equal(q3Contents, queue3Payloads) {
		t.Errorf("got %s, want %s", q1Contents, queue3Payloads)
	}
}

// Testing concurrent enqueues to different queues
func TestConcurrentEnqueueToSameQueue(t *testing.T) {
	queueName := "concurrentQueue"
	// Test Setup
	drainQueueContents(queueName)
	queue1Payloads := []string{"Q1payload1", "Q1payload2", "Q1payload3", "Q1payload4", "Q1payload5", "Q1payload6", "Q1payload7", "Q1payload8"}
	queue2Payloads := []string{"Q2payload1", "Q2payload2", "Q2payload3", "Q2payload4", "Q2payload5", "Q2payload6", "Q2payload7", "Q2payload8"}
	queue3Payloads := []string{"Q3payload1", "Q3payload2", "Q3payload3", "Q3payload4", "Q3payload5", "Q3payload6", "Q3payload7", "Q3payload8"}
	totalPayloads := slices.Concat(slices.Concat(queue1Payloads, queue2Payloads), queue3Payloads)

	// Load in separate queues concurrently
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		for _, payload := range queue1Payloads {
			err := enqueue(queueName, payload)
			if err != nil {
				t.Error(err)
			}
		}
		wg.Done()
	}()

	go func() {
		for _, payload := range queue2Payloads {
			err := enqueue(queueName, payload)
			if err != nil {
				t.Error(err)
			}
		}
		wg.Done()
	}()

	go func() {
		for _, payload := range queue3Payloads {
			err := enqueue(queueName, payload)
			if err != nil {
				t.Error(err)
			}
		}
		wg.Done()
	}()
	wg.Wait()

	// Verify queue received all msgs
	qContents, err := drainQueueContents(queueName)
	if err != nil {
		t.Error(err)
	}
	sort.Strings(qContents)
	sort.Strings(totalPayloads)
	if !slices.Equal(qContents, totalPayloads) {
		t.Errorf("got %s, want %s", qContents, totalPayloads)
	}
}

func enqueue(queue string, payload string) error {
	resp, err := http.Post(baseURL+"/queues/"+queue+"/messages", "application/json", strings.NewReader(payload))
	if err != nil {
		return fmt.Errorf("enqueue request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status on enqueue: %d, body=%s", resp.StatusCode, raw)
	}
	fmt.Println("sent message: ", payload, " to queue: ", queue)
	return nil
}

func dequeue(queue string) (string, error) {
	resp, err := http.Get(baseURL + "/queues/" + queue + "/messages/next")
	if err != nil {
		return "", fmt.Errorf("dequeue request failed: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading body failed: %w", err)
	}

	if resp.StatusCode == http.StatusNoContent {
		fmt.Println("empty queue: ", queue)
		return "", nil
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status on dequeue: %d, body=%s", resp.StatusCode, raw)
	}

	var got map[string]string
	if err := json.Unmarshal(raw, &got); err != nil {
		return "", fmt.Errorf("decode failed: %w, raw=%s", err, raw)
	}

	fmt.Println("got message: ", got, " from queue: ", queue)
	return got["Payload"], nil
}

func drainQueueContents(queue string) ([]string, error) {
	contents := []string{}

	for {
		resp, err := dequeue(queue)
		if err != nil {
			return contents, fmt.Errorf("drain request failed: %w", err)
		}
		if resp == "" {
			return contents, nil
		}
		contents = append(contents, resp)
	}
}
