package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

const baseURL = "http://localhost:8080"

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
