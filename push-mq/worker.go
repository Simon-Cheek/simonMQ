package main

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"time"
)

type deliveryResult struct {
	subName string
	success bool
}

func (q *Queue) RunQueueWorker() {
	for {
		for {
			msg := q.Pop()
			if msg == nil {
				break
			}
			q.ProcessMsg(msg)
		}

		select {
		case <-q.hasWork:
			// loop back, go pop again
		case <-q.isClosed:
			return
		}
	}
}

func (q *Queue) ProcessMsg(msg *QueueMsg) {

	// Obtain list of non-acked Subs
	q.mu.Lock()
	policyByName := make(map[string]*SubPolicy, len(q.SubPolicies))
	for name, policy := range q.SubPolicies {
		if _, ok := msg.AckedSubs[name]; ok {
			continue
		}
		policyByName[name] = policy
	}
	q.mu.Unlock()

	if len(policyByName) == 0 {
		return
	}

	// For each, retry request
	results := make(chan deliveryResult, len(policyByName))
	var wg sync.WaitGroup
	for _, sub := range policyByName {
		wg.Add(1)
		go func(sub *SubPolicy) {
			defer wg.Done()
			ok := q.SendMsg(sub, msg)
			results <- deliveryResult{subName: sub.SubName, success: ok}
		}(sub)
	}
	wg.Wait()
	close(results)

	// Increment retry on each, add to acked-subs if retry limit reached or success
	anySubsRemaining := false
	for result := range results {
		if result.success {
			msg.AckedSubs[result.subName] = struct{}{}
		} else {
			msg.RetryMap[result.subName]++

			policy := policyByName[result.subName]
			if msg.RetryMap[result.subName] >= policy.NumberOfRetries {
				msg.AckedSubs[result.subName] = struct{}{}
			} else {
				anySubsRemaining = true
			}
		}
	}

	// Requeue if any failed retries under associated limit
	if anySubsRemaining {
		q.Add(msg)
	}
}

func (q *Queue) SendMsg(sub *SubPolicy, msg *QueueMsg) bool {

	payload := msg.Payload
	url := sub.SubURL
	client := &http.Client{
		Timeout: time.Second * 10,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	urlWithPath := url + "/queue/message"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, urlWithPath, bytes.NewBuffer([]byte(payload)))
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	// Any 2xx counts as delivered. Must stay identical to durable-mq's
	// sendMsg: the two are benchmarked against the same subscriber, and a
	// stricter rule here would turn every accepted delivery into a retry.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false
	}
	return true
}
