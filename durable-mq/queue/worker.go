package queue

import (
	"bytes"
	"context"
	"durable-mq/model"
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
	// Obtain list of non-acked Subs from the message's own snapshot —
	// no catalog/queue lock needed, this is per-message immutable data
	policyByName := make(map[string]model.SubPolicy, len(msg.SubPolicySnapshot))
	for name, policy := range msg.SubPolicySnapshot {
		if _, ok := msg.AckedSubs[name]; ok {
			continue
		}
		policyByName[name] = policy
	}

	if len(policyByName) == 0 {
		return
	}

	results := make(chan deliveryResult, len(policyByName))
	var wg sync.WaitGroup
	for _, sub := range policyByName {
		wg.Add(1)
		go func(sub model.SubPolicy) {
			defer wg.Done()
			ok := q.sendMsg(&sub, msg)
			results <- deliveryResult{subName: sub.SubName, success: ok}
		}(sub)
	}
	wg.Wait()
	close(results)

	anySubsRemaining := false
	for result := range results {
		acked := false
		if result.success {
			acked = true
		} else {
			msg.RetryMap[result.subName]++
			policy := policyByName[result.subName]
			if msg.RetryMap[result.subName] >= policy.NumberOfRetries {
				acked = true // giving up counts as "done" for this subscriber
			}
		}

		if acked {
			if err := q.onAck(msg.MsgId, result.subName); err != nil {
				// WAL append failed — don't mark acked in memory;
				// safe to retry next pass rather than risk an
				// in-memory ack the durable log never recorded
				anySubsRemaining = true
				continue
			}
			msg.AckedSubs[result.subName] = struct{}{}
		} else {
			anySubsRemaining = true
		}
	}

	if anySubsRemaining {
		q.Add(msg)
	}
}

func (q *Queue) sendMsg(sub *model.SubPolicy, msg *QueueMsg) bool {

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
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false
	}
	return true
}
