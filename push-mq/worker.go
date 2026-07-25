package main

import "sync"

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
		if _, ok := msg.ackedSubs[name]; ok {
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
			results <- deliveryResult{subName: sub.subName, success: ok}
		}(sub)
	}
	wg.Wait()
	close(results)

	// Increment retry on each, add to acked-subs if retry limit reached or success
	anySubsRemaining := false
	for result := range results {
		if result.success {
			msg.ackedSubs[result.subName] = struct{}{}
		} else {
			msg.retryMap[result.subName]++

			policy := policyByName[result.subName]
			if msg.retryMap[result.subName] >= policy.numberOfRetries {
				msg.ackedSubs[result.subName] = struct{}{}
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

// TODO: Implement
func (q *Queue) SendMsg(sub *SubPolicy, msg *QueueMsg) bool {
	return true
}
