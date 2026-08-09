package delivery

import (
	"context"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"dist-mq/model"
)

const (
	deliveryTimeout     = 10 * time.Second
	deliveryPath        = "/queue/message"
	maxIdleConnsPerHost = 64
)

type deliveryResult struct {
	subName string
	ok      bool
}

type worker struct {
	queue  *Queue
	stopCh <-chan struct{}
	client *http.Client

	onAck  func(msgID string, subNames []string) error
	onDone func(msgID string)
}

// NewDeliveryClient is shared across all workers so connections are pooled
func NewDeliveryClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = maxIdleConnsPerHost
	return &http.Client{Transport: transport, Timeout: deliveryTimeout}
}

func newWorker(q *Queue, stopCh <-chan struct{}, client *http.Client,
	onAck func(msgID string, subNames []string) error, onDone func(msgID string)) *worker {
	return &worker{queue: q, stopCh: stopCh, client: client, onAck: onAck, onDone: onDone}
}

func (w *worker) run() {
	for {
		for {
			// Checked per message so a demotion mid-backlog stops promptly
			// instead of draining thousands of messages first.
			if w.stopped() {
				return
			}
			msg := w.queue.Pop()
			if msg == nil {
				break
			}
			w.process(msg)
		}

		select {
		case <-w.queue.HasWork():
		case <-w.stopCh:
			return
		}
	}
}

func (w *worker) stopped() bool {
	select {
	case <-w.stopCh:
		return true
	default:
		return false
	}
}

func (w *worker) process(msg *QueueMsg) {
	pending := msg.PendingSubs()
	if len(pending) == 0 {
		w.onDone(msg.MsgID)
		return
	}

	results := make(chan deliveryResult, len(pending))
	var wg sync.WaitGroup
	for _, sub := range pending {
		wg.Add(1)
		go func(sub model.SubPolicy) {
			defer wg.Done()
			results <- deliveryResult{subName: sub.SubName, ok: w.send(sub, msg)}
		}(sub)
	}
	wg.Wait()
	close(results)

	// finished = ack or expired retries
	var finished []string
	for result := range results {
		if result.ok {
			finished = append(finished, result.subName)
			continue
		}
		msg.RetryMap[result.subName]++
		if msg.RetryMap[result.subName] >= pending[result.subName].NumberOfRetries {
			finished = append(finished, result.subName)
		}
	}
	sort.Strings(finished)

	// Demoted mid-request, drop
	if w.stopped() {
		return
	}

	if len(finished) > 0 {
		if err := w.onAck(msg.MsgID, finished); err != nil {
			// Failed Ack callback means log possibly did not receive, requeue
			w.queue.Add(msg)
			return
		}
		for _, subName := range finished {
			msg.AckedSubs[subName] = struct{}{}
		}
	}

	if msg.Done() {
		w.onDone(msg.MsgID)
		return
	}
	w.queue.Add(msg)
}

func (w *worker) send(sub model.SubPolicy, msg *QueueMsg) bool {
	ctx, cancel := context.WithTimeout(context.Background(), deliveryTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		sub.SubURL+deliveryPath, strings.NewReader(msg.Payload))
	if err != nil {
		return false
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	// Drain before closing or the connection cannot be reused.
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode >= 200 && resp.StatusCode < 300
}
