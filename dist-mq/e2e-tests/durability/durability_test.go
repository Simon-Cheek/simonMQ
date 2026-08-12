package durability

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"dist-mq/e2e-tests/payload"
	"dist-mq/e2e-tests/sink"
	"dist-mq/e2e-tests/verify"
)

// TestAcceptedMessagesAreDelivered is the whole suite: a write the cluster
// acknowledged is on a quorum of disks, so every subscriber registered at
// enqueue time must eventually see it.
//
// It is deliberately indifferent to what else is happening. Run it alone and
// it is a plain durability check; run it while chaos is killing nodes and the
// same assertion becomes a failover test, because a message that survives its
// leader being destroyed is the thing worth proving.
func TestAcceptedMessagesAreDelivered(t *testing.T) {
	ctx := context.Background()
	resetSink(t)

	pubs := publish(t, ctx, *msgCount)

	if err := shared.h.WaitDrained(ctx, *drainTimeout); err != nil {
		// Not fatal on its own. The records are the evidence, and a drain that
		// timed out while everything still arrived is a slow cluster, not a
		// lost message.
		t.Logf("drain did not complete: %v", err)
	}
	// Delivery is scheduled after the commit, so replicated state can be empty
	// a moment before the last POST lands on the sink.
	settle(t)

	rec := fetchRecords(t)
	rep := verify.Check(pubs, rec.Subscribers, shared.subs)
	t.Log(rep)

	if err := rep.Err(); err != nil {
		t.Fatalf("durability violated: %v", err)
	}
	if rep.Accepted == 0 {
		t.Fatal("no writes were accepted — the run proved nothing")
	}
}

// publish writes n uniquely tokened messages across the queues and returns
// what the cluster said about each. Outcomes are recorded rather than
// asserted: classifying them is verify's job.
func publish(t *testing.T, ctx context.Context, n int) []verify.Publication {
	t.Helper()

	pubs := make([]verify.Publication, n)
	run := time.Now().UnixNano()

	const parallel = 16
	var wg sync.WaitGroup
	work := make(chan int)

	for w := 0; w < parallel; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				token := fmt.Sprintf("%d-%d", run, i)
				queue := shared.queues[i%len(shared.queues)]
				res := shared.client.Enqueue(ctx, queue, payload.Encode(token, *payloadLen))
				pubs[i] = verify.Publication{Token: token, Outcome: res.Outcome}
			}
		}()
	}
	for i := 0; i < n; i++ {
		work <- i
	}
	close(work)
	wg.Wait()

	return pubs
}

// settle waits for the sink to go quiet. Two consecutive polls with no new
// deliveries means the cluster has stopped, whether or not it stopped where
// the test hoped — which is the point, since where it stopped is the finding.
func settle(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	var last uint64
	quiet := 0

	for time.Now().Before(deadline) {
		st := fetchStats(t)
		if st.Delivered == last {
			if quiet++; quiet >= 2 {
				return
			}
		} else {
			quiet = 0
		}
		last = st.Delivered
		time.Sleep(500 * time.Millisecond)
	}
}

func resetSink(t *testing.T) {
	t.Helper()
	resp, err := http.Post(shared.sink+"/reset", "", nil)
	if err != nil {
		t.Fatalf("resetting sink at %s: %v", shared.sink, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("resetting sink: status %d", resp.StatusCode)
	}
}

func fetchRecords(t *testing.T) sink.Records {
	t.Helper()
	var rec sink.Records
	fetch(t, shared.sink+"/records", &rec)
	if rec.Mode != sink.ModeRecord {
		t.Fatalf("sink is in %q mode; durability needs record mode", rec.Mode)
	}
	return rec
}

func fetchStats(t *testing.T) sink.Stats {
	t.Helper()
	var st sink.Stats
	fetch(t, shared.sink+"/stats", &st)
	return st
}

func fetch(t *testing.T, url string, into any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		t.Fatalf("decoding %s: %v", url, err)
	}
}
