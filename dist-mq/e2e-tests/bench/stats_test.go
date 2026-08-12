package bench

import (
	"testing"
	"time"

	"dist-mq/e2e-tests/client"
)

func TestWarmupSamplesAreExcluded(t *testing.T) {
	samples := []sample{
		{offset: time.Second, latency: 100 * time.Millisecond, outcome: client.Accepted},   // warm-up
		{offset: 6 * time.Second, latency: 2 * time.Millisecond, outcome: client.Accepted}, // counted
		{offset: 7 * time.Second, latency: 4 * time.Millisecond, outcome: client.Accepted}, // counted
	}
	s := summarize(samples, 5*time.Second, 10*time.Second)

	if s.Attempted != 2 || s.Accepted != 2 {
		t.Fatalf("attempted=%d accepted=%d, want 2/2", s.Attempted, s.Accepted)
	}
	// The 100ms warm-up sample would dominate the tail if it leaked through.
	if s.Latency.Max != 4 {
		t.Fatalf("max = %.2fms, want 4 — a warm-up sample reached the summary", s.Latency.Max)
	}
}

// Only accepted publishes carry latency: a rejected write's duration is the
// cost of being turned away, and folding it in flatters the result.
func TestOnlyAcceptedContributeLatency(t *testing.T) {
	samples := []sample{
		{offset: time.Second, latency: 5 * time.Millisecond, outcome: client.Accepted},
		{offset: time.Second, latency: 900 * time.Millisecond, outcome: client.Rejected},
		{offset: time.Second, latency: 800 * time.Millisecond, outcome: client.Ambiguous},
	}
	s := summarize(samples, 0, 10*time.Second)

	if s.Accepted != 1 || s.Rejected != 1 || s.Ambiguous != 1 {
		t.Fatalf("accepted=%d rejected=%d ambiguous=%d, want 1/1/1", s.Accepted, s.Rejected, s.Ambiguous)
	}
	if s.Latency.Max != 5 {
		t.Fatalf("max = %.2fms, want 5 — a non-accepted sample contributed latency", s.Latency.Max)
	}
}

func TestQuantileIsNearestRank(t *testing.T) {
	v := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	cases := map[float64]float64{0.50: 5, 0.90: 9, 0.99: 10}
	for q, want := range cases {
		if got := quantile(v, q); got != want {
			t.Errorf("quantile(%.2f) = %.1f, want %.1f", q, got, want)
		}
	}
}

func TestEmptyRunDoesNotPanic(t *testing.T) {
	s := summarize(nil, 0, 10*time.Second)
	if s.Attempted != 0 || s.Latency.P99 != 0 {
		t.Fatalf("empty summary = %+v", s)
	}
}
