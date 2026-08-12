package sink_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"dist-mq/delivery"
	"dist-mq/e2e-tests/payload"
	"dist-mq/e2e-tests/sink"
)

func start(t *testing.T, mode sink.Mode, subs []string) (*sink.Server, string) {
	t.Helper()
	s, err := sink.New(sink.Config{Addr: "127.0.0.1:0", Subscribers: subs, Mode: mode})
	if err != nil {
		t.Fatalf("sink.New: %v", err)
	}
	go func() { _ = s.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})
	return s, "http://" + s.Addr()
}

// deliver posts the way the broker does: SubURL with DeliveryPath appended.
func deliver(t *testing.T, s *sink.Server, base, sub string, body []byte) int {
	t.Helper()
	url := s.SubURL(base, sub) + delivery.DeliveryPath
	resp, err := http.Post(url, "text/plain", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func getJSON[T any](t *testing.T, url string) T {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	var out T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding %s: %v", url, err)
	}
	return out
}

// The convention is split across two repos' worth of assumptions — the sink
// builds SubURL, the broker appends DeliveryPath — and if they disagree every
// delivery 404s while the run still looks like it is doing something. Pinning
// it against the broker's own constant is the only way that stays honest.
func TestDeliveryPathMatchesBrokerConvention(t *testing.T) {
	s, base := start(t, sink.ModeRecord, sink.Names(1))

	if code := deliver(t, s, base, "sub-0", payload.Encode("tok-1", 32)); code != http.StatusOK {
		t.Fatalf("delivery status = %d, want 200", code)
	}

	rec := getJSON[sink.Records](t, base+"/records")
	if n := rec.Subscribers["sub-0"]["tok-1"]; n != 1 {
		t.Fatalf("sub-0 saw tok-1 %d times, want 1 (records: %v)", n, rec.Subscribers)
	}
}

func TestRecordsTokensAndCountsDuplicates(t *testing.T) {
	s, base := start(t, sink.ModeRecord, sink.Names(1))

	deliver(t, s, base, "sub-0", payload.Encode("a", 16))
	deliver(t, s, base, "sub-0", payload.Encode("b", 16))
	deliver(t, s, base, "sub-0", payload.Encode("a", 16)) // at-least-once redelivery

	st := getJSON[sink.Stats](t, base+"/stats")
	got := st.Subscribers[0]
	if got.Delivered != 3 || got.Unique != 2 || got.Duplicates != 1 {
		t.Fatalf("delivered=%d unique=%d duplicates=%d, want 3/2/1", got.Delivered, got.Unique, got.Duplicates)
	}
}

// A mistyped SubURL that quietly recorded would produce a run that verifies
// nothing while reporting success.
func TestUnknownSubscriberIsRefused(t *testing.T) {
	s, base := start(t, sink.ModeRecord, sink.Names(1))

	if code := deliver(t, s, base, "sub-9", payload.Encode("x", 8)); code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", code)
	}

	st := getJSON[sink.Stats](t, base+"/stats")
	if st.Unknown != 1 {
		t.Fatalf("unknown = %d, want 1", st.Unknown)
	}
	if st.Delivered != 0 {
		t.Fatalf("delivered = %d, want 0", st.Delivered)
	}
}

// Benchmarks run at rates where retaining every token would put this process's
// allocator inside the measurement.
func TestCountModeRetainsNoTokens(t *testing.T) {
	s, base := start(t, sink.ModeCount, sink.Names(1))

	deliver(t, s, base, "sub-0", payload.Encode("a", 16))
	deliver(t, s, base, "sub-0", payload.Encode("b", 16))

	st := getJSON[sink.Stats](t, base+"/stats")
	if st.Delivered != 2 {
		t.Fatalf("delivered = %d, want 2", st.Delivered)
	}
	if st.Subscribers[0].Unique != 0 {
		t.Fatalf("unique = %d, want 0 in count mode", st.Subscribers[0].Unique)
	}

	rec := getJSON[sink.Records](t, base+"/records")
	if len(rec.Subscribers["sub-0"]) != 0 {
		t.Fatalf("records = %v, want empty in count mode", rec.Subscribers["sub-0"])
	}
}

func TestFanOutIsRecordedPerSubscriber(t *testing.T) {
	s, base := start(t, sink.ModeRecord, sink.Names(3))

	for _, name := range sink.Names(3) {
		deliver(t, s, base, name, payload.Encode("shared", 16))
	}

	rec := getJSON[sink.Records](t, base+"/records")
	for _, name := range sink.Names(3) {
		if rec.Subscribers[name]["shared"] != 1 {
			t.Fatalf("%s did not record the message: %v", name, rec.Subscribers)
		}
	}

	st := getJSON[sink.Stats](t, base+"/stats")
	if st.Delivered != 3 || len(st.Subscribers) != 3 {
		t.Fatalf("delivered=%d subs=%d, want 3/3", st.Delivered, len(st.Subscribers))
	}
}

// One sink process serves many benchmark repetitions, so a run has to be able
// to state what it alone delivered.
func TestResetZeroesTallies(t *testing.T) {
	s, base := start(t, sink.ModeRecord, sink.Names(1))

	deliver(t, s, base, "sub-0", payload.Encode("a", 16))
	deliver(t, s, base, "sub-9", payload.Encode("b", 16)) // bumps unknown

	resp, err := http.Post(base+"/reset", "", nil)
	if err != nil {
		t.Fatalf("POST /reset: %v", err)
	}
	resp.Body.Close()

	st := getJSON[sink.Stats](t, base+"/stats")
	if st.Delivered != 0 || st.Unknown != 0 || st.Subscribers[0].Unique != 0 {
		t.Fatalf("after reset: delivered=%d unknown=%d unique=%d, want all 0",
			st.Delivered, st.Unknown, st.Subscribers[0].Unique)
	}

	deliver(t, s, base, "sub-0", payload.Encode("c", 16))
	if st := getJSON[sink.Stats](t, base+"/stats"); st.Delivered != 1 {
		t.Fatalf("delivered after reset = %d, want 1", st.Delivered)
	}
}

func TestRejectsBadConfig(t *testing.T) {
	cases := map[string]sink.Config{
		"no subscribers":  {Addr: "127.0.0.1:0"},
		"duplicate names": {Addr: "127.0.0.1:0", Subscribers: []string{"a", "a"}},
		"empty name":      {Addr: "127.0.0.1:0", Subscribers: []string{""}},
		"unknown mode":    {Addr: "127.0.0.1:0", Subscribers: []string{"a"}, Mode: "guess"},
	}
	for name, cfg := range cases {
		if _, err := sink.New(cfg); err == nil {
			t.Errorf("%s: New returned no error", name)
		}
	}
}
