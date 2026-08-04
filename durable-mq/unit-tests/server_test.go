package unit_tests

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"durable-mq/catalog"
	"durable-mq/queue"
	"durable-mq/server"
)

// newTestServer builds the real route table over a broker isolated onto a
// fresh WAL directory. t.Chdir moves the process working directory, so these
// tests must not call t.Parallel.
func newTestServer(t *testing.T) http.Handler {
	t.Helper()
	t.Chdir(t.TempDir())

	b := queue.NewBroker()
	if err := b.RestoreWAL(); err != nil {
		t.Fatalf("RestoreWAL returned error: %v", err)
	}
	return server.NewServer(b).Routes()
}

func doRequest(t *testing.T, h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, target, r))
	return rec
}

func mustStatus(t *testing.T, rec *httptest.ResponseRecorder, want int, what string) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("%s: status = %d, want %d (body: %s)", what, rec.Code, want, strings.TrimSpace(rec.Body.String()))
	}
}

func listQueues(t *testing.T, h http.Handler) []catalog.QueueInfo {
	t.Helper()
	rec := doRequest(t, h, http.MethodGet, "/queues", "")
	mustStatus(t, rec, http.StatusOK, "GET /queues")

	var queues []catalog.QueueInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &queues); err != nil {
		t.Fatalf("GET /queues returned undecodable JSON %q: %v", rec.Body.String(), err)
	}
	return queues
}

func TestServerCreateQueue(t *testing.T) {
	h := newTestServer(t)

	rec := doRequest(t, h, http.MethodPost, "/queues/orders", "")
	mustStatus(t, rec, http.StatusCreated, "create queue")

	queues := listQueues(t, h)
	if len(queues) != 1 || queues[0].Name != "orders" {
		t.Errorf("queues = %+v, want a single queue named orders", queues)
	}
}

func TestServerCreateDuplicateQueueIsRejected(t *testing.T) {
	h := newTestServer(t)

	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders", ""), http.StatusCreated, "first create")
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders", ""), http.StatusBadRequest, "duplicate create")

	if queues := listQueues(t, h); len(queues) != 1 {
		t.Errorf("got %d queues after a rejected duplicate, want 1", len(queues))
	}
}

func TestServerListQueuesEmptyReturnsJSONArray(t *testing.T) {
	h := newTestServer(t)

	rec := doRequest(t, h, http.MethodGet, "/queues", "")
	mustStatus(t, rec, http.StatusOK, "GET /queues")

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	// An empty array, not null — clients shouldn't have to special-case it.
	if got := strings.TrimSpace(rec.Body.String()); got != "[]" {
		t.Errorf("body = %q, want []", got)
	}
}

func TestServerDeleteQueue(t *testing.T) {
	h := newTestServer(t)
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders", ""), http.StatusCreated, "create")

	mustStatus(t, doRequest(t, h, http.MethodDelete, "/queues/orders", ""), http.StatusNoContent, "delete")

	if queues := listQueues(t, h); len(queues) != 0 {
		t.Errorf("got %d queues after deletion, want 0", len(queues))
	}
}

func TestServerDeleteMissingQueueIsRejected(t *testing.T) {
	h := newTestServer(t)
	mustStatus(t, doRequest(t, h, http.MethodDelete, "/queues/ghost", ""), http.StatusBadRequest, "delete missing queue")
}

func TestServerEnqueue(t *testing.T) {
	h := newTestServer(t)
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders", ""), http.StatusCreated, "create")

	rec := doRequest(t, h, http.MethodPost, "/queues/orders/messages", "hello world")
	mustStatus(t, rec, http.StatusAccepted, "enqueue")
}

func TestServerEnqueueToMissingQueueIsRejected(t *testing.T) {
	h := newTestServer(t)
	rec := doRequest(t, h, http.MethodPost, "/queues/ghost/messages", "hello")
	mustStatus(t, rec, http.StatusBadRequest, "enqueue to missing queue")
}

func TestServerEnqueueEmptyBody(t *testing.T) {
	h := newTestServer(t)
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders", ""), http.StatusCreated, "create")

	// An empty payload is a valid message, not a client error.
	rec := doRequest(t, h, http.MethodPost, "/queues/orders/messages", "")
	mustStatus(t, rec, http.StatusAccepted, "enqueue empty body")
}

func TestServerAddSubscriber(t *testing.T) {
	h := newTestServer(t)
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders", ""), http.StatusCreated, "create")

	body := `{"SubName":"sub1","SubURL":"http://example.com","NumberOfRetries":3}`
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders/subscribers", body), http.StatusCreated, "add subscriber")

	queues := listQueues(t, h)
	if len(queues) != 1 {
		t.Fatalf("got %d queues, want 1", len(queues))
	}
	sub, ok := queues[0].SubPolicies["sub1"]
	if !ok {
		t.Fatalf("sub1 missing from %+v", queues[0].SubPolicies)
	}
	if sub.SubURL != "http://example.com" || sub.NumberOfRetries != 3 {
		t.Errorf("subscriber = %+v, want SubURL=http://example.com NumberOfRetries=3", sub)
	}
}

func TestServerAddSubscriberRejectsBadJSON(t *testing.T) {
	h := newTestServer(t)
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders", ""), http.StatusCreated, "create")

	rec := doRequest(t, h, http.MethodPost, "/queues/orders/subscribers", "{not json")
	mustStatus(t, rec, http.StatusBadRequest, "add subscriber with malformed JSON")

	if queues := listQueues(t, h); len(queues[0].SubPolicies) != 0 {
		t.Error("a subscriber was registered despite a malformed request body")
	}
}

func TestServerAddSubscriberToMissingQueueIsRejected(t *testing.T) {
	h := newTestServer(t)

	body := `{"SubName":"sub1","SubURL":"http://example.com","NumberOfRetries":3}`
	rec := doRequest(t, h, http.MethodPost, "/queues/ghost/subscribers", body)
	mustStatus(t, rec, http.StatusBadRequest, "add subscriber to missing queue")
}

func TestServerUpdateSubscriber(t *testing.T) {
	h := newTestServer(t)
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders", ""), http.StatusCreated, "create")

	create := `{"SubName":"sub1","SubURL":"http://before.com","NumberOfRetries":1}`
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders/subscribers", create), http.StatusCreated, "add subscriber")

	update := `{"SubName":"sub1","SubURL":"http://after.com","NumberOfRetries":7}`
	mustStatus(t, doRequest(t, h, http.MethodPut, "/queues/orders/subscribers/sub1", update), http.StatusOK, "update subscriber")

	queues := listQueues(t, h)
	if len(queues[0].SubPolicies) != 1 {
		t.Fatalf("got %d subscribers after update, want 1", len(queues[0].SubPolicies))
	}
	sub := queues[0].SubPolicies["sub1"]
	if sub.SubURL != "http://after.com" || sub.NumberOfRetries != 7 {
		t.Errorf("subscriber = %+v, want the updated URL and retry count", sub)
	}
}

func TestServerUpdateSubscriberPathWinsOverBody(t *testing.T) {
	h := newTestServer(t)
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders", ""), http.StatusCreated, "create")

	create := `{"SubName":"sub1","SubURL":"http://before.com","NumberOfRetries":1}`
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders/subscribers", create), http.StatusCreated, "add subscriber")

	// Body claims a different identity than the path. The path is
	// authoritative, so this must update sub1 rather than create "impostor".
	conflicting := `{"SubName":"impostor","SubURL":"http://after.com","NumberOfRetries":9}`
	mustStatus(t, doRequest(t, h, http.MethodPut, "/queues/orders/subscribers/sub1", conflicting), http.StatusOK, "update with conflicting body")

	subs := listQueues(t, h)[0].SubPolicies
	if _, ok := subs["impostor"]; ok {
		t.Error("the body's SubName created a second subscriber; the path should be authoritative")
	}
	if len(subs) != 1 {
		t.Fatalf("got %d subscribers, want 1", len(subs))
	}
	if subs["sub1"].SubURL != "http://after.com" || subs["sub1"].NumberOfRetries != 9 {
		t.Errorf("sub1 = %+v, want the body's non-identity fields applied", subs["sub1"])
	}
}

func TestServerUpdateSubscriberRejectsBadJSON(t *testing.T) {
	h := newTestServer(t)
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders", ""), http.StatusCreated, "create")

	rec := doRequest(t, h, http.MethodPut, "/queues/orders/subscribers/sub1", "{not json")
	mustStatus(t, rec, http.StatusBadRequest, "update subscriber with malformed JSON")
}

func TestServerDeleteSubscriber(t *testing.T) {
	h := newTestServer(t)
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders", ""), http.StatusCreated, "create")

	body := `{"SubName":"sub1","SubURL":"http://example.com","NumberOfRetries":3}`
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders/subscribers", body), http.StatusCreated, "add subscriber")

	mustStatus(t, doRequest(t, h, http.MethodDelete, "/queues/orders/subscribers/sub1", ""), http.StatusNoContent, "delete subscriber")

	if subs := listQueues(t, h)[0].SubPolicies; len(subs) != 0 {
		t.Errorf("got %d subscribers after deletion, want 0", len(subs))
	}
}

func TestServerDeleteSubscriberOnMissingQueueIsRejected(t *testing.T) {
	h := newTestServer(t)
	rec := doRequest(t, h, http.MethodDelete, "/queues/ghost/subscribers/sub1", "")
	mustStatus(t, rec, http.StatusBadRequest, "delete subscriber on missing queue")
}

func TestServerMultipleQueuesWithSubscribers(t *testing.T) {
	h := newTestServer(t)

	for _, q := range []string{"orders", "events"} {
		mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/"+q, ""), http.StatusCreated, "create "+q)
	}

	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders/subscribers",
		`{"SubName":"a","SubURL":"http://a.com","NumberOfRetries":1}`), http.StatusCreated, "add a")
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders/subscribers",
		`{"SubName":"b","SubURL":"http://b.com","NumberOfRetries":2}`), http.StatusCreated, "add b")
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/events/subscribers",
		`{"SubName":"c","SubURL":"http://c.com","NumberOfRetries":3}`), http.StatusCreated, "add c")

	byName := map[string]catalog.QueueInfo{}
	for _, qi := range listQueues(t, h) {
		byName[qi.Name] = qi
	}
	if len(byName) != 2 {
		t.Fatalf("got %d queues, want 2", len(byName))
	}
	if len(byName["orders"].SubPolicies) != 2 {
		t.Errorf("orders has %d subscribers, want 2", len(byName["orders"].SubPolicies))
	}
	if len(byName["events"].SubPolicies) != 1 {
		t.Errorf("events has %d subscribers, want 1", len(byName["events"].SubPolicies))
	}
	if _, leaked := byName["events"].SubPolicies["a"]; leaked {
		t.Error("a subscriber registered on orders showed up under events")
	}
}

func TestServerRouteTable(t *testing.T) {
	h := newTestServer(t)
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/orders", ""), http.StatusCreated, "create")

	// Method/path combinations the route table deliberately doesn't serve.
	// 405 means the path matched but the method didn't; 404 means no route.
	cases := []struct {
		method, target string
		want           int
	}{
		{http.MethodGet, "/queues/orders", http.StatusMethodNotAllowed},
		{http.MethodPut, "/queues/orders", http.StatusMethodNotAllowed},
		{http.MethodGet, "/queues/orders/messages", http.StatusMethodNotAllowed},
		{http.MethodPost, "/queues", http.StatusMethodNotAllowed},
		{http.MethodGet, "/nope", http.StatusNotFound},
	}
	for _, tc := range cases {
		rec := doRequest(t, h, tc.method, tc.target, "")
		if rec.Code != tc.want {
			t.Errorf("%s %s: status = %d, want %d", tc.method, tc.target, rec.Code, tc.want)
		}
	}
}

func TestServerQueueNameWithSpecialCharacters(t *testing.T) {
	h := newTestServer(t)

	// Path segments are percent-decoded before reaching PathValue, so a
	// space in the name must survive the round trip through routing.
	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/my%20queue", ""), http.StatusCreated, "create")

	queues := listQueues(t, h)
	if len(queues) != 1 || queues[0].Name != "my queue" {
		t.Fatalf("queues = %+v, want a single queue named %q", queues, "my queue")
	}

	mustStatus(t, doRequest(t, h, http.MethodPost, "/queues/my%20queue/messages", "hi"), http.StatusAccepted, "enqueue")
}
