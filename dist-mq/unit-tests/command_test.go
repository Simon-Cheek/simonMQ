package unit_tests

import (
	"strings"
	"testing"

	"dist-mq/command"
	"dist-mq/model"
)

func roundTrip(t *testing.T, cmd command.Command) command.Command {
	t.Helper()
	data, err := command.Encode(cmd)
	if err != nil {
		t.Fatalf("Encode(%s) returned error: %v", cmd.Type, err)
	}
	got, err := command.Decode(data)
	if err != nil {
		t.Fatalf("Decode(%s) returned error: %v", cmd.Type, err)
	}
	if got.Type != cmd.Type {
		t.Fatalf("type = %s, want %s", got.Type, cmd.Type)
	}
	if got.QueueName != cmd.QueueName {
		t.Fatalf("queue name = %q, want %q", got.QueueName, cmd.QueueName)
	}
	return got
}

func TestCreateQueueRoundTrip(t *testing.T) {
	roundTrip(t, command.NewCreateQueue("orders"))
}

func TestDeleteQueueRoundTrip(t *testing.T) {
	roundTrip(t, command.NewDeleteQueue("orders"))
}

func TestPutSubPolicyRoundTrip(t *testing.T) {
	policy := model.SubPolicy{SubName: "billing", SubURL: "http://billing:9000", NumberOfRetries: 5}
	got := roundTrip(t, command.NewPutSubPolicy("orders", policy))
	if got.Policy != policy {
		t.Fatalf("policy = %+v, want %+v", got.Policy, policy)
	}
}

func TestDeleteSubPolicyRoundTrip(t *testing.T) {
	got := roundTrip(t, command.NewDeleteSubPolicy("orders", "billing"))
	if got.SubName != "billing" {
		t.Fatalf("sub name = %q, want %q", got.SubName, "billing")
	}
}

func TestEnqueueRoundTrip(t *testing.T) {
	subs := map[string]model.SubPolicy{
		"billing": {SubName: "billing", SubURL: "http://billing:9000", NumberOfRetries: 5},
		"audit":   {SubName: "audit", SubURL: "http://audit:9000", NumberOfRetries: 2},
	}
	got := roundTrip(t, command.NewEnqueue("orders", "orders-abc123", "hello", subs))
	if got.MsgID != "orders-abc123" {
		t.Fatalf("msg id = %q, want %q", got.MsgID, "orders-abc123")
	}
	if got.Payload != "hello" {
		t.Fatalf("payload = %q, want %q", got.Payload, "hello")
	}
	if len(got.SubList) != len(subs) {
		t.Fatalf("sub list = %v, want %v", got.SubList, subs)
	}
	for name, want := range subs {
		if got.SubList[name] != want {
			t.Fatalf("sub %q = %+v, want %+v", name, got.SubList[name], want)
		}
	}
}

// A queue with no subscribers still enqueues; the message is simply owed to
// nobody, and storage drops it rather than leaving it pending forever.
func TestEnqueueWithEmptySubListRoundTrips(t *testing.T) {
	got := roundTrip(t, command.NewEnqueue("orders", "m1", "hello", nil))
	if len(got.SubList) != 0 {
		t.Fatalf("sub list = %v, want empty", got.SubList)
	}
}

func TestNewEnqueueCopiesSubList(t *testing.T) {
	subs := map[string]model.SubPolicy{"billing": {SubName: "billing"}}
	cmd := command.NewEnqueue("orders", "m1", "hello", subs)
	subs["injected"] = model.SubPolicy{SubName: "injected"}

	if _, ok := cmd.SubList["injected"]; ok {
		t.Fatalf("SubList aliased the caller's map: %v", cmd.SubList)
	}
}

func TestAckRoundTrip(t *testing.T) {
	got := roundTrip(t, command.NewAck("orders", "orders-abc123", []string{"billing", "audit"}))
	if got.MsgID != "orders-abc123" {
		t.Fatalf("msg id = %q, want %q", got.MsgID, "orders-abc123")
	}
	if len(got.SubNames) != 2 || got.SubNames[0] != "billing" || got.SubNames[1] != "audit" {
		t.Fatalf("sub names = %v, want [billing audit]", got.SubNames)
	}
}

// A pass where every subscriber failed but none exhausted its retries still
// produces an Ack command with nothing in it.
func TestAckWithNoSubscribersRoundTripsAsEmptyNotNil(t *testing.T) {
	got := roundTrip(t, command.NewAck("orders", "orders-abc123", nil))
	if got.SubNames == nil {
		t.Fatal("SubNames decoded as nil, want empty slice")
	}
	if len(got.SubNames) != 0 {
		t.Fatalf("SubNames = %v, want empty", got.SubNames)
	}
}

// Payloads are arbitrary client bytes and must survive intact.
func TestEnqueuePayloadSurvivesAwkwardContent(t *testing.T) {
	payloads := []string{
		"",
		"line one\nline two",
		`{"nested":"json","n":1}`,
		`quotes " and \ backslashes`,
		"unicode: 日本語 🎉",
		strings.Repeat("x", 64*1024),
	}
	for _, payload := range payloads {
		got := roundTrip(t, command.NewEnqueue("orders", "m1", payload, nil))
		if got.Payload != payload {
			t.Fatalf("payload round trip failed for %.40q", payload)
		}
	}
}

// Every field a command does not use must stay absent on the wire — this is
// what keeps log entries small now that the subscriber list lives in storage
// rather than riding along with each enqueue.
func TestUnusedFieldsAreOmitted(t *testing.T) {
	data, err := command.Encode(command.NewCreateQueue("orders"))
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}
	for _, field := range []string{"MsgId", "Payload", "SubName", "SubNames", "SubList", "Policy"} {
		if strings.Contains(string(data), field) {
			t.Fatalf("CreateQueue entry carries unused field %q: %s", field, data)
		}
	}
}

func TestDecodeRejectsMalformedEntries(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"nil", nil},
		{"not json", []byte("{not json")},
		{"truncated", []byte(`{"Type":5,"QueueName":"ord`)},
		{"unknown type", []byte(`{"Type":99,"QueueName":"orders"}`)},
		{"zero type", []byte(`{"Type":0,"QueueName":"orders"}`)},
		{"missing queue", []byte(`{"Type":5,"MsgId":"m1"}`)},
		{"enqueue without msg id", []byte(`{"Type":5,"QueueName":"orders"}`)},
		{"ack without msg id", []byte(`{"Type":6,"QueueName":"orders"}`)},
		{"delete sub without name", []byte(`{"Type":4,"QueueName":"orders"}`)},
		{"put policy without name", []byte(`{"Type":3,"QueueName":"orders"}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := command.Decode(tc.data); err == nil {
				t.Fatalf("Decode(%s) succeeded, want error", tc.data)
			}
		})
	}
}

// Encode validates too, so a bad command fails on the leader rather than
// becoming a committed entry every node has to cope with.
func TestEncodeRejectsMalformedCommands(t *testing.T) {
	cases := []struct {
		name string
		cmd  command.Command
	}{
		{"zero value", command.Command{}},
		{"unknown type", command.Command{Type: command.Type(42), QueueName: "orders"}},
		{"empty queue name", command.NewEnqueue("", "m1", "hi", nil)},
		{"empty msg id", command.NewEnqueue("orders", "", "hi", nil)},
		{"empty sub name", command.NewDeleteSubPolicy("orders", "")},
		{"policy without name", command.NewPutSubPolicy("orders", model.SubPolicy{SubURL: "http://x"})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := command.Encode(tc.cmd); err == nil {
				t.Fatal("Encode succeeded, want error")
			}
		})
	}
}

func TestTypeString(t *testing.T) {
	if got := command.Enqueue.String(); got != "Enqueue" {
		t.Fatalf("Enqueue.String() = %q", got)
	}
	if got := command.Type(99).String(); got != "Type(99)" {
		t.Fatalf("Type(99).String() = %q", got)
	}
}

// NewAck copies its input so a caller reusing its results buffer cannot mutate
// a command that has already been handed off.
func TestNewAckCopiesSubNames(t *testing.T) {
	names := []string{"billing", "audit"}
	cmd := command.NewAck("orders", "m1", names)
	names[0] = "mutated"

	if cmd.SubNames[0] != "billing" {
		t.Fatalf("SubNames aliased the caller's slice: %v", cmd.SubNames)
	}
}
