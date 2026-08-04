package unit_tests

import (
	"bytes"
	"testing"

	"durable-mq/record"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		rec  record.Record
	}{
		{
			name: "typical enqueue record",
			rec: record.Record{
				LSN:       42,
				OpType:    record.OpEnqueue,
				QueueName: "orders",
				Payload:   []byte(`{"MsgId":"abc","MsgContent":"hello"}`),
			},
		},
		{
			name: "empty queue name and nil payload",
			rec: record.Record{
				LSN:       1,
				OpType:    record.OpBeginCheckpoint,
				QueueName: "",
				Payload:   nil,
			},
		},
		{
			name: "zero LSN",
			rec: record.Record{
				LSN:       0,
				OpType:    record.OpCreateQueue,
				QueueName: "q",
				Payload:   nil,
			},
		},
		{
			name: "max uint64 LSN",
			rec: record.Record{
				LSN:       ^uint64(0),
				OpType:    record.OpAck,
				QueueName: "q",
				Payload:   []byte("x"),
			},
		},
		{
			name: "unicode queue name",
			rec: record.Record{
				LSN:       7,
				OpType:    record.OpUpdateSubPolicy,
				QueueName: "订单队列",
				Payload:   []byte(`{"SubName":"s"}`),
			},
		},
		{
			name: "large payload",
			rec: record.Record{
				LSN:       100,
				OpType:    record.OpEnqueue,
				QueueName: "bulk",
				Payload:   bytes.Repeat([]byte("x"), 64*1024),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := record.Encode(&tc.rec)
			if err != nil {
				t.Fatalf("Encode returned error: %v", err)
			}

			decoded, err := record.Decode(buf)
			if err != nil {
				t.Fatalf("Decode returned error: %v", err)
			}

			if decoded.LSN != tc.rec.LSN {
				t.Errorf("LSN = %d, want %d", decoded.LSN, tc.rec.LSN)
			}
			if decoded.OpType != tc.rec.OpType {
				t.Errorf("OpType = %d, want %d", decoded.OpType, tc.rec.OpType)
			}
			if decoded.QueueName != tc.rec.QueueName {
				t.Errorf("QueueName = %q, want %q", decoded.QueueName, tc.rec.QueueName)
			}
			if !bytes.Equal(decoded.Payload, tc.rec.Payload) {
				t.Errorf("Payload = %v, want %v", decoded.Payload, tc.rec.Payload)
			}
		})
	}
}

func TestEncodeDecodeAllOpTypes(t *testing.T) {
	opTypes := []record.OpType{
		record.OpEnqueue,
		record.OpAck,
		record.OpCreateQueue,
		record.OpDeleteQueue,
		record.OpUpdateSubPolicy,
		record.OpDeleteSubPolicy,
		record.OpBeginCheckpoint,
		record.OpEndCheckpoint,
	}

	for _, op := range opTypes {
		rec := record.Record{LSN: 1, OpType: op, QueueName: "q"}

		buf, err := record.Encode(&rec)
		if err != nil {
			t.Fatalf("Encode(opType=%d) returned error: %v", op, err)
		}

		decoded, err := record.Decode(buf)
		if err != nil {
			t.Fatalf("Decode(opType=%d) returned error: %v", op, err)
		}
		if decoded.OpType != op {
			t.Errorf("OpType = %d, want %d", decoded.OpType, op)
		}
	}
}

func TestDecodeRejectsTooShortData(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"shorter than header", make([]byte, 11)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := record.Decode(tc.data); err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}
}

func TestDecodeRejectsTruncatedRecord(t *testing.T) {
	rec := record.Record{
		LSN:       1,
		OpType:    record.OpEnqueue,
		QueueName: "q",
		Payload:   []byte("some payload"),
	}
	buf, err := record.Encode(&rec)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}

	// Chop off the tail so the header's declared length exceeds what's
	// actually present — simulates a torn write cut off mid-record.
	truncated := buf[:len(buf)-5]

	if _, err := record.Decode(truncated); err == nil {
		t.Fatal("expected an error decoding a truncated record, got nil")
	}
}

func TestDecodeRejectsCorruptedPayload(t *testing.T) {
	rec := record.Record{
		LSN:       1,
		OpType:    record.OpEnqueue,
		QueueName: "q",
		Payload:   []byte("some payload"),
	}
	buf, err := record.Encode(&rec)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}

	corrupted := make([]byte, len(buf))
	copy(corrupted, buf)
	corrupted[len(corrupted)/2] ^= 0xFF // flip a bit in the payload

	if _, err := record.Decode(corrupted); err == nil {
		t.Fatal("expected a CRC error decoding corrupted payload data, got nil")
	}
}

func TestDecodeRejectsCorruptedChecksum(t *testing.T) {
	rec := record.Record{
		LSN:       1,
		OpType:    record.OpAck,
		QueueName: "q",
		Payload:   []byte("payload"),
	}
	buf, err := record.Encode(&rec)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}

	corrupted := make([]byte, len(buf))
	copy(corrupted, buf)
	corrupted[len(corrupted)-1] ^= 0xFF // flip a bit in the trailing CRC itself

	if _, err := record.Decode(corrupted); err == nil {
		t.Fatal("expected a CRC error decoding a corrupted checksum, got nil")
	}
}
