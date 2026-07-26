package wal

import (
	"durable-mq/record"
	"os"
	"testing"
)

func TestReader_StopsCleanlyOnTornWrite(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "wal-test-*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	// Write 3 valid records
	writer := &Writer{file: tmpFile, nextLSN: 1} // adjust to however you construct Writer
	records := []*record.Record{
		{OpType: record.OpCreateQueue, QueueName: "orders", Payload: []byte("q1")},
		{OpType: record.OpEnqueue, QueueName: "orders", Payload: []byte("hello")},
		{OpType: record.OpEnqueue, QueueName: "orders", Payload: []byte("world")},
	}
	for _, r := range records {
		if _, err := writer.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	tmpFile.Close()

	// Get the full file size, then truncate to cut off partway through the last record
	info, err := os.Stat(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	fullSize := info.Size()
	tornSize := fullSize - 5 // chop the last 5 bytes — lands mid-record, not on a boundary

	if err := os.Truncate(tmpFile.Name(), tornSize); err != nil {
		t.Fatal(err)
	}

	// Now read it back
	reader, err := OpenReader(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	got, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("expected nil error on torn write, got: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 complete records survived truncation, got %d", len(got))
	}
}
