package unit_tests

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"durable-mq/model"
	"durable-mq/record"
	"durable-mq/wal"
)

func mustAppend(t *testing.T, w *wal.WAL, opType record.OpType, queueName string, payload []byte) uint64 {
	t.Helper()
	lsn, err := w.Append(&record.Record{OpType: opType, QueueName: queueName, Payload: payload})
	if err != nil {
		t.Fatalf("Append(%v) returned error: %v", opType, err)
	}
	return lsn
}

func walFilesIn(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s) returned error: %v", dir, err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".wal") {
			names = append(names, e.Name())
		}
	}
	return names
}

func TestAppendReadAllRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	defer w.Close()

	lsn1 := mustAppend(t, w, record.OpCreateQueue, "orders", nil)
	lsn2 := mustAppend(t, w, record.OpEnqueue, "orders", []byte("hello"))
	lsn3 := mustAppend(t, w, record.OpAck, "orders", []byte("ack-payload"))

	if lsn1 == 0 || lsn2 <= lsn1 || lsn3 <= lsn2 {
		t.Fatalf("LSNs not strictly increasing: %d, %d, %d", lsn1, lsn2, lsn3)
	}

	records, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("got %d records, want 3", len(records))
	}

	wantOps := []record.OpType{record.OpCreateQueue, record.OpEnqueue, record.OpAck}
	wantLSNs := []uint64{lsn1, lsn2, lsn3}
	for i, rec := range records {
		if rec.OpType != wantOps[i] {
			t.Errorf("record %d OpType = %v, want %v", i, rec.OpType, wantOps[i])
		}
		if rec.LSN != wantLSNs[i] {
			t.Errorf("record %d LSN = %d, want %d", i, rec.LSN, wantLSNs[i])
		}
		if rec.QueueName != "orders" {
			t.Errorf("record %d QueueName = %q, want %q", i, rec.QueueName, "orders")
		}
	}
	if string(records[1].Payload) != "hello" {
		t.Errorf("record 1 Payload = %q, want %q", records[1].Payload, "hello")
	}
}

func TestAppendRollsSegmentsAndReadAllPreservesOrder(t *testing.T) {
	dir := t.TempDir()
	// Small enough that a couple dozen tiny records force multiple rolls.
	w, err := wal.Open(dir, 150)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	defer w.Close()

	const n = 20
	var lsns []uint64
	for i := 0; i < n; i++ {
		lsns = append(lsns, mustAppend(t, w, record.OpEnqueue, "q", []byte(fmt.Sprintf("msg-%d", i))))
	}

	if segCount := len(walFilesIn(t, dir)); segCount < 2 {
		t.Fatalf("test setup only produced %d segment file(s); need multiple to exercise rolling", segCount)
	}

	records, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	if len(records) != n {
		t.Fatalf("got %d records, want %d", len(records), n)
	}
	for i, rec := range records {
		want := fmt.Sprintf("msg-%d", i)
		if string(rec.Payload) != want {
			t.Errorf("record %d payload = %q, want %q (order not preserved across segment boundaries)", i, rec.Payload, want)
		}
		if rec.LSN != lsns[i] {
			t.Errorf("record %d LSN = %d, want %d", i, rec.LSN, lsns[i])
		}
	}
}

func TestRecoverTruncatesTornTailAndContinuesLSNs(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, 1<<20) // one segment for the whole test
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}

	mustAppend(t, w, record.OpCreateQueue, "q", nil)
	mustAppend(t, w, record.OpEnqueue, "q", []byte("kept-message"))
	mustAppend(t, w, record.OpEnqueue, "q", []byte("torn-message"))
	// Deliberately not calling w.Close() — simulating a process that died
	// without a graceful shutdown.

	segs := walFilesIn(t, dir)
	if len(segs) != 1 {
		t.Fatalf("expected exactly one segment file, got %d", len(segs))
	}
	segPath := filepath.Join(dir, segs[0])

	raw, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatalf("failed to read segment file: %v", err)
	}
	if len(raw) < 10 {
		t.Fatalf("segment file too small to safely truncate for this test: %d bytes", len(raw))
	}
	// Chop bytes off the tail so the last record is torn — its own header
	// still claims a length longer than what's actually present.
	if err := os.Truncate(segPath, int64(len(raw)-5)); err != nil {
		t.Fatalf("failed to truncate segment file: %v", err)
	}

	// Reopen — simulates a restart after the crash.
	w2, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("Open after simulated crash returned error: %v", err)
	}

	records, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("got %d records after recovery, want 2 (torn record must be dropped)", len(records))
	}
	if string(records[1].Payload) != "kept-message" {
		t.Errorf("last surviving record payload = %q, want %q", records[1].Payload, "kept-message")
	}

	// A fresh append must reuse the LSN the torn record never durably got,
	// not skip past it and not restart from scratch.
	newLSN := mustAppend(t, w2, record.OpEnqueue, "q", []byte("new-message"))
	wantLSN := records[len(records)-1].LSN + 1
	if newLSN != wantLSN {
		t.Errorf("LSN after recovery = %d, want %d", newLSN, wantLSN)
	}
	w2.Close()

	// A third, fresh Open — not another ReadAll on w2 — since ReadAll's
	// internal reader doesn't rescan from the top on repeat calls; a real
	// full replay only happens via a new Open, same as a real restart.
	w3, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("final reopen returned error: %v", err)
	}
	defer w3.Close()

	finalRecords, err := w3.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	if len(finalRecords) != 3 {
		t.Fatalf("got %d records, want 3 (2 recovered + 1 new)", len(finalRecords))
	}
	if string(finalRecords[2].Payload) != "new-message" {
		t.Errorf("newest record payload = %q, want %q", finalRecords[2].Payload, "new-message")
	}
}

func TestReadAllResetsOnValidCheckpoint(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	defer w.Close()

	// Pre-checkpoint history, including a message a real compaction would
	// have already dropped (e.g. fully acknowledged).
	mustAppend(t, w, record.OpCreateQueue, "orders", nil)
	mustAppend(t, w, record.OpEnqueue, "orders", []byte("stale-message"))

	// The compacted checkpoint file, built the way derivation would: just
	// the surviving CREATE_QUEUE, none of the already-settled message.
	compacted := []*record.Record{
		{OpType: record.OpCreateQueue, QueueName: "orders"},
	}
	ckptName, ckptChecksum, err := w.WriteCheckpointFile("test-ckpt", compacted)
	if err != nil {
		t.Fatalf("WriteCheckpointFile returned error: %v", err)
	}

	mustAppend(t, w, record.OpBeginCheckpoint, "", nil)
	endPayload, err := model.EncodeEndCheckpoint(model.EndCheckpoint{FileName: ckptName, FileChecksum: ckptChecksum})
	if err != nil {
		t.Fatalf("EncodeEndCheckpoint returned error: %v", err)
	}
	mustAppend(t, w, record.OpEndCheckpoint, "", endPayload)

	// Traffic landing after the checkpoint boundary must survive the reset.
	mustAppend(t, w, record.OpEnqueue, "orders", []byte("post-checkpoint-message"))

	records, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}

	var sawStale, sawCreateQueue, sawPostMsg bool
	for _, rec := range records {
		if string(rec.Payload) == "stale-message" {
			sawStale = true
		}
		if rec.OpType == record.OpCreateQueue && rec.QueueName == "orders" {
			sawCreateQueue = true
		}
		if string(rec.Payload) == "post-checkpoint-message" {
			sawPostMsg = true
		}
	}
	if sawStale {
		t.Error("stale pre-checkpoint record survived the checkpoint reset — checkpointing isn't actually bounding replay")
	}
	if !sawCreateQueue {
		t.Error("compacted CREATE_QUEUE record missing from post-reset replay")
	}
	if !sawPostMsg {
		t.Error("record appended after the checkpoint boundary is missing from replay")
	}
}

func TestReadAllTreatsMissingCheckpointFileAsAbsent(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	defer w.Close()

	mustAppend(t, w, record.OpCreateQueue, "orders", nil)
	mustAppend(t, w, record.OpEnqueue, "orders", []byte("original-message"))

	mustAppend(t, w, record.OpBeginCheckpoint, "", nil)
	endPayload, err := model.EncodeEndCheckpoint(model.EndCheckpoint{
		FileName:     "checkpoint-does-not-exist.ckpt",
		FileChecksum: "deadbeef",
	})
	if err != nil {
		t.Fatalf("EncodeEndCheckpoint returned error: %v", err)
	}
	mustAppend(t, w, record.OpEndCheckpoint, "", endPayload)

	mustAppend(t, w, record.OpEnqueue, "orders", []byte("later-message"))

	records, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}

	var sawOriginal, sawLater bool
	for _, rec := range records {
		if string(rec.Payload) == "original-message" {
			sawOriginal = true
		}
		if string(rec.Payload) == "later-message" {
			sawLater = true
		}
	}
	if !sawOriginal {
		t.Error("pre-checkpoint record was discarded even though the referenced checkpoint file doesn't exist")
	}
	if !sawLater {
		t.Error("later record is missing")
	}
}

func TestReadAllDeletesCorruptCheckpointFile(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	defer w.Close()

	compacted := []*record.Record{{OpType: record.OpCreateQueue, QueueName: "orders"}}
	ckptName, _, err := w.WriteCheckpointFile("bad-ckpt", compacted)
	if err != nil {
		t.Fatalf("WriteCheckpointFile returned error: %v", err)
	}

	mustAppend(t, w, record.OpBeginCheckpoint, "", nil)
	endPayload, err := model.EncodeEndCheckpoint(model.EndCheckpoint{
		FileName:     ckptName,
		FileChecksum: "00000000", // deliberately wrong
	})
	if err != nil {
		t.Fatalf("EncodeEndCheckpoint returned error: %v", err)
	}
	mustAppend(t, w, record.OpEndCheckpoint, "", endPayload)

	if _, err := w.ReadAll(); err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, ckptName)); !os.IsNotExist(err) {
		t.Errorf("expected corrupt checkpoint file to be deleted, stat err = %v", err)
	}
}

func TestShouldCheckpointThreshold(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, 100) // tiny segments so records force rolls quickly
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	defer w.Close()

	if w.ShouldCheckpoint() {
		t.Fatal("ShouldCheckpoint is true before any segments have accumulated")
	}

	becameTrue := false
	for i := 0; i < 50; i++ {
		mustAppend(t, w, record.OpEnqueue, "q", []byte(fmt.Sprintf("msg-%d", i)))
		if w.ShouldCheckpoint() {
			becameTrue = true
			break
		}
	}
	if !becameTrue {
		t.Fatal("ShouldCheckpoint never became true despite many segment rolls")
	}
}

func TestWriteCheckpointFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	defer w.Close()

	recs := []*record.Record{
		{OpType: record.OpCreateQueue, QueueName: "q1"},
		{OpType: record.OpEnqueue, QueueName: "q1", Payload: []byte("hello")},
	}
	name, checksum, err := w.WriteCheckpointFile("abc123", recs)
	if err != nil {
		t.Fatalf("WriteCheckpointFile returned error: %v", err)
	}
	if !strings.HasSuffix(name, ".ckpt") {
		t.Errorf("checkpoint file name %q does not end in .ckpt", name)
	}

	raw, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("failed to read checkpoint file: %v", err)
	}
	wantChecksum := fmt.Sprintf("%08x", crc32.ChecksumIEEE(raw))
	if checksum != wantChecksum {
		t.Errorf("checksum = %s, want %s", checksum, wantChecksum)
	}

	sm, err := wal.NewSegmentManager(dir)
	if err != nil {
		t.Fatalf("NewSegmentManager returned error: %v", err)
	}
	r, err := wal.OpenReader(sm)
	if err != nil {
		t.Fatalf("OpenReader returned error: %v", err)
	}
	defer r.Close()

	decoded, err := r.ReadAllCkpt(name)
	if err != nil {
		t.Fatalf("ReadAllCkpt returned error: %v", err)
	}
	if len(decoded) != len(recs) {
		t.Fatalf("got %d records back, want %d", len(decoded), len(recs))
	}
	for i, rec := range recs {
		if decoded[i].OpType != rec.OpType || decoded[i].QueueName != rec.QueueName || string(decoded[i].Payload) != string(rec.Payload) {
			t.Errorf("record %d = %+v, want %+v", i, decoded[i], rec)
		}
	}
}

func TestDeleteSegmentsBefore(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, 100) // tiny, so this produces several segments
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}

	const n = 30
	var lsns []uint64
	for i := 0; i < n; i++ {
		lsns = append(lsns, mustAppend(t, w, record.OpEnqueue, "q", []byte(fmt.Sprintf("m%d", i))))
	}
	w.Close()

	sm, err := wal.NewSegmentManager(dir)
	if err != nil {
		t.Fatalf("NewSegmentManager returned error: %v", err)
	}
	before := sm.Segments()
	if len(before) < 3 {
		t.Fatalf("test setup didn't create enough segments: %d", len(before))
	}

	cutoff := lsns[len(lsns)/2]
	if err := sm.DeleteSegmentsBefore(cutoff); err != nil {
		t.Fatalf("DeleteSegmentsBefore returned error: %v", err)
	}

	after := sm.Segments()
	if len(after) >= len(before) {
		t.Errorf("expected fewer segments after cleanup: before=%d after=%d", len(before), len(after))
	}

	// Whatever remains must still let a fresh reader recover everything at
	// or after the cutoff — cleanup must never destroy needed data.
	w2, err := wal.Open(dir, 100)
	if err != nil {
		t.Fatalf("reopening after cleanup returned error: %v", err)
	}
	defer w2.Close()

	records, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	lastWantedPayload := fmt.Sprintf("m%d", n-1)
	var sawLast bool
	for _, rec := range records {
		if string(rec.Payload) == lastWantedPayload {
			sawLast = true
		}
	}
	if !sawLast {
		t.Error("most recent record missing after DeleteSegmentsBefore — cleanup deleted more than it should have")
	}
}

func TestDeleteCheckpointFilesExcept(t *testing.T) {
	dir := t.TempDir()
	sm, err := wal.NewSegmentManager(dir)
	if err != nil {
		t.Fatalf("NewSegmentManager returned error: %v", err)
	}

	recs := []*record.Record{{OpType: record.OpCreateQueue, QueueName: "q"}}
	keepName, _, err := sm.WriteCheckpointFile("keep-me", recs)
	if err != nil {
		t.Fatalf("WriteCheckpointFile returned error: %v", err)
	}
	dropName, _, err := sm.WriteCheckpointFile("drop-me", recs)
	if err != nil {
		t.Fatalf("WriteCheckpointFile returned error: %v", err)
	}

	if err := sm.DeleteCheckpointFilesExcept(keepName); err != nil {
		t.Fatalf("DeleteCheckpointFilesExcept returned error: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, keepName)); err != nil {
		t.Errorf("kept checkpoint file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, dropName)); !os.IsNotExist(err) {
		t.Errorf("expected superseded checkpoint file to be removed, stat err = %v", err)
	}
}

func TestDeleteTmpFiles(t *testing.T) {
	dir := t.TempDir()
	sm, err := wal.NewSegmentManager(dir)
	if err != nil {
		t.Fatalf("NewSegmentManager returned error: %v", err)
	}

	tmpPath := filepath.Join(dir, "leftover.ckpt.tmp")
	if err := os.WriteFile(tmpPath, []byte("garbage"), 0644); err != nil {
		t.Fatalf("failed to seed a stray tmp file: %v", err)
	}

	if err := sm.DeleteTmpFiles(); err != nil {
		t.Fatalf("DeleteTmpFiles returned error: %v", err)
	}

	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("expected stray .tmp file to be removed, stat err = %v", err)
	}
}
