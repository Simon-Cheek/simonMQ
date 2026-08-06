package wal

import (
	"bufio"
	"durable-mq/record"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type SegmentManager struct {
	mu           sync.Mutex // protects "segments" field
	dir          string
	segments     []string // sorted filenames
	ckptSegments []string

	// ckptThreshold is how many segments must exist before a checkpoint is
	// due. Configurable so a benchmark can reach the checkpoint path without
	// first writing hundreds of megabytes; production has no reason to change
	// it from DefaultCheckpointThreshold.
	ckptThreshold int
}

type RecoveryState struct {
	NextLSN uint64
	File    *os.File
	Size    int64
}

// DefaultCheckpointThreshold is the number of segments that must accumulate
// before a checkpoint is triggered.
const DefaultCheckpointThreshold = 4

func NewSegmentManager(dir string) (*SegmentManager, error) {
	return NewSegmentManagerWithThreshold(dir, DefaultCheckpointThreshold)
}

func NewSegmentManagerWithThreshold(dir string, ckptThreshold int) (*SegmentManager, error) {
	if ckptThreshold < 1 {
		ckptThreshold = DefaultCheckpointThreshold
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	sm := &SegmentManager{dir: dir, ckptThreshold: ckptThreshold}
	if err := sm.refresh(); err != nil {
		return nil, err
	}
	return sm, nil
}

func (sm *SegmentManager) refresh() error {
	entries, err := os.ReadDir(sm.dir)
	if err != nil {
		return err
	}
	var segs []string
	var ckpts []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".wal") {
			segs = append(segs, e.Name())
		}
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".ckpt") {
			ckpts = append(ckpts, e.Name())
		}
	}
	sort.Strings(segs)
	sort.Strings(ckpts)
	sm.mu.Lock()
	sm.segments = segs
	sm.ckptSegments = ckpts
	sm.mu.Unlock()
	return nil
}

// Segments returns a snapshot copy
func (sm *SegmentManager) Segments() []string {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	segs := make([]string, len(sm.segments))
	copy(segs, sm.segments)
	return segs
}

func (sm *SegmentManager) ShouldCheckpoint() bool {
	return len(sm.Segments()) >= sm.ckptThreshold
}

// SegmentAfter returns the segment immediately following `name`
func (sm *SegmentManager) SegmentAfter(name string) (string, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for i, s := range sm.segments {
		if s == name && i+1 < len(sm.segments) {
			return sm.segments[i+1], true
		}
	}
	return "", false
}

func (sm *SegmentManager) Last() (string, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if len(sm.segments) == 0 {
		return "", false
	}
	return sm.segments[len(sm.segments)-1], true
}

func (sm *SegmentManager) CreateSegment(firstLSN uint64) (*os.File, string, error) {
	name := segmentFileName(firstLSN)
	f, err := os.OpenFile(filepath.Join(sm.dir, name), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, "", err
	}
	sm.mu.Lock()
	sm.segments = append(sm.segments, name)
	sm.mu.Unlock()
	return f, name, nil
}

func (sm *SegmentManager) TruncateSegment(name string, size int64) error {
	return os.Truncate(filepath.Join(sm.dir, name), size)
}

func (sm *SegmentManager) DeleteSegmentsBefore(lsn uint64) error {
	segs := sm.Segments()

	var toDelete []string
	for i := 0; i < len(segs)-1; i++ {
		if firstLSNFromSegmentName(segs[i+1]) > lsn {
			break
		}
		toDelete = append(toDelete, segs[i])
	}
	if len(toDelete) == 0 {
		return nil
	}

	var errs []error
	for _, name := range toDelete {
		if err := os.Remove(filepath.Join(sm.dir, name)); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}

	deleted := make(map[string]struct{}, len(toDelete))
	for _, name := range toDelete {
		deleted[name] = struct{}{}
	}
	sm.mu.Lock()
	kept := make([]string, 0, len(sm.segments))
	for _, name := range sm.segments {
		if _, ok := deleted[name]; !ok {
			kept = append(kept, name)
		}
	}
	sm.segments = kept
	sm.mu.Unlock()

	return errors.Join(errs...)
}

func (sm *SegmentManager) OpenSegmentForAppend(name string) (*os.File, error) {
	return os.OpenFile(filepath.Join(sm.dir, name), os.O_APPEND|os.O_WRONLY, 0644)
}

func firstLSNFromSegmentName(name string) uint64 {
	trimmed := strings.TrimSuffix(name, ".wal")
	lsn, err := strconv.ParseUint(trimmed, 10, 64)
	if err != nil {
		// Should be unreachable — only SegmentManager ever names files,
		// and always via segmentFileName's fixed format.
		panic(fmt.Sprintf("wal: malformed segment filename %q: %v", name, err))
	}
	return lsn
}

// scanSegment reads a single segment file from the beginning, returning
// every valid record found and the byte offset at which valid data ends
func scanSegment(f *os.File) (records []*record.Record, validEnd int64, err error) {
	var offset int64
	for {
		header := make([]byte, 12)
		n, rErr := io.ReadFull(f, header)
		if rErr == io.EOF {
			break // clean end, nothing more to read
		}
		if rErr == io.ErrUnexpectedEOF {
			break // torn header at tail
		}
		if rErr != nil {
			return records, offset, rErr
		}

		remainingLength := binary.LittleEndian.Uint32(header[8:12])
		if remainingLength > maxLogLength {
			break // treat a nonsense length as corruption at the tail
		}

		remaining := make([]byte, remainingLength)
		n2, rErr := io.ReadFull(f, remaining)
		if rErr == io.EOF || rErr == io.ErrUnexpectedEOF {
			break
		}
		if rErr != nil {
			return records, offset, rErr
		}

		recordBytes := append(header, remaining...)
		rec, dErr := record.Decode(recordBytes)
		if dErr != nil {
			break // CRC mismatch — stop here, don't trust anything past this
		}

		records = append(records, rec)
		offset += int64(n) + int64(n2)
	}
	return records, offset, nil
}

func (sm *SegmentManager) Recover() (*RecoveryState, error) {
	last, ok := sm.Last()
	if !ok {
		// Fresh WAL: no segments yet.
		f, _, err := sm.CreateSegment(1)
		if err != nil {
			return nil, err
		}
		return &RecoveryState{NextLSN: 1, File: f, Size: 0}, nil
	}

	rf, err := sm.OpenSegment(last)
	if err != nil {
		return nil, err
	}
	records, validEnd, err := scanSegment(rf)
	rf.Close()
	if err != nil {
		return nil, err
	}

	nextLSN := firstLSNFromSegmentName(last)
	if len(records) > 0 {
		nextLSN = records[len(records)-1].LSN + 1
	}

	if err := sm.TruncateSegment(last, validEnd); err != nil {
		return nil, err
	}
	wf, err := sm.OpenSegmentForAppend(last)
	if err != nil {
		return nil, err
	}
	return &RecoveryState{NextLSN: nextLSN, File: wf, Size: validEnd}, nil
}

func (sm *SegmentManager) OpenSegment(name string) (*os.File, error) {
	return os.Open(filepath.Join(sm.dir, name))
}

func segmentFileName(firstLSN uint64) string {
	return fmt.Sprintf("%020d.wal", firstLSN)
}

type CheckpointFile struct {
	Name     string
	Checksum string
}

// GetCheckpointFiles scans the WAL directory directly
func (sm *SegmentManager) GetCheckpointFiles() ([]CheckpointFile, error) {
	entries, err := os.ReadDir(sm.dir)
	if err != nil {
		return nil, err
	}

	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".ckpt") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	files := make([]CheckpointFile, 0, len(names))
	for _, name := range names {
		checksum, err := checksumFile(filepath.Join(sm.dir, name))
		if err != nil {
			return nil, err
		}
		files = append(files, CheckpointFile{Name: name, Checksum: checksum})
	}
	return files, nil
}

func (sm *SegmentManager) DeleteCheckpointFilesExcept(keepName string) error {
	files, err := sm.GetCheckpointFiles()
	if err != nil {
		return err
	}
	var errs []error
	for _, f := range files {
		if f.Name == keepName {
			continue
		}
		if err := os.Remove(filepath.Join(sm.dir, f.Name)); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (sm *SegmentManager) DeleteTmpFiles() error {
	entries, err := os.ReadDir(sm.dir)
	if err != nil {
		return err
	}
	var errs []error
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		if err := os.Remove(filepath.Join(sm.dir, e.Name())); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func checkpointFileName(id string) string {
	return fmt.Sprintf("checkpoint-%s.ckpt", id)
}

// WriteCheckpointFile Writes to .tmp file and renames after successful write to .ckpt
func (sm *SegmentManager) WriteCheckpointFile(id string, records []*record.Record) (string, string, error) {
	name := checkpointFileName(id)
	finalPath := filepath.Join(sm.dir, name)
	tmpPath := finalPath + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return "", "", err
	}

	w := bufio.NewWriter(f)
	for _, rec := range records {
		buf, err := record.Encode(rec)
		if err != nil {
			f.Close()
			os.Remove(tmpPath)
			return "", "", err
		}
		if _, err := w.Write(buf); err != nil {
			f.Close()
			os.Remove(tmpPath)
			return "", "", err
		}
	}

	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return "", "", err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return "", "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return "", "", err
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return "", "", err
	}

	checksum, err := checksumFile(finalPath)
	if err != nil {
		return "", "", err
	}

	return name, checksum, nil
}

func checksumFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := crc32.NewIEEE()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%08x", h.Sum32()), nil
}
