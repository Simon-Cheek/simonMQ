package wal

import (
	"durable-mq/record"
	"encoding/binary"
	"fmt"
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
}

type RecoveryState struct {
	NextLSN uint64
	File    *os.File
	Size    int64
}

func NewSegmentManager(dir string) (*SegmentManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	sm := &SegmentManager{dir: dir}
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
