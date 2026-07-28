package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type SegmentManager struct {
	mu       sync.Mutex // protects "segments" field
	dir      string
	segments []string // sorted filenames
}

func NewSegmentManager(dir string) (*SegmentManager, error) {
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
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".wal") {
			segs = append(segs, e.Name())
		}
	}
	sort.Strings(segs)
	sm.mu.Lock()
	sm.segments = segs
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

func (sm *SegmentManager) Last() string {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if len(sm.segments) == 0 {
		return ""
	}
	return sm.segments[len(sm.segments)-1]
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

func (sm *SegmentManager) OpenSegment(name string) (*os.File, error) {
	return os.Open(filepath.Join(sm.dir, name))
}

func segmentFileName(firstLSN uint64) string {
	return fmt.Sprintf("%020d.wal", firstLSN)
}
