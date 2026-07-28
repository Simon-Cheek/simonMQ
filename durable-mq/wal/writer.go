package wal

import (
	"durable-mq/record"
	"os"
)

// Writer is not safe for concurrent use
type Writer struct {
	sm          *SegmentManager
	file        *os.File
	currentSize int64
	maxSegSize  int64
	nextLSN     uint64
}

// OpenWriter starts a fresh writer against sm, beginning a new segment at LSN 1
func OpenWriter(sm *SegmentManager, maxSegSize int64) (*Writer, error) {
	w := &Writer{sm: sm, maxSegSize: maxSegSize, nextLSN: 1}
	if err := w.rollSegment(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Writer) rollSegment() error {
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
	}
	f, _, err := w.sm.CreateSegment(w.nextLSN)
	if err != nil {
		return err
	}
	w.file = f
	w.currentSize = 0
	return nil
}

func (w *Writer) Append(rec *record.Record) (uint64, error) {
	rec.LSN = w.nextLSN
	buf, err := record.Encode(rec)
	if err != nil {
		return 0, err
	}
	if w.currentSize+int64(len(buf)) > w.maxSegSize {
		if err := w.rollSegment(); err != nil {
			return 0, err
		}
	}
	if _, err = w.file.Write(buf); err != nil {
		return 0, err
	}
	if err = w.file.Sync(); err != nil {
		return 0, err
	}
	w.currentSize += int64(len(buf))
	w.nextLSN++
	return rec.LSN, nil
}

func (w *Writer) Close() error {
	return w.file.Close()
}
