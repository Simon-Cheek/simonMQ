package wal

import (
	"durable-mq/record"
	"os"
	"path/filepath"
)

// Assumes SINGLE THREADED writes, this is NOT threadsafe
type Writer struct {
	dir         string
	file        *os.File
	nextLSN     uint64
	currentSize int64
	maxSegSize  int64
}

func OpenWriter(dir string, mexSegSize int64) (*Writer, error) {
	w := &Writer{dir: dir, maxSegSize: mexSegSize, nextLSN: 1}
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
	name := segmentFileName(w.nextLSN)
	f, err := os.OpenFile(filepath.Join(w.dir, name), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	w.file = f
	w.currentSize = 0
	return nil
}

func (w *Writer) Append(rec *record.Record) (lsn uint64, err error) {
	rec.LSN = w.nextLSN
	buf, err := record.Encode(rec)
	if err != nil {
		return 0, err
	}

	if (int64(len(buf)) + w.currentSize) > w.maxSegSize {
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
