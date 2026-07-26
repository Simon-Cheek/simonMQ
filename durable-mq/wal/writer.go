package wal

import (
	"durable-mq/record"
	"os"
)

type Writer struct {
	file    *os.File
	nextLSN uint64
}

func OpenWriter(filename string) (*Writer, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Writer{f, 1}, nil
}

func (w *Writer) Append(rec *record.Record) (lsn uint64, err error) {
	rec.LSN = w.nextLSN
	buf, err := record.Encode(rec)
	if err != nil {
		return 0, err
	}
	if _, err = w.file.Write(buf); err != nil {
		return 0, err
	}
	if err = w.file.Sync(); err != nil {
		return 0, err
	}
	w.nextLSN++
	return rec.LSN, nil
}
