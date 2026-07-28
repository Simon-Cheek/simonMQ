package wal

import "durable-mq/record"

type WAL struct {
	sm *SegmentManager
	w  *Writer
	r  *Reader
}

// TODO: Checkpointing
func Open(dir string, maxSegSize int64) (*WAL, error) {
	sm, err := NewSegmentManager(dir)
	if err != nil {
		return nil, err
	}
	// Writer first: Recover() truncates any torn tail before Reader
	// ever sees the segment, so replay never reads corrupt trailing bytes.
	w, err := OpenWriter(sm, maxSegSize)
	if err != nil {
		return nil, err
	}
	r, err := OpenReader(sm)
	if err != nil {
		return nil, err
	}
	return &WAL{sm: sm, w: w, r: r}, nil
}

func (l *WAL) Append(rec *record.Record) (uint64, error) {
	return l.w.Append(rec)
}

func (l *WAL) ReadAll() ([]*record.Record, error) {
	return l.r.ReadAll()
}

func (l *WAL) Close() error {
	if err := l.w.Close(); err != nil {
		return err
	}
	return l.r.Close()
}
