package wal

import (
	"durable-mq/record"
)

type WAL struct {
	sm *SegmentManager
	w  *Writer
	r  *Reader
}

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

func (l *WAL) ShouldCheckpoint() bool {
	return l.sm.ShouldCheckpoint()
}

// ReadUpTo is used for checkpointing and needs its own Reader
func (l *WAL) ReadUpTo(uptoLSN uint64) ([]*record.Record, error) {
	r, err := OpenReader(l.sm)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return r.ReadUpTo(uptoLSN)
}

func (l *WAL) WriteCheckpointFile(id string, records []*record.Record) (string, string, error) {
	return l.sm.WriteCheckpointFile(id, records)
}

func (l *WAL) DeleteSegmentsBefore(lsn uint64) error {
	return l.sm.DeleteSegmentsBefore(lsn)
}

func (l *WAL) DeleteCheckpointFilesExcept(keepName string) error {
	return l.sm.DeleteCheckpointFilesExcept(keepName)
}

func (l *WAL) ReadAll() ([]*record.Record, error) {
	records, validCkptName, validCkptLSN, err := l.r.ReadAll()
	if err != nil {
		return records, err
	}

	// If multiple ckpt files encountered, delete all except the current one
	l.sm.DeleteCheckpointFilesExcept(validCkptName)

	// Delete unnecessary wal files (covered by ckpt)
	l.sm.DeleteSegmentsBefore(validCkptLSN)

	l.sm.DeleteTmpFiles()
	return records, nil
}

func (l *WAL) Close() error {
	if err := l.w.Close(); err != nil {
		return err
	}
	return l.r.Close()
}
