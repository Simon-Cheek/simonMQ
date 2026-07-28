package wal

import (
	"durable-mq/record"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const maxLogLength = 16 * 1024 * 1024

type Reader struct {
	sm      *SegmentManager
	current string // name of the currently-open segment
	file    *os.File
}

// OpenReader opens the first segment known to sm, if any exist.
func OpenReader(sm *SegmentManager) (*Reader, error) {
	r := &Reader{sm: sm}
	segs := sm.Segments()
	if len(segs) == 0 {
		return r, nil // empty WAL — ReadAll will just return nothing
	}
	if err := r.openSegment(segs[0]); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Reader) openSegment(name string) error {
	f, err := r.sm.OpenSegment(name)
	if err != nil {
		return err
	}
	if r.file != nil {
		r.file.Close()
	}
	r.file = f
	r.current = name
	return nil
}

// isLastSegment consults the manager live, rather than a frozen list.
func (r *Reader) isLastSegment() bool {
	_, ok := r.sm.SegmentAfter(r.current)
	return !ok
}

func (r *Reader) ReadAll() ([]*record.Record, error) {
	var records []*record.Record

	if r.file == nil {
		return records, nil
	}

	for {
		header := make([]byte, 12)
		_, err := io.ReadFull(r.file, header)
		if err == io.EOF {
			next, ok := r.sm.SegmentAfter(r.current)
			if !ok {
				return records, nil
			}
			if err := r.openSegment(next); err != nil {
				return nil, err
			}
			continue
		}
		if err == io.ErrUnexpectedEOF {
			if r.isLastSegment() {
				return records, nil
			}
			return records, fmt.Errorf("unexpected partial record in non-final segment %s", r.current)
		}
		if err != nil {
			return records, err
		}

		remainingLength := binary.LittleEndian.Uint32(header[8:12])
		if remainingLength > maxLogLength {
			return records, fmt.Errorf("wal log over max header length: %d", remainingLength)
		}

		remaining := make([]byte, remainingLength)
		_, err = io.ReadFull(r.file, remaining)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if r.isLastSegment() {
				return records, nil
			}
			return records, fmt.Errorf("unexpected partial record in non-final segment %s", r.current)
		}
		if err != nil {
			return records, err
		}

		recordBytes := append(header, remaining...)
		rec, err := record.Decode(recordBytes)
		if err != nil {
			break // CRC mismatch
		}
		records = append(records, rec)
	}
	return records, nil
}

func (r *Reader) Close() error {
	if r.file == nil {
		return nil
	}
	return r.file.Close()
}
