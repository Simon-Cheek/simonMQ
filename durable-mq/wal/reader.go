package wal

import (
	"durable-mq/record"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Reader struct {
	dir      string
	segments []string
	idx      int
	file     *os.File
}

// Any Log bigger than this likely means some sort of corrupt data issue
const maxLogLength = 16 * 1024 * 1024

func OpenReader(dir string) (*Reader, error) {
	segs, err := listSegments(dir)
	if err != nil {
		return nil, err
	}

	r := &Reader{dir: dir, segments: segs}
	if len(segs) > 0 {
		if err := r.openSegment(0); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (r *Reader) openSegment(idx int) error {
	f, err := os.Open(filepath.Join(r.dir, r.segments[idx]))
	if err != nil {
		return err
	}
	r.file = f
	r.idx = idx
	return nil
}

func (r *Reader) isLastSegment() bool {
	return r.idx == len(r.segments)-1
}

// TODO: Add logic to handle hardware corruption, checksum corruption, etc
// TODO: Add ReadAll() version that streams / iterates
func (r *Reader) ReadAll() ([]*record.Record, error) {
	var records []*record.Record

	if len(r.segments) == 0 {
		return records, nil
	}
	if err := r.openSegment(0); err != nil {
		return nil, err
	}

	for {
		header := make([]byte, 12)
		_, err := io.ReadFull(r.file, header)
		if err == io.EOF {
			if r.isLastSegment() {
				return records, nil
			}
			if err := r.openSegment(r.idx + 1); err != nil {
				return nil, err
			}
			continue
		}
		if err == io.ErrUnexpectedEOF {
			if r.isLastSegment() {
				return records, nil
			}
			return records, fmt.Errorf("unexpected partial record in non-final segment %s", r.segments[r.idx])
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
			return records, fmt.Errorf("unexpected partial record in non-final segment %s", r.segments[r.idx])
		}
		if err != nil {
			return records, err
		}

		recordBytes := append(header, remaining...)
		rec, err := record.Decode(recordBytes)
		if err != nil {
			break
		}
		records = append(records, rec)
	}
	return records, nil
}
