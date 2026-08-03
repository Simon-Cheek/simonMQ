package wal

import (
	"durable-mq/record"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

func (r *Reader) ReadAll() ([]*record.Record, string, uint64, error) {
	return r.readLoop(nil)
}

// ReadUpTo behaves like ReadAll but stops after a given LSN
func (r *Reader) ReadUpTo(uptoLSN uint64) ([]*record.Record, error) {
	records, _, _, err := r.readLoop(func(rec *record.Record) bool {
		return rec.LSN >= uptoLSN
	})
	return records, err
}

// readLoop is the shared reset-on-valid-checkpoint scan behind ReadAll and ReadUpTo
func (r *Reader) readLoop(stop func(rec *record.Record) bool) ([]*record.Record, string, uint64, error) {
	var records []*record.Record
	lastBeginCkptLSN := uint64(0)
	validCkptName := ""
	validCkptLSN := uint64(0)

	for {
		rec, err := r.readNext()
		if err != nil {
			return records, validCkptName, validCkptLSN, err
		}
		if rec == nil {
			return records, validCkptName, validCkptLSN, nil
		}

		switch rec.OpType {
		case record.OpBeginCheckpoint:
			lastBeginCkptLSN = rec.LSN
		case record.OpEndCheckpoint:
			if endCkpt, err := DecodeEndCheckpoint(rec.Payload); err == nil {
				if ckptRecs, ok := r.resolveCheckpoint(endCkpt); ok {
					records = append(ckptRecs, records[splitAt(records, lastBeginCkptLSN):]...)
					validCkptName = endCkpt.FileName
					validCkptLSN = lastBeginCkptLSN
				}
			}
		}

		records = append(records, rec)

		if stop != nil && stop(rec) {
			return records, validCkptName, validCkptLSN, nil
		}
	}
}

// readNext decodes the next record, opening the following segment on EOF.
func (r *Reader) readNext() (*record.Record, error) {
	if r.file == nil {
		return nil, nil
	}

	for {
		header := make([]byte, 12)
		_, err := io.ReadFull(r.file, header)
		if err == io.EOF {
			next, ok := r.sm.SegmentAfter(r.current)
			if !ok {
				return nil, nil
			}
			if err := r.openSegment(next); err != nil {
				return nil, err
			}
			continue
		}
		if err == io.ErrUnexpectedEOF {
			if r.isLastSegment() {
				return nil, nil
			}
			return nil, fmt.Errorf("unexpected partial record in non-final segment %s", r.current)
		}
		if err != nil {
			return nil, err
		}

		remainingLength := binary.LittleEndian.Uint32(header[8:12])
		if remainingLength > maxLogLength {
			return nil, fmt.Errorf("wal log over max header length: %d", remainingLength)
		}

		remaining := make([]byte, remainingLength)
		_, err = io.ReadFull(r.file, remaining)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if r.isLastSegment() {
				return nil, nil
			}
			return nil, fmt.Errorf("unexpected partial record in non-final segment %s", r.current)
		}
		if err != nil {
			return nil, err
		}

		rec, err := record.Decode(append(header, remaining...))
		if err != nil {
			return nil, nil // CRC mismatch — stop, don't trust anything past this
		}
		if rec == nil {
			continue
		}
		return rec, nil
	}
}

// resolveCheckpoint verifies endCkpt's checksum against the file on disk
// and, if it matches, returns that file's records
// If invalid checksum, deletes the file
func (r *Reader) resolveCheckpoint(endCkpt EndCheckpoint) (recs []*record.Record, ok bool) {
	path := filepath.Join(r.sm.dir, endCkpt.FileName)
	checksum, err := checksumFile(path)
	if err != nil {
		return nil, false
	}
	if checksum != endCkpt.FileChecksum {
		os.Remove(path) // corrupt
		return nil, false
	}
	recs, err = r.ReadAllCkpt(endCkpt.FileName)
	if err != nil {
		return nil, false
	}
	return recs, true
}

func splitAt(records []*record.Record, lsn uint64) int {
	for i, rec := range records {
		if rec.LSN > lsn {
			return i
		}
	}
	return len(records)
}

func (r *Reader) ReadAllCkpt(ckptFileName string) ([]*record.Record, error) {
	f, err := r.sm.OpenSegment(ckptFileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	records, validEnd, err := scanSegment(f)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if validEnd != info.Size() {
		return nil, fmt.Errorf("checkpoint file %s: only decoded %d of %d bytes despite a valid whole-file checksum", ckptFileName, validEnd, info.Size())
	}

	return records, nil
}

func (r *Reader) Close() error {
	if r.file == nil {
		return nil
	}
	return r.file.Close()
}
