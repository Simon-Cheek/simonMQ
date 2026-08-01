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

// ReadAll returns every record surviving replay, plus the name and
// beginCheckpointLSN of the checkpoint file (if any) that replay ultimately
// settled on as valid — callers use these to sweep stale/orphaned
// checkpoint files and now-fully-superseded WAL segments from disk once
// replay has determined the truth, rather than deleting anything here.
func (r *Reader) ReadAll() ([]*record.Record, string, uint64, error) {
	var records []*record.Record
	lastBeginCkptLSN := uint64(0)
	validCkptName := ""
	validCkptLSN := uint64(0)

	if r.file == nil {
		return records, validCkptName, validCkptLSN, nil
	}

	for {
		header := make([]byte, 12)
		_, err := io.ReadFull(r.file, header)
		if err == io.EOF {
			next, ok := r.sm.SegmentAfter(r.current)
			if !ok {
				return records, validCkptName, validCkptLSN, nil
			}
			if err := r.openSegment(next); err != nil {
				return nil, validCkptName, validCkptLSN, err
			}
			continue
		}
		if err == io.ErrUnexpectedEOF {
			if r.isLastSegment() {
				return records, validCkptName, validCkptLSN, nil
			}
			return records, validCkptName, validCkptLSN, fmt.Errorf("unexpected partial record in non-final segment %s", r.current)
		}
		if err != nil {
			return records, validCkptName, validCkptLSN, err
		}

		remainingLength := binary.LittleEndian.Uint32(header[8:12])
		if remainingLength > maxLogLength {
			return records, validCkptName, validCkptLSN, fmt.Errorf("wal log over max header length: %d", remainingLength)
		}

		remaining := make([]byte, remainingLength)
		_, err = io.ReadFull(r.file, remaining)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if r.isLastSegment() {
				return records, validCkptName, validCkptLSN, nil
			}
			return records, validCkptName, validCkptLSN, fmt.Errorf("unexpected partial record in non-final segment %s", r.current)
		}
		if err != nil {
			return records, validCkptName, validCkptLSN, err
		}

		recordBytes := append(header, remaining...)
		rec, err := record.Decode(recordBytes)

		if err != nil {
			break // CRC mismatch
		}
		if rec == nil {
			continue
		}

		// Checkpointing logic
		opType := rec.OpType
		if opType == record.OpBeginCheckpoint {
			lastBeginCkptLSN = rec.LSN
		}
		if opType == record.OpEndCheckpoint {
			payload := rec.Payload
			endCkpt, err := DecodeEndCheckpoint(payload)
			if err != nil {
				continue
			}
			checksum, err := checksumFile(filepath.Join(r.sm.dir, endCkpt.FileName))
			if err != nil {
				continue // missing/unreadable
			}
			if checksum != endCkpt.FileChecksum {
				continue // corrupt, delete checksum file
			}
			// Checksum matches - discard everything before the matching
			// Prepend corresponding ckpt file
			ckptRecs, err := r.ReadAllCkpt(endCkpt.FileName)
			if err != nil {
				continue
			}
			keepFrom := len(records)
			for i, existing := range records {
				if existing.LSN > lastBeginCkptLSN {
					keepFrom = i
					break
				}
			}
			records = append(ckptRecs, records[keepFrom:]...)
			validCkptName = endCkpt.FileName
			validCkptLSN = lastBeginCkptLSN
		}

		records = append(records, rec)
	}
	return records, validCkptName, validCkptLSN, nil
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
