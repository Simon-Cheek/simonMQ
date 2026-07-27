package wal

import (
	"durable-mq/record"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type Reader struct {
	file *os.File
}

func OpenReader(filename string) (*Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	return &Reader{file}, nil
}

func (r *Reader) Close() error {
	return r.file.Close()
}

// Any Log bigger than this likely means some sort of corrupt data issue
const maxLogLength = 16 * 1024 * 1024

// TODO: Add logic to handle hardware corruption, checksum corruption, etc
func (r *Reader) ReadAll() ([]*record.Record, error) {
	var records []*record.Record

	for {
		header := make([]byte, 12) // Enough to hold LSN and length indicator
		_, err := io.ReadFull(r.file, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
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
			break
		}
		if err != nil {
			return records, err
		}
		recordBytes := append(header, remaining...)
		rec, err := record.Decode(recordBytes)
		if err != nil {
			break // CRC Mismatch
		}
		records = append(records, rec)
	}
	return records, nil
}
