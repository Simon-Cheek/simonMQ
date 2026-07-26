package wal

import (
	"durable-mq/record"
	"encoding/binary"
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

func (r *Reader) ReadAll() ([]*record.Record, error) {
	var records []*record.Record

	for {
		header := make([]byte, 12) // Enough to hold LSN and length indicator
		n, err := io.ReadFull(r.file, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF || n != 12 {
			break
		}
		if err != nil {
			return nil, err
		}
		remainingLength := binary.LittleEndian.Uint32(header[8:12])

		remaining := make([]byte, remainingLength)
		n, err = io.ReadFull(r.file, remaining)
		if err == io.EOF || err == io.ErrUnexpectedEOF || n < int(remainingLength) {
			break
		}
		if err != nil {
			return nil, err
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
