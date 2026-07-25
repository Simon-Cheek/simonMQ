package durable_mq

import (
	"encoding/binary"
	"hash/crc32"
)

type OpType uint8

const (
	OpEnqueue OpType = iota + 1
	OpAck
	OpCreateQueue
	OpDeleteQueue
	OpUpdateSubPolicy
	OpDeleteSubPolicy
)

type Record struct {
	LSN       uint64
	OpType    OpType
	QueueName string
	Payload   []byte
}

// Encode converts WAL record into Bytes
// LSN (uint64) - length (uint32) - opType (uint8) - queueNameLength (uint32) - queueName (string) - payload (string) - CRC checksum (uint32)
// Storing total len as uint32 caps length of queueName + payload at ~4B
func Encode(record *Record) ([]byte, error) {
	queueNameLen := len(record.QueueName)
	payloadLen := len(record.Payload)
	totalLen := 8 + 4 + 1 + 4 + queueNameLen + payloadLen + 4
	lenField := uint32(totalLen - 12) // bytes after LSN + length field

	buf := make([]byte, totalLen)
	binary.LittleEndian.PutUint64(buf[0:8], record.LSN)
	binary.LittleEndian.PutUint32(buf[8:12], lenField)
	buf[12] = byte(record.OpType)
	binary.LittleEndian.PutUint32(buf[13:17], uint32(queueNameLen))
	copy(buf[17:17+queueNameLen], record.QueueName)
	copy(buf[17+queueNameLen:17+queueNameLen+payloadLen], record.Payload)

	crc := crc32.ChecksumIEEE(buf[:totalLen-4])
	binary.LittleEndian.PutUint32(buf[totalLen-4:], crc)
	return buf, nil
}

func Decode(data []byte) (*Record, error) {}
