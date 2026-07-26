package record

import (
	"encoding/binary"
	"fmt"
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
// LSN (uint64) - length (uint32) - opType (uint8) - queueNameLength (uint32) - queueName (string) - payload (bytes) - CRC checksum (uint32)
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
	copy(buf[17:17+queueNameLen], record.QueueName) // copy accepts a string source directly
	copy(buf[17+queueNameLen:17+queueNameLen+payloadLen], record.Payload)

	crc := crc32.ChecksumIEEE(buf[:totalLen-4])
	binary.LittleEndian.PutUint32(buf[totalLen-4:], crc)
	return buf, nil
}

func Decode(data []byte) (*Record, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("data is too short")
	}

	lsn := binary.LittleEndian.Uint64(data[0:8])
	remainingLength := int(binary.LittleEndian.Uint32(data[8:12]))

	if len(data) < remainingLength+12 {
		return nil, fmt.Errorf("data is too short")
	}

	opType := OpType(data[12])
	queueNameLen := int(binary.LittleEndian.Uint32(data[13:17]))
	queueName := string(data[17 : 17+queueNameLen]) // string() conversion copies, safe against buffer reuse

	payloadStart := 17 + queueNameLen
	payloadEnd := remainingLength + 8
	payload := make([]byte, payloadEnd-payloadStart)
	copy(payload, data[payloadStart:payloadEnd]) // explicit copy — []byte doesn't get this for free

	crc := binary.LittleEndian.Uint32(data[remainingLength+8 : remainingLength+12])
	computedCrc := crc32.ChecksumIEEE(data[:remainingLength+8])
	if crc != computedCrc {
		return nil, fmt.Errorf("invalid CRC on log")
	}

	return &Record{
		LSN:       lsn,
		OpType:    opType,
		QueueName: queueName,
		Payload:   payload,
	}, nil
}
