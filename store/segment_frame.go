package store

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"strconv"

	"github.com/lyonbrown4d/ech0/internal/bufferpool"
)

const (
	segmentFrameMagic        uint32 = 0x45434831
	segmentFrameZstdMagic    uint32 = 0x45435a31
	segmentBatchMagic        uint32 = 0x45434231
	segmentBatchZstdMagic    uint32 = 0x45425a31
	segmentFrameHeader       int    = 12
	segmentFrameLegacyHeader int    = 8
	segmentBatchHeader       int    = 4
	segmentBatchItemHeader   int    = 4

	legacySegmentFrameMagic     uint32 = 0x45434830
	legacySegmentFrameZstdMagic uint32 = 0x45435a30
	legacySegmentBatchMagic     uint32 = 0x45434230
	legacySegmentBatchZstdMagic uint32 = 0x4543425a
)

func encodeSegmentFrameWithCompression(record Record, compression segmentFrameCompression) ([]byte, error) {
	bodyBuffer := bufferpool.Get()
	defer bufferpool.Put(bodyBuffer)
	bodyBuffer.B = appendSegmentRecordBody(bodyBuffer.B, record)
	body := bodyBuffer.B
	magic, payload, err := compression.encode(body)
	if err != nil {
		return nil, err
	}
	return encodeSegmentFrameEnvelope(magic, payload)
}

func encodeSegmentBatchFrameWithCompression(records []Record, compression segmentFrameCompression) ([]byte, error) {
	if len(records) == 1 {
		return encodeSegmentFrameWithCompression(records[0], compression)
	}
	count, countErr := segmentIntToU32(len(records), "segment batch count")
	if countErr != nil {
		return nil, countErr
	}
	bodyBuffer := bufferpool.Get()
	defer bufferpool.Put(bodyBuffer)
	bodyBuffer.B = binary.BigEndian.AppendUint32(bodyBuffer.B, count)
	for _, record := range records {
		start := len(bodyBuffer.B)
		bodyBuffer.B = append(bodyBuffer.B, 0, 0, 0, 0)
		itemStart := len(bodyBuffer.B)
		bodyBuffer.B = appendSegmentRecordBody(bodyBuffer.B, record)
		size, sizeErr := segmentIntToU32(len(bodyBuffer.B)-itemStart, "segment batch record size")
		if sizeErr != nil {
			return nil, sizeErr
		}
		binary.BigEndian.PutUint32(bodyBuffer.B[start:itemStart], size)
	}
	magic, payload, err := compression.encodeBatch(bodyBuffer.B)
	if err != nil {
		return nil, err
	}
	return encodeSegmentFrameEnvelope(magic, payload)
}

func appendSegmentRecordBody(out []byte, record Record) []byte {
	out = appendSegmentRecordFields(out, record)
	out = appendSegmentTransaction(out, record.Transaction)
	out = appendSegmentHeaders(out, record.Headers)
	return appendSegmentBytes(out, record.Payload)
}

func segmentRecordBodySize(record Record) int {
	size := 8 + 8 + 1 + 2
	if record.ExpiresAtMS != nil {
		size += 8
	}
	size += segmentBytesEncodedLen(len(record.Key))
	size++
	if record.Transaction != nil {
		size += 8 + 8 + 8 + 8 + 1
	}
	size += segmentIntEncodedLen(len(record.Headers))
	for _, header := range record.Headers {
		size += segmentStringEncodedLen(header.Key)
		size += segmentBytesEncodedLen(len(header.Value))
	}
	size += segmentBytesEncodedLen(len(record.Payload))
	return size
}

func appendSegmentRecordFields(out []byte, record Record) []byte {
	out = binary.BigEndian.AppendUint64(out, record.Offset)
	out = binary.BigEndian.AppendUint64(out, record.TimestampMS)
	out = appendSegmentOptionalU64(out, record.ExpiresAtMS)
	out = binary.BigEndian.AppendUint16(out, record.Attributes)
	return appendSegmentBytes(out, record.Key)
}

func appendSegmentOptionalU64(out []byte, value *uint64) []byte {
	if value == nil {
		return append(out, 0)
	}
	out = append(out, 1)
	return binary.BigEndian.AppendUint64(out, *value)
}

func appendSegmentTransaction(out []byte, metadata *TransactionRecordMetadata) []byte {
	if metadata == nil {
		return append(out, 0)
	}
	out = append(out, 1)
	out = binary.BigEndian.AppendUint64(out, metadata.TxID)
	out = binary.BigEndian.AppendUint64(out, metadata.ProducerID)
	out = binary.BigEndian.AppendUint64(out, metadata.ProducerEpoch)
	out = binary.BigEndian.AppendUint64(out, metadata.Sequence)
	return append(out, encodeSegmentTransactionControl(metadata.ControlType))
}

func encodeSegmentTransactionControl(controlType TransactionControlType) byte {
	switch controlType {
	case TransactionControlNone:
		return 0
	case TransactionControlCommit:
		return 1
	case TransactionControlAbort:
		return 2
	default:
		return 0
	}
}

func appendSegmentHeaders(out []byte, headers []RecordHeader) []byte {
	out = appendSegmentIntBytes(out, len(headers))
	for _, header := range headers {
		out = appendSegmentString(out, header.Key)
		out = appendSegmentBytes(out, header.Value)
	}
	return out
}

func encodeSegmentFrameEnvelope(magic uint32, body []byte) ([]byte, error) {
	bodyLen, err := segmentIntToU32(len(body), "segment frame body length")
	if err != nil {
		return nil, err
	}
	frame := make([]byte, segmentFrameHeader+len(body))
	binary.BigEndian.PutUint32(frame[0:4], magic)
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(body))
	binary.BigEndian.PutUint32(frame[8:12], bodyLen)
	copy(frame[segmentFrameHeader:], body)
	return frame, nil
}

func appendSegmentString(out []byte, value string) []byte {
	out = appendSegmentIntBytes(out, len(value))
	return append(out, value...)
}

func appendSegmentBytes(out, value []byte) []byte {
	out = appendSegmentIntBytes(out, len(value))
	return append(out, value...)
}

func appendSegmentIntBytes(out []byte, value int) []byte {
	out = strconv.AppendInt(out, int64(value), 10)
	return append(out, ':')
}

func segmentBytesEncodedLen(length int) int {
	return segmentIntEncodedLen(length) + length
}

func segmentStringEncodedLen(value string) int {
	return segmentIntEncodedLen(len(value)) + len(value)
}

func segmentIntEncodedLen(value int) int {
	if value == 0 {
		return 2
	}
	digits := 0
	for value > 0 {
		digits++
		value /= 10
	}
	return digits + 1
}

func estimatedSegmentFrameSize(record Record) int {
	return segmentFrameHeader + segmentBatchHeader + segmentBatchItemHeader + segmentRecordBodySize(record)
}

func readSegmentRecords(file *os.File, pointers []segmentRecordPointer) ([]Record, error) {
	return readSegmentPointerRecords(pointers, func(pointer segmentRecordPointer) ([]Record, error) {
		frameData := make([]byte, pointer.Length)
		if err := readSegmentFrameAt(file, frameData, pointer.Position); err != nil {
			return nil, err
		}
		return decodeSegmentFrameRecords(frameData)
	})
}

func readMappedSegmentRecords(data []byte, pointers []segmentRecordPointer) ([]Record, error) {
	return readSegmentPointerRecords(pointers, func(pointer segmentRecordPointer) ([]Record, error) {
		frameData, err := mappedSegmentSpan(data, pointer.Position, pointer.Length)
		if err != nil {
			return nil, err
		}
		return decodeSegmentFrameRecords(frameData)
	})
}

func readSegmentPointerRecords(
	pointers []segmentRecordPointer,
	load func(segmentRecordPointer) ([]Record, error),
) ([]Record, error) {
	records := make([]Record, len(pointers))
	cache := segmentFrameRecordCache{}
	for index, pointer := range pointers {
		record, err := cache.record(pointer, load)
		if err != nil {
			return nil, err
		}
		records[index] = record
	}
	return records, nil
}

type segmentFrameRecordCache struct {
	key     segmentFrameReadKey
	loaded  bool
	records []Record
}

func (c *segmentFrameRecordCache) record(
	pointer segmentRecordPointer,
	load func(segmentRecordPointer) ([]Record, error),
) (Record, error) {
	key := newSegmentFrameReadKey(pointer)
	if !c.loaded || key != c.key {
		records, err := load(pointer)
		if err != nil {
			return Record{}, err
		}
		c.key = key
		c.loaded = true
		c.records = records
	}
	return findSegmentPointerRecord(pointer, c.records)
}

type segmentFrameReadKey struct {
	position int64
	length   int
}

func newSegmentFrameReadKey(pointer segmentRecordPointer) segmentFrameReadKey {
	return segmentFrameReadKey{position: pointer.Position, length: pointer.Length}
}

func findSegmentPointerRecord(pointer segmentRecordPointer, records []Record) (Record, error) {
	for _, record := range records {
		if record.Offset == pointer.Offset {
			return record, nil
		}
	}
	return Record{}, E(CodeCodec, "segment record offset %d not found in frame", pointer.Offset)
}

func mappedSegmentSpan(data []byte, position int64, length int) ([]byte, error) {
	if position < 0 {
		return nil, E(CodeCodec, "segment mmap position %d is negative", position)
	}
	if length < segmentFrameLegacyHeader {
		return nil, E(CodeCodec, "segment mmap length %d is too small", length)
	}
	start := position
	end := position + int64(length)
	if end < start || end > int64(len(data)) {
		return nil, E(CodeCodec, "segment mmap span exceeds file: position=%d length=%d file_size=%d", position, length, len(data))
	}
	return data[int(start):int(end)], nil
}
