package store

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"strconv"
)

const (
	segmentFrameMagic     uint32 = 0x45434830
	segmentFrameZstdMagic uint32 = 0x45435a30
	segmentFrameHeader    int    = 8
)

func encodeSegmentFrameWithCompression(record Record, compression segmentFrameCompression) ([]byte, error) {
	body := encodeSegmentRecordBody(record)
	magic, payload, err := compression.encode(body)
	if err != nil {
		return nil, err
	}
	return encodeSegmentFrameEnvelope(magic, payload)
}

func encodeSegmentRecordBody(record Record) []byte {
	body := make([]byte, 0, segmentRecordBodySize(record))
	body = appendSegmentRecordFields(body, record)
	body = appendSegmentHeaders(body, record.Headers)
	body = appendSegmentBytes(body, record.Payload)
	return body
}

func segmentRecordBodySize(record Record) int {
	size := 8 + 8 + 2
	size += segmentBytesEncodedLen(len(record.Key))
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
	out = binary.BigEndian.AppendUint16(out, record.Attributes)
	return appendSegmentBytes(out, record.Key)
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
	frame := make([]byte, segmentFrameHeader+len(body))
	binary.BigEndian.PutUint32(frame[0:4], magic)
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(body))
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

func readSegmentRecords(file *os.File, pointers []segmentRecordPointer) ([]Record, error) {
	records := make([]Record, len(pointers))
	spans, err := contiguousSegmentPointerSpans(pointers)
	if err != nil {
		return nil, err
	}
	for _, span := range spans {
		if err := readSegmentPointerSpan(file, pointers, span, records); err != nil {
			return nil, err
		}
	}
	return records, nil
}

type segmentPointerSpan struct {
	start    int
	end      int
	position int64
	length   int
}

func contiguousSegmentPointerSpans(pointers []segmentRecordPointer) ([]segmentPointerSpan, error) {
	if len(pointers) == 0 {
		return nil, nil
	}
	nextPosition, err := segmentPointerEnd(pointers[0])
	if err != nil {
		return nil, err
	}
	spans := make([]segmentPointerSpan, 0, 1)
	current := segmentPointerSpan{start: 0, end: 1, position: pointers[0].Position, length: pointers[0].Length}
	for index := 1; index < len(pointers); index++ {
		pointer := pointers[index]
		pointerEnd, err := segmentPointerEnd(pointer)
		if err != nil {
			return nil, err
		}
		if pointer.Position == nextPosition {
			current.end = index + 1
			current.length += pointer.Length
			nextPosition = pointerEnd
			continue
		}
		spans = append(spans, current)
		current = segmentPointerSpan{start: index, end: index + 1, position: pointer.Position, length: pointer.Length}
		nextPosition = pointerEnd
	}
	return append(spans, current), nil
}

func segmentPointerEnd(pointer segmentRecordPointer) (int64, error) {
	if pointer.Position < 0 {
		return 0, E(CodeCodec, "segment frame position %d is negative", pointer.Position)
	}
	if pointer.Length < segmentFrameHeader {
		return 0, E(CodeCodec, "segment frame length %d is too small", pointer.Length)
	}
	length := int64(pointer.Length)
	const maxPosition = int64(1<<63 - 1)
	if pointer.Position > maxPosition-length {
		return 0, E(CodeCodec, "segment frame position overflows int64: position=%d length=%d", pointer.Position, pointer.Length)
	}
	return pointer.Position + length, nil
}

func readSegmentPointerSpan(file *os.File, pointers []segmentRecordPointer, span segmentPointerSpan, records []Record) error {
	if span.length < segmentFrameHeader {
		return E(CodeCodec, "segment pointer span length %d is too small", span.length)
	}
	frameData := make([]byte, span.length)
	if err := readSegmentFrameAt(file, frameData, span.position); err != nil {
		return err
	}
	cursor := 0
	for pointerIndex := span.start; pointerIndex < span.end; pointerIndex++ {
		pointer := pointers[pointerIndex]
		next := cursor + pointer.Length
		if next > len(frameData) {
			return E(CodeCodec, "segment pointer span decode exceeds buffer: next=%d len=%d", next, len(frameData))
		}
		record, err := decodeSegmentPointerFrame(pointer, frameData[cursor:next])
		if err != nil {
			return err
		}
		records[pointerIndex] = record
		cursor = next
	}
	return nil
}

func decodeSegmentPointerFrame(pointer segmentRecordPointer, frame []byte) (Record, error) {
	record, err := decodeSegmentFrame(frame)
	if err != nil {
		return Record{}, err
	}
	if record.Offset != pointer.Offset {
		return Record{}, E(CodeCodec, "segment record offset mismatch: index=%d record=%d", pointer.Offset, record.Offset)
	}
	return record, nil
}
