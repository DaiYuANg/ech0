package store

import (
	"encoding/binary"
	"hash/crc32"
	"os"
)

func readSegmentFrameAt(file *os.File, frame []byte, position int64) error {
	if _, err := file.ReadAt(frame, position); err != nil {
		return wrapExternal(err, "read segment frame")
	}
	return nil
}

func decodeSegmentFrame(frame []byte) (Record, error) {
	if len(frame) < segmentFrameHeader {
		return Record{}, E(CodeCodec, "segment frame length %d is too small", len(frame))
	}
	magic := binary.BigEndian.Uint32(frame[0:4])
	checksum := binary.BigEndian.Uint32(frame[4:8])
	if magic != segmentFrameMagic && magic != segmentFrameZstdMagic {
		return Record{}, E(CodeCodec, "invalid segment frame magic %x", magic)
	}
	body := frame[segmentFrameHeader:]
	if crc32.ChecksumIEEE(body) != checksum {
		return Record{}, E(CodeCodec, "segment frame checksum mismatch")
	}
	body, err := decodeSegmentFramePayload(magic, body)
	if err != nil {
		return Record{}, err
	}
	return decodeSegmentRecordBody(body)
}

func decodeSegmentFramePayload(magic uint32, body []byte) ([]byte, error) {
	switch magic {
	case segmentFrameMagic:
		return body, nil
	case segmentFrameZstdMagic:
		return decompressSegmentFrameZstd(body)
	default:
		return nil, E(CodeCodec, "invalid segment frame magic %x", magic)
	}
}

func decodeSegmentRecordBody(body []byte) (Record, error) {
	decoder := segmentRecordDecoder{body: body}
	record := Record{}
	if err := decoder.readRecordFields(&record); err != nil {
		return Record{}, err
	}
	headers, err := decoder.readHeaders()
	if err != nil {
		return Record{}, err
	}
	payload, err := decoder.readBytes()
	if err != nil {
		return Record{}, err
	}
	record.Headers = headers
	record.Payload = payload
	return record, nil
}

type segmentRecordDecoder struct {
	body   []byte
	cursor int
}

func (d *segmentRecordDecoder) readRecordFields(record *Record) error {
	offset, err := d.readU64("offset")
	if err != nil {
		return err
	}
	timestamp, err := d.readU64("timestamp")
	if err != nil {
		return err
	}
	attributes, err := d.readU16("attributes")
	if err != nil {
		return err
	}
	key, err := d.readBytes()
	if err != nil {
		return err
	}
	record.Offset = offset
	record.TimestampMS = timestamp
	record.Attributes = attributes
	record.Key = key
	return nil
}

func (d *segmentRecordDecoder) readU64(field string) (uint64, error) {
	raw, err := d.readFixed(8, field)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(raw), nil
}

func (d *segmentRecordDecoder) readU16(field string) (uint16, error) {
	raw, err := d.readFixed(2, field)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(raw), nil
}

func (d *segmentRecordDecoder) readFixed(length int, field string) ([]byte, error) {
	if length > d.remaining() {
		return nil, E(CodeCodec, "segment %s length %d exceeds remaining frame size %d", field, length, d.remaining())
	}
	value := d.body[d.cursor : d.cursor+length]
	d.cursor += length
	return value, nil
}

func (d *segmentRecordDecoder) readHeaders() ([]RecordHeader, error) {
	count, err := d.readInt()
	if err != nil {
		return nil, err
	}
	if count > d.remaining()/4 {
		return nil, E(CodeCodec, "segment header count %d exceeds remaining frame size", count)
	}
	headers := make([]RecordHeader, 0, count)
	for range count {
		header, err := d.readHeader()
		if err != nil {
			return nil, err
		}
		headers = append(headers, header)
	}
	return headers, nil
}

func (d *segmentRecordDecoder) readHeader() (RecordHeader, error) {
	key, err := d.readBytes()
	if err != nil {
		return RecordHeader{}, err
	}
	value, err := d.readBytes()
	if err != nil {
		return RecordHeader{}, err
	}
	return RecordHeader{Key: string(key), Value: value}, nil
}

func (d *segmentRecordDecoder) readBytes() ([]byte, error) {
	length, err := d.readInt()
	if err != nil {
		return nil, err
	}
	if length > d.remaining() {
		return nil, E(CodeCodec, "segment bytes length %d exceeds remaining frame size %d", length, d.remaining())
	}
	value := d.body[d.cursor : d.cursor+length]
	d.cursor += length
	return value, nil
}

func (d *segmentRecordDecoder) readInt() (int, error) {
	length := 0
	digits := 0
	for {
		digit, err := d.readByte()
		if err != nil {
			return 0, err
		}
		if digit == ':' {
			return finishSegmentInt(length, digits)
		}
		nextDigit, ok := segmentLengthDigit(digit)
		if !ok {
			return 0, E(CodeCodec, "invalid segment length digit %q", digit)
		}
		length, err = appendSegmentLengthDigit(length, nextDigit)
		if err != nil {
			return 0, err
		}
		digits++
	}
}

func (d *segmentRecordDecoder) readByte() (byte, error) {
	if d.cursor >= len(d.body) {
		return 0, E(CodeCodec, "decode segment length")
	}
	value := d.body[d.cursor]
	d.cursor++
	return value, nil
}

func (d *segmentRecordDecoder) remaining() int {
	return len(d.body) - d.cursor
}

func finishSegmentInt(length, digits int) (int, error) {
	if digits == 0 {
		return 0, E(CodeCodec, "empty segment length")
	}
	return length, nil
}

func segmentLengthDigit(digit byte) (int, bool) {
	if digit < '0' || digit > '9' {
		return 0, false
	}
	return int(digit - '0'), true
}

func appendSegmentLengthDigit(length, digit int) (int, error) {
	if length > (maxSegmentInt()-digit)/10 {
		return 0, E(CodeCodec, "segment length overflows int")
	}
	return length*10 + digit, nil
}

func maxSegmentInt() int {
	return int(^uint(0) >> 1)
}
