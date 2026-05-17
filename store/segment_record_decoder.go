package store

import "encoding/binary"

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
	expiresAt, err := d.readOptionalU64("expires_at_ms")
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
	record.ExpiresAtMS = expiresAt
	record.Attributes = attributes
	record.Key = key
	return nil
}

func (d *segmentRecordDecoder) readOptionalU64(field string) (*uint64, error) {
	present, err := d.readByte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return noRecordExpiry(), nil
	}
	if present != 1 {
		return nil, E(CodeCodec, "invalid segment optional %s marker %d", field, present)
	}
	value, err := d.readU64(field)
	if err != nil {
		return nil, err
	}
	return &value, nil
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

func (d *segmentRecordDecoder) readU32(field string) (uint32, error) {
	raw, err := d.readFixed(4, field)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(raw), nil
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

func (d *segmentRecordDecoder) readBytesU32(field string) ([]byte, error) {
	length, err := d.readU32(field + " length")
	if err != nil {
		return nil, err
	}
	lengthInt, err := segmentU32ToInt(length, field+" length")
	if err != nil {
		return nil, err
	}
	if lengthInt > d.remaining() {
		return nil, E(CodeCodec, "segment %s length %d exceeds remaining frame size %d", field, length, d.remaining())
	}
	value := d.body[d.cursor : d.cursor+lengthInt]
	d.cursor += lengthInt
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
