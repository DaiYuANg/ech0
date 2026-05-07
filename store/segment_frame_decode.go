package store

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"strconv"
)

func readSegmentFrameAt(file *os.File, frame []byte, position int64) error {
	if _, err := file.ReadAt(frame, position); err != nil {
		return wrapExternal(err, "read segment frame")
	}
	return nil
}

func decodeSegmentFrame(frame []byte) (Record, error) {
	reader := bytes.NewReader(frame)
	magic, checksum, err := readSegmentFrameHeader(reader)
	if err != nil {
		return Record{}, err
	}
	if magic != segmentFrameMagic && magic != segmentFrameZstdMagic {
		return Record{}, E(CodeCodec, "invalid segment frame magic %x", magic)
	}
	body, err := readSegmentFrameBody(reader)
	if err != nil {
		return Record{}, err
	}
	if crc32.ChecksumIEEE(body) != checksum {
		return Record{}, E(CodeCodec, "segment frame checksum mismatch")
	}
	body, err = decodeSegmentFramePayload(magic, body)
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

func readSegmentFrameHeader(reader *bytes.Reader) (uint32, uint32, error) {
	var magic uint32
	if err := binary.Read(reader, binary.BigEndian, &magic); err != nil {
		return 0, 0, wrapExternal(err, "decode segment magic")
	}
	var checksum uint32
	if err := binary.Read(reader, binary.BigEndian, &checksum); err != nil {
		return 0, 0, wrapExternal(err, "decode segment checksum")
	}
	return magic, checksum, nil
}

func readSegmentFrameBody(reader *bytes.Reader) ([]byte, error) {
	body := make([]byte, reader.Len())
	if _, err := io.ReadFull(reader, body); err != nil {
		return nil, wrapExternal(err, "read segment frame body")
	}
	return body, nil
}

func decodeSegmentRecordBody(body []byte) (Record, error) {
	reader := bytes.NewReader(body)
	record := Record{}
	if err := readSegmentRecordFields(reader, &record); err != nil {
		return Record{}, err
	}
	headers, err := readSegmentHeaders(reader)
	if err != nil {
		return Record{}, err
	}
	payload, err := readSegmentBytes(reader)
	if err != nil {
		return Record{}, err
	}
	record.Headers = headers
	record.Payload = payload
	return record, nil
}

func readSegmentRecordFields(reader *bytes.Reader, record *Record) error {
	if err := binary.Read(reader, binary.BigEndian, &record.Offset); err != nil {
		return wrapExternal(err, "decode segment offset")
	}
	if err := binary.Read(reader, binary.BigEndian, &record.TimestampMS); err != nil {
		return wrapExternal(err, "decode segment timestamp")
	}
	if err := binary.Read(reader, binary.BigEndian, &record.Attributes); err != nil {
		return wrapExternal(err, "decode segment attributes")
	}
	key, err := readSegmentBytes(reader)
	if err != nil {
		return err
	}
	record.Key = key
	return nil
}

func readSegmentHeaders(reader *bytes.Reader) ([]RecordHeader, error) {
	count, err := readSegmentInt(reader)
	if err != nil {
		return nil, err
	}
	if count > reader.Len()/4 {
		return nil, E(CodeCodec, "segment header count %d exceeds remaining frame size", count)
	}
	headers := make([]RecordHeader, 0, count)
	for range count {
		header, err := readSegmentHeader(reader)
		if err != nil {
			return nil, err
		}
		headers = append(headers, header)
	}
	return headers, nil
}

func readSegmentHeader(reader *bytes.Reader) (RecordHeader, error) {
	key, err := readSegmentBytes(reader)
	if err != nil {
		return RecordHeader{}, err
	}
	value, err := readSegmentBytes(reader)
	if err != nil {
		return RecordHeader{}, err
	}
	return RecordHeader{Key: string(key), Value: value}, nil
}

func readSegmentBytes(reader *bytes.Reader) ([]byte, error) {
	length, err := readSegmentInt(reader)
	if err != nil {
		return nil, err
	}
	if length > reader.Len() {
		return nil, E(CodeCodec, "segment bytes length %d exceeds remaining frame size %d", length, reader.Len())
	}
	value := make([]byte, length)
	if _, err := io.ReadFull(reader, value); err != nil {
		return nil, wrapExternal(err, "decode segment bytes")
	}
	return value, nil
}

func readSegmentInt(reader *bytes.Reader) (int, error) {
	digits, err := readSegmentLengthDigits(reader)
	if err != nil {
		return 0, err
	}
	if digits.Len() == 0 {
		return 0, E(CodeCodec, "empty segment length")
	}
	length, err := strconv.Atoi(digits.String())
	if err != nil {
		return 0, E(CodeCodec, "invalid segment length %q: %v", digits.String(), err)
	}
	return length, nil
}

func readSegmentLengthDigits(reader *bytes.Reader) (*bytes.Buffer, error) {
	digits := bytes.NewBuffer(nil)
	for {
		digit, err := reader.ReadByte()
		if err != nil {
			return nil, wrapExternal(err, "decode segment length")
		}
		if digit == ':' {
			break
		}
		if digit < '0' || digit > '9' {
			return nil, E(CodeCodec, "invalid segment length digit %q", digit)
		}
		if err := digits.WriteByte(digit); err != nil {
			return nil, wrapExternal(err, "decode segment length digit")
		}
	}
	return digits, nil
}
