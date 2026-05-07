package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"strconv"
)

const (
	segmentFrameMagic  uint32 = 0x45434830
	segmentFrameHeader int    = 8
)

func encodeSegmentFrame(record Record) ([]byte, error) {
	body, err := encodeSegmentRecordBody(record)
	if err != nil {
		return nil, err
	}
	return encodeSegmentFrameEnvelope(body)
}

func encodeSegmentRecordBody(record Record) ([]byte, error) {
	body := bytes.NewBuffer(nil)
	if err := writeSegmentRecordFields(body, record); err != nil {
		return nil, err
	}
	if err := writeSegmentHeaders(body, record.Headers); err != nil {
		return nil, err
	}
	if err := writeSegmentBytes(body, record.Payload); err != nil {
		return nil, err
	}
	return body.Bytes(), nil
}

func writeSegmentRecordFields(out *bytes.Buffer, record Record) error {
	if err := binary.Write(out, binary.BigEndian, record.Offset); err != nil {
		return wrapExternal(err, "encode segment offset")
	}
	if err := binary.Write(out, binary.BigEndian, record.TimestampMS); err != nil {
		return wrapExternal(err, "encode segment timestamp")
	}
	if err := binary.Write(out, binary.BigEndian, record.Attributes); err != nil {
		return wrapExternal(err, "encode segment attributes")
	}
	return writeSegmentBytes(out, record.Key)
}

func writeSegmentHeaders(out *bytes.Buffer, headers []RecordHeader) error {
	if err := writeSegmentInt(out, len(headers)); err != nil {
		return err
	}
	for _, header := range headers {
		if err := writeSegmentString(out, header.Key); err != nil {
			return err
		}
		if err := writeSegmentBytes(out, header.Value); err != nil {
			return err
		}
	}
	return nil
}

func encodeSegmentFrameEnvelope(body []byte) ([]byte, error) {
	frame := bytes.NewBuffer(make([]byte, 0, segmentFrameHeader+len(body)))
	if err := binary.Write(frame, binary.BigEndian, segmentFrameMagic); err != nil {
		return nil, wrapExternal(err, "encode segment magic")
	}
	if err := binary.Write(frame, binary.BigEndian, crc32.ChecksumIEEE(body)); err != nil {
		return nil, wrapExternal(err, "encode segment checksum")
	}
	if _, err := frame.Write(body); err != nil {
		return nil, wrapExternal(err, "encode segment payload")
	}
	return frame.Bytes(), nil
}

func writeSegmentString(out *bytes.Buffer, value string) error {
	return writeSegmentBytes(out, []byte(value))
}

func writeSegmentBytes(out *bytes.Buffer, value []byte) error {
	if err := writeSegmentInt(out, len(value)); err != nil {
		return err
	}
	if _, err := out.Write(value); err != nil {
		return wrapExternal(err, "encode segment bytes")
	}
	return nil
}

func writeSegmentInt(out *bytes.Buffer, value int) error {
	if value < 0 {
		return E(CodeInvalidArgument, "negative segment length: %d", value)
	}
	if _, err := out.WriteString(strconv.Itoa(value)); err != nil {
		return wrapExternal(err, "encode segment length")
	}
	if err := out.WriteByte(':'); err != nil {
		return wrapExternal(err, "encode segment length separator")
	}
	return nil
}

func readSegmentRecord(rootDir, relativePath string, position int64, length int) (Record, error) {
	if length < segmentFrameHeader {
		return Record{}, E(CodeCodec, "segment frame length %d is too small", length)
	}
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		return Record{}, wrapExternal(err, "open segment root")
	}
	file, err := root.Open(relativePath)
	if err != nil {
		return Record{}, errors.Join(wrapExternal(err, "open segment file"), wrapExternal(root.Close(), "close segment root"))
	}
	frame := make([]byte, length)
	readErr := readSegmentFrameAt(file, frame, position)
	closeErr := errors.Join(file.Close(), root.Close())
	if readErr != nil {
		return Record{}, errors.Join(readErr, wrapExternal(closeErr, "close segment file"))
	}
	if closeErr != nil {
		return Record{}, wrapExternal(closeErr, "close segment file")
	}
	record, err := decodeSegmentFrame(frame)
	if err != nil {
		return Record{}, err
	}
	return record, nil
}

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
	if magic != segmentFrameMagic {
		return Record{}, E(CodeCodec, "invalid segment frame magic %x", magic)
	}
	body, err := readSegmentFrameBody(reader)
	if err != nil {
		return Record{}, err
	}
	if crc32.ChecksumIEEE(body) != checksum {
		return Record{}, E(CodeCodec, "segment frame checksum mismatch")
	}
	return decodeSegmentRecordBody(body)
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
