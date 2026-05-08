package store

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	body, err := encodeSegmentRecordBody(record)
	if err != nil {
		return nil, err
	}
	magic, payload, err := compression.encode(body)
	if err != nil {
		return nil, err
	}
	return encodeSegmentFrameEnvelope(magic, payload)
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

func encodeSegmentFrameEnvelope(magic uint32, body []byte) ([]byte, error) {
	frame := bytes.NewBuffer(make([]byte, 0, segmentFrameHeader+len(body)))
	if err := binary.Write(frame, binary.BigEndian, magic); err != nil {
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

func readSegmentRecords(rootDir, relativePath string, pointers []segmentRecordPointer) ([]Record, error) {
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		return nil, wrapExternal(err, "open segment root")
	}
	file, err := root.Open(relativePath)
	if err != nil {
		return nil, errors.Join(wrapExternal(err, "open segment file"), wrapExternal(root.Close(), "close segment root"))
	}
	records := make([]Record, 0, len(pointers))
	for _, pointer := range pointers {
		record, readErr := readSegmentPointer(file, pointer)
		if readErr != nil {
			return nil, errors.Join(readErr, wrapExternal(errors.Join(file.Close(), root.Close()), "close segment file"))
		}
		records = append(records, record)
	}
	closeErr := errors.Join(file.Close(), root.Close())
	if closeErr != nil {
		return nil, wrapExternal(closeErr, "close segment file")
	}
	return records, nil
}

func readSegmentPointer(file *os.File, pointer segmentRecordPointer) (Record, error) {
	if pointer.Length < segmentFrameHeader {
		return Record{}, E(CodeCodec, "segment frame length %d is too small", pointer.Length)
	}
	frame := make([]byte, pointer.Length)
	if err := readSegmentFrameAt(file, frame, pointer.Position); err != nil {
		return Record{}, err
	}
	record, err := decodeSegmentFrame(frame)
	if err != nil {
		return Record{}, err
	}
	if record.Offset != pointer.Offset {
		return Record{}, E(CodeCodec, "segment record offset mismatch: index=%d record=%d", pointer.Offset, record.Offset)
	}
	return record, nil
}
