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

func decodeSegmentFrameRecords(frame []byte) ([]Record, error) {
	if len(frame) < segmentFrameLegacyHeader {
		return nil, E(CodeCodec, "segment frame length %d is too small", len(frame))
	}
	magic := binary.BigEndian.Uint32(frame[0:4])
	checksum := binary.BigEndian.Uint32(frame[4:8])
	if !validSegmentFrameMagic(magic) {
		return nil, E(CodeCodec, "invalid segment frame magic %x", magic)
	}
	body, err := segmentFrameBody(magic, frame)
	if err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(body) != checksum {
		return nil, E(CodeCodec, "segment frame checksum mismatch")
	}
	body, err = decodeSegmentFramePayload(magic, body)
	if err != nil {
		return nil, err
	}
	if segmentFrameMagicIsBatch(magic) {
		return decodeSegmentBatchBody(body)
	}
	record, err := decodeSegmentRecordBody(body)
	if err != nil {
		return nil, err
	}
	return []Record{record}, nil
}

func validSegmentFrameMagic(magic uint32) bool {
	return magic == segmentFrameMagic ||
		magic == segmentFrameZstdMagic ||
		magic == segmentBatchMagic ||
		magic == segmentBatchZstdMagic ||
		magic == legacySegmentFrameMagic ||
		magic == legacySegmentFrameZstdMagic ||
		magic == legacySegmentBatchMagic ||
		magic == legacySegmentBatchZstdMagic
}

func segmentFrameMagicIsBatch(magic uint32) bool {
	return magic == segmentBatchMagic ||
		magic == segmentBatchZstdMagic ||
		magic == legacySegmentBatchMagic ||
		magic == legacySegmentBatchZstdMagic
}

func segmentFrameBody(magic uint32, frame []byte) ([]byte, error) {
	if !sizedSegmentFrameMagic(magic) {
		return frame[segmentFrameLegacyHeader:], nil
	}
	if len(frame) < segmentFrameHeader {
		return nil, E(CodeCodec, "segment frame length %d is too small", len(frame))
	}
	bodyLen := binary.BigEndian.Uint32(frame[8:12])
	length, err := segmentU32ToInt(bodyLen, "segment frame body length")
	if err != nil {
		return nil, err
	}
	if len(frame) != segmentFrameHeader+length {
		return nil, E(CodeCodec, "segment frame length mismatch: header=%d actual=%d", length, len(frame)-segmentFrameHeader)
	}
	return frame[segmentFrameHeader:], nil
}

func sizedSegmentFrameMagic(magic uint32) bool {
	return magic == segmentFrameMagic ||
		magic == segmentFrameZstdMagic ||
		magic == segmentBatchMagic ||
		magic == segmentBatchZstdMagic
}

func decodeSegmentFramePayload(magic uint32, body []byte) ([]byte, error) {
	switch magic {
	case segmentFrameMagic, segmentBatchMagic, legacySegmentFrameMagic, legacySegmentBatchMagic:
		return body, nil
	case segmentFrameZstdMagic, segmentBatchZstdMagic, legacySegmentFrameZstdMagic, legacySegmentBatchZstdMagic:
		return decompressSegmentFrameZstd(body)
	default:
		return nil, E(CodeCodec, "invalid segment frame magic %x", magic)
	}
}

func decodeSegmentBatchBody(body []byte) ([]Record, error) {
	decoder := segmentRecordDecoder{body: body}
	count, err := decoder.readU32("batch count")
	if err != nil {
		return nil, err
	}
	size, err := segmentU32ToInt(count, "segment batch count")
	if err != nil {
		return nil, err
	}
	records := make([]Record, 0, size)
	for range size {
		item, err := decoder.readBytesU32("batch record")
		if err != nil {
			return nil, err
		}
		record, err := decodeSegmentRecordBody(item)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if decoder.remaining() != 0 {
		return nil, E(CodeCodec, "segment batch frame has %d trailing bytes", decoder.remaining())
	}
	return records, nil
}

func decodeSegmentRecordBody(body []byte) (Record, error) {
	decoder := segmentRecordDecoder{body: body}
	record := Record{}
	if err := decoder.readRecordFields(&record); err != nil {
		return Record{}, err
	}
	transaction, hasTransaction, err := decoder.readTransaction()
	if err != nil {
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
	if hasTransaction {
		record.Transaction = &transaction
	}
	record.Headers = headers
	record.Payload = payload
	return record, nil
}
