package store

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strconv"
)

const (
	segmentIndexEntryMagic       uint32 = 0x45534931
	segmentIndexEntryDeleteFlag  uint16 = 1 << 0
	segmentIndexEntryHeaderBytes int    = 4
)

type segmentIndexEntry struct {
	Pointer segmentRecordPointer
	Deleted bool
}

func encodeSegmentIndexEntry(entry segmentIndexEntry) ([]byte, error) {
	pointer := entry.Pointer
	if pointer.Position < 0 {
		return nil, E(CodeCodec, "segment pointer position %d is negative", pointer.Position)
	}
	if pointer.Length < 0 {
		return nil, E(CodeCodec, "segment pointer length %d is negative", pointer.Length)
	}
	topicLen := len(pointer.Topic)
	if topicLen > math.MaxUint32 {
		return nil, E(CodeCodec, "segment pointer topic length %d exceeds uint32", topicLen)
	}
	flags := uint16(0)
	if entry.Deleted {
		flags |= segmentIndexEntryDeleteFlag
	}
	out := make([]byte, 0, segmentIndexEntryEncodedLen(pointer))
	out = binary.BigEndian.AppendUint32(out, segmentIndexEntryMagic)
	out = binary.BigEndian.AppendUint16(out, flags)
	out = binary.BigEndian.AppendUint32(out, uint32(topicLen))
	out = append(out, pointer.Topic...)
	out = binary.BigEndian.AppendUint32(out, pointer.Partition)
	out = binary.BigEndian.AppendUint64(out, pointer.Offset)
	out = binary.BigEndian.AppendUint64(out, pointer.SegmentID)
	out = binary.BigEndian.AppendUint64(out, uint64(pointer.Position))
	out = binary.BigEndian.AppendUint64(out, uint64(pointer.Length))
	out = binary.BigEndian.AppendUint64(out, pointer.TimestampMS)
	out = binary.BigEndian.AppendUint16(out, pointer.Attributes)
	return out, nil
}

func decodeSegmentIndexEntry(data []byte) (segmentIndexEntry, error) {
	decoder := segmentIndexEntryDecoder{data: data}
	if err := decoder.expectMagic(); err != nil {
		return segmentIndexEntry{}, err
	}
	flags, err := decoder.readU16("flags")
	if err != nil {
		return segmentIndexEntry{}, err
	}
	topic, err := decoder.readTopic()
	if err != nil {
		return segmentIndexEntry{}, err
	}
	pointer, err := decoder.readPointerFields(topic)
	if err != nil {
		return segmentIndexEntry{}, err
	}
	if trailing := decoder.remaining(); trailing != 0 {
		return segmentIndexEntry{}, E(CodeCodec, "segment index entry has %d trailing bytes", trailing)
	}
	return segmentIndexEntry{Pointer: pointer, Deleted: (flags & segmentIndexEntryDeleteFlag) != 0}, nil
}

func encodeSegmentIndexEntries(entries []segmentIndexEntry) ([]byte, error) {
	out := make([]byte, 0, len(entries)*64)
	for _, entry := range entries {
		payload, err := encodeSegmentIndexEntry(entry)
		if err != nil {
			return nil, err
		}
		if len(payload) > math.MaxUint32 {
			return nil, E(CodeCodec, "segment index entry length %d exceeds uint32", len(payload))
		}
		var rawLength [8]byte
		binary.BigEndian.PutUint64(rawLength[:], uint64(len(payload)))
		out = append(out, rawLength[4:]...)
		out = append(out, payload...)
	}
	return out, nil
}

func readSegmentIndexEntries(reader io.Reader) ([]segmentIndexEntry, error) {
	entries := make([]segmentIndexEntry, 0)
	for {
		entry, err := readSegmentIndexEntry(reader)
		if errors.Is(err, io.EOF) {
			return entries, nil
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
}

func readSegmentIndexEntry(reader io.Reader) (segmentIndexEntry, error) {
	var rawLength [segmentIndexEntryHeaderBytes]byte
	if _, err := io.ReadFull(reader, rawLength[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return segmentIndexEntry{}, io.EOF
		}
		return segmentIndexEntry{}, wrapExternal(err, "read segment index entry length")
	}
	length := binary.BigEndian.Uint32(rawLength[:])
	if length == 0 {
		return segmentIndexEntry{}, E(CodeCodec, "empty segment index entry")
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return segmentIndexEntry{}, wrapExternal(err, "read segment index entry")
	}
	return decodeSegmentIndexEntry(payload)
}

func segmentIndexEntryEncodedLen(pointer segmentRecordPointer) int {
	return 56 + len(pointer.Topic)
}

type segmentIndexEntryDecoder struct {
	data   []byte
	cursor int
}

func (d *segmentIndexEntryDecoder) expectMagic() error {
	magic, err := d.readU32("magic")
	if err != nil {
		return err
	}
	if magic != segmentIndexEntryMagic {
		return E(CodeCodec, "invalid segment index entry magic %x", magic)
	}
	return nil
}

func (d *segmentIndexEntryDecoder) readTopic() (string, error) {
	topicLen, err := d.readU32("topic length")
	if err != nil {
		return "", err
	}
	if strconv.IntSize == 32 && topicLen > math.MaxInt32 {
		return "", E(CodeCodec, "segment index topic length %d exceeds int", topicLen)
	}
	return d.readString(int(topicLen), "topic")
}

func (d *segmentIndexEntryDecoder) readPointerFields(topic string) (segmentRecordPointer, error) {
	partition, err := d.readU32("partition")
	if err != nil {
		return segmentRecordPointer{}, err
	}
	offset, err := d.readU64("offset")
	if err != nil {
		return segmentRecordPointer{}, err
	}
	return d.readPointerTail(topic, partition, offset)
}

func (d *segmentIndexEntryDecoder) readPointerTail(
	topic string,
	partition uint32,
	offset uint64,
) (segmentRecordPointer, error) {
	segmentID, err := d.readU64("segment id")
	if err != nil {
		return segmentRecordPointer{}, err
	}
	position, err := d.readI64("position")
	if err != nil {
		return segmentRecordPointer{}, err
	}
	length, err := d.readInt("length")
	if err != nil {
		return segmentRecordPointer{}, err
	}
	timestampMS, err := d.readU64("timestamp")
	if err != nil {
		return segmentRecordPointer{}, err
	}
	attributes, err := d.readU16("attributes")
	if err != nil {
		return segmentRecordPointer{}, err
	}
	return segmentRecordPointer{
		Topic:       topic,
		Partition:   partition,
		Offset:      offset,
		SegmentID:   segmentID,
		Position:    position,
		Length:      length,
		TimestampMS: timestampMS,
		Attributes:  attributes,
	}, nil
}

func (d *segmentIndexEntryDecoder) readU32(field string) (uint32, error) {
	raw, err := d.readFixed(4, field)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(raw), nil
}

func (d *segmentIndexEntryDecoder) readU64(field string) (uint64, error) {
	raw, err := d.readFixed(8, field)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(raw), nil
}

func (d *segmentIndexEntryDecoder) readI64(field string) (int64, error) {
	raw, err := d.readU64(field)
	if err != nil {
		return 0, err
	}
	if raw > math.MaxInt64 {
		return 0, E(CodeCodec, "segment pointer %s %d exceeds int64", field, raw)
	}
	return int64(raw), nil
}

func (d *segmentIndexEntryDecoder) readInt(field string) (int, error) {
	raw, err := d.readU64(field)
	if err != nil {
		return 0, err
	}
	if raw > math.MaxInt {
		return 0, E(CodeCodec, "segment pointer %s %d exceeds int", field, raw)
	}
	return int(raw), nil
}

func (d *segmentIndexEntryDecoder) readU16(field string) (uint16, error) {
	raw, err := d.readFixed(2, field)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(raw), nil
}

func (d *segmentIndexEntryDecoder) readString(length int, field string) (string, error) {
	raw, err := d.readFixed(length, field)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func (d *segmentIndexEntryDecoder) readFixed(length int, field string) ([]byte, error) {
	if length > d.remaining() {
		return nil, E(CodeCodec, "segment index %s length %d exceeds remaining value size %d", field, length, d.remaining())
	}
	value := d.data[d.cursor : d.cursor+length]
	d.cursor += length
	return value, nil
}

func (d *segmentIndexEntryDecoder) remaining() int {
	return len(d.data) - d.cursor
}
