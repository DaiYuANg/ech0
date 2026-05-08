package store

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/arcgolabs/storx/badgerx"
)

func (s *StorxLogStore) nextOffset(topicPartition TopicPartition) (uint64, error) {
	offset, ok, err := s.nextOffsets.Get(context.Background(), nextOffsetKey(topicPartition))
	if err != nil {
		return 0, wrapExternal(err, "load next log offset")
	}
	if !ok {
		return 0, nil
	}
	return offset, nil
}

func (s *StorxLogStore) appendFrame(
	topic TopicConfig,
	topicPartition TopicPartition,
	offset uint64,
	frame []byte,
	record Record,
) (segmentRecordPointer, error) {
	segmentID, err := s.segmentIDForAppend(topic, topicPartition, offset, len(frame))
	if err != nil {
		return segmentRecordPointer{}, err
	}
	relativePath := s.segmentRelativePath(topicPartition, segmentID)
	if mkdirErr := os.MkdirAll(filepath.Join(s.segmentsDir, filepath.Dir(relativePath)), 0o750); mkdirErr != nil {
		return segmentRecordPointer{}, wrapExternal(mkdirErr, "create segment file directory")
	}
	position, err := appendSegmentFrame(s.segmentsDir, relativePath, frame)
	if err != nil {
		return segmentRecordPointer{}, err
	}
	return segmentRecordPointer{
		Topic:       topicPartition.Topic,
		Partition:   topicPartition.Partition,
		Offset:      record.Offset,
		SegmentID:   segmentID,
		Position:    position,
		Length:      len(frame),
		TimestampMS: record.TimestampMS,
		Attributes:  record.Attributes,
	}, nil
}

func appendSegmentFrame(rootDir, relativePath string, frame []byte) (int64, error) {
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		return 0, wrapExternal(err, "open segment root")
	}
	file, err := root.OpenFile(relativePath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return 0, errors.Join(wrapExternal(err, "open segment file"), wrapExternal(root.Close(), "close segment root"))
	}
	position, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, errors.Join(wrapExternal(err, "seek segment file"), closeSegmentFile(file, root))
	}
	if _, err := file.Write(frame); err != nil {
		return 0, errors.Join(wrapExternal(err, "write segment record"), closeSegmentFile(file, root))
	}
	if err := file.Sync(); err != nil {
		return 0, errors.Join(wrapExternal(err, "sync segment file"), closeSegmentFile(file, root))
	}
	if err := closeSegmentFile(file, root); err != nil {
		return 0, err
	}
	return position, nil
}

func closeSegmentFile(file *os.File, root *os.Root) error {
	return wrapExternal(errors.Join(file.Close(), root.Close()), "close segment file")
}

func statSegmentFile(rootDir, relativePath string) (os.FileInfo, error) {
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		return nil, wrapExternal(err, "open segment root")
	}
	info, err := root.Stat(relativePath)
	if err != nil {
		return nil, errors.Join(err, wrapExternal(root.Close(), "close segment root"))
	}
	if err := root.Close(); err != nil {
		return nil, wrapExternal(err, "close segment root")
	}
	return info, nil
}

func segmentHasCapacity(currentSize int64, frameSize int, maxBytes uint64) (bool, error) {
	used, err := nonNegativeInt64ToUint64(currentSize, "segment file size")
	if err != nil {
		return false, err
	}
	next, err := nonNegativeIntToUint64(frameSize, "segment frame size")
	if err != nil {
		return false, err
	}
	return used <= maxBytes && next <= maxBytes-used, nil
}

func nonNegativeInt64ToUint64(value int64, name string) (uint64, error) {
	if value < 0 {
		return 0, E(CodeCodec, "%s is negative: %d", name, value)
	}
	out, err := strconv.ParseUint(strconv.FormatInt(value, 10), 10, 64)
	if err != nil {
		return 0, E(CodeCodec, "invalid %s %d: %v", name, value, err)
	}
	return out, nil
}

func nonNegativeIntToUint64(value int, name string) (uint64, error) {
	if value < 0 {
		return 0, E(CodeCodec, "%s is negative: %d", name, value)
	}
	out, err := strconv.ParseUint(strconv.Itoa(value), 10, 64)
	if err != nil {
		return 0, E(CodeCodec, "invalid %s %d: %v", name, value, err)
	}
	return out, nil
}

func (s *StorxLogStore) segmentIDForAppend(
	topic TopicConfig,
	topicPartition TopicPartition,
	offset uint64,
	frameSize int,
) (uint64, error) {
	last, ok, err := s.lastRecordPointer(topicPartition)
	if err != nil {
		return 0, err
	}
	if !ok {
		return offset, nil
	}
	maxBytes := topic.SegmentMaxBytes
	if maxBytes == 0 {
		return last.SegmentID, nil
	}
	info, err := statSegmentFile(s.segmentsDir, s.segmentRelativePath(topicPartition, last.SegmentID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return offset, nil
		}
		return 0, wrapExternal(err, "stat segment file")
	}
	hasCapacity, err := segmentHasCapacity(info.Size(), frameSize, maxBytes)
	if err != nil {
		return 0, err
	}
	if hasCapacity {
		return last.SegmentID, nil
	}
	return offset, nil
}

func (s *StorxLogStore) lastRecordPointer(topicPartition TopicPartition) (segmentRecordPointer, bool, error) {
	prefix, err := recordIndexPrefix(topicPartition)
	if err != nil {
		return segmentRecordPointer{}, false, err
	}
	entries, err := s.records.List(
		context.Background(),
		badgerx.WithPrefix[recordIndexKey](prefix),
		badgerx.WithReverse[recordIndexKey](true),
		badgerx.WithLimit[recordIndexKey](1),
	)
	if err != nil {
		return segmentRecordPointer{}, false, wrapExternal(err, "load last segment record index")
	}
	if len(entries) == 0 {
		return segmentRecordPointer{}, false, nil
	}
	return entries[0].Value, true, nil
}
