package store

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func (s *StorxLogStore) rebuildMissingSegmentIndexes(
	loaded *collectionmapping.Map[TopicPartition, *collectionmapping.Map[uint64, segmentRecordPointer]],
	nextOffsets *collectionmapping.Map[TopicPartition, uint64],
) error {
	err := filepath.WalkDir(s.segmentsDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != segmentFileExtension {
			return nil
		}
		relativePath, relErr := filepath.Rel(s.segmentsDir, path)
		if relErr != nil {
			return wrapExternal(relErr, "resolve segment path")
		}
		if _, statErr := statSegmentIndex(s.segmentsDir, segmentIndexRelativePath(relativePath)); statErr == nil {
			return nil
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return wrapExternal(statErr, "stat segment index")
		}
		return s.rebuildMissingSegmentIndex(relativePath, loaded, nextOffsets)
	})
	return wrapExternal(err, "rebuild missing segment indexes")
}

func (s *StorxLogStore) rebuildMissingSegmentIndex(
	relativePath string,
	loaded *collectionmapping.Map[TopicPartition, *collectionmapping.Map[uint64, segmentRecordPointer]],
	nextOffsets *collectionmapping.Map[TopicPartition, uint64],
) error {
	tp, segmentID, err := parseSegmentRelativePath(relativePath)
	if err != nil {
		return err
	}
	if !s.segmentPartitionExists(tp) {
		return nil
	}
	data, err := readSegmentFile(s.segmentsDir, relativePath)
	if err != nil {
		return err
	}
	pointers, err := rebuildSegmentPointers(tp, segmentID, data)
	if err != nil {
		return err
	}
	if len(pointers) == 0 {
		return nil
	}
	indexRelativePath := segmentIndexRelativePath(relativePath)
	if err := writeRebuiltSegmentIndex(s.segmentsDir, indexRelativePath, pointers); err != nil {
		return err
	}
	for _, pointer := range pointers {
		applyLoadedSegmentIndexEntry(loaded, nextOffsets, segmentIndexEntry{Pointer: pointer})
	}
	return nil
}

func statSegmentIndex(rootDir, indexRelativePath string) (os.FileInfo, error) {
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		return nil, wrapExternal(err, "open segment index root")
	}
	info, err := root.Stat(indexRelativePath)
	closeErr := root.Close()
	if err != nil {
		return nil, errors.Join(err, wrapExternal(closeErr, "close segment index root"))
	}
	return info, wrapExternal(closeErr, "close segment index root")
}

func parseSegmentRelativePath(relativePath string) (TopicPartition, uint64, error) {
	if filepath.Ext(relativePath) != segmentFileExtension {
		return TopicPartition{}, 0, E(CodeCodec, "invalid segment file extension: %s", relativePath)
	}
	partitionDir := filepath.Base(filepath.Dir(relativePath))
	topicDir := filepath.Base(filepath.Dir(filepath.Dir(relativePath)))
	topicBytes, err := base64.RawURLEncoding.DecodeString(topicDir)
	if err != nil {
		return TopicPartition{}, 0, E(CodeCodec, "decode segment topic dir %s: %v", topicDir, err)
	}
	var partition uint32
	if _, scanErr := fmt.Sscan(partitionDir, &partition); scanErr != nil {
		return TopicPartition{}, 0, E(CodeCodec, "decode segment partition %s: %v", partitionDir, scanErr)
	}
	segmentName := strings.TrimSuffix(filepath.Base(relativePath), segmentFileExtension)
	segmentID, err := strconv.ParseUint(segmentName, 10, 64)
	if err != nil {
		return TopicPartition{}, 0, E(CodeCodec, "decode segment id %s: %v", segmentName, err)
	}
	return NewTopicPartition(string(topicBytes), partition), segmentID, nil
}

func (s *StorxLogStore) segmentPartitionExists(tp TopicPartition) bool {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	topic, ok := s.topics.Get(tp.Topic)
	return ok && tp.Partition < topic.Partitions
}

func readSegmentFile(rootDir, relativePath string) ([]byte, error) {
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		return nil, wrapExternal(err, "open segment root")
	}
	file, err := root.Open(relativePath)
	closeRootErr := root.Close()
	if err != nil {
		return nil, errors.Join(wrapExternal(err, "open segment file"), wrapExternal(closeRootErr, "close segment root"))
	}
	data, readErr := io.ReadAll(file)
	closeErr := file.Close()
	return data, errors.Join(wrapExternal(readErr, "read segment file"), wrapExternal(closeErr, "close segment file"), wrapExternal(closeRootErr, "close segment root"))
}

func rebuildSegmentPointers(tp TopicPartition, segmentID uint64, data []byte) ([]segmentRecordPointer, error) {
	pointers := collectionlist.NewListWithCapacity[segmentRecordPointer](0)
	position := 0
	for position < len(data) {
		frame, nextPosition, err := nextSelfDescribingSegmentFrame(data, position)
		if err != nil {
			return nil, err
		}
		framePosition, err := nonNegativeIntToInt64(position, "segment frame position")
		if err != nil {
			return nil, err
		}
		records, err := decodeSegmentFrameRecords(frame)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			pointers.Add(segmentRecordPointer{
				Topic:       tp.Topic,
				Partition:   tp.Partition,
				Offset:      record.Offset,
				SegmentID:   segmentID,
				Position:    framePosition,
				Length:      len(frame),
				TimestampMS: record.TimestampMS,
				Attributes:  record.Attributes,
			})
		}
		position = nextPosition
	}
	return pointers.Values(), nil
}

func nextSelfDescribingSegmentFrame(data []byte, position int) ([]byte, int, error) {
	if len(data)-position < segmentFrameHeader {
		return nil, 0, E(CodeCodec, "truncated segment frame header at position %d", position)
	}
	magic := binary.BigEndian.Uint32(data[position : position+4])
	if !validSegmentFrameMagic(magic) {
		return nil, 0, E(CodeCodec, "invalid segment frame magic %x at position %d", magic, position)
	}
	if !sizedSegmentFrameMagic(magic) {
		return nil, 0, E(CodeCodec, "missing segment index for legacy unsized segment frame at position %d", position)
	}
	bodyLen := binary.BigEndian.Uint32(data[position+8 : position+12])
	bodyLength, err := segmentU32ToInt(bodyLen, "segment frame body length")
	if err != nil {
		return nil, 0, err
	}
	length := segmentFrameHeader + bodyLength
	if length > len(data)-position {
		return nil, 0, E(CodeCodec, "truncated segment frame at position %d: length=%d remaining=%d", position, length, len(data)-position)
	}
	nextPosition := position + length
	return data[position:nextPosition], nextPosition, nil
}

func nonNegativeIntToInt64(value int, name string) (int64, error) {
	if value < 0 {
		return 0, E(CodeCodec, "%s is negative: %d", name, value)
	}
	out, err := strconv.ParseInt(strconv.Itoa(value), 10, 64)
	if err != nil {
		return 0, E(CodeCodec, "invalid %s %d: %v", name, value, err)
	}
	return out, nil
}

func writeRebuiltSegmentIndex(rootDir, indexRelativePath string, pointers []segmentRecordPointer) error {
	entries := collectionlist.NewListWithCapacity[segmentIndexEntry](len(pointers))
	for _, pointer := range pointers {
		entries.Add(segmentIndexEntry{Pointer: pointer})
	}
	payload, err := encodeSegmentIndexEntries(entries.Values())
	if err != nil {
		return err
	}
	indexDir := filepath.Join(rootDir, filepath.Dir(indexRelativePath))
	if mkdirErr := os.MkdirAll(indexDir, 0o750); mkdirErr != nil {
		return wrapExternal(mkdirErr, "create rebuilt segment index directory")
	}
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		return wrapExternal(err, "open segment index root")
	}
	file, err := root.OpenFile(indexRelativePath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	closeRootErr := root.Close()
	if errors.Is(err, os.ErrExist) {
		return wrapExternal(closeRootErr, "close segment index root")
	}
	if err != nil {
		return errors.Join(wrapExternal(err, "create rebuilt segment index"), wrapExternal(closeRootErr, "close segment index root"))
	}
	writeErr := writeAndSyncRebuiltSegmentIndex(file, payload)
	closeErr := file.Close()
	return errors.Join(writeErr, wrapExternal(closeErr, "close rebuilt segment index"), wrapExternal(closeRootErr, "close segment index root"))
}

func writeAndSyncRebuiltSegmentIndex(file *os.File, payload []byte) error {
	written, err := file.Write(payload)
	if err != nil {
		return wrapExternal(err, "write rebuilt segment index")
	}
	if written != len(payload) {
		return wrapExternal(io.ErrShortWrite, "write rebuilt segment index")
	}
	return wrapExternal(file.Sync(), "sync rebuilt segment index")
}
