package store

import (
	"encoding/base64"
	"path/filepath"
	"strconv"
	"strings"
)

func (s *StorxLogStore) partitionDir(tp TopicPartition) string {
	return filepath.Join(s.segmentsDir, partitionRelativeDir(tp))
}

func (s *StorxLogStore) segmentRelativePath(tp TopicPartition, segmentID uint64) string {
	return filepath.Join(partitionRelativeDir(tp), fmtOffset(segmentID)+".seg")
}

func partitionRelativeDir(tp TopicPartition) string {
	return filepath.Join(segmentTopicDir(tp.Topic), strconv.FormatUint(uint64(tp.Partition), 10))
}

func segmentTopicDir(topic string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(topic))
}

func recordPrefix(tp TopicPartition) string {
	return tp.Topic + "\x00" + strconv.FormatUint(uint64(tp.Partition), 10) + "\x00"
}

func recordKey(tp TopicPartition, offset uint64) string {
	return recordPrefix(tp) + fmtOffset(offset)
}

func parseRecordKey(key string) (TopicPartition, error) {
	parts := strings.Split(key, "\x00")
	if len(parts) != 3 {
		return TopicPartition{}, E(CodeCodec, "invalid record key %q", key)
	}
	partition, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return TopicPartition{}, E(CodeCodec, "invalid partition in record key %q: %v", key, err)
	}
	return NewTopicPartition(parts[0], uint32(partition)), nil
}

func nextOffsetKey(tp TopicPartition) string {
	return tp.Topic + "\x00" + strconv.FormatUint(uint64(tp.Partition), 10)
}

func parseNextOffsetKey(key string) (TopicPartition, error) {
	parts := strings.Split(key, "\x00")
	if len(parts) != 2 {
		return TopicPartition{}, E(CodeCodec, "invalid next offset key %q", key)
	}
	partition, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return TopicPartition{}, E(CodeCodec, "invalid partition in next offset key %q: %v", key, err)
	}
	return NewTopicPartition(parts[0], uint32(partition)), nil
}

func fmtOffset(offset uint64) string {
	raw := strconv.FormatUint(offset, 10)
	if len(raw) >= 20 {
		return raw
	}
	return strings.Repeat("0", 20-len(raw)) + raw
}
