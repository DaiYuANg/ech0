package store

import (
	"encoding/base64"
	"path/filepath"
	"strconv"
	"strings"
)

const segmentFileExtension = ".seg"

func (s *StorxLogStore) partitionDir(tp TopicPartition) string {
	return filepath.Join(s.segmentsDir, partitionRelativeDir(tp))
}

func (s *StorxLogStore) segmentRelativePath(tp TopicPartition, segmentID uint64) string {
	return filepath.Join(partitionRelativeDir(tp), fmtOffset(segmentID)+segmentFileExtension)
}

func partitionRelativeDir(tp TopicPartition) string {
	return filepath.Join(segmentTopicDir(tp.Topic), strconv.FormatUint(uint64(tp.Partition), 10))
}

func segmentTopicDir(topic string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(topic))
}

func fmtOffset(offset uint64) string {
	raw := strconv.FormatUint(offset, 10)
	if len(raw) >= 20 {
		return raw
	}
	return strings.Repeat("0", 20-len(raw)) + raw
}
