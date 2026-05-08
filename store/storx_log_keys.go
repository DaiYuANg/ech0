package store

import (
	"encoding/base64"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/arcgolabs/storx/keycodec"
)

type partitionIndexKey struct {
	Topic     string
	Partition uint64
}

type recordIndexKey struct {
	partitionIndexKey
	Offset uint64
}

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

func recordIndexPrefix(tp TopicPartition) ([]byte, error) {
	topic, err := keycodec.ComponentPrefix(keycodec.String(), tp.Topic)
	if err != nil {
		return nil, wrapExternal(err, "encode record topic prefix")
	}
	partition, err := keycodec.ComponentPrefix(keycodec.Uint64BE(), uint64(tp.Partition))
	if err != nil {
		return nil, wrapExternal(err, "encode record partition prefix")
	}
	return append(topic, partition...), nil
}

func recordKey(tp TopicPartition, offset uint64) recordIndexKey {
	return recordIndexKey{
		partitionIndexKey: nextOffsetKey(tp),
		Offset:            offset,
	}
}

func nextOffsetKey(tp TopicPartition) partitionIndexKey {
	return partitionIndexKey{
		Topic:     tp.Topic,
		Partition: uint64(tp.Partition),
	}
}

func (k partitionIndexKey) topicPartition() (TopicPartition, error) {
	partition, err := strconv.ParseUint(strconv.FormatUint(k.Partition, 10), 10, 32)
	if err != nil {
		return TopicPartition{}, E(CodeCodec, "invalid partition in index key %s/%d: %v", k.Topic, k.Partition, err)
	}
	return NewTopicPartition(k.Topic, uint32(partition)), nil
}

func (k recordIndexKey) topicPartition() (TopicPartition, error) {
	return k.partitionIndexKey.topicPartition()
}

func partitionIndexKeyCodec() keycodec.Codec[partitionIndexKey] {
	return keycodec.Composite(
		keycodec.Field[partitionIndexKey](keycodec.String(), func(key partitionIndexKey) string {
			return key.Topic
		}, func(target *partitionIndexKey, value string) {
			target.Topic = value
		}),
		keycodec.Field[partitionIndexKey](keycodec.Uint64BE(), func(key partitionIndexKey) uint64 {
			return key.Partition
		}, func(target *partitionIndexKey, value uint64) {
			target.Partition = value
		}),
	)
}

func recordIndexKeyCodec() keycodec.Codec[recordIndexKey] {
	return keycodec.Composite(
		keycodec.Field[recordIndexKey](keycodec.String(), func(key recordIndexKey) string {
			return key.Topic
		}, func(target *recordIndexKey, value string) {
			target.Topic = value
		}),
		keycodec.Field[recordIndexKey](keycodec.Uint64BE(), func(key recordIndexKey) uint64 {
			return key.Partition
		}, func(target *recordIndexKey, value uint64) {
			target.Partition = value
		}),
		keycodec.Field[recordIndexKey](keycodec.Uint64BE(), func(key recordIndexKey) uint64 {
			return key.Offset
		}, func(target *recordIndexKey, value uint64) {
			target.Offset = value
		}),
	)
}

func fmtOffset(offset uint64) string {
	raw := strconv.FormatUint(offset, 10)
	if len(raw) >= 20 {
		return raw
	}
	return strings.Repeat("0", 20-len(raw)) + raw
}
