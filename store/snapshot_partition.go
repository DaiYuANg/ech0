package store

import (
	"fmt"
	"strconv"
	"strings"
)

func partitionKey(tp TopicPartition) string {
	return fmt.Sprintf("%s\x00%d", tp.Topic, tp.Partition)
}

// SnapshotPartitionKey returns the stable key used by Snapshot record maps.
func SnapshotPartitionKey(tp TopicPartition) string {
	return partitionKey(tp)
}

// ParseSnapshotPartitionKey parses a key produced by SnapshotPartitionKey.
func ParseSnapshotPartitionKey(key string) (TopicPartition, error) {
	return parsePartitionKey(key)
}

func parsePartitionKey(key string) (TopicPartition, error) {
	parts := strings.Split(key, "\x00")
	if len(parts) != 2 {
		return TopicPartition{}, E(CodeCodec, "invalid partition key %q", key)
	}
	partition, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return TopicPartition{}, E(CodeCodec, "invalid partition in key %q: %v", key, err)
	}
	return NewTopicPartition(parts[0], uint32(partition)), nil
}
