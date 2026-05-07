package store

import "fmt"

func offsetKey(consumer string, tp TopicPartition) string {
	return fmt.Sprintf("%s\x00%s\x00%d", consumer, tp.Topic, tp.Partition)
}

func groupMemberKey(group, memberID string) string {
	return group + "\x00" + memberID
}

func normalizeTopic(topic *TopicConfig) {
	if topic.Partitions == 0 {
		topic.Partitions = 1
	}
	if topic.SegmentMaxBytes == 0 {
		topic.SegmentMaxBytes = 16 * 1024 * 1024
	}
	if topic.IndexIntervalBytes == 0 {
		topic.IndexIntervalBytes = 4 * 1024
	}
	if topic.RetentionMaxBytes == 0 {
		topic.RetentionMaxBytes = 256 * 1024 * 1024
	}
	if topic.CleanupPolicy == "" {
		topic.CleanupPolicy = TopicCleanupDelete
	}
	if topic.MaxMessageBytes == 0 {
		topic.MaxMessageBytes = 1024 * 1024
	}
	if topic.MaxBatchBytes == 0 {
		topic.MaxBatchBytes = 8 * 1024 * 1024
	}
	if topic.RetryPolicy.MaxAttempts == 0 {
		topic.RetryPolicy = DefaultTopicRetryPolicy()
	}
}

func cloneTopic(topic TopicConfig) TopicConfig {
	if topic.RetentionMS != nil {
		v := *topic.RetentionMS
		topic.RetentionMS = &v
	}
	if topic.DeadLetterTopic != nil {
		v := *topic.DeadLetterTopic
		topic.DeadLetterTopic = &v
	}
	if topic.CompactionTombstoneRetentionMS != nil {
		v := *topic.CompactionTombstoneRetentionMS
		topic.CompactionTombstoneRetentionMS = &v
	}
	return topic
}

func cloneRecord(record Record) Record {
	record.Key = cloneBytes(record.Key)
	record.Payload = cloneBytes(record.Payload)
	record.Headers = cloneHeaders(record.Headers)
	return record
}

func cloneHeaders(headers []RecordHeader) []RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	out := make([]RecordHeader, len(headers))
	for i, header := range headers {
		out[i] = RecordHeader{Key: header.Key, Value: cloneBytes(header.Value)}
	}
	return out
}

func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	return append([]byte(nil), in...)
}

func nextOffsetFromRecords(records []Record) uint64 {
	next := uint64(0)
	for _, record := range records {
		if record.Offset >= next {
			next = record.Offset + 1
		}
	}
	return next
}
