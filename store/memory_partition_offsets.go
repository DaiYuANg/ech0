package store

func (s *MemoryStore) LastOffset(topicPartition TopicPartition) (*uint64, error) {
	offsets, err := s.PartitionOffsets(topicPartition)
	if err != nil {
		return nil, err
	}
	return offsets.HighWatermark, nil
}

func (s *MemoryStore) PartitionOffsets(topicPartition TopicPartition) (PartitionOffsetState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.validatePartitionLocked(topicPartition); err != nil {
		return PartitionOffsetState{}, err
	}
	records := sortedRecords(s.records.GetOrDefault(topicPartition, nil))
	nextOffset := s.nextOffsets.GetOrDefault(topicPartition, nextOffsetFromRecords(records))
	return partitionOffsetsFromRecordOffsets(topicPartition, nextOffset, recordOffsets(records)), nil
}

func (s *MemoryStore) validatePartitionLocked(topicPartition TopicPartition) error {
	topic, ok := s.topics.Get(topicPartition.Topic)
	if !ok {
		return E(CodeTopicNotFound, "topic %s not found", topicPartition.Topic)
	}
	if topicPartition.Partition >= topic.Partitions {
		return E(CodePartitionNotFound, "partition %s/%d not found", topicPartition.Topic, topicPartition.Partition)
	}
	return nil
}
