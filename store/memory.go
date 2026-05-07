package store

import (
	"fmt"
	"sync"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
)

type MemoryStore struct {
	mu sync.RWMutex

	topics      *collectionmapping.OrderedMap[string, TopicConfig]
	topicNames  *collectionset.Set[string]
	records     *collectionmapping.Map[TopicPartition, []Record]
	offsets     *collectionmapping.Map[string, uint64]
	members     *collectionmapping.Map[string, ConsumerGroupMember]
	assignments *collectionmapping.Map[string, ConsumerGroupAssignment]
	brokerState *BrokerState
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		topics:      collectionmapping.NewOrderedMap[string, TopicConfig](),
		topicNames:  collectionset.NewSet[string](),
		records:     collectionmapping.NewMap[TopicPartition, []Record](),
		offsets:     collectionmapping.NewMap[string, uint64](),
		members:     collectionmapping.NewMap[string, ConsumerGroupMember](),
		assignments: collectionmapping.NewMap[string, ConsumerGroupAssignment](),
	}
}

func (s *MemoryStore) CreateTopic(topic TopicConfig) error {
	if topic.Name == "" {
		return E(CodeInvalidArgument, "topic name is required")
	}
	if topic.Partitions == 0 {
		return E(CodeInvalidArgument, "topic %s must have at least one partition", topic.Name)
	}
	normalizeTopic(&topic)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.topicNames.Contains(topic.Name) {
		return E(CodeTopicExists, "topic %s already exists", topic.Name)
	}
	s.topicNames.Add(topic.Name)
	s.topics.Set(topic.Name, cloneTopic(topic))
	for partition := range topic.Partitions {
		tp := NewTopicPartition(topic.Name, partition)
		if _, ok := s.records.Get(tp); !ok {
			s.records.Set(tp, nil)
		}
	}
	return nil
}

func (s *MemoryStore) TopicExists(topic string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.topicNames.Contains(topic), nil
}

func (s *MemoryStore) Append(topicPartition TopicPartition, payload []byte) (Record, error) {
	return s.AppendRecord(topicPartition, NewRecordAppend(payload))
}

func (s *MemoryStore) AppendRecord(topicPartition TopicPartition, appendRecord RecordAppend) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	topic, ok := s.topics.Get(topicPartition.Topic)
	if !ok {
		return Record{}, E(CodeTopicNotFound, "topic %s not found", topicPartition.Topic)
	}
	if topicPartition.Partition >= topic.Partitions {
		return Record{}, E(CodePartitionNotFound, "partition %s/%d not found", topicPartition.Topic, topicPartition.Partition)
	}
	if len(appendRecord.Payload) > int(topic.MaxMessageBytes) {
		return Record{}, E(CodeInvalidArgument, "payload size %d exceeds max_message_bytes %d", len(appendRecord.Payload), topic.MaxMessageBytes)
	}

	existing := s.records.GetOrDefault(topicPartition, nil)
	offset := uint64(len(existing))
	timestamp := NowMS()
	if appendRecord.TimestampMS != nil {
		timestamp = *appendRecord.TimestampMS
	}
	record := Record{
		Offset:      offset,
		TimestampMS: timestamp,
		Key:         cloneBytes(appendRecord.Key),
		Headers:     cloneHeaders(appendRecord.Headers),
		Attributes:  appendRecord.Attributes,
		Payload:     cloneBytes(appendRecord.Payload),
	}
	s.records.Set(topicPartition, append(existing, record))
	return cloneRecord(record), nil
}

func (s *MemoryStore) AppendRecordsBatch(topicPartition TopicPartition, records []RecordAppend) ([]Record, error) {
	out := make([]Record, 0, len(records))
	for _, record := range records {
		appended, err := s.AppendRecord(topicPartition, record)
		if err != nil {
			return nil, err
		}
		out = append(out, appended)
	}
	return out, nil
}

func (s *MemoryStore) ReadFrom(topicPartition TopicPartition, offset uint64, maxRecords int) ([]Record, error) {
	if maxRecords <= 0 {
		return []Record{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	topic, ok := s.topics.Get(topicPartition.Topic)
	if !ok {
		return nil, E(CodeTopicNotFound, "topic %s not found", topicPartition.Topic)
	}
	if topicPartition.Partition >= topic.Partitions {
		return nil, E(CodePartitionNotFound, "partition %s/%d not found", topicPartition.Topic, topicPartition.Partition)
	}
	records := s.records.GetOrDefault(topicPartition, nil)
	if offset >= uint64(len(records)) {
		return []Record{}, nil
	}
	out := make([]Record, 0, min(maxRecords, len(records)))
	for i, copied := offset, 0; i < uint64(len(records)) && copied < maxRecords; i, copied = i+1, copied+1 {
		record := records[i]
		out = append(out, cloneRecord(record))
	}
	return out, nil
}

func (s *MemoryStore) LastOffset(topicPartition TopicPartition) (*uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	topic, ok := s.topics.Get(topicPartition.Topic)
	if !ok {
		return nil, E(CodeTopicNotFound, "topic %s not found", topicPartition.Topic)
	}
	if topicPartition.Partition >= topic.Partitions {
		return nil, E(CodePartitionNotFound, "partition %s/%d not found", topicPartition.Topic, topicPartition.Partition)
	}
	records := s.records.GetOrDefault(topicPartition, nil)
	if len(records) == 0 {
		var absent *uint64
		return absent, nil
	}
	last := records[len(records)-1].Offset
	return &last, nil
}

func (s *MemoryStore) SaveConsumerOffset(consumer string, topicPartition TopicPartition, nextOffset uint64) error {
	if consumer == "" {
		return E(CodeInvalidArgument, "consumer is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsets.Set(offsetKey(consumer, topicPartition), nextOffset)
	return nil
}

func (s *MemoryStore) LoadConsumerOffset(consumer string, topicPartition TopicPartition) (*uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.offsets.Get(offsetKey(consumer, topicPartition))
	if !ok {
		var absent *uint64
		return absent, nil
	}
	return &value, nil
}

func (s *MemoryStore) SaveTopicConfig(topic TopicConfig) error {
	if topic.Name == "" {
		return E(CodeInvalidArgument, "topic name is required")
	}
	normalizeTopic(&topic)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topicNames.Add(topic.Name)
	s.topics.Set(topic.Name, cloneTopic(topic))
	for partition := range topic.Partitions {
		tp := NewTopicPartition(topic.Name, partition)
		if _, ok := s.records.Get(tp); !ok {
			s.records.Set(tp, nil)
		}
	}
	return nil
}

func (s *MemoryStore) LoadTopicConfig(topic string) (*TopicConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cfg, ok := s.topics.Get(topic)
	if !ok {
		var absent *TopicConfig
		return absent, nil
	}
	cfg = cloneTopic(cfg)
	return &cfg, nil
}

func (s *MemoryStore) ListTopics() ([]TopicConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	topics := s.topics.Values()
	out := make([]TopicConfig, 0, len(topics))
	for i := range topics {
		topic := topics[i]
		out = append(out, cloneTopic(topic))
	}
	return out, nil
}

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
