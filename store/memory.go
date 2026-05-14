package store

import (
	"strconv"
	"sync"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
)

type MemoryStore struct {
	mu sync.RWMutex

	topics          *collectionmapping.OrderedMap[string, TopicConfig]
	topicNames      *collectionset.Set[string]
	records         *collectionmapping.Map[TopicPartition, []Record]
	nextOffsets     *collectionmapping.Map[TopicPartition, uint64]
	offsets         *collectionmapping.Map[string, uint64]
	consumerPauses  *collectionmapping.Map[string, ConsumerPauseState]
	placements      *collectionmapping.Map[TopicPartition, ShardPlacement]
	members         *collectionmapping.Map[string, ConsumerGroupMember]
	assignments     *collectionmapping.Map[string, ConsumerGroupAssignment]
	transactions    *collectionmapping.Map[uint64, TransactionState]
	producerBatches *collectionmapping.Map[string, ProducerPublishedBatch]
	aclPolicies     *collectionmapping.Map[string, ACLPolicy]
	nextTxID        uint64
	brokerState     *BrokerState
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		topics:          collectionmapping.NewOrderedMap[string, TopicConfig](),
		topicNames:      collectionset.NewSet[string](),
		records:         collectionmapping.NewMap[TopicPartition, []Record](),
		nextOffsets:     collectionmapping.NewMap[TopicPartition, uint64](),
		offsets:         collectionmapping.NewMap[string, uint64](),
		consumerPauses:  collectionmapping.NewMap[string, ConsumerPauseState](),
		placements:      collectionmapping.NewMap[TopicPartition, ShardPlacement](),
		members:         collectionmapping.NewMap[string, ConsumerGroupMember](),
		assignments:     collectionmapping.NewMap[string, ConsumerGroupAssignment](),
		transactions:    collectionmapping.NewMap[uint64, TransactionState](),
		producerBatches: collectionmapping.NewMap[string, ProducerPublishedBatch](),
		aclPolicies:     collectionmapping.NewMap[string, ACLPolicy](),
		nextTxID:        1,
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
		if _, ok := s.nextOffsets.Get(tp); !ok {
			s.nextOffsets.Set(tp, 0)
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
	offset := s.nextOffsets.GetOrDefault(topicPartition, nextOffsetFromRecords(existing))
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
		Transaction: cloneTransactionRecordMetadata(appendRecord.Transaction),
		Payload:     cloneBytes(appendRecord.Payload),
	}
	s.records.Set(topicPartition, append(existing, record))
	s.nextOffsets.Set(topicPartition, offset+1)
	return cloneRecord(record), nil
}

func (s *MemoryStore) AppendRecordsBatch(topicPartition TopicPartition, records []RecordAppend) ([]Record, error) {
	out := collectionlist.NewListWithCapacity[Record](len(records))
	for _, record := range records {
		appended, err := s.AppendRecord(topicPartition, record)
		if err != nil {
			return nil, err
		}
		out.Add(appended)
	}
	return out.Values(), nil
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
	records := sortedRecords(s.records.GetOrDefault(topicPartition, nil))
	out := collectionlist.NewListWithCapacity[Record](min(maxRecords, len(records)))
	for _, record := range records {
		if record.Offset < offset {
			continue
		}
		out.Add(cloneRecord(record))
		if out.Len() >= maxRecords {
			break
		}
	}
	return out.Values(), nil
}

func (s *MemoryStore) ReadPage(topicPartition TopicPartition, cursor string, maxRecords int) (RecordPage, error) {
	offset, err := parseMemoryRecordPageCursor(cursor)
	if err != nil {
		return RecordPage{}, err
	}
	records, err := s.ReadFrom(topicPartition, offset, maxRecords)
	if err != nil {
		return RecordPage{}, err
	}
	page := RecordPage{Records: records}
	hasMore, nextCursor, err := s.memoryRecordPageNext(topicPartition, records, maxRecords)
	if err != nil {
		return RecordPage{}, err
	}
	page.HasMore = hasMore
	page.NextCursor = nextCursor
	return page, nil
}

func parseMemoryRecordPageCursor(cursor string) (uint64, error) {
	if cursor == "" {
		return 0, nil
	}
	offset, err := strconv.ParseUint(cursor, 10, 64)
	if err != nil {
		return 0, E(CodeInvalidArgument, "invalid record page cursor")
	}
	return offset, nil
}

func (s *MemoryStore) memoryRecordPageNext(topicPartition TopicPartition, records []Record, maxRecords int) (bool, string, error) {
	if len(records) != maxRecords || maxRecords <= 0 {
		return false, "", nil
	}
	next := records[len(records)-1].Offset + 1
	highWatermark, err := s.LastOffset(topicPartition)
	if err != nil {
		return false, "", err
	}
	if highWatermark == nil || next > *highWatermark {
		return false, "", nil
	}
	return true, strconv.FormatUint(next, 10), nil
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
		if _, ok := s.nextOffsets.Get(tp); !ok {
			s.nextOffsets.Set(tp, nextOffsetFromRecords(s.records.GetOrDefault(tp, nil)))
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
	out := collectionlist.NewListWithCapacity[TopicConfig](len(topics))
	for i := range topics {
		topic := topics[i]
		out.Add(cloneTopic(topic))
	}
	return out.Values(), nil
}
