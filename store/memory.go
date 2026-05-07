package store

import (
	"fmt"
	"sort"
	"sync"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
)

type MemoryStore struct {
	mu sync.RWMutex

	topics      *collectionmapping.OrderedMap[string, TopicConfig]
	topicNames  *collectionset.Set[string]
	records     map[TopicPartition][]Record
	offsets     map[string]uint64
	members     map[string]ConsumerGroupMember
	assignments map[string]ConsumerGroupAssignment
	brokerState *BrokerState
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		topics:      collectionmapping.NewOrderedMap[string, TopicConfig](),
		topicNames:  collectionset.NewSet[string](),
		records:     make(map[TopicPartition][]Record),
		offsets:     make(map[string]uint64),
		members:     make(map[string]ConsumerGroupMember),
		assignments: make(map[string]ConsumerGroupAssignment),
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
	for partition := uint32(0); partition < topic.Partitions; partition++ {
		tp := NewTopicPartition(topic.Name, partition)
		if _, ok := s.records[tp]; !ok {
			s.records[tp] = nil
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
	if uint32(len(appendRecord.Payload)) > topic.MaxMessageBytes {
		return Record{}, E(CodeInvalidArgument, "payload size %d exceeds max_message_bytes %d", len(appendRecord.Payload), topic.MaxMessageBytes)
	}

	existing := s.records[topicPartition]
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
	s.records[topicPartition] = append(existing, record)
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
		return nil, nil
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
	records := s.records[topicPartition]
	if offset >= uint64(len(records)) {
		return nil, nil
	}
	end := int(offset) + maxRecords
	if end > len(records) {
		end = len(records)
	}
	out := make([]Record, 0, end-int(offset))
	for _, record := range records[offset:end] {
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
	records := s.records[topicPartition]
	if len(records) == 0 {
		return nil, nil
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
	s.offsets[offsetKey(consumer, topicPartition)] = nextOffset
	return nil
}

func (s *MemoryStore) LoadConsumerOffset(consumer string, topicPartition TopicPartition) (*uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.offsets[offsetKey(consumer, topicPartition)]
	if !ok {
		return nil, nil
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
	for partition := uint32(0); partition < topic.Partitions; partition++ {
		tp := NewTopicPartition(topic.Name, partition)
		if _, ok := s.records[tp]; !ok {
			s.records[tp] = nil
		}
	}
	return nil
}

func (s *MemoryStore) LoadTopicConfig(topic string) (*TopicConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cfg, ok := s.topics.Get(topic)
	if !ok {
		return nil, nil
	}
	cfg = cloneTopic(cfg)
	return &cfg, nil
}

func (s *MemoryStore) ListTopics() ([]TopicConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	topics := s.topics.Values()
	out := make([]TopicConfig, 0, len(topics))
	for _, topic := range topics {
		out = append(out, cloneTopic(topic))
	}
	return out, nil
}

func (s *MemoryStore) SaveGroupMember(member ConsumerGroupMember) error {
	if member.Group == "" || member.MemberID == "" {
		return E(CodeInvalidArgument, "group and member_id are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	member.Topics = append([]string(nil), member.Topics...)
	sort.Strings(member.Topics)
	s.members[groupMemberKey(member.Group, member.MemberID)] = member
	return nil
}

func (s *MemoryStore) LoadGroupMember(group string, memberID string) (*ConsumerGroupMember, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	member, ok := s.members[groupMemberKey(group, memberID)]
	if !ok {
		return nil, nil
	}
	member.Topics = append([]string(nil), member.Topics...)
	return &member, nil
}

func (s *MemoryStore) ListGroupMembers(group string) ([]ConsumerGroupMember, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ConsumerGroupMember, 0)
	for _, member := range s.members {
		if member.Group != group {
			continue
		}
		member.Topics = append([]string(nil), member.Topics...)
		out = append(out, member)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MemberID < out[j].MemberID })
	return out, nil
}

func (s *MemoryStore) DeleteGroupMember(group string, memberID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.members, groupMemberKey(group, memberID))
	return nil
}

func (s *MemoryStore) DeleteExpiredGroupMembers(nowMS uint64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted := 0
	for key, member := range s.members {
		if member.ExpiredAt(nowMS) {
			delete(s.members, key)
			deleted++
		}
	}
	return deleted, nil
}

func (s *MemoryStore) SaveGroupAssignment(assignment ConsumerGroupAssignment) error {
	if assignment.Group == "" {
		return E(CodeInvalidArgument, "group is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	assignment.Assignments = append([]GroupPartitionAssignment(nil), assignment.Assignments...)
	s.assignments[assignment.Group] = assignment
	return nil
}

func (s *MemoryStore) LoadGroupAssignment(group string) (*ConsumerGroupAssignment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	assignment, ok := s.assignments[group]
	if !ok {
		return nil, nil
	}
	assignment.Assignments = append([]GroupPartitionAssignment(nil), assignment.Assignments...)
	return &assignment, nil
}

func (s *MemoryStore) ListGroupAssignments() ([]ConsumerGroupAssignment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ConsumerGroupAssignment, 0, len(s.assignments))
	for _, assignment := range s.assignments {
		assignment.Assignments = append([]GroupPartitionAssignment(nil), assignment.Assignments...)
		out = append(out, assignment)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Group < out[j].Group })
	return out, nil
}

func (s *MemoryStore) SaveBrokerState(state BrokerState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := state
	s.brokerState = &cp
	return nil
}

func (s *MemoryStore) LoadBrokerState() (*BrokerState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.brokerState == nil {
		return nil, nil
	}
	cp := *s.brokerState
	return &cp, nil
}

func offsetKey(consumer string, tp TopicPartition) string {
	return fmt.Sprintf("%s\x00%s\x00%d", consumer, tp.Topic, tp.Partition)
}

func groupMemberKey(group string, memberID string) string {
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
