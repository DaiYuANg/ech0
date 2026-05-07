package store

import (
	"fmt"
	"strconv"
	"strings"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
)

type Snapshot struct {
	Topics      []TopicConfig                           `json:"topics"`
	Records     collectionmapping.Map[string, []Record] `json:"records"`
	Offsets     collectionmapping.Map[string, uint64]   `json:"offsets"`
	Members     []ConsumerGroupMember                   `json:"members"`
	Assignments []ConsumerGroupAssignment               `json:"assignments"`
	BrokerState *BrokerState                            `json:"broker_state,omitempty"`
}

func (s *MemoryStore) Snapshot() (Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := Snapshot{
		Topics:      make([]TopicConfig, 0, s.topics.Len()),
		Records:     *collectionmapping.NewMapWithCapacity[string, []Record](s.records.Len()),
		Offsets:     *collectionmapping.NewMapWithCapacity[string, uint64](s.offsets.Len()),
		Members:     make([]ConsumerGroupMember, 0, s.members.Len()),
		Assignments: make([]ConsumerGroupAssignment, 0, s.assignments.Len()),
	}
	topics := s.topics.Values()
	for i := range topics {
		topic := topics[i]
		snap.Topics = append(snap.Topics, cloneTopic(topic))
	}
	s.records.Range(func(tp TopicPartition, records []Record) bool {
		copied := make([]Record, 0, len(records))
		for _, record := range records {
			copied = append(copied, cloneRecord(record))
		}
		snap.Records.Set(partitionKey(tp), copied)
		return true
	})
	s.offsets.Range(func(key string, value uint64) bool {
		snap.Offsets.Set(key, value)
		return true
	})
	s.members.Range(func(_ string, member ConsumerGroupMember) bool {
		member.Topics = sortedStrings(member.Topics)
		snap.Members = append(snap.Members, member)
		return true
	})
	s.assignments.Range(func(_ string, assignment ConsumerGroupAssignment) bool {
		assignment.Assignments = append([]GroupPartitionAssignment(nil), assignment.Assignments...)
		snap.Assignments = append(snap.Assignments, assignment)
		return true
	})
	if s.brokerState != nil {
		cp := *s.brokerState
		snap.BrokerState = &cp
	}
	return snap, nil
}

func (s *MemoryStore) Restore(snapshot Snapshot) error {
	state, err := buildMemoryRestoreState(snapshot)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics = state.topics
	s.topicNames = state.topicNames
	s.records = state.records
	s.offsets = state.offsets
	s.members = state.members
	s.assignments = state.assignments
	s.brokerState = state.brokerState
	return nil
}

type memoryRestoreState struct {
	topics      *collectionmapping.OrderedMap[string, TopicConfig]
	topicNames  *collectionset.Set[string]
	records     *collectionmapping.Map[TopicPartition, []Record]
	offsets     *collectionmapping.Map[string, uint64]
	members     *collectionmapping.Map[string, ConsumerGroupMember]
	assignments *collectionmapping.Map[string, ConsumerGroupAssignment]
	brokerState *BrokerState
}

func buildMemoryRestoreState(snapshot Snapshot) (memoryRestoreState, error) {
	topics := collectionmapping.NewOrderedMap[string, TopicConfig]()
	topicNames := collectionset.NewSet[string]()
	records := collectionmapping.NewMap[TopicPartition, []Record]()
	restoreMemoryTopics(snapshot.Topics, topics, topicNames, records)
	if err := restoreMemoryRecords(snapshot.Records, records); err != nil {
		return memoryRestoreState{}, err
	}
	return memoryRestoreState{
		topics:      topics,
		topicNames:  topicNames,
		records:     records,
		offsets:     restoreMemoryOffsets(snapshot.Offsets),
		members:     restoreMemoryMembers(snapshot.Members),
		assignments: restoreMemoryAssignments(snapshot.Assignments),
		brokerState: cloneBrokerState(snapshot.BrokerState),
	}, nil
}

func restoreMemoryTopics(topics []TopicConfig, out *collectionmapping.OrderedMap[string, TopicConfig], names *collectionset.Set[string], records *collectionmapping.Map[TopicPartition, []Record]) {
	for i := range topics {
		topic := topics[i]
		normalizeTopic(&topic)
		out.Set(topic.Name, cloneTopic(topic))
		names.Add(topic.Name)
		for partition := range topic.Partitions {
			records.Set(NewTopicPartition(topic.Name, partition), nil)
		}
	}
}

func restoreMemoryRecords(snapshotRecords collectionmapping.Map[string, []Record], records *collectionmapping.Map[TopicPartition, []Record]) error {
	var resultErr error
	snapshotRecords.Range(func(key string, topicRecords []Record) bool {
		tp, err := parsePartitionKey(key)
		if err != nil {
			resultErr = err
			return false
		}
		records.Set(tp, cloneRecords(topicRecords))
		return true
	})
	return resultErr
}

func restoreMemoryOffsets(snapshotOffsets collectionmapping.Map[string, uint64]) *collectionmapping.Map[string, uint64] {
	offsets := collectionmapping.NewMapWithCapacity[string, uint64](snapshotOffsets.Len())
	snapshotOffsets.Range(func(key string, value uint64) bool {
		offsets.Set(key, value)
		return true
	})
	return offsets
}

func restoreMemoryMembers(snapshotMembers []ConsumerGroupMember) *collectionmapping.Map[string, ConsumerGroupMember] {
	members := collectionmapping.NewMapWithCapacity[string, ConsumerGroupMember](len(snapshotMembers))
	for _, member := range snapshotMembers {
		member.Topics = sortedStrings(member.Topics)
		members.Set(groupMemberKey(member.Group, member.MemberID), member)
	}
	return members
}

func restoreMemoryAssignments(snapshotAssignments []ConsumerGroupAssignment) *collectionmapping.Map[string, ConsumerGroupAssignment] {
	assignments := collectionmapping.NewMapWithCapacity[string, ConsumerGroupAssignment](len(snapshotAssignments))
	for _, assignment := range snapshotAssignments {
		assignment.Assignments = append([]GroupPartitionAssignment(nil), assignment.Assignments...)
		assignments.Set(assignment.Group, assignment)
	}
	return assignments
}

func cloneRecords(records []Record) []Record {
	copied := make([]Record, 0, len(records))
	for _, record := range records {
		copied = append(copied, cloneRecord(record))
	}
	return copied
}

func cloneBrokerState(state *BrokerState) *BrokerState {
	if state == nil {
		return nil
	}
	cp := *state
	return &cp
}

func partitionKey(tp TopicPartition) string {
	return fmt.Sprintf("%s\x00%d", tp.Topic, tp.Partition)
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
