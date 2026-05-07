package store

import (
	"fmt"
	"strconv"
	"strings"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
)

type Snapshot struct {
	Topics      []TopicConfig             `json:"topics"`
	Records     map[string][]Record       `json:"records"`
	Offsets     map[string]uint64         `json:"offsets"`
	Members     []ConsumerGroupMember     `json:"members"`
	Assignments []ConsumerGroupAssignment `json:"assignments"`
	BrokerState *BrokerState              `json:"broker_state,omitempty"`
}

func (s *MemoryStore) Snapshot() (Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := Snapshot{
		Topics:      make([]TopicConfig, 0, s.topics.Len()),
		Records:     make(map[string][]Record, len(s.records)),
		Offsets:     make(map[string]uint64, len(s.offsets)),
		Members:     make([]ConsumerGroupMember, 0, len(s.members)),
		Assignments: make([]ConsumerGroupAssignment, 0, len(s.assignments)),
	}
	for _, topic := range s.topics.Values() {
		snap.Topics = append(snap.Topics, cloneTopic(topic))
	}
	for tp, records := range s.records {
		copied := make([]Record, 0, len(records))
		for _, record := range records {
			copied = append(copied, cloneRecord(record))
		}
		snap.Records[partitionKey(tp)] = copied
	}
	for key, value := range s.offsets {
		snap.Offsets[key] = value
	}
	for _, member := range s.members {
		member.Topics = append([]string(nil), member.Topics...)
		snap.Members = append(snap.Members, member)
	}
	for _, assignment := range s.assignments {
		assignment.Assignments = append([]GroupPartitionAssignment(nil), assignment.Assignments...)
		snap.Assignments = append(snap.Assignments, assignment)
	}
	if s.brokerState != nil {
		cp := *s.brokerState
		snap.BrokerState = &cp
	}
	return snap, nil
}

func (s *MemoryStore) Restore(snapshot Snapshot) error {
	topics := collectionmapping.NewOrderedMap[string, TopicConfig]()
	topicNames := collectionset.NewSet[string]()
	records := make(map[TopicPartition][]Record)
	for _, topic := range snapshot.Topics {
		normalizeTopic(&topic)
		topics.Set(topic.Name, cloneTopic(topic))
		topicNames.Add(topic.Name)
		for partition := uint32(0); partition < topic.Partitions; partition++ {
			records[NewTopicPartition(topic.Name, partition)] = nil
		}
	}
	for key, topicRecords := range snapshot.Records {
		tp, err := parsePartitionKey(key)
		if err != nil {
			return err
		}
		copied := make([]Record, 0, len(topicRecords))
		for _, record := range topicRecords {
			copied = append(copied, cloneRecord(record))
		}
		records[tp] = copied
	}
	members := make(map[string]ConsumerGroupMember, len(snapshot.Members))
	for _, member := range snapshot.Members {
		member.Topics = append([]string(nil), member.Topics...)
		members[groupMemberKey(member.Group, member.MemberID)] = member
	}
	assignments := make(map[string]ConsumerGroupAssignment, len(snapshot.Assignments))
	for _, assignment := range snapshot.Assignments {
		assignment.Assignments = append([]GroupPartitionAssignment(nil), assignment.Assignments...)
		assignments[assignment.Group] = assignment
	}
	offsets := make(map[string]uint64, len(snapshot.Offsets))
	for key, value := range snapshot.Offsets {
		offsets[key] = value
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics = topics
	s.topicNames = topicNames
	s.records = records
	s.offsets = offsets
	s.members = members
	s.assignments = assignments
	if snapshot.BrokerState != nil {
		cp := *snapshot.BrokerState
		s.brokerState = &cp
	} else {
		s.brokerState = nil
	}
	return nil
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
