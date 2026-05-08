package store

import (
	"fmt"
	"strconv"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
)

type Snapshot struct {
	Topics      collectionlist.List[TopicConfig]             `json:"topics"`
	Records     collectionmapping.Map[string, []Record]      `json:"records"`
	LogOffsets  collectionmapping.Map[string, uint64]        `json:"log_offsets"`
	Offsets     collectionmapping.Map[string, uint64]        `json:"offsets"`
	Placements  collectionlist.List[ShardPlacement]          `json:"shard_placements"`
	Members     collectionlist.List[ConsumerGroupMember]     `json:"members"`
	Assignments collectionlist.List[ConsumerGroupAssignment] `json:"assignments"`
	BrokerState *BrokerState                                 `json:"broker_state,omitempty"`
}

func (s *MemoryStore) Snapshot() (Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := Snapshot{
		Topics:      *collectionlist.NewListWithCapacity[TopicConfig](s.topics.Len()),
		Records:     *collectionmapping.NewMapWithCapacity[string, []Record](s.records.Len()),
		LogOffsets:  *collectionmapping.NewMapWithCapacity[string, uint64](s.nextOffsets.Len()),
		Offsets:     *collectionmapping.NewMapWithCapacity[string, uint64](s.offsets.Len()),
		Placements:  *collectionlist.NewListWithCapacity[ShardPlacement](s.placements.Len()),
		Members:     *collectionlist.NewListWithCapacity[ConsumerGroupMember](s.members.Len()),
		Assignments: *collectionlist.NewListWithCapacity[ConsumerGroupAssignment](s.assignments.Len()),
	}
	topics := s.topics.Values()
	for i := range topics {
		topic := topics[i]
		snap.Topics.Add(cloneTopic(topic))
	}
	s.records.Range(func(tp TopicPartition, records []Record) bool {
		snap.Records.Set(partitionKey(tp), cloneRecords(records))
		return true
	})
	s.nextOffsets.Range(func(tp TopicPartition, value uint64) bool {
		snap.LogOffsets.Set(partitionKey(tp), value)
		return true
	})
	s.offsets.Range(func(key string, value uint64) bool {
		snap.Offsets.Set(key, value)
		return true
	})
	placements := collectionlist.NewListWithCapacity[ShardPlacement](s.placements.Len())
	s.placements.Range(func(_ TopicPartition, placement ShardPlacement) bool {
		placements.Add(cloneShardPlacement(placement))
		return true
	})
	for _, placement := range sortShardPlacements(placements.Values()) {
		snap.Placements.Add(placement)
	}
	s.members.Range(func(_ string, member ConsumerGroupMember) bool {
		member.Topics = sortedStrings(member.Topics)
		snap.Members.Add(member)
		return true
	})
	s.assignments.Range(func(_ string, assignment ConsumerGroupAssignment) bool {
		assignment.Assignments = cloneGroupPartitionAssignments(assignment.Assignments)
		snap.Assignments.Add(assignment)
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
	s.nextOffsets = state.nextOffsets
	s.offsets = state.offsets
	s.placements = state.placements
	s.members = state.members
	s.assignments = state.assignments
	s.brokerState = state.brokerState
	return nil
}

type memoryRestoreState struct {
	topics      *collectionmapping.OrderedMap[string, TopicConfig]
	topicNames  *collectionset.Set[string]
	records     *collectionmapping.Map[TopicPartition, []Record]
	nextOffsets *collectionmapping.Map[TopicPartition, uint64]
	offsets     *collectionmapping.Map[string, uint64]
	placements  *collectionmapping.Map[TopicPartition, ShardPlacement]
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
		nextOffsets: restoreMemoryLogOffsets(snapshot.LogOffsets, records),
		offsets:     restoreMemoryOffsets(snapshot.Offsets),
		placements:  restoreMemoryShardPlacements(snapshot.Placements),
		members:     restoreMemoryMembers(snapshot.Members),
		assignments: restoreMemoryAssignments(snapshot.Assignments),
		brokerState: cloneBrokerState(snapshot.BrokerState),
	}, nil
}

func restoreMemoryLogOffsets(snapshotOffsets collectionmapping.Map[string, uint64], records *collectionmapping.Map[TopicPartition, []Record]) *collectionmapping.Map[TopicPartition, uint64] {
	nextOffsets := collectionmapping.NewMap[TopicPartition, uint64]()
	records.Range(func(tp TopicPartition, topicRecords []Record) bool {
		nextOffsets.Set(tp, nextOffsetFromRecords(topicRecords))
		return true
	})
	snapshotOffsets.Range(func(key string, value uint64) bool {
		tp, err := parsePartitionKey(key)
		if err == nil {
			nextOffsets.Set(tp, value)
		}
		return true
	})
	return nextOffsets
}

func restoreMemoryTopics(topics collectionlist.List[TopicConfig], out *collectionmapping.OrderedMap[string, TopicConfig], names *collectionset.Set[string], records *collectionmapping.Map[TopicPartition, []Record]) {
	topics.Range(func(_ int, topic TopicConfig) bool {
		normalizeTopic(&topic)
		out.Set(topic.Name, cloneTopic(topic))
		names.Add(topic.Name)
		for partition := range topic.Partitions {
			records.Set(NewTopicPartition(topic.Name, partition), nil)
		}
		return true
	})
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

func restoreMemoryShardPlacements(snapshotPlacements collectionlist.List[ShardPlacement]) *collectionmapping.Map[TopicPartition, ShardPlacement] {
	placements := collectionmapping.NewMapWithCapacity[TopicPartition, ShardPlacement](snapshotPlacements.Len())
	snapshotPlacements.Range(func(_ int, placement ShardPlacement) bool {
		if validateShardPlacement(placement) == nil {
			placements.Set(placement.TopicPartition(), cloneShardPlacement(placement))
		}
		return true
	})
	return placements
}

func restoreMemoryMembers(snapshotMembers collectionlist.List[ConsumerGroupMember]) *collectionmapping.Map[string, ConsumerGroupMember] {
	members := collectionmapping.NewMapWithCapacity[string, ConsumerGroupMember](snapshotMembers.Len())
	snapshotMembers.Range(func(_ int, member ConsumerGroupMember) bool {
		member.Topics = sortedStrings(member.Topics)
		members.Set(groupMemberKey(member.Group, member.MemberID), member)
		return true
	})
	return members
}

func restoreMemoryAssignments(snapshotAssignments collectionlist.List[ConsumerGroupAssignment]) *collectionmapping.Map[string, ConsumerGroupAssignment] {
	assignments := collectionmapping.NewMapWithCapacity[string, ConsumerGroupAssignment](snapshotAssignments.Len())
	snapshotAssignments.Range(func(_ int, assignment ConsumerGroupAssignment) bool {
		assignment.Assignments = cloneGroupPartitionAssignments(assignment.Assignments)
		assignments.Set(assignment.Group, assignment)
		return true
	})
	return assignments
}

func cloneRecords(records []Record) []Record {
	copied := collectionlist.NewListWithCapacity[Record](len(records))
	for _, record := range records {
		copied.Add(cloneRecord(record))
	}
	return copied.Values()
}

func cloneGroupPartitionAssignments(assignments []GroupPartitionAssignment) []GroupPartitionAssignment {
	return collectionlist.NewList(assignments...).Values()
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
