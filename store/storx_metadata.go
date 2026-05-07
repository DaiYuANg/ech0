package store

import (
	"cmp"
	"context"
	"os"
	"path/filepath"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/arcgolabs/storx/bboltx"
	"github.com/arcgolabs/storx/codec"
	"github.com/arcgolabs/storx/keycodec"
)

const (
	bucketTopics      = "topics"
	bucketOffsets     = "offsets"
	bucketMembers     = "group_members"
	bucketAssignments = "group_assignments"
	bucketBrokerState = "broker_state"
	brokerStateKey    = "current"
)

type StorxMetadataStore struct {
	db          *bboltx.DB
	topics      *bboltx.Bucket[string, TopicConfig]
	offsets     *bboltx.Bucket[string, uint64]
	members     *bboltx.Bucket[string, ConsumerGroupMember]
	assignments *bboltx.Bucket[string, ConsumerGroupAssignment]
	brokerState *bboltx.Bucket[string, BrokerState]
}

func OpenStorxMetadataStore(path string) (*StorxMetadataStore, error) {
	if path == "" {
		return nil, E(CodeInvalidArgument, "metadata path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, wrapExternal(err, "create metadata directory")
	}
	db, err := bboltx.Open(path, 0o600, nil)
	if err != nil {
		return nil, wrapExternal(err, "open metadata store")
	}
	keyCodec := keycodec.String()
	return &StorxMetadataStore{
		db:          db,
		topics:      bboltx.NewBucketWithDB(db, bucketTopics, keyCodec, codec.JSON[TopicConfig]()),
		offsets:     bboltx.NewBucketWithDB(db, bucketOffsets, keyCodec, codec.JSON[uint64]()),
		members:     bboltx.NewBucketWithDB(db, bucketMembers, keyCodec, codec.JSON[ConsumerGroupMember]()),
		assignments: bboltx.NewBucketWithDB(db, bucketAssignments, keyCodec, codec.JSON[ConsumerGroupAssignment]()),
		brokerState: bboltx.NewBucketWithDB(db, bucketBrokerState, keyCodec, codec.JSON[BrokerState]()),
	}, nil
}

func (s *StorxMetadataStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return wrapExternal(s.db.Close(), "close metadata store")
}

func (s *StorxMetadataStore) SaveTopicConfig(topic TopicConfig) error {
	if topic.Name == "" {
		return E(CodeInvalidArgument, "topic name is required")
	}
	normalizeTopic(&topic)
	return wrapExternal(s.topics.Put(context.Background(), topic.Name, cloneTopic(topic)), "save topic metadata")
}

func (s *StorxMetadataStore) LoadTopicConfig(topic string) (*TopicConfig, error) {
	cfg, ok, err := s.topics.Get(context.Background(), topic)
	if err != nil {
		return nil, wrapExternal(err, "load topic metadata")
	}
	if !ok {
		var absent *TopicConfig
		return absent, nil
	}
	cfg = cloneTopic(cfg)
	return &cfg, nil
}

func (s *StorxMetadataStore) ListTopics() ([]TopicConfig, error) {
	out := make([]TopicConfig, 0)
	err := s.topics.Walk(context.Background(), func(entry bboltx.Entry[string, TopicConfig]) error {
		out = append(out, cloneTopic(entry.Value))
		return nil
	})
	return collectionlist.NewList(out...).
		Sort(func(left, right TopicConfig) int {
			return cmp.Compare(left.Name, right.Name)
		}).
		Values(), err
}

func (s *StorxMetadataStore) LoadConsumerOffset(consumer string, topicPartition TopicPartition) (*uint64, error) {
	value, ok, err := s.offsets.Get(context.Background(), offsetKey(consumer, topicPartition))
	if err != nil {
		return nil, wrapExternal(err, "load consumer offset")
	}
	if !ok {
		var absent *uint64
		return absent, nil
	}
	return &value, nil
}

func (s *StorxMetadataStore) SaveConsumerOffset(consumer string, topicPartition TopicPartition, nextOffset uint64) error {
	if consumer == "" {
		return E(CodeInvalidArgument, "consumer is required")
	}
	return wrapExternal(s.offsets.Put(context.Background(), offsetKey(consumer, topicPartition), nextOffset), "save consumer offset")
}

func (s *StorxMetadataStore) SaveGroupMember(member ConsumerGroupMember) error {
	if member.Group == "" || member.MemberID == "" {
		return E(CodeInvalidArgument, "group and member_id are required")
	}
	member.Topics = sortedStrings(member.Topics)
	return wrapExternal(s.members.Put(context.Background(), groupMemberKey(member.Group, member.MemberID), member), "save group member")
}

func (s *StorxMetadataStore) LoadGroupMember(group, memberID string) (*ConsumerGroupMember, error) {
	member, ok, err := s.members.Get(context.Background(), groupMemberKey(group, memberID))
	if err != nil {
		return nil, wrapExternal(err, "load group member")
	}
	if !ok {
		var absent *ConsumerGroupMember
		return absent, nil
	}
	member.Topics = sortedStrings(member.Topics)
	return &member, nil
}

func (s *StorxMetadataStore) ListGroupMembers(group string) ([]ConsumerGroupMember, error) {
	out := make([]ConsumerGroupMember, 0)
	err := s.members.Walk(context.Background(), func(entry bboltx.Entry[string, ConsumerGroupMember]) error {
		if entry.Value.Group == group {
			member := entry.Value
			member.Topics = sortedStrings(member.Topics)
			out = append(out, member)
		}
		return nil
	})
	return collectionlist.NewList(out...).
		Sort(func(left, right ConsumerGroupMember) int {
			return cmp.Compare(left.MemberID, right.MemberID)
		}).
		Values(), err
}

func (s *StorxMetadataStore) DeleteGroupMember(group, memberID string) error {
	return wrapExternal(s.members.Delete(context.Background(), groupMemberKey(group, memberID)), "delete group member")
}

func (s *StorxMetadataStore) DeleteExpiredGroupMembers(nowMS uint64) (int, error) {
	members, err := s.listAllMembers()
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, member := range members {
		if member.ExpiredAt(nowMS) {
			if err := s.DeleteGroupMember(member.Group, member.MemberID); err != nil {
				return deleted, err
			}
			deleted++
		}
	}
	return deleted, nil
}

func (s *StorxMetadataStore) SaveGroupAssignment(assignment ConsumerGroupAssignment) error {
	if assignment.Group == "" {
		return E(CodeInvalidArgument, "group is required")
	}
	assignment.Assignments = append([]GroupPartitionAssignment(nil), assignment.Assignments...)
	return wrapExternal(s.assignments.Put(context.Background(), assignment.Group, assignment), "save group assignment")
}

func (s *StorxMetadataStore) LoadGroupAssignment(group string) (*ConsumerGroupAssignment, error) {
	assignment, ok, err := s.assignments.Get(context.Background(), group)
	if err != nil {
		return nil, wrapExternal(err, "load group assignment")
	}
	if !ok {
		var absent *ConsumerGroupAssignment
		return absent, nil
	}
	assignment.Assignments = append([]GroupPartitionAssignment(nil), assignment.Assignments...)
	return &assignment, nil
}

func (s *StorxMetadataStore) ListGroupAssignments() ([]ConsumerGroupAssignment, error) {
	out := make([]ConsumerGroupAssignment, 0)
	err := s.assignments.Walk(context.Background(), func(entry bboltx.Entry[string, ConsumerGroupAssignment]) error {
		assignment := entry.Value
		assignment.Assignments = append([]GroupPartitionAssignment(nil), assignment.Assignments...)
		out = append(out, assignment)
		return nil
	})
	return collectionlist.NewList(out...).
		Sort(func(left, right ConsumerGroupAssignment) int {
			return cmp.Compare(left.Group, right.Group)
		}).
		Values(), err
}

func (s *StorxMetadataStore) SaveBrokerState(state BrokerState) error {
	return wrapExternal(s.brokerState.Put(context.Background(), brokerStateKey, state), "save broker state")
}

func (s *StorxMetadataStore) LoadBrokerState() (*BrokerState, error) {
	state, ok, err := s.brokerState.Get(context.Background(), brokerStateKey)
	if err != nil {
		return nil, wrapExternal(err, "load broker state")
	}
	if !ok {
		var absent *BrokerState
		return absent, nil
	}
	return &state, nil
}

func (s *StorxMetadataStore) Snapshot() (Snapshot, error) {
	topics, err := s.ListTopics()
	if err != nil {
		return Snapshot{}, err
	}
	members, err := s.listAllMembers()
	if err != nil {
		return Snapshot{}, err
	}
	assignments, err := s.ListGroupAssignments()
	if err != nil {
		return Snapshot{}, err
	}
	state, err := s.LoadBrokerState()
	if err != nil {
		return Snapshot{}, err
	}
	offsets := collectionmapping.NewMap[string, uint64]()
	err = s.offsets.Walk(context.Background(), func(entry bboltx.Entry[string, uint64]) error {
		offsets.Set(entry.Key, entry.Value)
		return nil
	})
	if err != nil {
		return Snapshot{}, wrapExternal(err, "walk consumer offsets")
	}
	return Snapshot{
		Topics:      topics,
		Offsets:     *offsets,
		Members:     members,
		Assignments: assignments,
		BrokerState: state,
	}, nil
}

func (s *StorxMetadataStore) listAllMembers() ([]ConsumerGroupMember, error) {
	out := make([]ConsumerGroupMember, 0)
	err := s.members.Walk(context.Background(), func(entry bboltx.Entry[string, ConsumerGroupMember]) error {
		member := entry.Value
		member.Topics = sortedStrings(member.Topics)
		out = append(out, member)
		return nil
	})
	return collectionlist.NewList(out...).
		Sort(func(left, right ConsumerGroupMember) int {
			if left.Group == right.Group {
				return cmp.Compare(left.MemberID, right.MemberID)
			}
			return cmp.Compare(left.Group, right.Group)
		}).
		Values(), err
}

type bucketClearer[K any, V any] struct {
	bucket *bboltx.Bucket[K, V]
}

func (c bucketClearer[K, V]) Clear(ctx context.Context) error {
	keys := collectionlist.NewList[K]()
	if err := c.bucket.Walk(ctx, func(entry bboltx.Entry[K, V]) error {
		keys.Add(entry.Key)
		return nil
	}); err != nil {
		return wrapExternal(err, "walk bucket keys")
	}
	for _, key := range keys.Values() {
		if err := c.bucket.Delete(ctx, key); err != nil {
			return wrapExternal(err, "delete bucket key")
		}
	}
	return nil
}
