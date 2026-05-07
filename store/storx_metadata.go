package store

import (
	"context"
	"os"
	"path/filepath"
	"sort"

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
		return nil, err
	}
	db, err := bboltx.Open(path, 0o600, nil)
	if err != nil {
		return nil, err
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
	return s.db.Close()
}

func (s *StorxMetadataStore) SaveTopicConfig(topic TopicConfig) error {
	if topic.Name == "" {
		return E(CodeInvalidArgument, "topic name is required")
	}
	normalizeTopic(&topic)
	return s.topics.Put(context.Background(), topic.Name, cloneTopic(topic))
}

func (s *StorxMetadataStore) LoadTopicConfig(topic string) (*TopicConfig, error) {
	cfg, ok, err := s.topics.Get(context.Background(), topic)
	if err != nil || !ok {
		return nil, err
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
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, err
}

func (s *StorxMetadataStore) LoadConsumerOffset(consumer string, topicPartition TopicPartition) (*uint64, error) {
	value, ok, err := s.offsets.Get(context.Background(), offsetKey(consumer, topicPartition))
	if err != nil || !ok {
		return nil, err
	}
	return &value, nil
}

func (s *StorxMetadataStore) SaveConsumerOffset(consumer string, topicPartition TopicPartition, nextOffset uint64) error {
	if consumer == "" {
		return E(CodeInvalidArgument, "consumer is required")
	}
	return s.offsets.Put(context.Background(), offsetKey(consumer, topicPartition), nextOffset)
}

func (s *StorxMetadataStore) SaveGroupMember(member ConsumerGroupMember) error {
	if member.Group == "" || member.MemberID == "" {
		return E(CodeInvalidArgument, "group and member_id are required")
	}
	member.Topics = append([]string(nil), member.Topics...)
	sort.Strings(member.Topics)
	return s.members.Put(context.Background(), groupMemberKey(member.Group, member.MemberID), member)
}

func (s *StorxMetadataStore) LoadGroupMember(group string, memberID string) (*ConsumerGroupMember, error) {
	member, ok, err := s.members.Get(context.Background(), groupMemberKey(group, memberID))
	if err != nil || !ok {
		return nil, err
	}
	member.Topics = append([]string(nil), member.Topics...)
	return &member, nil
}

func (s *StorxMetadataStore) ListGroupMembers(group string) ([]ConsumerGroupMember, error) {
	out := make([]ConsumerGroupMember, 0)
	err := s.members.Walk(context.Background(), func(entry bboltx.Entry[string, ConsumerGroupMember]) error {
		if entry.Value.Group == group {
			member := entry.Value
			member.Topics = append([]string(nil), member.Topics...)
			out = append(out, member)
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].MemberID < out[j].MemberID })
	return out, err
}

func (s *StorxMetadataStore) DeleteGroupMember(group string, memberID string) error {
	return s.members.Delete(context.Background(), groupMemberKey(group, memberID))
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
	return s.assignments.Put(context.Background(), assignment.Group, assignment)
}

func (s *StorxMetadataStore) LoadGroupAssignment(group string) (*ConsumerGroupAssignment, error) {
	assignment, ok, err := s.assignments.Get(context.Background(), group)
	if err != nil || !ok {
		return nil, err
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
	sort.Slice(out, func(i, j int) bool { return out[i].Group < out[j].Group })
	return out, err
}

func (s *StorxMetadataStore) SaveBrokerState(state BrokerState) error {
	return s.brokerState.Put(context.Background(), brokerStateKey, state)
}

func (s *StorxMetadataStore) LoadBrokerState() (*BrokerState, error) {
	state, ok, err := s.brokerState.Get(context.Background(), brokerStateKey)
	if err != nil || !ok {
		return nil, err
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
	offsets := make(map[string]uint64)
	err = s.offsets.Walk(context.Background(), func(entry bboltx.Entry[string, uint64]) error {
		offsets[entry.Key] = entry.Value
		return nil
	})
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{
		Topics:      topics,
		Offsets:     offsets,
		Members:     members,
		Assignments: assignments,
		BrokerState: state,
	}, nil
}

func (s *StorxMetadataStore) Restore(snapshot Snapshot) error {
	for _, bucket := range []interface{ Clear(context.Context) error }{
		bucketClearer[string, TopicConfig]{s.topics},
		bucketClearer[string, uint64]{s.offsets},
		bucketClearer[string, ConsumerGroupMember]{s.members},
		bucketClearer[string, ConsumerGroupAssignment]{s.assignments},
		bucketClearer[string, BrokerState]{s.brokerState},
	} {
		if err := bucket.Clear(context.Background()); err != nil {
			return err
		}
	}
	for _, topic := range snapshot.Topics {
		if err := s.SaveTopicConfig(topic); err != nil {
			return err
		}
	}
	for key, offset := range snapshot.Offsets {
		if err := s.offsets.Put(context.Background(), key, offset); err != nil {
			return err
		}
	}
	for _, member := range snapshot.Members {
		if err := s.SaveGroupMember(member); err != nil {
			return err
		}
	}
	for _, assignment := range snapshot.Assignments {
		if err := s.SaveGroupAssignment(assignment); err != nil {
			return err
		}
	}
	if snapshot.BrokerState != nil {
		return s.SaveBrokerState(*snapshot.BrokerState)
	}
	return nil
}

func (s *StorxMetadataStore) listAllMembers() ([]ConsumerGroupMember, error) {
	out := make([]ConsumerGroupMember, 0)
	err := s.members.Walk(context.Background(), func(entry bboltx.Entry[string, ConsumerGroupMember]) error {
		member := entry.Value
		member.Topics = append([]string(nil), member.Topics...)
		out = append(out, member)
		return nil
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].Group == out[j].Group {
			return out[i].MemberID < out[j].MemberID
		}
		return out[i].Group < out[j].Group
	})
	return out, err
}

type bucketClearer[K any, V any] struct {
	bucket *bboltx.Bucket[K, V]
}

func (c bucketClearer[K, V]) Clear(ctx context.Context) error {
	keys := make([]K, 0)
	if err := c.bucket.Walk(ctx, func(entry bboltx.Entry[K, V]) error {
		keys = append(keys, entry.Key)
		return nil
	}); err != nil {
		return err
	}
	for _, key := range keys {
		if err := c.bucket.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}
