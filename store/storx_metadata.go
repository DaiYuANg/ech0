package store

import (
	"cmp"
	"context"
	"sync"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/storx/bboltx"
)

const (
	bucketTopics              = "topics"
	bucketOffsets             = "offsets"
	bucketConsumerPauses      = "consumer_pauses"
	bucketMembers             = "group_members"
	bucketMembersGroup        = "group_members_by_group"
	bucketAssignments         = "group_assignments"
	bucketBrokerState         = "broker_state"
	bucketPlacements          = "shard_placements"
	bucketTransactions        = "transactions"
	bucketTransactionCounters = "transaction_counters"
	bucketProducerBatches     = "producer_batches"
	bucketACLPolicies         = "acl_policies"
	brokerStateKey            = "current"
	transactionCounterNext    = "next"
)

type StorxMetadataOptions struct{}

type StorxMetadataStore struct {
	db                  *bboltx.DB
	topics              *bboltx.Bucket[string, TopicConfig]
	offsets             *bboltx.Bucket[string, uint64]
	consumerPauses      *bboltx.Bucket[string, ConsumerPauseState]
	members             *bboltx.ModelStore[string, ConsumerGroupMember]
	membersByGroup      *bboltx.SecondaryIndexMany[string, ConsumerGroupMember, string]
	assignments         *bboltx.Bucket[string, ConsumerGroupAssignment]
	brokerState         *bboltx.Bucket[string, BrokerState]
	placements          *bboltx.Bucket[string, ShardPlacement]
	transactions        *bboltx.Bucket[string, TransactionState]
	transactionCounters *bboltx.Bucket[string, uint64]
	producerBatches     *bboltx.Bucket[string, ProducerPublishedBatch]
	aclPolicies         *bboltx.Bucket[string, ACLPolicy]
	txMu                sync.Mutex
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
	out := collectionlist.NewList[TopicConfig]()
	err := s.topics.Walk(context.Background(), func(entry bboltx.Entry[string, TopicConfig]) error {
		out.Add(cloneTopic(entry.Value))
		return nil
	})
	return out.
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
	_, _, err := s.members.Upsert(context.Background(), member)
	return wrapExternal(err, "save group member")
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
	members, err := s.membersByGroup.Query(s.members, group).ValueList(context.Background())
	if err != nil {
		return nil, wrapExternal(err, "list group members")
	}
	out := collectionlist.MapList(members, func(_ int, member ConsumerGroupMember) ConsumerGroupMember {
		member.Topics = sortedStrings(member.Topics)
		return member
	})
	return out.
		Sort(func(left, right ConsumerGroupMember) int {
			return cmp.Compare(left.MemberID, right.MemberID)
		}).
		Values(), nil
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
	assignment.Assignments = cloneGroupPartitionAssignments(assignment.Assignments)
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
	assignment.Assignments = cloneGroupPartitionAssignments(assignment.Assignments)
	return &assignment, nil
}

func (s *StorxMetadataStore) ListGroupAssignments() ([]ConsumerGroupAssignment, error) {
	out := collectionlist.NewList[ConsumerGroupAssignment]()
	err := s.assignments.Walk(context.Background(), func(entry bboltx.Entry[string, ConsumerGroupAssignment]) error {
		assignment := entry.Value
		assignment.Assignments = cloneGroupPartitionAssignments(assignment.Assignments)
		out.Add(assignment)
		return nil
	})
	return out.
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

func (s *StorxMetadataStore) listAllMembers() ([]ConsumerGroupMember, error) {
	out := collectionlist.NewList[ConsumerGroupMember]()
	err := s.members.Walk(context.Background(), func(entry bboltx.Entry[string, ConsumerGroupMember]) error {
		member := entry.Value
		member.Topics = sortedStrings(member.Topics)
		out.Add(member)
		return nil
	})
	return out.
		Sort(func(left, right ConsumerGroupMember) int {
			if left.Group == right.Group {
				return cmp.Compare(left.MemberID, right.MemberID)
			}
			return cmp.Compare(left.Group, right.Group)
		}).
		Values(), err
}
