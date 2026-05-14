package store

import "context"

type MessageLogStore interface {
	CreateTopic(topic TopicConfig) error
	TopicExists(topic string) (bool, error)
	Append(topicPartition TopicPartition, payload []byte) (Record, error)
	AppendRecord(topicPartition TopicPartition, record RecordAppend) (Record, error)
	AppendRecordsBatch(topicPartition TopicPartition, records []RecordAppend) ([]Record, error)
	ReadFrom(topicPartition TopicPartition, offset uint64, maxRecords int) ([]Record, error)
	LastOffset(topicPartition TopicPartition) (*uint64, error)
}

type MessageLogPager interface {
	ReadPage(topicPartition TopicPartition, cursor string, maxRecords int) (RecordPage, error)
}

type RetentionCleaner interface {
	EnforceRetention(ctx context.Context, nowMS uint64) (RetentionCleanupResult, error)
}

type CompactionCleaner interface {
	Compact(ctx context.Context, nowMS uint64, sealedSegmentBatch int) (CompactionCleanupResult, error)
}

type OffsetStore interface {
	LoadConsumerOffset(consumer string, topicPartition TopicPartition) (*uint64, error)
	SaveConsumerOffset(consumer string, topicPartition TopicPartition, nextOffset uint64) error
}

type OffsetMetadataStore interface {
	SaveConsumerOffsetState(state ConsumerOffsetState) error
	LoadConsumerOffsetState(consumer string, topicPartition TopicPartition) (*ConsumerOffsetState, error)
	ListConsumerOffsetStates() ([]ConsumerOffsetState, error)
}

type ConsumerPauseStore interface {
	SaveConsumerPause(state ConsumerPauseState) error
	LoadConsumerPause(consumer string, topicPartition TopicPartition) (*ConsumerPauseState, error)
	ListConsumerPauses() ([]ConsumerPauseState, error)
	DeleteConsumerPause(consumer string, topicPartition TopicPartition) error
}

type TopicCatalogStore interface {
	SaveTopicConfig(topic TopicConfig) error
	LoadTopicConfig(topic string) (*TopicConfig, error)
	ListTopics() ([]TopicConfig, error)
}

type ShardPlacementStore interface {
	SaveShardPlacement(placement ShardPlacement) error
	LoadShardPlacement(topicPartition TopicPartition) (*ShardPlacement, error)
	ListShardPlacements() ([]ShardPlacement, error)
}

type ConsumerGroupStore interface {
	SaveGroupMember(member ConsumerGroupMember) error
	LoadGroupMember(group string, memberID string) (*ConsumerGroupMember, error)
	ListGroupMembers(group string) ([]ConsumerGroupMember, error)
	DeleteGroupMember(group string, memberID string) error
	DeleteExpiredGroupMembers(nowMS uint64) (int, error)
	SaveGroupAssignment(assignment ConsumerGroupAssignment) error
	LoadGroupAssignment(group string) (*ConsumerGroupAssignment, error)
	ListGroupAssignments() ([]ConsumerGroupAssignment, error)
}

type BrokerStateStore interface {
	SaveBrokerState(state BrokerState) error
	LoadBrokerState() (*BrokerState, error)
}

type TransactionStore interface {
	AllocateTransactionID() (uint64, error)
	SaveTransaction(state TransactionState) error
	LoadTransaction(txID uint64) (*TransactionState, error)
	ListTransactions() ([]TransactionState, error)
}

type ProducerBatchStore interface {
	SaveProducerBatch(batch ProducerPublishedBatch) error
	ListProducerBatches(filter ProducerBatchFilter) ([]ProducerPublishedBatch, error)
	DeleteProducerBatch(batch ProducerPublishedBatch) error
}

type ACLPolicyStore interface {
	SaveACLPolicy(policy ACLPolicy) error
	LoadACLPolicy(policyID string) (*ACLPolicy, error)
	ListACLPolicies(filter ACLPolicyFilter) ([]ACLPolicy, error)
	DeleteACLPolicy(policyID string) error
}

type Snapshotter interface {
	Snapshot() (Snapshot, error)
	Restore(snapshot Snapshot) error
}
