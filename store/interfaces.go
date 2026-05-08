package store

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
	EnforceRetention(nowMS uint64) (RetentionCleanupResult, error)
}

type CompactionCleaner interface {
	Compact(nowMS uint64, sealedSegmentBatch int) (CompactionCleanupResult, error)
}

type OffsetStore interface {
	LoadConsumerOffset(consumer string, topicPartition TopicPartition) (*uint64, error)
	SaveConsumerOffset(consumer string, topicPartition TopicPartition, nextOffset uint64) error
}

type TopicCatalogStore interface {
	SaveTopicConfig(topic TopicConfig) error
	LoadTopicConfig(topic string) (*TopicConfig, error)
	ListTopics() ([]TopicConfig, error)
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

type Snapshotter interface {
	Snapshot() (Snapshot, error)
	Restore(snapshot Snapshot) error
}
