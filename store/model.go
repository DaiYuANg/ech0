package store

import "time"

const RecordAttributeTombstone uint16 = 1 << 1

type TransactionStatus string

const (
	TransactionStatusOpen      TransactionStatus = "open"
	TransactionStatusCommitted TransactionStatus = "committed"
	TransactionStatusAborted   TransactionStatus = "aborted"
)

type TransactionControlType string

const (
	TransactionControlNone   TransactionControlType = ""
	TransactionControlCommit TransactionControlType = "commit"
	TransactionControlAbort  TransactionControlType = "abort"
)

type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition uint32 `json:"partition"`
}

type ShardID uint32

type ShardPlacement struct {
	Topic     string  `json:"topic"`
	Partition uint32  `json:"partition"`
	ShardID   ShardID `json:"shard_id"`
}

type ConsumerPauseState struct {
	Consumer    string `json:"consumer"`
	Topic       string `json:"topic"`
	Partition   uint32 `json:"partition"`
	Paused      bool   `json:"paused"`
	UpdatedAtMS uint64 `json:"updated_at_ms"`
}

func NewTopicPartition(topic string, partition uint32) TopicPartition {
	return TopicPartition{Topic: topic, Partition: partition}
}

func NewShardPlacement(topic string, partition uint32, shardID ShardID) ShardPlacement {
	return ShardPlacement{Topic: topic, Partition: partition, ShardID: shardID}
}

func (p ShardPlacement) TopicPartition() TopicPartition {
	return NewTopicPartition(p.Topic, p.Partition)
}

type TopicCleanupPolicy string

const (
	TopicCleanupDelete           TopicCleanupPolicy = "delete"
	TopicCleanupCompact          TopicCleanupPolicy = "compact"
	TopicCleanupCompactAndDelete TopicCleanupPolicy = "compact_and_delete"
)

type TopicRetryPolicy struct {
	MaxAttempts      uint32 `json:"max_attempts"       toml:"max_attempts"`
	BackoffInitialMS uint64 `json:"backoff_initial_ms" toml:"backoff_initial_ms"`
	BackoffMaxMS     uint64 `json:"backoff_max_ms"     toml:"backoff_max_ms"`
}

func DefaultTopicRetryPolicy() TopicRetryPolicy {
	return TopicRetryPolicy{
		MaxAttempts:      16,
		BackoffInitialMS: 100,
		BackoffMaxMS:     30000,
	}
}

type TopicConfig struct {
	Name                           string             `json:"name"                                        toml:"name"`
	Partitions                     uint32             `json:"partitions"                                  toml:"partitions"`
	SegmentMaxBytes                uint64             `json:"segment_max_bytes"                           toml:"segment_max_bytes"`
	IndexIntervalBytes             uint64             `json:"index_interval_bytes"                        toml:"index_interval_bytes"`
	RetentionMaxBytes              uint64             `json:"retention_max_bytes"                         toml:"retention_max_bytes"`
	CleanupPolicy                  TopicCleanupPolicy `json:"cleanup_policy"                              toml:"cleanup_policy"`
	MaxMessageBytes                uint32             `json:"max_message_bytes"                           toml:"max_message_bytes"`
	MaxBatchBytes                  uint32             `json:"max_batch_bytes"                             toml:"max_batch_bytes"`
	RetentionMS                    *uint64            `json:"retention_ms,omitempty"                      toml:"retention_ms"`
	RetryPolicy                    TopicRetryPolicy   `json:"retry_policy"                                toml:"retry_policy"`
	DeadLetterTopic                *string            `json:"dead_letter_topic,omitempty"                 toml:"dead_letter_topic"`
	DelayEnabled                   bool               `json:"delay_enabled"                               toml:"delay_enabled"`
	CompactionEnabled              bool               `json:"compaction_enabled"                          toml:"compaction_enabled"`
	CompactionTombstoneRetentionMS *uint64            `json:"compaction_tombstone_retention_ms,omitempty" toml:"compaction_tombstone_retention_ms"`
}

func NewTopicConfig(name string) TopicConfig {
	return TopicConfig{
		Name:               name,
		Partitions:         1,
		SegmentMaxBytes:    16 * 1024 * 1024,
		IndexIntervalBytes: 4 * 1024,
		RetentionMaxBytes:  256 * 1024 * 1024,
		CleanupPolicy:      TopicCleanupDelete,
		MaxMessageBytes:    1024 * 1024,
		MaxBatchBytes:      8 * 1024 * 1024,
		RetryPolicy:        DefaultTopicRetryPolicy(),
	}
}

type RecordHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type RecordAppend struct {
	TimestampMS *uint64                    `json:"timestamp_ms,omitempty"`
	Key         []byte                     `json:"key,omitempty"`
	Headers     []RecordHeader             `json:"headers,omitempty"`
	Attributes  uint16                     `json:"attributes,omitempty"`
	Transaction *TransactionRecordMetadata `json:"transaction,omitempty"`
	Payload     []byte                     `json:"payload"`
}

func NewRecordAppend(payload []byte) RecordAppend {
	return RecordAppend{Payload: append([]byte(nil), payload...)}
}

func (r RecordAppend) IsTombstone() bool {
	return (r.Attributes & RecordAttributeTombstone) != 0
}

type Record struct {
	Offset      uint64                     `json:"offset"`
	TimestampMS uint64                     `json:"timestamp_ms"`
	Key         []byte                     `json:"key,omitempty"`
	Headers     []RecordHeader             `json:"headers,omitempty"`
	Attributes  uint16                     `json:"attributes,omitempty"`
	Transaction *TransactionRecordMetadata `json:"transaction,omitempty"`
	Payload     []byte                     `json:"payload"`
}

func (r Record) IsTombstone() bool {
	return (r.Attributes & RecordAttributeTombstone) != 0
}

type PollResult struct {
	Records       []Record
	NextOffset    uint64
	HighWatermark *uint64
}

type TransactionRecordMetadata struct {
	TxID          uint64                 `json:"tx_id"`
	ProducerID    uint64                 `json:"producer_id"`
	ProducerEpoch uint64                 `json:"producer_epoch"`
	Sequence      uint64                 `json:"sequence"`
	ControlType   TransactionControlType `json:"control_type,omitempty"`
}

type TransactionOffsetCommit struct {
	Consumer   string `json:"consumer,omitempty"`
	Group      string `json:"group,omitempty"`
	MemberID   string `json:"member_id,omitempty"`
	Generation uint64 `json:"generation,omitempty"`
	Topic      string `json:"topic"`
	Partition  uint32 `json:"partition"`
	NextOffset uint64 `json:"next_offset"`
	Metadata   string `json:"metadata,omitempty"`
}

type TransactionPublishedBatch struct {
	Topic        string `json:"topic"`
	Partition    uint32 `json:"partition"`
	BaseSequence uint64 `json:"base_sequence"`
	RecordCount  uint64 `json:"record_count"`
	BaseOffset   uint64 `json:"base_offset"`
	LastOffset   uint64 `json:"last_offset"`
	NextOffset   uint64 `json:"next_offset"`
}

type ProducerPublishedBatch struct {
	ProducerID    uint64 `json:"producer_id"`
	ProducerEpoch uint64 `json:"producer_epoch"`
	Topic         string `json:"topic"`
	Partition     uint32 `json:"partition"`
	BaseSequence  uint64 `json:"base_sequence"`
	RecordCount   uint64 `json:"record_count"`
	BaseOffset    uint64 `json:"base_offset"`
	LastOffset    uint64 `json:"last_offset"`
	NextOffset    uint64 `json:"next_offset"`
	UpdatedAtMS   uint64 `json:"updated_at_ms"`
}

type ProducerBatchFilter struct {
	ProducerID    uint64
	Topic         string
	ProducerEpoch uint64
	EpochSet      bool
	Partition     uint32
	PartitionSet  bool
}

type TransactionState struct {
	TxID             uint64                      `json:"tx_id"`
	TransactionalID  string                      `json:"transactional_id"`
	ProducerID       uint64                      `json:"producer_id"`
	ProducerEpoch    uint64                      `json:"producer_epoch"`
	Status           TransactionStatus           `json:"status"`
	TimeoutMS        uint64                      `json:"timeout_ms"`
	CreatedAtMS      uint64                      `json:"created_at_ms"`
	UpdatedAtMS      uint64                      `json:"updated_at_ms"`
	ExpiresAtMS      uint64                      `json:"expires_at_ms"`
	NextSequence     uint64                      `json:"next_sequence"`
	Partitions       []TopicPartition            `json:"partitions,omitempty"`
	PublishedBatches []TransactionPublishedBatch `json:"published_batches,omitempty"`
	OffsetCommits    []TransactionOffsetCommit   `json:"offset_commits,omitempty"`
}

type RecordPage struct {
	Records    []Record
	NextCursor string
	HasMore    bool
}

type ConsumerGroupMember struct {
	Group             string   `json:"group"`
	MemberID          string   `json:"member_id"`
	Topics            []string `json:"topics"`
	SessionTimeoutMS  uint64   `json:"session_timeout_ms"`
	MaxPollIntervalMS uint64   `json:"max_poll_interval_ms"`
	JoinedAtMS        uint64   `json:"joined_at_ms"`
	LastHeartbeatMS   uint64   `json:"last_heartbeat_ms"`
	LastPollMS        uint64   `json:"last_poll_ms"`
}

func (m ConsumerGroupMember) ExpiresAtMS() uint64 {
	timeout := m.SessionTimeoutMS
	if timeout == 0 {
		timeout = 1
	}
	return m.LastHeartbeatMS + timeout
}

func (m ConsumerGroupMember) PollExpiresAtMS() uint64 {
	timeout := m.MaxPollIntervalMS
	if timeout == 0 {
		timeout = m.SessionTimeoutMS
	}
	if timeout == 0 {
		timeout = 1
	}
	return m.lastPollMS() + timeout
}

func (m ConsumerGroupMember) ExpiredAt(nowMS uint64) bool {
	return nowMS >= m.ExpiresAtMS() || nowMS >= m.PollExpiresAtMS()
}

func (m ConsumerGroupMember) lastPollMS() uint64 {
	if m.LastPollMS != 0 {
		return m.LastPollMS
	}
	if m.LastHeartbeatMS != 0 {
		return m.LastHeartbeatMS
	}
	return m.JoinedAtMS
}

type GroupPartitionAssignment struct {
	MemberID  string `json:"member_id"`
	Topic     string `json:"topic"`
	Partition uint32 `json:"partition"`
}

type ConsumerGroupAssignment struct {
	Group       string                     `json:"group"`
	Generation  uint64                     `json:"generation"`
	Assignments []GroupPartitionAssignment `json:"assignments"`
	UpdatedAtMS uint64                     `json:"updated_at_ms"`
}

type BrokerState struct {
	NodeID string `json:"node_id"`
	Epoch  uint64 `json:"epoch"`
}

func NowMS() uint64 {
	return uint64(time.Now().UnixMilli())
}
