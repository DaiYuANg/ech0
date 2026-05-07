// Package protocol defines the ech0 wire protocol messages.
//
//nolint:revive // Protocol constants and DTOs are grouped to keep wire compatibility auditable.
package protocol

import (
	"encoding/json"

	"github.com/samber/oops"
)

const Version1 uint8 = 1

const (
	CmdHandshakeRequest                  uint16 = 1
	CmdPingRequest                       uint16 = 2
	CmdCreateTopicRequest                uint16 = 3
	CmdProduceRequest                    uint16 = 4
	CmdFetchRequest                      uint16 = 5
	CmdCommitOffsetRequest               uint16 = 6
	CmdListTopicsRequest                 uint16 = 7
	CmdSendDirectRequest                 uint16 = 8
	CmdFetchInboxRequest                 uint16 = 9
	CmdAckDirectRequest                  uint16 = 10
	CmdJoinConsumerGroupRequest          uint16 = 11
	CmdHeartbeatConsumerGroupRequest     uint16 = 12
	CmdRebalanceConsumerGroupRequest     uint16 = 13
	CmdGetConsumerGroupAssignmentRequest uint16 = 14
	CmdProduceBatchRequest               uint16 = 15
	CmdFetchBatchRequest                 uint16 = 16
	CmdNackRequest                       uint16 = 17
	CmdProcessRetryRequest               uint16 = 18
	CmdScheduleDelayRequest              uint16 = 19
	CmdFetchConsumerGroupRequest         uint16 = 20
	CmdCommitConsumerGroupOffsetRequest  uint16 = 21
	CmdFetchConsumerGroupBatchRequest    uint16 = 22
)

const (
	CmdHandshakeResponse                  uint16 = 101
	CmdPingResponse                       uint16 = 102
	CmdCreateTopicResponse                uint16 = 103
	CmdProduceResponse                    uint16 = 104
	CmdFetchResponse                      uint16 = 105
	CmdCommitOffsetResponse               uint16 = 106
	CmdListTopicsResponse                 uint16 = 107
	CmdSendDirectResponse                 uint16 = 108
	CmdFetchInboxResponse                 uint16 = 109
	CmdAckDirectResponse                  uint16 = 110
	CmdJoinConsumerGroupResponse          uint16 = 111
	CmdHeartbeatConsumerGroupResponse     uint16 = 112
	CmdRebalanceConsumerGroupResponse     uint16 = 113
	CmdGetConsumerGroupAssignmentResponse uint16 = 114
	CmdProduceBatchResponse               uint16 = 115
	CmdFetchBatchResponse                 uint16 = 116
	CmdNackResponse                       uint16 = 117
	CmdProcessRetryResponse               uint16 = 118
	CmdScheduleDelayResponse              uint16 = 119
	CmdFetchConsumerGroupResponse         uint16 = 120
	CmdCommitConsumerGroupOffsetResponse  uint16 = 121
	CmdFetchConsumerGroupBatchResponse    uint16 = 122
	CmdErrorResponse                      uint16 = 500
)

type TopicCleanupPolicy string

const (
	TopicCleanupDelete           TopicCleanupPolicy = "delete"
	TopicCleanupCompact          TopicCleanupPolicy = "compact"
	TopicCleanupCompactAndDelete TopicCleanupPolicy = "compact_and_delete"
)

type ProducePartitioning string

const (
	ProducePartitioningExplicit   ProducePartitioning = "explicit"
	ProducePartitioningRoundRobin ProducePartitioning = "round_robin"
	ProducePartitioningKeyHash    ProducePartitioning = "key_hash"
)

type HandshakeRequest struct {
	ClientID string `json:"client_id"`
}

type HandshakeResponse struct {
	ServerID        string `json:"server_id"`
	ProtocolVersion uint8  `json:"protocol_version"`
}

type PingRequest struct {
	Nonce uint64 `json:"nonce"`
}

type PingResponse struct {
	Nonce uint64 `json:"nonce"`
}

type TopicRetryPolicy struct {
	MaxAttempts      uint32 `json:"max_attempts"`
	BackoffInitialMS uint64 `json:"backoff_initial_ms"`
	BackoffMaxMS     uint64 `json:"backoff_max_ms"`
}

type CreateTopicRequest struct {
	Topic                          string              `json:"topic"`
	Partitions                     uint32              `json:"partitions"`
	RetentionMaxBytes              *uint64             `json:"retention_max_bytes,omitempty"`
	CleanupPolicy                  *TopicCleanupPolicy `json:"cleanup_policy,omitempty"`
	MaxMessageBytes                *uint32             `json:"max_message_bytes,omitempty"`
	MaxBatchBytes                  *uint32             `json:"max_batch_bytes,omitempty"`
	RetentionMS                    *uint64             `json:"retention_ms,omitempty"`
	RetryPolicy                    *TopicRetryPolicy   `json:"retry_policy,omitempty"`
	DeadLetterTopic                *string             `json:"dead_letter_topic,omitempty"`
	DelayEnabled                   *bool               `json:"delay_enabled,omitempty"`
	CompactionEnabled              *bool               `json:"compaction_enabled,omitempty"`
	CompactionTombstoneRetentionMS *uint64             `json:"compaction_tombstone_retention_ms,omitempty"`
}

type CreateTopicResponse struct {
	Topic      string `json:"topic"`
	Partitions uint32 `json:"partitions"`
}

type ProduceRequest struct {
	Topic        string              `json:"topic"`
	Partition    *uint32             `json:"partition,omitempty"`
	Partitioning ProducePartitioning `json:"partitioning"`
	Key          []byte              `json:"key,omitempty"`
	Tombstone    bool                `json:"tombstone,omitempty"`
	Payload      []byte              `json:"payload"`
}

type ProduceResponse struct {
	Partition  uint32 `json:"partition"`
	Offset     uint64 `json:"offset"`
	NextOffset uint64 `json:"next_offset"`
}

type ProduceBatchRecord struct {
	Key       []byte `json:"key,omitempty"`
	Tombstone bool   `json:"tombstone,omitempty"`
	Payload   []byte `json:"payload"`
}

type ProduceBatchRequest struct {
	Topic        string               `json:"topic"`
	Partition    *uint32              `json:"partition,omitempty"`
	Partitioning ProducePartitioning  `json:"partitioning"`
	Payloads     [][]byte             `json:"payloads,omitempty"`
	Records      []ProduceBatchRecord `json:"records,omitempty"`
}

type ProduceBatchResponse struct {
	Partition  uint32 `json:"partition"`
	BaseOffset uint64 `json:"base_offset"`
	LastOffset uint64 `json:"last_offset"`
	NextOffset uint64 `json:"next_offset"`
	Appended   int    `json:"appended"`
}

type FetchRequest struct {
	Consumer   string  `json:"consumer"`
	Topic      string  `json:"topic"`
	Partition  uint32  `json:"partition"`
	Offset     *uint64 `json:"offset,omitempty"`
	MaxRecords int     `json:"max_records"`
	MinRecords *int    `json:"min_records,omitempty"`
	MaxWaitMS  *uint64 `json:"max_wait_ms,omitempty"`
}

type FetchRecord struct {
	Offset      uint64 `json:"offset"`
	TimestampMS uint64 `json:"timestamp_ms"`
	Key         []byte `json:"key,omitempty"`
	Tombstone   bool   `json:"tombstone,omitempty"`
	Payload     []byte `json:"payload"`
}

type FetchResponse struct {
	Topic         string        `json:"topic"`
	Partition     uint32        `json:"partition"`
	Records       []FetchRecord `json:"records"`
	NextOffset    uint64        `json:"next_offset"`
	HighWatermark *uint64       `json:"high_watermark,omitempty"`
}

type FetchBatchItemRequest struct {
	Topic      string  `json:"topic"`
	Partition  uint32  `json:"partition"`
	Offset     *uint64 `json:"offset,omitempty"`
	MaxRecords int     `json:"max_records"`
}

type FetchBatchRequest struct {
	Consumer   string                  `json:"consumer"`
	Items      []FetchBatchItemRequest `json:"items"`
	MinRecords *int                    `json:"min_records,omitempty"`
	MaxWaitMS  *uint64                 `json:"max_wait_ms,omitempty"`
}

type FetchBatchItemResponse struct {
	Topic         string        `json:"topic"`
	Partition     uint32        `json:"partition"`
	Records       []FetchRecord `json:"records"`
	NextOffset    uint64        `json:"next_offset"`
	HighWatermark *uint64       `json:"high_watermark,omitempty"`
}

type FetchBatchResponse struct {
	Items []FetchBatchItemResponse `json:"items"`
}

type CommitOffsetRequest struct {
	Consumer   string `json:"consumer"`
	Topic      string `json:"topic"`
	Partition  uint32 `json:"partition"`
	NextOffset uint64 `json:"next_offset"`
}

type CommitOffsetResponse struct {
	Consumer   string `json:"consumer"`
	Topic      string `json:"topic"`
	Partition  uint32 `json:"partition"`
	NextOffset uint64 `json:"next_offset"`
}

type TopicMetadata struct {
	Topic      string `json:"topic"`
	Partitions uint32 `json:"partitions"`
}

type ListTopicsResponse struct {
	Topics []TopicMetadata `json:"topics"`
}

type SendDirectRequest struct {
	Sender         string  `json:"sender"`
	Recipient      string  `json:"recipient"`
	ConversationID *string `json:"conversation_id,omitempty"`
	Payload        []byte  `json:"payload"`
}

type SendDirectResponse struct {
	MessageID      string `json:"message_id"`
	ConversationID string `json:"conversation_id"`
	Offset         uint64 `json:"offset"`
	NextOffset     uint64 `json:"next_offset"`
}

type FetchInboxRequest struct {
	Recipient  string `json:"recipient"`
	MaxRecords int    `json:"max_records"`
}

type DirectMessageRecord struct {
	Offset         uint64 `json:"offset"`
	MessageID      string `json:"message_id"`
	ConversationID string `json:"conversation_id"`
	Sender         string `json:"sender"`
	Recipient      string `json:"recipient"`
	TimestampMS    uint64 `json:"timestamp_ms"`
	Payload        []byte `json:"payload"`
}

type FetchInboxResponse struct {
	Recipient     string                `json:"recipient"`
	Records       []DirectMessageRecord `json:"records"`
	NextOffset    uint64                `json:"next_offset"`
	HighWatermark *uint64               `json:"high_watermark,omitempty"`
}

type AckDirectRequest struct {
	Recipient  string `json:"recipient"`
	NextOffset uint64 `json:"next_offset"`
}

type AckDirectResponse struct {
	Recipient  string `json:"recipient"`
	NextOffset uint64 `json:"next_offset"`
}

type JoinConsumerGroupRequest struct {
	Group            string   `json:"group"`
	MemberID         string   `json:"member_id"`
	Topics           []string `json:"topics"`
	SessionTimeoutMS uint64   `json:"session_timeout_ms"`
}

type ConsumerGroupMemberLease struct {
	Group            string   `json:"group"`
	MemberID         string   `json:"member_id"`
	Topics           []string `json:"topics"`
	SessionTimeoutMS uint64   `json:"session_timeout_ms"`
	JoinedAtMS       uint64   `json:"joined_at_ms"`
	LastHeartbeatMS  uint64   `json:"last_heartbeat_ms"`
	ExpiresAtMS      uint64   `json:"expires_at_ms"`
}

type JoinConsumerGroupResponse struct {
	Lease ConsumerGroupMemberLease `json:"lease"`
}

type HeartbeatConsumerGroupRequest struct {
	Group            string  `json:"group"`
	MemberID         string  `json:"member_id"`
	SessionTimeoutMS *uint64 `json:"session_timeout_ms,omitempty"`
}

type HeartbeatConsumerGroupResponse struct {
	Lease ConsumerGroupMemberLease `json:"lease"`
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

type RebalanceConsumerGroupRequest struct {
	Group string `json:"group"`
}

type RebalanceConsumerGroupResponse struct {
	Assignment ConsumerGroupAssignment `json:"assignment"`
}

type GetConsumerGroupAssignmentRequest struct {
	Group string `json:"group"`
}

type GetConsumerGroupAssignmentResponse struct {
	Assignment *ConsumerGroupAssignment `json:"assignment,omitempty"`
}

type FetchConsumerGroupRequest struct {
	Group      string  `json:"group"`
	MemberID   string  `json:"member_id"`
	Generation uint64  `json:"generation"`
	Topic      string  `json:"topic"`
	Partition  uint32  `json:"partition"`
	Offset     *uint64 `json:"offset,omitempty"`
	MaxRecords int     `json:"max_records"`
	MinRecords *int    `json:"min_records,omitempty"`
	MaxWaitMS  *uint64 `json:"max_wait_ms,omitempty"`
}

type FetchConsumerGroupResponse struct {
	Group         string        `json:"group"`
	MemberID      string        `json:"member_id"`
	Generation    uint64        `json:"generation"`
	Topic         string        `json:"topic"`
	Partition     uint32        `json:"partition"`
	Records       []FetchRecord `json:"records"`
	NextOffset    uint64        `json:"next_offset"`
	HighWatermark *uint64       `json:"high_watermark,omitempty"`
}

type CommitConsumerGroupOffsetRequest struct {
	Group      string `json:"group"`
	MemberID   string `json:"member_id"`
	Generation uint64 `json:"generation"`
	Topic      string `json:"topic"`
	Partition  uint32 `json:"partition"`
	NextOffset uint64 `json:"next_offset"`
}

type CommitConsumerGroupOffsetResponse struct {
	Group      string `json:"group"`
	MemberID   string `json:"member_id"`
	Generation uint64 `json:"generation"`
	Topic      string `json:"topic"`
	Partition  uint32 `json:"partition"`
	NextOffset uint64 `json:"next_offset"`
}

type FetchConsumerGroupBatchRequest struct {
	Group      string                  `json:"group"`
	MemberID   string                  `json:"member_id"`
	Generation uint64                  `json:"generation"`
	Items      []FetchBatchItemRequest `json:"items"`
	MinRecords *int                    `json:"min_records,omitempty"`
	MaxWaitMS  *uint64                 `json:"max_wait_ms,omitempty"`
}

type FetchConsumerGroupBatchItemResponse struct {
	Topic         string        `json:"topic"`
	Partition     uint32        `json:"partition"`
	Records       []FetchRecord `json:"records"`
	NextOffset    uint64        `json:"next_offset"`
	HighWatermark *uint64       `json:"high_watermark,omitempty"`
}

type FetchConsumerGroupBatchResponse struct {
	Group      string                                `json:"group"`
	MemberID   string                                `json:"member_id"`
	Generation uint64                                `json:"generation"`
	Items      []FetchConsumerGroupBatchItemResponse `json:"items"`
}

type NackRequest struct {
	Consumer  string  `json:"consumer"`
	Topic     string  `json:"topic"`
	Partition uint32  `json:"partition"`
	Offset    uint64  `json:"offset"`
	LastError *string `json:"last_error,omitempty"`
}

type NackResponse struct {
	RetryTopic      string `json:"retry_topic"`
	RetryPartition  uint32 `json:"retry_partition"`
	RetryOffset     uint64 `json:"retry_offset"`
	RetryNextOffset uint64 `json:"retry_next_offset"`
	RetryCount      uint32 `json:"retry_count"`
}

type ProcessRetryRequest struct {
	Consumer    string `json:"consumer"`
	SourceTopic string `json:"source_topic"`
	Partition   uint32 `json:"partition"`
	MaxRecords  int    `json:"max_records"`
}

type ProcessRetryResponse struct {
	RetryTopic          string  `json:"retry_topic"`
	Partition           uint32  `json:"partition"`
	MovedToOrigin       int     `json:"moved_to_origin"`
	MovedToDeadLetter   int     `json:"moved_to_dead_letter"`
	CommittedNextOffset *uint64 `json:"committed_next_offset,omitempty"`
}

type ScheduleDelayRequest struct {
	Topic       string `json:"topic"`
	Partition   uint32 `json:"partition"`
	Payload     []byte `json:"payload"`
	DeliverAtMS uint64 `json:"deliver_at_ms"`
}

type ScheduleDelayResponse struct {
	DelayTopic  string `json:"delay_topic"`
	Partition   uint32 `json:"partition"`
	Offset      uint64 `json:"offset"`
	NextOffset  uint64 `json:"next_offset"`
	DeliverAtMS uint64 `json:"deliver_at_ms"`
}

type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func EncodeJSON[T any](value T) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, oops.In("protocol").Code("json_encode_failed").Wrapf(err, "encode json")
	}
	return data, nil
}

func DecodeJSON[T any](data []byte) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	if err != nil {
		return value, oops.In("protocol").Code("json_decode_failed").Wrapf(err, "decode json")
	}
	return value, nil
}
