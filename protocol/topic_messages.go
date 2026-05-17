package protocol

type HandshakeRequest struct {
	ClientID     string   `json:"client_id"`
	Tenant       string   `json:"tenant,omitempty"`
	Namespace    string   `json:"namespace,omitempty"`
	Principal    string   `json:"principal,omitempty"`
	AuthToken    string   `json:"auth_token,omitempty"`
	Capabilities []string `json:"capabilities,omitempty"`
}

type HandshakeResponse struct {
	ServerID        string   `json:"server_id"`
	ProtocolVersion uint8    `json:"protocol_version"`
	Tenant          string   `json:"tenant,omitempty"`
	Namespace       string   `json:"namespace,omitempty"`
	Principal       string   `json:"principal,omitempty"`
	Capabilities    []string `json:"capabilities,omitempty"`
}

type PingRequest struct {
	Nonce uint64 `json:"nonce"`
}

type PingResponse struct {
	Nonce uint64 `json:"nonce"`
}

type MessageHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type TopicRetryPolicy struct {
	MaxAttempts         uint32  `json:"max_attempts"`
	BackoffInitialMS    uint64  `json:"backoff_initial_ms"`
	BackoffMaxMS        uint64  `json:"backoff_max_ms"`
	BackoffJitterFactor float64 `json:"backoff_jitter_factor"`
}

type MessageExpiryAction string

const (
	MessageExpiryDelete MessageExpiryAction = "delete"
	MessageExpiryDLQ    MessageExpiryAction = "dlq"
)

type CreateTopicRequest struct {
	Topic                          string               `json:"topic"`
	Partitions                     uint32               `json:"partitions"`
	RetentionMaxBytes              *uint64              `json:"retention_max_bytes,omitempty"`
	CleanupPolicy                  *TopicCleanupPolicy  `json:"cleanup_policy,omitempty"`
	MaxMessageBytes                *uint32              `json:"max_message_bytes,omitempty"`
	MaxBatchBytes                  *uint32              `json:"max_batch_bytes,omitempty"`
	RetentionMS                    *uint64              `json:"retention_ms,omitempty"`
	RetryPolicy                    *TopicRetryPolicy    `json:"retry_policy,omitempty"`
	DeadLetterTopic                *string              `json:"dead_letter_topic,omitempty"`
	DelayEnabled                   *bool                `json:"delay_enabled,omitempty"`
	MessageTTLMS                   *uint64              `json:"message_ttl_ms,omitempty"`
	MessageExpiryAction            *MessageExpiryAction `json:"message_expiry_action,omitempty"`
	CompactionEnabled              *bool                `json:"compaction_enabled,omitempty"`
	CompactionTombstoneRetentionMS *uint64              `json:"compaction_tombstone_retention_ms,omitempty"`
}

type CreateTopicResponse struct {
	Topic      string `json:"topic"`
	Partitions uint32 `json:"partitions"`
}

type ProduceRequest struct {
	Topic        string              `json:"topic"`
	Partition    *uint32             `json:"partition,omitempty"`
	Partitioning ProducePartitioning `json:"partitioning"`
	Idempotency  *ProduceIdempotency `json:"idempotency,omitempty"`
	Key          []byte              `json:"key,omitempty"`
	Headers      []MessageHeader     `json:"headers,omitempty"`
	Tombstone    bool                `json:"tombstone,omitempty"`
	ExpiresAtMS  *uint64             `json:"expires_at_ms,omitempty"`
	Payload      []byte              `json:"payload"`
}

type ProduceResponse struct {
	Partition  uint32 `json:"partition"`
	Offset     uint64 `json:"offset"`
	NextOffset uint64 `json:"next_offset"`
}

type ProduceBatchRecord struct {
	Key         []byte          `json:"key,omitempty"`
	Headers     []MessageHeader `json:"headers,omitempty"`
	Tombstone   bool            `json:"tombstone,omitempty"`
	ExpiresAtMS *uint64         `json:"expires_at_ms,omitempty"`
	Payload     []byte          `json:"payload"`
}

type ProduceIdempotency struct {
	ProducerID    uint64 `json:"producer_id"`
	ProducerEpoch uint64 `json:"producer_epoch"`
	BaseSequence  uint64 `json:"base_sequence"`
}

type TransactionRecordMetadata struct {
	TxID          uint64                 `json:"tx_id"`
	ProducerID    uint64                 `json:"producer_id"`
	ProducerEpoch uint64                 `json:"producer_epoch"`
	Sequence      uint64                 `json:"sequence"`
	ControlType   TransactionControlType `json:"control_type,omitempty"`
}

type ProduceBatchRequest struct {
	Topic        string               `json:"topic"`
	Partition    *uint32              `json:"partition,omitempty"`
	Partitioning ProducePartitioning  `json:"partitioning"`
	Idempotency  *ProduceIdempotency  `json:"idempotency,omitempty"`
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

type ProduceBatchesItemRequest struct {
	Topic        string               `json:"topic"`
	Partition    *uint32              `json:"partition,omitempty"`
	Partitioning ProducePartitioning  `json:"partitioning"`
	Idempotency  *ProduceIdempotency  `json:"idempotency,omitempty"`
	Payloads     [][]byte             `json:"payloads,omitempty"`
	Records      []ProduceBatchRecord `json:"records,omitempty"`
}

type ProduceBatchesRequest struct {
	Items []ProduceBatchesItemRequest `json:"items"`
}

type ProduceBatchesItemResponse struct {
	Topic      string `json:"topic"`
	Partition  uint32 `json:"partition"`
	BaseOffset uint64 `json:"base_offset"`
	LastOffset uint64 `json:"last_offset"`
	NextOffset uint64 `json:"next_offset"`
	Appended   int    `json:"appended"`
	Error      string `json:"error,omitempty"`
}

type ProduceBatchesResponse struct {
	Items []ProduceBatchesItemResponse `json:"items"`
}

type FetchRequest struct {
	Consumer   string         `json:"consumer"`
	Topic      string         `json:"topic"`
	Partition  uint32         `json:"partition"`
	Offset     *uint64        `json:"offset,omitempty"`
	MaxRecords int            `json:"max_records"`
	MinRecords *int           `json:"min_records,omitempty"`
	MaxWaitMS  *uint64        `json:"max_wait_ms,omitempty"`
	Isolation  FetchIsolation `json:"isolation,omitempty"`
}

type FetchRecord struct {
	Offset      uint64                     `json:"offset"`
	TimestampMS uint64                     `json:"timestamp_ms"`
	Key         []byte                     `json:"key,omitempty"`
	Headers     []MessageHeader            `json:"headers,omitempty"`
	Tombstone   bool                       `json:"tombstone,omitempty"`
	ExpiresAtMS *uint64                    `json:"expires_at_ms,omitempty"`
	Transaction *TransactionRecordMetadata `json:"transaction,omitempty"`
	Payload     []byte                     `json:"payload"`
}

type FetchResponse struct {
	Topic          string        `json:"topic"`
	Partition      uint32        `json:"partition"`
	Records        []FetchRecord `json:"records"`
	NextOffset     uint64        `json:"next_offset"`
	HighWatermark  *uint64       `json:"high_watermark,omitempty"`
	LowWatermark   *uint64       `json:"low_watermark,omitempty"`
	LogStartOffset uint64        `json:"log_start_offset"`
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
	Isolation  FetchIsolation          `json:"isolation,omitempty"`
}

type FetchBatchItemResponse struct {
	Topic          string        `json:"topic"`
	Partition      uint32        `json:"partition"`
	Records        []FetchRecord `json:"records"`
	NextOffset     uint64        `json:"next_offset"`
	HighWatermark  *uint64       `json:"high_watermark,omitempty"`
	LowWatermark   *uint64       `json:"low_watermark,omitempty"`
	LogStartOffset uint64        `json:"log_start_offset"`
}

type FetchBatchResponse struct {
	Items []FetchBatchItemResponse `json:"items"`
}

type CommitOffsetRequest struct {
	Consumer   string `json:"consumer"`
	Topic      string `json:"topic"`
	Partition  uint32 `json:"partition"`
	NextOffset uint64 `json:"next_offset"`
	Metadata   string `json:"metadata,omitempty"`
}

type CommitOffsetResponse struct {
	Consumer   string `json:"consumer"`
	Topic      string `json:"topic"`
	Partition  uint32 `json:"partition"`
	NextOffset uint64 `json:"next_offset"`
	Metadata   string `json:"metadata,omitempty"`
}

type TopicMetadata struct {
	Topic      string `json:"topic"`
	Partitions uint32 `json:"partitions"`
}

type ListTopicsResponse struct {
	Topics []TopicMetadata `json:"topics"`
}
