package broker

type TopicSummary struct {
	Name                           string  `json:"name"`
	Partitions                     uint32  `json:"partitions"`
	SegmentMaxBytes                uint64  `json:"segment_max_bytes"`
	IndexIntervalBytes             uint64  `json:"index_interval_bytes"`
	RetentionMaxBytes              uint64  `json:"retention_max_bytes"`
	CleanupPolicy                  string  `json:"cleanup_policy"`
	RetentionMS                    *uint64 `json:"retention_ms,omitempty"`
	CompactionTombstoneRetentionMS *uint64 `json:"compaction_tombstone_retention_ms,omitempty"`
	MaxMessageBytes                uint32  `json:"max_message_bytes"`
	MaxBatchBytes                  uint32  `json:"max_batch_bytes"`
	RetryMaxAttempts               uint32  `json:"retry_max_attempts"`
	DeadLetterTopic                *string `json:"dead_letter_topic,omitempty"`
	DelayEnabled                   bool    `json:"delay_enabled"`
	CompactionEnabled              bool    `json:"compaction_enabled"`
	ProducedRecordsTotal           uint64  `json:"produced_records_total"`
	HottestPartition               *uint32 `json:"hottest_partition,omitempty"`
	HottestPartitionRecords        uint64  `json:"hottest_partition_records"`
	TotalBacklogRecords            uint64  `json:"total_backlog_records"`
	MaxPartitionBacklog            uint64  `json:"max_partition_backlog"`
}

type TopicMessageSummary struct {
	Offset             uint64  `json:"offset"`
	TimestampMS        uint64  `json:"timestamp_ms"`
	PayloadSize        int     `json:"payload_size"`
	PayloadUTF8Preview string  `json:"payload_utf8_preview"`
	PayloadHexPreview  string  `json:"payload_hex_preview"`
	PayloadJSONPreview *string `json:"payload_json_preview,omitempty"`
}

type TopicMessagesPageSummary struct {
	Topic         string                `json:"topic"`
	Partition     uint32                `json:"partition"`
	Offset        uint64                `json:"offset"`
	Limit         int                   `json:"limit"`
	Cursor        string                `json:"cursor,omitempty"`
	NextOffset    uint64                `json:"next_offset"`
	NextCursor    string                `json:"next_cursor,omitempty"`
	HasMore       bool                  `json:"has_more"`
	HighWatermark *uint64               `json:"high_watermark,omitempty"`
	Records       []TopicMessageSummary `json:"records"`
}

type GroupMemberSummary struct {
	Group            string   `json:"group"`
	MemberID         string   `json:"member_id"`
	Topics           []string `json:"topics"`
	SessionTimeoutMS uint64   `json:"session_timeout_ms"`
	JoinedAtMS       uint64   `json:"joined_at_ms"`
	LastHeartbeatMS  uint64   `json:"last_heartbeat_ms"`
	ExpiresAtMS      uint64   `json:"expires_at_ms"`
}

type GroupPartitionOwnerSummary struct {
	MemberID  string `json:"member_id"`
	Topic     string `json:"topic"`
	Partition uint32 `json:"partition"`
}

type GroupAssignmentSummary struct {
	Group       string                       `json:"group"`
	Generation  uint64                       `json:"generation"`
	Assignments []GroupPartitionOwnerSummary `json:"assignments"`
	UpdatedAtMS uint64                       `json:"updated_at_ms"`
}

type GroupPartitionLagSummary struct {
	MemberID            string  `json:"member_id"`
	Topic               string  `json:"topic"`
	Partition           uint32  `json:"partition"`
	CommittedNextOffset uint64  `json:"committed_next_offset"`
	HighWatermark       *uint64 `json:"high_watermark,omitempty"`
	BacklogRecords      uint64  `json:"backlog_records"`
	LagRecords          uint64  `json:"lag_records"`
}

type GroupLagSummary struct {
	Group               string                     `json:"group"`
	Generation          uint64                     `json:"generation"`
	TotalBacklogRecords uint64                     `json:"total_backlog_records"`
	TotalLagRecords     uint64                     `json:"total_lag_records"`
	Partitions          []GroupPartitionLagSummary `json:"partitions"`
}

type GroupMemberLoadSummary struct {
	MemberID   string `json:"member_id"`
	Partitions int    `json:"partitions"`
}

type GroupRebalanceExplainSummary struct {
	Group             string                   `json:"group"`
	NextGeneration    uint64                   `json:"next_generation"`
	Strategy          string                   `json:"strategy"`
	StickyAssignments bool                     `json:"sticky_assignments"`
	ActiveMembers     int                      `json:"active_members"`
	TotalAssignments  int                      `json:"total_assignments"`
	MovedPartitions   uint64                   `json:"moved_partitions"`
	StickyCandidates  uint64                   `json:"sticky_candidates"`
	StickyApplied     uint64                   `json:"sticky_applied"`
	MemberLoads       []GroupMemberLoadSummary `json:"member_loads"`
}

type StreamMetricsSnapshot struct {
	TopicCount                       int    `json:"topic_count"`
	TopicsWithBacklog                int    `json:"topics_with_backlog"`
	TotalTopicBacklogRecords         uint64 `json:"total_topic_backlog_records"`
	MaxTopicBacklogRecords           uint64 `json:"max_topic_backlog_records"`
	MaxPartitionBacklogRecords       uint64 `json:"max_partition_backlog_records"`
	ConsumerGroupCount               int    `json:"consumer_group_count"`
	ConsumerGroupsWithLag            int    `json:"consumer_groups_with_lag"`
	TotalConsumerGroupBacklogRecords uint64 `json:"total_consumer_group_backlog_records"`
	TotalConsumerGroupLagRecords     uint64 `json:"total_consumer_group_lag_records"`
	MaxConsumerGroupLagRecords       uint64 `json:"max_consumer_group_lag_records"`
}
