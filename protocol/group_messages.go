package protocol

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
	Group             string   `json:"group"`
	MemberID          string   `json:"member_id"`
	Topics            []string `json:"topics"`
	SessionTimeoutMS  uint64   `json:"session_timeout_ms"`
	MaxPollIntervalMS uint64   `json:"max_poll_interval_ms"`
}

type ConsumerGroupMemberLease struct {
	Group             string   `json:"group"`
	MemberID          string   `json:"member_id"`
	Topics            []string `json:"topics"`
	SessionTimeoutMS  uint64   `json:"session_timeout_ms"`
	MaxPollIntervalMS uint64   `json:"max_poll_interval_ms"`
	JoinedAtMS        uint64   `json:"joined_at_ms"`
	LastHeartbeatMS   uint64   `json:"last_heartbeat_ms"`
	LastPollMS        uint64   `json:"last_poll_ms"`
	ExpiresAtMS       uint64   `json:"expires_at_ms"`
	PollExpiresAtMS   uint64   `json:"poll_expires_at_ms"`
}

type JoinConsumerGroupResponse struct {
	Lease ConsumerGroupMemberLease `json:"lease"`
}

type HeartbeatConsumerGroupRequest struct {
	Group             string  `json:"group"`
	MemberID          string  `json:"member_id"`
	SessionTimeoutMS  *uint64 `json:"session_timeout_ms,omitempty"`
	MaxPollIntervalMS *uint64 `json:"max_poll_interval_ms,omitempty"`
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
	Group      string         `json:"group"`
	MemberID   string         `json:"member_id"`
	Generation uint64         `json:"generation"`
	Topic      string         `json:"topic"`
	Partition  uint32         `json:"partition"`
	Offset     *uint64        `json:"offset,omitempty"`
	MaxRecords int            `json:"max_records"`
	MinRecords *int           `json:"min_records,omitempty"`
	MaxWaitMS  *uint64        `json:"max_wait_ms,omitempty"`
	Isolation  FetchIsolation `json:"isolation,omitempty"`
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
	Metadata   string `json:"metadata,omitempty"`
}

type CommitConsumerGroupOffsetResponse struct {
	Group      string `json:"group"`
	MemberID   string `json:"member_id"`
	Generation uint64 `json:"generation"`
	Topic      string `json:"topic"`
	Partition  uint32 `json:"partition"`
	NextOffset uint64 `json:"next_offset"`
	Metadata   string `json:"metadata,omitempty"`
}

type FetchConsumerGroupBatchRequest struct {
	Group      string                  `json:"group"`
	MemberID   string                  `json:"member_id"`
	Generation uint64                  `json:"generation"`
	Items      []FetchBatchItemRequest `json:"items"`
	MinRecords *int                    `json:"min_records,omitempty"`
	MaxWaitMS  *uint64                 `json:"max_wait_ms,omitempty"`
	Isolation  FetchIsolation          `json:"isolation,omitempty"`
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
