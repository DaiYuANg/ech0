package protocol

type StartRequestRequest struct {
	Subject        string              `json:"subject"`
	InstanceID     string              `json:"instance_id"`
	TimeoutMS      *uint64             `json:"timeout_ms,omitempty"`
	PollIntervalMS *uint64             `json:"poll_interval_ms,omitempty"`
	Partition      *uint32             `json:"partition,omitempty"`
	Partitioning   ProducePartitioning `json:"partitioning"`
	ReplyMode      RequestReplyMode    `json:"reply_mode,omitempty"`
	Headers        []MessageHeader     `json:"headers,omitempty"`
	Payload        []byte              `json:"payload"`
}

type StartRequestResponse struct {
	Subject       string `json:"subject"`
	InstanceID    string `json:"instance_id"`
	ReplyTo       string `json:"reply_to"`
	CorrelationID string `json:"correlation_id"`
	ExpiresAtMS   uint64 `json:"expires_at_ms"`
	ReplyMode     string `json:"reply_mode"`
	Partition     uint32 `json:"partition"`
	Offset        uint64 `json:"offset"`
	NextOffset    uint64 `json:"next_offset"`
}

type FetchRequestsRequest struct {
	Consumer   string         `json:"consumer"`
	Subject    string         `json:"subject"`
	Partition  uint32         `json:"partition"`
	Offset     *uint64        `json:"offset,omitempty"`
	MaxRecords int            `json:"max_records"`
	MinRecords *int           `json:"min_records,omitempty"`
	MaxWaitMS  *uint64        `json:"max_wait_ms,omitempty"`
	Isolation  FetchIsolation `json:"isolation,omitempty"`
}

type RequestRecord struct {
	Offset        uint64          `json:"offset"`
	TimestampMS   uint64          `json:"timestamp_ms"`
	Subject       string          `json:"subject"`
	ReplyTo       string          `json:"reply_to"`
	CorrelationID string          `json:"correlation_id"`
	SenderID      string          `json:"sender_id"`
	ExpiresAtMS   uint64          `json:"expires_at_ms"`
	ReplyMode     string          `json:"reply_mode"`
	Headers       []MessageHeader `json:"headers,omitempty"`
	Payload       []byte          `json:"payload"`
}

type FetchRequestsResponse struct {
	Subject        string          `json:"subject"`
	Partition      uint32          `json:"partition"`
	Requests       []RequestRecord `json:"requests"`
	NextOffset     uint64          `json:"next_offset"`
	HighWatermark  *uint64         `json:"high_watermark,omitempty"`
	LowWatermark   *uint64         `json:"low_watermark,omitempty"`
	LogStartOffset uint64          `json:"log_start_offset"`
}

type ReplyRequest struct {
	Subject       string `json:"subject"`
	ReplyTo       string `json:"reply_to"`
	CorrelationID string `json:"correlation_id"`
	ExpiresAtMS   uint64 `json:"expires_at_ms,omitempty"`
	ResponderID   string `json:"responder_id"`
	Payload       []byte `json:"payload"`
}

type ReplyResponse struct {
	MessageID      string `json:"message_id"`
	ConversationID string `json:"conversation_id"`
	Offset         uint64 `json:"offset"`
	NextOffset     uint64 `json:"next_offset"`
}

type ReplyErrorResponse = ReplyResponse

type ReplyErrorRequest struct {
	Subject       string `json:"subject"`
	ReplyTo       string `json:"reply_to"`
	CorrelationID string `json:"correlation_id"`
	ExpiresAtMS   uint64 `json:"expires_at_ms,omitempty"`
	ResponderID   string `json:"responder_id"`
	Error         string `json:"error"`
}

type AwaitReplyRequest struct {
	ReplyTo        string  `json:"reply_to"`
	CorrelationID  string  `json:"correlation_id"`
	ExpiresAtMS    uint64  `json:"expires_at_ms,omitempty"`
	TimeoutMS      *uint64 `json:"timeout_ms,omitempty"`
	PollIntervalMS *uint64 `json:"poll_interval_ms,omitempty"`
}

type AwaitRepliesRequest struct {
	ReplyTo        string  `json:"reply_to"`
	CorrelationID  string  `json:"correlation_id"`
	ExpiresAtMS    uint64  `json:"expires_at_ms,omitempty"`
	TimeoutMS      *uint64 `json:"timeout_ms,omitempty"`
	PollIntervalMS *uint64 `json:"poll_interval_ms,omitempty"`
	MaxReplies     int     `json:"max_replies"`
}

type ReplyRecord struct {
	Offset        uint64  `json:"offset"`
	MessageID     string  `json:"message_id"`
	TimestampMS   uint64  `json:"timestamp_ms"`
	Subject       string  `json:"subject"`
	CorrelationID string  `json:"correlation_id"`
	ResponderID   string  `json:"responder_id"`
	Error         *string `json:"error,omitempty"`
	Payload       []byte  `json:"payload"`
}

type AwaitReplyResponse struct {
	Reply ReplyRecord `json:"reply"`
}

type AwaitRepliesResponse struct {
	Replies []ReplyRecord `json:"replies"`
}
