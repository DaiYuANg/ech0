package broker

import (
	"time"

	"github.com/lyonbrown4d/ech0/direct"
	"github.com/lyonbrown4d/ech0/store"
)

const (
	requestEnvelopeType = "ech0.request.v1"
	replyEnvelopeType   = "ech0.reply.v1"

	defaultRequestTimeout      = 5 * time.Second
	defaultRequestPollInterval = 5 * time.Millisecond
	defaultReplyFetchRecords   = 16
	replyCursorConsumerPrefix  = "__direct_reply/"
)

type RequestReplyMode string

const (
	RequestReplyModeFirstResponseWins RequestReplyMode = "first_response_wins"
	RequestReplyModeMultiReplier      RequestReplyMode = "multi_replier"
)

type RequestOptions struct {
	InstanceID   string
	Timeout      time.Duration
	PollInterval time.Duration
	Partitioning PublishPartitioning
	ReplyMode    RequestReplyMode
	Headers      []store.RecordHeader
}

type PendingRequest struct {
	Subject       string
	InstanceID    string
	ReplyTo       string
	CorrelationID string
	ExpiresAtMS   uint64
	PollInterval  time.Duration
	ReplyMode     RequestReplyMode
	Produce       ProduceResult
}

type RequestMessage struct {
	Subject       string
	ReplyTo       string
	CorrelationID string
	SenderID      string
	ExpiresAtMS   uint64
	ReplyMode     RequestReplyMode
	Headers       []store.RecordHeader
	Payload       []byte
	Record        store.Record
}

type ReplyMessage struct {
	Offset        uint64
	Subject       string
	CorrelationID string
	SenderID      string
	Error         *string
	Payload       []byte
	Message       direct.Message
}

type RequestPollResult struct {
	Subject        string
	Partition      uint32
	Requests       []RequestMessage
	NextOffset     uint64
	HighWatermark  *uint64
	LowWatermark   *uint64
	LogStartOffset uint64
}

type RequestReplyCleanupResult struct {
	RemovedCursors int
}

func (m RequestMessage) ExpiredAt(nowMS uint64) bool {
	return m.ExpiresAtMS > 0 && nowMS >= m.ExpiresAtMS
}
