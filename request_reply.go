package ech0

import (
	"context"
	"time"

	internalbroker "github.com/lyonbrown4d/ech0/broker"
	"github.com/samber/oops"
)

type RequestOption func(*requestOptions)

type RequestReplyMode = internalbroker.RequestReplyMode

const (
	RequestReplyModeFirstResponseWins RequestReplyMode = internalbroker.RequestReplyModeFirstResponseWins
	RequestReplyModeMultiReplier      RequestReplyMode = internalbroker.RequestReplyModeMultiReplier
)

type requestOptions struct {
	instanceID   string
	timeout      time.Duration
	pollInterval time.Duration
	partition    *uint32
	replyMode    RequestReplyMode
	headers      []Header
}

type PendingRequest struct {
	Subject       string
	InstanceID    string
	ReplyTo       string
	CorrelationID string
	ExpiresAt     time.Time
	PollInterval  time.Duration
	ReplyMode     RequestReplyMode
	Message       Message
}

type RequestMessage struct {
	Subject       string
	ReplyTo       string
	CorrelationID string
	SenderID      string
	ExpiresAt     time.Time
	ReplyMode     RequestReplyMode
	Headers       []Header
	Payload       []byte
	Message       Message
}

type RequestFetchResult struct {
	Subject        string
	Partition      uint32
	Requests       []RequestMessage
	NextOffset     uint64
	HighWatermark  *uint64
	LowWatermark   *uint64
	LogStartOffset uint64
}

type ReplyMessage struct {
	Offset        uint64
	Subject       string
	CorrelationID string
	ResponderID   string
	Error         *string
	Payload       []byte
	Direct        DirectMessage
}

func RequestInstance(id string) RequestOption {
	return func(opts *requestOptions) {
		opts.instanceID = id
	}
}

func RequestTimeout(timeout time.Duration) RequestOption {
	return func(opts *requestOptions) {
		if timeout > 0 {
			opts.timeout = timeout
		}
	}
}

func RequestPollInterval(interval time.Duration) RequestOption {
	return func(opts *requestOptions) {
		if interval > 0 {
			opts.pollInterval = interval
		}
	}
}

func RequestPartition(partition uint32) RequestOption {
	return func(opts *requestOptions) {
		opts.partition = &partition
	}
}

func RequestHeader(key string, value []byte) RequestOption {
	return func(opts *requestOptions) {
		opts.headers = append(opts.headers, Header{Key: key, Value: append([]byte(nil), value...)})
	}
}

func RequestMode(mode RequestReplyMode) RequestOption {
	return func(opts *requestOptions) {
		opts.replyMode = mode
	}
}

func (b *Broker) Request(ctx context.Context, subject string, payload []byte, opts ...RequestOption) (ReplyMessage, error) {
	pending, err := b.StartRequest(ctx, subject, payload, opts...)
	if err != nil {
		return ReplyMessage{}, err
	}
	return b.AwaitReply(ctx, pending)
}

func (b *Broker) StartRequest(ctx context.Context, subject string, payload []byte, opts ...RequestOption) (PendingRequest, error) {
	requestOpts := collectRequestOptions(opts)
	result, err := b.broker.StartRequest(ctx, subject, payload, requestOptionsToBroker(requestOpts))
	if err != nil {
		return PendingRequest{}, oops.In("embedded").Code("request_start_failed").With("subject", subject).Wrapf(err, "start request")
	}
	return pendingRequestFromBroker(result), nil
}

func (b *Broker) AwaitReply(ctx context.Context, pending PendingRequest) (ReplyMessage, error) {
	result, err := b.broker.AwaitReply(ctx, pendingRequestToBroker(pending))
	if err != nil {
		return ReplyMessage{}, oops.In("embedded").Code("request_await_failed").With("reply_to", pending.ReplyTo).Wrapf(err, "await reply")
	}
	return replyMessageFromBroker(result), nil
}

func (b *Broker) FetchRequests(ctx context.Context, consumer, subject string, opts ...FetchOption) (RequestFetchResult, error) {
	fetchOpts := fetchOptions{maxRecords: 100}
	for _, opt := range opts {
		if opt != nil {
			opt(&fetchOpts)
		}
	}
	result, err := b.broker.FetchRequestsWithIsolation(ctx, consumer, subject, fetchOpts.partition, fetchOpts.offset, fetchOpts.maxRecords, fetchOpts.isolation)
	if err != nil {
		return RequestFetchResult{}, oops.In("embedded").Code("request_fetch_failed").With("subject", subject).Wrapf(err, "fetch requests")
	}
	return requestFetchResultFromBroker(result), nil
}

func (b *Broker) Reply(ctx context.Context, req RequestMessage, responderID string, payload []byte) (DirectSendResult, error) {
	result, err := b.broker.Reply(ctx, requestMessageToBroker(req), responderID, payload)
	if err != nil {
		return DirectSendResult{}, oops.In("embedded").Code("reply_failed").With("subject", req.Subject).Wrapf(err, "send reply")
	}
	return directSendResultFromBroker(result), nil
}

func (b *Broker) ReplyError(ctx context.Context, req RequestMessage, responderID, errorMessage string) (DirectSendResult, error) {
	result, err := b.broker.ReplyError(ctx, requestMessageToBroker(req), responderID, errorMessage)
	if err != nil {
		return DirectSendResult{}, oops.In("embedded").Code("reply_error_failed").With("subject", req.Subject).Wrapf(err, "send reply error")
	}
	return directSendResultFromBroker(result), nil
}

func collectRequestOptions(opts []RequestOption) requestOptions {
	out := requestOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&out)
		}
	}
	return out
}

func requestOptionsToBroker(opts requestOptions) internalbroker.RequestOptions {
	return internalbroker.RequestOptions{
		InstanceID:   opts.instanceID,
		Timeout:      opts.timeout,
		PollInterval: opts.pollInterval,
		Partitioning: requestPartitioning(opts),
		ReplyMode:    opts.replyMode,
		Headers:      headersToStore(opts.headers),
	}
}

func requestPartitioning(opts requestOptions) internalbroker.PublishPartitioning {
	if opts.partition != nil {
		return internalbroker.PublishPartitioning{Mode: internalbroker.PartitionExplicit, Partition: *opts.partition}
	}
	return internalbroker.PublishPartitioning{Mode: internalbroker.PartitionRoundRobin}
}
