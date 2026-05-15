package ech0

import (
	"context"
	"time"

	internalbroker "github.com/lyonbrown4d/ech0/broker"
	"github.com/samber/oops"
)

type RequestOption func(*requestOptions)

type requestOptions struct {
	instanceID   string
	timeout      time.Duration
	pollInterval time.Duration
	partition    *uint32
	headers      []Header
}

type PendingRequest struct {
	Subject       string
	InstanceID    string
	ReplyTo       string
	CorrelationID string
	ExpiresAt     time.Time
	PollInterval  time.Duration
	Message       Message
}

type RequestMessage struct {
	Subject       string
	ReplyTo       string
	CorrelationID string
	SenderID      string
	ExpiresAt     time.Time
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
		Headers:      headersToStore(opts.headers),
	}
}

func requestPartitioning(opts requestOptions) internalbroker.PublishPartitioning {
	if opts.partition != nil {
		return internalbroker.PublishPartitioning{Mode: internalbroker.PartitionExplicit, Partition: *opts.partition}
	}
	return internalbroker.PublishPartitioning{Mode: internalbroker.PartitionRoundRobin}
}

func pendingRequestFromBroker(pending internalbroker.PendingRequest) PendingRequest {
	return PendingRequest{
		Subject:       pending.Subject,
		InstanceID:    pending.InstanceID,
		ReplyTo:       pending.ReplyTo,
		CorrelationID: pending.CorrelationID,
		ExpiresAt:     time.UnixMilli(unixMillis(pending.ExpiresAtMS)),
		PollInterval:  pending.PollInterval,
		Message:       messageFromRecord(pending.Subject, pending.Produce.Partition, pending.Produce.Record),
	}
}

func pendingRequestToBroker(pending PendingRequest) internalbroker.PendingRequest {
	return internalbroker.PendingRequest{
		Subject:       pending.Subject,
		InstanceID:    pending.InstanceID,
		ReplyTo:       pending.ReplyTo,
		CorrelationID: pending.CorrelationID,
		ExpiresAtMS:   timeToUnixMillis(pending.ExpiresAt),
		PollInterval:  pending.PollInterval,
	}
}

func requestFetchResultFromBroker(result internalbroker.RequestPollResult) RequestFetchResult {
	return RequestFetchResult{
		Subject:        result.Subject,
		Partition:      result.Partition,
		Requests:       requestMessagesFromBroker(result.Subject, result.Partition, result.Requests),
		NextOffset:     result.NextOffset,
		HighWatermark:  result.HighWatermark,
		LowWatermark:   result.LowWatermark,
		LogStartOffset: result.LogStartOffset,
	}
}

func requestMessagesFromBroker(subject string, partition uint32, requests []internalbroker.RequestMessage) []RequestMessage {
	if len(requests) == 0 {
		return nil
	}
	out := make([]RequestMessage, 0, len(requests))
	for index := range requests {
		out = append(out, requestMessageFromBroker(subject, partition, requests[index]))
	}
	return out
}

func requestMessageFromBroker(subject string, partition uint32, req internalbroker.RequestMessage) RequestMessage {
	return RequestMessage{
		Subject:       req.Subject,
		ReplyTo:       req.ReplyTo,
		CorrelationID: req.CorrelationID,
		SenderID:      req.SenderID,
		ExpiresAt:     time.UnixMilli(unixMillis(req.ExpiresAtMS)),
		Headers:       headersFromStore(req.Headers),
		Payload:       append([]byte(nil), req.Payload...),
		Message: Message{
			Topic:      subject,
			Partition:  partition,
			Offset:     req.Record.Offset,
			Timestamp:  time.UnixMilli(unixMillis(req.Record.TimestampMS)),
			Headers:    headersFromStore(req.Headers),
			Payload:    append([]byte(nil), req.Payload...),
			NextOffset: req.Record.Offset + 1,
		},
	}
}

func requestMessageToBroker(req RequestMessage) internalbroker.RequestMessage {
	return internalbroker.RequestMessage{
		Subject:       req.Subject,
		ReplyTo:       req.ReplyTo,
		CorrelationID: req.CorrelationID,
		SenderID:      req.SenderID,
		ExpiresAtMS:   timeToUnixMillis(req.ExpiresAt),
		Headers:       headersToStore(req.Headers),
		Payload:       append([]byte(nil), req.Payload...),
	}
}

func replyMessageFromBroker(reply internalbroker.ReplyMessage) ReplyMessage {
	return ReplyMessage{
		Offset:        reply.Offset,
		Subject:       reply.Subject,
		CorrelationID: reply.CorrelationID,
		ResponderID:   reply.SenderID,
		Error:         cloneString(reply.Error),
		Payload:       append([]byte(nil), reply.Payload...),
		Direct: DirectMessage{
			Offset:         reply.Offset,
			MessageID:      reply.Message.MessageID,
			ConversationID: reply.Message.ConversationID,
			Sender:         reply.Message.Sender,
			Recipient:      reply.Message.Recipient,
			Timestamp:      time.UnixMilli(unixMillis(reply.Message.TimestampMS)),
			Payload:        append([]byte(nil), reply.Message.Payload...),
		},
	}
}

func timeToUnixMillis(value time.Time) uint64 {
	if value.IsZero() {
		return 0
	}
	millis := value.UnixMilli()
	if millis <= 0 {
		return 0
	}
	return uint64(millis)
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}
