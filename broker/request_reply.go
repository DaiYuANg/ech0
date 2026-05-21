package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/direct"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) Request(ctx context.Context, subject string, payload []byte, opts RequestOptions) (ReplyMessage, error) {
	pending, err := b.StartRequest(ctx, subject, payload, opts)
	if err != nil {
		return ReplyMessage{}, err
	}
	return b.AwaitReply(ctx, pending)
}

func (b *Broker) StartRequest(ctx context.Context, subject string, payload []byte, opts RequestOptions) (PendingRequest, error) {
	identity := b.identity(ctx)
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return PendingRequest{}, brokerStoreError(store.CodeInvalidArgument, "request subject is required")
	}
	if err := b.checkQuota(ctx, QuotaRequest{Identity: identity, Action: QuotaActionRequest, Topic: subject, Records: 1, Bytes: len(payload)}); err != nil {
		return PendingRequest{}, err
	}
	normalized, err := normalizeRequestOptions(opts)
	if err != nil {
		return PendingRequest{}, err
	}
	correlationID, err := newCorrelationID()
	if err != nil {
		return PendingRequest{}, err
	}
	timeoutMS := durationMillis(normalized.Timeout)
	expiresAtMS := store.NowMS() + timeoutMS
	replyTo := replyRecipient(normalized.InstanceID)
	requestPayload, err := encodeRequestPayload(subject, normalized, correlationID, replyTo, expiresAtMS, payload)
	if err != nil {
		return PendingRequest{}, err
	}
	produced, err := b.Publish(ctx, subject, normalized.Partitioning, nil, false, requestPayload)
	if err != nil {
		return PendingRequest{}, err
	}
	return PendingRequest{
		Subject:       subject,
		InstanceID:    normalized.InstanceID,
		ReplyTo:       replyTo,
		CorrelationID: correlationID,
		ExpiresAtMS:   expiresAtMS,
		PollInterval:  normalized.PollInterval,
		ReplyMode:     normalized.ReplyMode,
		Produce:       produced,
	}, nil
}

func (b *Broker) FetchRequests(ctx context.Context, consumer, subject string, partition uint32, offset *uint64, maxRecords int) (RequestPollResult, error) {
	return b.FetchRequestsWithIsolation(ctx, consumer, subject, partition, offset, maxRecords, FetchIsolationReadUncommitted)
}

func (b *Broker) FetchRequestsWithIsolation(
	ctx context.Context,
	consumer string,
	subject string,
	partition uint32,
	offset *uint64,
	maxRecords int,
	isolation FetchIsolation,
) (RequestPollResult, error) {
	poll, err := b.FetchWithIsolation(ctx, consumer, subject, partition, offset, maxRecords, isolation)
	if err != nil {
		return RequestPollResult{}, err
	}
	requests, err := requestMessagesFromRecords(subject, poll.Records)
	if err != nil {
		return RequestPollResult{}, err
	}
	return RequestPollResult{
		Subject:        subject,
		Partition:      partition,
		Requests:       requests,
		NextOffset:     poll.NextOffset,
		HighWatermark:  poll.HighWatermark,
		LowWatermark:   poll.LowWatermark,
		LogStartOffset: poll.LogStartOffset,
	}, nil
}

func (b *Broker) Reply(ctx context.Context, req RequestMessage, responderID string, payload []byte) (direct.SendResult, error) {
	return b.reply(ctx, req, responderID, payload, nil)
}

func (b *Broker) ReplyError(ctx context.Context, req RequestMessage, responderID, errorMessage string) (direct.SendResult, error) {
	return b.reply(ctx, req, responderID, nil, &errorMessage)
}

func (b *Broker) reply(ctx context.Context, req RequestMessage, responderID string, payload []byte, errorMessage *string) (direct.SendResult, error) {
	if req.ReplyTo == "" || req.CorrelationID == "" {
		return direct.SendResult{}, brokerStoreError(store.CodeInvalidArgument, "request message is missing reply routing fields")
	}
	if req.ExpiredAt(store.NowMS()) {
		return direct.SendResult{}, brokerStoreError(store.CodeUnavailable, "request %s already expired", req.CorrelationID)
	}
	replyPayload, err := encodeReplyPayload(req, responderID, payload, errorMessage)
	if err != nil {
		return direct.SendResult{}, err
	}
	conversationID := req.CorrelationID
	return b.SendDirect(ctx, responderID, req.ReplyTo, &conversationID, replyPayload)
}

func requestMessagesFromRecords(subject string, records []store.Record) ([]RequestMessage, error) {
	requests := collectionlist.NewListWithCapacity[RequestMessage](len(records))
	for _, record := range records {
		request, err := requestFromRecord(subject, record)
		if err != nil {
			return nil, err
		}
		requests.Add(request)
	}
	return requests.Values(), nil
}

func normalizeRequestOptions(opts RequestOptions) (RequestOptions, error) {
	opts.InstanceID = strings.TrimSpace(opts.InstanceID)
	if opts.InstanceID == "" {
		return RequestOptions{}, brokerStoreError(store.CodeInvalidArgument, "request instance_id is required")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaultRequestTimeout
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = defaultRequestPollInterval
	}
	if opts.Partitioning.Mode == "" {
		opts.Partitioning = PublishPartitioning{Mode: PartitionRoundRobin}
	}
	mode, err := normalizeRequestReplyMode(opts.ReplyMode)
	if err != nil {
		return RequestOptions{}, err
	}
	opts.ReplyMode = mode
	return opts, nil
}

func normalizeRequestReplyMode(mode RequestReplyMode) (RequestReplyMode, error) {
	value := RequestReplyMode(strings.TrimSpace(string(mode)))
	switch value {
	case "":
		return RequestReplyModeFirstResponseWins, nil
	case RequestReplyModeFirstResponseWins, RequestReplyModeMultiReplier:
		return value, nil
	default:
		return "", brokerStoreError(store.CodeInvalidArgument, "unsupported request reply mode %q", value)
	}
}

func requestReplyModeFromString(mode string) RequestReplyMode {
	normalized, err := normalizeRequestReplyMode(RequestReplyMode(mode))
	if err != nil {
		return RequestReplyModeFirstResponseWins
	}
	return normalized
}

func newCorrelationID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", wrapBroker("request_correlation_id_failed", err, "generate request correlation id")
	}
	return hex.EncodeToString(raw[:]), nil
}

func replyRecipient(instanceID string) string {
	return "__reply/" + instanceID
}

func durationMillis(duration time.Duration) uint64 {
	millis := duration.Milliseconds()
	if millis <= 0 {
		return 1
	}
	out, err := strconv.ParseUint(strconv.FormatInt(millis, 10), 10, 64)
	if err != nil {
		return 1
	}
	return out
}
