package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/DaiYuANg/ech0/direct"
	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/cenkalti/backoff/v5"
)

func (b *Broker) Request(ctx context.Context, subject string, payload []byte, opts RequestOptions) (ReplyMessage, error) {
	pending, err := b.StartRequest(ctx, subject, payload, opts)
	if err != nil {
		return ReplyMessage{}, err
	}
	return b.AwaitReply(ctx, pending)
}

func (b *Broker) StartRequest(ctx context.Context, subject string, payload []byte, opts RequestOptions) (PendingRequest, error) {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return PendingRequest{}, brokerStoreError(store.CodeInvalidArgument, "request subject is required")
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
	replyTo := replyRecipient(normalized.InstanceID, correlationID)
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
		Produce:       produced,
	}, nil
}

func (b *Broker) AwaitReply(ctx context.Context, pending PendingRequest) (ReplyMessage, error) {
	if pending.CorrelationID == "" || pending.ReplyTo == "" {
		return ReplyMessage{}, brokerStoreError(store.CodeInvalidArgument, "pending request is missing reply routing fields")
	}
	waitCtx, cancel := context.WithDeadline(ctx, requestDeadline(pending))
	defer cancel()
	pollBackoff := newRequestReplyBackOff(pending.PollInterval)
	for {
		reply, ok, err := b.fetchReplyOnce(waitCtx, pending)
		if err != nil || ok {
			return reply, err
		}
		if err := sleepRequestPoll(waitCtx, pollBackoff.NextBackOff()); err != nil {
			return ReplyMessage{}, err
		}
	}
}

func (b *Broker) FetchRequests(consumer, subject string, partition uint32, offset *uint64, maxRecords int) (RequestPollResult, error) {
	poll, err := b.Fetch(consumer, subject, partition, offset, maxRecords)
	if err != nil {
		return RequestPollResult{}, err
	}
	requests, err := requestMessagesFromRecords(subject, poll.Records)
	if err != nil {
		return RequestPollResult{}, err
	}
	return RequestPollResult{
		Subject:       subject,
		Partition:     partition,
		Requests:      requests,
		NextOffset:    poll.NextOffset,
		HighWatermark: poll.HighWatermark,
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

func (b *Broker) fetchReplyOnce(ctx context.Context, pending PendingRequest) (ReplyMessage, bool, error) {
	inbox, err := b.FetchInbox(pending.ReplyTo, defaultReplyFetchRecords)
	if err != nil {
		return ReplyMessage{}, false, err
	}
	for _, record := range inbox.Records {
		reply, err := replyFromDirect(record.Message)
		if err != nil {
			return ReplyMessage{}, false, err
		}
		if reply.CorrelationID != pending.CorrelationID {
			continue
		}
		reply.Offset = record.Offset
		return reply, true, b.AckDirect(ctx, pending.ReplyTo, record.Offset+1)
	}
	return ReplyMessage{}, false, nil
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
	return opts, nil
}

func requestDeadline(pending PendingRequest) time.Time {
	now := time.Now()
	if pending.ExpiresAtMS <= store.NowMS() {
		return now
	}
	return time.UnixMilli(safeUint64ToInt64(pending.ExpiresAtMS))
}

func sleepRequestPoll(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return wrapBroker("request_reply_timeout", ctx.Err(), "wait for request reply")
	case <-timer.C:
		return nil
	}
}

func newRequestReplyBackOff(interval time.Duration) *backoff.ConstantBackOff {
	if interval <= 0 {
		interval = defaultRequestPollInterval
	}
	return backoff.NewConstantBackOff(interval)
}

func newCorrelationID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", wrapBroker("request_correlation_id_failed", err, "generate request correlation id")
	}
	return hex.EncodeToString(raw[:]), nil
}

func replyRecipient(instanceID, correlationID string) string {
	return "__reply/" + instanceID + "/" + correlationID
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
