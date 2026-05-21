package broker

import (
	"context"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/cenkalti/backoff/v5"
	"github.com/lyonbrown4d/ech0/direct"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) RequestMany(ctx context.Context, subject string, payload []byte, maxReplies int, opts RequestOptions) ([]ReplyMessage, error) {
	pending, err := b.StartRequest(ctx, subject, payload, opts)
	if err != nil {
		return nil, err
	}
	return b.AwaitReplies(ctx, pending, maxReplies)
}

func (b *Broker) AwaitReply(ctx context.Context, pending PendingRequest) (ReplyMessage, error) {
	replies, err := b.AwaitReplies(ctx, pending, 1)
	if err != nil {
		return ReplyMessage{}, err
	}
	if len(replies) == 0 {
		return ReplyMessage{}, brokerStoreError(store.CodeUnavailable, "request %s completed without a reply", pending.CorrelationID)
	}
	return replies[0], nil
}

func (b *Broker) AwaitReplies(ctx context.Context, pending PendingRequest, maxReplies int) ([]ReplyMessage, error) {
	identity, err := b.validateAwaitReplies(ctx, pending, maxReplies)
	if err != nil {
		return nil, err
	}
	waitCtx, cancel := context.WithDeadline(ctx, requestDeadline(pending))
	defer cancel()

	pollBackoff := newRequestReplyBackOff(pending.PollInterval)
	replies := collectionlist.NewListWithCapacity[ReplyMessage](maxReplies)
	found := 0
	for found < maxReplies {
		reply, ok, err := b.fetchReplyOnce(waitCtx, identity, pending)
		if err != nil {
			return nil, err
		}
		if ok {
			replies.Add(reply)
			found++
			continue
		}
		if err := sleepRequestPoll(waitCtx, pollBackoff.NextBackOff()); err != nil {
			return awaitRepliesResult(replies, found, err)
		}
	}
	return replies.Values(), nil
}

func (b *Broker) validateAwaitReplies(ctx context.Context, pending PendingRequest, maxReplies int) (Identity, error) {
	if pending.CorrelationID == "" || pending.ReplyTo == "" {
		return Identity{}, brokerStoreError(store.CodeInvalidArgument, "pending request is missing reply routing fields")
	}
	if maxReplies <= 0 {
		return Identity{}, brokerStoreError(store.CodeInvalidArgument, "max_replies must be greater than zero")
	}
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionConsume, directInboxResource(identity, pending.ReplyTo)); err != nil {
		return Identity{}, err
	}
	if err := b.authorize(ctx, identity, ACLActionCommit, directInboxResource(identity, pending.ReplyTo)); err != nil {
		return Identity{}, err
	}
	return identity, nil
}

func awaitRepliesResult(replies *collectionlist.List[ReplyMessage], found int, err error) ([]ReplyMessage, error) {
	if found > 0 {
		return replies.Values(), nil
	}
	return nil, err
}

func (b *Broker) fetchReplyOnce(ctx context.Context, identity Identity, pending PendingRequest) (ReplyMessage, bool, error) {
	inbox, err := b.direct.FetchInboxForConsumer(
		replyInboxConsumer(identity, pending),
		scopedName(identity, "direct", pending.ReplyTo),
		defaultReplyFetchRecords,
	)
	if err != nil {
		return ReplyMessage{}, false, wrapBroker("reply_inbox_fetch_failed", err, "fetch reply inbox")
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
		return reply, true, b.ackReplyInbox(ctx, identity, pending, record.Offset+1)
	}
	if len(inbox.Records) > 0 {
		return ReplyMessage{}, false, b.ackReplyInbox(ctx, identity, pending, inbox.NextOffset)
	}
	return ReplyMessage{}, false, nil
}

func (b *Broker) ackReplyInbox(ctx context.Context, identity Identity, pending PendingRequest, nextOffset uint64) error {
	req := ackDirectCommand{
		Recipient:  scopedName(identity, "direct", pending.ReplyTo),
		Consumer:   replyInboxConsumer(identity, pending),
		NextOffset: nextOffset,
	}
	_, err := routePartitionCommand(ctx, b, topicCommandTarget(direct.InternalInboxTopicPrefix), raftCommandDirectAck, req, b.applyDirectAck)
	return err
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

func replyInboxConsumer(identity Identity, pending PendingRequest) string {
	name := pending.ReplyTo + "/" + pending.CorrelationID
	if defaultScope(identity) {
		return replyCursorConsumerPrefix + name
	}
	return scopedName(identity, "direct_reply", name)
}
