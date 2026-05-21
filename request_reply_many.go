package ech0

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	internalbroker "github.com/lyonbrown4d/ech0/broker"
	"github.com/samber/oops"
)

func (b *Broker) RequestMany(ctx context.Context, subject string, payload []byte, maxReplies int, opts ...RequestOption) ([]ReplyMessage, error) {
	pending, err := b.StartRequest(ctx, subject, payload, opts...)
	if err != nil {
		return nil, err
	}
	return b.AwaitReplies(ctx, pending, maxReplies)
}

func (b *Broker) AwaitReplies(ctx context.Context, pending PendingRequest, maxReplies int) ([]ReplyMessage, error) {
	result, err := b.broker.AwaitReplies(ctx, pendingRequestToBroker(pending), maxReplies)
	if err != nil {
		return nil, oops.In("embedded").Code("request_await_many_failed").With("reply_to", pending.ReplyTo).Wrapf(err, "await replies")
	}
	return replyMessagesFromBroker(result), nil
}

func replyMessagesFromBroker(replies []internalbroker.ReplyMessage) []ReplyMessage {
	out := collectionlist.NewListWithCapacity[ReplyMessage](len(replies))
	for index := range replies {
		out.Add(replyMessageFromBroker(replies[index]))
	}
	return out.Values()
}
