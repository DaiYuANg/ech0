package broker

import (
	"context"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/direct"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) CleanupRequestRepliesOnce(ctx context.Context) (RequestReplyCleanupResult, error) {
	return b.CleanupRequestReplies(ctx, store.NowMS())
}

func (b *Broker) CleanupRequestReplies(ctx context.Context, nowMS uint64) (RequestReplyCleanupResult, error) {
	states, err := b.meta.ListConsumerOffsetStates()
	if err != nil {
		return RequestReplyCleanupResult{}, wrapBrokerStore(err, "list reply cursor offsets")
	}
	requests := staleReplyCursorDeleteCommands(states, nowMS, b.cfg.Broker.RequestReplyCursorTTLMS)
	if len(requests) == 0 {
		return RequestReplyCleanupResult{}, nil
	}
	result, err := routeMetadataCommand(
		ctx,
		b,
		raftCommandDeleteConsumerOffsets,
		deleteConsumerOffsetsCommand{Requests: requests},
		b.applyDeleteConsumerOffsets,
	)
	if err != nil {
		return RequestReplyCleanupResult{}, wrapBroker("request_reply_cleanup_failed", err, "cleanup request reply cursors")
	}
	return RequestReplyCleanupResult{RemovedCursors: result.Deleted}, nil
}

func staleReplyCursorDeleteCommands(states []store.ConsumerOffsetState, nowMS, ttlMS uint64) []deleteConsumerOffsetCommand {
	out := collectionlist.NewList[deleteConsumerOffsetCommand]()
	for index := range states {
		state := states[index]
		if !isReplyCursorOffset(state) || !replyCursorExpired(state, nowMS, ttlMS) {
			continue
		}
		out.Add(deleteConsumerOffsetCommand{
			Consumer:  state.Consumer,
			Topic:     state.Topic,
			Partition: state.Partition,
		})
	}
	return out.Values()
}

func isReplyCursorOffset(state store.ConsumerOffsetState) bool {
	return direct.IsInternalTopicName(state.Topic) && isReplyCursorConsumer(state.Consumer)
}

func isReplyCursorConsumer(consumer string) bool {
	return strings.HasPrefix(consumer, replyCursorConsumerPrefix) || strings.Contains(consumer, "/direct_reply/")
}

func replyCursorExpired(state store.ConsumerOffsetState, nowMS, ttlMS uint64) bool {
	if state.UpdatedAtMS == 0 {
		return true
	}
	if ttlMS == 0 {
		return false
	}
	return nowMS >= state.UpdatedAtMS && nowMS-state.UpdatedAtMS >= ttlMS
}
