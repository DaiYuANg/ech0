package ech0

import (
	"context"
	"time"

	"github.com/lyonbrown4d/ech0/store"
	"github.com/samber/oops"
)

type CommittedOffset struct {
	Topic      string
	Partition  uint32
	NextOffset uint64
	Metadata   string
	UpdatedAt  time.Time
}

func (b *Broker) AckWithMetadata(ctx context.Context, consumer string, msg Message, metadata string) error {
	return b.CommitWithMetadata(ctx, consumer, msg.Topic, msg.Partition, msg.NextOffset, metadata)
}

func (b *Broker) CommitWithMetadata(ctx context.Context, consumer, topic string, partition uint32, nextOffset uint64, metadata string) error {
	return oops.In("embedded").Code("commit_failed").With("consumer", consumer, "topic", topic).Wrapf(
		b.broker.CommitOffsetWithMetadata(ctx, consumer, topic, partition, nextOffset, metadata),
		"commit offset",
	)
}

func (b *Broker) CommittedOffset(ctx context.Context, consumer, topic string, partition uint32) (*CommittedOffset, error) {
	state, err := b.broker.CommittedOffset(ctx, consumer, topic, partition)
	if err != nil {
		return nil, oops.In("embedded").Code("committed_offset_failed").With("consumer", consumer, "topic", topic).Wrapf(err, "load committed offset")
	}
	return committedOffsetFromStore(state), nil
}

func committedOffsetFromStore(state *store.ConsumerOffsetState) *CommittedOffset {
	if state == nil {
		return nil
	}
	return &CommittedOffset{
		Topic:      state.Topic,
		Partition:  state.Partition,
		NextOffset: state.NextOffset,
		Metadata:   state.Metadata,
		UpdatedAt:  time.UnixMilli(unixMillis(state.UpdatedAtMS)),
	}
}
