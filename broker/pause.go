package broker

import (
	"context"
	"strings"

	"github.com/lyonbrown4d/ech0/store"
)

type PauseResult struct {
	Consumer    string
	Topic       string
	Partition   uint32
	Paused      bool
	UpdatedAtMS uint64
}

func (b *Broker) PauseConsumer(ctx context.Context, consumer, topic string, partition uint32) (PauseResult, error) {
	return b.setConsumerPause(ctx, consumer, topic, partition, true)
}

func (b *Broker) ResumeConsumer(ctx context.Context, consumer, topic string, partition uint32) (PauseResult, error) {
	return b.setConsumerPause(ctx, consumer, topic, partition, false)
}

func (b *Broker) PauseConsumerGroup(ctx context.Context, group, memberID string, generation uint64, topic string, partition uint32) (PauseResult, error) {
	return b.setConsumerGroupPause(ctx, group, memberID, generation, topic, partition, true)
}

func (b *Broker) ResumeConsumerGroup(ctx context.Context, group, memberID string, generation uint64, topic string, partition uint32) (PauseResult, error) {
	return b.setConsumerGroupPause(ctx, group, memberID, generation, topic, partition, false)
}

func (b *Broker) setConsumerPause(ctx context.Context, consumer, topic string, partition uint32, paused bool) (PauseResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionCommit, topicResource(identity, topic)); err != nil {
		return PauseResult{}, err
	}
	scopedTopic := scopedTopicName(identity, topic)
	result, err := b.setPauseScoped(ctx, scopedName(identity, "consumer", consumer), scopedTopic, partition, paused)
	if err != nil {
		return PauseResult{}, err
	}
	return b.visiblePauseResult(identity, result), nil
}

func (b *Broker) setConsumerGroupPause(ctx context.Context, group, memberID string, generation uint64, topic string, partition uint32, paused bool) (PauseResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionCommit, groupResource(identity, group)); err != nil {
		return PauseResult{}, err
	}
	scopedGroup := scopedName(identity, "group", group)
	scopedTopic := scopedTopicName(identity, topic)
	if err := b.validateConsumerGroupLease(scopedGroup, memberID, generation, store.NewTopicPartition(scopedTopic, partition)); err != nil {
		return PauseResult{}, err
	}
	result, err := b.setPauseScoped(ctx, groupConsumer(scopedGroup), scopedTopic, partition, paused)
	if err != nil {
		return PauseResult{}, err
	}
	return b.visiblePauseResult(identity, result), nil
}

func (b *Broker) setPauseScoped(ctx context.Context, consumer, topic string, partition uint32, paused bool) (PauseResult, error) {
	if err := b.validateSeekTopicPartition(topic, partition); err != nil {
		return PauseResult{}, err
	}
	req := consumerPauseCommand{
		Consumer:    consumer,
		Topic:       topic,
		Partition:   partition,
		Paused:      paused,
		UpdatedAtMS: store.NowMS(),
	}
	value, err := routePartitionCommand(ctx, b, exactPartitionCommandTarget(topic, partition), raftCommandConsumerPause, req, b.applyConsumerPause)
	if err != nil {
		return PauseResult{}, err
	}
	return raftValueAs[PauseResult](value)
}

func (b *Broker) applyConsumerPause(_ context.Context, req consumerPauseCommand) (PauseResult, error) {
	state := store.ConsumerPauseState(req)
	if req.Paused {
		if err := b.meta.SaveConsumerPause(state); err != nil {
			return PauseResult{}, wrapBrokerStore(err, "save consumer pause")
		}
	} else if err := b.meta.DeleteConsumerPause(req.Consumer, state.TopicPartition()); err != nil {
		return PauseResult{}, wrapBrokerStore(err, "delete consumer pause")
	}
	return PauseResult(req), nil
}

func (b *Broker) visiblePauseResult(identity Identity, result PauseResult) PauseResult {
	result.Topic = visibleTopicName(identity, result.Topic)
	result.Consumer = visiblePauseConsumerName(identity, result.Consumer)
	return result
}

func visiblePauseConsumerName(identity Identity, consumer string) string {
	group, ok := strings.CutPrefix(consumer, "__group_offset/")
	if ok {
		return "__group_offset/" + visibleName(identity, "group", group)
	}
	return visibleName(identity, "consumer", consumer)
}
