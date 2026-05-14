package broker

import "github.com/lyonbrown4d/ech0/store"

func (b *Broker) pausedPollResult(consumer, topic string, partition uint32, offset *uint64) (store.PollResult, bool, error) {
	paused, err := b.isConsumerPaused(consumer, topic, partition)
	if err != nil || !paused {
		return store.PollResult{}, false, err
	}
	nextOffset, err := b.fetchStartOffset(consumer, topic, partition, offset)
	if err != nil {
		return store.PollResult{}, false, err
	}
	highWatermark, err := b.queue.LastOffset(store.NewTopicPartition(topic, partition))
	if err != nil {
		return store.PollResult{}, false, wrapBrokerStore(err, "load paused fetch high watermark")
	}
	return store.PollResult{NextOffset: nextOffset, HighWatermark: highWatermark}, true, nil
}

func (b *Broker) isConsumerPaused(consumer, topic string, partition uint32) (bool, error) {
	state, err := b.meta.LoadConsumerPause(consumer, store.NewTopicPartition(topic, partition))
	if err != nil {
		return false, wrapBrokerStore(err, "load consumer pause")
	}
	return state != nil && state.Paused, nil
}
