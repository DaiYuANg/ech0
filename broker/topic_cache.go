package broker

import (
	"errors"

	"github.com/DaiYuANg/ech0/store"
	"github.com/dgraph-io/ristretto/v2"
)

const topicCacheBufferItems = 64

var errTopicCacheDisabled = errors.New("topic cache disabled")

func newTopicConfigCache(maxEntries int64) (*ristretto.Cache[string, store.TopicConfig], error) {
	if maxEntries <= 0 {
		return nil, errTopicCacheDisabled
	}
	cache, err := ristretto.NewCache(&ristretto.Config[string, store.TopicConfig]{
		NumCounters: maxEntries * 10,
		MaxCost:     maxEntries,
		BufferItems: topicCacheBufferItems,
	})
	if err != nil {
		return nil, wrapBroker("topic_cache_create_failed", err, "create topic metadata cache")
	}
	return cache, nil
}

func (b *Broker) topicConfig(name string) (*store.TopicConfig, error) {
	if b.topicCache != nil {
		if cached, ok := b.topicCache.Get(name); ok {
			topic := cached
			return &topic, nil
		}
	}
	topic, err := b.meta.LoadTopicConfig(name)
	if err != nil {
		return nil, wrapBrokerStore(err, "load topic config")
	}
	if topic != nil {
		b.cacheTopicConfig(*topic)
	}
	return topic, nil
}

func (b *Broker) cacheTopicConfig(topic store.TopicConfig) {
	if b.topicCache == nil {
		return
	}
	if b.topicCache.Set(topic.Name, topic, 1) {
		b.topicCache.Wait()
	}
}

func (b *Broker) closeTopicCache() {
	cache := b.topicCache
	if cache != nil {
		b.topicCache = nil
		cache.Close()
	}
}
