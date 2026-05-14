package broker

import (
	"errors"
	"log/slog"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/ech0/queue"
	"github.com/lyonbrown4d/ech0/store"
)

type shardedMessageRuntime struct {
	meta     metadataStore
	resolver *brokerShardResolver
	shards   *collectionmapping.Map[store.ShardID, *messageShard]
}

type messageShard struct {
	id    store.ShardID
	log   *store.StorxLogStore
	queue *queue.Runtime
}

func newShardedMessageRuntime(
	cfg Config,
	specs []dataShardSpec,
	resolver *brokerShardResolver,
	meta metadataStore,
	logger *slog.Logger,
	metrics *MetricsRuntime,
) (messageRuntime, error) {
	if len(specs) == 0 {
		specs = buildDataShardSpecs(cfg)
	}
	if resolver == nil {
		resolver = newBrokerShardResolver(meta, cfg.Broker.DataShardCount)
	}
	runtime := &shardedMessageRuntime{
		meta:     meta,
		resolver: resolver,
		shards:   collectionmapping.NewMapWithCapacity[store.ShardID, *messageShard](len(specs)),
	}
	for _, spec := range specs {
		shard, err := openMessageShard(spec, cfg.Storage.SegmentReadMode, meta, logger, metrics)
		if err != nil {
			return nil, errors.Join(err, runtime.Close())
		}
		runtime.shards.Set(spec.ShardID, shard)
	}
	return runtime, nil
}

func openMessageShard(spec dataShardSpec, readMode string, meta metadataStore, _ *slog.Logger, metrics *MetricsRuntime) (*messageShard, error) {
	logStore, err := store.OpenStorxLogStoreWithOptions(spec.SegmentLogPath, store.StorxLogOptions{
		Metrics:  metrics,
		ReadMode: store.SegmentReadMode(readMode),
	})
	if err != nil {
		return nil, wrapBroker("message_shard_open_failed", err, "open message shard %d", spec.ShardID)
	}
	return &messageShard{id: spec.ShardID, log: logStore, queue: queue.New(logStore, meta)}, nil
}

func (r *shardedMessageRuntime) CreateTopic(topic store.TopicConfig) error {
	exists, err := r.TopicExists(topic.Name)
	if err != nil {
		return err
	}
	if exists {
		return brokerStoreError(store.CodeTopicExists, "topic %s already exists", topic.Name)
	}
	if err := r.createTopicLogs(topic); err != nil {
		return err
	}
	return wrapBrokerStore(r.meta.SaveTopicConfig(topic), "save sharded topic config")
}

func (r *shardedMessageRuntime) createTopicLogs(topic store.TopicConfig) error {
	var result error
	r.shards.Range(func(shardID store.ShardID, shard *messageShard) bool {
		if shard != nil {
			err := shard.log.CreateTopic(topic)
			result = errors.Join(result, wrapBrokerStore(err, "create sharded topic log"))
			if err != nil {
				result = errors.Join(result, brokerStoreError(store.CodeUnavailable, "create topic %s on message shard %d", topic.Name, shardID))
			}
		}
		return true
	})
	return result
}

func (r *shardedMessageRuntime) TopicExists(topic string) (bool, error) {
	cfg, err := r.meta.LoadTopicConfig(topic)
	if err != nil {
		return false, wrapBrokerStore(err, "load sharded topic config")
	}
	if cfg != nil {
		return true, nil
	}
	for _, shard := range r.shards.Values() {
		exists, err := shard.log.TopicExists(topic)
		if err != nil {
			return false, wrapBrokerStore(err, "check sharded topic")
		}
		if exists {
			return true, nil
		}
	}
	return false, nil
}

func (r *shardedMessageRuntime) PublishRecord(topic string, partition uint32, record store.RecordAppend) (store.Record, error) {
	shard, err := r.shardForPartition(topic, partition)
	if err != nil {
		return store.Record{}, err
	}
	out, err := shard.queue.PublishRecord(topic, partition, record)
	if err != nil {
		return store.Record{}, wrapBroker("sharded_message_publish_failed", err, "publish message on shard %d", shard.id)
	}
	return out, nil
}

func (r *shardedMessageRuntime) PublishBatchRecords(topic string, partition uint32, records []store.RecordAppend) ([]store.Record, error) {
	shard, err := r.shardForPartition(topic, partition)
	if err != nil {
		return nil, err
	}
	out, err := shard.queue.PublishBatchRecords(topic, partition, records)
	if err != nil {
		return nil, wrapBroker("sharded_message_batch_publish_failed", err, "publish message batch on shard %d", shard.id)
	}
	return out, nil
}

func (r *shardedMessageRuntime) Fetch(
	consumer string,
	topic string,
	partition uint32,
	offset *uint64,
	maxRecords int,
) (store.PollResult, error) {
	shard, err := r.shardForPartition(topic, partition)
	if err != nil {
		return store.PollResult{}, err
	}
	out, err := shard.queue.Fetch(consumer, topic, partition, offset, maxRecords)
	if err != nil {
		return store.PollResult{}, wrapBroker("sharded_message_fetch_failed", err, "fetch messages on shard %d", shard.id)
	}
	return out, nil
}

func (r *shardedMessageRuntime) Ack(consumer, topic string, partition uint32, nextOffset uint64) error {
	shard, err := r.shardForPartition(topic, partition)
	if err != nil {
		return err
	}
	return wrapBroker("sharded_message_ack_failed", shard.queue.Ack(consumer, topic, partition, nextOffset), "ack messages on shard %d", shard.id)
}

func (r *shardedMessageRuntime) ListTopics() ([]store.TopicConfig, error) {
	out, err := r.meta.ListTopics()
	if err != nil {
		return nil, wrapBrokerStore(err, "list sharded topic configs")
	}
	return out, nil
}

func (r *shardedMessageRuntime) ReadFrom(topicPartition store.TopicPartition, offset uint64, maxRecords int) ([]store.Record, error) {
	shard, err := r.shardForTopicPartition(topicPartition)
	if err != nil {
		return nil, err
	}
	out, err := shard.log.ReadFrom(topicPartition, offset, maxRecords)
	if err != nil {
		return nil, wrapBrokerStore(err, "read sharded messages")
	}
	return out, nil
}

func (r *shardedMessageRuntime) LastOffset(topicPartition store.TopicPartition) (*uint64, error) {
	shard, err := r.shardForTopicPartition(topicPartition)
	if err != nil {
		return nil, err
	}
	out, err := shard.log.LastOffset(topicPartition)
	if err != nil {
		return nil, wrapBrokerStore(err, "load sharded message high watermark")
	}
	return out, nil
}

func (r *shardedMessageRuntime) ReadPage(topicPartition store.TopicPartition, cursor string, maxRecords int) (store.RecordPage, error) {
	shard, err := r.shardForTopicPartition(topicPartition)
	if err != nil {
		return store.RecordPage{}, err
	}
	out, err := shard.log.ReadPage(topicPartition, cursor, maxRecords)
	if err != nil {
		return store.RecordPage{}, wrapBrokerStore(err, "page sharded messages")
	}
	return out, nil
}

func (r *shardedMessageRuntime) Close() error {
	if r == nil || r.shards == nil {
		return nil
	}
	var result error
	r.shards.Range(func(shardID store.ShardID, shard *messageShard) bool {
		if shard != nil {
			result = errors.Join(result, wrapBroker("message_shard_close_failed", shard.log.Close(), "close message shard %d", shardID))
		}
		return true
	})
	return result
}

func (r *shardedMessageRuntime) shardForPartition(topic string, partition uint32) (*messageShard, error) {
	return r.shardForTopicPartition(store.NewTopicPartition(topic, partition))
}

func (r *shardedMessageRuntime) shardForTopicPartition(tp store.TopicPartition) (*messageShard, error) {
	placement, err := r.resolver.Resolve(tp)
	if err != nil {
		return nil, err
	}
	shard, ok := r.shards.Get(placement.ShardID)
	if !ok || shard == nil {
		return nil, brokerStoreError(store.CodeInvalidArgument, "message shard %d is not registered", placement.ShardID)
	}
	return shard, nil
}
