package ech0

import (
	"context"
	"log/slog"
	"time"

	internalbroker "github.com/DaiYuANg/ech0/broker"
	"github.com/DaiYuANg/ech0/store"
)

type Options struct {
	DataDir        string
	NodeID         uint64
	Raft           *RaftOptions
	DisableRetry   bool
	DisableDelay   bool
	MaxFetch       int
	MaxPayloadSize int
}

type RaftOptions struct {
	BindAddr string
	Peers    []RaftPeer
}

type RaftPeer struct {
	NodeID uint64
	Addr   string
}

type Broker struct {
	broker    *internalbroker.Broker
	scheduled *internalbroker.ScheduledRuntime
	logStore  *store.StorxLogStore
	metaStore *store.StorxMetadataStore
}

type TopicOption func(*topicOptions)

type topicOptions struct {
	partitions      uint32
	delayEnabled    bool
	deadLetterTopic string
	retryPolicy     *RetryPolicy
}

type RetryPolicy struct {
	MaxAttempts    uint32
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

type PublishOption func(*publishOptions)

type publishOptions struct {
	key       []byte
	partition *uint32
	tombstone bool
}

type FetchOption func(*fetchOptions)

type fetchOptions struct {
	partition  uint32
	offset     *uint64
	maxRecords int
}

type Message struct {
	Topic      string
	Partition  uint32
	Offset     uint64
	Timestamp  time.Time
	Key        []byte
	Payload    []byte
	NextOffset uint64
	Tombstone  bool
}

type FetchResult struct {
	Messages      []Message
	NextOffset    uint64
	HighWatermark *uint64
}

func DefaultOptions() Options {
	return Options{
		DataDir:        "./data",
		NodeID:         1,
		MaxFetch:       1000,
		MaxPayloadSize: 1024 * 1024,
	}
}

func Open(ctx context.Context, opts Options) (*Broker, error) {
	opts = normalizeOptions(opts)
	cfg := configFromOptions(opts)

	logStore, err := store.OpenStorxLogStore(cfg.SegmentLogPath())
	if err != nil {
		return nil, err
	}
	metaStore, err := store.OpenStorxMetadataStore(cfg.MetadataPath())
	if err != nil {
		_ = logStore.Close()
		return nil, err
	}
	b, err := internalbroker.NewWithStores(cfg, logStore, metaStore)
	if err != nil {
		_ = metaStore.Close()
		_ = logStore.Close()
		return nil, err
	}
	if err := b.Start(ctx); err != nil {
		_ = metaStore.Close()
		_ = logStore.Close()
		return nil, err
	}
	scheduled, err := internalbroker.NewScheduledRuntime(cfg, b, slog.Default())
	if err != nil {
		_ = b.Stop(ctx)
		_ = metaStore.Close()
		_ = logStore.Close()
		return nil, err
	}
	if err := scheduled.Start(ctx); err != nil {
		_ = b.Stop(ctx)
		_ = metaStore.Close()
		_ = logStore.Close()
		return nil, err
	}
	return &Broker{broker: b, scheduled: scheduled, logStore: logStore, metaStore: metaStore}, nil
}

func Run(ctx context.Context, opts Options) error {
	b, err := Open(ctx, opts)
	if err != nil {
		return err
	}
	defer b.Close(context.Background())
	<-ctx.Done()
	return ctx.Err()
}

func (b *Broker) Close(ctx context.Context) error {
	if b == nil {
		return nil
	}
	var err error
	if b.scheduled != nil {
		if closeErr := b.scheduled.Stop(ctx); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if b.broker != nil {
		if closeErr := b.broker.Stop(ctx); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if b.metaStore != nil {
		if closeErr := b.metaStore.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if b.logStore != nil {
		if closeErr := b.logStore.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

func (b *Broker) CreateTopic(ctx context.Context, name string, opts ...TopicOption) error {
	topicOpts := topicOptions{partitions: 1}
	for _, opt := range opts {
		if opt != nil {
			opt(&topicOpts)
		}
	}
	topic := store.NewTopicConfig(name)
	topic.Partitions = topicOpts.partitions
	topic.DelayEnabled = topicOpts.delayEnabled
	if topicOpts.deadLetterTopic != "" {
		topic.DeadLetterTopic = &topicOpts.deadLetterTopic
	}
	if topicOpts.retryPolicy != nil {
		topic.RetryPolicy = store.TopicRetryPolicy{
			MaxAttempts:      topicOpts.retryPolicy.MaxAttempts,
			BackoffInitialMS: uint64(topicOpts.retryPolicy.InitialBackoff.Milliseconds()),
			BackoffMaxMS:     uint64(topicOpts.retryPolicy.MaxBackoff.Milliseconds()),
		}
	}
	_, err := b.broker.CreateTopic(ctx, topic)
	return err
}

func (b *Broker) Publish(ctx context.Context, topic string, payload []byte, opts ...PublishOption) (Message, error) {
	publishOpts := publishOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&publishOpts)
		}
	}
	partitioning := internalbroker.PublishPartitioning{Mode: internalbroker.PartitionRoundRobin}
	if publishOpts.partition != nil {
		partitioning = internalbroker.PublishPartitioning{Mode: internalbroker.PartitionExplicit, Partition: *publishOpts.partition}
	} else if len(publishOpts.key) > 0 {
		partitioning = internalbroker.PublishPartitioning{Mode: internalbroker.PartitionKeyHash}
	}
	result, err := b.broker.Publish(ctx, topic, partitioning, publishOpts.key, publishOpts.tombstone, payload)
	if err != nil {
		return Message{}, err
	}
	return messageFromRecord(topic, result.Partition, result.Record), nil
}

func (b *Broker) Fetch(ctx context.Context, consumer string, topic string, opts ...FetchOption) (FetchResult, error) {
	_ = ctx
	fetchOpts := fetchOptions{maxRecords: 100}
	for _, opt := range opts {
		if opt != nil {
			opt(&fetchOpts)
		}
	}
	poll, err := b.broker.Fetch(consumer, topic, fetchOpts.partition, fetchOpts.offset, fetchOpts.maxRecords)
	if err != nil {
		return FetchResult{}, err
	}
	messages := make([]Message, 0, len(poll.Records))
	for _, record := range poll.Records {
		messages = append(messages, messageFromRecord(topic, fetchOpts.partition, record))
	}
	return FetchResult{Messages: messages, NextOffset: poll.NextOffset, HighWatermark: poll.HighWatermark}, nil
}

func (b *Broker) Ack(ctx context.Context, consumer string, msg Message) error {
	return b.broker.CommitOffset(ctx, consumer, msg.Topic, msg.Partition, msg.NextOffset)
}

func (b *Broker) Commit(ctx context.Context, consumer string, topic string, partition uint32, nextOffset uint64) error {
	return b.broker.CommitOffset(ctx, consumer, topic, partition, nextOffset)
}

func (b *Broker) Nack(ctx context.Context, consumer string, msg Message, cause error) error {
	var lastError *string
	if cause != nil {
		value := cause.Error()
		lastError = &value
	}
	_, err := b.broker.Nack(ctx, consumer, msg.Topic, msg.Partition, msg.Offset, lastError)
	return err
}

func (b *Broker) Schedule(ctx context.Context, topic string, payload []byte, deliverAt time.Time) (Message, error) {
	result, err := b.broker.ScheduleDelay(ctx, topic, 0, payload, uint64(deliverAt.UnixMilli()))
	if err != nil {
		return Message{}, err
	}
	return Message{Topic: result.DelayTopic, Partition: result.Partition, Offset: result.Offset, NextOffset: result.NextOffset, Payload: append([]byte(nil), payload...)}, nil
}

func Partitions(n uint32) TopicOption {
	return func(opts *topicOptions) {
		if n > 0 {
			opts.partitions = n
		}
	}
}

func EnableDelay() TopicOption {
	return func(opts *topicOptions) {
		opts.delayEnabled = true
	}
}

func DeadLetterTopic(topic string) TopicOption {
	return func(opts *topicOptions) {
		opts.deadLetterTopic = topic
	}
}

func Retry(policy RetryPolicy) TopicOption {
	return func(opts *topicOptions) {
		opts.retryPolicy = &policy
	}
}

func Key(key []byte) PublishOption {
	return func(opts *publishOptions) {
		opts.key = append([]byte(nil), key...)
	}
}

func Partition(partition uint32) PublishOption {
	return func(opts *publishOptions) {
		opts.partition = &partition
	}
}

func Tombstone() PublishOption {
	return func(opts *publishOptions) {
		opts.tombstone = true
	}
}

func FetchPartition(partition uint32) FetchOption {
	return func(opts *fetchOptions) {
		opts.partition = partition
	}
}

func FetchOffset(offset uint64) FetchOption {
	return func(opts *fetchOptions) {
		opts.offset = &offset
	}
}

func FetchLimit(maxRecords int) FetchOption {
	return func(opts *fetchOptions) {
		if maxRecords > 0 {
			opts.maxRecords = maxRecords
		}
	}
}

func normalizeOptions(opts Options) Options {
	defaults := DefaultOptions()
	if opts.DataDir == "" {
		opts.DataDir = defaults.DataDir
	}
	if opts.NodeID == 0 {
		opts.NodeID = defaults.NodeID
	}
	if opts.MaxFetch == 0 {
		opts.MaxFetch = defaults.MaxFetch
	}
	if opts.MaxPayloadSize == 0 {
		opts.MaxPayloadSize = defaults.MaxPayloadSize
	}
	return opts
}

func configFromOptions(opts Options) internalbroker.Config {
	cfg := internalbroker.DefaultConfig()
	cfg.Broker.NodeID = opts.NodeID
	cfg.Broker.DataDir = opts.DataDir
	cfg.Broker.MaxFetchRecords = opts.MaxFetch
	cfg.Broker.MaxPayloadBytes = opts.MaxPayloadSize
	cfg.Admin.Enabled = false
	cfg.Broker.RetryWorkerEnabled = !opts.DisableRetry
	cfg.Broker.DelaySchedulerEnabled = !opts.DisableDelay
	cfg.Raft.Enabled = opts.Raft != nil
	if opts.Raft != nil {
		if opts.Raft.BindAddr != "" {
			cfg.Raft.BindAddr = opts.Raft.BindAddr
		}
		cfg.Raft.Cluster = make([]internalbroker.RaftPeerConfig, 0, len(opts.Raft.Peers))
		for _, peer := range opts.Raft.Peers {
			cfg.Raft.Cluster = append(cfg.Raft.Cluster, internalbroker.RaftPeerConfig{NodeID: peer.NodeID, Addr: peer.Addr})
		}
		if len(cfg.Raft.Cluster) == 0 {
			cfg.Raft.Cluster = []internalbroker.RaftPeerConfig{{NodeID: opts.NodeID, Addr: cfg.Raft.BindAddr}}
		}
	}
	return cfg
}

func messageFromRecord(topic string, partition uint32, record store.Record) Message {
	return Message{
		Topic:      topic,
		Partition:  partition,
		Offset:     record.Offset,
		Timestamp:  time.UnixMilli(int64(record.TimestampMS)),
		Key:        append([]byte(nil), record.Key...),
		Payload:    append([]byte(nil), record.Payload...),
		NextOffset: record.Offset + 1,
		Tombstone:  record.IsTombstone(),
	}
}
