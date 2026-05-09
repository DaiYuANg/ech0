package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/DaiYuANg/ech0/direct"
	"github.com/DaiYuANg/ech0/queue"
	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/eventx"
	"github.com/dgraph-io/ristretto/v2"
)

type Option func(*Broker)

type Broker struct {
	cfg        Config
	log        store.MessageLogStore
	meta       metadataStore
	queue      *queue.Runtime
	direct     *direct.Runtime
	router     *partitionRouter
	events     eventx.BusRuntime
	logger     *slog.Logger
	metrics    *MetricsRuntime
	topicCache *ristretto.Cache[string, store.TopicConfig]
	commands   brokerCommandRouter
	shards     *brokerShardResolver
	shardSpecs []dataShardSpec
	dataShards dataShardRuntime

	raftMu sync.RWMutex
	raft   *raftNode

	produceBatcher *raftProduceBatcher
	commitBatcher  *raftCommitBatcher
}

type ProduceResult struct {
	Partition uint32
	Record    store.Record
}

type ProduceBatchResult struct {
	Partition uint32
	Records   []store.Record
}

type metadataStore interface {
	store.OffsetStore
	store.TopicCatalogStore
	store.ConsumerGroupStore
	store.BrokerStateStore
}

func New(cfg Config, opts ...Option) (*Broker, error) {
	normalizeConfig(&cfg)
	st := store.NewMemoryStore()
	return NewWithStores(cfg, st, st, opts...)
}

func NewWithStores(cfg Config, logStore store.MessageLogStore, metaStore metadataStore, opts ...Option) (*Broker, error) {
	normalizeConfig(&cfg)
	if logStore == nil {
		return nil, brokerStoreError(store.CodeInvalidArgument, "log store is required")
	}
	if metaStore == nil {
		return nil, brokerStoreError(store.CodeInvalidArgument, "metadata store is required")
	}
	topicCache, err := newTopicConfigCache(cfg.Broker.TopicCacheMaxEntries)
	if err != nil && !errors.Is(err, errTopicCacheDisabled) {
		return nil, err
	}
	b := &Broker{
		cfg:            cfg,
		log:            logStore,
		meta:           metaStore,
		router:         newPartitionRouter(),
		events:         eventx.New(),
		logger:         slog.Default(),
		topicCache:     topicCache,
		produceBatcher: newRaftProduceBatcher(),
		commitBatcher:  newRaftCommitBatcher(),
	}
	b.shards = newBrokerShardResolver(metaStore, cfg.Broker.DataShardCount)
	b.shardSpecs = buildDataShardSpecs(cfg)
	fallbackCommands := newSingleGroupCommandRouter(b)
	b.dataShards = newCompatibilityDataShardRegistry(b.shardSpecs, newSingleGroupDataShardRuntime(fallbackCommands))
	b.commands = fallbackCommands
	if cfg.Raft.Enabled {
		b.commands = newClusterCommandRouter(fallbackCommands, b.dataShards, b.shards)
	}
	b.queue = queue.New(logStore, metaStore)
	b.direct = direct.New(logStore, metaStore)
	for _, opt := range opts {
		opt(b)
	}
	if b.events == nil {
		b.events = eventx.New()
	}
	if b.logger == nil {
		b.logger = slog.Default()
	}
	if b.metrics == nil {
		b.metrics = NewNoopMetricsRuntime(b.logger)
	}
	return b, nil
}

func WithLogger(logger *slog.Logger) Option {
	return func(b *Broker) {
		if logger != nil {
			b.logger = logger
		}
	}
}

func WithEventBus(events eventx.BusRuntime) Option {
	return func(b *Broker) {
		if events != nil {
			b.events = events
		}
	}
}

func WithMetrics(metrics *MetricsRuntime) Option {
	return func(b *Broker) {
		if metrics != nil {
			b.metrics = metrics
		}
	}
}

func (b *Broker) Config() Config {
	return b.cfg
}

func (b *Broker) Events() eventx.BusRuntime {
	return b.events
}

func (b *Broker) Start(ctx context.Context) error {
	if err := b.meta.SaveBrokerState(store.BrokerState{
		NodeID: fmt.Sprintf("node-%d", b.cfg.Broker.NodeID),
		Epoch:  1,
	}); err != nil {
		return wrapBrokerStore(err, "save broker state")
	}
	if b.cfg.Raft.Enabled {
		node, err := startRaft(ctx, b)
		if err != nil {
			return err
		}
		b.raftMu.Lock()
		b.raft = node
		b.raftMu.Unlock()
	}
	return nil
}

func (b *Broker) Stop(ctx context.Context) error {
	_ = ctx
	b.raftMu.Lock()
	node := b.raft
	b.raft = nil
	b.raftMu.Unlock()
	if node != nil {
		if err := node.Close(); err != nil {
			return err
		}
	}
	b.closeTopicCache()
	if b.events != nil {
		return wrapBroker("event_bus_close_failed", b.events.Close(), "close event bus")
	}
	return nil
}

func (b *Broker) RuntimeHealth() RuntimeHealth {
	health := RuntimeHealth{
		Status:      "ok",
		RuntimeMode: "standalone",
		DataShards:  dataShardHealth(b.shardSpecs),
	}
	b.raftMu.RLock()
	node := b.raft
	b.raftMu.RUnlock()
	if b.cfg.Raft.Enabled {
		health.RuntimeMode = "raft"
		if node == nil {
			health.Status = "degraded"
			health.Raft = &RaftHealth{NodeID: b.cfg.Broker.NodeID, KnownNodes: len(b.cfg.Raft.Cluster)}
			return health
		}
		health.Raft = node.Health()
		if health.Raft.LeaderID == 0 {
			health.Status = "degraded"
		}
	}
	return health
}

type RuntimeHealth struct {
	Status      string            `json:"status"`
	RuntimeMode string            `json:"runtime_mode"`
	DataShards  []DataShardHealth `json:"data_shards,omitempty"`
	Raft        *RaftHealth       `json:"raft,omitempty"`
}

type RaftHealth struct {
	NodeID        uint64 `json:"node_id"`
	KnownNodes    int    `json:"known_nodes"`
	LeaderID      uint64 `json:"leader_id,omitempty"`
	LocalIsLeader bool   `json:"local_is_leader"`
}

type DataShardHealth struct {
	ShardID     store.ShardID `json:"shard_id"`
	RuntimeMode string        `json:"runtime_mode"`
}

func dataShardHealth(specs []dataShardSpec) []DataShardHealth {
	out := collectionlist.NewListWithCapacity[DataShardHealth](len(specs))
	for _, spec := range specs {
		out.Add(DataShardHealth{ShardID: spec.ShardID, RuntimeMode: "compat_single_group"})
	}
	return out.Values()
}
