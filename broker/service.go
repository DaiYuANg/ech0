package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/arcgolabs/authx"
	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/eventx"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/lyonbrown4d/ech0/direct"
	"github.com/lyonbrown4d/ech0/discovery"
	"github.com/lyonbrown4d/ech0/store"
)

type Option func(*Broker)

type Broker struct {
	cfg                     Config
	log                     store.MessageLogStore
	meta                    metadataStore
	queue                   messageRuntime
	direct                  *direct.Runtime
	router                  *partitionRouter
	events                  eventx.BusRuntime
	discovery               discovery.Provider
	logger                  *slog.Logger
	metrics                 *MetricsRuntime
	auth                    *authx.Engine
	quota                   QuotaLimiter
	quotaUsage              *quotaUsageTracker
	topicCache              *ristretto.Cache[string, store.TopicConfig]
	groupRebalanceHistory   *collectionlist.ConcurrentRingBuffer[GroupRebalanceHistorySummary]
	commands                brokerCommandRouter
	shards                  *brokerShardResolver
	shardSpecs              []dataShardSpec
	dataShards              dataShardRuntime
	aclAuthorizerConfigured bool
	producerDedupeMu        sync.Mutex

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
	store.OffsetMetadataStore
	store.ConsumerPauseStore
	store.TopicCatalogStore
	store.ConsumerGroupStore
	store.BrokerStateStore
	store.TransactionStore
	store.ProducerBatchStore
	store.ACLPolicyStore
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
		cfg:        cfg,
		log:        logStore,
		meta:       metaStore,
		router:     newPartitionRouter(),
		logger:     slog.Default(),
		auth:       newDefaultAuthEngine(slog.Default()),
		quota:      newConfiguredQuotaLimiter(cfg.Governance.Quota),
		quotaUsage: newQuotaUsageTracker(),
		topicCache: topicCache,
		groupRebalanceHistory: collectionlist.NewConcurrentRingBuffer[GroupRebalanceHistorySummary](
			brokerLifecycleEvents,
		),
		produceBatcher: newRaftProduceBatcher(),
		commitBatcher:  newRaftCommitBatcher(),
	}
	b.shards = newBrokerShardResolver(metaStore, cfg.Broker.DataShardCount)
	b.shardSpecs = buildDataShardSpecs(cfg)
	b.configureCommandRuntime()
	for _, opt := range opts {
		opt(b)
	}
	b.applyRuntimeDefaults()
	if err := b.initMessageRuntime(logStore, metaStore); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Broker) configureCommandRuntime() {
	fallbackCommands := newSingleGroupCommandRouter(b)
	if dataRaftEnabled(b.cfg) {
		b.dataShards = newRaftDataShardRegistry(b, b.shardSpecs)
		b.commands = newClusterCommandRouter(fallbackCommands, b.dataShards, b.shards)
	} else {
		b.dataShards = newLocalDataShardRegistry(b.shardSpecs)
		b.commands = newMetadataOnlyCommandRouter(fallbackCommands)
	}
}

func (b *Broker) applyRuntimeDefaults() {
	if b.logger == nil {
		b.logger = slog.Default()
	}
	configureDragonboatLogger(b.logger)
	if b.metrics == nil {
		b.metrics = NewNoopMetricsRuntime(b.logger)
	}
	if b.auth == nil {
		b.auth = newDefaultAuthEngine(b.logger)
	}
	if !b.aclAuthorizerConfigured {
		b.auth.SetAuthorizer(newMetadataACLAuthorizer(b))
	}
	if b.quota == nil {
		b.quota = UnlimitedQuotaLimiter{}
	}
	if b.quotaUsage == nil {
		b.quotaUsage = newQuotaUsageTracker()
	}
	if b.events == nil {
		b.events = newBrokerEventBus(b.cfg, b.logger, b.metrics)
	}
}

func (b *Broker) initMessageRuntime(logStore store.MessageLogStore, metaStore metadataStore) error {
	runtime, err := newBrokerMessageRuntime(b, logStore, metaStore)
	if err != nil {
		return err
	}
	b.queue = runtime
	b.direct = direct.New(logStore, metaStore)
	return nil
}

func (b *Broker) Config() Config {
	return b.cfg
}

func (b *Broker) Events() eventx.BusRuntime {
	return b.events
}

func (b *Broker) Start(ctx context.Context) error {
	if err := b.startDiscovery(ctx); err != nil {
		return err
	}
	if err := b.meta.SaveBrokerState(store.BrokerState{
		NodeID: fmt.Sprintf("node-%d", b.cfg.Broker.NodeID),
		Epoch:  1,
	}); err != nil {
		return errors.Join(wrapBrokerStore(err, "save broker state"), b.stopDiscovery(context.WithoutCancel(ctx)))
	}
	node, err := startRaft(ctx, b)
	if err != nil {
		return errors.Join(err, b.stopDiscovery(context.WithoutCancel(ctx)))
	}
	b.raftMu.Lock()
	b.raft = node
	b.raftMu.Unlock()
	return nil
}

func (b *Broker) Stop(ctx context.Context) error {
	b.raftMu.Lock()
	node := b.raft
	b.raft = nil
	b.raftMu.Unlock()
	var result error
	if node != nil {
		result = errors.Join(result, node.Snapshot(ctx))
		result = errors.Join(result, node.Close())
	}
	result = errors.Join(result, b.stopDiscovery(context.WithoutCancel(ctx)))
	result = errors.Join(result, b.closeDataShards())
	result = errors.Join(result, b.closeMessageRuntime())
	b.closeTopicCache()
	if b.events != nil {
		result = errors.Join(result, wrapBroker("event_bus_close_failed", b.events.Close(), "close event bus"))
	}
	return result
}

func (b *Broker) RuntimeHealth() RuntimeHealth {
	health := RuntimeHealth{
		Status:      "ok",
		RuntimeMode: runtimeMode(b.cfg),
		Discovery:   b.discoveryHealth(),
		DataShards:  dataShardHealth(b.shardSpecs, b.dataShards),
	}
	b.raftMu.RLock()
	node := b.raft
	b.raftMu.RUnlock()
	if node == nil {
		health.Status = "degraded"
		health.Raft = &RaftHealth{NodeID: b.cfg.Broker.NodeID, KnownNodes: len(b.cfg.Raft.Cluster), Engine: "dragonboat"}
		return health
	}
	health.Raft = node.Health()
	if health.Raft.LeaderID == 0 {
		health.Status = "degraded"
	}
	return health
}

func runtimeMode(cfg Config) string {
	if dataRaftEnabled(cfg) {
		return "cluster"
	}
	return "single_replica_cluster"
}

type RuntimeHealth struct {
	Status      string            `json:"status"`
	RuntimeMode string            `json:"runtime_mode"`
	Discovery   *DiscoveryHealth  `json:"discovery,omitempty"`
	DataShards  []DataShardHealth `json:"data_shards,omitempty"`
	Raft        *RaftHealth       `json:"raft,omitempty"`
}

type RaftHealth struct {
	NodeID        uint64            `json:"node_id"`
	KnownNodes    int               `json:"known_nodes"`
	Engine        string            `json:"engine,omitempty"`
	LeaderID      uint64            `json:"leader_id,omitempty"`
	LocalIsLeader bool              `json:"local_is_leader"`
	Groups        []RaftGroupHealth `json:"groups,omitempty"`
}

type RaftGroupHealth struct {
	GroupID       uint64 `json:"group_id"`
	LeaderID      uint64 `json:"leader_id,omitempty"`
	LocalIsLeader bool   `json:"local_is_leader"`
	Ready         bool   `json:"ready"`
}

type DataShardHealth struct {
	ShardID     store.ShardID `json:"shard_id"`
	RuntimeMode string        `json:"runtime_mode"`
}
