package broker

import (
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/arcgolabs/observabilityx"
	promobs "github.com/arcgolabs/observabilityx/prometheus"
	prom "github.com/prometheus/client_golang/prometheus"
)

type MetricsRuntime struct {
	startedAt time.Time
	obs       observabilityx.Observability
	prom      *promobs.Adapter

	tcpConnectionsTotal           observabilityx.Counter
	commandsTotal                 observabilityx.Counter
	commandErrorsTotal            observabilityx.Counter
	commandDuration               observabilityx.Histogram
	quotaChecksTotal              observabilityx.Counter
	quotaRejectionsTotal          observabilityx.Counter
	rebalancesTotal               observabilityx.Counter
	rebalanceAssignmentsTotal     observabilityx.Counter
	rebalanceMovedPartitionsTotal observabilityx.Counter
	retentionCleanupRunsTotal     observabilityx.Counter
	retentionRemovedRecordsTotal  observabilityx.Counter
	compactionCleanupRunsTotal    observabilityx.Counter
	compactionPartitionsTotal     observabilityx.Counter
	compactionRemovedRecordsTotal observabilityx.Counter
	produceRequestsTotal          observabilityx.Counter
	producedRecordsTotal          observabilityx.Counter
	raftStageDuration             observabilityx.Histogram
	raftStoreStageDuration        observabilityx.Histogram
	raftStoreRequestsTotal        observabilityx.Counter
	raftStoreEntriesTotal         observabilityx.Counter
	raftStoreBytesTotal           observabilityx.Counter
	fsmStageDuration              observabilityx.Histogram
	fetchStageDuration            observabilityx.Histogram
	fetchRequestsTotal            observabilityx.Counter
	fetchRecordsTotal             observabilityx.Counter
	storeAppendStageDuration      observabilityx.Histogram
	storeAppendRequestsTotal      observabilityx.Counter
	storeAppendRecordsTotal       observabilityx.Counter
	storeReadStageDuration        observabilityx.Histogram
	storeReadRequestsTotal        observabilityx.Counter
	storeReadRecordsTotal         observabilityx.Counter
	storageOperationsTotal        observabilityx.Counter
	storageOperationErrorsTotal   observabilityx.Counter
	storageOperationDuration      observabilityx.Histogram

	visibleTopicsCurrent                observabilityx.Gauge
	topicsWithBacklogCurrent            observabilityx.Gauge
	totalTopicBacklogRecordsCurrent     observabilityx.Gauge
	maxTopicBacklogRecordsCurrent       observabilityx.Gauge
	maxPartitionBacklogRecordsCurrent   observabilityx.Gauge
	consumerGroupsCurrent               observabilityx.Gauge
	consumerGroupsWithLagCurrent        observabilityx.Gauge
	totalConsumerGroupBacklogRecords    observabilityx.Gauge
	totalConsumerGroupLagRecordsCurrent observabilityx.Gauge
	maxConsumerGroupLagRecordsCurrent   observabilityx.Gauge

	tcpConnections           atomic.Uint64
	commands                 atomic.Uint64
	commandErrors            atomic.Uint64
	quotaChecks              atomic.Uint64
	quotaRejections          atomic.Uint64
	rebalances               atomic.Uint64
	rebalanceAssignments     atomic.Uint64
	rebalanceMovedPartitions atomic.Uint64
	retentionCleanupRuns     atomic.Uint64
	retentionRemovedRecords  atomic.Uint64
	compactionCleanupRuns    atomic.Uint64
	compactionPartitions     atomic.Uint64
	compactionRemovedRecords atomic.Uint64
	produceRequests          atomic.Uint64
	producedRecords          atomic.Uint64
	explicitProduceRequests  atomic.Uint64
	explicitProducedRecords  atomic.Uint64
	roundRobinRequests       atomic.Uint64
	roundRobinRecords        atomic.Uint64
	keyHashRequests          atomic.Uint64
	keyHashRecords           atomic.Uint64
	storageOperations        atomic.Uint64
	storageOperationErrors   atomic.Uint64

	visibleTopics                atomic.Uint64
	topicsWithBacklog            atomic.Uint64
	totalTopicBacklogRecords     atomic.Uint64
	maxTopicBacklogRecords       atomic.Uint64
	maxPartitionBacklogRecords   atomic.Uint64
	consumerGroups               atomic.Uint64
	consumerGroupsWithLag        atomic.Uint64
	totalConsumerGroupBacklog    atomic.Uint64
	totalConsumerGroupLagRecords atomic.Uint64
	maxConsumerGroupLagRecords   atomic.Uint64
}

type MetricsSnapshot struct {
	UptimeSeconds                       uint64 `json:"uptime_seconds"`
	TCPConnectionsTotal                 uint64 `json:"tcp_connections_total"`
	CommandsTotal                       uint64 `json:"commands_total"`
	CommandErrorsTotal                  uint64 `json:"command_errors_total"`
	QuotaChecksTotal                    uint64 `json:"quota_checks_total"`
	QuotaRejectionsTotal                uint64 `json:"quota_rejections_total"`
	RebalancesTotal                     uint64 `json:"rebalances_total"`
	RebalanceAssignmentsTotal           uint64 `json:"rebalance_assignments_total"`
	RebalanceMovedPartitionsTotal       uint64 `json:"rebalance_moved_partitions_total"`
	RetentionCleanupRunsTotal           uint64 `json:"retention_cleanup_runs_total"`
	RetentionRemovedRecordsTotal        uint64 `json:"retention_removed_records_total"`
	CompactionCleanupRunsTotal          uint64 `json:"compaction_cleanup_runs_total"`
	CompactionPartitionsTotal           uint64 `json:"compaction_partitions_total"`
	CompactionRemovedRecordsTotal       uint64 `json:"compaction_removed_records_total"`
	ProduceRequestsTotal                uint64 `json:"produce_requests_total"`
	ProducedRecordsTotal                uint64 `json:"produced_records_total"`
	ExplicitProduceRequestsTotal        uint64 `json:"explicit_produce_requests_total"`
	ExplicitProducedRecordsTotal        uint64 `json:"explicit_produced_records_total"`
	RoundRobinProduceRequestsTotal      uint64 `json:"round_robin_produce_requests_total"`
	RoundRobinProducedRecordsTotal      uint64 `json:"round_robin_produced_records_total"`
	KeyHashProduceRequestsTotal         uint64 `json:"key_hash_produce_requests_total"`
	KeyHashProducedRecordsTotal         uint64 `json:"key_hash_produced_records_total"`
	StorageOperationsTotal              uint64 `json:"storage_operations_total"`
	StorageOperationErrorsTotal         uint64 `json:"storage_operation_errors_total"`
	VisibleTopicsCurrent                uint64 `json:"visible_topics_current"`
	TopicsWithBacklogCurrent            uint64 `json:"topics_with_backlog_current"`
	TotalTopicBacklogRecordsCurrent     uint64 `json:"total_topic_backlog_records_current"`
	MaxTopicBacklogRecordsCurrent       uint64 `json:"max_topic_backlog_records_current"`
	MaxPartitionBacklogRecordsCurrent   uint64 `json:"max_partition_backlog_records_current"`
	ConsumerGroupsCurrent               uint64 `json:"consumer_groups_current"`
	ConsumerGroupsWithLagCurrent        uint64 `json:"consumer_groups_with_lag_current"`
	TotalConsumerGroupBacklogRecords    uint64 `json:"total_consumer_group_backlog_records"`
	TotalConsumerGroupLagRecordsCurrent uint64 `json:"total_consumer_group_lag_records_current"`
	MaxConsumerGroupLagRecordsCurrent   uint64 `json:"max_consumer_group_lag_records_current"`
}

func NewMetricsRuntime(_ Config, logger *slog.Logger) *MetricsRuntime {
	registry := prom.NewRegistry()
	adapter := promobs.New(
		promobs.WithLogger(logger),
		promobs.WithNamespace("ech0"),
		promobs.WithRegisterer(registry),
		promobs.WithGatherer(registry),
	)
	runtime := &MetricsRuntime{
		startedAt: time.Now(),
		obs:       adapter,
		prom:      adapter,
	}
	runtime.initInstruments()
	return runtime
}

func NewNoopMetricsRuntime(logger *slog.Logger) *MetricsRuntime {
	runtime := &MetricsRuntime{
		startedAt: time.Now(),
		obs:       observabilityx.NopWithLogger(logger),
	}
	runtime.initInstruments()
	return runtime
}

func (m *MetricsRuntime) Handler() http.Handler {
	if m == nil || m.prom == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
			w.WriteHeader(http.StatusOK)
		})
	}
	return m.prom.Handler()
}

func (m *MetricsRuntime) Snapshot() MetricsSnapshot {
	if m == nil {
		return MetricsSnapshot{}
	}
	return MetricsSnapshot{
		UptimeSeconds:                       uint64(time.Since(m.startedAt).Seconds()),
		TCPConnectionsTotal:                 m.tcpConnections.Load(),
		CommandsTotal:                       m.commands.Load(),
		CommandErrorsTotal:                  m.commandErrors.Load(),
		QuotaChecksTotal:                    m.quotaChecks.Load(),
		QuotaRejectionsTotal:                m.quotaRejections.Load(),
		RebalancesTotal:                     m.rebalances.Load(),
		RebalanceAssignmentsTotal:           m.rebalanceAssignments.Load(),
		RebalanceMovedPartitionsTotal:       m.rebalanceMovedPartitions.Load(),
		RetentionCleanupRunsTotal:           m.retentionCleanupRuns.Load(),
		RetentionRemovedRecordsTotal:        m.retentionRemovedRecords.Load(),
		CompactionCleanupRunsTotal:          m.compactionCleanupRuns.Load(),
		CompactionPartitionsTotal:           m.compactionPartitions.Load(),
		CompactionRemovedRecordsTotal:       m.compactionRemovedRecords.Load(),
		ProduceRequestsTotal:                m.produceRequests.Load(),
		ProducedRecordsTotal:                m.producedRecords.Load(),
		ExplicitProduceRequestsTotal:        m.explicitProduceRequests.Load(),
		ExplicitProducedRecordsTotal:        m.explicitProducedRecords.Load(),
		RoundRobinProduceRequestsTotal:      m.roundRobinRequests.Load(),
		RoundRobinProducedRecordsTotal:      m.roundRobinRecords.Load(),
		KeyHashProduceRequestsTotal:         m.keyHashRequests.Load(),
		KeyHashProducedRecordsTotal:         m.keyHashRecords.Load(),
		StorageOperationsTotal:              m.storageOperations.Load(),
		StorageOperationErrorsTotal:         m.storageOperationErrors.Load(),
		VisibleTopicsCurrent:                m.visibleTopics.Load(),
		TopicsWithBacklogCurrent:            m.topicsWithBacklog.Load(),
		TotalTopicBacklogRecordsCurrent:     m.totalTopicBacklogRecords.Load(),
		MaxTopicBacklogRecordsCurrent:       m.maxTopicBacklogRecords.Load(),
		MaxPartitionBacklogRecordsCurrent:   m.maxPartitionBacklogRecords.Load(),
		ConsumerGroupsCurrent:               m.consumerGroups.Load(),
		ConsumerGroupsWithLagCurrent:        m.consumerGroupsWithLag.Load(),
		TotalConsumerGroupBacklogRecords:    m.totalConsumerGroupBacklog.Load(),
		TotalConsumerGroupLagRecordsCurrent: m.totalConsumerGroupLagRecords.Load(),
		MaxConsumerGroupLagRecordsCurrent:   m.maxConsumerGroupLagRecords.Load(),
	}
}
