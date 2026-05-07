package broker

import (
	"context"
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

func (m *MetricsRuntime) RecordTCPConnection(ctx context.Context) {
	if m == nil {
		return
	}
	m.tcpConnections.Add(1)
	m.tcpConnectionsTotal.Add(ctx, 1)
}

func (m *MetricsRuntime) RecordCommand(ctx context.Context, commandID uint16) {
	if m == nil {
		return
	}
	m.commands.Add(1)
	m.commandsTotal.Add(ctx, 1, observabilityx.Int64("command_id", int64(commandID)))
}

func (m *MetricsRuntime) RecordCommandError(ctx context.Context, code string) {
	if m == nil {
		return
	}
	m.commandErrors.Add(1)
	m.commandErrorsTotal.Add(ctx, 1, observabilityx.String("code", code))
}

func (m *MetricsRuntime) RecordProduce(ctx context.Context, partitioning string, records uint64) {
	if m == nil {
		return
	}
	if records == 0 {
		records = 1
	}
	m.produceRequests.Add(1)
	m.producedRecords.Add(records)
	switch partitioning {
	case PartitionExplicit:
		m.explicitProduceRequests.Add(1)
		m.explicitProducedRecords.Add(records)
	case PartitionKeyHash:
		m.keyHashRequests.Add(1)
		m.keyHashRecords.Add(records)
	default:
		m.roundRobinRequests.Add(1)
		m.roundRobinRecords.Add(records)
	}
	m.produceRequestsTotal.Add(ctx, 1, observabilityx.String("partitioning", partitioning))
	m.producedRecordsTotal.Add(ctx, safeUint64ToInt64(records), observabilityx.String("partitioning", partitioning))
}

func (m *MetricsRuntime) RecordRebalance(ctx context.Context, strategy string, assignments, moved uint64) {
	if m == nil {
		return
	}
	m.rebalances.Add(1)
	m.rebalanceAssignments.Add(assignments)
	m.rebalanceMovedPartitions.Add(moved)
	m.rebalancesTotal.Add(ctx, 1, observabilityx.String("strategy", strategy))
	m.rebalanceAssignmentsTotal.Add(ctx, safeUint64ToInt64(assignments))
	m.rebalanceMovedPartitionsTotal.Add(ctx, safeUint64ToInt64(moved))
}

func (m *MetricsRuntime) RecordRetentionCleanup(ctx context.Context, removedRecords uint64) {
	if m == nil {
		return
	}
	m.retentionCleanupRuns.Add(1)
	m.retentionRemovedRecords.Add(removedRecords)
	m.retentionCleanupRunsTotal.Add(ctx, 1)
	m.retentionRemovedRecordsTotal.Add(ctx, safeUint64ToInt64(removedRecords))
}

func (m *MetricsRuntime) RecordCompactionCleanup(ctx context.Context, compactedPartitions, removedRecords uint64) {
	if m == nil {
		return
	}
	m.compactionCleanupRuns.Add(1)
	m.compactionPartitions.Add(compactedPartitions)
	m.compactionRemovedRecords.Add(removedRecords)
	m.compactionCleanupRunsTotal.Add(ctx, 1)
	m.compactionPartitionsTotal.Add(ctx, safeUint64ToInt64(compactedPartitions))
	m.compactionRemovedRecordsTotal.Add(ctx, safeUint64ToInt64(removedRecords))
}

func (m *MetricsRuntime) RefreshStream(ctx context.Context, broker *Broker) error {
	if m == nil || broker == nil {
		return nil
	}
	snapshot, err := broker.StreamMetricsSnapshot()
	if err != nil {
		return err
	}
	m.setGauge(ctx, m.visibleTopicsCurrent, &m.visibleTopics, safeIntToUint64(snapshot.TopicCount))
	m.setGauge(ctx, m.topicsWithBacklogCurrent, &m.topicsWithBacklog, safeIntToUint64(snapshot.TopicsWithBacklog))
	m.setGauge(ctx, m.totalTopicBacklogRecordsCurrent, &m.totalTopicBacklogRecords, snapshot.TotalTopicBacklogRecords)
	m.setGauge(ctx, m.maxTopicBacklogRecordsCurrent, &m.maxTopicBacklogRecords, snapshot.MaxTopicBacklogRecords)
	m.setGauge(ctx, m.maxPartitionBacklogRecordsCurrent, &m.maxPartitionBacklogRecords, snapshot.MaxPartitionBacklogRecords)
	m.setGauge(ctx, m.consumerGroupsCurrent, &m.consumerGroups, safeIntToUint64(snapshot.ConsumerGroupCount))
	m.setGauge(ctx, m.consumerGroupsWithLagCurrent, &m.consumerGroupsWithLag, safeIntToUint64(snapshot.ConsumerGroupsWithLag))
	m.setGauge(ctx, m.totalConsumerGroupBacklogRecords, &m.totalConsumerGroupBacklog, snapshot.TotalConsumerGroupBacklogRecords)
	m.setGauge(ctx, m.totalConsumerGroupLagRecordsCurrent, &m.totalConsumerGroupLagRecords, snapshot.TotalConsumerGroupLagRecords)
	m.setGauge(ctx, m.maxConsumerGroupLagRecordsCurrent, &m.maxConsumerGroupLagRecords, snapshot.MaxConsumerGroupLagRecords)
	return nil
}

func (m *MetricsRuntime) setGauge(ctx context.Context, gauge observabilityx.Gauge, slot *atomic.Uint64, value uint64) {
	slot.Store(value)
	gauge.Set(ctx, float64(value))
}
