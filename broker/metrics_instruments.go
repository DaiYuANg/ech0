package broker

import (
	"github.com/DaiYuANg/ech0/store"
	"github.com/arcgolabs/observabilityx"
)

func (m *MetricsRuntime) initInstruments() {
	obs := observabilityx.Normalize(m.obs, nil)
	m.obs = obs
	m.initCoreCounters(obs)
	m.initStorageCounters(obs)
	m.initProduceCounters(obs)
	m.initStreamGauges(obs)
}

func (m *MetricsRuntime) initCoreCounters(obs observabilityx.Observability) {
	m.tcpConnectionsTotal = obs.Counter(observabilityx.NewCounterSpec(
		"broker_tcp_connections_total",
		observabilityx.WithDescription("Accepted TCP connections"),
	))
	m.commandsTotal = obs.Counter(observabilityx.NewCounterSpec(
		"broker_commands_total",
		observabilityx.WithDescription("Handled broker commands"),
		observabilityx.WithLabelKeys("command_id"),
	))
	m.commandErrorsTotal = obs.Counter(observabilityx.NewCounterSpec(
		"broker_command_errors_total",
		observabilityx.WithDescription("Broker command errors by code"),
		observabilityx.WithLabelKeys("code"),
	))
	m.rebalancesTotal = obs.Counter(observabilityx.NewCounterSpec(
		"broker_rebalances_total",
		observabilityx.WithDescription("Consumer-group rebalance executions"),
		observabilityx.WithLabelKeys("strategy"),
	))
	m.rebalanceAssignmentsTotal = obs.Counter(observabilityx.NewCounterSpec(
		"broker_rebalance_assignments_total",
		observabilityx.WithDescription("Partition assignments emitted by rebalances"),
	))
	m.rebalanceMovedPartitionsTotal = obs.Counter(observabilityx.NewCounterSpec(
		"broker_rebalance_moved_partitions_total",
		observabilityx.WithDescription("Partition ownership moves caused by rebalances"),
	))
}

func (m *MetricsRuntime) initStorageCounters(obs observabilityx.Observability) {
	m.retentionCleanupRunsTotal = obs.Counter(observabilityx.NewCounterSpec(
		"broker_retention_cleanup_runs_total",
		observabilityx.WithDescription("Background retention cleanup iterations"),
	))
	m.retentionRemovedRecordsTotal = obs.Counter(observabilityx.NewCounterSpec(
		"broker_retention_removed_records_total",
		observabilityx.WithDescription("Records removed by retention cleanup"),
	))
	m.compactionCleanupRunsTotal = obs.Counter(observabilityx.NewCounterSpec(
		"broker_compaction_cleanup_runs_total",
		observabilityx.WithDescription("Background compaction cleanup iterations"),
	))
	m.compactionPartitionsTotal = obs.Counter(observabilityx.NewCounterSpec(
		"broker_compaction_partitions_total",
		observabilityx.WithDescription("Partitions compacted by compaction cleanup"),
	))
	m.compactionRemovedRecordsTotal = obs.Counter(observabilityx.NewCounterSpec(
		"broker_compaction_removed_records_total",
		observabilityx.WithDescription("Records removed by compaction cleanup"),
	))
}

func (m *MetricsRuntime) initProduceCounters(obs observabilityx.Observability) {
	m.produceRequestsTotal = obs.Counter(observabilityx.NewCounterSpec(
		"broker_produce_requests_total",
		observabilityx.WithDescription("Produce requests by partitioning mode"),
		observabilityx.WithLabelKeys("partitioning"),
	))
	m.producedRecordsTotal = obs.Counter(observabilityx.NewCounterSpec(
		"broker_produced_records_total",
		observabilityx.WithDescription("Records appended by produce operations"),
		observabilityx.WithLabelKeys("partitioning"),
	))
}

func (m *MetricsRuntime) initStreamGauges(obs observabilityx.Observability) {
	m.visibleTopicsCurrent = streamGauge(obs, "broker_visible_topics_current", "Current number of visible topics")
	m.topicsWithBacklogCurrent = streamGauge(obs, "broker_topics_with_backlog_current", "Current number of visible topics with backlog")
	m.totalTopicBacklogRecordsCurrent = streamGauge(obs, "broker_topic_backlog_records_current", "Current total backlog records across visible topics")
	m.maxTopicBacklogRecordsCurrent = streamGauge(obs, "broker_topic_backlog_records_max_current", "Current maximum backlog records for a single visible topic")
	m.maxPartitionBacklogRecordsCurrent = streamGauge(obs, "broker_partition_backlog_records_max_current", "Current maximum backlog records for a single partition")
	m.consumerGroupsCurrent = streamGauge(obs, "broker_consumer_groups_current", "Current number of consumer groups with saved assignments")
	m.consumerGroupsWithLagCurrent = streamGauge(obs, "broker_consumer_groups_with_lag_current", "Current number of consumer groups with lag")
	m.totalConsumerGroupBacklogRecords = streamGauge(obs, "broker_consumer_group_backlog_records_current", "Current total backlog records across consumer groups")
	m.totalConsumerGroupLagRecordsCurrent = streamGauge(obs, "broker_consumer_group_lag_records_current", "Current total lag records across consumer groups")
	m.maxConsumerGroupLagRecordsCurrent = streamGauge(obs, "broker_consumer_group_lag_records_max_current", "Current maximum lag records for a single consumer group")
}

func streamGauge(obs observabilityx.Observability, name, description string) observabilityx.Gauge {
	return obs.Gauge(observabilityx.NewGaugeSpec(name, observabilityx.WithDescription(description)))
}

func safeUint64ToInt64(value uint64) int64 {
	const maxInt64 = uint64(1<<63 - 1)
	if value > maxInt64 {
		return int64(maxInt64)
	}
	return int64(value)
}

func safeIntToUint64(value int) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value)
}

func highWatermarkBacklog(highWatermark *uint64) uint64 {
	if highWatermark == nil {
		return 0
	}
	return *highWatermark + 1
}

func lagRecords(committedNextOffset uint64, highWatermark *uint64) uint64 {
	backlog := highWatermarkBacklog(highWatermark)
	if committedNextOffset >= backlog {
		return 0
	}
	return backlog - committedNextOffset
}

func topicPartitionHighWatermark(log store.MessageLogStore, tp store.TopicPartition) (*uint64, error) {
	highWatermark, err := log.LastOffset(tp)
	if err != nil {
		return nil, wrapBrokerStore(err, "load partition high watermark")
	}
	return highWatermark, nil
}
