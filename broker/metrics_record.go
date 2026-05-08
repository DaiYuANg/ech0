package broker

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/arcgolabs/observabilityx"
)

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

func (m *MetricsRuntime) RecordCommandDuration(ctx context.Context, commandID uint16, duration time.Duration, status string) {
	if m == nil {
		return
	}
	m.commandDuration.Record(ctx, duration.Seconds(),
		observabilityx.Int64("command_id", int64(commandID)),
		observabilityx.String("status", status),
	)
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
	m.recordPartitioningProduce(partitioning, records)
	m.produceRequestsTotal.Add(ctx, 1, observabilityx.String("partitioning", partitioning))
	m.producedRecordsTotal.Add(ctx, safeUint64ToInt64(records), observabilityx.String("partitioning", partitioning))
}

func (m *MetricsRuntime) recordPartitioningProduce(partitioning string, records uint64) {
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
