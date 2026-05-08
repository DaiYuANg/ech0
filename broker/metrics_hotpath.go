package broker

import (
	"context"
	"time"

	"github.com/arcgolabs/observabilityx"
)

func (m *MetricsRuntime) RecordRaftStage(ctx context.Context, command, stage string, duration time.Duration, err error) {
	if m == nil {
		return
	}
	m.raftStageDuration.Record(ctx, duration.Seconds(), commandStageAttrs(command, stage, err)...)
}

func (m *MetricsRuntime) RecordRaftStoreStage(
	ctx context.Context,
	operation string,
	stage string,
	entries int,
	bytes int,
	duration time.Duration,
	err error,
) {
	if m == nil {
		return
	}
	attrs := storeStageAttrs(operation, stage, err)
	m.raftStoreStageDuration.Record(ctx, duration.Seconds(), attrs...)
	if stage != "total" {
		return
	}
	status := statusLabel(err)
	m.raftStoreRequestsTotal.Add(ctx, 1,
		observabilityx.String("operation", operation),
		observabilityx.String("status", status),
	)
	m.raftStoreEntriesTotal.Add(ctx, safeIntToInt64(entries),
		observabilityx.String("operation", operation),
		observabilityx.String("status", status),
	)
	m.raftStoreBytesTotal.Add(ctx, safeIntToInt64(bytes),
		observabilityx.String("operation", operation),
		observabilityx.String("status", status),
	)
}

func (m *MetricsRuntime) RecordFSMStage(ctx context.Context, command, stage string, duration time.Duration, err error) {
	if m == nil {
		return
	}
	m.fsmStageDuration.Record(ctx, duration.Seconds(), commandStageAttrs(command, stage, err)...)
}

func (m *MetricsRuntime) RecordFetchStage(ctx context.Context, operation, stage string, records int, duration time.Duration, err error) {
	if m == nil {
		return
	}
	attrs := storeStageAttrs(operation, stage, err)
	m.fetchStageDuration.Record(ctx, duration.Seconds(), attrs...)
	if stage != "total" {
		return
	}
	status := statusLabel(err)
	m.fetchRequestsTotal.Add(ctx, 1,
		observabilityx.String("operation", operation),
		observabilityx.String("status", status),
	)
	m.fetchRecordsTotal.Add(ctx, safeIntToInt64(records),
		observabilityx.String("operation", operation),
		observabilityx.String("status", status),
	)
}

func (b *Broker) recordFetchStage(ctx context.Context, operation, stage string, records int, start time.Time, err error) {
	if b == nil || b.metrics == nil {
		return
	}
	b.metrics.RecordFetchStage(ctx, operation, stage, records, time.Since(start), err)
}

func (m *MetricsRuntime) RecordStoreAppendStage(ctx context.Context, operation, stage string, records int, duration time.Duration, err error) {
	if m == nil {
		return
	}
	attrs := storeStageAttrs(operation, stage, err)
	m.storeAppendStageDuration.Record(ctx, duration.Seconds(), attrs...)
	if stage != "total" {
		return
	}
	status := statusLabel(err)
	m.storeAppendRequestsTotal.Add(ctx, 1,
		observabilityx.String("operation", operation),
		observabilityx.String("status", status),
	)
	m.storeAppendRecordsTotal.Add(ctx, safeIntToInt64(records),
		observabilityx.String("operation", operation),
		observabilityx.String("status", status),
	)
}

func (m *MetricsRuntime) RecordStoreReadStage(ctx context.Context, operation, stage string, records int, duration time.Duration, err error) {
	if m == nil {
		return
	}
	attrs := storeStageAttrs(operation, stage, err)
	m.storeReadStageDuration.Record(ctx, duration.Seconds(), attrs...)
	if stage != "total" {
		return
	}
	status := statusLabel(err)
	m.storeReadRequestsTotal.Add(ctx, 1,
		observabilityx.String("operation", operation),
		observabilityx.String("status", status),
	)
	m.storeReadRecordsTotal.Add(ctx, safeIntToInt64(records),
		observabilityx.String("operation", operation),
		observabilityx.String("status", status),
	)
}

func commandStageAttrs(command, stage string, err error) []observabilityx.Attribute {
	return []observabilityx.Attribute{
		observabilityx.String("command", command),
		observabilityx.String("stage", stage),
		observabilityx.String("status", statusLabel(err)),
	}
}

func storeStageAttrs(operation, stage string, err error) []observabilityx.Attribute {
	return []observabilityx.Attribute{
		observabilityx.String("operation", operation),
		observabilityx.String("stage", stage),
		observabilityx.String("status", statusLabel(err)),
	}
}

func statusLabel(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}
