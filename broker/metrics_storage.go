package broker

import (
	"context"
	"time"

	"github.com/arcgolabs/observabilityx"
)

func (m *MetricsRuntime) RecordStorageOperation(
	ctx context.Context,
	engine string,
	targetType string,
	target string,
	operation string,
	duration time.Duration,
	err error,
) {
	if m == nil {
		return
	}
	status := "ok"
	if err != nil {
		status = "error"
		m.recordStorageOperationError(ctx, engine, targetType, target, operation)
	}
	attrs := storageOperationAttrs(engine, targetType, target, operation, status)
	m.storageOperations.Add(1)
	m.storageOperationsTotal.Add(ctx, 1, attrs...)
	m.storageOperationDuration.Record(ctx, duration.Seconds(), attrs...)
}

func (m *MetricsRuntime) recordStorageOperationError(ctx context.Context, engine, targetType, target, operation string) {
	m.storageOperationErrors.Add(1)
	m.storageOperationErrorsTotal.Add(ctx, 1,
		observabilityx.String("engine", engine),
		observabilityx.String("target_type", targetType),
		observabilityx.String("target", target),
		observabilityx.String("operation", operation),
	)
}

func storageOperationAttrs(engine, targetType, target, operation, status string) []observabilityx.Attribute {
	return []observabilityx.Attribute{
		observabilityx.String("engine", engine),
		observabilityx.String("target_type", targetType),
		observabilityx.String("target", target),
		observabilityx.String("operation", operation),
		observabilityx.String("status", status),
	}
}
