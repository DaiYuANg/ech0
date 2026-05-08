package broker

import (
	"context"

	storxobserver "github.com/arcgolabs/storx/observer"
)

type storageMetricsObserver struct {
	metrics *MetricsRuntime
}

func newStorageMetricsObserver(metrics *MetricsRuntime) storxobserver.Observer {
	if metrics == nil {
		return nil
	}
	return storageMetricsObserver{metrics: metrics}
}

func (o storageMetricsObserver) Observe(ctx context.Context, event storxobserver.Event) {
	o.metrics.RecordStorageOperation(
		ctx,
		event.Engine,
		event.TargetType,
		event.Target,
		event.Operation,
		event.Duration,
		event.Err,
	)
}
