package store

import (
	"context"
	"time"
)

type AppendMetrics interface {
	RecordStoreAppendStage(ctx context.Context, operation string, stage string, records int, duration time.Duration, err error)
}

type ReadMetrics interface {
	RecordStoreReadStage(ctx context.Context, operation string, stage string, records int, duration time.Duration, err error)
}

type StoreMetrics interface {
	AppendMetrics
	ReadMetrics
}
