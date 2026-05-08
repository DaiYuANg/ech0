package store

import (
	"context"
	"time"
)

func (s *StorxLogStore) recordAppendStage(operation, stage string, records int, start time.Time, err error) {
	if s == nil || s.metrics == nil {
		return
	}
	s.metrics.RecordStoreAppendStage(context.Background(), operation, stage, records, time.Since(start), err)
}

func (s *StorxLogStore) recordReadStage(operation, stage string, records int, start time.Time, err error) {
	if s == nil || s.metrics == nil {
		return
	}
	s.metrics.RecordStoreReadStage(context.Background(), operation, stage, records, time.Since(start), err)
}
