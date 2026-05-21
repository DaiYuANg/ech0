package broker

import (
	"context"

	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/store"
)

func (s *TCPServer) publishProtocolRecord(
	ctx context.Context,
	req protocol.ProduceRequest,
	record store.RecordAppend,
) (ProduceResult, error) {
	partitioning := partitioningFromProtocol(req.Partitioning, req.Partition, req.RoutingKey)
	idempotency := produceIdempotencyFromProtocol(req.Idempotency)
	if idempotency == nil {
		return s.broker.PublishRecord(ctx, req.Topic, partitioning, record)
	}
	return s.broker.PublishRecordIdempotent(ctx, req.Topic, partitioning, record, *idempotency)
}

func (s *TCPServer) publishProtocolBatch(
	ctx context.Context,
	req protocol.ProduceBatchRequest,
	records []store.RecordAppend,
) (ProduceBatchResult, error) {
	partitioning := partitioningFromProtocol(req.Partitioning, req.Partition, req.RoutingKey)
	idempotency := produceIdempotencyFromProtocol(req.Idempotency)
	if idempotency == nil {
		return s.broker.PublishBatch(ctx, req.Topic, partitioning, records)
	}
	return s.broker.PublishBatchIdempotent(ctx, req.Topic, partitioning, records, *idempotency)
}
