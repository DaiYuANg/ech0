package broker

import (
	"context"
	"fmt"

	"github.com/DaiYuANg/ech0/protocol"
	"github.com/DaiYuANg/ech0/store"
	"github.com/DaiYuANg/ech0/transport"
)

func (s *TCPServer) handleTxBeginFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.TxBeginRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	result, err := s.broker.BeginTransaction(ctx, req.TransactionalID, req.TimeoutMS)
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdTxBeginResponse, protocol.TxBeginResponse{
		Identity:    transactionIdentityToProtocol(result.Identity),
		ExpiresAtMS: result.ExpiresAtMS,
		Status:      protocol.TransactionStatus(result.Status),
	})
}

func (s *TCPServer) handleTxPublishFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.TxPublishRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	if len(req.Payload) > s.limits.MaxPayloadBytes {
		msg := fmt.Sprintf("transactional produce payload size %d exceeds limit %d", len(req.Payload), s.limits.MaxPayloadBytes)
		return errorFrame("payload_too_large", msg), nil
	}
	record := store.NewRecordAppend(req.Payload)
	record.Key = append([]byte(nil), req.Key...)
	record.Headers = storeHeadersFromProtocol(req.Headers)
	if req.Tombstone {
		record.Attributes |= store.RecordAttributeTombstone
	}
	result, err := s.broker.PublishTransactionalRecord(
		ctx,
		transactionIdentityFromProtocol(req.Identity),
		req.Sequence,
		req.Topic,
		partitioningFromProtocol(req.Partitioning, req.Partition),
		record,
	)
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdTxPublishResponse, protocol.TxPublishResponse{
		TxID:       result.TxID,
		Partition:  result.Partition,
		Offset:     result.Record.Offset,
		NextOffset: result.NextOffset,
	})
}

func (s *TCPServer) handleTxPublishBatchFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.TxPublishBatchRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	records, err := batchRecordsFromProtocol(protocol.ProduceBatchRequest{
		Topic:        req.Topic,
		Partition:    req.Partition,
		Partitioning: req.Partitioning,
		Payloads:     req.Payloads,
		Records:      req.Records,
	})
	if err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	result, err := s.broker.PublishTransactionalBatch(
		ctx,
		transactionIdentityFromProtocol(req.Identity),
		req.BaseSequence,
		req.Topic,
		partitioningFromProtocol(req.Partitioning, req.Partition),
		records,
	)
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdTxPublishBatchResponse, protocol.TxPublishBatchResponse{
		TxID:       result.TxID,
		Partition:  result.Partition,
		BaseOffset: result.BaseOffset,
		LastOffset: result.LastOffset,
		NextOffset: result.NextOffset,
		Appended:   len(result.Records),
	})
}

func (s *TCPServer) handleTxCommitOffsetFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.TxCommitOffsetRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	result, err := s.broker.CommitTransactionOffset(ctx, transactionIdentityFromProtocol(req.Identity), TransactionOffsetCommit{
		Consumer:   req.Consumer,
		Group:      req.Group,
		MemberID:   req.MemberID,
		Generation: req.Generation,
		Topic:      req.Topic,
		Partition:  req.Partition,
		NextOffset: req.NextOffset,
	})
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdTxCommitOffsetResponse, protocol.TxCommitOffsetResponse{
		TxID:       result.TxID,
		Consumer:   result.Offset.Consumer,
		Group:      result.Offset.Group,
		Topic:      result.Offset.Topic,
		Partition:  result.Offset.Partition,
		NextOffset: result.Offset.NextOffset,
	})
}

func (s *TCPServer) handleTxCommitFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.TxCommitRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	result, err := s.broker.CommitTransaction(ctx, transactionIdentityFromProtocol(req.Identity))
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdTxCommitResponse, protocol.TxCommitResponse{
		TxID:   result.TxID,
		Status: protocol.TransactionStatus(result.Status),
	})
}

func (s *TCPServer) handleTxAbortFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.TxAbortRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	result, err := s.broker.AbortTransaction(ctx, transactionIdentityFromProtocol(req.Identity))
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdTxAbortResponse, protocol.TxAbortResponse{
		TxID:   result.TxID,
		Status: protocol.TransactionStatus(result.Status),
	})
}

func transactionIdentityToProtocol(identity TransactionIdentity) protocol.TransactionIdentity {
	return protocol.TransactionIdentity{
		TxID:          identity.TxID,
		ProducerID:    identity.ProducerID,
		ProducerEpoch: identity.ProducerEpoch,
	}
}
