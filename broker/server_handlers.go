package broker

import (
	"context"
	"fmt"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/lyonbrown4d/ech0/transport"
)

type frameHandler func(*TCPServer, context.Context, transport.Frame) (transport.Frame, error)

var tcpFrameHandlers = map[uint16]frameHandler{
	protocol.CmdHandshakeRequest:                  (*TCPServer).handleHandshakeFrame,
	protocol.CmdPingRequest:                       (*TCPServer).handlePingFrame,
	protocol.CmdCreateTopicRequest:                (*TCPServer).handleCreateTopicFrame,
	protocol.CmdListTopicsRequest:                 (*TCPServer).handleListTopicsFrame,
	protocol.CmdProduceRequest:                    (*TCPServer).handleProduceFrame,
	protocol.CmdProduceBatchRequest:               (*TCPServer).handleProduceBatchFrame,
	protocol.CmdProduceBatchesRequest:             (*TCPServer).handleProduceBatchesFrame,
	protocol.CmdFetchRequest:                      (*TCPServer).handleFetchFrame,
	protocol.CmdFetchBatchRequest:                 (*TCPServer).handleFetchBatchFrame,
	protocol.CmdCommitOffsetRequest:               (*TCPServer).handleCommitOffsetFrame,
	protocol.CmdNackRequest:                       (*TCPServer).handleNackFrame,
	protocol.CmdProcessRetryRequest:               (*TCPServer).handleProcessRetryFrame,
	protocol.CmdScheduleDelayRequest:              (*TCPServer).handleScheduleDelayFrame,
	protocol.CmdTxBeginRequest:                    (*TCPServer).handleTxBeginFrame,
	protocol.CmdTxPublishRequest:                  (*TCPServer).handleTxPublishFrame,
	protocol.CmdTxPublishBatchRequest:             (*TCPServer).handleTxPublishBatchFrame,
	protocol.CmdTxCommitOffsetRequest:             (*TCPServer).handleTxCommitOffsetFrame,
	protocol.CmdTxCommitRequest:                   (*TCPServer).handleTxCommitFrame,
	protocol.CmdTxAbortRequest:                    (*TCPServer).handleTxAbortFrame,
	protocol.CmdSendDirectRequest:                 (*TCPServer).handleSendDirectFrame,
	protocol.CmdFetchInboxRequest:                 (*TCPServer).handleFetchInboxFrame,
	protocol.CmdAckDirectRequest:                  (*TCPServer).handleAckDirectFrame,
	protocol.CmdStartRequestRequest:               (*TCPServer).handleStartRequestFrame,
	protocol.CmdFetchRequestsRequest:              (*TCPServer).handleFetchRequestsFrame,
	protocol.CmdReplyRequest:                      (*TCPServer).handleReplyFrame,
	protocol.CmdReplyErrorRequest:                 (*TCPServer).handleReplyErrorFrame,
	protocol.CmdAwaitReplyRequest:                 (*TCPServer).handleAwaitReplyFrame,
	protocol.CmdJoinConsumerGroupRequest:          (*TCPServer).handleJoinConsumerGroupFrame,
	protocol.CmdHeartbeatConsumerGroupRequest:     (*TCPServer).handleHeartbeatConsumerGroupFrame,
	protocol.CmdRebalanceConsumerGroupRequest:     (*TCPServer).handleRebalanceConsumerGroupFrame,
	protocol.CmdGetConsumerGroupAssignmentRequest: (*TCPServer).handleGetConsumerGroupAssignmentFrame,
	protocol.CmdFetchConsumerGroupRequest:         (*TCPServer).handleFetchConsumerGroupFrame,
	protocol.CmdCommitConsumerGroupOffsetRequest:  (*TCPServer).handleCommitConsumerGroupOffsetFrame,
	protocol.CmdFetchConsumerGroupBatchRequest:    (*TCPServer).handleFetchConsumerGroupBatchFrame,
}

func (s *TCPServer) handleHandshakeFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.HandshakeRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	_ = req
	identity := s.broker.identity(ctx)
	return okFrame(protocol.CmdHandshakeResponse, protocol.HandshakeResponse{
		ServerID:        fmt.Sprintf("%s-node-%d", s.broker.cfg.Broker.ClusterName, s.broker.cfg.Broker.NodeID),
		ProtocolVersion: protocol.Version,
		Tenant:          identity.Tenant,
		Namespace:       identity.Namespace,
		Principal:       identity.Principal,
	})
}

func (s *TCPServer) handlePingFrame(_ context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.PingRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	return okFrame(protocol.CmdPingResponse, protocol.PingResponse(req))
}

func (s *TCPServer) handleCreateTopicFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.CreateTopicRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	created, err := s.broker.CreateTopic(ctx, topicConfigFromProtocol(req))
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdCreateTopicResponse, protocol.CreateTopicResponse{Topic: created.Name, Partitions: created.Partitions})
}

func (s *TCPServer) handleListTopicsFrame(ctx context.Context, _ transport.Frame) (transport.Frame, error) {
	topics, err := s.broker.ListTopicsFor(ctx)
	if err != nil {
		return errorFromErr(err), nil
	}
	out := collectionlist.NewListWithCapacity[protocol.TopicMetadata](len(topics))
	for i := range topics {
		topic := topics[i]
		out.Add(protocol.TopicMetadata{Topic: topic.Name, Partitions: topic.Partitions})
	}
	return okFrame(protocol.CmdListTopicsResponse, protocol.ListTopicsResponse{Topics: out.Values()})
}

func (s *TCPServer) handleProduceFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.ProduceRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	if len(req.Payload) > s.limits.MaxPayloadBytes {
		msg := fmt.Sprintf("produce payload size %d exceeds limit %d", len(req.Payload), s.limits.MaxPayloadBytes)
		return errorFrame("payload_too_large", msg), nil
	}
	record := store.NewRecordAppend(req.Payload)
	record.Key = append([]byte(nil), req.Key...)
	record.Headers = storeHeadersFromProtocol(req.Headers)
	if req.Tombstone {
		record.Attributes |= store.RecordAttributeTombstone
	}
	result, err := s.broker.PublishRecord(ctx, req.Topic, partitioningFromProtocol(req.Partitioning, req.Partition), record)
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdProduceResponse, protocol.ProduceResponse{
		Partition:  result.Partition,
		Offset:     result.Record.Offset,
		NextOffset: result.Record.Offset + 1,
	})
}

func (s *TCPServer) handleProduceBatchFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.ProduceBatchRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	records, err := batchRecordsFromProtocol(req)
	if err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	result, err := s.broker.PublishBatch(ctx, req.Topic, partitioningFromProtocol(req.Partitioning, req.Partition), records)
	if err != nil {
		return errorFromErr(err), nil
	}
	if len(result.Records) == 0 {
		return errorFrame("invalid_request", "produce_batch appended no records"), nil
	}
	return okFrame(protocol.CmdProduceBatchResponse, protocol.ProduceBatchResponse{
		Partition:  result.Partition,
		BaseOffset: result.Records[0].Offset,
		LastOffset: result.Records[len(result.Records)-1].Offset,
		NextOffset: result.Records[len(result.Records)-1].Offset + 1,
		Appended:   len(result.Records),
	})
}

func (s *TCPServer) handleProduceBatchesFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.ProduceBatchesRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	commands := collectionlist.NewListWithCapacity[produceBatchCommand](len(req.Items))
	items := collectionlist.NewListWithCapacity[protocol.ProduceBatchesItemResponse](len(req.Items))
	for _, item := range req.Items {
		records, err := batchRecordsFromProtocol(protocol.ProduceBatchRequest(item))
		if err != nil {
			items.Add(protocol.ProduceBatchesItemResponse{Topic: item.Topic, Error: err.Error()})
			commands.Add(produceBatchCommand{})
			continue
		}
		commands.Add(produceBatchCommand{
			Topic:        item.Topic,
			Partitioning: partitioningFromProtocol(item.Partitioning, item.Partition),
			Records:      records,
		})
		items.Add(protocol.ProduceBatchesItemResponse{Topic: item.Topic})
	}
	result, err := s.broker.publishBatches(ctx, commands.Values())
	if err != nil {
		return errorFromErr(err), nil
	}
	out := mergeProduceBatchesResponse(req.Items, items.Values(), result.Items)
	return okFrame(protocol.CmdProduceBatchesResponse, protocol.ProduceBatchesResponse{Items: out})
}

func (s *TCPServer) handleFetchFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.FetchRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	poll, err := s.broker.FetchWithIsolation(ctx, req.Consumer, req.Topic, req.Partition, req.Offset, req.MaxRecords, isolationFromProtocol(req.Isolation))
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdFetchResponse, protocol.FetchResponse{
		Topic:         req.Topic,
		Partition:     req.Partition,
		Records:       fetchRecordsFromStore(poll.Records),
		NextOffset:    poll.NextOffset,
		HighWatermark: poll.HighWatermark,
	})
}

func (s *TCPServer) handleFetchBatchFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.FetchBatchRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	items := collectionlist.NewListWithCapacity[protocol.FetchBatchItemResponse](len(req.Items))
	for _, item := range req.Items {
		poll, err := s.broker.FetchWithIsolation(ctx, req.Consumer, item.Topic, item.Partition, item.Offset, item.MaxRecords, isolationFromProtocol(req.Isolation))
		if err != nil {
			return errorFromErr(err), nil
		}
		items.Add(protocol.FetchBatchItemResponse{
			Topic:         item.Topic,
			Partition:     item.Partition,
			Records:       fetchRecordsFromStore(poll.Records),
			NextOffset:    poll.NextOffset,
			HighWatermark: poll.HighWatermark,
		})
	}
	return okFrame(protocol.CmdFetchBatchResponse, protocol.FetchBatchResponse{Items: items.Values()})
}

func (s *TCPServer) handleCommitOffsetFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.CommitOffsetRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	if err := s.broker.CommitOffset(ctx, req.Consumer, req.Topic, req.Partition, req.NextOffset); err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdCommitOffsetResponse, protocol.CommitOffsetResponse(req))
}

func (s *TCPServer) handleNackFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.NackRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	result, err := s.broker.Nack(ctx, req.Consumer, req.Topic, req.Partition, req.Offset, req.LastError)
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdNackResponse, protocol.NackResponse{
		RetryTopic:      result.RetryTopic,
		RetryPartition:  result.RetryPartition,
		RetryOffset:     result.RetryOffset,
		RetryNextOffset: result.RetryNextOffset,
		RetryCount:      result.RetryCount,
	})
}

func (s *TCPServer) handleProcessRetryFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.ProcessRetryRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	result, err := s.broker.ProcessRetryBatch(ctx, req.Consumer, req.SourceTopic, req.Partition, req.MaxRecords)
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdProcessRetryResponse, protocol.ProcessRetryResponse{
		RetryTopic:          result.RetryTopic,
		Partition:           result.Partition,
		MovedToOrigin:       result.MovedToOrigin,
		MovedToDeadLetter:   result.MovedToDeadLetter,
		CommittedNextOffset: result.CommittedNextOffset,
	})
}

func (s *TCPServer) handleScheduleDelayFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.ScheduleDelayRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	if len(req.Payload) > s.limits.MaxPayloadBytes {
		msg := fmt.Sprintf("schedule_delay payload size %d exceeds limit %d", len(req.Payload), s.limits.MaxPayloadBytes)
		return errorFrame("payload_too_large", msg), nil
	}
	result, err := s.broker.ScheduleDelay(ctx, req.Topic, req.Partition, req.Payload, req.DeliverAtMS)
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdScheduleDelayResponse, protocol.ScheduleDelayResponse{
		DelayTopic:  result.DelayTopic,
		Partition:   result.Partition,
		Offset:      result.Offset,
		NextOffset:  result.NextOffset,
		DeliverAtMS: result.DeliverAtMS,
	})
}
