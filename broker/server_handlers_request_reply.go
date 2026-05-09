package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/DaiYuANg/ech0/direct"
	"github.com/DaiYuANg/ech0/protocol"
	"github.com/DaiYuANg/ech0/store"
	"github.com/DaiYuANg/ech0/transport"
	collectionlist "github.com/arcgolabs/collectionx/list"
)

func (s *TCPServer) handleStartRequestFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.StartRequestRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	if len(req.Payload) > s.limits.MaxPayloadBytes {
		msg := fmt.Sprintf("request payload size %d exceeds limit %d", len(req.Payload), s.limits.MaxPayloadBytes)
		return errorFrame("payload_too_large", msg), nil
	}
	pending, err := s.broker.StartRequest(ctx, req.Subject, req.Payload, requestOptionsFromProtocol(req))
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdStartRequestResponse, startRequestResponseFromBroker(pending))
}

func (s *TCPServer) handleFetchRequestsFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.FetchRequestsRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	result, err := s.broker.FetchRequestsWithIsolation(ctx, req.Consumer, req.Subject, req.Partition, req.Offset, req.MaxRecords, isolationFromProtocol(req.Isolation))
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdFetchRequestsResponse, fetchRequestsResponseFromBroker(result))
}

func (s *TCPServer) handleReplyFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.ReplyRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	result, err := s.broker.Reply(ctx, requestMessageFromReplyRequest(req), req.ResponderID, req.Payload)
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdReplyResponse, replyResponseFromDirect(result))
}

func (s *TCPServer) handleReplyErrorFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.ReplyErrorRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	result, err := s.broker.ReplyError(ctx, requestMessageFromReplyErrorRequest(req), req.ResponderID, req.Error)
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdReplyErrorResponse, replyResponseFromDirect(result))
}

func (s *TCPServer) handleAwaitReplyFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.AwaitReplyRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	reply, err := s.broker.AwaitReply(ctx, pendingRequestFromProtocol(req))
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdAwaitReplyResponse, protocol.AwaitReplyResponse{Reply: replyRecordFromBroker(reply)})
}

func requestOptionsFromProtocol(req protocol.StartRequestRequest) RequestOptions {
	return RequestOptions{
		InstanceID:   req.InstanceID,
		Timeout:      durationFromOptionalMS(req.TimeoutMS),
		PollInterval: durationFromOptionalMS(req.PollIntervalMS),
		Partitioning: partitioningFromProtocol(req.Partitioning, req.Partition),
		Headers:      storeHeadersFromProtocol(req.Headers),
	}
}

func startRequestResponseFromBroker(pending PendingRequest) protocol.StartRequestResponse {
	return protocol.StartRequestResponse{
		Subject:       pending.Subject,
		InstanceID:    pending.InstanceID,
		ReplyTo:       pending.ReplyTo,
		CorrelationID: pending.CorrelationID,
		ExpiresAtMS:   pending.ExpiresAtMS,
		Partition:     pending.Produce.Partition,
		Offset:        pending.Produce.Record.Offset,
		NextOffset:    pending.Produce.Record.Offset + 1,
	}
}

func fetchRequestsResponseFromBroker(result RequestPollResult) protocol.FetchRequestsResponse {
	return protocol.FetchRequestsResponse{
		Subject:       result.Subject,
		Partition:     result.Partition,
		Requests:      requestRecordsFromBroker(result.Requests),
		NextOffset:    result.NextOffset,
		HighWatermark: result.HighWatermark,
	}
}

func requestRecordsFromBroker(requests []RequestMessage) []protocol.RequestRecord {
	out := collectionlist.NewListWithCapacity[protocol.RequestRecord](len(requests))
	for i := range requests {
		req := requests[i]
		out.Add(protocol.RequestRecord{
			Offset:        req.Record.Offset,
			TimestampMS:   req.Record.TimestampMS,
			Subject:       req.Subject,
			ReplyTo:       req.ReplyTo,
			CorrelationID: req.CorrelationID,
			SenderID:      req.SenderID,
			ExpiresAtMS:   req.ExpiresAtMS,
			Headers:       protocolHeadersFromStore(req.Headers),
			Payload:       append([]byte(nil), req.Payload...),
		})
	}
	return out.Values()
}

func requestMessageFromReplyRequest(req protocol.ReplyRequest) RequestMessage {
	return RequestMessage{
		Subject:       req.Subject,
		ReplyTo:       req.ReplyTo,
		CorrelationID: req.CorrelationID,
		ExpiresAtMS:   req.ExpiresAtMS,
	}
}

func requestMessageFromReplyErrorRequest(req protocol.ReplyErrorRequest) RequestMessage {
	return RequestMessage{
		Subject:       req.Subject,
		ReplyTo:       req.ReplyTo,
		CorrelationID: req.CorrelationID,
		ExpiresAtMS:   req.ExpiresAtMS,
	}
}

func replyResponseFromDirect(result direct.SendResult) protocol.ReplyResponse {
	return protocol.ReplyResponse{
		MessageID:      result.MessageID,
		ConversationID: result.ConversationID,
		Offset:         result.Offset,
		NextOffset:     result.NextOffset,
	}
}

func pendingRequestFromProtocol(req protocol.AwaitReplyRequest) PendingRequest {
	expiresAtMS := req.ExpiresAtMS
	timeout := durationFromOptionalMS(req.TimeoutMS)
	if expiresAtMS == 0 {
		if timeout <= 0 {
			timeout = defaultRequestTimeout
		}
		expiresAtMS = store.NowMS() + durationMillis(timeout)
	}
	pollInterval := durationFromOptionalMS(req.PollIntervalMS)
	if pollInterval <= 0 {
		pollInterval = defaultRequestPollInterval
	}
	return PendingRequest{
		ReplyTo:       req.ReplyTo,
		CorrelationID: req.CorrelationID,
		ExpiresAtMS:   expiresAtMS,
		PollInterval:  pollInterval,
	}
}

func replyRecordFromBroker(reply ReplyMessage) protocol.ReplyRecord {
	return protocol.ReplyRecord{
		Offset:        reply.Offset,
		MessageID:     reply.Message.MessageID,
		TimestampMS:   reply.Message.TimestampMS,
		Subject:       reply.Subject,
		CorrelationID: reply.CorrelationID,
		ResponderID:   reply.SenderID,
		Error:         cloneStringPointer(reply.Error),
		Payload:       append([]byte(nil), reply.Payload...),
	}
}

func durationFromOptionalMS(value *uint64) time.Duration {
	if value == nil || *value == 0 {
		return 0
	}
	return time.Duration(safeUint64ToInt64(*value)) * time.Millisecond
}
