package broker

import (
	"context"
	"fmt"

	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/lyonbrown4d/ech0/transport"
)

func (s *TCPServer) handleProduceFanoutFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.ProduceFanoutRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	if len(req.Payload) > s.limits.MaxPayloadBytes {
		msg := fmt.Sprintf("produce_fanout payload size %d exceeds limit %d", len(req.Payload), s.limits.MaxPayloadBytes)
		return errorFrame("payload_too_large", msg), nil
	}
	record := store.NewRecordAppend(req.Payload)
	record.Key = append([]byte(nil), req.Key...)
	record.Headers = storeHeadersFromProtocol(req.Headers)
	record.ExpiresAtMS = cloneUint64Ptr(req.ExpiresAtMS)
	applyRoutingKey(&record, req.RoutingKey)
	if req.Tombstone {
		record.Attributes |= store.RecordAttributeTombstone
	}
	result, err := s.broker.PublishFanoutRecord(ctx, req.Topic, record)
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdProduceFanoutResponse, fanoutResultToProtocol(result))
}
