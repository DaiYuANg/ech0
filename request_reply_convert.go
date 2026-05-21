package ech0

import (
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	internalbroker "github.com/lyonbrown4d/ech0/broker"
)

func pendingRequestFromBroker(pending internalbroker.PendingRequest) PendingRequest {
	return PendingRequest{
		Subject:       pending.Subject,
		InstanceID:    pending.InstanceID,
		ReplyTo:       pending.ReplyTo,
		CorrelationID: pending.CorrelationID,
		ExpiresAt:     time.UnixMilli(unixMillis(pending.ExpiresAtMS)),
		PollInterval:  pending.PollInterval,
		ReplyMode:     pending.ReplyMode,
		Message:       messageFromRecord(pending.Subject, pending.Produce.Partition, pending.Produce.Record),
	}
}

func pendingRequestToBroker(pending PendingRequest) internalbroker.PendingRequest {
	return internalbroker.PendingRequest{
		Subject:       pending.Subject,
		InstanceID:    pending.InstanceID,
		ReplyTo:       pending.ReplyTo,
		CorrelationID: pending.CorrelationID,
		ExpiresAtMS:   timeToUnixMillis(pending.ExpiresAt),
		PollInterval:  pending.PollInterval,
		ReplyMode:     pending.ReplyMode,
	}
}

func requestFetchResultFromBroker(result internalbroker.RequestPollResult) RequestFetchResult {
	return RequestFetchResult{
		Subject:        result.Subject,
		Partition:      result.Partition,
		Requests:       requestMessagesFromBroker(result.Subject, result.Partition, result.Requests),
		NextOffset:     result.NextOffset,
		HighWatermark:  result.HighWatermark,
		LowWatermark:   result.LowWatermark,
		LogStartOffset: result.LogStartOffset,
	}
}

func requestMessagesFromBroker(subject string, partition uint32, requests []internalbroker.RequestMessage) []RequestMessage {
	if len(requests) == 0 {
		return nil
	}
	out := collectionlist.NewListWithCapacity[RequestMessage](len(requests))
	for index := range requests {
		out.Add(requestMessageFromBroker(subject, partition, requests[index]))
	}
	return out.Values()
}

func requestMessageFromBroker(subject string, partition uint32, req internalbroker.RequestMessage) RequestMessage {
	return RequestMessage{
		Subject:       req.Subject,
		ReplyTo:       req.ReplyTo,
		CorrelationID: req.CorrelationID,
		SenderID:      req.SenderID,
		ExpiresAt:     time.UnixMilli(unixMillis(req.ExpiresAtMS)),
		ReplyMode:     req.ReplyMode,
		Headers:       headersFromStore(req.Headers),
		Payload:       append([]byte(nil), req.Payload...),
		Message: Message{
			Topic:      subject,
			Partition:  partition,
			Offset:     req.Record.Offset,
			Timestamp:  time.UnixMilli(unixMillis(req.Record.TimestampMS)),
			Headers:    headersFromStore(req.Headers),
			Payload:    append([]byte(nil), req.Payload...),
			NextOffset: req.Record.Offset + 1,
		},
	}
}

func requestMessageToBroker(req RequestMessage) internalbroker.RequestMessage {
	return internalbroker.RequestMessage{
		Subject:       req.Subject,
		ReplyTo:       req.ReplyTo,
		CorrelationID: req.CorrelationID,
		SenderID:      req.SenderID,
		ExpiresAtMS:   timeToUnixMillis(req.ExpiresAt),
		ReplyMode:     req.ReplyMode,
		Headers:       headersToStore(req.Headers),
		Payload:       append([]byte(nil), req.Payload...),
	}
}

func replyMessageFromBroker(reply internalbroker.ReplyMessage) ReplyMessage {
	return ReplyMessage{
		Offset:        reply.Offset,
		Subject:       reply.Subject,
		CorrelationID: reply.CorrelationID,
		ResponderID:   reply.SenderID,
		Error:         cloneString(reply.Error),
		Payload:       append([]byte(nil), reply.Payload...),
		Direct: DirectMessage{
			Offset:         reply.Offset,
			MessageID:      reply.Message.MessageID,
			ConversationID: reply.Message.ConversationID,
			Sender:         reply.Message.Sender,
			Recipient:      reply.Message.Recipient,
			Timestamp:      time.UnixMilli(unixMillis(reply.Message.TimestampMS)),
			Payload:        append([]byte(nil), reply.Message.Payload...),
		},
	}
}

func timeToUnixMillis(value time.Time) uint64 {
	if value.IsZero() {
		return 0
	}
	millis := value.UnixMilli()
	if millis <= 0 {
		return 0
	}
	return uint64(millis)
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}
