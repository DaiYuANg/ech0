package ech0

import (
	"context"
	"time"

	internaldirect "github.com/lyonbrown4d/ech0/direct"
	"github.com/samber/oops"
)

type DirectOption func(*directOptions)

type directOptions struct {
	conversationID *string
}

type InboxOption func(*inboxOptions)

type inboxOptions struct {
	maxRecords int
}

type DirectSendResult struct {
	MessageID      string
	ConversationID string
	Offset         uint64
	NextOffset     uint64
}

type DirectMessage struct {
	Offset         uint64
	MessageID      string
	ConversationID string
	Sender         string
	Recipient      string
	Timestamp      time.Time
	Payload        []byte
}

type DirectInboxResult struct {
	Recipient      string
	Messages       []DirectMessage
	NextOffset     uint64
	HighWatermark  *uint64
	LowWatermark   *uint64
	LogStartOffset uint64
}

func ConversationID(value string) DirectOption {
	return func(opts *directOptions) {
		if value != "" {
			opts.conversationID = &value
		}
	}
}

func InboxLimit(maxRecords int) InboxOption {
	return func(opts *inboxOptions) {
		if maxRecords > 0 {
			opts.maxRecords = maxRecords
		}
	}
}

func (b *Broker) SendDirect(ctx context.Context, sender, recipient string, payload []byte, opts ...DirectOption) (DirectSendResult, error) {
	directOpts := directOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&directOpts)
		}
	}
	result, err := b.broker.SendDirect(ctx, sender, recipient, directOpts.conversationID, payload)
	if err != nil {
		return DirectSendResult{}, oops.In("embedded").Code("direct_send_failed").With("recipient", recipient).Wrapf(err, "send direct message")
	}
	return directSendResultFromBroker(result), nil
}

func (b *Broker) FetchInbox(ctx context.Context, recipient string, opts ...InboxOption) (DirectInboxResult, error) {
	inboxOpts := inboxOptions{maxRecords: 100}
	for _, opt := range opts {
		if opt != nil {
			opt(&inboxOpts)
		}
	}
	result, err := b.broker.FetchInboxFor(ctx, recipient, inboxOpts.maxRecords)
	if err != nil {
		return DirectInboxResult{}, oops.In("embedded").Code("direct_fetch_failed").With("recipient", recipient).Wrapf(err, "fetch direct inbox")
	}
	return directInboxResultFromBroker(result), nil
}

func (b *Broker) AckDirect(ctx context.Context, recipient string, nextOffset uint64) error {
	return oops.In("embedded").Code("direct_ack_failed").With("recipient", recipient).Wrapf(
		b.broker.AckDirect(ctx, recipient, nextOffset),
		"ack direct inbox",
	)
}

func directSendResultFromBroker(result internaldirect.SendResult) DirectSendResult {
	return DirectSendResult{
		MessageID:      result.MessageID,
		ConversationID: result.ConversationID,
		Offset:         result.Offset,
		NextOffset:     result.NextOffset,
	}
}

func directInboxResultFromBroker(result internaldirect.FetchInboxResult) DirectInboxResult {
	return DirectInboxResult{
		Recipient:      result.Recipient,
		Messages:       directMessagesFromBroker(result.Records),
		NextOffset:     result.NextOffset,
		HighWatermark:  result.HighWatermark,
		LowWatermark:   result.LowWatermark,
		LogStartOffset: result.LogStartOffset,
	}
}

func directMessagesFromBroker(records []internaldirect.InboxRecord) []DirectMessage {
	if len(records) == 0 {
		return nil
	}
	out := make([]DirectMessage, 0, len(records))
	for index := range records {
		out = append(out, directMessageFromBroker(records[index]))
	}
	return out
}

func directMessageFromBroker(record internaldirect.InboxRecord) DirectMessage {
	message := record.Message
	return DirectMessage{
		Offset:         record.Offset,
		MessageID:      message.MessageID,
		ConversationID: message.ConversationID,
		Sender:         message.Sender,
		Recipient:      message.Recipient,
		Timestamp:      time.UnixMilli(unixMillis(message.TimestampMS)),
		Payload:        append([]byte(nil), message.Payload...),
	}
}
