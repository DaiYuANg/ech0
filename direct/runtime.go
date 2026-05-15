// Package direct contains point-to-point messaging runtime support.
package direct

import (
	"cmp"
	"fmt"
	"strings"
	"sync/atomic"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/queue"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/samber/oops"
)

const (
	InternalInboxTopicPrefix = "__direct.inbox"
	InboxShardCount          = 256
	inboxSegmentMaxBytes     = 1024 * 1024
	inboxIndexIntervalBytes  = 1024
	directAttributeFlag      = 1 << 0
	headerMessageID          = "x-direct-message-id"
	headerConversationID     = "x-direct-conversation-id"
	headerSender             = "x-direct-sender"
	headerRecipient          = "x-direct-recipient"
)

var directSequence atomic.Uint64

type Message struct {
	MessageID      string
	ConversationID string
	Sender         string
	Recipient      string
	TimestampMS    uint64
	Payload        []byte
}

type SendResult struct {
	MessageID      string
	ConversationID string
	Offset         uint64
	NextOffset     uint64
}

type InboxRecord struct {
	Offset  uint64
	Message Message
}

type FetchInboxResult struct {
	Recipient      string
	Records        []InboxRecord
	NextOffset     uint64
	HighWatermark  *uint64
	LowWatermark   *uint64
	LogStartOffset uint64
}

type Runtime struct {
	queue *queue.Runtime
	log   store.MessageLogStore
	meta  interface {
		store.OffsetStore
		store.TopicCatalogStore
	}
}

func New(log store.MessageLogStore, meta interface {
	store.OffsetStore
	store.TopicCatalogStore
}) *Runtime {
	return &Runtime{queue: queue.New(log, meta), log: log, meta: meta}
}

func (r *Runtime) Send(sender, recipient string, conversationID *string, payload []byte) (SendResult, error) {
	if sender == "" || recipient == "" {
		return SendResult{}, oops.In("direct").Code("invalid_message_target").Wrapf(store.E(store.CodeInvalidArgument, "sender and recipient are required"), "validate direct message")
	}
	if err := r.ensureInbox(recipient); err != nil {
		return SendResult{}, oops.In("direct").Code("ensure_inbox_failed").With("recipient", recipient).Wrapf(err, "ensure inbox")
	}
	cid := defaultConversationID(sender, recipient)
	if conversationID != nil && *conversationID != "" {
		cid = *conversationID
	}
	message := Message{
		MessageID:      nextMessageID(),
		ConversationID: cid,
		Sender:         sender,
		Recipient:      recipient,
		TimestampMS:    store.NowMS(),
		Payload:        append([]byte(nil), payload...),
	}
	ts := message.TimestampMS
	record, err := r.queue.PublishRecord(inboxTopic(recipient), 0, store.RecordAppend{
		TimestampMS: &ts,
		Headers: []store.RecordHeader{
			header(headerMessageID, message.MessageID),
			header(headerConversationID, message.ConversationID),
			header(headerSender, message.Sender),
			header(headerRecipient, message.Recipient),
		},
		Attributes: directAttributeFlag,
		Payload:    message.Payload,
	})
	if err != nil {
		return SendResult{}, oops.In("direct").Code("publish_direct_failed").With("recipient", recipient).Wrapf(err, "publish direct message")
	}
	return SendResult{
		MessageID:      message.MessageID,
		ConversationID: message.ConversationID,
		Offset:         record.Offset,
		NextOffset:     record.Offset + 1,
	}, nil
}

func (r *Runtime) FetchInbox(recipient string, maxRecords int) (FetchInboxResult, error) {
	poll, err := r.queue.Fetch(consumerName(recipient), inboxTopic(recipient), 0, nil, maxRecords)
	if err != nil {
		if store.ErrorCode(err) == store.CodeTopicNotFound {
			return FetchInboxResult{Recipient: recipient, Records: nil, NextOffset: 0}, nil
		}
		return FetchInboxResult{}, oops.In("direct").Code("fetch_inbox_failed").With("recipient", recipient).Wrapf(err, "fetch inbox")
	}
	records := collectionlist.NewListWithCapacity[InboxRecord](len(poll.Records))
	for _, record := range poll.Records {
		decoded, err := decodeRecord(record)
		if err != nil {
			return FetchInboxResult{}, oops.In("direct").Code("decode_inbox_record_failed").With("recipient", recipient).Wrapf(err, "decode inbox record")
		}
		records.Add(decoded)
	}
	return FetchInboxResult{
		Recipient:      recipient,
		Records:        records.Values(),
		NextOffset:     poll.NextOffset,
		HighWatermark:  poll.HighWatermark,
		LowWatermark:   poll.LowWatermark,
		LogStartOffset: poll.LogStartOffset,
	}, nil
}

func (r *Runtime) AckInbox(recipient string, nextOffset uint64) error {
	return oops.In("direct").Code("ack_inbox_failed").With("recipient", recipient).Wrapf(
		r.queue.Ack(consumerName(recipient), inboxTopic(recipient), 0, nextOffset),
		"ack inbox",
	)
}

func IsInternalTopicName(topic string) bool {
	return topic == InternalInboxTopicPrefix ||
		strings.HasPrefix(topic, InternalInboxTopicPrefix+".") ||
		strings.HasPrefix(topic, InternalInboxTopicPrefix+"/")
}

func (r *Runtime) ensureInbox(recipient string) error {
	topicName := inboxTopic(recipient)
	exists, err := r.log.TopicExists(topicName)
	if err != nil {
		return oops.In("direct").Code("check_inbox_topic_failed").With("topic", topicName).Wrapf(err, "check inbox topic")
	}
	cfg := inboxTopicConfig(topicName)
	if exists {
		return oops.In("direct").Code("save_inbox_topic_failed").With("topic", topicName).Wrapf(r.meta.SaveTopicConfig(cfg), "save inbox topic")
	}
	if err := r.queue.CreateTopic(cfg); err != nil && store.ErrorCode(err) != store.CodeTopicExists {
		return oops.In("direct").Code("create_inbox_topic_failed").With("topic", topicName).Wrapf(err, "create inbox topic")
	}
	return nil
}

func inboxTopicConfig(name string) store.TopicConfig {
	cfg := store.NewTopicConfig(name)
	cfg.Partitions = 1
	cfg.SegmentMaxBytes = inboxSegmentMaxBytes
	cfg.IndexIntervalBytes = inboxIndexIntervalBytes
	return cfg
}

func inboxTopic(recipient string) string {
	return fmt.Sprintf("%s/%02x/%s", InternalInboxTopicPrefix, shard(recipient), hexEncode(recipient))
}

func consumerName(recipient string) string {
	return "__direct.consumer/" + recipient
}

func shard(recipient string) uint8 {
	var hash uint64
	for _, b := range []byte(recipient) {
		hash = hash*31 + uint64(b)
	}
	return uint8(hash % InboxShardCount)
}

func defaultConversationID(a, b string) string {
	participants := collectionlist.NewList(a, b).
		Sort(cmp.Compare[string]).
		Values()
	return participants[0] + ":" + participants[1]
}

func nextMessageID() string {
	return fmt.Sprintf("dm-%d-%d", store.NowMS(), directSequence.Add(1))
}

func header(key, value string) store.RecordHeader {
	return store.RecordHeader{Key: key, Value: []byte(value)}
}

func decodeRecord(record store.Record) (InboxRecord, error) {
	if (record.Attributes & directAttributeFlag) == 0 {
		return InboxRecord{}, oops.In("direct").Code("invalid_inbox_record").Wrapf(store.E(store.CodeCodec, "direct inbox record missing direct attribute"), "decode inbox record")
	}
	message := Message{
		MessageID:      requiredHeader(record.Headers, headerMessageID),
		ConversationID: requiredHeader(record.Headers, headerConversationID),
		Sender:         requiredHeader(record.Headers, headerSender),
		Recipient:      requiredHeader(record.Headers, headerRecipient),
		TimestampMS:    record.TimestampMS,
		Payload:        append([]byte(nil), record.Payload...),
	}
	if message.MessageID == "" || message.ConversationID == "" || message.Sender == "" || message.Recipient == "" {
		return InboxRecord{}, oops.In("direct").Code("invalid_inbox_record").Wrapf(store.E(store.CodeCodec, "direct inbox record missing required headers"), "decode inbox record")
	}
	return InboxRecord{Offset: record.Offset, Message: message}, nil
}

func requiredHeader(headers []store.RecordHeader, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func hexEncode(value string) string {
	const alphabet = "0123456789abcdef"
	raw := []byte(value)
	out := make([]byte, len(raw)*2)
	for i, b := range raw {
		out[i*2] = alphabet[b>>4]
		out[i*2+1] = alphabet[b&0x0f]
	}
	return string(out)
}
