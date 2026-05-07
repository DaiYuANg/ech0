package direct

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/DaiYuANg/ech0/queue"
	"github.com/DaiYuANg/ech0/store"
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
	Recipient     string
	Records       []InboxRecord
	NextOffset    uint64
	HighWatermark *uint64
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

func (r *Runtime) Send(sender string, recipient string, conversationID *string, payload []byte) (SendResult, error) {
	if sender == "" || recipient == "" {
		return SendResult{}, store.E(store.CodeInvalidArgument, "sender and recipient are required")
	}
	if err := r.ensureInbox(recipient); err != nil {
		return SendResult{}, err
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
		return SendResult{}, err
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
		return FetchInboxResult{}, err
	}
	records := make([]InboxRecord, 0, len(poll.Records))
	for _, record := range poll.Records {
		decoded, err := decodeRecord(record)
		if err != nil {
			return FetchInboxResult{}, err
		}
		records = append(records, decoded)
	}
	return FetchInboxResult{
		Recipient:     recipient,
		Records:       records,
		NextOffset:    poll.NextOffset,
		HighWatermark: poll.HighWatermark,
	}, nil
}

func (r *Runtime) AckInbox(recipient string, nextOffset uint64) error {
	return r.queue.Ack(consumerName(recipient), inboxTopic(recipient), 0, nextOffset)
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
		return err
	}
	cfg := inboxTopicConfig(topicName)
	if exists {
		return r.meta.SaveTopicConfig(cfg)
	}
	if err := r.queue.CreateTopic(cfg); err != nil && store.ErrorCode(err) != store.CodeTopicExists {
		return err
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

func defaultConversationID(a string, b string) string {
	participants := []string{a, b}
	sort.Strings(participants)
	return participants[0] + ":" + participants[1]
}

func nextMessageID() string {
	return fmt.Sprintf("dm-%d-%d", store.NowMS(), directSequence.Add(1))
}

func header(key string, value string) store.RecordHeader {
	return store.RecordHeader{Key: key, Value: []byte(value)}
}

func decodeRecord(record store.Record) (InboxRecord, error) {
	if (record.Attributes & directAttributeFlag) == 0 {
		return InboxRecord{}, store.E(store.CodeCodec, "direct inbox record missing direct attribute")
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
		return InboxRecord{}, store.E(store.CodeCodec, "direct inbox record missing required headers")
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
