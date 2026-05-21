package ech0

import (
	"context"

	internalbroker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/samber/oops"
)

func (b *Broker) PublishFanout(ctx context.Context, topic string, payload []byte, opts ...PublishOption) ([]Message, error) {
	publishOpts := publishOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&publishOpts)
		}
	}
	record := store.NewRecordAppend(payload)
	record.Key = append([]byte(nil), publishOpts.key...)
	applyEmbeddedRoutingKey(&record, publishOpts.routingKey)
	applyEmbeddedPriority(&record, publishOpts.priority)
	record.ExpiresAtMS = cloneUint64(publishOpts.expiresAt)
	if publishOpts.tombstone {
		record.Attributes |= store.RecordAttributeTombstone
	}
	result, err := b.broker.PublishFanoutRecord(ctx, topic, record)
	if err != nil {
		return nil, oops.In("embedded").Code("publish_fanout_failed").With("topic", topic).Wrapf(err, "publish fanout message")
	}
	return fanoutMessagesFromBroker(topic, result.Records), nil
}

func (b *Broker) Broadcast(ctx context.Context, subject string, payload []byte, opts ...PublishOption) ([]Message, error) {
	return b.PublishFanout(ctx, subject, payload, opts...)
}

func fanoutMessagesFromBroker(topic string, records []internalbroker.FanoutRecordResult) []Message {
	messages := make([]Message, 0, len(records))
	for index := range records {
		record := &records[index]
		messages = append(messages, messageFromRecord(topic, record.Partition, record.Record))
	}
	return messages
}
