package broker

import (
	"context"
	"fmt"
	"strconv"

	"github.com/lyonbrown4d/ech0/store"
)

const messageExpiryScanBatch = 1024

func (b *Broker) deadLetterExpiredMessages(ctx context.Context, nowMS uint64) (int, error) {
	topics, err := b.queue.ListTopics()
	if err != nil {
		return 0, wrapBroker("message_expiry_list_topics_failed", err, "list topics for message expiry")
	}
	total := 0
	for i := range topics {
		moved, moveErr := b.deadLetterExpiredTopicMessages(ctx, topics[i], nowMS)
		if moveErr != nil {
			return total, moveErr
		}
		total += moved
	}
	return total, nil
}

func (b *Broker) deadLetterExpiredTopicMessages(ctx context.Context, topic store.TopicConfig, nowMS uint64) (int, error) {
	if topic.MessageExpiryAction != store.MessageExpiryDLQ || isInternalTopicName(topic.Name) {
		return 0, nil
	}
	deadLetterTopic := expiredMessageDLQTopic(topic)
	if err := b.ensureAuxTopic(topic, deadLetterTopic); err != nil {
		return 0, err
	}
	total := 0
	for partition := range topic.Partitions {
		moved, err := b.deadLetterExpiredPartitionMessages(ctx, topic.Name, deadLetterTopic, partition, nowMS)
		if err != nil {
			return total, err
		}
		total += moved
	}
	return total, nil
}

func expiredMessageDLQTopic(topic store.TopicConfig) string {
	if topic.DeadLetterTopic != nil && *topic.DeadLetterTopic != "" {
		return *topic.DeadLetterTopic
	}
	return dlqTopicName(topic.Name)
}

func (b *Broker) deadLetterExpiredPartitionMessages(ctx context.Context, sourceTopic, deadLetterTopic string, partition uint32, nowMS uint64) (int, error) {
	cursor := uint64(0)
	moved := 0
	for {
		records, err := b.readMessageExpiryBatch(sourceTopic, partition, cursor)
		if err != nil {
			return moved, err
		}
		if len(records) == 0 {
			return moved, nil
		}
		batchMoved, moveErr := b.deadLetterExpiredRecords(ctx, sourceTopic, deadLetterTopic, partition, nowMS, records)
		if moveErr != nil {
			return moved, moveErr
		}
		moved += batchMoved
		cursor = records[len(records)-1].Offset + 1
		if len(records) < messageExpiryScanBatch {
			return moved, nil
		}
	}
}

func (b *Broker) readMessageExpiryBatch(sourceTopic string, partition uint32, cursor uint64) ([]store.Record, error) {
	records, err := b.queue.ReadFrom(store.NewTopicPartition(sourceTopic, partition), cursor, messageExpiryScanBatch)
	if err != nil {
		return nil, wrapBrokerStore(err, "read records for message expiry")
	}
	return records, nil
}

func (b *Broker) deadLetterExpiredRecords(
	ctx context.Context,
	sourceTopic string,
	deadLetterTopic string,
	partition uint32,
	nowMS uint64,
	records []store.Record,
) (int, error) {
	moved := 0
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return moved, wrapBroker("message_expiry_canceled", err, "dead letter expired messages")
		}
		if !recordExpiresAtOrBefore(record, nowMS) {
			continue
		}
		appendRecord := buildExpiredMessageDLQAppend(record, sourceTopic, partition)
		appended, err := b.queue.PublishRecord(deadLetterTopic, partition, appendRecord)
		if err != nil {
			return moved, wrapBroker("message_expiry_dead_letter_failed", err, "publish expired message to dead letter topic")
		}
		if err := b.saveDLQIndexFromRecord(deadLetterTopic, partition, appended); err != nil {
			return moved, err
		}
		moved++
	}
	return moved, nil
}

func recordExpiresAtOrBefore(record store.Record, nowMS uint64) bool {
	return record.ExpiresAtMS != nil && *record.ExpiresAtMS <= nowMS
}

func buildExpiredMessageDLQAppend(record store.Record, sourceTopic string, partition uint32) store.RecordAppend {
	appendRecord := store.NewRecordAppend(record.Payload)
	appendRecord.TimestampMS = &record.TimestampMS
	appendRecord.Key = append([]byte(nil), record.Key...)
	appendRecord.Attributes = record.Attributes
	appendRecord.Headers = removeHeaders(cloneRecordHeaders(record.Headers), internalDLQAndRetryHeaders()...)
	upsertHeader(&appendRecord.Headers, dlqHeaderOriginalTopic, sourceTopic)
	upsertHeader(&appendRecord.Headers, dlqHeaderOriginalPartition, strconv.FormatUint(uint64(partition), 10))
	upsertHeader(&appendRecord.Headers, dlqHeaderOriginalOffset, strconv.FormatUint(record.Offset, 10))
	upsertHeader(&appendRecord.Headers, dlqHeaderRetryCount, "0")
	upsertHeader(&appendRecord.Headers, dlqHeaderErrorCode, dlqErrorCodeMessageExpired)
	upsertHeader(&appendRecord.Headers, dlqHeaderErrorMessage, expiredMessageError(record))
	return appendRecord
}

func expiredMessageError(record store.Record) string {
	if record.ExpiresAtMS == nil {
		return "message expired"
	}
	return fmt.Sprintf("message expired at %d", *record.ExpiresAtMS)
}
