package broker

import (
	"context"
	"strconv"

	"github.com/lyonbrown4d/ech0/store"
)

const dlqErrorCodePoisonMessage = "poison_message"

type PoisonMessageResult struct {
	Topic         string
	Partition     uint32
	Offset        uint64
	NextOffset    uint64
	Record        *store.Record
	DeadLetter    *DLQRecord
	Committed     bool
	CommittedNext *uint64
	ReplayResult  *DLQReplayResult
}

func (b *Broker) InspectPoisonMessage(ctx context.Context, topic string, partition uint32, offset uint64) (PoisonMessageResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionConsume, topicResource(identity, topic)); err != nil {
		return PoisonMessageResult{}, err
	}
	scopedTopic := scopedTopicName(identity, topic)
	record, err := b.readRecordAtOffset(store.NewTopicPartition(scopedTopic, partition), offset)
	if err != nil {
		return PoisonMessageResult{}, err
	}
	recordCopy := record
	return PoisonMessageResult{
		Topic:      topic,
		Partition:  partition,
		Offset:     offset,
		NextOffset: offset + 1,
		Record:     &recordCopy,
	}, nil
}

func (b *Broker) SkipPoisonMessage(ctx context.Context, consumer, topic string, partition uint32, offset uint64) (PoisonMessageResult, error) {
	nextOffset := offset + 1
	if err := b.CommitOffset(ctx, consumer, topic, partition, nextOffset); err != nil {
		return PoisonMessageResult{}, err
	}
	return PoisonMessageResult{
		Topic:         topic,
		Partition:     partition,
		Offset:        offset,
		NextOffset:    nextOffset,
		Committed:     true,
		CommittedNext: &nextOffset,
	}, nil
}

func (b *Broker) IsolatePoisonMessage(
	ctx context.Context,
	consumer string,
	topic string,
	partition uint32,
	offset uint64,
	reason string,
) (PoisonMessageResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionConsume, topicResource(identity, topic)); err != nil {
		return PoisonMessageResult{}, err
	}
	if err := b.authorize(ctx, identity, ACLActionCommit, topicResource(identity, topic)); err != nil {
		return PoisonMessageResult{}, err
	}
	scopedTopic := scopedTopicName(identity, topic)
	record, err := b.readRecordAtOffset(store.NewTopicPartition(scopedTopic, partition), offset)
	if err != nil {
		return PoisonMessageResult{}, err
	}
	deadLetter, err := b.isolatePoisonRecord(scopedTopic, partition, record, reason)
	if err != nil {
		return PoisonMessageResult{}, err
	}
	nextOffset := offset + 1
	if err := b.CommitOffset(ctx, consumer, topic, partition, nextOffset); err != nil {
		return PoisonMessageResult{}, err
	}
	deadLetter.DLQTopic = visibleTopicName(identity, deadLetter.DLQTopic)
	deadLetter.OriginalTopic = visibleTopicName(identity, deadLetter.OriginalTopic)
	return PoisonMessageResult{
		Topic:         topic,
		Partition:     partition,
		Offset:        offset,
		NextOffset:    nextOffset,
		DeadLetter:    deadLetter,
		Committed:     true,
		CommittedNext: &nextOffset,
	}, nil
}

func (b *Broker) ReplayPoisonMessage(ctx context.Context, sourceTopic string, dlqPartition uint32, dlqOffset uint64) (PoisonMessageResult, error) {
	result, err := b.ReplayDLQ(ctx, sourceTopic, dlqPartition, dlqOffset)
	if err != nil {
		return PoisonMessageResult{}, err
	}
	return PoisonMessageResult{
		Topic:        result.Topic,
		Partition:    result.Partition,
		Offset:       result.Offset,
		NextOffset:   result.NextOffset,
		ReplayResult: &result,
	}, nil
}

func (b *Broker) isolatePoisonRecord(scopedTopic string, partition uint32, record store.Record, reason string) (*DLQRecord, error) {
	sourceTopic, err := b.loadTopicConfig(scopedTopic)
	if err != nil {
		return nil, err
	}
	deadLetterTopic := dlqTopicName(scopedTopic)
	if sourceTopic.DeadLetterTopic != nil && *sourceTopic.DeadLetterTopic != "" {
		deadLetterTopic = *sourceTopic.DeadLetterTopic
	}
	if ensureErr := b.ensureAuxTopic(*sourceTopic, deadLetterTopic); ensureErr != nil {
		return nil, ensureErr
	}
	appended, err := b.queue.PublishRecord(deadLetterTopic, partition, buildPoisonDLQAppend(record, scopedTopic, partition, reason))
	if err != nil {
		return nil, wrapBroker("poison_dead_letter_failed", err, "publish poison message to dead letter topic")
	}
	if indexErr := b.saveDLQIndexFromRecord(deadLetterTopic, partition, appended); indexErr != nil {
		return nil, indexErr
	}
	dlqRecord, err := dlqRecordFromStore(deadLetterTopic, partition, appended)
	if err != nil {
		return nil, err
	}
	return &dlqRecord, nil
}

func buildPoisonDLQAppend(record store.Record, sourceTopic string, partition uint32, reason string) store.RecordAppend {
	appendRecord := store.NewRecordAppend(record.Payload)
	appendRecord.TimestampMS = &record.TimestampMS
	appendRecord.ExpiresAtMS = nil
	appendRecord.Key = append([]byte(nil), record.Key...)
	appendRecord.Attributes = record.Attributes
	appendRecord.Headers = removeHeaders(cloneRecordHeaders(record.Headers), internalDLQAndRetryHeaders()...)
	upsertHeader(&appendRecord.Headers, dlqHeaderOriginalTopic, sourceTopic)
	upsertHeader(&appendRecord.Headers, dlqHeaderOriginalPartition, strconv.FormatUint(uint64(partition), 10))
	upsertHeader(&appendRecord.Headers, dlqHeaderOriginalOffset, strconv.FormatUint(record.Offset, 10))
	upsertHeader(&appendRecord.Headers, dlqHeaderRetryCount, "0")
	upsertHeader(&appendRecord.Headers, dlqHeaderErrorCode, dlqErrorCodePoisonMessage)
	upsertHeader(&appendRecord.Headers, dlqHeaderErrorMessage, poisonMessageReason(reason))
	return appendRecord
}

func poisonMessageReason(reason string) string {
	if reason == "" {
		return "poison message isolated"
	}
	return reason
}
