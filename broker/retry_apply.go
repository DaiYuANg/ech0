package broker

import (
	"context"
	"strconv"

	"github.com/DaiYuANg/ech0/store"
)

func (b *Broker) applyNack(ctx context.Context, req nackCommand) (RetryResult, error) {
	_ = ctx
	if req.Consumer == "" {
		return RetryResult{}, brokerStoreError(store.CodeInvalidArgument, "consumer is required")
	}
	sourceRecord, err := b.readRecordAtOffset(store.NewTopicPartition(req.Topic, req.Partition), req.Offset)
	if err != nil {
		return RetryResult{}, err
	}
	origin, err := retryOriginFromRecord(sourceRecord, req.Topic, req.Partition)
	if err != nil {
		return RetryResult{}, err
	}
	sourceTopic, err := b.loadTopicConfig(origin.Topic)
	if err != nil {
		return RetryResult{}, err
	}
	retryTopic := retryTopicName(origin.Topic)
	if ensureErr := b.ensureAuxTopic(*sourceTopic, retryTopic); ensureErr != nil {
		return RetryResult{}, ensureErr
	}
	return b.publishRetryRecord(req, sourceRecord, origin, *sourceTopic, retryTopic)
}

func (b *Broker) publishRetryRecord(
	req nackCommand,
	sourceRecord store.Record,
	origin retryOrigin,
	sourceTopic store.TopicConfig,
	retryTopic string,
) (RetryResult, error) {
	nextRetryCount := origin.RetryCount + 1
	deliverAtMS := store.NowMS() + retryBackoffMS(nextRetryCount, sourceTopic.RetryPolicy)
	retryRecord := retryRecordAppend(req, sourceRecord, origin, nextRetryCount, deliverAtMS)
	appended, err := b.queue.PublishRecord(retryTopic, origin.Partition, retryRecord)
	if err != nil {
		return RetryResult{}, wrapBroker("retry_record_publish_failed", err, "publish retry record")
	}
	return RetryResult{
		RetryTopic:      retryTopic,
		RetryPartition:  origin.Partition,
		RetryOffset:     appended.Offset,
		RetryNextOffset: appended.Offset + 1,
		RetryCount:      nextRetryCount,
		DeliverAtMS:     deliverAtMS,
	}, nil
}

func retryRecordAppend(
	req nackCommand,
	sourceRecord store.Record,
	origin retryOrigin,
	nextRetryCount uint32,
	deliverAtMS uint64,
) store.RecordAppend {
	retryRecord := cloneAsAppend(sourceRecord)
	upsertHeader(&retryRecord.Headers, retryHeaderOriginalTopic, origin.Topic)
	upsertHeader(&retryRecord.Headers, retryHeaderOriginalPartition, strconv.FormatUint(uint64(origin.Partition), 10))
	upsertHeader(&retryRecord.Headers, retryHeaderOriginalOffset, strconv.FormatUint(origin.Offset, 10))
	upsertHeader(&retryRecord.Headers, retryHeaderRetryCount, strconv.FormatUint(uint64(nextRetryCount), 10))
	upsertHeader(&retryRecord.Headers, retryHeaderDeliverAtMS, strconv.FormatUint(deliverAtMS, 10))
	upsertHeader(&retryRecord.Headers, retryHeaderLastError, stringValue(req.LastError, "unknown"))
	upsertHeader(&retryRecord.Headers, retryHeaderFailedConsumer, req.Consumer)
	return retryRecord
}

func (b *Broker) applyProcessRetryBatch(ctx context.Context, req processRetryCommand) (ProcessRetryResult, error) {
	_ = ctx
	if err := validateProcessRetryCommand(req); err != nil {
		return ProcessRetryResult{}, err
	}
	req.MaxRecords = b.processRetryMaxRecords(req.MaxRecords)
	retryTopic := retryTopicName(req.SourceTopic)
	records, err := b.fetchRetryRecords(req, retryTopic)
	if err != nil {
		return ProcessRetryResult{}, err
	}
	result := ProcessRetryResult{RetryTopic: retryTopic, Partition: req.Partition}
	lastProcessedOffset, err := b.processFetchedRetryRecords(req, records, &result)
	if err != nil {
		return ProcessRetryResult{}, err
	}
	if err := b.ackProcessedRetryRecords(req, retryTopic, lastProcessedOffset, &result); err != nil {
		return ProcessRetryResult{}, err
	}
	return result, nil
}

func validateProcessRetryCommand(req processRetryCommand) error {
	if req.Consumer == "" {
		return brokerStoreError(store.CodeInvalidArgument, "consumer is required")
	}
	return nil
}

func (b *Broker) processRetryMaxRecords(maxRecords int) int {
	if maxRecords <= 0 {
		return b.cfg.Broker.RetryWorkerMaxRecords
	}
	return maxRecords
}

func (b *Broker) fetchRetryRecords(req processRetryCommand, retryTopic string) ([]store.Record, error) {
	fetched, err := b.queue.Fetch(req.Consumer, retryTopic, req.Partition, nil, req.MaxRecords)
	if err != nil {
		return nil, wrapBroker("retry_fetch_failed", err, "fetch retry records")
	}
	return fetched.Records, nil
}

func (b *Broker) processFetchedRetryRecords(
	req processRetryCommand,
	records []store.Record,
	result *ProcessRetryResult,
) (*uint64, error) {
	var lastProcessedOffset *uint64
	nowMS := processRetryNowMS(req.NowMS)
	for _, retryRecord := range records {
		ready, err := retryRecordReady(retryRecord, nowMS)
		if err != nil {
			return nil, err
		}
		if !ready {
			break
		}
		deadLettered, err := b.processDueRetryRecord(retryRecord, req.SourceTopic, req.Partition)
		if err != nil {
			return nil, err
		}
		recordRetryMove(result, deadLettered)
		next := retryRecord.Offset + 1
		lastProcessedOffset = &next
	}
	return lastProcessedOffset, nil
}

func processRetryNowMS(nowMS uint64) uint64 {
	if nowMS == 0 {
		return store.NowMS()
	}
	return nowMS
}

func retryRecordReady(retryRecord store.Record, nowMS uint64) (bool, error) {
	deliverAt, err := headerUint64(retryRecord.Headers, retryHeaderDeliverAtMS)
	if err != nil {
		return false, err
	}
	return deliverAt == nil || *deliverAt <= nowMS, nil
}

func (b *Broker) processDueRetryRecord(retryRecord store.Record, sourceTopic string, partition uint32) (bool, error) {
	origin, err := retryOriginFromRecord(retryRecord, sourceTopic, partition)
	if err != nil {
		return false, err
	}
	originTopic, err := b.loadTopicConfig(origin.Topic)
	if err != nil {
		return false, err
	}
	return b.moveRetryRecord(retryRecord, origin, *originTopic)
}

func recordRetryMove(result *ProcessRetryResult, deadLettered bool) {
	if deadLettered {
		result.MovedToDeadLetter++
		return
	}
	result.MovedToOrigin++
}

func (b *Broker) ackProcessedRetryRecords(
	req processRetryCommand,
	retryTopic string,
	lastProcessedOffset *uint64,
	result *ProcessRetryResult,
) error {
	if lastProcessedOffset == nil {
		return nil
	}
	if err := b.queue.Ack(req.Consumer, retryTopic, req.Partition, *lastProcessedOffset); err != nil {
		return wrapBroker("retry_ack_failed", err, "ack retry records")
	}
	result.CommittedNextOffset = lastProcessedOffset
	return nil
}

func (b *Broker) moveRetryRecord(retryRecord store.Record, origin retryOrigin, originTopic store.TopicConfig) (bool, error) {
	if origin.RetryCount < originTopic.RetryPolicy.MaxAttempts {
		_, err := b.queue.PublishRecord(origin.Topic, origin.Partition, cloneAsAppend(retryRecord))
		return false, wrapBroker("retry_restore_failed", err, "publish retry record to origin topic")
	}
	deadLetterTopic := dlqTopicName(origin.Topic)
	if originTopic.DeadLetterTopic != nil && *originTopic.DeadLetterTopic != "" {
		deadLetterTopic = *originTopic.DeadLetterTopic
	}
	if err := b.ensureAuxTopic(originTopic, deadLetterTopic); err != nil {
		return false, err
	}
	_, err := b.queue.PublishRecord(deadLetterTopic, origin.Partition, buildDLQAppend(retryRecord, origin))
	return true, wrapBroker("retry_dead_letter_failed", err, "publish retry record to dead letter topic")
}
