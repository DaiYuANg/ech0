//nolint:revive // Retry and delay internals are grouped by shared headers and topic conventions.
package broker

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/DaiYuANg/ech0/direct"
	"github.com/DaiYuANg/ech0/store"
	collectionset "github.com/arcgolabs/collectionx/set"
)

const (
	internalRetryTopicPrefix = "__retry"
	internalDelayTopicPrefix = "__delay"
	internalDLQTopicPrefix   = "__dlq"

	retryHeaderOriginalTopic     = "x-retry-original-topic"
	retryHeaderOriginalPartition = "x-retry-original-partition"
	retryHeaderOriginalOffset    = "x-retry-original-offset"
	retryHeaderRetryCount        = "x-retry-count"
	retryHeaderDeliverAtMS       = "x-retry-deliver-at-ms"
	retryHeaderLastError         = "x-retry-last-error"
	retryHeaderFailedConsumer    = "x-retry-failed-consumer"

	delayHeaderDeliverAtMS     = "x-delay-deliver-at-ms"
	delayHeaderTargetTopic     = "x-delay-target-topic"
	delayHeaderTargetPartition = "x-delay-target-partition"
	dlqHeaderOriginalTopic     = "x-dlq-original-topic"
	dlqHeaderOriginalPartition = "x-dlq-original-partition"
	dlqHeaderOriginalOffset    = "x-dlq-original-offset"
	dlqHeaderRetryCount        = "x-dlq-retry-count"
	dlqHeaderErrorCode         = "x-dlq-error-code"
	dlqHeaderErrorMessage      = "x-dlq-error-message"
	dlqErrorCodeRetryExhausted = "retry_exhausted"
	raftCommandNack            = "nack"
	raftCommandProcessRetry    = "process_retry"
	raftCommandScheduleDelay   = "schedule_delay"
	raftCommandProcessDelay    = "process_delay"
)

type RetryResult struct {
	RetryTopic      string
	RetryPartition  uint32
	RetryOffset     uint64
	RetryNextOffset uint64
	RetryCount      uint32
	DeliverAtMS     uint64
}

type ProcessRetryResult struct {
	RetryTopic          string
	Partition           uint32
	MovedToOrigin       int
	MovedToDeadLetter   int
	CommittedNextOffset *uint64
}

type DelayScheduleResult struct {
	DelayTopic  string
	Partition   uint32
	Offset      uint64
	NextOffset  uint64
	DeliverAtMS uint64
}

func (b *Broker) Nack(ctx context.Context, consumer, topic string, partition uint32, offset uint64, lastError *string) (RetryResult, error) {
	req := nackCommand{Consumer: consumer, Topic: topic, Partition: partition, Offset: offset, LastError: lastError}
	return proposeOrApply(ctx, b, raftCommandNack, req, b.applyNack)
}

func (b *Broker) ProcessRetryBatch(ctx context.Context, consumer, sourceTopic string, partition uint32, maxRecords int) (ProcessRetryResult, error) {
	req := processRetryCommand{Consumer: consumer, SourceTopic: sourceTopic, Partition: partition, MaxRecords: maxRecords, NowMS: store.NowMS()}
	return proposeOrApply(ctx, b, raftCommandProcessRetry, req, b.applyProcessRetryBatch)
}

//nolint:gocognit // Retry worker iteration keeps topic and partition progress accounting together.
func (b *Broker) ProcessRetryTopicsOnce(ctx context.Context, consumerPrefix string, maxRecordsPerPartition int) (int, error) {
	if consumerPrefix == "" {
		consumerPrefix = b.cfg.Broker.RetryWorkerConsumerPrefix
	}
	if maxRecordsPerPartition <= 0 {
		maxRecordsPerPartition = b.cfg.Broker.RetryWorkerMaxRecords
	}
	topics, err := b.meta.ListTopics()
	if err != nil {
		return 0, wrapBrokerStore(err, "list topics for retry worker")
	}
	movedTotal := 0
	for i := range topics {
		topic := topics[i]
		if isInternalTopicName(topic.Name) {
			continue
		}
		retryTopic := retryTopicName(topic.Name)
		exists, err := b.log.TopicExists(retryTopic)
		if err != nil {
			return movedTotal, wrapBrokerStore(err, "check retry topic")
		}
		if !exists {
			continue
		}
		for partition := range topic.Partitions {
			consumer := fmt.Sprintf("%s:%s:%d", consumerPrefix, topic.Name, partition)
			result, err := b.ProcessRetryBatch(ctx, consumer, topic.Name, partition, maxRecordsPerPartition)
			if err != nil {
				return movedTotal, err
			}
			movedTotal += result.MovedToOrigin + result.MovedToDeadLetter
		}
	}
	return movedTotal, nil
}

func (b *Broker) ScheduleDelay(ctx context.Context, topic string, partition uint32, payload []byte, deliverAtMS uint64) (DelayScheduleResult, error) {
	req := scheduleDelayCommand{Topic: topic, Partition: partition, Payload: append([]byte(nil), payload...), DeliverAtMS: deliverAtMS}
	return proposeOrApply(ctx, b, raftCommandScheduleDelay, req, b.applyScheduleDelay)
}

//nolint:gocognit // Delay scheduler iteration keeps topic and partition progress accounting together.
func (b *Broker) ProcessDueDelayedOnce(ctx context.Context, consumerPrefix string, maxRecordsPerPartition int) (int, error) {
	if consumerPrefix == "" {
		consumerPrefix = b.cfg.Broker.DelaySchedulerConsumerPrefix
	}
	if maxRecordsPerPartition <= 0 {
		maxRecordsPerPartition = b.cfg.Broker.DelaySchedulerMaxRecords
	}
	topics, err := b.meta.ListTopics()
	if err != nil {
		return 0, wrapBrokerStore(err, "list topics for delay scheduler")
	}
	movedTotal := 0
	nowMS := store.NowMS()
	for i := range topics {
		topic := topics[i]
		if !isDelayTopicName(topic.Name) {
			continue
		}
		defaultTarget := strings.TrimPrefix(topic.Name, internalDelayTopicPrefix+".")
		for partition := range topic.Partitions {
			consumer := fmt.Sprintf("%s:%s:%d", consumerPrefix, topic.Name, partition)
			req := processDelayCommand{
				Consumer:           consumer,
				DelayTopic:         topic.Name,
				DefaultTargetTopic: defaultTarget,
				Partition:          partition,
				MaxRecords:         maxRecordsPerPartition,
				NowMS:              nowMS,
			}
			moved, err := proposeOrApply(ctx, b, raftCommandProcessDelay, req, b.applyProcessDelayPartition)
			if err != nil {
				return movedTotal, err
			}
			movedTotal += moved
		}
	}
	return movedTotal, nil
}

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

	nextRetryCount := origin.RetryCount + 1
	deliverAtMS := store.NowMS() + retryBackoffMS(nextRetryCount, sourceTopic.RetryPolicy)
	retryRecord := cloneAsAppend(sourceRecord)
	upsertHeader(&retryRecord.Headers, retryHeaderOriginalTopic, origin.Topic)
	upsertHeader(&retryRecord.Headers, retryHeaderOriginalPartition, strconv.FormatUint(uint64(origin.Partition), 10))
	upsertHeader(&retryRecord.Headers, retryHeaderOriginalOffset, strconv.FormatUint(origin.Offset, 10))
	upsertHeader(&retryRecord.Headers, retryHeaderRetryCount, strconv.FormatUint(uint64(nextRetryCount), 10))
	upsertHeader(&retryRecord.Headers, retryHeaderDeliverAtMS, strconv.FormatUint(deliverAtMS, 10))
	upsertHeader(&retryRecord.Headers, retryHeaderLastError, stringValue(req.LastError, "unknown"))
	upsertHeader(&retryRecord.Headers, retryHeaderFailedConsumer, req.Consumer)

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

//nolint:cyclop,gocyclo,gocognit // Retry batch handling keeps delivery, DLQ routing, and offset advancement atomic.
func (b *Broker) applyProcessRetryBatch(ctx context.Context, req processRetryCommand) (ProcessRetryResult, error) {
	_ = ctx
	if req.Consumer == "" {
		return ProcessRetryResult{}, brokerStoreError(store.CodeInvalidArgument, "consumer is required")
	}
	if req.MaxRecords <= 0 {
		req.MaxRecords = b.cfg.Broker.RetryWorkerMaxRecords
	}
	retryTopic := retryTopicName(req.SourceTopic)
	fetched, err := b.queue.Fetch(req.Consumer, retryTopic, req.Partition, nil, req.MaxRecords)
	if err != nil {
		return ProcessRetryResult{}, wrapBroker("retry_fetch_failed", err, "fetch retry records")
	}
	result := ProcessRetryResult{RetryTopic: retryTopic, Partition: req.Partition}
	var lastProcessedOffset *uint64
	nowMS := req.NowMS
	if nowMS == 0 {
		nowMS = store.NowMS()
	}
	for _, retryRecord := range fetched.Records {
		deliverAt, err := headerUint64(retryRecord.Headers, retryHeaderDeliverAtMS)
		if err != nil {
			return ProcessRetryResult{}, err
		}
		if deliverAt != nil && *deliverAt > nowMS {
			break
		}
		origin, err := retryOriginFromRecord(retryRecord, req.SourceTopic, req.Partition)
		if err != nil {
			return ProcessRetryResult{}, err
		}
		originTopic, err := b.loadTopicConfig(origin.Topic)
		if err != nil {
			return ProcessRetryResult{}, err
		}
		deadLettered, err := b.moveRetryRecord(retryRecord, origin, *originTopic)
		if err != nil {
			return ProcessRetryResult{}, err
		}
		if deadLettered {
			result.MovedToDeadLetter++
		} else {
			result.MovedToOrigin++
		}
		next := retryRecord.Offset + 1
		lastProcessedOffset = &next
	}
	if lastProcessedOffset != nil {
		if err := b.queue.Ack(req.Consumer, retryTopic, req.Partition, *lastProcessedOffset); err != nil {
			return ProcessRetryResult{}, wrapBroker("retry_ack_failed", err, "ack retry records")
		}
		result.CommittedNextOffset = lastProcessedOffset
	}
	return result, nil
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

func (b *Broker) applyScheduleDelay(ctx context.Context, req scheduleDelayCommand) (DelayScheduleResult, error) {
	_ = ctx
	target, err := b.loadTopicConfig(req.Topic)
	if err != nil {
		return DelayScheduleResult{}, err
	}
	if req.Partition >= target.Partitions {
		return DelayScheduleResult{}, brokerStoreError(store.CodePartitionNotFound, "partition %s/%d not found", req.Topic, req.Partition)
	}
	delayTopic := delayTopicName(req.Topic)
	if ensureErr := b.ensureAuxTopic(*target, delayTopic); ensureErr != nil {
		return DelayScheduleResult{}, ensureErr
	}
	appendRecord := store.NewRecordAppend(req.Payload)
	appendRecord.Headers = []store.RecordHeader{
		header(delayHeaderDeliverAtMS, strconv.FormatUint(req.DeliverAtMS, 10)),
		header(delayHeaderTargetTopic, req.Topic),
		header(delayHeaderTargetPartition, strconv.FormatUint(uint64(req.Partition), 10)),
	}
	appended, err := b.queue.PublishRecord(delayTopic, req.Partition, appendRecord)
	if err != nil {
		return DelayScheduleResult{}, wrapBroker("delay_publish_failed", err, "publish delayed record")
	}
	return DelayScheduleResult{
		DelayTopic:  delayTopic,
		Partition:   req.Partition,
		Offset:      appended.Offset,
		NextOffset:  appended.Offset + 1,
		DeliverAtMS: req.DeliverAtMS,
	}, nil
}

//nolint:cyclop,gocyclo,gocognit // Delay partition handling keeps due checks, forwarding, and offset advancement atomic.
func (b *Broker) applyProcessDelayPartition(ctx context.Context, req processDelayCommand) (int, error) {
	_ = ctx
	if req.MaxRecords <= 0 {
		req.MaxRecords = b.cfg.Broker.DelaySchedulerMaxRecords
	}
	delayTP := store.NewTopicPartition(req.DelayTopic, req.Partition)
	startOffset := uint64(0)
	if committed, err := b.meta.LoadConsumerOffset(req.Consumer, delayTP); err != nil {
		return 0, wrapBrokerStore(err, "load delay consumer offset")
	} else if committed != nil {
		startOffset = *committed
	}
	records, err := b.log.ReadFrom(delayTP, startOffset, req.MaxRecords)
	if err != nil {
		return 0, wrapBrokerStore(err, "read delayed records")
	}
	nowMS := req.NowMS
	if nowMS == 0 {
		nowMS = store.NowMS()
	}
	moved := 0
	var commitNext *uint64
	for _, record := range records {
		deliverAt, err := headerUint64(record.Headers, delayHeaderDeliverAtMS)
		if err != nil {
			return moved, err
		}
		if deliverAt == nil {
			return moved, brokerStoreError(store.CodeCodec, "delay record missing %s header", delayHeaderDeliverAtMS)
		}
		if *deliverAt > nowMS {
			break
		}
		targetTopic := headerString(record.Headers, delayHeaderTargetTopic)
		if targetTopic == "" {
			targetTopic = req.DefaultTargetTopic
		}
		targetPartition := req.Partition
		if parsed, err := headerUint32(record.Headers, delayHeaderTargetPartition); err != nil {
			return moved, err
		} else if parsed != nil {
			targetPartition = *parsed
		}
		appendRecord := cloneAsAppend(record)
		appendRecord.Headers = removeHeaders(appendRecord.Headers, delayHeaderDeliverAtMS, delayHeaderTargetTopic, delayHeaderTargetPartition)
		if _, err := b.queue.PublishRecord(targetTopic, targetPartition, appendRecord); err != nil {
			return moved, wrapBroker("delay_forward_failed", err, "forward delayed record")
		}
		moved++
		next := record.Offset + 1
		commitNext = &next
	}
	if commitNext != nil {
		if err := b.meta.SaveConsumerOffset(req.Consumer, delayTP, *commitNext); err != nil {
			return moved, wrapBrokerStore(err, "save delay consumer offset")
		}
	}
	return moved, nil
}

func (b *Broker) readRecordAtOffset(tp store.TopicPartition, offset uint64) (store.Record, error) {
	records, err := b.log.ReadFrom(tp, offset, 1)
	if err != nil {
		return store.Record{}, wrapBrokerStore(err, "read record by offset")
	}
	if len(records) == 0 || records[0].Offset != offset {
		return store.Record{}, brokerStoreError(store.CodeInvalidArgument, "record %s/%d@%d not found", tp.Topic, tp.Partition, offset)
	}
	return records[0], nil
}

func (b *Broker) loadTopicConfig(topic string) (*store.TopicConfig, error) {
	cfg, err := b.meta.LoadTopicConfig(topic)
	if err != nil {
		return nil, wrapBrokerStore(err, "load topic config")
	}
	if cfg == nil {
		return nil, brokerStoreError(store.CodeTopicNotFound, "topic %s not found", topic)
	}
	return cfg, nil
}

func (b *Broker) ensureAuxTopic(source store.TopicConfig, auxTopic string) error {
	cfg := store.NewTopicConfig(auxTopic)
	cfg.Partitions = source.Partitions
	cfg.SegmentMaxBytes = source.SegmentMaxBytes
	cfg.IndexIntervalBytes = source.IndexIntervalBytes
	cfg.RetentionMaxBytes = source.RetentionMaxBytes
	cfg.RetentionMS = source.RetentionMS
	cfg.MaxMessageBytes = source.MaxMessageBytes
	cfg.MaxBatchBytes = source.MaxBatchBytes
	cfg.RetryPolicy = source.RetryPolicy
	cfg.DeadLetterTopic = source.DeadLetterTopic
	cfg.DelayEnabled = source.DelayEnabled
	cfg.CompactionEnabled = false
	exists, err := b.log.TopicExists(auxTopic)
	if err != nil {
		return wrapBrokerStore(err, "check auxiliary topic")
	}
	if exists {
		return wrapBrokerStore(b.meta.SaveTopicConfig(cfg), "save auxiliary topic config")
	}
	if err := b.queue.CreateTopic(cfg); err != nil && store.ErrorCode(err) != store.CodeTopicExists {
		return wrapBroker("auxiliary_topic_create_failed", err, "create auxiliary topic")
	}
	return nil
}

func retryTopicName(sourceTopic string) string {
	return internalRetryTopicPrefix + "." + sourceTopic
}

func delayTopicName(targetTopic string) string {
	return internalDelayTopicPrefix + "." + targetTopic
}

func dlqTopicName(sourceTopic string) string {
	return internalDLQTopicPrefix + "." + sourceTopic
}

func isInternalTopicName(topic string) bool {
	return direct.IsInternalTopicName(topic) || isRetryTopicName(topic) || isDelayTopicName(topic) || isDLQTopicName(topic)
}

func isRetryTopicName(topic string) bool {
	return hasInternalPrefix(topic, internalRetryTopicPrefix)
}

func isDelayTopicName(topic string) bool {
	return hasInternalPrefix(topic, internalDelayTopicPrefix)
}

func isDLQTopicName(topic string) bool {
	return hasInternalPrefix(topic, internalDLQTopicPrefix)
}

func hasInternalPrefix(topic, prefix string) bool {
	return topic == prefix || strings.HasPrefix(topic, prefix+".") || strings.HasPrefix(topic, prefix+"/")
}

func retryBackoffMS(retryCount uint32, policy store.TopicRetryPolicy) uint64 {
	if policy.BackoffInitialMS == 0 {
		policy = store.DefaultTopicRetryPolicy()
	}
	maxDelay := policy.BackoffMaxMS
	if maxDelay == 0 {
		maxDelay = policy.BackoffInitialMS
	}
	delay := policy.BackoffInitialMS
	for i := uint32(1); i < retryCount; i++ {
		if delay >= maxDelay/2 {
			return maxDelay
		}
		delay *= 2
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func retryOriginFromRecord(record store.Record, topic string, partition uint32) (retryOrigin, error) {
	origin := retryOrigin{
		Topic:     valueOr(headerString(record.Headers, retryHeaderOriginalTopic), topic),
		Partition: partition,
		Offset:    record.Offset,
	}
	if parsed, err := headerUint32(record.Headers, retryHeaderOriginalPartition); err != nil {
		return retryOrigin{}, err
	} else if parsed != nil {
		origin.Partition = *parsed
	}
	if parsed, err := headerUint64(record.Headers, retryHeaderOriginalOffset); err != nil {
		return retryOrigin{}, err
	} else if parsed != nil {
		origin.Offset = *parsed
	}
	if parsed, err := headerUint32(record.Headers, retryHeaderRetryCount); err != nil {
		return retryOrigin{}, err
	} else if parsed != nil {
		origin.RetryCount = *parsed
	}
	return origin, nil
}

func header(key, value string) store.RecordHeader {
	return store.RecordHeader{Key: key, Value: []byte(value)}
}

func headerString(headers []store.RecordHeader, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func headerUint64(headers []store.RecordHeader, key string) (*uint64, error) {
	raw := headerString(headers, key)
	if raw == "" {
		var absent *uint64
		return absent, nil
	}
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return nil, brokerStoreError(store.CodeCodec, "invalid header %s: %v", key, err)
	}
	return &parsed, nil
}

func headerUint32(headers []store.RecordHeader, key string) (*uint32, error) {
	raw := headerString(headers, key)
	if raw == "" {
		var absent *uint32
		return absent, nil
	}
	parsed, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return nil, brokerStoreError(store.CodeCodec, "invalid header %s: %v", key, err)
	}
	value := uint32(parsed)
	return &value, nil
}

func upsertHeader(headers *[]store.RecordHeader, key, value string) {
	for i := range *headers {
		if (*headers)[i].Key == key {
			(*headers)[i].Value = []byte(value)
			return
		}
	}
	*headers = append(*headers, header(key, value))
}

func removeHeaders(headers []store.RecordHeader, keys ...string) []store.RecordHeader {
	remove := collectionset.NewSet(keys...)
	out := headers[:0]
	for _, header := range headers {
		if !remove.Contains(header.Key) {
			out = append(out, header)
		}
	}
	return out
}

func cloneAsAppend(record store.Record) store.RecordAppend {
	return store.RecordAppend{
		TimestampMS: &record.TimestampMS,
		Key:         append([]byte(nil), record.Key...),
		Headers:     cloneRecordHeaders(record.Headers),
		Attributes:  record.Attributes,
		Payload:     append([]byte(nil), record.Payload...),
	}
}

func cloneRecordHeaders(headers []store.RecordHeader) []store.RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	out := make([]store.RecordHeader, 0, len(headers))
	for _, header := range headers {
		out = append(out, store.RecordHeader{Key: header.Key, Value: append([]byte(nil), header.Value...)})
	}
	return out
}

func buildDLQAppend(retryRecord store.Record, origin retryOrigin) store.RecordAppend {
	errorMessage := valueOr(headerString(retryRecord.Headers, retryHeaderLastError), "unknown")
	appendRecord := store.NewRecordAppend(retryRecord.Payload)
	appendRecord.TimestampMS = &retryRecord.TimestampMS
	appendRecord.Key = append([]byte(nil), retryRecord.Key...)
	appendRecord.Attributes = retryRecord.Attributes
	appendRecord.Headers = []store.RecordHeader{
		header(dlqHeaderOriginalTopic, origin.Topic),
		header(dlqHeaderOriginalPartition, strconv.FormatUint(uint64(origin.Partition), 10)),
		header(dlqHeaderOriginalOffset, strconv.FormatUint(origin.Offset, 10)),
		header(dlqHeaderRetryCount, strconv.FormatUint(uint64(origin.RetryCount), 10)),
		header(dlqHeaderErrorCode, dlqErrorCodeRetryExhausted),
		header(dlqHeaderErrorMessage, errorMessage),
	}
	return appendRecord
}

func stringValue(value *string, fallback string) string {
	if value == nil || *value == "" {
		return fallback
	}
	return *value
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

type retryOrigin struct {
	Topic      string
	Partition  uint32
	Offset     uint64
	RetryCount uint32
}

type nackCommand struct {
	Consumer  string
	Topic     string
	Partition uint32
	Offset    uint64
	LastError *string
}

type processRetryCommand struct {
	Consumer    string
	SourceTopic string
	Partition   uint32
	MaxRecords  int
	NowMS       uint64
}

type scheduleDelayCommand struct {
	Topic       string
	Partition   uint32
	Payload     []byte
	DeliverAtMS uint64
}

type processDelayCommand struct {
	Consumer           string
	DelayTopic         string
	DefaultTargetTopic string
	Partition          uint32
	MaxRecords         int
	NowMS              uint64
}
