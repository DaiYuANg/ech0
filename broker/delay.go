package broker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/DaiYuANg/ech0/store"
)

func (b *Broker) ScheduleDelay(ctx context.Context, topic string, partition uint32, payload []byte, deliverAtMS uint64) (DelayScheduleResult, error) {
	req := scheduleDelayCommand{Topic: topic, Partition: partition, Payload: append([]byte(nil), payload...), DeliverAtMS: deliverAtMS}
	return routePartitionCommand(ctx, b, exactPartitionCommandTarget(delayTopicName(topic), partition), raftCommandScheduleDelay, req, b.applyScheduleDelay)
}

func (b *Broker) ProcessDueDelayedOnce(ctx context.Context, consumerPrefix string, maxRecordsPerPartition int) (int, error) {
	consumerPrefix, maxRecordsPerPartition = b.delaySchedulerConfig(consumerPrefix, maxRecordsPerPartition)
	topics, err := b.meta.ListTopics()
	if err != nil {
		return 0, wrapBrokerStore(err, "list topics for delay scheduler")
	}
	return b.processDelayTopics(ctx, topics, consumerPrefix, maxRecordsPerPartition, store.NowMS())
}

func (b *Broker) delaySchedulerConfig(consumerPrefix string, maxRecordsPerPartition int) (string, int) {
	if consumerPrefix == "" {
		consumerPrefix = b.cfg.Broker.DelaySchedulerConsumerPrefix
	}
	if maxRecordsPerPartition <= 0 {
		maxRecordsPerPartition = b.cfg.Broker.DelaySchedulerMaxRecords
	}
	return consumerPrefix, maxRecordsPerPartition
}

func (b *Broker) processDelayTopics(
	ctx context.Context,
	topics []store.TopicConfig,
	consumerPrefix string,
	maxRecordsPerPartition int,
	nowMS uint64,
) (int, error) {
	movedTotal := 0
	var movedMu sync.Mutex
	err := runBounded(ctx, b.cfg.Broker.MaintenanceConcurrency, len(topics), func(ctx context.Context, index int) error {
		moved, err := b.processDelayTopic(ctx, topics[index], consumerPrefix, maxRecordsPerPartition, nowMS)
		if err != nil {
			return err
		}
		movedMu.Lock()
		movedTotal += moved
		movedMu.Unlock()
		return nil
	})
	if err != nil {
		return movedTotal, err
	}
	return movedTotal, nil
}

func (b *Broker) processDelayTopic(
	ctx context.Context,
	topic store.TopicConfig,
	consumerPrefix string,
	maxRecordsPerPartition int,
	nowMS uint64,
) (int, error) {
	if !isDelayTopicName(topic.Name) {
		return 0, nil
	}
	defaultTarget := strings.TrimPrefix(topic.Name, internalDelayTopicPrefix+".")
	movedTotal := 0
	for partition := range topic.Partitions {
		moved, err := b.processDelayPartition(ctx, topic, defaultTarget, consumerPrefix, maxRecordsPerPartition, partition, nowMS)
		if err != nil {
			return movedTotal, err
		}
		movedTotal += moved
	}
	return movedTotal, nil
}

func (b *Broker) processDelayPartition(
	ctx context.Context,
	topic store.TopicConfig,
	defaultTarget string,
	consumerPrefix string,
	maxRecords int,
	partition uint32,
	nowMS uint64,
) (int, error) {
	consumer := fmt.Sprintf("%s:%s:%d", consumerPrefix, topic.Name, partition)
	req := processDelayCommand{
		Consumer:           consumer,
		DelayTopic:         topic.Name,
		DefaultTargetTopic: defaultTarget,
		Partition:          partition,
		MaxRecords:         maxRecords,
		NowMS:              nowMS,
	}
	return routePartitionCommand(ctx, b, exactPartitionCommandTarget(topic.Name, partition), raftCommandProcessDelay, req, b.applyProcessDelayPartition)
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

func (b *Broker) applyProcessDelayPartition(ctx context.Context, req processDelayCommand) (int, error) {
	_ = ctx
	req.MaxRecords = b.processDelayMaxRecords(req.MaxRecords)
	delayTP := store.NewTopicPartition(req.DelayTopic, req.Partition)
	records, err := b.fetchDelayedRecords(req, delayTP)
	if err != nil {
		return 0, err
	}
	moved, commitNext, err := b.forwardDueDelayedRecords(req, records)
	if err != nil {
		return moved, err
	}
	return moved, b.commitDelayedOffset(req.Consumer, delayTP, commitNext)
}

func (b *Broker) processDelayMaxRecords(maxRecords int) int {
	if maxRecords <= 0 {
		return b.cfg.Broker.DelaySchedulerMaxRecords
	}
	return maxRecords
}

func (b *Broker) fetchDelayedRecords(req processDelayCommand, delayTP store.TopicPartition) ([]store.Record, error) {
	startOffset, err := b.loadDelayConsumerOffset(req.Consumer, delayTP)
	if err != nil {
		return nil, err
	}
	records, err := b.queue.ReadFrom(delayTP, startOffset, req.MaxRecords)
	if err != nil {
		return nil, wrapBrokerStore(err, "read delayed records")
	}
	return records, nil
}

func (b *Broker) loadDelayConsumerOffset(consumer string, delayTP store.TopicPartition) (uint64, error) {
	committed, err := b.meta.LoadConsumerOffset(consumer, delayTP)
	if err != nil {
		return 0, wrapBrokerStore(err, "load delay consumer offset")
	}
	if committed == nil {
		return 0, nil
	}
	return *committed, nil
}

func (b *Broker) forwardDueDelayedRecords(req processDelayCommand, records []store.Record) (int, *uint64, error) {
	moved := 0
	var commitNext *uint64
	nowMS := processDelayNowMS(req.NowMS)
	for _, record := range records {
		ready, err := delayedRecordReady(record, nowMS)
		if err != nil {
			return moved, commitNext, err
		}
		if !ready {
			break
		}
		if err := b.forwardDelayedRecord(req, record); err != nil {
			return moved, commitNext, err
		}
		moved++
		next := record.Offset + 1
		commitNext = &next
	}
	return moved, commitNext, nil
}

func processDelayNowMS(nowMS uint64) uint64 {
	if nowMS == 0 {
		return store.NowMS()
	}
	return nowMS
}

func delayedRecordReady(record store.Record, nowMS uint64) (bool, error) {
	deliverAt, err := headerUint64(record.Headers, delayHeaderDeliverAtMS)
	if err != nil {
		return false, err
	}
	if deliverAt == nil {
		return false, brokerStoreError(store.CodeCodec, "delay record missing %s header", delayHeaderDeliverAtMS)
	}
	return *deliverAt <= nowMS, nil
}

func (b *Broker) forwardDelayedRecord(req processDelayCommand, record store.Record) error {
	targetTopic, targetPartition, err := delayTarget(req, record)
	if err != nil {
		return err
	}
	appendRecord := cloneAsAppend(record)
	appendRecord.Headers = removeHeaders(appendRecord.Headers, delayHeaderDeliverAtMS, delayHeaderTargetTopic, delayHeaderTargetPartition)
	_, err = b.queue.PublishRecord(targetTopic, targetPartition, appendRecord)
	return wrapBroker("delay_forward_failed", err, "forward delayed record")
}

func delayTarget(req processDelayCommand, record store.Record) (string, uint32, error) {
	targetTopic := headerString(record.Headers, delayHeaderTargetTopic)
	if targetTopic == "" {
		targetTopic = req.DefaultTargetTopic
	}
	targetPartition, err := delayTargetPartition(req, record)
	return targetTopic, targetPartition, err
}

func delayTargetPartition(req processDelayCommand, record store.Record) (uint32, error) {
	parsed, err := headerUint32(record.Headers, delayHeaderTargetPartition)
	if err != nil {
		return 0, err
	}
	if parsed == nil {
		return req.Partition, nil
	}
	return *parsed, nil
}

func (b *Broker) commitDelayedOffset(consumer string, delayTP store.TopicPartition, commitNext *uint64) error {
	if commitNext == nil {
		return nil
	}
	return wrapBrokerStore(b.meta.SaveConsumerOffset(consumer, delayTP, *commitNext), "save delay consumer offset")
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
