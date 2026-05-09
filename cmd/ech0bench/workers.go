package main

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"
)

type benchCounters struct {
	produced      atomic.Uint64
	consumed      atomic.Uint64
	producedBytes atomic.Uint64
	consumedBytes atomic.Uint64
	publishErrors atomic.Uint64
	consumeErrors atomic.Uint64
}

func runProducer(
	ctx context.Context,
	mq benchBroker,
	cfg benchConfig,
	producerID uint32,
	partition uint32,
	payload []byte,
	counters *benchCounters,
	latencies *latencyRecorder,
) {
	if cfg.producerInflight > 1 {
		runAsyncProducer(ctx, mq, cfg, producerID, partition, payload, counters, latencies)
		return
	}
	if cfg.batchSize > 1 {
		runBatchProducer(ctx, mq, cfg, producerID, partition, payload, counters, latencies)
		return
	}
	sequence := uint64(0)
	for ctx.Err() == nil {
		key := benchmarkRecordKey(producerID, sequence)
		start := time.Now()
		msg, err := mq.Publish(ctx, cfg.topic, payload, key)
		elapsed := time.Since(start)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			latencies.record(elapsed)
			counters.publishErrors.Add(1)
			continue
		}
		latencies.record(elapsed)
		counters.produced.Add(1)
		counters.producedBytes.Add(uint64(len(msg.Payload)))
		sequence++
	}
}

func runBatchProducer(
	ctx context.Context,
	mq benchBroker,
	cfg benchConfig,
	producerID uint32,
	partition uint32,
	payload []byte,
	counters *benchCounters,
	latencies *latencyRecorder,
) {
	sequence := uint64(0)
	for ctx.Err() == nil {
		records := benchmarkBatchRecords(producerID, sequence, payload, cfg.batchSize)
		start := time.Now()
		messages, err := mq.PublishBatch(ctx, cfg.topic, partition, records)
		elapsed := time.Since(start)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			latencies.record(elapsed)
			counters.publishErrors.Add(1)
			continue
		}
		latencies.record(elapsed)
		counters.produced.Add(uint64(len(messages)))
		for _, msg := range messages {
			counters.producedBytes.Add(uint64(len(msg.Payload)))
		}
		sequence += uint64(len(records))
	}
}

func runConsumer(
	ctx context.Context,
	mq benchBroker,
	cfg benchConfig,
	partition uint32,
	counters *benchCounters,
	latencies *latencyRecorder,
) {
	consumer := "ech0bench-" + strconv.FormatUint(uint64(partition), 10)
	for ctx.Err() == nil {
		start := time.Now()
		batch, err := mq.Fetch(ctx, consumer, cfg.topic, partition, cfg.fetchBatch)
		elapsed := time.Since(start)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			latencies.record(elapsed)
			counters.consumeErrors.Add(1)
			continue
		}
		latencies.record(elapsed)
		if len(batch.Messages) == 0 {
			sleepIdle(ctx, cfg.pollIdle)
			continue
		}
		commitBatch(ctx, mq, cfg, consumer, partition, batch, counters)
	}
}

func commitBatch(
	ctx context.Context,
	mq benchBroker,
	cfg benchConfig,
	consumer string,
	partition uint32,
	batch benchFetchResult,
	counters *benchCounters,
) {
	if err := mq.Commit(ctx, consumer, cfg.topic, partition, batch.NextOffset); err != nil {
		if ctx.Err() != nil {
			return
		}
		counters.consumeErrors.Add(1)
		return
	}
	for _, msg := range batch.Messages {
		counters.consumedBytes.Add(uint64(len(msg.Payload)))
	}
	counters.consumed.Add(uint64(len(batch.Messages)))
}

func sleepIdle(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func benchmarkPayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	return payload
}

func benchmarkBatchRecords(producerID uint32, baseSequence uint64, payload []byte, batchSize int) []benchPublishRecord {
	records := make([]benchPublishRecord, 0, batchSize)
	for idx := range batchSize {
		sequence := baseSequence + uint64(idx)
		records = append(records, benchPublishRecord{Payload: payload, Key: benchmarkRecordKey(producerID, sequence)})
	}
	return records
}

func benchmarkRecordKey(producerID uint32, sequence uint64) []byte {
	return []byte(strconv.FormatUint(uint64(producerID), 10) + "/" + strconv.FormatUint(sequence, 10))
}
