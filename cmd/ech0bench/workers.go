package main

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	ech0 "github.com/DaiYuANg/ech0"
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
	mq *ech0.Broker,
	topic string,
	producerID uint32,
	payload []byte,
	counters *benchCounters,
	latencies *latencyRecorder,
) {
	sequence := uint64(0)
	for ctx.Err() == nil {
		key := []byte(strconv.FormatUint(uint64(producerID), 10) + "/" + strconv.FormatUint(sequence, 10))
		start := time.Now()
		msg, err := mq.Publish(ctx, topic, payload, ech0.Key(key))
		latencies.record(time.Since(start))
		if err != nil {
			counters.publishErrors.Add(1)
			continue
		}
		counters.produced.Add(1)
		counters.producedBytes.Add(uint64(len(msg.Payload)))
		sequence++
	}
}

func runConsumer(
	ctx context.Context,
	mq *ech0.Broker,
	cfg benchConfig,
	partition uint32,
	counters *benchCounters,
	latencies *latencyRecorder,
) {
	consumer := "ech0bench-" + strconv.FormatUint(uint64(partition), 10)
	for ctx.Err() == nil {
		start := time.Now()
		batch, err := mq.Fetch(ctx, consumer, cfg.topic, ech0.FetchPartition(partition), ech0.FetchLimit(cfg.fetchBatch))
		latencies.record(time.Since(start))
		if err != nil {
			counters.consumeErrors.Add(1)
			continue
		}
		if len(batch.Messages) == 0 {
			sleepIdle(ctx, cfg.pollIdle)
			continue
		}
		commitBatch(ctx, mq, cfg, consumer, partition, batch, counters)
	}
}

func commitBatch(
	ctx context.Context,
	mq *ech0.Broker,
	cfg benchConfig,
	consumer string,
	partition uint32,
	batch ech0.FetchResult,
	counters *benchCounters,
) {
	last := batch.Messages[len(batch.Messages)-1]
	if err := mq.Commit(ctx, consumer, cfg.topic, partition, last.NextOffset); err != nil {
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
