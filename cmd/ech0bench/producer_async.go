package main

import (
	"context"
	"sync"
	"time"
)

type asyncProducerWork struct {
	producerID uint32
	partition  uint32
	sequence   uint64
	payload    []byte
}

func runAsyncProducer(
	ctx context.Context,
	mq benchBroker,
	cfg benchConfig,
	producerID uint32,
	partition uint32,
	payload []byte,
	counters *benchCounters,
	latencies *latencyRecorder,
) {
	workCh := make(chan asyncProducerWork, int(cfg.producerInflight))
	workers := startAsyncProducerWorkers(ctx, mq, cfg, workCh, counters, latencies)
	enqueueAsyncProducerWork(ctx, cfg, workCh, producerID, partition, payload)
	close(workCh)
	workers.Wait()
}

func startAsyncProducerWorkers(
	ctx context.Context,
	mq benchBroker,
	cfg benchConfig,
	workCh <-chan asyncProducerWork,
	counters *benchCounters,
	latencies *latencyRecorder,
) *sync.WaitGroup {
	workers := &sync.WaitGroup{}
	for range cfg.producerInflight {
		workers.Go(func() {
			for work := range workCh {
				if ctx.Err() != nil {
					continue
				}
				publishAsyncProducerWork(ctx, mq, cfg, work, counters, latencies)
			}
		})
	}
	return workers
}

func enqueueAsyncProducerWork(
	ctx context.Context,
	cfg benchConfig,
	workCh chan<- asyncProducerWork,
	producerID uint32,
	partition uint32,
	payload []byte,
) {
	sequence := uint64(0)
	increment := producerBatchIncrement(cfg.batchSize)
	for ctx.Err() == nil {
		work := asyncProducerWork{
			producerID: producerID,
			partition:  partition,
			sequence:   sequence,
			payload:    payload,
		}
		select {
		case workCh <- work:
			sequence += increment
		case <-ctx.Done():
		}
	}
}

func producerBatchIncrement(batchSize int) uint64 {
	if batchSize < 1 {
		return 1
	}
	return uint64(batchSize)
}

func publishAsyncProducerWork(
	ctx context.Context,
	mq benchBroker,
	cfg benchConfig,
	work asyncProducerWork,
	counters *benchCounters,
	latencies *latencyRecorder,
) {
	if cfg.batchSize > 1 {
		publishAsyncBatchProducerWork(ctx, mq, cfg, work, counters, latencies)
		return
	}
	key := benchmarkRecordKey(work.producerID, work.sequence)
	start := time.Now()
	msg, err := mq.Publish(ctx, cfg.topic, work.payload, key)
	elapsed := time.Since(start)
	if err != nil {
		recordAsyncPublishError(ctx, counters, latencies, elapsed)
		return
	}
	latencies.record(elapsed)
	counters.produced.Add(1)
	counters.producedBytes.Add(uint64(len(msg.Payload)))
}

func publishAsyncBatchProducerWork(
	ctx context.Context,
	mq benchBroker,
	cfg benchConfig,
	work asyncProducerWork,
	counters *benchCounters,
	latencies *latencyRecorder,
) {
	records := benchmarkBatchRecords(work.producerID, work.sequence, work.payload, cfg.batchSize)
	start := time.Now()
	messages, err := mq.PublishBatch(ctx, cfg.topic, work.partition, records)
	elapsed := time.Since(start)
	if err != nil {
		recordAsyncPublishError(ctx, counters, latencies, elapsed)
		return
	}
	latencies.record(elapsed)
	counters.produced.Add(uint64(len(messages)))
	for _, msg := range messages {
		counters.producedBytes.Add(uint64(len(msg.Payload)))
	}
}

func recordAsyncPublishError(ctx context.Context, counters *benchCounters, latencies *latencyRecorder, elapsed time.Duration) {
	if ctx.Err() != nil {
		return
	}
	latencies.record(elapsed)
	counters.publishErrors.Add(1)
}
