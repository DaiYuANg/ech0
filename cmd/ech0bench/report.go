package main

import "time"

func printReport(
	cfg benchConfig,
	dataDir string,
	elapsed time.Duration,
	counters *benchCounters,
	publish latencySnapshot,
	fetch latencySnapshot,
) {
	produced := counters.produced.Load()
	consumed := counters.consumed.Load()
	producedBytes := counters.producedBytes.Load()
	consumedBytes := counters.consumedBytes.Load()
	seconds := elapsed.Seconds()

	writeStdout("ech0bench completed\n")
	writeStdout("data_dir=%s topic=%s duration=%s producers=%d consumers=%d partitions=%d payload_bytes=%d fetch_batch=%d\n",
		dataDir, cfg.topic, elapsed.Round(time.Millisecond), cfg.producers, cfg.activeConsumers, cfg.partitions, cfg.payloadBytes, cfg.fetchBatch)
	writeStdout("produced=%d consumed=%d publish_errors=%d consume_errors=%d\n",
		produced, consumed, counters.publishErrors.Load(), counters.consumeErrors.Load())
	writeStdout("produce_rate=%.2f msg/s produce_throughput=%.2f MiB/s\n",
		float64(produced)/seconds, mibPerSecond(producedBytes, seconds))
	writeStdout("consume_rate=%.2f msg/s consume_throughput=%.2f MiB/s\n",
		float64(consumed)/seconds, mibPerSecond(consumedBytes, seconds))
	printLatency("publish_latency", publish)
	printLatency("fetch_latency", fetch)
}

func mibPerSecond(bytes uint64, seconds float64) float64 {
	if seconds <= 0 {
		return 0
	}
	return float64(bytes) / 1024 / 1024 / seconds
}

func printLatency(name string, snapshot latencySnapshot) {
	writeStdout("%s count=%d avg=%s p50=%s p95=%s p99=%s max=%s sampled=%d\n",
		name,
		snapshot.count,
		formatDurationASCII(snapshot.avg()),
		formatDurationASCII(snapshot.percentile(50)),
		formatDurationASCII(snapshot.percentile(95)),
		formatDurationASCII(snapshot.percentile(99)),
		formatDurationASCII(snapshot.max),
		len(snapshot.samples),
	)
}
