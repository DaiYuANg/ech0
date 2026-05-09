# Benchmarks and Stress Testing

ech0 includes two benchmark layers:

- Go benchmarks for repeatable micro and subsystem measurements.
- `cmd/ech0bench` for an end-to-end sustained stress run against either embedded broker mode or a running TCP broker.

Use the Go benchmarks to locate likely bottlenecks. Use `ech0bench` to validate whether those bottlenecks matter under concurrent publish/fetch load.

## Go Benchmarks

Run all benchmark packages:

```sh
go test ./protocol ./transport ./store ./broker -run '^$' -bench . -benchmem
```

Run a focused package:

```sh
go test ./protocol -run '^$' -bench . -benchmem
go test ./store -run '^$' -bench Storx -benchmem
go test ./broker -run '^$' -bench Broker -benchmem
```

Recommended longer run:

```sh
go test ./protocol ./transport ./store ./broker -run '^$' -bench . -benchmem -benchtime=5s -count=5
```

If `benchstat` is installed, compare two runs:

```sh
go test ./broker -run '^$' -bench . -benchmem -count=10 > before.txt
go test ./broker -run '^$' -bench . -benchmem -count=10 > after.txt
benchstat before.txt after.txt
```

## Covered Paths

| Package | Benchmarks |
| --- | --- |
| `protocol` | Binary command body encode/decode for produce, fetch response, and request start. |
| `transport` | Frame read/write overhead for 1 KiB and 64 KiB bodies. |
| `store` | Memory store baseline, segment-log append, batch append, and indexed reads. |
| `broker` | Publish, publish/fetch/commit, TCP frame handling, request/reply, and parallel publish. |

## Stress Tool

Run the embedded broker stress tool:

```sh
go run ./cmd/ech0bench --duration 30s --producers 8 --consumers 4 --partitions 4 --payload-bytes 1024 --fetch-batch 128
```

Run against a TCP broker, such as a raft leader in the Docker cluster:

```sh
go run ./cmd/ech0bench --broker-addr 127.0.0.1:29090 --topic ech0-bench-cluster --duration 30s --producers 4 --consumers 4 --partitions 4 --payload-bytes 1024 --fetch-batch 128
```

Run a batched produce workload:

```sh
go run ./cmd/ech0bench --broker-addr 127.0.0.1:29090 --topic ech0-bench-cluster-batch --duration 30s --producers 4 --consumers 4 --partitions 4 --payload-bytes 1024 --batch-size 16 --fetch-batch 256
```

For a clean Docker cluster run without deleting the default compose data directory:

```sh
cd deploy/docker
ECH0_CLUSTER_DATA_ROOT=./data/bench-cluster docker compose -f docker-compose.cluster.yml up -d --build
```

Discover the current leader from `/healthz`, then point `--broker-addr` at that node's broker port.

Useful flags:

| Flag | Default | Meaning |
| --- | ---: | --- |
| `--data-dir` | temp dir | Store data directory. Leave empty for disposable runs. |
| `--broker-addr` | empty | TCP broker address. Leave empty to run embedded mode. |
| `--topic` | `ech0-bench` | Topic used by the run. |
| `--partitions` | `4` | Topic partition count. |
| `--producers` | `4` | Producer goroutines. |
| `--consumers` | `4` | Consumer goroutines, capped to partition count. |
| `--duration` | `30s` | Run duration. |
| `--payload-bytes` | `1024` | Message payload size. |
| `--batch-size` | `1` | Records per produce request. Values greater than 1 use the broker batch produce path. |
| `--producer-inflight` | `1` | Max in-flight produce requests per producer. Values greater than 1 make the producer Kafka-like and async, which is useful for separating broker throughput from synchronous client wait time. |
| `--fetch-batch` | `128` | Max records per fetch. |
| `--commit-every` | `1` | Commit consumer offsets every N non-empty fetch batches. Raising this separates message fetch throughput from synchronous offset commit raft traffic. |
| `--poll-idle` | `1ms` | Consumer sleep after an empty fetch. |
| `--samples` | `200000` | Deprecated compatibility flag. Percentiles are now calculated from an HDR histogram over all recorded latencies. |

The report includes:

- Produced and consumed message counts.
- Publish and consume throughput.
- Publish and fetch latency average, p50, p95, p99, p99.9, p99.99, and max.
- Publish and consume error counts.

Publish latency is measured per produce operation. When `--batch-size` is greater than 1, throughput counts messages but publish latency counts batch requests.
When `--producer-inflight` is greater than 1, each producer keeps multiple produce operations in flight. Latency still measures individual publish or publish-batch round trips, but throughput is no longer limited by a producer waiting synchronously for each request before issuing the next one.

## Architecture Change Note

The Dragonboat cluster run below is the current clustered runtime baseline. Older cluster numbers are kept as the baseline that motivated the raft architecture change; they were captured before raft log storage, leader distribution, and per-shard proposal paths moved from the legacy single-group implementation to Dragonboat groups. Rows labeled legacy local single node predate the Dragonboat-only runtime and should be rerun as single-replica Dragonboat before release comparisons.

## Docker Desktop Kafka Comparison: 2026-05-09

Environment:

- Host: Windows amd64, Docker Desktop 4.72.0, Docker Engine 29.4.2.
- Workload: producer-only writes, 1 KiB payloads, 4 partitions, 4 producers, batch size 32, producer in-flight 4.
- ech0: current workspace Docker image, TCP broker target, `ech0bench --consumers 0`.
- Kafka: `apache/kafka:4.2.0`, official `kafka-producer-perf-test.sh`, `acks=all`, `linger.ms=5`, `batch.size=32768`, `compression.type=none`.
- Single-node Kafka used replication factor 1. Cluster Kafka used 3 brokers, replication factor 3, `min.insync.replicas=2`.
- Each row is the average of 3 runs.

| Mode | Runs | Avg Produce Rate | Avg Throughput | Avg Publish p50 | Avg Publish p95 | Avg Publish p99 | Errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ech0 legacy local single node | 3 | 16,298.09 msg/s | 15.92 MiB/s | 29.529ms | 37.225ms | 48.966ms | 0 |
| Kafka single node | 3 | 92,854.90 msg/s | 90.68 MiB/s | 303.333ms | 409.000ms | 425.000ms | 0 |
| ech0 Dragonboat Raft, 3 nodes, 4 shards | 3 | 12,677.00 msg/s | 12.38 MiB/s | 40.763ms | 51.806ms | 84.946ms | 8 |
| Kafka KRaft, 3 nodes, RF=3, min ISR=2 | 3 | 28,037.51 msg/s | 27.38 MiB/s | 893.667ms | 1.701s | 1.857s | 0 |

Relative write throughput:

| Comparison | Ratio | ech0 Share |
| --- | ---: | ---: |
| Kafka single node / ech0 legacy local single node | 5.70x | 17.55% |
| Kafka 3-node RF=3 / ech0 3-node Raft | 2.21x | 45.21% |

Run details:

| Mode | Run 1 | Run 2 | Run 3 |
| --- | ---: | ---: | ---: |
| ech0 legacy local single node | 17,270.71 msg/s | 15,916.31 msg/s | 15,707.26 msg/s |
| Kafka single node | 81,059.17 msg/s | 92,279.30 msg/s | 105,226.24 msg/s |
| ech0 Dragonboat Raft | 13,051.01 msg/s | 13,059.14 msg/s | 11,920.86 msg/s |
| Kafka KRaft RF=3 | 28,549.68 msg/s | 23,607.18 msg/s | 31,955.69 msg/s |

Readout:

- Single-node ech0 is far behind Kafka on producer-only throughput. The main gap is not consensus; it is the local TCP + broker + segment append path.
- In replicated mode, the gap narrows materially: ech0 Raft reaches about 45% of Kafka RF=3 write throughput in this Docker Desktop run.
- ech0 latency is much lower than Kafka in this producer-only setup because `ech0bench` keeps only 16 batch requests in flight total, while Kafka's producer perf tool saturates a deeper async pipeline and reports higher queueing latency.
- The third ech0 Raft run reported 8 publish errors. Container logs showed no broker panic/fatal/error beyond Dragonboat's benign filesystem error-injection status line, so treat that run as mild transient client/proposal instability rather than a storage crash.
- The next comparison should add a balanced produce+consume run and a Kafka consumer perf run. Producer-only isolates write throughput but does not measure consumer lag or end-to-end queue behavior.

## Dragonboat Cluster Run: 2026-05-09

Environment:

- Host: Windows amd64, AMD Ryzen 7 9800X3D 8-Core Processor.
- Cluster: 3-node Docker Dragonboat cluster, Linux containers, clean data roots.
- Packaging path: image built from the current workspace with the default UPX-compressed binary.
- Workload: 1 KiB payloads, 4 producers, 4 consumers, 4 partitions, 15s target duration.
- Single-shard run: 1 data shard group, benchmark target `127.0.0.1:39190`.
- Four-shard run: 4 data shard groups, benchmark target `127.0.0.1:19190`.

End-to-end stress results:

| Mode | Settings | Produced | Consumed | Produce Rate | Consume Rate | Publish p50 | Publish p95 | Publish p99 | Fetch p99 | Errors |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TCP Dragonboat cluster, 1 shard | `batch_size=16`, `commit_every=1` | 47,936 | 47,840 | 3,188.90 msg/s, 3.11 MiB/s | 3,182.51 msg/s | 19.605ms | 22.698ms | 31.024ms | 10.856ms | 0 |
| TCP Dragonboat cluster, 1 shard async | `batch_size=32`, `producer_inflight=4`, `commit_every=8` | 123,392 | 160,576 | 8,225.22 msg/s, 8.03 MiB/s | 10,703.88 msg/s | 59.510ms | 86.517ms | 96.710ms | 268.469ms | 0 |
| TCP Dragonboat cluster, 4 shards | `batch_size=16`, `commit_every=1` | 60,240 | 60,080 | 4,010.92 msg/s, 3.92 MiB/s | 4,000.27 msg/s | 15.365ms | 19.307ms | 23.804ms | 23.790ms | 0 |
| TCP Dragonboat cluster, 4 shards async | `batch_size=32`, `producer_inflight=4`, `commit_every=8` | 240,992 | 127,488 | 16,064.93 msg/s, 15.69 MiB/s | 8,498.56 msg/s | 32.738ms | 40.002ms | 46.387ms | 304.597ms | 0 |

The 1-shard async consume count is higher than the produced count because consumers drained backlog at the beginning of the measurement window. The 4-shard async run is the best current write-throughput signal, but consumers did not keep up, so it should not be treated as a balanced steady-state result.

Relative to the last pre-Dragonboat clustered baseline, `batch_size=16` with the binary raft hot command codec at 1,404.43 msg/s:

| Current Run | Produce Rate | Multiple |
| --- | ---: | ---: |
| Dragonboat 1 shard sync | 3,188.90 msg/s | 2.27x |
| Dragonboat 4 shards sync | 4,010.92 msg/s | 2.86x |
| Dragonboat 4 shards async | 16,064.93 msg/s | 11.44x |

Selected metrics from the same runs:

| Run | Layer | Stage | Avg |
| --- | --- | --- | ---: |
| 1 shard sync | raft `produce_batches` | `total` | 22.17ms |
| 1 shard sync | FSM `produce_batches` | `total` | 17.56ms |
| 1 shard sync | store `append_batch` | `total` | 4.55ms |
| 1 shard sync | store `read_from` | `total` | 35.06ms |
| 4 shards sync/async mixed window | raft `produce_batches` | `total` | 13.68ms |
| 4 shards sync/async mixed window | FSM `produce_batches` | `total` | 9.64ms |
| 4 shards sync/async mixed window | store `append_batch` | `total` | 9.21ms |
| 4 shards sync/async mixed window | store `read_from` | `total` | 37.20ms |

Operational check:

- Dragonboat metadata and data groups were all ready before benchmark collection.
- The 4-shard run had data group leaders distributed across nodes.
- Docker log scans found no `rollback`, `fail`, `error`, `panic`, or `fatal` matches; the only `error`-like line was Dragonboat's benign filesystem error-injection status log.

Readout:

- Dragonboat removes the old single-group raft bottleneck and makes data-shard parallelism visible: 4 sync shards improved write throughput by about 26% over 1 sync shard, and async publication reached about 16k msg/s.
- Write throughput is no longer the only limiter. The 4-shard async run produced much faster than it consumed, and the metrics still show `read_from/read_segments` around 37ms on average.
- Compared with the earlier same-host Kafka producer-only result of 19,969.82 msg/s, the 4-shard async Dragonboat write path is about 80% of Kafka's producer wall-clock rate in this Docker setup. That is encouraging but not parity: the Kafka run was producer-only, while ech0's consumers lagged in the simultaneous run.
- The next high-impact targets are leader-aware routing, fetch/read parallelism, larger consumer batch handling, and reducing request/response copy pressure on the TCP path.

## Local Run: 2026-05-08

Environment:

- Host: Windows amd64, AMD Ryzen 7 9800X3D 8-Core Processor.
- Go: `go 1.26.2`.
- Docker Compose: v5.1.3.
- Workload: 1 KiB payloads, 4 producers, 4 consumers, 4 partitions, fetch batch 128, 30s target duration.
- Cluster run: 3-node Docker raft cluster, clean `ECH0_CLUSTER_DATA_ROOT`, leader node2, benchmark target `127.0.0.1:29090`.

End-to-end stress results:

| Mode | Produced | Consumed | Produce Rate | Consume Rate | Publish p50 | Publish p95 | Publish p99 | Fetch p99 | Errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Embedded library | 31,286 | 31,282 | 964.01 msg/s, 0.94 MiB/s | 963.89 msg/s, 0.94 MiB/s | 3.671ms | 4.819ms | 6.302ms | 618.500us | 0 |
| TCP raft cluster | 2,176 | 2,172 | 72.53 msg/s, 0.07 MiB/s | 72.40 msg/s, 0.07 MiB/s | 49.198ms | 61.937ms | 76.254ms | 9.436ms | 0 |

Selected Go benchmark results:

| Path | Result | Allocation Signal |
| --- | ---: | ---: |
| `protocol` encode produce request | 326.7 ns/op | 1,520 B/op, 5 allocs/op |
| `protocol` decode produce request | 475.8 ns/op | 1,576 B/op, 27 allocs/op |
| `protocol` decode fetch response, 100x1KiB | 43.532 us/op | 157,305 B/op, 2,113 allocs/op |
| `transport` write 1KiB frame | 315.3 ns/op | 464 B/op, 10 allocs/op |
| `transport` read 1KiB frame | 170.5 ns/op | 1,104 B/op, 3 allocs/op |
| `store` memory append 1KiB | 239.631 us/op | 3,560 B/op, 3 allocs/op |
| `store` segment-log append 1KiB | 1.286 ms/op | 72,511 B/op, 865 allocs/op |
| `store` segment-log append batch 100x1KiB | 1.717 ms/op | 730,650 B/op, 6,131 allocs/op |
| `store` segment-log read 100x1KiB | 0.717 ms/op | 652,083 B/op, 6,299 allocs/op |
| `broker` publish, memory store | 189.749 us/op | 5,988 B/op, 15 allocs/op |
| `broker` TCP produce frame, memory store | 101.399 us/op | 8,283 B/op, 62 allocs/op |
| `mapper` fetch records manual, 100x1KiB | 24.358 us/op | 141,728 B/op, 701 allocs/op |
| `mapper` fetch records mapper, 100x1KiB | 297.741 us/op | 234,757 B/op, 4,716 allocs/op |

Readout:

- Protocol and frame IO are not the current bottleneck for 1 KiB messages.
- The persistent path is dominated by segment append/index work. A single segment-log append is roughly 6.8x slower than the broker memory-store publish benchmark.
- The raft cluster path is roughly 13.3x lower throughput than embedded mode in this run. The difference is expected because the benchmark currently sends one TCP produce per message and raft serializes writes through a single leader/quorum path.
- Mapper remains useful for control-plane and maintenance conversions, but manual conversion should stay on hot fetch/produce loops.
- The next highest-impact benchmark feature is producer batching. Kafka and NATS both rely heavily on async or batch publication for throughput, while this `ech0bench` run intentionally used one produce request per message.

Historical follow-up runs after adding `--batch-size` and lowering the legacy raft commit flush interval:

| Mode | Settings | Produce Rate | Consume Rate | Publish p50 | Publish p95 | Errors |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Embedded library | `batch_size=16`, 15s | 984.46 msg/s | 983.07 msg/s | 64.248ms per batch | 79.024ms per batch | 0 |
| Embedded library | `batch_size=16`, `producer_inflight=8`, 5s, library `Producer` | 57,982.49 msg/s | 57,963.51 msg/s | 8.587ms per batch | 11.317ms per batch | 0 |
| TCP raft cluster | `legacy_flush=5ms`, `batch_size=1`, 15s | 73.33 msg/s | 72.93 msg/s | 50.017ms | 71.646ms | 0 |
| TCP raft cluster | `legacy_flush=5ms`, `batch_size=16`, 15s | 109.86 msg/s | 102.40 msg/s | 568.516ms per batch | 608.446ms per batch | 0 |
| TCP raft cluster | `legacy_flush=5ms`, `batch_size=16`, real store batch append, 15s | 429.83 msg/s | 248.51 msg/s | 63.611ms per batch | 93.292ms per batch | 0 |
| TCP raft cluster | `legacy_flush=5ms`, `batch_size=16`, raft proposal coalescing, 15s | 456.51 msg/s | 433.04 msg/s | 59.726ms per batch | 116.874ms per batch | 0 |
| TCP raft cluster | `legacy_flush=5ms`, `batch_size=16`, coalescing + grouped segment reads, 15s | 1,041.05 msg/s | 1,030.38 msg/s | 41.487ms per batch | 56.762ms per batch | 0 |
| TCP raft cluster | `legacy_flush=5ms`, `batch_size=16`, partition locks + grouped produce apply, 15s | 1,557.19 msg/s | 1,546.53 msg/s | 40.275ms per batch | 45.957ms per batch | 0 |
| TCP raft cluster | `legacy_flush=5ms`, `batch_size=16`, segment write coalescing + reader cache, 15s | 1,522.10 msg/s | 1,511.44 msg/s | 39.298ms per batch | 63.744ms per batch | 0 |
| TCP raft cluster | `legacy_flush=5ms`, `batch_size=16`, binary raft hot command codec, 15s | 1,404.43 msg/s | 1,400.16 msg/s | 36.285ms per batch | 100.158ms per batch | 0 |

These follow-up numbers showed that lowering the legacy commit flush interval was necessary but not enough by itself. The library `Producer` async path moves embedded mode above 57k msg/s in this local run, which confirms that synchronous client wait time was dominating embedded `batch_size=16` throughput. The real store batch append path improved cluster produce throughput by roughly 3.9x over the previous `batch_size=16` run and moved batch publish p50 from about 569ms to about 64ms. Raft proposal coalescing helped consume keep up by batching commit offsets. Grouped segment reads then removed the major fetch bottleneck and moved this local Docker run above 1,000 msg/s for both produce and consume.

## Cluster Metrics Drilldown

The broker exports hot-path histograms for raft, FSM, and append stages:

- `ech0_broker_command_duration_seconds`
- `ech0_broker_raft_stage_duration_seconds`
- `ech0_broker_fsm_stage_duration_seconds`
- `ech0_broker_fetch_stage_duration_seconds`
- `ech0_store_append_stage_duration_seconds`
- `ech0_store_read_stage_duration_seconds`
- `ech0_store_append_requests_total`
- `ech0_store_append_records_total`
- `ech0_store_read_requests_total`
- `ech0_store_read_records_total`

Older drilldowns below include legacy raft store metrics. Dragonboat owns its internal raft log storage, so current cluster runs should use proposal, FSM, and broker/store-stage metrics instead.

Pre-optimization sample run, 2026-05-08:

- 3-node Docker raft cluster.
- Legacy raft commit flush interval: 5ms.
- `--batch-size 16`, 4 producers, 4 consumers, 4 partitions, 1 KiB payload, 15s.
- Leader target: `127.0.0.1:19090`.
- Result: 1,584 produced, 1,472 consumed, 105.59 msg/s produce rate, 0 errors.

Histogram averages below are computed from Prometheus `_sum / _count`.

| Layer | Stage | Count | Avg |
| --- | --- | ---: | ---: |
| raft `produce_batch` | `apply_wait` | 103 | 596.470ms |
| raft `produce_batch` | `encode` | 103 | 0.060ms |
| raft `produce_batch` | `total` | 103 | 596.550ms |
| FSM `produce_batch` | `apply` | 103 | 149.630ms |
| FSM `produce_batch` | `decode_payload` | 103 | 0.030ms |
| FSM `produce_batch` | `total` | 103 | 149.700ms |
| store `append_batch` | `total` | 103 | 149.600ms |
| store `append_record` | `total` | 1,648 | 9.350ms |
| store `append_record` | `append_frame` | 1,648 | 8.950ms |
| store `append_record` | `lock_wait` | 1,648 | 0.250ms |
| store `append_record` | `index_set` | 1,648 | 0.050ms |
| store `append_record` | `encode_frame` | 1,648 | 0.010ms |
| raft `commit_offset` | `apply_wait` | 99 | 549.180ms |
| FSM `commit_offset` | `apply` | 99 | 2.040ms |

This pre-optimization data narrowed the cluster bottleneck:

- `raft apply_wait` includes waiting behind previous raft work. It is much larger than FSM execution time, which means requests are queueing behind slow applied commands.
- The slow applied command is `produce_batch`: FSM `apply` is effectively the same as store `append_batch total`.
- Store batch still looped over `AppendRecord` in this run, so each 16-record batch performed 16 segment appends and 16 index/offset updates.
- Within `append_record`, `append_frame` dominates at roughly 8.95ms out of 9.35ms. JSON raft command encode/decode is not material in this run.
- Consumer offset commits are cheap in FSM (`~2ms`) but expensive in raft wait (`~549ms`) because they queue behind produce batches.

After real store batch append, 2026-05-08:

- 3-node Docker raft cluster.
- Legacy raft commit flush interval: 5ms.
- `--batch-size 16`, 4 producers, 4 consumers, 4 partitions, 1 KiB payload, 15s.
- Leader target: `127.0.0.1:39090`.
- Result: 6,448 produced, 3,728 consumed, 429.83 msg/s produce rate, 0 errors.
- Publish latency: avg 66.729ms, p50 63.611ms, p95 93.292ms, p99 105.178ms.

| Layer | Stage | Count | Avg |
| --- | --- | ---: | ---: |
| raft `produce_batch` | `apply_wait` | 403 | 65.763ms |
| raft `produce_batch` | `encode` | 407 | 0.052ms |
| raft `produce_batch` | `total` | 403 | 65.833ms |
| FSM `produce_batch` | `apply` | 403 | 16.398ms |
| FSM `produce_batch` | `decode_payload` | 404 | 0.030ms |
| FSM `produce_batch` | `total` | 403 | 16.460ms |
| store `append_batch` | `total` | 403 | 16.382ms |
| store `append_batch` | `append_frame` | 403 | 11.344ms |
| store `append_batch` | `index_set` | 403 | 0.135ms |
| store `append_batch` | `encode_frame` | 403 | 0.090ms |
| store `append_batch` | `next_offset_set` | 403 | 0.024ms |
| raft `commit_offset` | `apply_wait` | 30 | 55.123ms |
| FSM `commit_offset` | `apply` | 30 | 2.734ms |

The batch append refactor removed the per-record append loop from the batch path:

- Store `append_batch total` improved from about 149.6ms to about 16.4ms per 16-record batch, or roughly 9.1x.
- `append_record` hot-path metrics disappear from the optimized batch run because `AppendRecordsBatch` no longer calls `AppendRecord` internally.
- `append_frame` is still the largest store substage at about 11.3ms per 16-record batch, which points to durable segment file write/sync cost as the next storage target.
- Raft `apply_wait` is now about 65.8ms versus FSM `total` at about 16.5ms, so the remaining cluster produce latency in that historical run was mostly raft queue/quorum/log wait rather than command JSON codec or the old Badger index writes.
- Consumer throughput trails producer throughput in this sample. The next benchmark pass should separate read/fetch batching, consumer offset commit cadence, and backlog drain behavior.

After raft coalescing, persistent segment writers, and grouped segment reads, 2026-05-08:

- 3-node Docker raft cluster.
- Legacy raft commit flush interval: 5ms.
- `--batch-size 16`, 4 producers, 4 consumers, 4 partitions, 1 KiB payload, 15s.
- Leader target: `127.0.0.1:39090`.
- Result: 15,616 produced, 15,456 consumed, 1,041.05 msg/s produce rate, 1,030.38 msg/s consume rate, 0 errors.
- Publish latency: avg 43.584ms, p50 41.487ms, p95 56.762ms, p99 77.477ms.
- Fetch latency: avg 10.465ms, p50 11.536ms, p95 14.957ms, p99 24.736ms.

| Layer | Stage | Count | Avg |
| --- | --- | ---: | ---: |
| raft `produce_batches` | `apply_wait` | 246 | 60.229ms |
| raft `produce_batches` | `encode` | 246 | 0.168ms |
| raft `produce_batches` | `total` | 246 | 60.418ms |
| raft store `logs` | `put_many` | 496 | 7.514ms |
| FSM `produce_batches` | `apply` | 246 | 44.335ms |
| store `append_batch` | `total` | 980 | 11.114ms |
| store `append_batch` | `append_frame` | 980 | 3.851ms |
| broker `fetch` | `queue_fetch` | 581 | 9.567ms |
| store `read_from` | `total` | 581 | 9.190ms |
| store `read_from` | `read_segments` | 581 | 9.011ms |
| raft `commit_offsets` | `apply_wait` | 247 | 61.306ms |
| FSM `commit_offsets` | `apply` | 247 | 4.694ms |

This run changes the bottleneck picture again:

- The grouped read path moved `ReadFrom100x1KB` from about 8.9ms/op to about 0.7ms/op in the store benchmark, and cluster fetch p99 dropped to about 24.7ms.
- Persistent segment writers reduced store `append_frame` to about 3.9ms per 16-record append batch in the cluster run.
- Raft proposal coalescing reduced the number of raft produce proposals: 980 logical append batches were applied through 246 `produce_batches` raft commands.
- The remaining write-side cost is split between Raft log persistence (`raft store logs put_many`, about 7.5ms) and FSM/store work. Further work should target reducing per-command raft log fsync cost and distributing partition leadership across nodes.

After partition-scoped locks and grouped produce apply, 2026-05-08:

- 3-node Docker raft cluster.
- Legacy raft commit flush interval: 5ms.
- `--batch-size 16`, 4 producers, 4 consumers, 4 partitions, 1 KiB payload, 15s.
- Leader target: `127.0.0.1:19090`.
- Result: 23,360 produced, 23,200 consumed, 1,557.19 msg/s produce rate, 1,546.53 msg/s consume rate, 0 errors.
- Publish latency: avg 40.995ms, p50 40.275ms, p95 45.957ms, p99 51.109ms.
- Fetch latency: avg 9.942ms, p50 11.039ms, p95 12.820ms, p99 15.296ms.

| Layer | Stage | Count | Avg |
| --- | --- | ---: | ---: |
| raft `produce_batches` | `apply_wait` | 366 | 38.547ms |
| raft `produce_batches` | `encode` | 366 | 0.152ms |
| raft `produce_batches` | `total` | 366 | 38.718ms |
| raft store `logs` | `put_many` | 736 | 7.175ms |
| FSM `produce_batches` | `apply` | 366 | 23.609ms |
| store `append_batch` | `total` | 1,464 | 5.890ms |
| store `append_batch` | `append_frame` | 1,464 | 3.632ms |
| store `append_batch` | `lock_wait` | 1,464 | 0.001ms |
| broker `fetch` | `queue_fetch` | 841 | 9.091ms |
| store `read_from` | `total` | 841 | 8.731ms |
| store `read_from` | `read_segments` | 841 | 8.562ms |
| raft `commit_offsets` | `apply_wait` | 367 | 39.701ms |
| FSM `commit_offsets` | `apply` | 367 | 4.331ms |

This run keeps writes deterministic inside the Raft FSM while reducing local store contention:

- `append_batch lock_wait` is effectively gone after replacing the global store append lock with partition-scoped locks.
- Produce requests inside one raft command are grouped by target partition, so a partition receives one larger `AppendRecordsBatch` instead of several smaller batch appends.
- A tested variant that applied partition groups concurrently regressed to about 647 msg/s and raised `append_frame` to about 10.3ms because multiple segment fsyncs competed on the same Docker Desktop storage path.
- The current remaining cost is mostly durable IO: raft log `put_many` is about 7.2ms, and segment `append_frame` is about 3.6ms per grouped append batch.

After segment write coalescing and reader cache, 2026-05-08:

- 3-node Docker raft cluster.
- Legacy raft commit flush interval: 5ms.
- `--batch-size 16`, 4 producers, 4 consumers, 4 partitions, 1 KiB payload, 15s.
- Leader target: `127.0.0.1:39090`.
- Result: 22,832 produced, 22,672 consumed, 1,522.10 msg/s produce rate, 1,511.44 msg/s consume rate, 0 errors.
- Publish latency: avg 42.030ms, p50 39.298ms, p95 63.744ms, p99 94.377ms.
- Fetch latency: avg 8.632ms, p50 8.482ms, p95 13.983ms, p99 25.402ms.

| Layer | Stage | Count | Avg |
| --- | --- | ---: | ---: |
| raft `produce_batches` | `apply_wait` | 368 | 38.245ms |
| raft `produce_batches` | `encode` | 368 | 0.164ms |
| raft `produce_batches` | `total` | 368 | 38.427ms |
| raft store `logs` | `put_many` | 740 | 8.958ms |
| FSM `produce_batches` | `apply` | 368 | 13.570ms |
| store `append_batch` | `total` | 1,431 | 3.476ms |
| store `append_batch` | `append_frame` | 1,431 | 1.049ms |
| broker `fetch` | `queue_fetch` | 849 | 7.328ms |
| store `read_from` | `total` | 849 | 6.885ms |
| store `read_from` | `read_segments` | 849 | 6.686ms |
| raft `commit_offsets` | `apply_wait` | 369 | 39.410ms |
| FSM `commit_offsets` | `apply` | 369 | 4.813ms |

This run shows local segment improvements but also exposes raft log variance:

- Coalescing frames per segment write reduced `append_frame` from about 3.6ms to about 1.0ms and cut `store append_batch total` from about 5.9ms to about 3.5ms.
- Caching read-only segment file handles reduced `read_segments` from about 8.6ms to about 6.7ms and improved fetch p95 from about 15ms to about 14ms compared with the previous best run.
- End-to-end throughput stayed near the previous best rather than increasing because raft log `put_many` was slower in this Docker sample (`~9.0ms` versus `~7.2ms`).
- A legacy bbolt `NoFreelistSync` experiment did not materially improve raft log writes in this workload, so it was not retained. Dragonboat now owns raft storage; raft log entry count and fsync cadence remain Dragonboat-level tuning targets.

After binary raft hot command codec, 2026-05-08:

- 3-node Docker raft cluster.
- Legacy raft commit flush interval: 5ms.
- `--batch-size 16`, 4 producers, 4 consumers, 4 partitions, 1 KiB payload, 15s.
- Leader target: `127.0.0.1:39090`.
- Result: 21,072 produced, 21,008 consumed, 1,404.43 msg/s produce rate, 1,400.16 msg/s consume rate, 0 errors.
- Publish latency: avg 45.455ms, p50 36.285ms, p95 100.158ms, p99 130.952ms.
- Fetch latency: avg 11.710ms, p50 9.364ms, p95 32.827ms, p99 66.494ms.
- Docker logs check: no `rollback`, `fail`, `error`, `panic`, or `fatal` matches in the final 200 lines.

| Layer | Stage | Count | Avg |
| --- | --- | ---: | ---: |
| raft `produce_batches` | `apply_wait` | 384 | 36.399ms |
| raft `produce_batches` | `encode` | 384 | 0.030ms |
| raft `produce_batches` | `total` | 384 | 36.450ms |
| raft store `logs` | `put_many` | 780 | 7.943ms |
| FSM `produce_batches` | `decode_payload` | 384 | 0.031ms |
| FSM `produce_batches` | `apply` | 384 | 14.224ms |
| store `append_batch` | `total` | 1,321 | 4.117ms |
| store `append_batch` | `append_frame` | 1,321 | 1.200ms |
| broker `fetch` | `queue_fetch` | 910 | 8.905ms |
| store `read_from` | `total` | 910 | 8.175ms |
| store `read_from` | `read_segments` | 910 | 7.959ms |
| raft `commit_offsets` | `apply_wait` | 393 | 36.497ms |
| FSM `commit_offsets` | `apply` | 393 | 5.668ms |

Raft log volume in this sample:

| Metric | Value |
| --- | ---: |
| raft log bytes | 22,196,655 |
| raft log entries | 780 |
| raft log bytes per entry | 28,457 |

Readout:

- The compact binary codec reduces raft payload overhead on `produce_batches` and `commit_offsets`; encode time drops from the previous sample's `~0.164ms` to `~0.030ms`.
- Raft log bytes per entry are now about 28.5 KiB in this workload. The previous local sample was roughly 32 MiB across about 740 log entries, so the hot command codec materially reduces log byte volume.
- End-to-end throughput did not exceed the previous best in this particular Docker run because p95 publish and fetch latency regressed. The remaining dominant costs are still raft log persistence (`put_many`) and local read/fetch IO variance, not command encoding.

## Pre-Dragonboat Local Kafka Comparison: 2026-05-09

This Kafka comparison was captured before the Dragonboat replacement. Keep it as a same-host external reference point, not as the current ech0 cluster result.

This run compares ech0 and Kafka on the same Windows Docker Desktop host. It is still not a perfectly identical workload because `ech0bench` runs simultaneous produce and consume through ech0's TCP API, while Kafka's built-in perf tools report producer and consumer paths separately.

Kafka environment:

- Image: `apache/kafka:4.2.0`.
- Cluster: 3 broker/controller KRaft nodes from `deploy/docker/docker-compose.kafka-bench.yml`.
- Topic: 4 partitions, replication factor 3, `min.insync.replicas=2`.
- Producer: 4 concurrent `kafka-producer-perf-test.sh` processes, total 200,000 records, 1 KiB record size, `acks=all`, `batch.size=16384`, `linger.ms=5`, no compression.
- Consumer: `kafka-consumer-perf-test.sh`, 200,000 records, fresh group, `auto.offset.reset=earliest`, run after produce to drain the topic.

ech0 environment:

- Image built from current workspace, commit `b8507d4`.
- Cluster: 3-node Docker raft cluster, clean `ECH0_CLUSTER_DATA_ROOT=./data/ech0-kafka-compare-20260509`.
- Workload: `--duration 15s --producers 4 --consumers 4 --partitions 4 --payload-bytes 1024 --batch-size 16 --fetch-batch 256`.
- Leader target: `127.0.0.1:29090`.

Results:

| System | Workload | Produced | Consumed | Produce Rate | Consume Rate | Latency Signal |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| ech0 TCP raft cluster | simultaneous produce/consume, 15s | 30,576 | 30,336 | 2,038.38 msg/s | 2,022.38 msg/s | publish p50 30.345ms, p95 36.489ms; fetch p99 11.191ms |
| Kafka KRaft cluster | producer-only wall clock | 200,000 | n/a | 19,969.82 msg/s | n/a | producer records had multi-second latency because the perf tool allows a large async backlog |
| Kafka KRaft cluster | consumer drain after produce | n/a | 200,000 | n/a | 69,255.17 msg/s wall clock; 166,805.67 msg/s fetch window | consumer perf fetch window excludes startup and group coordination |

Readout:

- On this host, Kafka producer-only wall-clock throughput was about 9.8x the pre-Dragonboat ech0 clustered produce rate.
- Compared with the current 4-shard Dragonboat async result above, Kafka's producer-only wall-clock rate is about 1.24x ech0's produce rate, but that is not a balanced comparison because the Kafka measurement did not run simultaneous consumers and ech0 consumers lagged in the async run.
- Kafka's consumer drain was about 34x the pre-Dragonboat ech0 run by full command wall clock, or about 82x by Kafka's own fetch-window metric.
- The Kafka producer comparison is throughput-oriented, not latency-equivalent. Kafka's perf producer keeps many requests in flight; the observed record latency was seconds, while ech0's TCP benchmark measured synchronous batch request latency.
- The pre-Dragonboat core architectural difference was single-group raft serialization. The current Dragonboat runtime now distributes data shard leaders, so the remaining gap is mostly in mature async batch log behavior, fetch/read throughput, and client/server copy overhead.

## Kafka and NATS Comparison Notes

Do not compare the local numbers above directly to public Kafka or NATS benchmark snippets unless all systems run on the same machine, payload size, durability settings, replication factor, batching, compression, and client concurrency.

Useful comparison anchors:

- Kafka's design emphasizes batching, sequential log IO, page cache reuse, zero-copy reads, compressed batches, and partition leaders distributed across brokers. See the Apache Kafka design documentation: <https://kafka.apache.org/39/design/design/>.
- Kafka ships producer and consumer performance tools through `kafka-producer-perf-test.sh` and `kafka-consumer-perf-test.sh`. See the Apache Kafka source scripts: <https://github.com/apache/kafka/blob/trunk/bin/kafka-producer-perf-test.sh> and <https://github.com/apache/kafka/blob/trunk/bin/kafka-consumer-perf-test.sh>.
- NATS provides `nats bench` and `nats bench js` for Core NATS and JetStream publish/consume/request-reply benchmarks. See the NATS benchmark documentation: <https://docs.nats.io/using-nats/nats-tools/nats_cli/natsbench>.
- NATS JetStream writes are documented as linearizable, and replicated streams acknowledge after quorum replication. File streams flush writes to the OS synchronously by default, with configurable `fsync` behavior. See the JetStream documentation: <https://docs.nats.io/nats-concepts/jetstream>.

Current ech0 positioning from this run:

| System Shape | Expected Difference |
| --- | --- |
| Kafka | Kafka should still be ahead for high-throughput logs because it has mature page-cache/zero-copy paths, async producer batching, and partition leaders distributed across brokers. ech0 now uses Dragonboat data shard groups and leader distribution, but its read/fetch and TCP copy paths are still early. |
| NATS Core | NATS Core should be much faster for ephemeral pub/sub and request/reply because it does not pay durable stream replication cost on every message. ech0 is a durable MQ path in these tests. |
| NATS JetStream R3 file storage | This is the closest external comparison shape, but JetStream has mature async/batch publication and tuned storage/replication. ech0 now has producer batching and local raft proposal coalescing, but its storage and raft paths are still early. |

Recommended fair comparison plan:

1. Run Kafka with replication factor 3, 1 KiB records, same host or same VM class, and report `acks=all` plus `kafka-producer-perf-test.sh` / `kafka-consumer-perf-test.sh`.
2. Run NATS JetStream with file storage, replicas 3, 1 KiB messages, and both sync and async `nats bench js pub`.
3. Run ech0 with both synchronous produce and `--producer-inflight > 1` async batched produce to separate client wait time from storage and raft overhead.
4. Capture CPU, disk write latency, fsync policy, and Docker-vs-native placement for every system.

## Bottleneck Reading Guide

- High `protocol` encode/decode allocations point to binary codec copy pressure.
- High `transport` time with small bodies points to frame IO overhead.
- Slow `StorxLogStoreAppend` relative to `MemoryStoreAppend` points to segment file or segment index cost.
- Slow `StorxLogStoreReadFrom` points to pointer lookup, segment seek/read, or record decode cost.
- Broker publish slower than store append points to routing, cloning, events, metrics, or Raft/propose overhead.
- TCP frame handling slower than broker publish points to protocol encode/decode and handler conversion overhead.
- Stress runs where produced greatly exceeds consumed point to fetch/commit/read bottlenecks.
- Stress runs with high publish p99 point to append path contention or disk flush behavior.
