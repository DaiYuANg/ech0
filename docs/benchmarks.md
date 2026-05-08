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
| `store` | Memory store baseline, storx segment append, batch append, and indexed reads. |
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
ECH0_CLUSTER_DATA_ROOT=./data/bench-cluster ECH0_RAFT_COMMIT_TIMEOUT_MS=5 docker compose -f docker-compose.cluster.yml up -d --build
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
| `--fetch-batch` | `128` | Max records per fetch. |
| `--poll-idle` | `1ms` | Consumer sleep after an empty fetch. |
| `--samples` | `200000` | Max latency samples retained for percentile output. |

The report includes:

- Produced and consumed message counts.
- Publish and consume throughput.
- Publish and fetch latency average, p50, p95, p99, and max.
- Publish and consume error counts.

Publish latency is measured per produce operation. When `--batch-size` is greater than 1, throughput counts messages but publish latency counts batch requests.

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
| `store` storx append 1KiB | 1.286 ms/op | 72,511 B/op, 865 allocs/op |
| `store` storx append batch 100x1KiB | 1.717 ms/op | 730,650 B/op, 6,131 allocs/op |
| `store` storx read 100x1KiB | 0.717 ms/op | 652,083 B/op, 6,299 allocs/op |
| `broker` publish, memory store | 189.749 us/op | 5,988 B/op, 15 allocs/op |
| `broker` TCP produce frame, memory store | 101.399 us/op | 8,283 B/op, 62 allocs/op |
| `mapper` fetch records manual, 100x1KiB | 24.358 us/op | 141,728 B/op, 701 allocs/op |
| `mapper` fetch records mapper, 100x1KiB | 297.741 us/op | 234,757 B/op, 4,716 allocs/op |

Readout:

- Protocol and frame IO are not the current bottleneck for 1 KiB messages.
- The persistent path is dominated by storx append/index work. A single storx append is roughly 6.8x slower than the broker memory-store publish benchmark.
- The raft cluster path is roughly 13.3x lower throughput than embedded mode in this run. The difference is expected because the benchmark currently sends one TCP produce per message and raft serializes writes through a single leader/quorum path.
- Mapper remains useful for control-plane and maintenance conversions, but manual conversion should stay on hot fetch/produce loops.
- The next highest-impact benchmark feature is producer batching. Kafka and NATS both rely heavily on async or batch publication for throughput, while this `ech0bench` run intentionally used one produce request per message.

Follow-up runs after adding `--batch-size` and configurable `raft.commit_timeout_ms`:

| Mode | Settings | Produce Rate | Consume Rate | Publish p50 | Publish p95 | Errors |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Embedded library | `batch_size=16`, 15s | 984.46 msg/s | 983.07 msg/s | 64.248ms per batch | 79.024ms per batch | 0 |
| TCP raft cluster | `commit_timeout_ms=5`, `batch_size=1`, 15s | 73.33 msg/s | 72.93 msg/s | 50.017ms | 71.646ms | 0 |
| TCP raft cluster | `commit_timeout_ms=5`, `batch_size=16`, 15s | 109.86 msg/s | 102.40 msg/s | 568.516ms per batch | 608.446ms per batch | 0 |
| TCP raft cluster | `commit_timeout_ms=5`, `batch_size=16`, real store batch append, 15s | 429.83 msg/s | 248.51 msg/s | 63.611ms per batch | 93.292ms per batch | 0 |
| TCP raft cluster | `commit_timeout_ms=5`, `batch_size=16`, raft proposal coalescing, 15s | 456.51 msg/s | 433.04 msg/s | 59.726ms per batch | 116.874ms per batch | 0 |
| TCP raft cluster | `commit_timeout_ms=5`, `batch_size=16`, coalescing + grouped segment reads, 15s | 1,041.05 msg/s | 1,030.38 msg/s | 41.487ms per batch | 56.762ms per batch | 0 |
| TCP raft cluster | `commit_timeout_ms=5`, `batch_size=16`, partition locks + grouped produce apply, 15s | 1,557.19 msg/s | 1,546.53 msg/s | 40.275ms per batch | 45.957ms per batch | 0 |
| TCP raft cluster | `commit_timeout_ms=5`, `batch_size=16`, segment write coalescing + reader cache, 15s | 1,522.10 msg/s | 1,511.44 msg/s | 39.298ms per batch | 63.744ms per batch | 0 |

These follow-up numbers show that making `CommitTimeout` configurable is necessary but not enough by itself. The real store batch append path improved cluster produce throughput by roughly 3.9x over the previous `batch_size=16` run and moved batch publish p50 from about 569ms to about 64ms. Raft proposal coalescing helped consume keep up by batching commit offsets. Grouped segment reads then removed the major fetch bottleneck and moved this local Docker run above 1,000 msg/s for both produce and consume.

## Cluster Metrics Drilldown

The broker exports hot-path histograms for raft, FSM, and append stages:

- `ech0_broker_command_duration_seconds`
- `ech0_broker_raft_stage_duration_seconds`
- `ech0_broker_raft_store_stage_duration_seconds`
- `ech0_broker_fsm_stage_duration_seconds`
- `ech0_broker_fetch_stage_duration_seconds`
- `ech0_store_append_stage_duration_seconds`
- `ech0_store_read_stage_duration_seconds`
- `ech0_store_append_requests_total`
- `ech0_store_append_records_total`
- `ech0_store_read_requests_total`
- `ech0_store_read_records_total`

Pre-optimization sample run, 2026-05-08:

- 3-node Docker raft cluster.
- `ECH0_RAFT_COMMIT_TIMEOUT_MS=5`.
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
- `ECH0_RAFT_COMMIT_TIMEOUT_MS=5`.
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
- Raft `apply_wait` is now about 65.8ms versus FSM `total` at about 16.5ms, so the remaining cluster produce latency is mostly raft queue/quorum/log wait rather than command JSON codec or Badger index writes.
- Consumer throughput trails producer throughput in this sample. The next benchmark pass should separate read/fetch batching, consumer offset commit cadence, and backlog drain behavior.

After raft coalescing, persistent segment writers, and grouped segment reads, 2026-05-08:

- 3-node Docker raft cluster.
- `ECH0_RAFT_COMMIT_TIMEOUT_MS=5`.
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
- `ECH0_RAFT_COMMIT_TIMEOUT_MS=5`.
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
- `ECH0_RAFT_COMMIT_TIMEOUT_MS=5`.
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
- A bbolt `NoFreelistSync` experiment did not materially improve raft log writes in this workload, so it was not retained. The next high-impact area is reducing raft log entry count or replacing the JSON raft command payload on the hot produce path.

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
| Kafka | Kafka should still be materially faster for high-throughput logs because it has mature page-cache/zero-copy paths, async producer batching, and partition leaders distributed across brokers. ech0 now batches raft proposals locally, but does not yet distribute partition leadership. |
| NATS Core | NATS Core should be much faster for ephemeral pub/sub and request/reply because it does not pay durable stream replication cost on every message. ech0 is a durable MQ path in these tests. |
| NATS JetStream R3 file storage | This is the closest external comparison shape, but JetStream has mature async/batch publication and tuned storage/replication. ech0 now has producer batching and local raft proposal coalescing, but its storage and raft paths are still early. |

Recommended fair comparison plan:

1. Run Kafka with replication factor 3, 1 KiB records, same host or same VM class, and report `acks=all` plus `kafka-producer-perf-test.sh` / `kafka-consumer-perf-test.sh`.
2. Run NATS JetStream with file storage, replicas 3, 1 KiB messages, and both sync and async `nats bench js pub`.
3. Run ech0 with both per-message produce and batched produce to separate protocol overhead from storage and raft overhead.
4. Capture CPU, disk write latency, fsync policy, and Docker-vs-native placement for every system.

## Bottleneck Reading Guide

- High `protocol` encode/decode allocations point to binary codec copy pressure.
- High `transport` time with small bodies points to frame IO overhead.
- Slow `StorxLogStoreAppend` relative to `MemoryStoreAppend` points to segment file or Badger index cost.
- Slow `StorxLogStoreReadFrom` points to pointer lookup, segment seek/read, or record decode cost.
- Broker publish slower than store append points to routing, cloning, events, metrics, or Raft/propose overhead.
- TCP frame handling slower than broker publish points to protocol encode/decode and handler conversion overhead.
- Stress runs where produced greatly exceeds consumed point to fetch/commit/read bottlenecks.
- Stress runs with high publish p99 point to append path contention or disk flush behavior.
