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
| `--fetch-batch` | `128` | Max records per fetch. |
| `--poll-idle` | `1ms` | Consumer sleep after an empty fetch. |
| `--samples` | `200000` | Max latency samples retained for percentile output. |

The report includes:

- Produced and consumed message counts.
- Publish and consume throughput.
- Publish and fetch latency average, p50, p95, p99, and max.
- Publish and consume error counts.

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
| `store` storx read 100x1KiB | 8.866 ms/op | 757,581 B/op, 8,374 allocs/op |
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
| Kafka | Kafka should be materially faster for high-throughput logs because it batches producer requests, writes compressed batches, uses mature page-cache/zero-copy paths, and distributes partition leaders. ech0 does not yet distribute partition leadership or batch raft proposals. |
| NATS Core | NATS Core should be much faster for ephemeral pub/sub and request/reply because it does not pay durable stream replication cost on every message. ech0 is a durable MQ path in these tests. |
| NATS JetStream R3 file storage | This is the closest external comparison shape, but JetStream has mature async/batch publication and tuned storage/replication. ech0 currently uses per-message TCP produce plus raft apply, so the gap is expected. |

Recommended fair comparison plan:

1. Run Kafka with replication factor 3, 1 KiB records, same host or same VM class, and report `acks=all` plus `kafka-producer-perf-test.sh` / `kafka-consumer-perf-test.sh`.
2. Run NATS JetStream with file storage, replicas 3, 1 KiB messages, and both sync and async `nats bench js pub`.
3. Run ech0 with both per-message produce and future batched produce to separate protocol overhead from storage and raft overhead.
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
