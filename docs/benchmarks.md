# Benchmarks and Stress Testing

ech0 includes two benchmark layers:

- Go benchmarks for repeatable micro and subsystem measurements.
- `cmd/ech0bench` for an end-to-end sustained stress run against the embedded broker path.

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

Useful flags:

| Flag | Default | Meaning |
| --- | ---: | --- |
| `--data-dir` | temp dir | Store data directory. Leave empty for disposable runs. |
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

## Bottleneck Reading Guide

- High `protocol` encode/decode allocations point to binary codec copy pressure.
- High `transport` time with small bodies points to frame IO overhead.
- Slow `StorxLogStoreAppend` relative to `MemoryStoreAppend` points to segment file or Badger index cost.
- Slow `StorxLogStoreReadFrom` points to pointer lookup, segment seek/read, or record decode cost.
- Broker publish slower than store append points to routing, cloning, events, metrics, or Raft/propose overhead.
- TCP frame handling slower than broker publish points to protocol encode/decode and handler conversion overhead.
- Stress runs where produced greatly exceeds consumed point to fetch/commit/read bottlenecks.
- Stress runs with high publish p99 point to append path contention or disk flush behavior.

