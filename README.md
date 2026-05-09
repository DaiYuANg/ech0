# ech0

`ech0` is a Go embedded message broker that can be used as a small library first, while still shipping as a single executable for standalone or Raft-backed clustered deployments.

The public root package keeps the mental model intentionally small: configure a data directory and a few broker options, open a broker, then create topics, publish, fetch, ack, nack, or schedule delayed messages. Operational wiring such as `configx`, `logstore`, storage internals, Admin UI, OpenAPI, and cluster setup stays behind the binary and advanced packages.

## Features

- Library-first embedded broker API in the root `ech0` package.
- Single binary entry point in `cmd/ech0`.
- Persistent segment-log storage with shard-local binary indexes.
- Dependency injection, logging, events, and helpers using `arcgolabs/dix`, `logx`, `eventx`, and `collectionx`.
- Admin and OpenAPI HTTP surface built with `arcgolabs/httpx` on Fiber.
- Raft mode for clustered broker coordination.
- Retry, delay, nack, and scheduled workers using `go-co-op/gocron`, gated by Raft leadership in cluster mode.

## Library Usage

```go
package main

import (
	"context"
	"log"

	ech0 "github.com/DaiYuANg/ech0"
)

func main() {
	ctx := context.Background()

	mq, err := ech0.Open(ctx, ech0.Options{
		DataDir: "./data",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close(ctx)

	if err := mq.CreateTopic(ctx, "orders"); err != nil {
		log.Fatal(err)
	}

	if _, err := mq.Publish(ctx, "orders", []byte(`{"event":"created"}`)); err != nil {
		log.Fatal(err)
	}

	batch, err := mq.Fetch(ctx, "worker-1", "orders", ech0.FetchLimit(10))
	if err != nil {
		log.Fatal(err)
	}

	for _, msg := range batch.Messages {
		if err := mq.Ack(ctx, "worker-1", msg); err != nil {
			log.Fatal(err)
		}
	}
}
```

For producer-side batching and multiple in-flight publishes, use the library producer:

```go
producer, err := mq.NewProducer(ctx, "orders",
	ech0.ProducerBatchSize(64),
	ech0.ProducerInFlight(8),
)
if err != nil {
	log.Fatal(err)
}
defer producer.Close(ctx)

future, err := producer.Send(ctx, []byte(`{"event":"created"}`), ech0.Key([]byte("order-1")))
if err != nil {
	log.Fatal(err)
}
if _, err := future.Await(ctx); err != nil {
	log.Fatal(err)
}
```

Use the `broker` and `store` packages only when you need lower-level control over runtime wiring, storage, Raft, or transports.

## Binary Usage

Run a standalone broker:

```sh
go run ./cmd/ech0 --config config/ech0.toml.example --raft=false
```

Common flags:

```sh
--config config/ech0.toml.example
--broker-addr 127.0.0.1:9092
--admin-addr 127.0.0.1:8080
--data-dir ./data
--raft=false
```

Configuration is loaded with `arcgolabs/configx`. Environment variables use the `ECH0` prefix and `__` as the nesting separator, for example `ECH0_BROKER__BIND_ADDR` or `ECH0_RAFT__ENABLED`.

## Admin Endpoints

- `GET /healthz`
- `GET /metrics`
- `GET /docs`
- `GET /openapi.json`
- `GET /api/healthz`
- `GET /api/topics`
- `GET /api/metrics`

When `admin.debug_enabled = true`, debug builds also expose `GET /debug/fgprof` on the admin server for wall-clock profiling.

## Development

```sh
go test ./... -count=1
go run ./cmd/ech0 --config config/ech0.toml.example
```

Design documentation:

- [Architecture](docs/architecture.md)
- [Wire Protocol](docs/wire-protocol.md)
- [Storage](docs/storage.md)
- [Request Reply](docs/request-reply.md)
- [Operations](docs/operations.md)
- [Benchmarks](docs/benchmarks.md)

Docker examples:

```sh
cd deploy/docker
docker compose -f docker-compose.single.yml up --build
docker compose -f docker-compose.cluster.yml up --build
docker compose -f docker-compose.single.release.yml up
docker build -f ../../Dockerfile -t ech0:local ../..
```

## Release

Releases are driven by GoReleaser. A tag such as `v0.1.0` builds:

- `ech0` archives for Linux, macOS, and Windows.
- Linux `.deb` and `.rpm` packages with `/etc/ech0/ech0.toml` and a systemd unit.
- Multi-platform Docker images for `linux/amd64` and `linux/arm64` published to GHCR.
- UPX-compressed Linux release and Docker binaries.

Local checks:

```sh
goreleaser check
goreleaser release --snapshot --clean
```

Full local release verification expects `upx` and Docker to be available. Without Docker, use `goreleaser release --snapshot --clean --skip=docker` to verify archives and Linux packages.

GitHub release publishing runs from `.github/workflows/release.yml` on `v*` tags.
