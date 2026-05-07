# Architecture

ech0 is a Go embedded message broker with a library-first public API and a standalone binary for operational deployments. The root package is intentionally small; it exposes the mental model needed to run an embedded broker without forcing callers to understand configuration loading, logging, Admin UI, OpenAPI, or Raft wiring.

## Package Boundaries

| Package | Role |
| --- | --- |
| `ech0` | Public embedded API. Opens and closes a broker, creates topics, publishes, fetches, acks, nacks, and schedules messages. |
| `broker` | Runtime orchestration. Owns queue/direct/request-reply APIs, Raft apply paths, scheduler jobs, admin server, metrics, and config loading. |
| `store` | Persistence contracts and implementations. Owns topic metadata, offsets, groups, snapshots, segment log, Badger index, and bbolt metadata. |
| `protocol` | Wire messages and custom binary command body codecs. |
| `transport` | TCP frame header/body IO. |
| `queue` | Topic/partition queue runtime over `store.MessageLogStore` and offset metadata. |
| `direct` | Direct inbox runtime used by direct messaging and request/reply responses. |
| `cmd/ech0` | Cobra-based single binary entry point. |

## Runtime Composition

The binary path builds a full broker runtime:

1. Load config with `configx` from defaults, files, env, and flags.
2. Initialize logging and runtime dependencies through the broker package.
3. Open the segment log store and metadata store.
4. Construct the broker service and TCP server.
5. Optionally start Raft, Admin UI, metrics, and scheduled jobs.

The embedded path in the root package does less:

1. Normalize `ech0.Options`.
2. Open storx-backed stores under `Options.DataDir`.
3. Construct and start the internal broker.
4. Start the scheduled runtime if retry or delay is enabled.
5. Return a small `*ech0.Broker` API to the caller.

This split keeps `configx`, logstore-style binary configuration, Admin UI, OpenAPI, and operational options out of the root API.

## Library-First API

The root `ech0` package exposes:

- `Open(ctx, Options)` and `Run(ctx, Options)`.
- Topic creation through `CreateTopic`.
- Publishing through `Publish`.
- Pull consumption through `Fetch`, `Ack`, and `Commit`.
- Failure and delayed delivery through `Nack` and `Schedule`.

The root API deliberately does not expose the full broker `Config`. Embedded users configure only required runtime choices such as data directory, node ID, payload limits, and optional Raft settings.

## Internal Service Model

`broker.Broker` is the main service boundary. Mutating operations are routed through `proposeOrApply`:

- In standalone mode, commands apply directly to the local store-backed runtime.
- In Raft mode, commands are proposed to the Raft leader and applied by the FSM.

Read operations use the local runtime today, with `RaftReadPolicy` reserved in configuration for stricter clustered read behavior.

## Dependency Strategy

The implementation favors arcgolabs libraries where they match the responsibility:

- `dix` for runtime dependency organization.
- `logx` for binary logging integration.
- `eventx` for broker events.
- `collectionx` for collection operations and protocol codec registries.
- `configx` for binary configuration loading.
- `httpx` on Fiber for Admin/OpenAPI surfaces.
- `observabilityx` for metrics wiring.
- `storx` adapters for Badger and bbolt-backed stores.

These dependencies are allowed inside implementation and binary-facing packages, but the public embedded API should stay smaller than the operational runtime.

## Background Work

Retry, delay, retention cleanup, and compaction run through `go-co-op/gocron`. In standalone mode, jobs run locally. In Raft mode, the scheduler uses a distributed elector so only the current leader runs jobs.

This avoids duplicate background processing across cluster members while keeping the job logic independent from Raft internals.

