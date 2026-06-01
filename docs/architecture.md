# Architecture

ech0 is a Go embedded message broker with a library-first public API and a single binary for operational deployments. The root package is intentionally small; it exposes the mental model needed to run an embedded broker without forcing callers to understand configuration loading, logging, Admin UI, OpenAPI, or Raft wiring.

## Package Boundaries

| Package | Role |
| --- | --- |
| `ech0` | Public embedded API. Opens and closes a broker, creates topics, publishes, fetches, acks, nacks, and schedules messages. |
| `broker` | Runtime orchestration. Owns queue/direct/request-reply APIs, Raft apply paths, scheduler jobs, admin server, metrics, and config loading. |
| `store` | Persistence contracts and implementations. Owns topic metadata contracts, offsets, groups, snapshots, segment log, and segment indexes. |
| `protocol` | Wire messages and custom binary command body codecs. |
| `transport` | TCP frame header/body IO. |
| `queue` | Topic/partition queue runtime over `store.MessageLogStore` and offset metadata. |
| `direct` | Direct inbox runtime used by direct messaging and request/reply responses. |
| `cmd/ech0` | Cobra-based single binary entry point. |

## Runtime Composition

The binary path builds a full broker runtime:

1. Build a `dix` application from a config source.
2. Load config with a `configx` provider from defaults, files, env, and flags.
3. Create logging, metrics, event bus, storage, broker, scheduler, TCP transport, and Admin UI dependencies from `dix` modules.
4. Open the segment log store and in-memory state machine metadata store.
5. Construct the broker service and TCP server.
6. Start Dragonboat, Admin UI, metrics, and scheduled jobs.

The binary runtime is split into `dix` modules for core broker state, scheduler, TCP transport, and admin surfaces. The `Config`, `*slog.Logger`, and `dix.EventLogger` are all provided by the module graph, so config loading and operational logging follow the same dependency-injection path as the rest of the runtime.

The embedded path in the root package does less:

1. Normalize `ech0.Options`.
2. Open segment-log storage under `Options.DataDir` and create the Dragonboat-backed state machine store.
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

The root API deliberately does not expose the full broker `Config`. Embedded users configure only required runtime choices such as data directory, node ID, payload limits, and optional peer settings.

The wire protocol includes transaction commands and record metadata in protocol version `1`. The broker now has a core transaction engine: transaction state is stored in metadata snapshots, transactional records carry metadata in the segment log, `read_committed` fetches hide open or aborted transactions, and `TxCommitOffset` lets consume-process-produce flows commit offsets with the transaction.

## Internal Service Model

`broker.Broker` is the main service boundary. Mutating operations are routed through command routers:

- Metadata commands are proposed through Dragonboat. In single-replica mode, partition-owned commands apply directly to local segment shards. In multi-node cluster mode, partition-owned commands target the owning Dragonboat data shard group.
- Coalesced produce and offset commit commands are split by shard before they enter Raft, so one client batch can become several independent group proposals.

Read operations use `raft.read_policy` to choose the clustered consistency cost:

- `local` reads directly from the local runtime and is the default low-latency mode.
- `leader` requires the local broker to be leader for the owning read group and performs a Dragonboat read barrier before reading.
- `linearizable` performs a Dragonboat read barrier for the owning read group, allowing follower callers to pay the barrier cost without requiring local leadership.

## Target Cluster Architecture

The broker uses Dragonboat as the metadata consensus runtime in both single-node and multi-node deployments. One configured peer is a single-replica cluster with local segment data shards; multiple peers form a replicated cluster with Dragonboat data shard groups. The broker starts one Dragonboat NodeHost per process and always starts the metadata group. Data shard groups start only in multi-node cluster mode.

The target clustered architecture splits the system into a control plane and a data plane:

| Plane | Ownership | Write Path |
| --- | --- | --- |
| Metadata Raft group | Topic configs, partition placement, cluster members, consumer group membership, scheduler leases, and shard membership. | Low-frequency metadata commands. |
| Data shard Raft groups | Message appends, partition offsets, consumer offsets, retry/delay internal topic records, and direct inbox records. | High-frequency per-shard commands. |

Data shards are the scalability unit. A shard owns one or more topic partitions. For Kafka-like scaling, operators can configure shard count at or above the hot partition count so leaders distribute across nodes. A first production implementation can map `topic/partition` to a stable shard ID, then later support explicit placement and rebalancing.

The current clustered implementation has these properties:

- `broker.data_shard_count` configures the deterministic shard plan.
- Topic creation persists one `ShardPlacement` per `topic/partition`.
- The default placement is `partition % data_shard_count`.
- Memory and file-backed metadata stores both persist shard placements.
- Snapshots include `shard_placements`, so future Raft metadata snapshots carry the placement map.
- Command routing now carries partition command targets.
- Cluster mode installs a cluster router that resolves known `topic/partition` targets to shard IDs and proposes data commands to the matching Dragonboat data group.
- Coalesced `produce_batches` and `commit_offsets` are split by resolved shard target before dispatch. Result merging preserves original request order across group proposals.
- Non-explicit produce partitioning is resolved before dispatch and rewritten to an explicit partition command. That makes the data-plane command target stable for future per-shard Raft groups.
- The router depends on a `dataShardRuntime` boundary. Single-replica mode points each configured shard at a local segment runtime; multi-node cluster mode points each shard at a Dragonboat group runtime.
- Each configured shard also has a runtime spec with a target directory for its shard-local segment log. Runtime health exposes the configured shard IDs and their current runtime mode.
- The broker message runtime is now an internal interface. The default adapter preserves the existing single log behavior.
- When sharded segment storage is used with `broker.data_shard_count > 1`, broker message reads and writes use a sharded message runtime. Each shard opens its own segment log under `data/shards/shard-NNNN`.
- Topic metadata remains global. `CreateTopic` writes one global topic config, while each message shard initializes its own local log state for that topic.
- `Publish`, `Fetch`, `Ack`, admin topic message snapshots, and direct `ReadFrom` helpers route by the resolved `topic/partition -> shard` placement.
- Retention and compaction maintenance run through the message runtime, so sharded segment mode applies cleanup across all local shard logs.
- Raft FSM snapshots now read and restore message data through the message runtime, so sharded segment snapshots merge shard log records and restore them by recorded placement.
- Clustered writes no longer use the single global data path. Live partition reassignment now moves Storx segment data between shards before committing placement. The remaining cluster work is leader-aware client routing/forwarding, per-group scheduler ownership, and narrower per-group snapshots.

The public API remains library-first:

- Embedded users still call `Open`, `CreateTopic`, `Publish`, `Fetch`, `Ack`, `Nack`, `Schedule`, and request/reply helpers.
- Binary users run one `ech0` process per node; a single node is represented as a single-replica Dragonboat cluster.
- The root package does not expose Raft group topology. It only exposes minimal cluster options.

## Target Write Routing

Mutating commands should no longer call one global `proposeOrApply` path. They should route by command ownership:

| Command Family | Target |
| --- | --- |
| `create_topic`, topic policy changes | Metadata group, then create local data shard state. |
| `produce`, `produce_batch`, `produce_batches` | Data shard for each target `topic/partition`. |
| `commit_offset`, `commit_offsets` | Data shard for the committed `topic/partition`. |
| `nack`, retry processing, delay scheduling | Data shard for the source or internal retry/delay partition. |
| `direct_send`, `direct_ack` | Data shard for the recipient inbox partition. |
| `join_group`, `heartbeat_group`, `rebalance_group` | Metadata group for membership and assignment state. |

This creates two router implementations:

| Router | Use |
| --- | --- |
| Metadata-only router | Single-replica mode. Metadata commands go through Dragonboat; data commands apply to local segment shards. |
| Cluster router | Multi-node mode. Resolves metadata commands to the metadata group and data commands to the owning data shard group. |

The implementation has cut over to the Dragonboat group router. The old single global clustered data path is no longer part of the runtime.

## Target Read Routing

Reads also need a clear ownership model:

| Read | Default |
| --- | --- |
| `Fetch` | Use `raft.read_policy`: default local reads for throughput, leader-only reads for strict leader ownership, or linearizable read barriers for follower-safe reads. |
| Admin topic metadata | Read from local metadata cache backed by the metadata group. |
| Admin message browser | Route to the owning data shard. |
| Consumer group views | Combine metadata group membership with data shard offsets. |

This keeps the default read path cheap while letting operators opt into Dragonboat read barriers when stale follower reads are not acceptable.

## Target Storage Layout

The target storage layout keeps Dragonboat state and shard-local segment logs separate:

```text
data/
  dragonboat/
    1/
      wal/
      ...
  shards/
    shard-0000/
      segments/
    shard-0001/
      segments/
```

Within a shard, the segment log remains the message storage backend and maintains its own `*.idx` files for offset-to-frame pointers. Experimental `segment_read_mode = "mmap"` maps sealed segment files for reads, while active segments continue to use positional reads. Dragonboat owns the metadata log in single-replica mode and both metadata/data ordering logs in multi-node mode. In multi-node mode message writes still pass through Raft and the segment log, but the work is spread across independent groups and independent stores instead of one global queue.

A later, more invasive storage optimization can make the Raft log and message segment log share a batch format or reduce duplicated payload writes. That should be a second-stage optimization after sharding proves out.

## Implementation Phases

1. Introduce command routing interfaces:
   - `MetadataCommandRouter`
   - `PartitionCommandRouter`
   - `ClusterDirectory`
   - `ShardResolver`

2. Split command ownership:
   - Move topic and group membership commands to metadata ownership.
   - Move produce, fetch, offset commits, retry/delay records, and direct inbox records to partition ownership.

3. Add shard placement metadata:
   - Stable `ShardID`.
   - `topic/partition -> shard`.
   - `shard -> raft peers`.
   - `shard -> leader`.

4. Add data shard runtimes:
   - One local queue/store runtime per shard.
   - One Dragonboat Raft group per shard in clustered mode.
   - Separate shard-local segment stores.

5. Cut over hot paths:
   - Route produce batches to data shard leaders.
   - Route commit offsets to the same data shard as the topic partition.
   - Route fetches to the data shard owner.

6. Rework scheduler ownership:
   - Retry and delay workers should run on the leader of the internal topic shard, not only on the global cluster leader.
   - Retention and compaction should run per shard leader.

7. Add leader-aware client routing:
   - Return leader hints on not-leader errors.
   - Forward internal broker requests to the owning group leader when direct client routing is not available.
   - Keep embedded mode usable with minimal peer configuration.

8. Add cluster control operations:
   - Dragonboat add/remove voting replica requests for every configured broker group.
   - Best-effort leader transfer and leader balance requests.
   - Live shard reassignment that moves partition segment data before committing placement.

## Expected Performance Shape

The first target is removing the single-leader ceiling while keeping the single-node hot path batch-first:

- With 4 data shards and balanced leaders, the write path should be able to use multiple raft FSMs and multiple segment stores.
- Produce and commit offset commands should queue only behind work for their shard, not the whole cluster.
- Fetch pressure should spread across shard stores.

The main remaining gaps to Kafka after this change are replicated storage efficiency and network/client maturity:

- Kafka has a partition log that is both the storage log and replication unit.
- ech0 will still have Raft log plus segment log until the second-stage storage optimization.
- ech0 now has broker-side append batching, batch segment frames, and asynchronous group sync for the local segment path.
- Kafka still has more mature TCP batching, page-cache/zero-copy reads, and heavily optimized producer and consumer clients.

## Dependency Strategy

The implementation favors arcgolabs libraries where they match the responsibility:

- `dix` for runtime dependency organization.
- `logx` for binary logging integration.
- `eventx` for broker control-plane events with bounded `ants` dispatch.
- `collectionx` for collection operations, protocol codec registries, recent event buffers, shard metadata caches, and group assignment views.
- `configx` for binary configuration loading through a `dix` provider.
- Native Fiber v3 for Admin HTTP and operator UI surfaces.
- `observabilityx` for metrics wiring.
- OpenTelemetry `trace.TracerProvider` injection for library-safe tracing.
- `dragonboat` for clustered multi-group Raft.
- `ants` for bounded background and shard fan-out work.
- `HdrHistogram` for benchmark latency percentiles.
- `fgprof` for optional admin-side wall-clock profiling.

These dependencies are allowed inside implementation and binary-facing packages, but the public embedded API should stay smaller than the operational runtime.

## Background Work

Retry, delay, retention cleanup, compaction, configured sinks, mirror jobs, and database outbox pollers run through `go-co-op/gocron`. The scheduler uses a Dragonboat-backed distributed elector so only the current leader runs jobs.

This avoids duplicate background processing across cluster members while keeping the job logic independent from Raft internals.

Configured integration jobs share an at-least-once contract: read committed records, perform the external side effect, then commit the integration consumer offset only after the side effect succeeds. This keeps webhook, file, S3-compatible, and mirror sinks consistent with the broker's pull-consumer model while leaving exactly-once behavior to idempotent producers, transactional writes, or downstream dedupe.
