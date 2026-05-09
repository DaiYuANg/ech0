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

`broker.Broker` is the main service boundary. Mutating operations are routed through command routers:

- In standalone mode, commands apply directly to the local store-backed runtime.
- In Raft mode, commands are proposed to the Raft leader and applied by the FSM.
- Metadata commands and partition-owned commands are separated before they enter the compatibility Raft path.

Read operations use the local runtime today, with `RaftReadPolicy` reserved in configuration for stricter clustered read behavior.

## Target Cluster Architecture

The current Raft mode uses one Raft group for all mutating commands. This is simple, but it puts every topic, partition, direct inbox, retry, delay, consumer offset, and group update behind one leader and one ordered FSM. That design is the main reason the clustered TCP benchmark is far below Kafka-like throughput.

The target clustered architecture splits the system into a control plane and a data plane:

| Plane | Ownership | Write Path |
| --- | --- | --- |
| Metadata Raft group | Topic configs, partition placement, cluster members, consumer group membership, scheduler leases, and shard membership. | Low-frequency metadata commands. |
| Data shard Raft groups | Message appends, partition offsets, consumer offsets, retry/delay internal topic records, and direct inbox records. | High-frequency per-shard commands. |

Data shards are the scalability unit. A shard owns one or more topic partitions. For Kafka-like scaling, operators can configure shard count at or above the hot partition count so leaders distribute across nodes. A first production implementation can map `topic/partition` to a stable shard ID, then later support explicit placement and rebalancing.

The current first step has landed the placement model without changing runtime routing behavior:

- `broker.data_shard_count` configures the deterministic shard plan.
- Topic creation persists one `ShardPlacement` per `topic/partition`.
- The default placement is `partition % data_shard_count`.
- Memory and storx metadata stores both persist shard placements.
- Snapshots include `shard_placements`, so future Raft metadata snapshots carry the placement map.
- Command routing now carries partition command targets.
- Cluster mode installs a cluster router skeleton that resolves known `topic/partition` targets to shard IDs and then delegates to the current single Raft group.
- Coalesced `produce_batches` and `commit_offsets` are split by resolved shard target before dispatch. The current runtime still delegates each shard group to the compatibility single Raft group, but result merging already preserves original request order.
- Non-explicit produce partitioning is resolved before dispatch and rewritten to an explicit partition command. That makes the data-plane command target stable for future per-shard Raft groups.
- The cluster router now depends on a `dataShardRuntime` boundary. The default implementation is still a single-group adapter, but data commands now enter the codebase through an explicit shard runtime seam.
- Data shard runtimes are registered behind a shard registry. Today each configured shard points at the compatibility single-group runtime; the next implementation can swap individual shard entries to local or Raft-backed runtimes.

The public API remains library-first:

- Embedded users still call `Open`, `CreateTopic`, `Publish`, `Fetch`, `Ack`, `Nack`, `Schedule`, and request/reply helpers.
- Standalone binary users still run one `ech0` process per node.
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
| Local router | Embedded and non-Raft mode. Applies directly to local queue/store. |
| Cluster router | Raft mode. Resolves metadata commands to the metadata group and data commands to the owning data shard group. |

The current single Raft path should be kept temporarily behind a compatibility router while the command split lands. Once data shard groups are functional, the single-group command path can be removed from clustered mode.

## Target Read Routing

Reads also need a clear ownership model:

| Read | Default |
| --- | --- |
| `Fetch` | Read from the owning data shard leader for correctness, with follower/local reads as a later read policy optimization. |
| Admin topic metadata | Read from local metadata cache backed by the metadata group. |
| Admin message browser | Route to the owning data shard. |
| Consumer group views | Combine metadata group membership with data shard offsets. |

This keeps correctness simple during the first architecture change. Faster follower reads can be added after shard placement and high watermark tracking are stable.

## Target Storage Layout

The target storage layout keeps badger plus segment log, but scopes it by ownership:

```text
data/
  metadata/
    raft/
    bbolt-or-badger/
  shards/
    shard-0000/
      raft/
      segments/
      badger/
    shard-0001/
      raft/
      segments/
      badger/
```

Within a shard, the segment log remains the message storage backend. The Raft log is the replication and ordering log for that shard. This still writes through Raft and the segment log, but the work is spread across independent leaders and independent stores instead of one global queue.

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
   - Keep the current single Raft router as an adapter during the transition.

3. Add shard placement metadata:
   - Stable `ShardID`.
   - `topic/partition -> shard`.
   - `shard -> raft peers`.
   - `shard -> leader`.

4. Add data shard runtimes:
   - One local queue/store runtime per shard.
   - One Raft FSM per shard in clustered mode.
   - Separate raft stores and snapshots per shard.

5. Add raft transport multiplexing:
   - Prefer one node-to-node transport listener with a group ID prefix.
   - Avoid requiring one TCP port per shard in the final design.
   - A temporary dev implementation may use derived ports, but the production design should use a mux.

6. Cut over hot paths:
   - Route produce batches to data shard leaders.
   - Route commit offsets to the same data shard as the topic partition.
   - Route fetches to the data shard owner.

7. Rework scheduler ownership:
   - Retry and delay workers should run on the leader of the internal topic shard, not only on the global cluster leader.
   - Retention and compaction should run per shard leader.

8. Remove the single global clustered data path:
   - Keep metadata Raft.
   - Keep local non-Raft mode.
   - Remove clustered message writes from the metadata/global group.

## Expected Performance Shape

The first target is not matching Kafka immediately. The first target is removing the single-leader ceiling:

- With 4 data shards and balanced leaders, the write path should be able to use multiple raft FSMs and multiple segment stores.
- Produce and commit offset commands should queue only behind work for their shard, not the whole cluster.
- Fetch pressure should spread across shard stores.

The main remaining gap to Kafka after this change will be storage efficiency:

- Kafka has a partition log that is both the storage log and replication unit.
- ech0 will still have Raft log plus segment log until the second-stage storage optimization.
- Kafka has mature client async batching, batch compression, and zero-copy/page-cache-heavy reads. ech0 still needs those as separate work items.

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
