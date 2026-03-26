# Broker Design and Implementation

[中文版本](../zh-CN/broker-design.md)

This document describes the current design and implementation of the `broker` crate. It is not an external protocol reference. It is an implementation guide for maintainers, and it should be updated whenever broker bootstrap, runtime behavior, background workers, or admin behavior changes.

## 1. Where Broker Sits in the Workspace

Inside the `ech0` workspace, `broker` is the layer that assembles protocol handling, runtimes, storage, and the admin plane into a single process.

- `protocol` defines command IDs and request/response types.
- `transport` handles frame encoding and decoding.
- `queue` and `direct` provide the two business runtimes.
- `store` provides message-log storage, metadata, state-machine execution, and consistency-related command handling.
- `broker` wires these pieces into a runnable TCP broker and adds Admin HTTP, background workers, and observability.

In practice, `broker` is primarily a process-runtime and orchestration layer rather than a single business component.

## 2. Bootstrap and Process Wiring

The main entrypoint is `broker/src/lib.rs`:

1. load `AppConfig`
2. initialize logging and Prometheus metrics
3. open `SegmentLog` and `RedbMetadataStore`
4. in standalone mode, ensure the bootstrap topic exists and validate topic consistency between catalog and log
5. choose `BrokerRuntimeMode::Standalone` or `BrokerRuntimeMode::Raft`
6. construct `BrokerService`
7. build TCP server and Admin HTTP wiring through the `di` module
8. spawn background workers
9. hand control to lifecycle management and wait for the shutdown future

Important details in the current implementation:

- `SegmentLogOptions` receives `storage.compaction_sealed_segment_batch`, which limits how many sealed segments can be compacted in one pass.
- broker state is persisted into the metadata store during startup.
- `BootstrapModule` initializes observability and also pushes protocol limits and worker settings into runtime state.

## 3. Network Entry and Command Dispatch

The TCP entrypoint is under `broker/src/server`, and command dispatch happens in `broker/src/server/handler.rs`.

The handler currently has a narrow set of responsibilities:

- validate protocol version
- record command metrics
- apply frame-level and request-level guardrails
- translate requests into `BrokerService` calls
- map service results back into wire responses

Main command groups implemented today:

- queue: `create_topic`, `produce`, `produce_batch`, `fetch`, `fetch_batch`, `commit_offset`
- direct: `send_direct`, `fetch_inbox`, `ack_direct`
- retry / delay: `nack`, `process_retry`, `schedule_delay`
- group: `join_consumer_group`, `heartbeat_consumer_group`, `rebalance_consumer_group`, `get_consumer_group_assignment`
- base: `handshake`, `ping`, `list_topics`

The handler also enforces runtime limits from `BootstrapModule::configure_runtime_limits()`:

- `max_payload_bytes`
- `max_batch_payload_bytes`
- `max_fetch_records`

Those checks are centralized instead of being duplicated across command implementations.

## 4. BrokerService as the Unified Business Facade

`BrokerService` in `broker/src/service.rs` is the main business facade of the broker.

It currently owns:

- topic creation and topic-policy validation
- queue publish / fetch / commit
- direct inbox send / fetch / ack
- retry / DLQ / delay scheduling behavior
- consumer-group membership and rebalance behavior
- runtime health and read-policy enforcement

Structurally, `BrokerService` does not talk to raw stores directly. It owns:

- `QueueRuntime`
- `DirectRuntime`
- mode-aware log and metadata-store wrappers
- an optional `BrokerRaftRuntime`

The goal is to keep business APIs stable while switching the underlying mode between standalone and raft.

## 5. Mode-Aware Read and Write Paths

One of the central design choices in the current broker is that the business layer does not care whether writes are local or replicated through Raft.

### 5.1 Write Path

The write path is abstracted in `broker/src/service/mode/appenders.rs` as `PartitionAppender`:

- `StandalonePartitionAppender`
- `RaftPartitionAppender`

In standalone mode, writes go directly to the local log and metadata store.

In raft mode, writes are wrapped as replicated commands, submitted through `BrokerRaftRuntime`, and then applied by the state machine inside `store`.

The current implementation also includes an important optimization:

- `ApplyResult::Appended` returns appended records directly
- broker does not need a follow-up read-back after apply succeeds

That keeps standalone and raft append / batch-append paths aligned around the same result shape.

### 5.2 Read Path

Read access is guarded through `BrokerService::ensure_read_allowed()`.

In raft mode, the current read policies are:

- `Local`
- `Leader`
- `Linearizable`

More specifically:

- `Leader` requires the local node to be the leader
- `Linearizable` adds a linearizable-read check on top of leader-only access

This changes read eligibility, not the external API shape.

## 6. Topic and Message Model

The current broker supports more than just “plain topic + payload”.

### 6.1 Queue

The queue path uses the topic / partition / offset model.

- `offset` is the record ID
- fetch request `offset` is the inclusive starting position
- response `next_offset` is the cursor for the next read
- `high_watermark` is the highest stably readable offset

### 6.2 Direct

The direct path still reuses the same storage foundation, but externally behaves like an inbox model.

- each recipient maps to a hidden inbox topic
- `send_direct` appends to the inbox
- `fetch_inbox` reads the inbox
- `ack_direct` commits the inbox cursor

### 6.3 Internal Topics

The broker currently creates and depends on several classes of internal topics:

- direct inbox topics
- retry topics
- delay topics

User-facing topic creation rejects those reserved names.

### 6.4 Keyed Records and Tombstones

The current implementation supports:

- record `key`
- tombstone semantics
- keyed batch produce
- fetch / fetch_batch returning keys

This is the basis for compacted topics.

### 6.5 Topic Policies

Topic-policy validation is currently centralized in `BrokerService::create_topic_with_policies()` and helpers. That includes:

- `max_message_bytes <= max_batch_bytes`
- valid retry backoff bounds
- internal topic names rejected for user topics
- `compaction_tombstone_retention_ms` requiring `compaction_enabled`
- `cleanup_policy` matching `compaction_enabled`
- valid dead-letter topic naming and no self-reference

## 7. Storage Maintenance and Background Workers

The broker process currently owns four background task families:

- retention cleanup
- compaction cleanup
- delay scheduler
- retry worker

### 7.1 Retention Cleanup

Retention cleanup periodically calls `SegmentLog::enforce_retention_once()`.

It currently handles:

- removing old segments according to retention policy
- controlling disk growth
- reporting last-run state plus cumulative removed-segment metrics

### 7.2 Compaction Cleanup

Compaction cleanup periodically calls `SegmentLog::compact_once()`.

Current behavior:

- only compact topics with compaction enabled
- keep the latest keyed record version
- support tombstone retention windows
- prevent compact-only topics from being touched by delete retention
- compact a sealed prefix per pass instead of fully rewriting an entire partition
- use `compaction_sealed_segment_batch` to cap how many sealed segments are processed in one iteration

There is also an intentional current boundary:

- the active segment is not compacted in place
- some tombstones are only physically cleaned after the active segment gets sealed

### 7.3 Delay Scheduler and Retry Worker

- the delay scheduler moves due delayed records back into their origin topics
- the retry worker scans retry topics and republishes records either to the origin topic or to DLQ

Both remain part of the broker process rather than separate services.

## 8. Admin, Health, and Observability

The broker also exposes Admin HTTP:

- `/healthz`
- `/metrics`
- UI pages
- group-management APIs

### 8.1 Current Admin Structure

The current `admin` module has been split by responsibility into:

- `admin/health.rs`
- `admin/dashboard.rs`
- `admin/topics.rs`
- `admin/groups.rs`
- `admin/api.rs`
- `admin/models.rs`

That split matters because the admin plane is no longer a trivial single-file handler.

### 8.2 Health Semantics

`/healthz` now reports more than runtime mode and raft leader status. It also includes background-worker readiness.

The current runtime-state coverage includes:

- retention cleanup last started / finished / success / error
- compaction cleanup last started / finished / success / error
- worker readiness state

Readiness is currently evaluated with a “last successful run exceeds `3 x interval`” rule. If a worker stops succeeding for too long, broker health moves from `ok` to `degraded`.

### 8.3 Metrics

The broker currently exposes at least these metric groups:

- TCP connection counters
- command totals and command error totals
- rebalance metrics
- raft client write retry / failure metrics
- retention cleanup runs / removed segments
- compaction cleanup runs / compacted partitions / removed records

Those metrics are consumed both by the dashboard and the Prometheus scrape endpoint.

## 9. Current Boundaries

These points should be treated as current implementation boundaries rather than future guarantees:

- admin is still primarily a single-node operational view, not a full cluster control plane
- compaction is usable, but it is still a sealed-prefix rebuild model rather than a fully incremental compaction engine
- the raft path is implemented, but cluster governance and cluster operations are still relatively early
- direct, retry, delay, and DLQ all share the same storage foundation, which improves consistency but also means worker health directly affects overall broker health

## 10. Files to Review When Updating This Document

When broker internals change, these files should be checked first:

- `broker/src/lib.rs`
- `broker/src/server/handler.rs`
- `broker/src/service.rs`
- `broker/src/service/mode/appenders.rs`
- `broker/src/di/bootstrap_module.rs`
- `broker/src/admin/health.rs`
- `broker/src/admin/*.rs`
- `store/src/segment/partition.rs`
- `store/src/command/*`
- `broker/src/raft/*`

The goal of this document is not to exhaustively cover every detail. Its goal is to let maintainers quickly answer three questions:

1. How is the broker process assembled?
2. How does a request flow into storage in the current implementation?
3. What does the current version support, and what does it still intentionally not support?

## 11. Durable Stream Core Status

The current broker has moved beyond a raw log API and now includes a more opinionated durable-stream data plane.

### 11.1 Consumer-Group Data Plane

Consumer groups are no longer only control-plane metadata.

The broker now has explicit group-aware fetch and commit commands that carry:

- `group`
- `member_id`
- `generation`

Those requests are validated against the current assignment snapshot before data is returned or offsets are advanced.

Current behavior:

- stale members are fenced after rebalance
- fetch and commit are rejected when the member does not own the target partition
- group progress is tracked as a group-level cursor rather than a member-local cursor

This is an important current design point: group membership now constrains the data path, not only rebalance state.

### 11.2 Producer Partitioning

Producer requests now use broker-side partition selection rather than relying only on client-provided partitions.

The current partitioning modes are:

- `explicit`
- `round_robin`
- `key_hash`

Current rules:

- `explicit` requires the request to provide a partition
- `round_robin` chooses a partition inside the broker
- `key_hash` requires keyed records and keeps the same key on a stable partition
- keyed batch produce requires a consistent non-empty key across the batch when `key_hash` is used

This was introduced as a breaking protocol change and should be treated as the current baseline for producer behavior.

### 11.3 Fetch Waiting Semantics

Fetch is no longer only a short-poll API.

The current fetch family supports:

- `max_wait_ms`
- `min_records`

That applies to:

- `fetch`
- `fetch_batch`
- `fetch_consumer_group`
- `fetch_consumer_group_batch`

Current behavior:

- if `max_wait_ms` is omitted or zero, the broker returns immediately
- otherwise the broker keeps polling until enough records are available or the timeout expires
- `min_records` defaults to `1`
- batch fetches evaluate readiness on the total records returned across the batch

This gives the broker a basic Kafka-style long-polling data path, while still keeping the implementation simple.

### 11.4 Stream Observability

The broker now exposes more durable-stream operational state than earlier versions.

Current coverage includes:

- topic backlog snapshots
- consumer-group lag snapshots
- low-cardinality stream summary metrics refreshed through `/metrics`
- producer partitioning totals and producer-side hot-partition summaries

The current dashboard and admin APIs are therefore no longer limited to node-health views. They also expose stream pressure and consumer progress.

### 11.5 Current Stream Boundaries

Even after these changes, the current durable-stream core still has clear boundaries:

- fetch waiting is based on `min_records`, not `min_bytes`
- there is no transactional producer model
- there is no idempotent producer state
- the admin plane still emphasizes a single-node operational view
- work-queue delivery semantics are still separate future work rather than part of the stream core
