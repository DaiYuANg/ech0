# ech0 Roadmap

This roadmap tracks the product and engine work needed to move ech0 from a working library-first MQ prototype toward a production-ready broker. It intentionally focuses on MQ functionality and excludes security and ops items such as auth, TLS, ACLs, and quotas unless they directly shape the feature model.

## Current Baseline

ech0 currently has a Go library API, a Cobra-based binary, a custom binary TCP protocol, segment-log storage, Dragonboat-backed cluster mode, memberlist discovery, request/reply, transactions, retry/delay basics, DLQ basics, consumer groups, embedded admin UI, metrics, Docker examples, and benchmark tooling.

Recent Docker verification covered a three-node memberlist/Dragonboat cluster with successful TCP produce/fetch/commit traffic.

## Phase 1: MQ Semantics

- Idempotent producer with producer ID, epoch, per-topic-partition sequence, and broker-side dedupe window.
- Transactional offset commit for consume-transform-produce workflows.
- Producer fencing and transaction timeout cleanup.
- Transaction recovery tests across restart and cluster failover.
- Seek by offset and seek by timestamp.
- Pause and resume partition consumption.
- Offset commit metadata.
- Replay by offset, timestamp, and cursor.
- Stronger `read_committed` correctness tests.

## Phase 2: Consumer Group Maturity

- Static membership to avoid unnecessary full rebalances during short restarts.
- Cooperative-sticky rebalance to reduce partition movement.
- Assignment versioning to reject stale commits and stale fetches.
- Revoke and assign callback semantics for clients.
- Group health views for lag, members, assignments, and rebalance history.
- Max poll interval and session timeout behavior.

## Phase 3: Topic And Message Lifecycle

- Retention by time.
- Retention by size.
- Compaction by key.
- Tombstone cleanup.
- Topic-level policies for retention, compaction, retry, DLQ, priority, and ordering.
- Partition high watermark, low watermark, and log start offset.
- Per-message TTL with expire-or-DLQ policy.

## Phase 4: Retry, Delay, And DLQ

- Per-message delay and scheduled delivery.
- Cron-like scheduled message support.
- Retry policy improvements: exponential backoff, jitter, max attempts, and retry topic isolation.
- DLQ replay by offset, time range, header filter, and error reason.
- DLQ query indexes for original topic, partition, offset, error reason, and retry count.
- Poison message handling: skip, isolate, inspect, and replay.

## Phase 5: Multi-Tenant Isolation

- Tenant and namespace model: tenant -> namespace -> topic.
- Topic name scope isolation across tenants and namespaces.
- Tenant-level limits for topic count, partition count, message size, storage usage, and connection count.
- Tenant-level retention defaults and overrides.
- Tenant metrics for throughput, latency, storage, lag, and error rate.
- Admin UI tenant views.
- Future integration points for ACL and auth without baking those concerns into the initial model.

## Phase 6: Routing And Bidirectional Messaging

- Routing key support beyond topic and partition.
- Fanout topic or broadcast subject.
- Wildcard subject matching for lightweight pub/sub routing.
- Request/reply timeout cleanup.
- Reply inbox garbage collection.
- Multi-replier mode.
- First-response-wins request/reply mode.
- Correlation tracing across request, internal inbox, and reply.
- Ordered key guarantee for messages sharing the same key.

## Phase 7: Protocol And Client Ecosystem

- Protocol capability negotiation during handshake.
- Negotiation for compression, batch support, transaction support, and fetch wait behavior.
- Non-Go client codec documentation.
- Go client split into producer, consumer, admin, and transactional producer packages.
- Standardized error codes.
- Schema hints through headers such as `content-type`, `schema-id`, and `encoding`.
- Keep zstd compression as the default and record compression metadata at message or batch boundaries.

## Phase 8: Storage And Recovery Reliability

- Offset index strengthening.
- Timestamp index.
- Optional key index for compaction and query support.
- Zero-copy or mmap read path experiments.
- Crash recovery fault tests.
- Snapshot and replay correctness tests.
- Compaction correctness tests.
- Segment checksum and corruption detection.
- Log repair tooling.

## Phase 9: Cluster Behavior

- Dragonboat group management improvements.
- Node join and leave flows.
- Partition reassignment.
- Leader balance.
- Cluster metadata admin API.
- Gossip discovery stability tests.
- Rolling restart tests.
- Restart-from-existing-data tests.
- Cross-cluster mirror or replicator.

## Phase 10: Ecosystem Integrations

- Continuous Kafka and NATS comparison benchmarks.
- HTTP gateway.
- Webhook sink.
- Database outbox connector.
- File and S3 sinks.
- OpenTelemetry tracing.
- Admin UI operation panels.

## Suggested Execution Order

1. Idempotent producer.
2. Transactional offset commit.
3. Seek and replay.
4. Multi-tenant namespace model.
5. Static membership and cooperative-sticky consumer group rebalance.
6. Retention, compaction, and partition watermarks.
7. DLQ replay and delay queue productization.
