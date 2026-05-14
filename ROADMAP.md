# ech0 Roadmap

This roadmap tracks the product and engine work needed to move ech0 from a working library-first MQ prototype toward a production-ready broker. It focuses on MQ functionality while treating multi-tenancy, auth, ACL, and quota as core broker semantics because they shape topic identity, resource ownership, and client behavior.

## Current Baseline

ech0 currently has a Go library API, a Cobra-based binary, a custom binary TCP protocol, segment-log storage, Dragonboat-backed cluster mode, memberlist discovery, request/reply, transactions, retry/delay basics, DLQ basics, consumer groups, embedded admin UI, metrics, Docker examples, and benchmark tooling.

Recent Docker verification covered a three-node memberlist/Dragonboat cluster with successful TCP produce/fetch/commit traffic.

## Phase 1: Multi-Tenant Governance

Multi-tenancy should land before the next large MQ feature work because it changes the identity model used by topics, consumer groups, transactions, request/reply inboxes, metrics, admin queries, auth decisions, ACL checks, and quota enforcement.

- Tenant and namespace model: tenant -> namespace -> topic.
- Default tenant and namespace for embedded and single-binary usage.
- Topic name scope isolation across tenants and namespaces.
- Consumer group scope isolation across tenants and namespaces.
- Transactional ID and producer ID scope isolation.
- Internal topic scope for retry, delay, DLQ, and request/reply inboxes.
- Tenant-aware metadata keys and storage paths.
- Tenant-aware metrics for throughput, latency, storage, lag, and error rate.
- Admin UI tenant and namespace views.
- Tenant-level defaults for retention, retry, delay, and DLQ policies.
- Auth identity model with principal, tenant, namespace, and optional client instance identity.
- Pluggable auth provider interface with an allow-all default for embedded usage.
- Binary/server auth configuration through configx and DIX.
- TCP handshake authentication metadata.
- Admin API authentication metadata.
- ACL resource model for cluster, tenant, namespace, topic, consumer group, transactional ID, and admin operations.
- ACL actions for create, describe, produce, consume, commit, alter, delete, transact, and admin.
- ACL enforcement in broker service APIs and TCP handlers.
- Quota model for tenant and principal scopes.
- Quotas for topic count, partition count, message size, storage usage, connection count, produce rate, consume rate, request rate, and in-flight requests.
- Quota enforcement in hot paths with low overhead.
- Quota metrics and admin visibility.

The first implementation cut should keep the public mental model small: existing APIs continue to work by using the default tenant, namespace, allow-all auth, and unlimited quotas, while advanced callers can opt into explicit tenant, namespace, principal, ACL, and quota configuration.

## Phase 2: MQ Semantics

- Done: idempotent producer with producer ID, epoch, per-topic-partition sequence, broker-side dedupe window, TCP/raft protocol fields, and default embedded producer IDs.
- Done: transactional offset commit for consume-transform-produce workflows.
- Done: producer fencing and transaction timeout cleanup.
- Done: transaction recovery tests across restart and cluster failover.
- Done: seek by offset and seek by timestamp for consumers and consumer groups.
- Done: pause and resume partition consumption for consumers and consumer groups.
- Done: offset commit metadata for direct consumers, consumer groups, transactions, binary protocol, and persisted snapshots.
- Done: replay by offset, timestamp, and cursor without advancing consumer offsets.
- Done: stronger `read_committed` correctness tests for open transaction boundaries, control markers, and aborted batches.

## Phase 3: Consumer Group Maturity

- Done: static membership keeps assignment generation stable when the same member rejoins without assignment changes.
- Done: cooperative-sticky rebalance balances new or remaining members while moving the minimum eligible partitions.
- Assignment versioning to reject stale commits and stale fetches.
- Revoke and assign callback semantics for clients.
- Group health views for lag, members, assignments, and rebalance history.
- Max poll interval and session timeout behavior.

## Phase 4: Topic And Message Lifecycle

- Retention by time.
- Retention by size.
- Compaction by key.
- Tombstone cleanup.
- Topic-level policies for retention, compaction, retry, DLQ, priority, and ordering.
- Partition high watermark, low watermark, and log start offset.
- Per-message TTL with expire-or-DLQ policy.

## Phase 5: Retry, Delay, And DLQ

- Per-message delay and scheduled delivery.
- Cron-like scheduled message support.
- Retry policy improvements: exponential backoff, jitter, max attempts, and retry topic isolation.
- DLQ replay by offset, time range, header filter, and error reason.
- DLQ query indexes for original topic, partition, offset, error reason, and retry count.
- Poison message handling: skip, isolate, inspect, and replay.

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

1. Multi-tenant namespace model.
2. Auth identity and pluggable auth provider.
3. ACL resource model and enforcement.
4. Quota model and enforcement.
5. Tenant-aware topic, group, transaction, and request/reply identities.
6. Tenant-aware metrics and admin views.
7. Idempotent producer.
8. Transactional offset commit.
9. Producer fencing and transaction timeout cleanup.
10. Seek and replay.
11. Static membership and cooperative-sticky consumer group rebalance.
12. Retention, compaction, and partition watermarks.
13. DLQ replay and delay queue productization.
