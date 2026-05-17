# ech0 Roadmap

This roadmap tracks the product and engine work needed to move ech0 from a working library-first MQ prototype toward a production-ready broker. It focuses on MQ functionality while treating multi-tenancy, auth, ACL, and quota as core broker semantics because they shape topic identity, resource ownership, and client behavior.

## Current Baseline

ech0 currently has a Go library API, a Cobra-based binary, a custom binary TCP protocol, segment-log storage, Dragonboat-backed cluster mode, memberlist discovery, request/reply, transactions, retry/delay basics, DLQ basics, consumer groups, embedded admin UI, metrics, Docker examples, and benchmark tooling.

Recent Docker verification covered a three-node memberlist/Dragonboat cluster with successful TCP produce/fetch/commit traffic.

## Phase 1: Multi-Tenant Governance

Multi-tenancy should land before the next large MQ feature work because it changes the identity model used by topics, consumer groups, transactions, request/reply inboxes, metrics, admin queries, auth decisions, ACL checks, and quota enforcement.

- Done: tenant and namespace model: tenant -> namespace -> topic.
- Done: default tenant and namespace for embedded and single-binary usage.
- Done: topic name scope isolation across tenants and namespaces.
- Done: consumer group scope isolation across tenants and namespaces.
- Done: transactional ID and producer ID scope isolation.
- Done: internal topic scope for retry, delay, DLQ, and request/reply inboxes.
- Done: tenant-aware metadata keys and storage paths through scoped broker names and segment paths.
- Partial: tenant-aware metrics for throughput, latency, storage, lag, and error rate; command, produce, fetch, quota, and admin stream metrics are identity scoped, while deeper storage/raft internals can still be enriched.
- Done: admin UI tenant and namespace views.
- Done: tenant-level defaults for retention, retry, delay, and DLQ policies.
- Done: auth identity model with principal, tenant, namespace, and optional client instance identity.
- Done: pluggable auth provider interface with an allow-all default for embedded usage.
- Partial: binary/server auth configuration through configx and DIX; config loading is DIX/configx based, while custom auth provider selection remains a library option.
- Done: TCP handshake authentication metadata.
- Done: admin API authentication metadata.
- Done: ACL resource model for cluster, tenant, namespace, topic, consumer group, transactional ID, and admin operations.
- Done: ACL actions for create, describe, produce, consume, commit, alter, delete, transact, and admin.
- Done: ACL enforcement in broker service APIs and TCP handlers.
- Done: quota model for tenant and principal scopes.
- Done: quotas for topic count, partition count, message size, storage usage, connection count, produce rate, consume rate, request rate, and in-flight TCP requests.
- Done: quota enforcement in hot paths with low overhead across create-topic, produce, fetch, request, TCP connection, storage, and TCP in-flight paths.
- Done: quota metrics and admin visibility.

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
- Done: assignment versioning rejects stale group fetches, offset commits, seeks, pauses, and transactional group offset commits.
- Done: embedded consumer group sessions expose revoke and assign callback semantics around rebalance.
- Done: group health views aggregate lag, members, assignments, rebalance explain, and recent rebalance history.
- Done: max poll interval and session timeout behavior.

## Phase 4: Topic And Message Lifecycle

- Done: retention by time with monotonic next offset and fetch log-start clamping.
- Done: retention by size with low watermark advancement.
- Done: compaction by key for segment-log storage.
- Done: partition high watermark, low watermark, and log start offset in store/runtime/protocol/admin views.
- Done: tombstone cleanup.
- Partial: topic-level policies for retention, compaction, retry, DLQ, priority, and ordering; retention, compaction, retry, delay, and DLQ policies exist, while priority and ordering policies remain open.
- Done: per-message TTL with delete-or-DLQ expiry policy.

## Phase 5: Retry, Delay, And DLQ

- Done: per-message delay and scheduled delivery.
- Done: DLQ query by offset, timestamp range, error reason, and header filters through broker and embedded APIs.
- Done: DLQ replay by DLQ offset with internal retry/DLQ headers stripped before republish.
- Cron-like scheduled message support.
- Partial: retry policy improvements: exponential backoff, max attempts, and retry topic isolation exist; jitter remains open.
- Bulk DLQ replay from query results by time range, header filter, and error reason.
- DLQ query indexes for original topic, partition, offset, error reason, and retry count.
- Poison message handling: skip, isolate, inspect, and replay.

## Phase 6: Routing And Bidirectional Messaging

- Done: embedded library APIs for direct inbox messaging and request/reply.
- Done: stable per-instance reply inbox with per-correlation consumer cursors for concurrent pending requests.
- Routing key support beyond topic and partition.
- Fanout topic or broadcast subject.
- Wildcard subject matching for lightweight pub/sub routing.
- Partial: request/reply timeout cleanup; deadlines are enforced and expired replies are rejected, while background inbox cleanup remains open.
- Reply inbox garbage collection.
- Multi-replier mode.
- First-response-wins request/reply mode.
- Done: correlation tracing across request, internal inbox, and reply.
- Done: ordered key guarantee for messages sharing the same key through stable key-hash partitioning.

## Phase 7: Protocol And Client Ecosystem

- Done: protocol capability negotiation during handshake.
- Done: negotiation constants for compression, batch support, transaction support, fetch wait behavior, direct, request/reply, consumer groups, retry/delay, idempotency, and schema headers.
- Done: non-Go client codec documentation.
- Go client split into producer, consumer, admin, and transactional producer packages.
- Done: standardized error codes.
- Done: schema hints through headers such as `content-type`, `schema-id`, and `encoding`.
- Done: keep zstd compression as the default and record compression metadata at message or batch boundaries.

## Phase 8: Storage And Recovery Reliability

- Done: self-describing segment frame header with body length for new writes.
- Done: startup rebuild of missing `.idx` files from self-describing segment frames.
- Done: offset index strengthening.
- Done: timestamp index.
- Optional key index for compaction and query support.
- Done: zero-copy or mmap read path experiments.
- Crash recovery fault tests.
- Done: graceful Raft snapshot-on-stop and replay correctness coverage for follower restart from existing data.
- Done: compaction correctness tests.
- Done: segment checksum and corruption detection.
- Log repair tooling.

## Phase 9: Cluster Behavior

- Partial: Dragonboat group management improvements; metadata and data shard groups exist, while membership management remains open.
- Node join and leave flows.
- Partition reassignment.
- Leader balance.
- Cluster metadata admin API.
- Gossip discovery stability tests.
- Rolling restart tests.
- Done: follower restart-from-existing-data test for a multi-node Dragonboat cluster.
- Cross-cluster mirror or replicator.

## Phase 10: Ecosystem Integrations

- Partial: continuous Kafka and NATS comparison benchmarks; benchmark tooling and same-host comparison docs exist, while continuous automation remains open.
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
