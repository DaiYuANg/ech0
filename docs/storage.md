# Storage

ech0 separates broker metadata from message storage. Metadata uses bbolt through `arcgolabs/storx/bboltx`; message indexing uses Badger through `arcgolabs/storx/badgerx`; message bytes live in append-only segment files.

## Storage Layers

| Layer | Implementation | Data |
| --- | --- | --- |
| Metadata | `StorxMetadataStore` over bbolt | Topic configs, consumer offsets, consumer group members, assignments, broker state. |
| Message index | `StorxLogStore` Badger index | Topic existence, per-record segment pointers, next offsets. |
| Message bytes | Segment files | zstd-compressed record frames containing key, headers, attributes, timestamp, and payload. |

This gives the broker ordered append/read behavior without making bbolt or Badger store large message payloads directly.

## Message Append Path

Appending a message follows this path:

1. Load the topic config for the target partition.
2. Validate payload limits.
3. Load the next offset from Badger.
4. Build a `store.Record`.
5. Encode the record as a segment frame.
6. Append the frame to the active segment file.
7. Store a `segmentRecordPointer` in Badger.
8. Advance the next offset.

The Badger pointer records topic, partition, offset, segment ID, byte position, byte length, timestamp, and attributes. Reads use the pointer to seek into the segment file and decode only the requested records.

## Segment Frame

Each segment frame has:

| Field | Description |
| --- | --- |
| magic | `ECZ0` (`0x45435a30`) for zstd-compressed frames, or legacy `ECH0` (`0x45434830`) for uncompressed frames. |
| checksum | CRC32 of the stored frame body. |
| body | zstd-compressed record body for `ECZ0`, raw record body for `ECH0`. |

The record body stores offset, timestamp, attributes, key, headers, and payload. Length-prefixed byte fields use an ASCII decimal length followed by `:`, then raw bytes.

New writes use zstd compression by default and reads continue to accept legacy uncompressed frames. The checksum protects against partial or corrupted frame reads. The Badger pointer provides the exact frame position and length.

## Metadata Store

`StorxMetadataStore` owns cluster and broker metadata:

- Topic configurations.
- Consumer offsets.
- Consumer group members.
- Consumer group assignments.
- Broker state.

It also implements `store.Snapshotter`, so Raft snapshots can persist and restore metadata.

## Storx Usage

The storage layer standardizes bbolt and Badger access through `arcgolabs/storx` subpackages:

- `bboltx.DB` and typed `bboltx.Bucket` are used for broker metadata and Raft log/stable storage.
- `badgerx.DB` and typed `badgerx.Namespace` are used for message indexes.
- `keycodec.String`, `keycodec.Bytes`, and `keycodec.Uint64BE` provide ordered, typed keys.
- `keycodec.Composite` stores typed topic/partition/offset index keys without hand-built Badger key strings.
- `codec.JSON` stores metadata and segment pointers; `codec.Bytes` stores Raft raw values.
- `SetMany`, `PutMany`, and `DeleteMany` are used for snapshot restore and index cleanup paths.
- `View`/`Update` transaction helpers are used where an operation must stay inside one storage transaction.
- `Page` is used by the Admin UI message browser for cursor pagination over the Badger record index.
- `observer` is wired into the binary runtime so completed storx operations feed broker storage metrics.
- `bboltx.ModelStore` plus a non-unique secondary index stores consumer-group members by group for indexed admin and rebalance queries.
- `RunValueLogGC` runs after retention or compaction removes Badger index entries.

Potential next steps are using `Iter` for long-running maintenance scans and evaluating additional ModelStore indexes only for admin-query metadata, not for the append/read message hot path.

## Snapshot and Restore

Both metadata and log stores implement snapshot/restore contracts:

- Metadata snapshots copy topics, offsets, members, assignments, and broker state.
- Log snapshots copy topic configs, segment pointers, next offsets, and segment file data.

Raft mode requires both stores to implement `store.Snapshotter`; startup validates this before creating the Raft runtime.

## Retention

Retention cleanup is driven by topic config:

- `RetentionMS` removes old records by timestamp.
- `RetentionMaxBytes` removes older records when retained bytes exceed the configured limit.

Cleanup runs as a scheduled job when enabled. In cluster mode it runs only on the Raft leader.

## Compaction

Compaction uses record keys:

- For a key, only the latest non-tombstone record should remain visible after compaction.
- Tombstones mark deletes.
- `CompactionTombstoneRetentionMS` controls how long tombstones remain before cleanup.

Compaction is a scheduled job and is leader-gated in Raft mode.

## Backend Decision

Message storage is fixed to the hybrid model:

- bbolt for metadata.
- Badger for message metadata and indexes.
- Segment files for actual message bytes.

There is no optional bbolt message-log backend. This keeps the operational and performance model clear.
