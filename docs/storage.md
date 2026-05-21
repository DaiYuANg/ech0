# Storage

ech0 separates broker metadata from message storage. Broker runtime is Dragonboat-first for metadata: a single node runs a single-replica Dragonboat metadata group and writes messages directly to local segment logs, while a multi-node cluster also enables Dragonboat data shard groups. Dragonboat snapshots provide durable recovery for business metadata and clustered state machine data. Message bytes live in append-only segment files with shard-local binary index files. The message hot path does not depend on Badger or bbolt.

## Storage Layers

| Layer | Implementation | Data |
| --- | --- | --- |
| Metadata | Dragonboat state machine | Topic configs, consumer offsets, consumer group members, assignments, broker state. |
| Message manifest | `topics.json` | Topic configs needed by the local segment log. |
| Message index | `*.idx` files plus in-memory indexes | Per-record segment pointers, computed next offsets, timestamp lookup state, and on-demand key lookup state. |
| Message bytes | Segment files | zstd-compressed single-record or batch frames containing key, headers, attributes, timestamp, and payload. |

This gives the broker ordered append/read behavior without making a KV store hold message payloads or record pointers.

## Message Append Path

Appending messages follows this path:

1. Load the topic config for the target partition.
2. Validate payload limits.
3. Load the next offset from the in-memory partition index.
4. Build a `store.Record`.
5. Queue the request into the partition append pipeline, which drains immediately available work for the same partition.
6. Encode the drained records as one segment batch frame per target segment.
7. Append the frame to the active segment file through a cached segment writer.
8. Append binary `segmentRecordPointer` entries through a cached `.idx` writer.
9. Schedule an asynchronous group sync for the dirty segment and index files.
10. Advance the in-memory next offset.

The pointer records topic, partition, offset, segment ID, byte position, byte length, timestamp, and attributes. Records from the same batch frame can share the same position and length; reads decode that frame once and select records by offset. `segment_read_mode = "mmap"` is an experimental read path that maps sealed segment files and decodes directly from the mapped byte slice. Active append segments still use positional reads.

The key index is intentionally runtime-owned instead of being added to the `.idx` file format. Appends update it once a partition has built the index, and after restart the first key lookup or compaction pass rebuilds it by scanning visible records. This keeps the durable index small while still giving compaction and admin/query code a latest-record-by-key view.

## Segment Frame

Each segment frame has:

| Field | Description |
| --- | --- |
| magic | `ECZ1` (`0x45435a31`) for zstd-compressed single-record frames, `EBZ1` (`0x45425a31`) for zstd-compressed batch frames, `ECH1` (`0x45434831`) for raw single-record frames, or `ECB1` (`0x45434231`) for raw batch frames. Legacy `ECH0` / `ECZ0` / `ECB0` / `ECBZ` frames are still accepted when an index entry provides the exact length. |
| checksum | CRC32 of the stored frame body. |
| body_len | Stored frame body byte length. This makes new frames self-describing during recovery. |
| body | zstd-compressed or raw record body for single-record frames; zstd-compressed or raw count-prefixed record bodies for batch frames. |

The record body stores offset, timestamp, attributes, key, headers, and payload. Length-prefixed byte fields use an ASCII decimal length followed by `:`, then raw bytes.

New writes use zstd compression by default and include the body length in the frame header. The checksum protects against partial or corrupted frame reads. Startup loads existing `.idx` files first, then rebuilds any missing segment index from self-describing segment frames and writes the rebuilt `.idx` next to the `.seg`. Legacy unsized frames still require an existing `.idx` because they cannot be safely scanned without an external length. Clean shutdown flushes pending asynchronous group sync work before segment and index files are closed.

Crash recovery is intentionally fail-closed for ambiguous storage faults. If a segment is truncated while its index is missing, rebuild stops with a codec error instead of silently dropping or guessing bytes. If an existing `.idx` is truncated, startup also stops rather than replaying a potentially stale segment file that might reintroduce compacted records.

Offline repair is available through the library API `store.RepairStorxLog` and the binary command `ech0 repair segments`. The safe default rebuilds missing indexes only. Existing corrupt indexes require an explicit rebuild option, and the old file is backed up before a replacement is written.

## Metadata Store

The metadata store owns cluster and broker metadata:

- Topic configurations.
- Consumer offsets.
- Consumer group members.
- Consumer group assignments.
- Broker state.

It also implements `store.Snapshotter`, so Raft snapshots can persist and restore metadata.

## Metadata Persistence

The broker does not maintain a separate runtime metadata database. Dragonboat owns raft log files, raft snapshots, membership state, and recovery under the configured Dragonboat directory. The broker state machine exposes `Snapshot` and `Restore`; Dragonboat decides when those snapshots are created and replayed.

Message indexes are now owned by the segment log itself. Startup loads `.idx` files into memory, rebuilds missing indexes for self-describing segment frames, reads use binary search over the partition pointer slice, and retention/compaction append delete markers to the affected index files.

## Snapshot and Restore

Both metadata and log stores implement snapshot/restore contracts:

- Metadata snapshots copy topics, offsets, members, assignments, and broker state.
- Log snapshots copy topic configs, visible records, and computed next offsets.

Dragonboat runtime requires both metadata and message runtimes to implement `store.Snapshotter`; startup validates this before creating the Dragonboat runtime. In single-replica mode only the metadata group is started. In multi-node mode Dragonboat also starts data shard groups. Dragonboat owns the replicated Raft log and raft-side metadata under `data/dragonboat/<node_id>`.

## Retention

Retention cleanup is driven by topic config:

- `RetentionMS` removes old records by timestamp.
- `RetentionMaxBytes` removes older records when retained bytes exceed the configured limit.

Cleanup runs as a scheduled job when enabled. In cluster mode it runs only on the Raft leader.

## Ordering

Topic configs can declare an ordering policy:

- `key`: every produce request must carry a non-empty message key; batches must contain one key, and the broker selects the partition with stable key hashing.
- `routing_key`: every produce request must carry one routing key, either as the produce partitioning routing key or as the `x-ech0-routing-key` header; batches must contain one routing key.
- `partition`: preserves append order within whichever partition the producer selects.

Key and routing-key policies are enforced inside the broker, so embedded and TCP clients get the same partitioning behavior even if they omit an explicit partitioning mode.

## Priority

Topic configs can enable a priority policy with a minimum, maximum, and default priority. Priority is stored as the `x-ech0-priority` message header using an ASCII decimal `u8`.

When a priority policy is enabled:

- A missing priority header is filled with the topic default before append.
- An invalid priority header is rejected.
- A priority outside the configured range is rejected.
- Fetch results from one contiguous offset window are ordered by priority descending, then by offset ascending for equal priorities.
- Embedded record acks use pending offset tracking, so acking a higher offset before a lower offset does not advance the committed cursor past the lower offset.

Segment append order remains offset-ordered. Priority affects delivery order inside the fetched batch, not the physical log order. Explicit offset commits still mean "commit this next offset"; callers that need per-record out-of-order processing should use record ack semantics instead of direct next-offset commits.

## Compaction

Compaction uses record keys:

- For a key, only the latest non-tombstone record should remain visible after compaction.
- Tombstones mark deletes.
- `CompactionTombstoneRetentionMS` controls how long tombstones remain before cleanup.

Compaction is a scheduled job and is leader-gated by Dragonboat leadership. Per-shard scheduler ownership is still a follow-up now that data shards have their own Raft groups.

## Backend Decision

Message storage is fixed to the hybrid model:

- Dragonboat state machines as the source of truth for metadata and, in multi-node mode, replicated shard state.
- Segment files for actual message bytes.
- Segment `.idx` files for local message offset indexes.

There is no optional bbolt or Badger message-log backend. This keeps the message-storage model clear and removes KV value-log GC from the message hot path.
