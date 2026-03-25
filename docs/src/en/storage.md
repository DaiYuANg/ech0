# Storage Design

[中文版本](../zh-CN/storage.md)

## Segment Log (`SegmentLog`)

`SegmentLog` stores message payload data using a partitioned multi-segment model:

- each topic partition has a segment set
- append-only writes with rolling by `segment_max_bytes`
- index files support faster location
- checkpoints track the next offset

This balances sequential-write throughput with partition isolation.

## Metadata Store (`RedbMetadataStore`)

Metadata is separated from payload logs and includes:

- topic configurations
- consumer offsets
- group members and assignments
- broker and consensus-related state

`redb` provides persistent metadata updates and fast recovery.

## Retention

Partition-level retention cleanup is based on max bytes:

- when total segment bytes exceed `retention_max_bytes`
- oldest segments are removed (at least one active segment remains)
- disk usage stays bounded

## Truncation

For recovery or consistency scenarios, a partition can be truncated at an offset:

- preserve records before truncate offset
- rebuild partition directory and segments
- update checkpoints and in-memory runtime state
