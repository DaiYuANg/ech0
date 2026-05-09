# Wire Protocol

ech0 uses a custom binary TCP protocol. The project has not shipped a stable public release yet, so breaking protocol changes are still folded into protocol version `1` instead of carrying a compatibility layer.

## Frame

The transport frame carries:

- protocol version: currently `1`
- command id: `uint16`
- flags
- body length
- body bytes

Bodies are command-specific binary payloads encoded by the `protocol` package.

## Normal Message Path

Normal producers and consumers keep the smallest command surface:

- `Produce`
- `ProduceBatch`
- `ProduceBatches`
- `Fetch`
- `FetchBatch`
- `CommitOffset`
- consumer-group fetch and commit commands

Fetch commands now carry an isolation field:

- `read_uncommitted`: default behavior; returns normal and transactional records
- `read_committed`: hides open transactions, skips aborted transactions, and only exposes committed transactional records

Fetched records may include optional transaction metadata:

- `tx_id`
- `producer_id`
- `producer_epoch`
- `sequence`
- `control_type`: empty, `commit`, or `abort`

Non-transactional records omit this metadata.

## Transaction Commands

Transaction commands are active in protocol v1:

- `TxBegin`
- `TxPublish`
- `TxPublishBatch`
- `TxCommitOffset`
- `TxCommit`
- `TxAbort`

Each transactional write command carries a `TransactionIdentity`:

- `tx_id`
- `producer_id`
- `producer_epoch`

`TxPublish` carries a per-record `sequence`. `TxPublishBatch` carries `base_sequence`, with record sequence derived from batch order. The broker currently enforces strict monotonic sequencing for a transaction.

`TxCommitOffset` supports both plain consumer offsets and consumer-group offsets so the same protocol can cover consume-process-produce:

```text
TxBegin
TxPublish / TxPublishBatch
TxCommitOffset
TxCommit
```

The broker stores transaction state in metadata snapshots, writes transactional record metadata into the segment log, and applies `read_committed` filtering during fetch. `TxCommitOffset` stages consumer offsets inside the transaction; `TxCommit` commits those offsets together with the transaction status. `TxAbort` leaves written records in the log but hides them from `read_committed` consumers.

Current implementation limits:

- transaction commands route through the metadata Raft group first, prioritizing correctness over the multi-data-group fast path
- idempotent retry dedupe for duplicate publish sequences is not implemented yet
- explicit segment commit/abort control marker records are still reserved by the protocol but not emitted by the broker
