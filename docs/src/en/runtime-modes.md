# Runtime Modes and Consistency

[中文版本](../zh-CN/runtime-modes.md)

`ech0` supports two runtime modes:

- `Standalone`: local reads/writes on a single node
- `Raft`: replicated consistency path across nodes

## Mode-Aware Access

Business APIs are kept stable through mode-aware store wrappers:

- mode-transparent interface for upper layers
- write path can switch between local append and consensus submission
- read path is constrained by configured read policy

## Read Policies

In Raft mode, read semantics can be configured as:

- `Local`: allow local reads for lower latency
- `Leader`: require reads on leader node
- `Linearizable`: enforce linearizable reads on leader

Stronger consistency usually implies higher read cost.

## Consumer Group Assignment

Consumer groups support:

- `round_robin` and `range` assignment strategies
- optional sticky behavior to reduce partition movement
- rebalance metrics for movement and churn visibility

## Offset / Watermark Semantics (Normative)

To prevent semantic drift when replication, leader/follower behavior, and read policies evolve, `ech0` uses the following unified definitions.

### 1) `offset`

- `offset` is the **unique record sequence number inside a `topic-partition`** (starting from `0`, monotonic, never reused)
- `offset` identifies "this record", not "the next position to read"

### 2) Fetch request `offset`

- The Fetch request field `offset` is the **inclusive start position**
- It means "try to return visible records from this offset"
- Semantically, it is the consumer's current next offset

### 3) Fetch response `next_offset`

- `next_offset` is the **offset the client should send in the next Fetch**
- Rule:
  - if records are returned: `next_offset = last_returned_record.offset + 1`
  - if no records are returned: `next_offset = request.offset`
- Consumer offset commits should store `next_offset`, not "last consumed offset"

### 4) `high_watermark`

- `high_watermark` is the **largest committed and visible record offset (inclusive)**
- It is the stable read upper bound
- It may be `None` when no visible records exist

### 5) Cross-mode consistency requirement

Standalone and Raft must expose the same API semantics; only the internal source differs:

- `Standalone`: `committed == appended`, so `high_watermark == last_appended_offset`
- `Raft`: `high_watermark` comes from replication commit progress (committed and visible), not merely local append tail

In other words:

- **For clients, `offset/next_offset/high_watermark` semantics are identical across modes**
- **For implementation, only the watermark derivation path differs**

### 6) Why this is P0

Without these definitions, the system quickly becomes ambiguous:

- committing "last consumed" vs "next to consume" causes duplicates or skips
- leader/follower divergence is hard to diagnose (replication lag vs cursor bugs)
- batch fetch, rebalancing, and retry/delay features cannot share one cursor model

This spec is the baseline for replication, read-policy behavior, and SDK semantics.
