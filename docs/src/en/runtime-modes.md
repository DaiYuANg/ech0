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
