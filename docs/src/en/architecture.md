# System Architecture

[中文版本](../zh-CN/architecture.md)

From a layered perspective, `ech0` can be understood as four layers:

1. Access layer: TCP connections, request decoding, response encoding
2. Service layer: `BrokerService` aggregating queue/direct/group capabilities
3. Runtime layer: `QueueRuntime` and `DirectRuntime`
4. Storage layer: `SegmentLog` and `RedbMetadataStore`

## Main Wiring Point

`broker/src/main.rs` wires the runtime:

- load config and initialize observability
- open segment log and metadata stores
- select `Standalone` or `Raft` mode from config
- create `BrokerService`
- start TCP server and Admin HTTP server
- start background jobs (such as retention cleanup)

## Runtime Composition

`BrokerService` composes two runtime paths:

- `queue`: topic/partition publish and fetch, offset commit, group assignment
- `direct`: send direct message, fetch inbox, ack inbox offsets

It also relies on mode-aware store wrappers so business APIs stay the same across modes:

- standalone: direct local storage reads/writes
- raft: consistency-aware write path and policy-constrained read path
