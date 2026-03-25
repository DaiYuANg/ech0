# Overview

[中文版本](../zh-CN/overview.md)

`ech0` is a Rust-based TCP message broker workspace with two messaging paths:

- queue (topic/partition + offset consumption)
- direct (point-to-point inbox)

The core crates are:

- `broker`: process entrypoint, network serving, admin plane, runtime wiring
- `store`: segment log storage, metadata, state machine, storage traits
- `protocol`: command IDs and request/response types
- `transport`: frame encoding/decoding
- `queue`: queue delivery and consumption runtime
- `direct`: direct messaging runtime

By default, `broker` exposes:

- a TCP endpoint for protocol requests
- an Admin HTTP endpoint for health, metrics, UI, and APIs

This book focuses on overall structure and design, not command-level API details.
