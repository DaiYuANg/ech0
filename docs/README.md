# ech0 Design Notes

This directory documents the current Go design of ech0. The docs describe the code that exists today and the constraints behind it; they are not a long-term roadmap.

## Documents

- [Architecture](architecture.md): package boundaries, runtime composition, and library-first design.
- [Wire Protocol](wire-protocol.md): TCP frame layout, binary body encoding, command registry, and compatibility rules.
- [Storage](storage.md): metadata store, segment log, Badger index, retention, compaction, and snapshots.
- [Request Reply](request-reply.md): request/reply semantics for address-agnostic services and instance-pinned replies.
- [Operations](operations.md): binary configuration, Raft mode, scheduled jobs, Admin UI, metrics, Docker, and release packaging.
- [Benchmarks](benchmarks.md): repeatable Go benchmarks and the end-to-end stress tool.

## Design Goals

- Keep the root `ech0` package small enough for embedded use.
- Hide operational dependencies behind the binary and advanced packages.
- Keep the wire protocol portable for non-Go clients without code generation.
- Preserve append-only message semantics through a segment log while using storx-backed metadata and indexing.
- Make clustered scheduling deterministic by running background jobs only on the Raft leader.
