# Operations and Observability

[中文版本](../zh-CN/operations.md)

## Service Endpoints

By default, two endpoints are exposed:

- broker TCP endpoint for protocol traffic
- admin HTTP endpoint for health, metrics, UI, and management APIs

## Configuration Loading

Configuration is loaded in layered order (later overrides earlier):

1. `./ech0.toml`
2. `./config/ech0.toml`
3. `./config/ech0.local.toml`
4. environment variables prefixed with `ECH0_`

Environment variables use `__` for nested keys, which is convenient for container deployments.

## Runtime Safeguards

The system includes guardrails such as:

- max frame body size
- max payload size
- max fetch record count
- retention cleanup for disk growth control

## Common Commands

- build: `cargo build --workspace`
- test: `cargo test --workspace`
- format check: `cargo fmt --all -- --check`
- lint: `cargo clippy --workspace --all-targets`

## Documentation Commands

- live preview: `mdbook serve docs`
- static build: `mdbook build docs`

The generated site is written to `docs/book`.
