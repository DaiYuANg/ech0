# ech0

`ech0` is a Rust workspace for a TCP message broker with queue and direct-message flows.

## Workspace crates

- `broker`: broker runtime and TCP server entrypoint.
- `protocol`: command IDs and request/response payload types.
- `transport`: frame encoding/decoding.
- `store`: log and metadata persistence.
- `queue`: queue delivery runtime.
- `direct`: direct messaging runtime.

## Prerequisites

- Rust toolchain with Cargo (edition 2024 compatible).

## Quick start

1. Create a local config file from the example:

   - Linux/macOS:
     - `cp config/ech0.toml.example config/ech0.toml`
   - Windows PowerShell:
     - `Copy-Item config/ech0.toml.example config/ech0.toml`

2. Optional: force single-node local mode by adding `config/ech0.local.toml`:

   ```toml
   [raft]
   enabled = false
   ```

3. Start broker:

   - `cargo run -p broker-bin`

The broker listens on `127.0.0.1:9090` by default and stores runtime data under `./data`.
Admin HTTP server listens on `127.0.0.1:9091` by default:

- ops dashboard UI: `http://127.0.0.1:9091/ui`
- topics UI: `http://127.0.0.1:9091/ui/topics`
- group UI: `http://127.0.0.1:9091/ui/groups/{group}`
- health: `http://127.0.0.1:9091/healthz`
- metrics (Prometheus): `http://127.0.0.1:9091/metrics`
- swagger ui: `http://127.0.0.1:9091/swagger-ui`
- openapi json: `http://127.0.0.1:9091/api-docs/openapi.json`
- group members API: `http://127.0.0.1:9091/api/groups/{group}/members`
- group assignment API: `http://127.0.0.1:9091/api/groups/{group}/assignment`
- trigger rebalance API: `POST http://127.0.0.1:9091/api/groups/{group}/rebalance`
- rebalance explain API: `http://127.0.0.1:9091/api/groups/{group}/rebalance-explain`

## Useful commands

- Build all crates: `cargo build --workspace`
- Run tests: `cargo test --workspace`
- Run broker tests only: `cargo test -p broker`
- Format check: `cargo fmt --all -- --check`
- Clippy check: `cargo clippy --workspace --all-targets`
- Run queue smoke test client: `cargo run -p broker --example smoke_e2e`
- Run direct smoke test client: `cargo run -p broker --example smoke_direct`
- Run cross-platform automated smoke workflow: `cargo run -p broker --example smoke_all`
- Run smoke wrapper (PowerShell): `./scripts/smoke.ps1`
- Run smoke wrapper (Bash): `./scripts/smoke.sh`

## Embed broker as a library

`broker` now supports both binary and library usage.

- Binary entrypoint: `cargo run -p broker-bin`
- Library entrypoints:
  - `broker::run()`
  - `broker::run_with_config(app_config)`
  - `broker::run_with_config_and_shutdown(app_config, shutdown_future)`
  - `broker::BrokerBuilder` (chainable programmatic configuration)

Example:

```rust
use broker::AppConfig;

#[tokio::main]
async fn main() -> store::Result<()> {
  let app = AppConfig::load()
    .map_err(|err| store::StoreError::Codec(format!("failed to load config: {err}")))?;

  // Stop broker when your own signal/future resolves.
  let shutdown = async {
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
  };

  broker::run_with_config_and_shutdown(app, shutdown).await
}
```

Builder example:

```rust
#[tokio::main]
async fn main() -> store::Result<()> {
  broker::BrokerBuilder::from_env()
    .map_err(|err| store::StoreError::Codec(format!("failed to load config: {err}")))?
    .raft_enabled(false)
    .broker_bind_addr("127.0.0.1:19090")
    .admin_bind_addr("127.0.0.1:19091")
    .max_payload_bytes(2 * 1024 * 1024)
    .run()
    .await
}
```

## End-to-end smoke test

1. Start broker in terminal A:

   - `cargo run -p broker-bin`

2. Run queue smoke client in terminal B:

   - `cargo run -p broker --example smoke_e2e`

3. Run direct smoke client in terminal B (or C):

   - `cargo run -p broker --example smoke_direct`

4. Optional: if broker runs on another address:

   - Linux/macOS:
     - `BROKER_ADDR=127.0.0.1:9091 cargo run -p broker --example smoke_e2e`
     - `BROKER_ADDR=127.0.0.1:9091 cargo run -p broker --example smoke_direct`
   - Windows PowerShell:
     - `$env:BROKER_ADDR="127.0.0.1:9091"; cargo run -p broker --example smoke_e2e`
     - `$env:BROKER_ADDR="127.0.0.1:9091"; cargo run -p broker --example smoke_direct`

The queue smoke client verifies this request chain:

- handshake
- ping
- create_topic
- list_topics
- produce
- fetch
- commit_offset

The direct smoke client verifies this request chain:

- handshake
- send_direct
- fetch_inbox
- ack_direct

## One-command smoke workflow (cross-platform)

Run everything in one command:

- `cargo run -p broker --example smoke_all`

What it does:

- creates `config/ech0.toml` from example if missing
- creates `config/ech0.local.toml` with raft disabled if missing
- starts broker and waits for TCP readiness
- runs `smoke_e2e` and `smoke_direct`
- stops broker automatically

Environment variables:

- `BROKER_ADDR` (default: `127.0.0.1:9090`)
- `SMOKE_STARTUP_TIMEOUT_SECS` (default: `25`)

Wrapper scripts (optional):

- PowerShell:
  - `./scripts/smoke.ps1 -BrokerAddr "127.0.0.1:9091" -StartupTimeoutSeconds 40`
- Bash:
  - `./scripts/smoke.sh --broker-addr 127.0.0.1:9091 --startup-timeout-seconds 40`

## Docker Examples

`Dockerfile` is provided at repo root for the `broker` binary.

- Optional env template for compose:
  - template: `deploy/docker/.env.example`
  - PowerShell: `Copy-Item deploy/docker/.env.example deploy/docker/.env`
  - bash: `cp deploy/docker/.env.example deploy/docker/.env`

- Build image manually:
  - `docker build -t ech0-broker:local .`
- Build with explicit runtime base:
  - Debian: `docker build --build-arg BASE_IMAGE=debian -t ech0-broker:debian .`
  - Alpine: `docker build --build-arg BASE_IMAGE=alpine -t ech0-broker:alpine .`

Single-node compose:

- File: `deploy/docker/docker-compose.single.yml`
- Start:
  - `docker compose --env-file deploy/docker/.env -f deploy/docker/docker-compose.single.yml up --build`
- Select runtime base image:
  - Debian (default): `ECH0_BASE_IMAGE=debian docker compose --env-file deploy/docker/.env -f deploy/docker/docker-compose.single.yml up --build`
  - Alpine: `ECH0_BASE_IMAGE=alpine docker compose --env-file deploy/docker/.env -f deploy/docker/docker-compose.single.yml up --build`
- Endpoints:
  - broker tcp: `127.0.0.1:${ECH0_SINGLE_BROKER_PORT:-9090}`
  - admin http: `127.0.0.1:${ECH0_SINGLE_ADMIN_PORT:-9091}`
  - swagger ui: `http://127.0.0.1:${ECH0_SINGLE_ADMIN_PORT:-9091}/swagger-ui`

Three-node raft cluster compose:

- File: `deploy/docker/docker-compose.cluster.yml`
- Start:
  - `docker compose --env-file deploy/docker/.env -f deploy/docker/docker-compose.cluster.yml up --build`
- Select runtime base image:
  - Debian (default): `ECH0_BASE_IMAGE=debian docker compose --env-file deploy/docker/.env -f deploy/docker/docker-compose.cluster.yml up --build`
  - Alpine: `ECH0_BASE_IMAGE=alpine docker compose --env-file deploy/docker/.env -f deploy/docker/docker-compose.cluster.yml up --build`
- Host ports:
  - node1: broker/admin/raft = `${ECH0_NODE1_BROKER_PORT:-19090}` / `${ECH0_NODE1_ADMIN_PORT:-19091}` / `${ECH0_NODE1_RAFT_PORT:-13210}`
  - node2: broker/admin/raft = `${ECH0_NODE2_BROKER_PORT:-29090}` / `${ECH0_NODE2_ADMIN_PORT:-29091}` / `${ECH0_NODE2_RAFT_PORT:-23210}`
  - node3: broker/admin/raft = `${ECH0_NODE3_BROKER_PORT:-39090}` / `${ECH0_NODE3_ADMIN_PORT:-39091}` / `${ECH0_NODE3_RAFT_PORT:-33210}`

Compose-specific config samples:

- single node: `deploy/docker/configs/single/ech0.toml`
- cluster nodes:
  - `deploy/docker/configs/cluster/node1.toml`
  - `deploy/docker/configs/cluster/node2.toml`
  - `deploy/docker/configs/cluster/node3.toml`

## Configuration

Config is loaded in this order (later wins):

1. `./ech0.toml`
2. `./config/ech0.toml`
3. `./config/ech0.local.toml`
4. environment variables with `ECH0_` prefix

Environment variables use `__` for nesting, for example:

- `ECH0_BROKER__BIND_ADDR=127.0.0.1:9091`
- `ECH0_BROKER__MAX_FRAME_BODY_BYTES=4194304`
- `ECH0_BROKER__MAX_PAYLOAD_BYTES=1048576`
- `ECH0_BROKER__MAX_FETCH_RECORDS=1000`
- `ECH0_BROKER__GROUP_ASSIGNMENT_STRATEGY=round_robin` (or `range`)
- `ECH0_BROKER__GROUP_STICKY_ASSIGNMENTS=true`
- `ECH0_ADMIN__BIND_ADDR=127.0.0.1:9191`
- `ECH0_ADMIN__ENABLED=true`
- `ECH0_STORAGE__RETENTION_CLEANUP_ENABLED=true`
- `ECH0_STORAGE__RETENTION_CLEANUP_INTERVAL_SECS=30`
- `ECH0_RAFT__ENABLED=false`

`.env` is also loaded if present.

## Runtime safeguards

- `broker.max_frame_body_bytes` rejects oversized protocol frame bodies (`frame_too_large`).
- `broker.max_payload_bytes` rejects oversized `produce` and `send_direct` payloads (`payload_too_large`).
- `broker.max_fetch_records` caps `fetch` and `fetch_inbox` requests (`fetch_limit_exceeded`).
- `broker.group_assignment_strategy` controls consumer-group partition allocation (`round_robin` or `range`).
- `broker.group_sticky_assignments` reuses prior owners when valid while keeping partition load balanced.
- Rebalance observability metrics include `broker_rebalances_total`, `broker_rebalances_total_by_strategy`, and `broker_rebalance_moved_partitions_total`.
- `storage.retention_cleanup_enabled` runs background log-segment retention cleanup.
- topic-level `retention_max_bytes` controls per-partition retained segment bytes.

## First files to read

- `Cargo.toml`
- `config/ech0.toml.example`
- `broker/src/main.rs`
- `broker/src/server/handler.rs`
- `broker/src/service.rs`
- `protocol/src/lib.rs`
- `transport/src/lib.rs`

## Troubleshooting

- If startup fails with config errors, verify `config/ech0.toml` exists and TOML syntax is valid.
- If port binding fails, change `broker.bind_addr` in config or set `ECH0_BROKER__BIND_ADDR`.
- If admin HTTP port conflicts, change `admin.bind_addr` in config or set `ECH0_ADMIN__BIND_ADDR`.
- If local iteration is the goal, disable raft in `config/ech0.local.toml` to reduce moving parts.
