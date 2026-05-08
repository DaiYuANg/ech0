# Operations

The `ech0` binary packages the broker runtime, TCP protocol server, Admin UI, metrics, configuration loading, scheduling, and optional Raft cluster mode into one executable.

## Configuration

Binary configuration is loaded with `arcgolabs/configx`.

Load order:

1. Typed defaults from `broker.DefaultConfig`.
2. Config files.
3. Environment variables with prefix `ECH0`.
4. Cobra/pflag command flags.

Environment variables use `__` as the nesting separator. For example:

```text
ECH0_BROKER__BIND_ADDR=0.0.0.0:9092
ECH0_ADMIN__BIND_ADDR=0.0.0.0:8080
ECH0_RAFT__ENABLED=true
```

The embedded root package does not expose this full config surface. Embedded users should use `ech0.Options`.

## Standalone Mode

Standalone mode runs one broker process with local stores:

```sh
ech0 --config config/ech0.toml.example --raft=false
```

Writes apply directly to the local broker service. Scheduled jobs run in the local process.

## Raft Mode

Raft mode coordinates mutating broker commands through HashiCorp Raft:

- Topic creation.
- Produces and batch produces.
- Offset commits.
- Direct sends and direct acks.
- Consumer group membership, heartbeats, and rebalances.

Raft stores its own logs and snapshots under the configured data directory. Broker metadata and message log stores must implement `store.Snapshotter`; startup validates this requirement.

`raft.bind_addr` is the local listen address and may use `0.0.0.0` in containers. The current node's matching entry in `raft.cluster` is used as the Raft advertised address, so cluster entries should use routable peer addresses such as Docker service names.

`raft.commit_timeout_ms` controls HashiCorp Raft's commit flush interval. The default is `50`. Lower values can reduce low-throughput write latency but may reduce Raft's ability to batch append entries efficiently. In Docker examples it can be overridden with `ECH0_RAFT_COMMIT_TIMEOUT_MS`.

When a node is not leader, mutating commands return a not-leader error. Scheduled jobs use a gocron distributed elector and only run on the current leader.

## Scheduled Jobs

The scheduled runtime is built on `go-co-op/gocron` and includes:

- Delay scheduler.
- Retry worker.
- Retention cleanup.
- Compaction cleanup.

All jobs use singleton mode. In standalone mode, the local process is allowed to run them. In Raft mode, `raftElector` permits execution only when the node is the current leader.

## Admin UI and API

Admin is served from the broker package on Fiber. It provides:

- Health endpoints.
- Metrics endpoint.
- OpenAPI document endpoint.
- Topic and message views.
- Embedded HTML templates styled with Tailwind CDN.

Admin and OpenAPI use `arcgolabs/httpx` for the HTTP surface while the default server is Fiber.

## Metrics

Metrics are exposed through the admin server and are wired through the broker metrics package. The project uses the arcgolabs observability stack where it fits the runtime surface.

Current metric coverage includes broker/runtime counters, stream gauges, cleanup counters, produce counters, and storx storage operation metrics. Storage metrics are emitted from `storx/observer` and include operation count, error count, and duration labels for engine, target type, target, operation, and status.

Hot-path performance metrics include broker command duration, raft proposal duration, raft store duration, FSM duration, fetch duration, store append duration, and store read duration. They are intended for short benchmark and load-test runs where stage-level labels are useful for locating bottlenecks.

## Docker

Docker examples live in `deploy/docker`:

- `docker-compose.single.yml` builds a local single-node image.
- `docker-compose.cluster.yml` builds a local multi-node cluster.
- `docker-compose.single.release.yml` uses the release image.
- `docker-compose.cluster.release.yml` uses the release image.

The release Dockerfile is multi-stage. It installs UPX in the build stage, compresses the binary, and copies only the compressed executable into the runtime stage.

## Release Packaging

GoReleaser drives release artifacts:

- Archives for Linux, macOS, and Windows.
- `.deb` and `.rpm` packages.
- systemd unit and default config.
- Multi-platform Docker images.

Packaging assets live under `packaging/`.

Full local release verification expects `upx` and Docker to be available. Without Docker, `goreleaser release --snapshot --clean --skip=docker` verifies archives and Linux packages.

## Operational Boundaries

The binary owns operational concerns:

- `configx` config loading.
- file/stdout logging setup.
- Admin UI and OpenAPI.
- Raft networking and state.
- Release packaging and container defaults.

The root `ech0` library should not expose these concerns unless they become essential for embedded broker users.
