# Operations

The `ech0` binary packages the broker runtime, TCP protocol server, Admin UI, metrics, configuration loading, scheduling, and Dragonboat Raft runtime into one executable.

## Configuration

Binary configuration is loaded with `arcgolabs/configx` as part of the broker `dix` application. The CLI passes config paths and flags as a config source; the broker module graph resolves the final `Config`.

Load order:

1. Typed defaults from `broker.DefaultConfig`.
2. Config files.
3. Environment variables with prefix `ECH0`.
4. Cobra/pflag command flags.

Environment variables use `__` as the nesting separator. For example:

```text
ECH0_BROKER__BIND_ADDR=0.0.0.0:9092
ECH0_ADMIN__BIND_ADDR=0.0.0.0:8080
ECH0_RAFT__BIND_ADDR=0.0.0.0:3210
ECH0_DISCOVERY__ENABLED=true
```

The embedded root package does not expose this full config surface. Embedded users should use `ech0.Options`.

## Auth Configuration

The binary runtime wires auth through the same `configx` and `dix` graph as the rest of the broker. By default auth is disabled for the small embedded/single-binary mental model. Adding static tokens enables auth and disables anonymous access:

```toml
[governance.auth]

[[governance.auth.static_tokens]]
token = "change-me"
principal = "admin"
tenant = "default"
namespace = "default"
```

TCP clients pass the token in the handshake `auth_token` field. Admin requests can use `Authorization: Bearer <token>`, `X-Ech0-Auth-Token`, or the `auth_token` query parameter. Library users can still inject a custom `authx` provider or engine with `broker.WithAuthProvider` / `broker.WithAuthEngine`.

## Single-Replica Cluster

Single-replica cluster mode runs one broker process with a Dragonboat metadata group and local segment-log data shards. This is the default when `raft.cluster` contains one peer:

```sh
ech0 --config config/ech0.toml.example
```

Metadata writes go through Dragonboat. Message writes use the local segment hot path in a single-peer deployment and the owning Dragonboat data shard group in a multi-peer cluster. Scheduled jobs are gated by the local Dragonboat leader state.

## Cluster Mode

Cluster mode coordinates mutating broker commands through Dragonboat multi-group Raft when `raft.cluster` or the discovery-resolved peer set contains multiple peers:

- Topic creation.
- Produces and batch produces on the owning data shard group.
- Offset commits on the same data shard group as the topic partition.
- Direct sends and direct acks.
- Consumer group membership, heartbeats, and rebalances.

Dragonboat owns raft-side logs, snapshots, membership state, and recovery files under `data/dragonboat/<node_id>`. Broker state machines expose `store.Snapshotter` so Dragonboat can capture and restore business state; the broker does not open a separate metadata database in binary or embedded runtime.

`raft.bind_addr` is the local listen address and may use `0.0.0.0` in containers. `raft.advertise_addr` is the routable Raft address announced to peers. When `raft.advertise_addr` is empty, the current node's matching entry in `raft.cluster` is used as the Raft advertised address.

`raft.read_policy` controls clustered read consistency. `local` reads from the local runtime with the lowest latency. `leader` requires the local node to lead the owning group and uses a Dragonboat read barrier before fetching. `linearizable` also uses a Dragonboat read barrier, but it can be called on followers and is useful when correctness is more important than the extra consensus round-trip.

## Discovery

Discovery is an optional bootstrap layer. It is not the authority for Raft membership after startup; Dragonboat still owns replicated state, raft logs, snapshots, and membership files. The initial implementation supports a static provider and a `memberlist` provider.

With memberlist enabled, each node advertises its node id, cluster name, Raft address, broker address, admin address, and data shard count. Startup joins the configured seeds, waits for `discovery.bootstrap_expect` alive nodes, filters nodes by `broker.cluster_name`, then builds the Dragonboat initial peer set from the discovered Raft addresses.

Example:

```toml
[raft]
bind_addr = "0.0.0.0:3210"
advertise_addr = "ech0-node1:3210"

[discovery]
enabled = true
provider = "memberlist"
bind_addr = "0.0.0.0:7946"
seeds = ["ech0-node2:7946", "ech0-node3:7946"]
bootstrap_expect = 3
join_timeout_ms = 30000
```

`discovery.advertise_addr` is optional. When set for memberlist it must be an IP address and port; Docker Compose examples leave it empty so memberlist advertises the container IP while seeds still use service names.

Use discovery for new cluster bootstrap and live node visibility. Do not use gossip suspicion alone to remove Dragonboat voting members; membership changes must go through controlled Raft operations.

When a node is not leader, mutating commands return a not-leader error. Scheduled jobs use a gocron distributed elector and only run on the current leader.

Cluster control endpoints are exposed on the admin API:

- `POST /api/cluster/nodes/join` asks Dragonboat to add a voting replica for every configured broker group. Body fields: `node_id`, `addr`.
- `POST /api/cluster/nodes/leave` asks Dragonboat to remove a voting replica from every configured broker group. Body field: `node_id`.
- `POST /api/cluster/leadership/transfer` asks Dragonboat to transfer one group leader. Body fields: `group_id`, `target_node`.
- `POST /api/cluster/leadership/balance` requests best-effort leader transfers from overloaded nodes to less-loaded nodes.
- `POST /api/cluster/partitions/reassign` updates the shard placement for an empty partition. Body fields: `topic`, `partition`, `shard_id`.

Live partition reassignment is intentionally rejected until data movement is implemented; moving a partition with retained records would otherwise make existing segment data unreachable.

## Scheduled Jobs

The scheduled runtime is built on `go-co-op/gocron` and includes:

- Delay scheduler.
- Retry worker.
- Retention cleanup.
- Compaction cleanup.
- Configured webhook sinks.
- Configured file sinks.
- Configured S3-compatible sinks.
- Configured HTTP mirror sinks.
- Configured database outbox pollers.

All jobs use singleton mode. `raftElector` permits execution only when the node is the current Dragonboat leader.

Configured sinks are at-least-once delivery loops. Each sink polls a single topic partition with `read_committed` isolation and commits the sink consumer offset only after the external side effect succeeds. A non-2xx HTTP response, failed write, failed sync, SQL error, or request error leaves the offset uncommitted so the next run retries the same message.

Webhook sinks are configured under `broker.webhook_sinks` and POST one JSON envelope per message to the configured URL.

```toml
[[broker.webhook_sinks]]
name = "orders-webhook"
topic = "orders"
partition = 0
consumer = "orders-webhook"
url = "http://127.0.0.1:8088/events"
interval_secs = 5
max_records = 100
timeout_ms = 5000

[[broker.webhook_sinks.headers]]
key = "X-Ech0-Sink"
value = "orders-webhook"
```

File sinks are configured under `broker.file_sinks`. They append one JSON envelope per message to the target JSONL file and sync the file before committing the sink offset.

```toml
[[broker.file_sinks]]
name = "orders-file"
topic = "orders"
partition = 0
consumer = "orders-file"
path = "./data/exports/orders.jsonl"
interval_secs = 10
max_records = 100
```

S3-compatible sinks are configured under `broker.s3_sinks`. They PUT one JSON envelope per message to `endpoint/bucket/prefix/topic/partition/offset.json`. When access key and secret key are configured, the sink signs requests with AWS SigV4.

```toml
[[broker.s3_sinks]]
name = "orders-s3"
topic = "orders"
partition = 0
consumer = "orders-s3"
endpoint = "http://127.0.0.1:9000"
bucket = "ech0"
prefix = "exports"
region = "us-east-1"
interval_secs = 30
max_records = 100
```

Mirror sinks are configured under `broker.mirror_sinks`. They replicate records to another ech0 cluster through the Admin HTTP gateway and commit the source offset only after the target gateway returns 2xx.

```toml
[[broker.mirror_sinks]]
name = "orders-mirror"
source_topic = "orders"
source_partition = 0
source_consumer = "orders-mirror"
target_base_url = "http://remote-ech0:8080"
target_topic = "orders"
interval_secs = 5
max_records = 100
```

Database outboxes are configured under `broker.database_outboxes`. The SQL query must return columns in this order: `id`, `topic`, `partition`, `key`, `routing_key`, and `payload`. The poller publishes each row, then executes `mark_delivered_sql` with the row id. A mark failure can cause duplicate publish on retry, so consumers should use idempotent keys or producer sequence semantics for critical flows.

```toml
[[broker.database_outboxes]]
name = "orders-outbox"
driver_name = "postgres"
dsn = "postgres://user:pass@127.0.0.1:5432/app?sslmode=disable"
query = "select id, topic, partition, key, routing_key, payload from outbox where delivered_at is null order by id limit 100"
mark_delivered_sql = "update outbox set delivered_at = now() where id = $1"
interval_secs = 5
max_records = 100
```

Sink delivery bodies contain `sink`, `consumer`, `topic`, `partition`, and a `record` object matching the HTTP gateway fetch record shape. Record payloads always include `payload_base64`, and include `payload` only when the payload is valid UTF-8.

## Admin UI and API

Admin is served from the broker package on Fiber. It provides:

- Health endpoints.
- Metrics endpoint.
- OpenAPI document endpoint.
- Cluster metadata endpoint.
- JSON HTTP gateway endpoints for produce, fetch, and offset commit.
- Topic and message views.
- Operations UI for manually triggering configured webhook, file, mirror, S3, and database outbox jobs.
- Embedded HTML templates styled with Tailwind CDN.

Admin and OpenAPI use `arcgolabs/httpx` for the HTTP surface while the default server is Fiber.

`GET /api/cluster` reports the configured Dragonboat peers, local advertise address, raft group health, data shard runtime modes, discovery state, and a leader distribution summary.

HTTP gateway endpoints:

- `POST /api/gateway/topics/{topic}/records` appends one record.
- `GET /api/gateway/topics/{topic}/partitions/{partition}/records?consumer=...&max_records=...` fetches records.
- `POST /api/gateway/topics/{topic}/partitions/{partition}/commit` commits a consumer offset.

Produce accepts JSON text fields (`payload`, `key`, header `value`) or base64 alternatives (`payload_base64`, `key_base64`, header `value_base64`). Fetch responses always include `payload_base64`, and include `payload` only when the payload is valid UTF-8. Gateway requests use the same admin identity middleware, so tenant, namespace, auth, ACL, and quota behavior stays identical to TCP and embedded APIs.

`GET /ui/ops` renders the operations panel. Each configured integration can be run once from the UI, and the corresponding API endpoints are available under `/api/ops/*/run`.

`admin.debug_enabled` defaults to `false`. When enabled, the admin server also exposes `GET /debug/fgprof` for wall-clock profiling with `fgprof`, `GET /api/runtime/events` for recent `dix` build/lifecycle/debug events plus broker control-plane events, and `GET /api/runtime/events/stream` as a Server-Sent Events stream for live admin diagnostics; keep it disabled on public admin surfaces.

## Metrics

Metrics are exposed through the admin server and are wired through the broker metrics package. The project uses the arcgolabs observability stack where it fits the runtime surface.

Current metric coverage includes broker/runtime counters, stream gauges, cleanup counters, produce counters, Dragonboat storage metrics, and segment-log hot-path metrics. Segment-log metrics are emitted through the internal `store.StoreMetrics` interface and include append/read operation, stage, record count, duration, and error status.

Hot-path performance metrics include broker command duration, Dragonboat proposal duration, FSM duration, fetch duration, store append duration, and store read duration. They are intended for short benchmark and load-test runs where stage-level labels are useful for locating bottlenecks.

For mixed CPU and IO investigations, enable `admin.debug_enabled` and collect `fgprof` output from `/debug/fgprof`. This is useful for distinguishing on-CPU work from raft, storage, and filesystem wait time during benchmark runs.

## Tracing

The broker service accepts an OpenTelemetry `trace.TracerProvider` through `broker.WithTracerProvider`. When no provider is supplied, the broker uses a noop provider. Current spans cover create-topic, publish, batch publish, fetch, `read_committed` fetch, and offset commit operations. Binary deployments can wire a concrete exporter outside the root library API while embedded users can keep the default no-op behavior.

## Docker

Docker examples live in `deploy/docker`:

- `docker-compose.single.yml` builds a local single-node image.
- `docker-compose.cluster.yml` builds a local multi-node cluster.
- `docker-compose.single.release.yml` uses the release image.
- `docker-compose.cluster.release.yml` uses the release image.

The release Dockerfiles are multi-stage. They install UPX in a compressor stage, compress the GoReleaser binary when needed, and copy only the compressed executable into the runtime stage. The default release image uses Alpine; Debian slim is published with the `-debian` tag suffix.

## Release Packaging

GoReleaser drives release artifacts:

- Archives for Linux, macOS, and Windows.
- `.deb` and `.rpm` packages.
- systemd unit and default config.
- Multi-platform Docker images for Alpine and Debian runtimes.

Packaging assets live under `packaging/`.

Full local release verification expects `upx` and Docker to be available. Without Docker, `goreleaser release --snapshot --clean --skip=docker` verifies archives and Linux packages.

## Segment Repair

`ech0 repair segments --path <segment-log-root>` validates segment index files offline and rebuilds missing `.idx` files from self-describing segment frames. Use `--dry-run` to inspect planned repairs without writing. Corrupt existing indexes are reported but not rewritten unless `--rebuild-corrupt-indexes` is passed; that mode moves the corrupt index aside with a `.corrupt.<timestamp>` suffix before rebuilding from the segment file.

## Operational Boundaries

The binary owns operational concerns:

- `configx` config loading through `dix`.
- file/stdout logging setup.
- Admin UI and OpenAPI.
- Raft networking and state.
- Release packaging and container defaults.

The root `ech0` library should not expose these concerns unless they become essential for embedded broker users.
