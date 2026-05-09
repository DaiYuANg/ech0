# Docker Deploy Examples

Copy the example environment file when you want to customize image names, base image, or host ports:

```sh
cd deploy/docker
cp .env.example .env
```

## Local Build

These examples build from the repository Dockerfile. UPX is enabled in the builder stage and the runtime image only receives the compressed `ech0` binary.

```sh
docker compose -f docker-compose.single.yml up --build
docker compose -f docker-compose.cluster.yml up --build
```

Use a separate cluster data root for clean benchmark or test runs:

```sh
ECH0_CLUSTER_DATA_ROOT=./data/bench-cluster docker compose -f docker-compose.cluster.yml up -d --build
```

Use `-p` plus explicit ports when you need to run a benchmark cluster beside another local ech0 cluster:

```sh
ECH0_NODE1_BROKER_PORT=19190 ECH0_NODE1_ADMIN_PORT=19191 ECH0_NODE1_RAFT_PORT=14210 \
ECH0_NODE2_BROKER_PORT=29190 ECH0_NODE2_ADMIN_PORT=29191 ECH0_NODE2_RAFT_PORT=24210 \
ECH0_NODE3_BROKER_PORT=39190 ECH0_NODE3_ADMIN_PORT=39191 ECH0_NODE3_RAFT_PORT=34210 \
ECH0_CLUSTER_DATA_ROOT=./data/bench-cluster \
docker compose -p ech0-bench -f docker-compose.cluster.yml up -d --build
```

## Release Image

These examples use the published image instead of building locally:

```sh
docker compose -f docker-compose.single.release.yml up
docker compose -f docker-compose.cluster.release.yml up
```

Override the image tag with `ECH0_IMAGE`, for example:

```sh
ECH0_IMAGE=ghcr.io/daiyuang/ech0:v0.1.0 docker compose -f docker-compose.single.release.yml up
```

## Endpoints

Single-node defaults:

- Broker TCP: `127.0.0.1:9090`
- Admin UI: `http://127.0.0.1:9091/ui`
- Metrics: `http://127.0.0.1:9091/metrics`

Cluster defaults:

- Node 1 admin: `http://127.0.0.1:19091/ui`
- Node 2 admin: `http://127.0.0.1:29091/ui`
- Node 3 admin: `http://127.0.0.1:39091/ui`
