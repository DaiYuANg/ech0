# Docker Deploy Examples

Copy the example environment file when you want to customize image names, base image, UPX, or host ports:

```sh
cd deploy/docker
cp .env.example .env
```

## Local Build

These examples build from the repository Dockerfile. UPX is enabled by default in the builder stage and the runtime image only receives the compressed `ech0` binary.

```sh
docker compose -f docker-compose.single.yml up --build
docker compose -f docker-compose.cluster.yml up --build
```

Disable Dockerfile UPX compression when you need an uncompressed binary for debugging:

```sh
ECH0_ENABLE_UPX=false docker compose -f docker-compose.single.yml up --build
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
