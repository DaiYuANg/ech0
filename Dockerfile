ARG BASE_IMAGE=debian

FROM rust:1.90-bookworm AS builder-debian

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY broker-bin/Cargo.toml broker-bin/Cargo.toml
COPY broker/Cargo.toml broker/Cargo.toml
COPY direct/Cargo.toml direct/Cargo.toml
COPY protocol/Cargo.toml protocol/Cargo.toml
COPY queue/Cargo.toml queue/Cargo.toml
COPY store/Cargo.toml store/Cargo.toml
COPY transport/Cargo.toml transport/Cargo.toml
COPY broker-bin broker-bin
COPY broker broker
COPY direct direct
COPY protocol protocol
COPY queue queue
COPY store store
COPY transport transport
COPY config config

RUN cargo build --release -p broker-bin

FROM rust:1.90-alpine AS builder-alpine

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY broker-bin/Cargo.toml broker-bin/Cargo.toml
COPY broker/Cargo.toml broker/Cargo.toml
COPY direct/Cargo.toml direct/Cargo.toml
COPY protocol/Cargo.toml protocol/Cargo.toml
COPY queue/Cargo.toml queue/Cargo.toml
COPY store/Cargo.toml store/Cargo.toml
COPY transport/Cargo.toml transport/Cargo.toml
COPY broker-bin broker-bin
COPY broker broker
COPY direct direct
COPY protocol protocol
COPY queue queue
COPY store store
COPY transport transport
COPY config config

RUN apk add --no-cache musl-dev \
  && cargo build --release -p broker-bin

FROM debian:bookworm-slim AS runtime-debian

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder-debian /app/target/release/broker /usr/local/bin/broker

EXPOSE 9090 9091 3210

ENTRYPOINT ["/usr/local/bin/broker"]

FROM alpine:3.22 AS runtime-alpine

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder-alpine /app/target/release/broker /usr/local/bin/broker

EXPOSE 9090 9091 3210

ENTRYPOINT ["/usr/local/bin/broker"]

FROM runtime-${BASE_IMAGE} AS runtime

EXPOSE 9090 9091 3210

ENTRYPOINT ["/usr/local/bin/broker"]
