ARG BASE_IMAGE=debian
ARG GO_VERSION=1.26

FROM golang:${GO_VERSION}-bookworm AS builder-debian

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY ech0.go ./
COPY broker broker
COPY cmd cmd
COPY direct direct
COPY protocol protocol
COPY queue queue
COPY store store
COPY transport transport

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/broker ./cmd/ech0

FROM golang:${GO_VERSION}-alpine AS builder-alpine

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY ech0.go ./
COPY broker broker
COPY cmd cmd
COPY direct direct
COPY protocol protocol
COPY queue queue
COPY store store
COPY transport transport

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/broker ./cmd/ech0

FROM debian:bookworm-slim AS runtime-debian

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder-debian /out/broker /usr/local/bin/broker

EXPOSE 9090 9091 3210

ENTRYPOINT ["/usr/local/bin/broker"]

FROM alpine:3.22 AS runtime-alpine

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder-alpine /out/broker /usr/local/bin/broker

EXPOSE 9090 9091 3210

ENTRYPOINT ["/usr/local/bin/broker"]

FROM runtime-${BASE_IMAGE} AS runtime

EXPOSE 9090 9091 3210

ENTRYPOINT ["/usr/local/bin/broker"]
