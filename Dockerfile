ARG BASE_IMAGE=debian
ARG GO_VERSION=1.26

FROM golang:${GO_VERSION}-alpine AS builder

ARG ENABLE_UPX=true

WORKDIR /src

RUN if [ "${ENABLE_UPX}" = "true" ]; then apk add --no-cache upx; fi

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ech0 ./cmd/ech0 \
  && if [ "${ENABLE_UPX}" = "true" ]; then upx --best --lzma /out/ech0; fi

FROM debian:bookworm-slim AS runtime-debian

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mkdir -p /app/config /var/lib/ech0 /var/log/ech0

COPY --from=builder /out/ech0 /usr/local/bin/ech0
COPY packaging/ech0.toml /app/config/ech0.toml

EXPOSE 9090 9091 3210

ENTRYPOINT ["/usr/local/bin/ech0"]
CMD ["--config", "/app/config/ech0.toml"]

FROM alpine:3.22 AS runtime-alpine

RUN apk add --no-cache ca-certificates

WORKDIR /app

RUN mkdir -p /app/config /var/lib/ech0 /var/log/ech0

COPY --from=builder /out/ech0 /usr/local/bin/ech0
COPY packaging/ech0.toml /app/config/ech0.toml

EXPOSE 9090 9091 3210

ENTRYPOINT ["/usr/local/bin/ech0"]
CMD ["--config", "/app/config/ech0.toml"]

FROM runtime-${BASE_IMAGE} AS runtime

EXPOSE 9090 9091 3210

ENTRYPOINT ["/usr/local/bin/ech0"]
CMD ["--config", "/app/config/ech0.toml"]
