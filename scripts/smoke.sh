#!/usr/bin/env bash
set -euo pipefail

BROKER_ADDR="${BROKER_ADDR:-127.0.0.1:9090}"
SMOKE_STARTUP_TIMEOUT_SECS="${SMOKE_STARTUP_TIMEOUT_SECS:-25}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --broker-addr)
      BROKER_ADDR="$2"
      shift 2
      ;;
    --startup-timeout-seconds)
      SMOKE_STARTUP_TIMEOUT_SECS="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

export BROKER_ADDR
export SMOKE_STARTUP_TIMEOUT_SECS

cargo run -p broker --example smoke_all
