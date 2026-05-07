#!/bin/sh
set -eu

if command -v useradd >/dev/null 2>&1 && ! id ech0 >/dev/null 2>&1; then
  useradd --system --home-dir /var/lib/ech0 --shell /usr/sbin/nologin ech0
fi

if command -v install >/dev/null 2>&1; then
  install -d -o ech0 -g ech0 -m 0750 /var/lib/ech0 /var/log/ech0
fi

if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload || true
fi
