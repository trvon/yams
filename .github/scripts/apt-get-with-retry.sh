#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

apt_sources=(
  /etc/apt/sources.list
  /etc/apt/sources.list.d
  /etc/apt/apt-mirrors.txt
)

if [ "$(dpkg --print-architecture)" = "arm64" ]; then
  source_files="$(sudo grep -RIl '://ports.ubuntu.com/ubuntu-ports' \
    "${apt_sources[@]}" 2>/dev/null || true)"
  if [ -n "$source_files" ]; then
    printf '%s\n' "$source_files" | xargs sudo sed -i \
      's|http://ports.ubuntu.com/ubuntu-ports|https://azure.ports.ubuntu.com/ubuntu-ports|g; s|https://ports.ubuntu.com/ubuntu-ports|https://azure.ports.ubuntu.com/ubuntu-ports|g'
  fi
else
  # GitHub's x86_64 image prefers an Azure mirror that can accept a connection and then stall.
  # Use the canonical archive so apt's bounded retries can make progress during mirror outages.
  source_files="$(sudo grep -RIl '://azure.archive.ubuntu.com/ubuntu' \
    "${apt_sources[@]}" 2>/dev/null || true)"
  if [ -n "$source_files" ]; then
    printf '%s\n' "$source_files" | xargs sudo sed -i \
      's|http://azure.archive.ubuntu.com/ubuntu|https://archive.ubuntu.com/ubuntu|g; s|https://azure.archive.ubuntu.com/ubuntu|https://archive.ubuntu.com/ubuntu|g'
  fi
fi

attempt=1
while ! sudo apt-get \
  -o Acquire::Retries=2 \
  -o Acquire::ForceIPv4=true \
  -o Acquire::http::Timeout=20 \
  -o Acquire::https::Timeout=20 \
  "$@"; do
  if [ "$attempt" -ge 4 ]; then
    exit 1
  fi
  echo "apt-get failed (attempt ${attempt}/4): $*" >&2
  sleep $((attempt * 10))
  attempt=$((attempt + 1))
done
