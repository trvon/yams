#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
#
# Run TLC on every TLA+ model configuration in formal/tla and compare the
# outcome with the expected one. Fails on any unexpected result.
#
# Usage: scripts/formal/check-tla.sh [config-name ...]
#   config-name  e.g. DaemonReadiness_new (default: all listed below)
#
# Environment:
#   TLA_TOOLS_JAR   use this tla2tools.jar instead of the cached download
#   TLA_CACHE_DIR   download cache (default: ${XDG_CACHE_HOME:-$HOME/.cache}/yams/tla)
#   JAVA            java binary (default: java on PATH)
#   TLC_WORKERS     TLC -workers value (default: auto)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TLA_DIR="$REPO_ROOT/formal/tla"

TLA_VERSION="1.7.4"
TLA_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLA_VERSION}/tla2tools.jar"
TLA_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"

JAVA="${JAVA:-java}"
TLC_WORKERS="${TLC_WORKERS:-auto}"

# config name, expected outcome: pass | invariant:<Name> | property | deadlock
CHECKS="
DaemonReadiness_new pass
DaemonReadiness_daemonfix pass
DaemonReadiness_clientfix pass
DaemonReadiness_old invariant:GetNeverNotFound
DaemonReadiness_witness invariant:NoAddEarly
DaemonReadiness_ipclate invariant:GetNeverNotFound
ClientIoReset_new_wp pass
ClientIoReset_new_rp pass
ClientIoReset_new_wp_load pass
ClientIoReset_announce_rp_load pass
ClientIoReset_new_rp_load property
ClientIoReset_old_wp deadlock
ClientIoReset_old_rp deadlock
"

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

resolve_jar() {
  if [ -n "${TLA_TOOLS_JAR:-}" ]; then
    JAR="$TLA_TOOLS_JAR"
  else
    local cache="${TLA_CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/yams/tla}"
    JAR="$cache/tla2tools-$TLA_VERSION.jar"
    if [ ! -f "$JAR" ]; then
      mkdir -p "$cache"
      echo "downloading tla2tools.jar $TLA_VERSION" >&2
      curl -fsSL -o "$JAR.part" "$TLA_URL"
      mv "$JAR.part" "$JAR"
    fi
  fi
  local got
  got="$(sha256_of "$JAR")"
  if [ "$got" != "$TLA_SHA256" ]; then
    echo "error: $JAR: sha256 $got, expected $TLA_SHA256" >&2
    exit 2
  fi
}

# Classify TLC output: pass | invariant:<Name> | property | deadlock | error
classify() {
  local log="$1" inv
  inv="$(sed -n 's/^Error: Invariant \([A-Za-z0-9_]*\) is violated.*/\1/p' "$log" | head -1)"
  if [ -n "$inv" ]; then
    echo "invariant:$inv"
  elif grep -q '^Error: Temporal properties were violated' "$log"; then
    echo "property"
  elif grep -q '^Error: Deadlock reached' "$log"; then
    echo "deadlock"
  elif grep -q '^Model checking completed. No error has been found' "$log"; then
    echo "pass"
  else
    echo "error"
  fi
}

run_one() {
  local cfg="$1" expected="$2" module log meta got stats
  module="${cfg%%_*}"
  log="$WORK/$cfg.log"
  meta="$WORK/$cfg.states"
  (cd "$TLA_DIR" && "$JAVA" -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
     -workers "$TLC_WORKERS" -metadir "$meta" -config "$cfg.cfg" "$module.tla") \
     >"$log" 2>&1 || true
  rm -rf "$meta"
  got="$(classify "$log")"
  stats="$(sed -n \
    -e 's/^\([0-9]*\) states generated, \([0-9]*\) distinct states found.*/\1 generated, \2 distinct,/p' \
    -e 's/^The depth of the complete state graph search is \([0-9]*\).*/depth \1,/p' \
    -e 's/^Finished in \([^ ]*\( [^ ]*s\)\{0,1\}\) at.*/\1/p' "$log" | tr '\n' ' ')"
  if [ "$got" = "$expected" ]; then
    printf 'ok    %-32s %-28s %s\n' "$cfg" "$got" "$stats"
  else
    printf 'FAIL  %-32s got %s, expected %s (log: %s)\n' "$cfg" "$got" "$expected" "$log"
    FAILED=$((FAILED + 1))
  fi
}

main() {
  if ! command -v "$JAVA" >/dev/null 2>&1; then
    echo "error: java not found (set JAVA)" >&2
    exit 2
  fi
  resolve_jar
  WORK="$(mktemp -d "${TMPDIR:-/tmp}/yams-tlc.XXXXXX")"
  FAILED=0
  local name expected want found
  while read -r name expected; do
    [ -n "$name" ] || continue
    if [ "$#" -gt 0 ]; then
      found=0
      for want in "$@"; do
        [ "$want" = "$name" ] && found=1
      done
      [ "$found" -eq 1 ] || continue
    fi
    run_one "$name" "$expected"
  done <<EOF
$CHECKS
EOF
  if [ "$FAILED" -gt 0 ]; then
    echo "$FAILED check(s) failed; logs in $WORK" >&2
    exit 1
  fi
  rm -rf "$WORK"
}

main "$@"
