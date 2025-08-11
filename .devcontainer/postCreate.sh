#!/usr/bin/env bash
# postCreate.sh - DevContainer post-create hook for YAMS
# - Verifies toolchain availability/versions
# - Configures git safe.directory for mounted workspace
# - Emits a concise environment summary

set -euo pipefail

say() { printf '%s\n' "$*"; }
hr() { printf '%s\n' "----------------------------------------------------------------"; }

# Print tool version helpers
print_ver() {
  local cmd="$1" label="${2:-$1}" args="${3:---version}"
  if command -v "$cmd" >/dev/null 2>&1; then
    # Some tools (openssl) expect a different form
    if [ "$args" = "<raw>" ]; then
      say "[$label] $($cmd 2>/dev/null | head -n1)"
    else
      say "[$label] $($cmd $args 2>/dev/null | head -n1)"
    fi
  else
    say "[$label] not found"
  fi
}

# Environment snapshot
hr
say "YAMS DevContainer postCreate starting..."
hr

say "[system] uname: $(uname -a)"
if command -v lsb_release >/dev/null 2>&1; then
  say "[system] distro: $(lsb_release -ds)"
fi

say "[env]   CC=${CC:-unset} | CXX=${CXX:-unset}"
say "[env]   PKG_CONFIG_PATH=${PKG_CONFIG_PATH:-unset}"
say "[env]   OPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR:-unset}"
say "[env]   CMAKE_GENERATOR=${CMAKE_GENERATOR:-unset}"

hr
# Toolchain checks
print_ver gcc GCC
print_ver g++ G++
print_ver clang Clang
print_ver cmake CMake
print_ver ninja Ninja --version
print_ver pkg-config "pkg-config"
print_ver git Git
print_ver openssl OpenSSL "<raw>"
print_ver sqlite3 SQLite3 --version
print_ver protoc Protobuf "—version"
# act is optional; only printed if present
print_ver act "act (GitHub Actions local runner)" --version
hr

# Configure git safe.directory for the workspace (helpful when mount ownership differs)
# Determine repository root if this is a git repo
repo_dir="$(pwd)"
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  repo_dir="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
fi

say "[git] Marking repository as safe.directory: ${repo_dir}"
git config --global --add safe.directory "${repo_dir}" || true

# Optionally mark /workspaces/* as safe (useful in VS Code Dev Containers)
if [ -d "/workspaces" ]; then
  say "[git] Marking /workspaces as a safe parent directory (non-fatal)"
  # Enumerate immediate subdirectories and add them as safe
  for d in /workspaces/*; do
    [ -d "$d" ] || continue
    git config --global --add safe.directory "$d" || true
  done
fi

hr
say "YAMS DevContainer postCreate complete."
hr

exit 0
