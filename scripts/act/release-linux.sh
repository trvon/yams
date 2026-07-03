#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

mkdir -p ./.act/artifacts

ACT_BIN=""
if command -v act >/dev/null 2>&1; then
  ACT_BIN="act"
elif command -v gh >/dev/null 2>&1 && gh act --help >/dev/null 2>&1; then
  ACT_BIN="gh act"
else
  echo "act not found. Install with: brew install act" >&2
  exit 2
fi

ARCH_ARG=()
if [ "$(uname -s)" = "Darwin" ] && [ "$(uname -m)" = "arm64" ]; then
  ARCH_ARG=(--container-architecture linux/amd64)
fi

extra_container_opts=("-v" "$HOME/.conan2:/github/home/.conan2")

# Run only the linux-hosted release matrix entry in fast mode for local parity.
# shellcheck disable=SC2086
${ACT_BIN} workflow_dispatch \
  -W .github/workflows/release.yml \
  -j build-release \
  --matrix os:linux-hosted \
  --input channel=nightly \
  --input fast_mode=true \
  "${ARCH_ARG[@]}" \
  --artifact-server-path ./.act/artifacts \
  --container-options "${extra_container_opts[*]}"
