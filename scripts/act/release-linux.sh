#!/usr/bin/env bash
set -euo pipefail
# Create artifacts dir if missing
mkdir -p ./.act/artifacts
# Map Conan cache to persist dependencies
extra_container_opts=("-v" "$HOME/.conan2:/github/home/.conan2")
# Run only the Linux matrix include using act matrix filter if available
if gh act --help 2>&1 | grep -qE '\-\-matrix|\-M'; then
  gh act push \
    -j build-release \
    -e .act/release.tag.json \
    -M os=linux-hosted \
    --container-options "${extra_container_opts[@]}"
else
  echo "act version lacks matrix filter; running Linux-only via default runner mapping" >&2
  echo "Consider updating act. Trying anyway; macOS jobs may fail."
  gh act push \
    -j build-release \
    -e .act/release.tag.json \
    --container-options "${extra_container_opts[@]}"
fi
