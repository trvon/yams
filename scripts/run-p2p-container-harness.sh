#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 YAMS Contributors
#
# Run the container P2P memory-sync validation harness (scenarios 1 + 3).
# Builds the saged image from source, starts two daemons + MinIO, and asserts
# cross-daemon convergence and causal-ordering determinism.
#
# Usage: scripts/run-p2p-container-harness.sh

set -euo pipefail

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../tests/integration/p2p" && pwd)"
exec "$HARNESS_DIR/validate_p2p_sync.sh" "$@"
