// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <string>

#include <yams/core/types.h>

namespace yams::daemon {
struct MemorySyncResponse;
} // namespace yams::daemon

namespace yams::cli {

/// Render `yams p2p status` from a memory-sync status reply, as text or JSON. Pure: no RPCs.
///
/// Text output keeps its first line of space-separated key=value tokens stable for scripts and
/// test harnesses, adds an `apply ...` line with per-stage deferral and failure counts, and adds
/// a line for the last apply and last inbound failures when there are any.
Result<std::string> formatP2pStatus(const yams::daemon::MemorySyncResponse& response, bool json);

} // namespace yams::cli
