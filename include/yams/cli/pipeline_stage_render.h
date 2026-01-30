#pragma once

#include <yams/cli/ui_helpers.hpp>

#include <cstdint>
#include <cstddef>
#include <vector>

namespace yams::cli::detail {

struct StageInfo {
    const char* name;
    uint64_t inflight;
    uint64_t queueDepth;
    uint64_t limit;
    uint64_t queued;
    uint64_t consumed;
    uint64_t dropped;
};

/// Render pipeline stage rows for the daemon status display.
/// Returns rows for visible stages; empty if all stages are idle.
std::vector<ui::Row> renderPipelineStages(const StageInfo* stages, size_t count);

} // namespace yams::cli::detail
