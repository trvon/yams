#pragma once

#include <cstddef>
#include <vector>

#include <yams/app/services/services.hpp>
#include <yams/daemon/ipc/ipc_protocol_responses.h>

namespace yams::daemon::grep_result_window {

struct SelectionStats {
    std::size_t inputMatches{0};
    std::size_t uniqueMatches{0};
    std::size_t emittedMatches{0};
};

struct Selection {
    std::vector<GrepMatch> matches;
    SelectionStats stats;
};

/// Select a stable, identity-deduplicated final match window.
///
/// A zero maxTotalMatches keeps all unique matches. Evidence identity is file,
/// line number, and whitespace-normalized line text. The first occurrence wins.
[[nodiscard]] Selection select(const std::vector<app::services::GrepFileResult>& fileResults,
                               std::size_t maxTotalMatches);

} // namespace yams::daemon::grep_result_window
