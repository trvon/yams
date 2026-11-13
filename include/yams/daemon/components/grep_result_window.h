#pragma once

#include <cstddef>
#include <string>
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

/// Matches for a request that asked for files or counts rather than lines.
///
/// The grep service answers those with a file set and per-file counts and builds no line
/// records, but clients read `matches`. Files-only yields one "path" record per matching
/// file; count yields one record per matching line, with no line text or number.
[[nodiscard]] std::vector<GrepMatch>
summarize(const std::vector<app::services::GrepFileResult>& fileResults,
          const std::vector<std::string>& filesWith, bool filesOnly);

} // namespace yams::daemon::grep_result_window
