#pragma once

#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

namespace yams::cli {
class YamsCLI;
} // namespace yams::cli

namespace yams::daemon {
struct GetStatsResponse;
} // namespace yams::daemon

namespace yams::cli::doctor {

class DoctorContext;

/// Aggregates orphan statistics from the daemon's repair metrics:
/// FTS5 orphans, metadata orphans, chunk orphans, KG orphans.
/// Reports a summary health score and cleanup guidance.
class OrphanSummaryCheck {
public:
    struct Result {
        uint64_t fts5OrphansDetected{0};
        uint64_t fts5OrphansRemoved{0};
        bool repairInProgress{false};
        uint64_t repairQueueDepth{0};
        uint64_t corruptArtifacts{0};
        bool ok{true};
    };

    /// Run the diagnostic — reads from daemon stats when available.
    Result execute(const DoctorContext& ctx);

    /// Render the result to the given output stream.
    static void render(std::ostream& os, const Result& r);
};

} // namespace yams::cli::doctor
