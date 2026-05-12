#include <yams/cli/doctor/checks/orphan_summary.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <filesystem>
#include <sstream>

namespace yams::cli::doctor {

OrphanSummaryCheck::Result OrphanSummaryCheck::execute(const DoctorContext& ctx) {
    Result r;
    const auto& state = ctx.cachedState();

    // ── Daemon-side metrics (when available) ──
    if (state.stats) {
        const auto& st = *state.stats;
        // Check for orphan-related repair stats in additionalStats
        auto getU64 = [&](const char* k) -> uint64_t {
            auto it = st.additionalStats.find(k);
            if (it == st.additionalStats.end())
                return 0;
            try {
                return static_cast<uint64_t>(std::stoull(it->second));
            } catch (...) {
                return 0;
            }
        };
        r.repairInProgress = getU64("repair_in_progress") > 0;
        r.repairQueueDepth = getU64("repair_queue_depth");
        r.fts5OrphansDetected = getU64("fts5_orphans_detected");
        r.fts5OrphansRemoved = getU64("fts5_orphans_removed");
    }

    // ── Corrupt artifacts on disk ──
    auto dataDir = ctx.dataDir();
    std::error_code ec;
    namespace fs = std::filesystem;
    if (fs::exists(dataDir, ec)) {
        for (const auto& entry : fs::directory_iterator(dataDir, ec)) {
            if (ec)
                break;
            auto name = entry.path().filename().string();
            if (name.find(".corrupt-") != std::string::npos)
                r.corruptArtifacts++;
        }
    }

    // Health assessment
    if (r.fts5OrphansDetected > 0 || r.corruptArtifacts > 0)
        r.ok = false;

    return r;
}

void OrphanSummaryCheck::render(std::ostream& os, const Result& r) {
    using namespace yams::cli::ui;

    if (r.corruptArtifacts > 0) {
        os << "  "
           << status_warning(std::to_string(r.corruptArtifacts) +
                             " corrupt database artifact(s) found")
           << "\n";
        os << "    → Run 'yams doctor --fix' or restart daemon for automatic salvage.\n";
    }

    if (r.fts5OrphansDetected > 0) {
        os << "  "
           << status_ok("FTS5 orphan scan: " + std::to_string(r.fts5OrphansDetected) +
                        " detected, " + std::to_string(r.fts5OrphansRemoved) + " removed")
           << "\n";
    }

    if (r.repairInProgress) {
        os << "  "
           << status_pending(
                  "Repair in progress (queue depth: " + std::to_string(r.repairQueueDepth) + ")")
           << "\n";
    }

    if (r.ok) {
        os << "  " << status_ok("No orphan or integrity issues detected.") << "\n";
    }
}

} // namespace yams::cli::doctor
