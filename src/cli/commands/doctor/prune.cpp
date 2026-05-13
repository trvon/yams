#include <yams/cli/doctor/prune.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/result_helpers.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <iomanip>
#include <memory>

namespace yams::cli::doctor {

PruneCommand::PruneCommand(YamsCLI* cli, Config config) : cli_(cli), config_(std::move(config)) {}

void PruneCommand::execute(std::ostream& os) {
    using namespace yams::cli::ui;
    namespace daemon = yams::daemon;

    if (config_.categories.empty() && config_.extensions.empty()) {
        // Display help when no filter args provided
        os << "\n" << section_header("YAMS Doctor Prune") << "\n\n";
        os << "Remove build artifacts, logs, cache, and temporary files from YAMS index.\n\n";
        os << subsection_header("Composite Categories") << "\n\n";
        os << "  " << colorize("build-artifacts", Ansi::CYAN)
           << "  - Compiled objects, libraries, executables, archives\n";
        os << "  " << colorize("build-system", Ansi::CYAN)
           << "     - CMake, Ninja, Meson, Make, Gradle, Maven, NPM, Cargo, Go, Flutter, Dart\n";
        os << "  " << colorize("build", Ansi::CYAN)
           << "            - Both build-artifacts and build-system\n";
        os << "  " << colorize("git-artifacts", Ansi::CYAN)
           << "   - Git internal files (.git/objects, .git/logs, .git/refs)\n";
        os << "  " << colorize("logs", Ansi::CYAN) << "             - Build and test logs\n";
        os << "  " << colorize("cache", Ansi::CYAN)
           << "            - Compiler and package manager cache\n";
        os << "  " << colorize("temp", Ansi::CYAN)
           << "             - Temporary files, backups, swap files\n";
        os << "  " << colorize("coverage", Ansi::CYAN) << "         - Code coverage data\n";
        os << "  " << colorize("ide", Ansi::CYAN)
           << "              - IDE project files and caches\n";
        os << "  " << colorize("all", Ansi::CYAN)
           << "              - Everything except distribution packages\n\n";
        os << subsection_header("Usage Examples") << "\n\n";
        os << "  " << colorize("yams doctor prune --category build-artifacts", Ansi::DIM) << "\n";
        os << "  "
           << colorize("yams doctor prune --category logs --older-than 30d --apply", Ansi::DIM)
           << "\n";
        os << "  " << colorize("yams doctor prune --extension o,obj,log --apply", Ansi::DIM)
           << "\n\n";
        os << colorize("Note:", Ansi::YELLOW)
           << " Use --apply to actually delete files (dry-run by default).\n\n";
        return;
    }

    if (!cli_) {
        os << "  " << status_error("CLI context unavailable") << "\n";
        return;
    }

    daemon::PruneRequest req;
    req.categories = config_.categories;
    req.extensions = config_.extensions;
    req.olderThan = config_.olderThan;
    req.largerThan = config_.largerThan;
    req.smallerThan = config_.smallerThan;
    req.dryRun = !config_.apply;
    req.verbose = config_.verbose;

    daemon::ClientConfig clientCfg;
    clientCfg.socketPath = YamsCLI::resolveConfiguredDaemonSocketPath();
    clientCfg.requestTimeout = std::chrono::minutes(10);

    if (!daemon::DaemonClient::isDaemonRunning(clientCfg.socketPath)) {
        os << "  " << status_error("Daemon not running on socket: " + clientCfg.socketPath.string())
           << "\n";
        os << colorize("\nHint:", Ansi::YELLOW) << " Start the daemon with 'yams daemon start'\n\n";
        return;
    }

    os << "\n" << colorize("Sending prune request to daemon...", Ansi::CYAN) << "\n";

    auto leaseRes = acquire_cli_daemon_client(clientCfg, 1, 4);
    if (!leaseRes) {
        os << "  " << status_error("Failed to acquire daemon client: " + leaseRes.error().message)
           << "\n";
        return;
    }
    auto lease = std::move(leaseRes.value());

    auto respRes = run_result(lease->call<daemon::PruneRequest>(req), std::chrono::minutes(10));
    if (!respRes) {
        os << "  " << status_error("Prune request failed: " + respRes.error().message) << "\n";
        return;
    }
    auto resp = respRes.value();

    if (!resp.errorMessage.empty()) {
        os << "  " << status_error("Prune operation failed: " + resp.errorMessage) << "\n";
        return;
    }

    if (!resp.statusMessage.empty()) {
        os << "\n" << colorize(resp.statusMessage, Ansi::CYAN) << "\n";
        os << "Note: Prune is running in the background. Check daemon logs for progress.\n";
        return;
    }

    os << "\n" << section_header("Prune Summary") << "\n\n";

    uint64_t totalFiles = resp.filesDeleted + resp.filesFailed;
    uint64_t totalCandidates = 0;
    for (const auto& [cat, count] : resp.categoryCounts)
        totalCandidates += count;

    const bool useCandidateTotal = req.dryRun && totalCandidates > 0;
    os << "  " << colorize(useCandidateTotal ? "Total candidates:" : "Total files:", Ansi::BOLD)
       << " " << (useCandidateTotal ? totalCandidates : totalFiles) << "\n";
    os << "  " << colorize("Total size:", Ansi::BOLD) << " " << std::fixed << std::setprecision(2)
       << (resp.totalBytesFreed / 1024.0 / 1024.0) << " MB\n\n";

    if (!resp.categoryCounts.empty()) {
        os << subsection_header("By Category") << "\n\n";
        for (const auto& [cat, count] : resp.categoryCounts) {
            auto it = resp.categorySizes.find(cat);
            uint64_t size = (it != resp.categorySizes.end()) ? it->second : 0;
            double sizeMB = size / 1024.0 / 1024.0;
            os << "  " << colorize(cat, Ansi::CYAN) << ": " << count << " files, " << std::fixed
               << std::setprecision(2) << sizeMB << " MB\n";
        }
        os << "\n";
    }

    if (req.dryRun) {
        os << colorize("Dry-run mode.", Ansi::YELLOW) << " Use " << colorize("--apply", Ansi::BOLD)
           << " to actually delete files.\n\n";
    } else {
        os << colorize("Deleted " + std::to_string(resp.filesDeleted) + " files", Ansi::GREEN)
           << "\n";
        if (resp.filesFailed > 0)
            os << colorize("Failed to delete " + std::to_string(resp.filesFailed) + " files",
                           Ansi::YELLOW)
               << "\n";
        os << "\n";
    }
}

} // namespace yams::cli::doctor
