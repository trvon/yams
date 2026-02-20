/// @file repair_command.cpp
/// @brief Thin RPC client for `yams repair` — delegates all work to the daemon's RepairService.

#include <spdlog/spdlog.h>
#include <cstdlib>
#include <iostream>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/core/types.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::cli {

class RepairCommand : public ICommand {
public:
    std::string getName() const override { return "repair"; }

    std::string getDescription() const override { return "Repair and maintain storage integrity"; }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("repair", getDescription());

        cmd->add_flag("--orphans", repairOrphans_, "Clean orphaned metadata entries");
        cmd->add_flag("--mime", repairMime_, "Repair missing MIME types");
        cmd->add_flag("--chunks", repairChunks_, "Clean orphaned chunk files");
        cmd->add_flag("--embeddings", repairEmbeddings_, "Generate missing vector embeddings");
        cmd->add_flag("--fts5", repairFts5_, "Rebuild FTS5 index for documents (best-effort)");
        cmd->add_flag("--optimize", optimizeDb_, "Optimize and vacuum database");
        cmd->add_flag("--downloads", repairDownloads_,
                      "Repair download documents: add tags/metadata and normalize names");
        cmd->add_flag("--path-tree", repairPathTree_,
                      "Rebuild path tree index for documents missing from the tree");
        cmd->add_flag("--refs,--ref", repairBlockRefs_,
                      "Repair block_references with correct sizes from CAS files");
        cmd->add_flag("--stuck", repairStuckDocs_,
                      "Recover stuck documents (failed extraction, ghost success, stalled)");
        cmd->add_option("--include-mime", includeMime_,
                        "Additional MIME types to embed (e.g., application/pdf). Repeatable.")
            ->type_size(-1);
        cmd->add_option("--model", embeddingModel_, "Embedding model to use (overrides preferred)");
        cmd->add_flag("--all", repairAll_, "Run all repair operations");
        cmd->add_flag("--dry-run", dryRun_, "Preview changes without applying");
        cmd->add_flag("--foreground", foreground_,
                      "Run embeddings repair in the foreground (stream progress)");
        cmd->add_flag("-v,--verbose", verbose_, "Show detailed progress and transient errors");
        cmd->add_flag("--force", force_, "Skip confirmation prompts");
        cmd->add_option("--max-retries", maxRetries_, "Max retry attempts for stuck documents");

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                std::string msg = result.error().message;
                auto addHint = [&](std::string_view h) {
                    if (msg.find("hint:") == std::string::npos &&
                        msg.find(h) == std::string::npos) {
                        msg += std::string(" (") + std::string(h) + ")";
                    }
                };
                if (msg.find("FTS5") != std::string::npos ||
                    msg.find("tokenize") != std::string::npos) {
                    addHint("hint: run 'yams repair --fts5'");
                } else if (msg.find("daemon") != std::string::npos ||
                           msg.find("connect") != std::string::npos) {
                    addHint("hint: start the daemon first with 'yams daemon start'");
                }
                spdlog::error("Repair failed: {}", msg);
                std::exit(1);
            }
        });
    }

    Result<void> execute() override {
        using namespace yams::daemon;

        // If no specific repair requested, default to orphans
        if (!repairOrphans_ && !repairMime_ && !repairChunks_ && !repairEmbeddings_ &&
            !repairFts5_ && !optimizeDb_ && !repairDownloads_ && !repairPathTree_ &&
            !repairBlockRefs_ && !repairStuckDocs_ && !repairAll_) {
            repairOrphans_ = true;
        }

        // Build the RPC request
        // NOTE: --all intentionally excludes block_refs for now because it can be very slow
        // on large stores. Use --refs (or --all --refs) to include it explicitly.
        const bool effectiveAll = repairAll_;
        RepairRequest req;
        req.repairOrphans = repairOrphans_ || effectiveAll;
        req.repairMime = repairMime_ || effectiveAll;
        req.repairDownloads = repairDownloads_ || effectiveAll;
        req.repairPathTree = repairPathTree_ || effectiveAll;
        req.repairChunks = repairChunks_ || effectiveAll;
        req.repairBlockRefs = repairBlockRefs_;
        req.repairFts5 = repairFts5_ || effectiveAll;
        req.repairEmbeddings = repairEmbeddings_ || effectiveAll;
        req.repairStuckDocs = repairStuckDocs_ || effectiveAll;
        req.optimizeDb = optimizeDb_ || effectiveAll;
        req.repairAll = false;
        req.dryRun = dryRun_;
        req.verbose = verbose_;
        req.force = force_;
        req.foreground = foreground_;
        req.embeddingModel = embeddingModel_;
        req.includeMime = includeMime_;
        req.maxRetries = maxRetries_;

        // Banner
        std::cout << ui::title_banner("YAMS Storage Repair") << "\n\n";
        try {
            auto dd = cli_->getDataPath();
            std::cout << "  " << ui::key_value("Data directory", dd.string()) << "\n\n";
        } catch (...) {
        }

        if (dryRun_) {
            std::cout << ui::status_info("[DRY RUN MODE] No changes will be made") << "\n\n";
        }

        // Connect to daemon
        ClientConfig cfg;
        if (cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        // Repair can take a long time — generous timeout
        cfg.requestTimeout = std::chrono::milliseconds(600000);
        cfg.headerTimeout = std::chrono::milliseconds(600000);
        cfg.bodyTimeout = std::chrono::milliseconds(600000);

        auto leaseRes = acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            return Error{ErrorCode::NetworkError,
                         "Cannot connect to daemon: " + leaseRes.error().message +
                             " (is the daemon running?)"};
        }
        auto leaseHandle = std::move(leaseRes.value());

        // State for rendering progress
        std::string lastOperation;
        uint64_t lastProcessed = 0;
        uint64_t lastTotal = 0;

        // Streaming event callback — renders progress to stdout
        auto onEvent = [&](const RepairEvent& ev) {
            // Print section transitions
            if (ev.operation != lastOperation) {
                if (!lastOperation.empty()) {
                    std::cout << "\n";
                }
                lastOperation = ev.operation;
                std::cout << "  " << ui::status_pending(ev.operation) << "\n";
            }

            // Phase-based rendering
            if (ev.phase == "completed") {
                if (ev.succeeded > 0 || ev.failed > 0) {
                    std::cout << "    " << ui::status_ok("Done") << "  processed=" << ev.processed
                              << " ok=" << ev.succeeded << " failed=" << ev.failed;
                    if (ev.skipped > 0)
                        std::cout << " skipped=" << ev.skipped;
                    std::cout << "\n";
                } else if (!ev.message.empty()) {
                    std::cout << "    " << ev.message << "\n";
                }
            } else if (ev.phase == "error") {
                std::cout << "    " << ui::status_warning(ev.message) << "\n";
            } else if (ev.phase == "repairing" && ev.total > 0 && ev.processed != lastProcessed) {
                // Show progress for long-running operations (fts5, embeddings)
                // even without --verbose, so the user knows it's not hung.
                std::cout << "    [" << ev.processed << "/" << ev.total << "] " << ev.message
                          << "\r" << std::flush;
                lastProcessed = ev.processed;
                lastTotal = ev.total;
            } else if (verbose_ && ev.phase == "repairing") {
                // Extra detail in verbose mode for operations without a total count
                if (!ev.message.empty()) {
                    std::cout << "    " << ev.message << "\r" << std::flush;
                }
            }
        };

        // Execute the RPC call
        std::promise<Result<RepairResponse>> prom;
        auto fut = prom.get_future();
        boost::asio::co_spawn(
            getExecutor(),
            [leaseHandle, req, &prom, &onEvent]() mutable -> boost::asio::awaitable<void> {
                auto& client = **leaseHandle;
                auto r = co_await client.callRepair(req, onEvent);
                prom.set_value(std::move(r));
                co_return;
            },
            boost::asio::detached);

        std::chrono::milliseconds localWaitTimeout = cfg.requestTimeout;
        if (const char* envMs = std::getenv("YAMS_REPAIR_RPC_TIMEOUT_MS")) {
            try {
                auto parsed = std::stoll(std::string(envMs));
                if (parsed > 0) {
                    localWaitTimeout = std::chrono::milliseconds(parsed);
                }
            } catch (...) {
            }
        }

        auto res = (fut.wait_for(localWaitTimeout) == std::future_status::ready)
                       ? fut.get()
                       : Result<RepairResponse>(Error{ErrorCode::Timeout, "Repair RPC timed out"});

        if (!res) {
            return Error{res.error().code, res.error().message};
        }

        const auto& resp = res.value();

        // Check if repair was rejected because another is already in progress
        if (!resp.success && resp.operationResults.empty() && !resp.errors.empty()) {
            bool alreadyRunning = false;
            for (const auto& e : resp.errors) {
                if (e.find("already in progress") != std::string::npos) {
                    alreadyRunning = true;
                    break;
                }
            }
            if (alreadyRunning) {
                std::cout << "\n"
                          << ui::status_info(
                                 "A repair operation is already running. "
                                 "Please wait for it to finish before starting another.")
                          << "\n";
                return Result<void>();
            }
        }

        // Final summary
        std::cout << "\n" << ui::horizontal_rule(60, '=') << "\n";

        if (!resp.errors.empty()) {
            std::cout << ui::status_warning("Some repair operations failed:") << "\n";
            for (const auto& err : resp.errors) {
                std::cout << "  - " << err << "\n";
            }
        }

        // Per-operation results table
        if (!resp.operationResults.empty()) {
            std::cout << "\n";
            for (const auto& op : resp.operationResults) {
                std::vector<ui::Row> rows;
                rows.push_back({op.operation, "", ""});
                rows.push_back({"  Processed", std::to_string(op.processed), ""});
                rows.push_back({"  Succeeded", std::to_string(op.succeeded), ""});
                if (op.failed > 0)
                    rows.push_back(
                        {"  Failed", ui::colorize(std::to_string(op.failed), ui::Ansi::RED), ""});
                if (op.skipped > 0)
                    rows.push_back({"  Skipped", std::to_string(op.skipped), ""});
                if (!op.message.empty())
                    rows.push_back({"  Note", op.message, ""});
                ui::render_rows(std::cout, rows);
            }
        }

        // Overall
        std::cout << "\n  Total: " << resp.totalOperations << " operations, " << resp.totalSucceeded
                  << " succeeded, " << resp.totalFailed << " failed";
        if (resp.totalSkipped > 0)
            std::cout << ", " << resp.totalSkipped << " skipped";
        std::cout << "\n";

        if (resp.success) {
            std::cout << ui::status_ok("All repair operations completed successfully") << "\n";
        } else {
            std::cout << ui::status_warning("Repair completed with errors") << "\n";
        }

        return Result<void>();
    }

private:
    YamsCLI* cli_ = nullptr;
    bool repairOrphans_ = false;
    bool repairMime_ = false;
    bool repairChunks_ = false;
    bool repairEmbeddings_ = false;
    bool repairFts5_ = false;
    bool optimizeDb_ = false;
    bool repairDownloads_ = false;
    bool repairPathTree_ = false;
    bool repairBlockRefs_ = false;
    bool repairStuckDocs_ = false;
    bool foreground_ = false;
    std::vector<std::string> includeMime_;
    std::string embeddingModel_;
    bool repairAll_ = false;
    bool dryRun_ = false;
    bool verbose_ = false;
    bool force_ = false;
    int32_t maxRetries_ = 3;
};

// Factory function
std::unique_ptr<ICommand> createRepairCommand() {
    return std::make_unique<RepairCommand>();
}

} // namespace yams::cli
