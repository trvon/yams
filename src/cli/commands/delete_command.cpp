#include <nlohmann/json.hpp>
#include <chrono>
#include <cstdint>
#include <future>
#include <iostream>
#include <optional>
#include <sstream>
#include <vector>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <yams/app/services/services.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>

#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::cli {

class DeleteCommand : public ICommand {
private:
    // Member variables
    YamsCLI* cli_ = nullptr;
    std::string hash_;
    std::string name_;
    std::string names_;
    std::string pattern_;
    std::string directory_;
    std::vector<std::string> targets_;
    bool force_ = false;
    bool dryRun_ = false;
    bool keepRefs_ = false;
    bool verbose_ = false;
    bool recursive_ = false;
    bool jsonOutput_ = false;

public:
    std::string getName() const override { return "delete"; }

    std::string getDescription() const override {
        return "Delete documents by hash, name, or pattern";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("delete", getDescription());
        cmd->alias("rm"); // Add rm as alias for delete

        // Create option group for deletion methods (only one can be used at a time)
        auto* group = cmd->add_option_group("deletion_method");
        group->add_option("--hash", hash_, "Document hash to delete");
        group->add_option("--name", name_, "Delete document by name");
        group->add_option("--names", names_,
                          "Delete multiple documents by names (comma-separated)");
        group->add_option("--pattern", pattern_, "Delete documents matching pattern (e.g., *.log)");
        group->add_option("--directory", directory_, "Directory to delete (requires --recursive)");
        group->require_option(0);
        auto* positional =
            cmd->add_option("targets", targets_, "Targets to delete (name/path/pattern)");
        positional->expected(-1);

        // Flags (can be combined with any deletion method)
        cmd->add_flag("--force,-f,--no-confirm", force_, "Skip confirmation prompt");
        cmd->add_flag("--dry-run", dryRun_,
                      "Preview what would be deleted without actually deleting");
        cmd->add_flag("--keep-refs", keepRefs_, "Keep reference counts (don't decrement)");
        cmd->add_flag("-v,--verbose", verbose_, "Enable verbose output");
        cmd->add_flag("--recursive,-r", recursive_, "Delete directory contents recursively");
        cmd->add_flag("--json", jsonOutput_, "Output results as JSON");

        cmd->callback([this]() { cli_->setPendingCommand(this); });
    }

    Result<void> execute() override {
        auto prom = std::make_shared<std::promise<Result<void>>>();
        auto fut = prom->get_future();
        boost::asio::co_spawn(
            getExecutor(),
            [this, prom]() -> boost::asio::awaitable<void> {
                try {
                    auto r = co_await this->executeAsync();
                    prom->set_value(std::move(r));
                } catch (const std::exception& e) {
                    prom->set_value(Error{ErrorCode::InternalError, e.what()});
                } catch (...) {
                    prom->set_value(Error{ErrorCode::InternalError, "unknown delete error"});
                }
                co_return;
            },
            boost::asio::detached);
        return fut.get();
    }

    boost::asio::awaitable<Result<void>> executeAsync() override {
        try {
            // Infer deletion mode from positional targets if no explicit selector is provided
            if (hash_.empty() && name_.empty() && names_.empty() && pattern_.empty() &&
                directory_.empty() && !targets_.empty()) {
                auto has_wildcard = [](const std::string& s) {
                    return s.find('*') != std::string::npos || s.find('?') != std::string::npos;
                };
                // Treat explicit trailing '/' or --recursive as directory; otherwise treat as
                // name/path.
                auto has_trailing_slash = [](const std::string& s) {
                    return !s.empty() && (s.back() == '/' || s.back() == '\\');
                };

                if (targets_.size() == 1) {
                    const std::string& t = targets_.front();
                    // If -r is set, treat single target as a directory
                    if (recursive_) {
                        directory_ = t;
                    } else if (has_wildcard(t)) {
                        pattern_ = t;
                    } else if (has_trailing_slash(t)) {
                        directory_ = t;
                    } else {
                        name_ = t;
                    }
                } else {
                    // Multiple targets: treat as names list to mirror rm behavior
                    std::ostringstream oss;
                    for (size_t i = 0; i < targets_.size(); ++i) {
                        if (i > 0)
                            oss << ",";
                        oss << targets_[i];
                    }
                    names_ = oss.str();
                }
            }
            // Always try daemon-first for all deletion modes
            yams::daemon::DeleteRequest dreq;

            // Map all CLI options to daemon request
            dreq.hash = hash_;
            dreq.name = name_;

            // Parse comma-separated names
            if (!names_.empty()) {
                std::stringstream ss(names_);
                std::string name;
                while (std::getline(ss, name, ',')) {
                    dreq.names.push_back(name);
                }
            }

            dreq.pattern = pattern_;
            dreq.directory = directory_;
            dreq.purge = !keepRefs_;
            dreq.force = force_;
            dreq.dryRun = dryRun_;
            dreq.keepRefs = keepRefs_;
            dreq.recursive = recursive_;
            dreq.verbose = verbose_ || (cli_ && cli_->getVerbose());

            // Capture verbose flag by value to avoid referencing local variables after co_await
            bool verboseMode = dreq.verbose;
            bool forceMode = force_;
            bool jsonMode = jsonOutput_ || (cli_ && cli_->getJsonOutput());

            auto render = [this, verboseMode, forceMode,
                           jsonMode](const yams::daemon::DeleteResponse& response) {
                return renderDeleteResponse(response, verboseMode, forceMode, jsonMode);
            };

            try {
                yams::daemon::ClientConfig cfg;
                if (cli_->hasExplicitDataDir()) {
                    cfg.dataDir = cli_->getDataPath();
                }
                cfg.enableChunkedResponses = false;
                cfg.requestTimeout = std::chrono::milliseconds(30000);
                auto leaseRes = yams::cli::acquire_cli_daemon_client_shared_with_fallback(
                    cfg, yams::cli::CliDaemonAccessPolicy::AllowInProcessFallback);
                if (!leaseRes) {
                    co_return leaseRes.error();
                }
                auto leaseHandle = std::move(leaseRes.value());
                auto& client = **leaseHandle;
                auto result = co_await client.call(dreq);
                if (result) {
                    auto r = render(result.value());
                    if (!r)
                        co_return r.error();
                    co_return Result<void>();
                }
            } catch (...) {
            }

            // If daemon failed, try local execution
            co_return executeLocal();

        } catch (const std::exception& e) {
            co_return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

private:
    Result<void> renderDeleteResponse(const yams::daemon::DeleteResponse& resp, bool verboseMode,
                                      bool forceMode, bool jsonMode) const {
        if (jsonMode) {
            nlohmann::json j;
            j["dry_run"] = resp.dryRun;
            j["success_count"] = resp.successCount;
            j["failure_count"] = resp.failureCount;
            j["results"] = nlohmann::json::array();
            for (const auto& result : resp.results) {
                nlohmann::json item;
                item["name"] = result.name;
                item["hash"] = result.hash;
                item["success"] = result.success;
                if (!result.error.empty()) {
                    item["error"] = result.error;
                }
                j["results"].push_back(item);
            }
            std::cout << j.dump(2) << std::endl;
            return Result<void>();
        }

        if (resp.dryRun) {
            std::cout << "[DRY RUN] Documents that would be deleted:\n";
            for (const auto& result : resp.results) {
                std::cout << "  " << result.name;
                if (!result.hash.empty()) {
                    std::cout << " (" << result.hash.substr(0, 12) << "...)";
                }
                std::cout << "\n";
            }
            std::cout << "\nTotal: " << resp.results.size() << " document(s)\n";
            return Result<void>();
        }

        if (resp.successCount > 0) {
            std::cout << "Successfully deleted " << resp.successCount << " document(s)\n";
        }
        if (resp.failureCount > 0) {
            std::cout << "Failed to delete " << resp.failureCount << " document(s):\n";
            bool hasCorrupted = false;
            for (const auto& result : resp.results) {
                if (!result.success && !result.error.empty()) {
                    std::cout << "  " << result.name << ": " << result.error << "\n";
                    if (result.error.find("Corrupted data") != std::string::npos) {
                        hasCorrupted = true;
                    }
                }
            }
            if (hasCorrupted && !forceMode) {
                std::cout << "\nHint: Use --force to delete corrupted entries "
                             "(removes metadata even if content is corrupted)\n";
            }
        }

        if (verboseMode && resp.successCount > 0) {
            std::cout << "\nDeleted documents:\n";
            for (const auto& result : resp.results) {
                if (result.success) {
                    std::cout << "  " << result.name;
                    if (!result.hash.empty()) {
                        std::cout << " (" << result.hash.substr(0, 12) << "...)";
                    }
                    std::cout << "\n";
                }
            }
        }

        return Result<void>();
    }

    app::services::DeleteByNameRequest buildServiceDeleteRequest(bool dryRun) const {
        app::services::DeleteByNameRequest req;
        req.hash = hash_;
        req.name = name_;

        if (!names_.empty()) {
            std::stringstream ss(names_);
            std::string name;
            while (std::getline(ss, name, ',')) {
                yams::config::trim(name);
                if (!name.empty()) {
                    req.names.push_back(name);
                }
            }
        }

        req.pattern = pattern_;
        if (!directory_.empty()) {
            req.pattern = directory_;
            if (!req.pattern.empty() && req.pattern.back() != '/') {
                req.pattern += '/';
            }
            req.pattern += "*";
        }

        req.dryRun = dryRun;
        req.force = force_;
        req.keepRefs = keepRefs_;
        req.recursive = recursive_;
        req.verbose = verbose_ || (cli_ && cli_->getVerbose());
        return req;
    }

    yams::daemon::DeleteResponse
    toDaemonDeleteResponse(const app::services::DeleteByNameResponse& serviceResp) const {
        yams::daemon::DeleteResponse response;
        response.dryRun = serviceResp.dryRun;

        auto appendResult = [&](const app::services::DeleteByNameResult& deleteResult,
                                bool success) {
            yams::daemon::DeleteResponse::DeleteResult daemonResult;
            daemonResult.name = deleteResult.name;
            daemonResult.hash = deleteResult.hash;
            daemonResult.success = success;
            daemonResult.error = deleteResult.error.value_or("");
            if (success) {
                response.successCount++;
            } else {
                response.failureCount++;
            }
            response.results.push_back(std::move(daemonResult));
        };

        for (const auto& deleteResult : serviceResp.deleted) {
            appendResult(deleteResult, deleteResult.deleted);
        }
        for (const auto& errorResult : serviceResp.errors) {
            appendResult(errorResult, false);
        }

        return response;
    }

    static void printDeleteCandidates(const yams::daemon::DeleteResponse& response) {
        std::cout << "Documents to be deleted:\n";
        for (const auto& result : response.results) {
            std::cout << "  " << result.name;
            if (!result.hash.empty()) {
                std::cout << " (" << result.hash.substr(0, 12) << "...)";
            }
            std::cout << "\n";
        }
    }

    Result<void> executeLocal() {
        auto ensured = cli_->ensureStorageInitialized();
        if (!ensured) {
            return ensured;
        }

        if (hash_.empty() && name_.empty() && names_.empty() && pattern_.empty() &&
            directory_.empty()) {
            return Error{ErrorCode::InvalidArgument, "No deletion criteria specified"};
        }

        if (!directory_.empty() && !recursive_) {
            return Error{ErrorCode::InvalidArgument,
                         "Directory deletion requires --recursive flag for safety"};
        }

        auto appContext = cli_->getAppContext();
        if (!appContext) {
            return Error{ErrorCode::NotInitialized, "Application context not initialized"};
        }
        auto documentService = app::services::makeDocumentService(*appContext);

        const bool verboseMode = verbose_ || (cli_ && cli_->getVerbose());
        const bool jsonMode = jsonOutput_ || (cli_ && cli_->getJsonOutput());
        std::optional<yams::daemon::DeleteResponse> previewResponse;

        if (dryRun_ || verboseMode || !force_) {
            auto previewReq = buildServiceDeleteRequest(true);
            auto previewResult = documentService->deleteByName(previewReq);
            if (!previewResult) {
                return previewResult.error();
            }
            previewResponse = toDaemonDeleteResponse(previewResult.value());
            if (previewResponse->results.empty()) {
                if (jsonMode) {
                    return renderDeleteResponse(*previewResponse, verboseMode, force_, jsonMode);
                }
                std::cout << "No documents found matching the criteria.\n";
                return Result<void>();
            }
            if (dryRun_) {
                return renderDeleteResponse(*previewResponse, verboseMode, force_, jsonMode);
            }
            if (verboseMode) {
                printDeleteCandidates(*previewResponse);
            }
        }

        // Confirm deletion unless --force is used
        if (!force_) {
            const auto candidateCount = previewResponse ? previewResponse->results.size() : 0;
            uint64_t totalSize = 0;
            if (appContext->metadataRepo && previewResponse) {
                for (const auto& result : previewResponse->results) {
                    if (result.hash.empty()) {
                        continue;
                    }
                    auto docResult = appContext->metadataRepo->getDocumentByHash(result.hash);
                    if (docResult && docResult.value()) {
                        totalSize += docResult.value()->fileSize;
                    }
                }
            }

            std::string prompt = candidateCount == 1
                                     ? "Delete 1 document"
                                     : "Delete " + std::to_string(candidateCount) + " documents";

            if (totalSize > 0) {
                prompt += " (total size: " + ui::format_bytes(totalSize) + ")";
            }
            prompt += "? (y/N): ";

            std::cout << prompt;
            std::string response;
            std::getline(std::cin, response);
            if (response != "y" && response != "Y") {
                std::cout << "Deletion cancelled.\n";
                return Result<void>();
            }
        }

        auto deleteReq = buildServiceDeleteRequest(false);
        auto deleteResult = documentService->deleteByName(deleteReq);
        if (!deleteResult) {
            return deleteResult.error();
        }
        auto response = toDaemonDeleteResponse(deleteResult.value());
        if (response.results.empty()) {
            if (jsonMode) {
                return renderDeleteResponse(response, verboseMode, force_, jsonMode);
            }
            std::cout << "No documents found matching the criteria.\n";
            return Result<void>();
        }

        auto renderResult = renderDeleteResponse(response, verboseMode, force_, jsonMode);
        if (!renderResult) {
            return renderResult.error();
        }

        // Show storage stats if verbose
        if (verboseMode && response.successCount > 0 && appContext->store) {
            auto stats = appContext->store->getStats();
            std::cout << "\nStorage statistics:\n";
            std::cout << "  Total objects: " << stats.totalObjects << "\n";
            std::cout << "  Unique blocks: " << stats.uniqueBlocks << "\n";
            std::cout << "  Storage size: " << ui::format_bytes(stats.totalBytes) << "\n";
        }

        return response.failureCount > 0 && response.successCount == 0
                   ? Error{ErrorCode::InvalidOperation, "All deletions failed"}
                   : Result<void>();
    } // End of executeLocal()
}; // End of DeleteCommand class

// Factory function
std::unique_ptr<ICommand> createDeleteCommand() {
    return std::make_unique<DeleteCommand>();
}

} // namespace yams::cli
