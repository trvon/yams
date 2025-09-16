#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <regex>
#include <sstream>
#include <thread>
#include <yams/api/content_metadata.h>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/detection/file_type_detector.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/embedding_service.h>
// App services for service-based architecture
#include <yams/app/services/services.hpp>
// Service façade for daemon-first document ingestion
#include <yams/app/services/document_ingestion_service.h>
// Daemon client API for daemon-first add
#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/response_of.hpp>
// Async helpers for interim non-blocking daemon operations
#include <future>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/system_executor.hpp>

namespace yams::cli {

using json = nlohmann::json;

class AddCommand : public ICommand {
public:
    std::string getName() const override { return "add"; }

    std::string getDescription() const override {
        return "Add document(s) or directory to the content store";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("add", getDescription());
        // Accept multiple paths; keep legacy single-path for backward compatibility
        cmd->add_option("paths", targetPaths_,
                        "File or directory paths to add (use '-' for stdin). Accepts multiple.");
        cmd->add_option("path", legacyTargetPath_,
                        "[Deprecated] Single path to file/directory (use '-' for stdin)");

        cmd->add_option("-n,--name", documentName_,
                        "Name for the document (especially useful for stdin)");
        cmd->add_option("-t,--tags", tags_, "Tags for the document (comma-separated)")
            ->delimiter(',');
        cmd->add_option("-m,--metadata", metadata_, "Metadata key=value pairs");
        cmd->add_option("--mime-type", mimeType_, "MIME type of the document");
        cmd->add_flag("--no-auto-mime", disableAutoMime_, "Disable automatic MIME type detection");
        cmd->add_flag("--no-embeddings", noEmbeddings_,
                      "Disable automatic embedding generation for added documents");

        // Daemon interaction robustness controls (always use daemon for file/dir)
        cmd->add_option("--daemon-timeout-ms", daemonTimeoutMs_,
                        "Per-request timeout when talking to the daemon (ms)")
            ->default_val(30000);
        cmd->add_option("--daemon-retries", daemonRetries_,
                        "Retry attempts for transient daemon failures")
            ->default_val(3)
            ->check(CLI::Range(0, 10));
        cmd->add_option("--daemon-backoff-ms", daemonBackoffMs_,
                        "Initial backoff between retries (ms); doubles each attempt")
            ->default_val(250);
        cmd->add_option("--daemon-ready-timeout-ms", daemonReadyTimeoutMs_,
                        "Max time to wait for daemon readiness (ms)")
            ->default_val(10000);

        // Collection and snapshot options
        cmd->add_option("-c,--collection", collection_, "Collection name for organizing documents");
        cmd->add_option("--snapshot-id", snapshotId_, "Unique snapshot identifier");
        cmd->add_option("--snapshot-label", snapshotLabel_, "User-friendly snapshot label");

        // Directory options
        cmd->add_flag("-r,--recursive", recursive_, "Recursively add files from directories");
        cmd->add_option("--include", includePatterns_,
                        "File patterns to include (e.g., '*.txt,*.md')")
            ->delimiter(',');
        cmd->add_option("--exclude", excludePatterns_,
                        "File patterns to exclude (e.g., '*.tmp,*.log')")
            ->delimiter(',');

        cmd->callback([this]() { cli_->setPendingCommand(this); });
    }

    Result<void> execute() override {
        try {
            // Attempt daemon-first add; fall back to service-based local execution
            {
                // Determine effective paths list (prefer multi-path if provided)
                std::vector<std::filesystem::path> paths;
                if (!targetPaths_.empty()) {
                    paths = targetPaths_;
                } else if (!legacyTargetPath_.empty()) {
                    paths.push_back(legacyTargetPath_);
                } else {
                    // Default to stdin when nothing provided
                    paths.push_back(std::filesystem::path("-"));
                }

                // If multiple paths include stdin, require it to be the only entry
                if (paths.size() > 1) {
                    for (const auto& p : paths) {
                        if (p.string() == "-") {
                            return Error{ErrorCode::InvalidArgument,
                                         "Stdin '-' cannot be combined with other paths"};
                        }
                    }
                }

                // Ensure daemon is running; auto-start if necessary
                std::filesystem::path effectiveSocket =
                    yams::daemon::DaemonClient::resolveSocketPathConfigFirst();
                if (!yams::daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
                    yams::daemon::ClientConfig startCfg;
                    if (cli_->hasExplicitDataDir()) {
                        startCfg.dataDir = cli_->getDataPath();
                    }
                    if (auto r = yams::daemon::DaemonClient::startDaemon(startCfg); !r) {
                        return Error{ErrorCode::InternalError,
                                     std::string("Failed to start daemon: ") + r.error().message};
                    }
                    // Wait for readiness with bounded backoff (up to ~3 seconds)
                    int waits[] = {100, 200, 400, 800, 1500};
                    bool ready = false;
                    for (int w : waits) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(w));
                        if (yams::daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
                            ready = true;
                            break;
                        }
                    }
                    if (!ready) {
                        return Error{ErrorCode::Timeout, "Daemon did not become ready in time"};
                    }
                }

                // Connectivity probe only: a successful Status call indicates dispatcher is up.
                // Do not block on full readiness; add path can operate while daemon is degraded.
                {
                    yams::daemon::ClientConfig cfg;
                    if (cli_->hasExplicitDataDir()) {
                        cfg.dataDir = cli_->getDataPath();
                    }
                    cfg.enableChunkedResponses = false;
                    cfg.requestTimeout = std::chrono::milliseconds(2000);
                    auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
                    if (!leaseRes) {
                        return leaseRes.error();
                    }
                    auto leaseHandle = std::move(leaseRes.value());
                    auto& client = **leaseHandle;

                    auto start = std::chrono::steady_clock::now();
                    for (;;) {
                        yams::daemon::StatusRequest sreq;
                        sreq.detailed = true;
                        std::promise<Result<yams::daemon::StatusResponse>> prom;
                        auto fut = prom.get_future();
                        auto work = [&, sreq]() -> boost::asio::awaitable<void> {
                            auto r = co_await client.call(sreq);
                            prom.set_value(std::move(r));
                            co_return;
                        };
                        boost::asio::co_spawn(boost::asio::system_executor{}, work(),
                                              boost::asio::detached);
                        if (fut.wait_for(std::chrono::milliseconds(2500)) ==
                            std::future_status::ready) {
                            auto st = fut.get();
                            if (st) {
                                break; // dispatcher reachable; proceed with add
                            }
                        }
                        if (std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - start)
                                .count() >= daemonReadyTimeoutMs_) {
                            break; // best-effort: proceed; daemon may return StatusResponse
                        }
                        std::this_thread::sleep_for(std::chrono::milliseconds(200));
                    }
                }

                // Process each path with per-path fallback; collect remaining for services
                size_t ok = 0, failed = 0;
                bool hasStdin = std::any_of(paths.begin(), paths.end(),
                                            [](const auto& p) { return p.string() == "-"; });
                std::vector<std::filesystem::path> serviceDirs;
                for (const auto& path : paths) {
                    if (path.string() == "-") {
                        continue; // handled later via services path (stdin)
                    }

                    // Build daemon add options
                    yams::app::services::AddOptions aopts;
                    aopts.path = path.string();
                    aopts.name = documentName_;
                    aopts.tags = tags_;
                    for (const auto& kv : metadata_) {
                        auto pos = kv.find('=');
                        if (pos != std::string::npos) {
                            std::string key = kv.substr(0, pos);
                            std::string value = kv.substr(pos + 1);
                            aopts.metadata[key] = value;
                        }
                    }
                    aopts.recursive = recursive_;
                    aopts.includePatterns = includePatterns_;
                    aopts.excludePatterns = excludePatterns_;
                    aopts.collection = collection_;
                    aopts.snapshotId = snapshotId_;
                    aopts.snapshotLabel = snapshotLabel_;
                    if (!mimeType_.empty()) {
                        aopts.mimeType = mimeType_;
                    }
                    aopts.disableAutoMime = disableAutoMime_;
                    aopts.noEmbeddings = noEmbeddings_;
                    if (cli_->hasExplicitDataDir()) {
                        aopts.explicitDataDir = cli_->getDataPath();
                    }
                    aopts.timeoutMs = daemonTimeoutMs_;
                    aopts.retries = daemonRetries_;
                    aopts.backoffMs = daemonBackoffMs_;

                    // Auto-detect directory targets and enable recursive ingestion when omitted
                    if (std::error_code dir_ec; std::filesystem::is_directory(path, dir_ec)) {
                        if (!aopts.recursive) {
                            aopts.recursive = true;
                            spdlog::info(
                                "'{}' is a directory — enabling recursive ingestion automatically.",
                                path.string());
                        }
                    }

                    auto render = [&](const yams::daemon::AddDocumentResponse& resp) -> void {
                        if (resp.documentsAdded == 1) {
                            std::cout << "Added document: " << resp.hash.substr(0, 16) << "..."
                                      << std::endl;
                        } else {
                            std::cout << "Added " << resp.documentsAdded << " documents"
                                      << std::endl;
                        }
                        // After a successful single-file add, perform lightweight
                        // text extraction + FTS index so search works immediately.
                        if (resp.documentsAdded == 1) {
                            if (auto appContext = cli_->getAppContext()) {
                                auto searchService = app::services::makeSearchService(*appContext);
                                if (searchService) {
                                    (void)searchService->lightIndexForHash(resp.hash);
                                }
                            }
                        }
                    };

                    // Retry with exponential backoff on transient failures
                    bool success = false;
                    std::string lastError;
                    {
                        yams::app::services::DocumentIngestionService ing;
                        auto result = ing.addViaDaemon(aopts);
                        if (result) {
                            render(result.value());
                            success = true;
                        } else {
                            lastError = result.error().message;
                        }
                    }

                    if (!success) {
                        if (std::error_code dir_ec; std::filesystem::is_directory(path, dir_ec)) {
                            spdlog::warn("Daemon add failed for directory '{}': {}. Falling back "
                                         "to local indexing service.",
                                         path.string(), lastError);
                            serviceDirs.push_back(path);
                        } else {
                            spdlog::error("Daemon add failed for '{}': {}.", path.string(),
                                          lastError);
                            failed++;
                        }
                    } else {
                        ok++;
                    }
                }

                // If there were any non-stdin paths processed, report summary here
                if (ok + failed > 0) {
                    std::cout << "Daemon add completed: " << ok << " ok, " << failed << " failed"
                              << std::endl;
                }
                // If there were any failures, abort before considering stdin
                if (failed > 0) {
                    return Error{ErrorCode::InternalError, "One or more adds failed via daemon"};
                }

                // Prepare remaining service work: stdin and any directories that failed via daemon
                if (hasStdin || !serviceDirs.empty()) {
                    targetPaths_.clear();
                    for (const auto& d : serviceDirs)
                        targetPaths_.push_back(d);
                    if (hasStdin)
                        targetPaths_.push_back(std::filesystem::path("-"));
                } else {
                    // Nothing left to do if we processed only non-stdin paths successfully
                    if (ok > 0 && failed == 0) {
                        return Result<void>();
                    }
                }
            }

            // Fall back to service-based execution for stdin and for any remaining paths
            return executeWithServices();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
        }
    }

    boost::asio::awaitable<Result<void>> executeAsync() override {
        // For now, call the existing execute() to reuse its logic; next pass will fully co_await
        co_return execute();
    }

private:
    Result<void> executeWithServices() {
        // Get app context and services
        auto appContext = cli_->getAppContext();
        if (!appContext) {
            return Error{ErrorCode::NotInitialized, "Failed to initialize app context"};
        }

        // Build effective path list
        std::vector<std::filesystem::path> paths;
        if (!targetPaths_.empty()) {
            paths = targetPaths_;
        } else if (!legacyTargetPath_.empty()) {
            paths.push_back(legacyTargetPath_);
        } else {
            paths.push_back(std::filesystem::path("-"));
        }

        // If stdin only
        if (paths.size() == 1 && paths[0].string() == "-") {
            return storeFromStdinWithServices(*appContext);
        }

        // Iterate and process each path
        size_t ok = 0, failed = 0, indexed = 0;
        for (const auto& p : paths) {
            if (p.string() == "-") {
                auto r = storeFromStdinWithServices(*appContext);
                if (r) {
                    ok++;
                } else {
                    failed++;
                }
                continue;
            }
            if (!std::filesystem::exists(p)) {
                spdlog::warn("Path not found: {}", p.string());
                failed++;
                continue;
            }
            if (std::filesystem::is_directory(p)) {
                auto r = storeDirectoryWithServices(*appContext, p);
                if (r) {
                    ok++;
                } else {
                    failed++;
                }
            } else {
                auto r = storeFileWithServices(*appContext, p);
                if (r) {
                    ok++;
                    indexed++;
                } else {
                    failed++;
                }
            }
        }

        if (!cli_->getJsonOutput()) {
            std::cout << "Completed add: " << ok << " ok, " << failed << " failed" << std::endl;
        }
        return Result<void>();
    }

    Result<void> storeFromStdinWithServices(const app::services::AppContext& appContext) {
        auto documentService = app::services::makeDocumentService(appContext);
        if (!documentService) {
            return Error{ErrorCode::NotInitialized, "Failed to create document service"};
        }

        // Read all content from stdin
        std::string content;
        std::string line;
        while (std::getline(std::cin, line)) {
            content += line + "\n";
        }

        if (content.empty()) {
            return Error{ErrorCode::InvalidArgument, "No content received from stdin"};
        }

        // Build service request
        app::services::StoreDocumentRequest req;
        req.content = content;
        req.name = documentName_.empty() ? "stdin" : documentName_;
        req.tags = tags_;
        req.noEmbeddings = noEmbeddings_;
        req.collection = collection_;
        req.snapshotId = snapshotId_;
        req.snapshotLabel = snapshotLabel_;
        // Keep fallback fast and resilient: defer extraction to background tooling
        req.deferExtraction = true;

        // Parse metadata key=value pairs
        for (const auto& kv : metadata_) {
            auto pos = kv.find('=');
            if (pos != std::string::npos) {
                std::string key = kv.substr(0, pos);
                std::string value = kv.substr(pos + 1);
                req.metadata[key] = value;
            }
        }

        if (!mimeType_.empty()) {
            req.mimeType = mimeType_;
        }
        req.disableAutoMime = disableAutoMime_;

        auto result = documentService->store(req);
        if (!result) {
            return result.error();
        }

        // Output result
        outputServiceResult(result.value());
        // Immediate light index so search can find it
        if (auto appContext2 = cli_->getAppContext()) {
            auto searchService = app::services::makeSearchService(*appContext2);
            if (searchService) {
                (void)searchService->lightIndexForHash(result.value().hash);
            }
        }
        return Result<void>();
    }

    Result<void> storeFileWithServices(const app::services::AppContext& appContext,
                                       const std::filesystem::path& filePath) {
        auto documentService = app::services::makeDocumentService(appContext);
        if (!documentService) {
            return Error{ErrorCode::NotInitialized, "Failed to create document service"};
        }

        // Build service request
        app::services::StoreDocumentRequest req;
        req.path = filePath.string();
        req.tags = tags_;
        req.noEmbeddings = noEmbeddings_;
        req.collection = collection_;
        req.snapshotId = snapshotId_;
        req.snapshotLabel = snapshotLabel_;
        // Defer extraction for fast CLI fallback and to avoid plugin-related crashes
        req.deferExtraction = true;

        // Parse metadata key=value pairs
        for (const auto& kv : metadata_) {
            auto pos = kv.find('=');
            if (pos != std::string::npos) {
                std::string key = kv.substr(0, pos);
                std::string value = kv.substr(pos + 1);
                req.metadata[key] = value;
            }
        }

        if (!mimeType_.empty()) {
            req.mimeType = mimeType_;
        }
        req.disableAutoMime = disableAutoMime_;

        auto result = documentService->store(req);
        if (!result) {
            return result.error();
        }

        // Output result
        outputServiceResult(result.value());
        // Immediate light index so search can find it
        if (auto appContext2 = cli_->getAppContext()) {
            auto searchService = app::services::makeSearchService(*appContext2);
            if (searchService) {
                (void)searchService->lightIndexForHash(result.value().hash);
            }
        }
        return Result<void>();
    }

    Result<void> storeDirectoryWithServices(const app::services::AppContext& appContext,
                                            const std::filesystem::path& dirPath) {
        if (!recursive_) {
            return Error{ErrorCode::InvalidArgument, "Directory specified but --recursive not set"};
        }

        // Use IndexingService for directory operations
        auto indexingService = app::services::makeIndexingService(appContext);
        if (!indexingService) {
            return Error{ErrorCode::NotInitialized, "Failed to create indexing service"};
        }

        // Build IndexingService request
        app::services::AddDirectoryRequest req;
        req.directoryPath = dirPath.string();
        req.collection = collection_;
        req.includePatterns = includePatterns_;
        req.excludePatterns = excludePatterns_;
        req.recursive = recursive_;
        // Match daemon behavior: directory ingestion defers extraction
        req.deferExtraction = true;

        // Parse metadata key=value pairs
        for (const auto& kv : metadata_) {
            auto pos = kv.find('=');
            if (pos != std::string::npos) {
                std::string key = kv.substr(0, pos);
                std::string value = kv.substr(pos + 1);
                req.metadata[key] = value;
            }
        }

        auto result = indexingService->addDirectory(req);
        if (!result) {
            return result.error();
        }

        // Output summary
        const auto& resp = result.value();
        if (cli_->getJsonOutput() || cli_->getVerbose()) {
            json output;
            output["files_processed"] = resp.filesProcessed;
            output["files_indexed"] = resp.filesIndexed;
            output["files_skipped"] = resp.filesSkipped;
            output["files_failed"] = resp.filesFailed;
            output["directory"] = resp.directoryPath;
            output["collection"] = resp.collection;

            if (cli_->getVerbose()) {
                json files = json::array();
                for (const auto& file : resp.results) {
                    json fileObj;
                    fileObj["path"] = file.path;
                    fileObj["hash"] = file.hash;
                    fileObj["size"] = file.sizeBytes;
                    fileObj["success"] = file.success;
                    if (file.error) {
                        fileObj["error"] = file.error.value();
                    }
                    files.push_back(fileObj);
                }
                output["files"] = files;
            }

            std::cout << output.dump(2) << std::endl;
        } else {
            std::cout << "Added " << resp.filesIndexed << " files from directory"
                      << " (" << resp.filesSkipped << " skipped, " << resp.filesFailed << " failed)"
                      << std::endl;
        }

        // Perform light indexing for each successfully added file
        if (auto appContext2 = cli_->getAppContext()) {
            auto searchService = app::services::makeSearchService(*appContext2);
            if (searchService) {
                for (const auto& f : resp.results) {
                    if (f.success && !f.hash.empty()) {
                        (void)searchService->lightIndexForHash(f.hash);
                    }
                }
            }
        }

        return Result<void>();
    }

    void outputServiceResult(const app::services::StoreDocumentResponse& result) {
        if (cli_->getJsonOutput()) {
            json output;
            output["hash"] = result.hash;
            output["bytes_stored"] = result.bytesStored;
            output["bytes_deduped"] = result.bytesDeduped;
            std::cout << output.dump(2) << std::endl;
        } else {
            std::cout << "Added document: " << result.hash.substr(0, 16) << "..." << std::endl;
            if (cli_->getVerbose()) {
                std::cout << "  Bytes stored: " << formatSize(result.bytesStored) << std::endl;
                std::cout << "  Bytes deduped: " << formatSize(result.bytesDeduped) << std::endl;
            }
        }
    }

    std::string formatSize(uint64_t bytes) const {
        const char* units[] = {"B", "KB", "MB", "GB", "TB"};
        int unitIndex = 0;
        double size = static_cast<double>(bytes);
        while (size >= 1024 && unitIndex < 4) {
            size /= 1024;
            unitIndex++;
        }
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << size << " " << units[unitIndex];
        return oss.str();
    }

    // Member variables
    YamsCLI* cli_ = nullptr;
    // Multi-path support
    std::vector<std::filesystem::path> targetPaths_;
    std::filesystem::path legacyTargetPath_;
    std::string documentName_;
    std::vector<std::string> tags_;
    std::vector<std::string> metadata_;
    std::string mimeType_;
    bool disableAutoMime_ = false;
    bool noEmbeddings_ = false;

    // Collection and snapshot options
    std::string collection_;
    std::string snapshotId_;
    std::string snapshotLabel_;

    // Directory options
    bool recursive_ = false;
    std::vector<std::string> includePatterns_;
    std::vector<std::string> excludePatterns_;

    // Daemon interaction controls
    int daemonTimeoutMs_ = 30000;
    int daemonRetries_ = 3;
    int daemonBackoffMs_ = 250;
    int daemonReadyTimeoutMs_ = 10000;
};

// Factory function
std::unique_ptr<ICommand> createAddCommand() {
    return std::make_unique<AddCommand>();
}

} // namespace yams::cli
