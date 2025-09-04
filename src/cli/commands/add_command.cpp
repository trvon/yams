#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <regex>
#include <sstream>
#include <yams/api/content_metadata.h>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/detection/file_type_detector.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/embedding_service.h>
// App services for service-based architecture
#include <yams/app/services/services.hpp>
// Daemon client API for daemon-first add
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/async_bridge.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/response_of.hpp>

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
        cmd->add_option("-t,--tags", tags_, "Tags for the document (comma-separated)");
        cmd->add_option("-m,--metadata", metadata_, "Metadata key=value pairs");
        cmd->add_option("--mime-type", mimeType_, "MIME type of the document");
        cmd->add_flag("--no-auto-mime", disableAutoMime_, "Disable automatic MIME type detection");
        cmd->add_flag("--no-embeddings", noEmbeddings_,
                      "Disable automatic embedding generation for added documents");

        // Collection and snapshot options
        cmd->add_option("-c,--collection", collection_, "Collection name for organizing documents");
        cmd->add_option("--snapshot-id", snapshotId_, "Unique snapshot identifier");
        cmd->add_option("--snapshot-label", snapshotLabel_, "User-friendly snapshot label");

        // Directory options
        cmd->add_flag("-r,--recursive", recursive_, "Recursively add files from directories");
        cmd->add_option("--include", includePatterns_,
                        "File patterns to include (e.g., '*.txt,*.md')");
        cmd->add_option("--exclude", excludePatterns_,
                        "File patterns to exclude (e.g., '*.tmp,*.log')");

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Command failed: {}", result.error().message);
                throw CLI::RuntimeError(1);
            }
        });
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

                // Process each path; use daemon-first for non-stdin targets
                bool any_daemon_ok = false;
                for (const auto& path : paths) {
                    if (path.string() == "-") {
                        continue; // handled later via services path
                    }

                    yams::daemon::AddDocumentRequest dreq;
                    dreq.path = std::filesystem::absolute(path).string();
                    dreq.name = documentName_;
                    dreq.tags = tags_;

                    // Parse metadata key=value pairs
                    for (const auto& kv : metadata_) {
                        auto pos = kv.find('=');
                        if (pos != std::string::npos) {
                            std::string key = kv.substr(0, pos);
                            std::string value = kv.substr(pos + 1);
                            dreq.metadata[key] = value;
                        }
                    }

                    dreq.recursive = recursive_;
                    dreq.includePatterns = includePatterns_;
                    dreq.excludePatterns = excludePatterns_;
                    dreq.collection = collection_;
                    dreq.snapshotId = snapshotId_;
                    dreq.snapshotLabel = snapshotLabel_;
                    if (!mimeType_.empty()) {
                        dreq.mimeType = mimeType_;
                    }
                    dreq.disableAutoMime = disableAutoMime_;

                    auto render =
                        [&](const yams::daemon::AddDocumentResponse& resp) -> Result<void> {
                        // Display results
                        if (resp.documentsAdded == 1) {
                            std::cout << "Added document: " << resp.hash.substr(0, 16) << "..."
                                      << std::endl;
                        } else {
                            std::cout << "Added " << resp.documentsAdded << " documents"
                                      << std::endl;
                        }
                        return Result<void>();
                    };

                    auto fallback = [&]() -> Result<void> {
                        // Fall back to service-based local execution
                        return executeWithServices();
                    };

                    try {
                        yams::daemon::ClientConfig cfg;
                        cfg.dataDir = cli_->getDataPath();
                        cfg.enableChunkedResponses = false; // small unary response
                        cfg.singleUseConnections = true;
                        cfg.requestTimeout = std::chrono::milliseconds(30000);
                        yams::daemon::DaemonClient client(cfg);
                        auto result = run_sync(client.call(dreq), std::chrono::seconds(30));
                        if (result) {
                            auto r = render(result.value());
                            if (!r) return r.error();
                            any_daemon_ok = true;
                            continue;
                        }
                    } catch (...) {
                        // fall through to fallback
                    }
                }
                if (any_daemon_ok && (targetPaths_.empty() ||
                                      (targetPaths_.size() == 1 && targetPaths_[0].string() != "-"))) {
                    return Result<void>();
                }
            }

            // Fall back to service-based execution for stdin and for any remaining paths
            return executeWithServices();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
        }
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
};

// Factory function
std::unique_ptr<ICommand> createAddCommand() {
    return std::make_unique<AddCommand>();
}

} // namespace yams::cli
