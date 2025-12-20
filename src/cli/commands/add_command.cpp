#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
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
#include <yams/app/services/session_service.hpp>
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

namespace {

std::string trimCopy(std::string_view sv) {
    std::size_t start = 0;
    std::size_t end = sv.size();
    while (start < end && std::isspace(static_cast<unsigned char>(sv[start]))) {
        ++start;
    }
    while (end > start && std::isspace(static_cast<unsigned char>(sv[end - 1]))) {
        --end;
    }
    return std::string(sv.substr(start, end - start));
}

bool hasUnsupportedControlChars(std::string_view sv) {
    return std::any_of(sv.begin(), sv.end(), [](unsigned char ch) {
        return std::iscntrl(ch) && ch != '\n' && ch != '\r' && ch != '\t';
    });
}

constexpr std::size_t kMaxPatternLength = 512;
constexpr std::size_t kMaxTagLength = 128;
constexpr std::size_t kMaxMetadataKeyLength = 256;
constexpr std::size_t kMaxMetadataValueLength = 4096;
constexpr std::size_t kMaxCollectionLength = 256;
constexpr std::size_t kMaxSnapshotLength = 512;
constexpr std::size_t kMaxMimeTypeLength = 256;
constexpr std::size_t kMaxNameLength = 256;

Result<std::vector<std::string>> sanitizeStringList(const std::vector<std::string>& raw,
                                                    const char* label, std::size_t maxLen) {
    std::vector<std::string> cleaned;
    cleaned.reserve(raw.size());
    for (const auto& entry : raw) {
        std::string trimmed = trimCopy(entry);
        if (trimmed.empty())
            continue;
        if (trimmed.size() > maxLen) {
            std::ostringstream oss;
            oss << label << " exceeds maximum length of " << maxLen << " characters";
            return Error{ErrorCode::InvalidArgument, oss.str()};
        }
        if (hasUnsupportedControlChars(trimmed)) {
            std::ostringstream oss;
            oss << label << " contains unsupported control characters";
            return Error{ErrorCode::InvalidArgument, oss.str()};
        }
        cleaned.push_back(std::move(trimmed));
    }
    return cleaned;
}

Result<std::map<std::string, std::string>>
sanitizeMetadata(const std::vector<std::string>& metadataRaw) {
    std::map<std::string, std::string> out;
    for (const auto& kv : metadataRaw) {
        auto pos = kv.find('=');
        if (pos == std::string::npos) {
            return Error{ErrorCode::InvalidArgument,
                         "Metadata entries must be formatted as key=value"};
        }
        std::string key = trimCopy(std::string_view(kv).substr(0, pos));
        std::string value = trimCopy(std::string_view(kv).substr(pos + 1));
        if (key.empty()) {
            return Error{ErrorCode::InvalidArgument, "Metadata key cannot be empty"};
        }
        if (key.size() > kMaxMetadataKeyLength) {
            return Error{ErrorCode::InvalidArgument, "Metadata key exceeds maximum length"};
        }
        if (value.size() > kMaxMetadataValueLength) {
            return Error{ErrorCode::InvalidArgument, "Metadata value exceeds maximum length"};
        }
        if (hasUnsupportedControlChars(key) || hasUnsupportedControlChars(value)) {
            return Error{ErrorCode::InvalidArgument,
                         "Metadata entries cannot contain control characters"};
        }
        out[key] = value;
    }
    return out;
}

// Helper to get active session ID (returns empty string if no active session or bypassed)
std::string getActiveSessionId(YamsCLI* cli, bool bypass) {
    if (bypass)
        return {};
    auto appContext = cli->getAppContext();
    if (!appContext)
        return {};
    auto sessionSvc = app::services::makeSessionService(appContext.get());
    if (!sessionSvc)
        return {};
    auto currentSession = sessionSvc->current();
    if (!currentSession)
        return {};
    auto state = sessionSvc->getState(*currentSession);
    if (state != app::services::SessionState::Active)
        return {};
    return *currentSession;
}

} // namespace

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
        cmd->add_option("-m,--metadata", metadata_, "Metadata key=value pairs (comma-separated)")
            ->delimiter(',');
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
        cmd->add_flag("--verify", verify_,
                      "Verify stored content (hash + existence) after add; slower but safer");
        cmd->add_flag("--no-gitignore", noGitignore_,
                      "Ignore .gitignore patterns when adding files recursively");

        // Session isolation options (PBI-082)
        cmd->add_flag("--global,--no-session", bypassSession_,
                      "Add to global memory (bypass active session)");

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

                // Get active session for session-isolated memory (PBI-082)
                std::string activeSessionId = getActiveSessionId(cli_, bypassSession_);

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

                auto sanitizedTagsRes = sanitizeStringList(tags_, "Tag", kMaxTagLength);
                if (!sanitizedTagsRes)
                    return sanitizedTagsRes.error();
                const auto& sanitizedTags = sanitizedTagsRes.value();

                auto sanitizedIncludeRes =
                    sanitizeStringList(includePatterns_, "Include pattern", kMaxPatternLength);
                if (!sanitizedIncludeRes)
                    return sanitizedIncludeRes.error();
                const auto& sanitizedInclude = sanitizedIncludeRes.value();

                auto sanitizedExcludeRes =
                    sanitizeStringList(excludePatterns_, "Exclude pattern", kMaxPatternLength);
                if (!sanitizedExcludeRes)
                    return sanitizedExcludeRes.error();
                const auto& sanitizedExclude = sanitizedExcludeRes.value();

                auto sanitizedMetadataRes = sanitizeMetadata(metadata_);
                if (!sanitizedMetadataRes)
                    return sanitizedMetadataRes.error();
                const auto& sanitizedMetadata = sanitizedMetadataRes.value();

                std::string sanitizedCollection = trimCopy(collection_);
                if (hasUnsupportedControlChars(sanitizedCollection)) {
                    return Error{ErrorCode::InvalidArgument,
                                 "Collection contains unsupported control characters"};
                }
                if (sanitizedCollection.size() > kMaxCollectionLength) {
                    return Error{ErrorCode::InvalidArgument, "Collection name exceeds limit"};
                }
                std::string sanitizedSnapshotId = trimCopy(snapshotId_);
                std::string sanitizedSnapshotLabel = trimCopy(snapshotLabel_);
                if (hasUnsupportedControlChars(sanitizedSnapshotId) ||
                    hasUnsupportedControlChars(sanitizedSnapshotLabel)) {
                    return Error{ErrorCode::InvalidArgument,
                                 "Snapshot identifiers cannot contain control characters"};
                }
                if (sanitizedSnapshotId.size() > kMaxSnapshotLength ||
                    sanitizedSnapshotLabel.size() > kMaxSnapshotLength) {
                    return Error{ErrorCode::InvalidArgument,
                                 "Snapshot identifiers exceed maximum length"};
                }
                std::string sanitizedMimeType = trimCopy(mimeType_);
                if (sanitizedMimeType.size() > kMaxMimeTypeLength) {
                    return Error{ErrorCode::InvalidArgument, "MIME type exceeds maximum length"};
                }

                // Separate single files from directories for proper handling
                // Single files should be added directly (to respect --name),
                // directories use include patterns
                std::vector<std::filesystem::path> singleFiles;
                std::map<std::filesystem::path, std::vector<std::string>> groupedPaths;
                for (const auto& p : paths) {
                    if (p.string() == "-") {
                        groupedPaths[p].push_back("-");
                    } else if (std::filesystem::is_directory(p)) {
                        groupedPaths[p];
                    } else {
                        // Single file - add directly to preserve --name behavior
                        singleFiles.push_back(p);
                    }
                }

                // Process each group
                size_t ok = 0, failed = 0;
                size_t totalAdded = 0, totalUpdated = 0, totalSkipped = 0;
                bool hasStdin = false;

                auto render = [&](const yams::daemon::AddDocumentResponse& resp,
                                  const std::filesystem::path& path) -> void {
                    if (resp.documentsAdded > 0 || resp.documentsUpdated > 0) {
                        std::cout << "From " << path.string() << ": added=" << resp.documentsAdded
                                  << ", updated=" << resp.documentsUpdated
                                  << ", skipped=" << resp.documentsSkipped << std::endl;
                        if (!resp.hash.empty()) {
                            std::cout << "  Hash: " << resp.hash << std::endl;
                        }
                        if (!resp.message.empty()) {
                            std::cout << "  Note: " << resp.message << std::endl;
                        }
                    } else if (!resp.message.empty() &&
                               resp.message.find("asynchronous") != std::string::npos) {
                        // Directory adds are processed asynchronously - show acknowledgment
                        std::cout << "From " << path.string() << ": " << resp.message << std::endl;
                        if (!resp.hash.empty()) {
                            std::cout << "  Hash: " << resp.hash << std::endl;
                        }
                    } else {
                        // Document already exists (skipped) - still show the hash if available
                        std::cout << "No new or updated documents from " << path.string()
                                  << std::endl;
                        if (!resp.hash.empty()) {
                            std::cout << "  Hash: " << resp.hash << std::endl;
                        }
                    }

                    if (resp.documentsAdded > 0) {
                        if (auto appContext = cli_->getAppContext()) {
                            auto searchService = app::services::makeSearchService(*appContext);
                            if (searchService) {
                                (void)searchService->lightIndexForHash(resp.hash);
                            }
                        }
                    }
                };

                // Process single files first (these respect --name directly)
                for (const auto& filePath : singleFiles) {
                    yams::app::services::AddOptions aopts;
                    aopts.path = filePath.string();
                    aopts.recursive = false; // Single file, not recursive
                    aopts.name = trimCopy(documentName_);
                    if (aopts.name.size() > kMaxNameLength) {
                        return Error{ErrorCode::InvalidArgument,
                                     "Document name exceeds maximum length"};
                    }
                    if (hasUnsupportedControlChars(aopts.name)) {
                        return Error{ErrorCode::InvalidArgument,
                                     "Document name contains unsupported control characters"};
                    }
                    aopts.tags = sanitizedTags;
                    aopts.metadata = sanitizedMetadata;
                    aopts.excludePatterns = sanitizedExclude;
                    aopts.collection = sanitizedCollection;
                    aopts.snapshotId = sanitizedSnapshotId;
                    aopts.snapshotLabel = sanitizedSnapshotLabel;
                    aopts.sessionId = activeSessionId;
                    if (!sanitizedMimeType.empty()) {
                        aopts.mimeType = sanitizedMimeType;
                    }
                    aopts.disableAutoMime = disableAutoMime_;
                    aopts.noEmbeddings = noEmbeddings_;
                    if (cli_->hasExplicitDataDir()) {
                        aopts.explicitDataDir = cli_->getDataPath();
                    }
                    aopts.timeoutMs = daemonTimeoutMs_;
                    aopts.retries = daemonRetries_;
                    aopts.backoffMs = daemonBackoffMs_;
                    aopts.verify = verify_;
                    aopts.noGitignore = noGitignore_;

                    yams::app::services::DocumentIngestionService ing;
                    auto result = ing.addViaDaemon(aopts);
                    if (result) {
                        totalAdded += result.value().documentsAdded;
                        totalUpdated += result.value().documentsUpdated;
                        totalSkipped += result.value().documentsSkipped;
                        render(result.value(), filePath);
                        ok++;
                    } else {
                        spdlog::warn("Daemon add failed for file '{}': {}", filePath.string(),
                                     result.error().message);
                        failed++;
                    }
                }

                // Process directories
                for (const auto& [dir, files] : groupedPaths) {
                    if (dir.string() == "-") {
                        hasStdin = true;
                        continue; // Handle stdin via local services later
                    }

                    yams::app::services::AddOptions aopts;
                    aopts.path = dir.string();
                    aopts.recursive = true; // A directory is being processed
                    aopts.includePatterns = files;
                    aopts.name = trimCopy(documentName_);
                    if (aopts.name.size() > kMaxNameLength) {
                        return Error{ErrorCode::InvalidArgument,
                                     "Document name exceeds maximum length"};
                    }
                    if (hasUnsupportedControlChars(aopts.name)) {
                        return Error{ErrorCode::InvalidArgument,
                                     "Document name contains unsupported control characters"};
                    }
                    aopts.tags = sanitizedTags;
                    aopts.metadata = sanitizedMetadata;
                    aopts.excludePatterns = sanitizedExclude;
                    aopts.collection = sanitizedCollection;
                    aopts.snapshotId = sanitizedSnapshotId;
                    aopts.snapshotLabel = sanitizedSnapshotLabel;
                    aopts.sessionId = activeSessionId;
                    if (!sanitizedMimeType.empty()) {
                        aopts.mimeType = sanitizedMimeType;
                    }
                    aopts.disableAutoMime = disableAutoMime_;
                    aopts.noEmbeddings = noEmbeddings_;
                    if (cli_->hasExplicitDataDir()) {
                        aopts.explicitDataDir = cli_->getDataPath();
                    }
                    aopts.timeoutMs = daemonTimeoutMs_;
                    aopts.retries = daemonRetries_;
                    aopts.backoffMs = daemonBackoffMs_;
                    aopts.verify = verify_;
                    aopts.noGitignore = noGitignore_;

                    yams::app::services::DocumentIngestionService ing;
                    auto result = ing.addViaDaemon(aopts);
                    if (result) {
                        totalAdded += result.value().documentsAdded;
                        totalUpdated += result.value().documentsUpdated;
                        totalSkipped += result.value().documentsSkipped;
                        render(result.value(), dir);
                        ok++;
                    } else {
                        spdlog::warn("Daemon add failed for directory '{}': {}",
                                     dir.string(), result.error().message);
                        failed++;
                    }
                }

                // If there were any non-stdin paths processed, report summary here
                if (ok + failed > 0) {
                    std::cout << "Daemon add completed: " << ok << " ok, " << failed << " failed";
                    if (totalAdded == 0 && totalUpdated == 0) {
                        std::cout << " (no changes)";
                    } else {
                        std::cout << ", added: " << totalAdded << ", updated: " << totalUpdated
                                  << ", skipped: " << totalSkipped;
                    }
                    std::cout << std::endl;
                }
                // If there were any failures, abort
                if (failed > 0) {
                    return Error{ErrorCode::InternalError, "One or more adds failed via daemon"};
                }

                // Handle stdin if present
                if (hasStdin) {
                    targetPaths_.clear();
                    targetPaths_.push_back(std::filesystem::path("-"));
                } else {
                    // Nothing left to do if we processed files/directories successfully
                    if (ok > 0) {
                        return Result<void>();
                    }
                }
            }

            // Fall back to service-based execution for stdin
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
            // Surface storage init hint (e.g., FTS5 repair guidance)
            auto initHint = cli_->ensureStorageInitialized();
            if (!initHint) {
                return Error{initHint.error().code,
                             std::string("Failed to initialize app context: ") +
                                 initHint.error().message};
            }
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
        size_t ok = 0, failed = 0;
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

        // Read all content from stdin (preserve bytes/newlines)
        std::string content((std::istreambuf_iterator<char>(std::cin)),
                            std::istreambuf_iterator<char>());

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
        req.sessionId = getActiveSessionId(cli_, bypassSession_);

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
        req.sessionId = getActiveSessionId(cli_, bypassSession_);

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
        req.verify = verify_;

        // Session-isolated memory (PBI-082): tag documents with active session
        req.sessionId = getActiveSessionId(cli_, bypassSession_);

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
            std::cout << "Processed " << resp.filesProcessed << " files"
                      << " (" << resp.filesIndexed << " indexed, " << resp.filesSkipped
                      << " skipped, " << resp.filesFailed << " failed)" << std::endl;
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
        auto size = static_cast<double>(bytes);
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
    bool verify_ = false;
    bool noGitignore_ = false;

    // Session isolation options (PBI-082)
    bool bypassSession_ = false;

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
