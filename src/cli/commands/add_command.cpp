#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <regex>
#include <sstream>
#include <thread>
#include <utility>
#include <yams/api/content_metadata.h>
#include <yams/cli/command.h>
#include <yams/cli/error_hints.h>
#include <yams/cli/progress_indicator.h>
#include <yams/cli/ui_helpers.hpp>
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
#ifndef _WIN32
#include <unistd.h>
#else
#include <io.h>
#define STDOUT_FILENO 1
#endif

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

std::string scrubDaemonLoadMessage(std::string_view message) {
    std::string cleaned(message);
    const std::string needle(kDaemonLoadMessage);
    auto pos = cleaned.find(needle);
    if (pos != std::string::npos) {
        cleaned.erase(pos, needle.size());
    }
    cleaned = trimCopy(cleaned);
    if (cleaned.empty()) {
        cleaned = "Daemon rejected request";
    }
    return cleaned;
}

bool hasUnsupportedControlChars(std::string_view sv) {
    return std::any_of(sv.begin(), sv.end(), [](unsigned char ch) {
        return std::iscntrl(ch) && ch != '\n' && ch != '\r' && ch != '\t';
    });
}

bool isStdoutTty() {
#ifdef _WIN32
    return _isatty(STDOUT_FILENO) != 0;
#else
    return isatty(STDOUT_FILENO) != 0;
#endif
}

class SpinnerThread {
public:
    SpinnerThread() = default;
    ~SpinnerThread() { shutdown(); }
    SpinnerThread(const SpinnerThread&) = delete;
    SpinnerThread& operator=(const SpinnerThread&) = delete;

    void start(const std::string& message, bool showCount, size_t current = 0, size_t total = 0) {
        std::lock_guard<std::mutex> lock(mutex_);
        message_ = message;
        showCount_ = showCount;
        current_ = current;
        total_ = total;
        if (!spinner_) {
            spinner_.emplace(ProgressIndicator::Style::Spinner, true);
        }
        spinner_->setShowCount(showCount_);
        spinner_->start(message_);
        spinner_->update(current_, total_);
        ensureThread();
    }

    void setMessage(const std::string& message) {
        std::lock_guard<std::mutex> lock(mutex_);
        message_ = message;
        if (spinner_) {
            spinner_->setMessage(message_);
        }
    }

    void setShowCount(bool showCount) {
        std::lock_guard<std::mutex> lock(mutex_);
        showCount_ = showCount;
        if (spinner_) {
            spinner_->setShowCount(showCount_);
        }
    }

    void setCounts(size_t current, size_t total) {
        std::lock_guard<std::mutex> lock(mutex_);
        current_ = current;
        total_ = total;
        if (spinner_) {
            spinner_->update(current_, total_);
        }
    }

    void pause() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (spinner_) {
            spinner_->stop();
        }
    }

    void resume() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!spinner_) {
            return;
        }
        spinner_->setShowCount(showCount_);
        spinner_->start(message_);
        spinner_->update(current_, total_);
        ensureThread();
    }

    bool enabled() const { return spinner_.has_value(); }

private:
    void ensureThread() {
        if (running_) {
            return;
        }
        running_ = true;
        worker_ = std::thread([this]() {
            while (running_) {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (spinner_ && spinner_->isActive()) {
                        spinner_->update(current_, total_);
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        });
    }

    void shutdown() {
        running_ = false;
        if (worker_.joinable()) {
            worker_.join();
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (spinner_) {
            spinner_->stop();
        }
    }

    mutable std::mutex mutex_;
    std::optional<ProgressIndicator> spinner_;
    std::thread worker_;
    std::atomic<bool> running_{false};
    std::string message_;
    bool showCount_ = false;
    size_t current_ = 0;
    size_t total_ = 0;
};

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
// Note: initContext=false avoids triggering expensive storage initialization in daemon path
std::string getActiveSessionId(YamsCLI* cli, bool bypass, bool initContext = true) {
    if (bypass)
        return {};
    // Check environment variable first (fast path, no storage init needed)
    if (const char* envSession = std::getenv("YAMS_SESSION_CURRENT")) {
        if (*envSession) {
            return std::string(envSession);
        }
    }
    // Only initialize context if explicitly requested (avoids 40+ second delay in daemon path)
    if (!initContext) {
        return {};
    }
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

        // Sync extraction wait options
        cmd->add_flag(
            "--sync", waitForProcessing_,
            "Wait for text extraction to complete before returning (default timeout: 30s)");
        cmd->add_option("--sync-timeout", waitTimeoutSeconds_,
                        "Timeout for --sync mode (seconds, 0 = no timeout)")
            ->default_val(30)
            ->check(CLI::Range(0, 300));

        // Daemon interaction robustness controls (always use daemon for file/dir)
        cmd->add_option("--daemon-timeout-ms", daemonTimeoutMs_,
                        "Per-request timeout when talking to the daemon (ms)")
            ->default_val(5000);
        cmd->add_option("--daemon-retries", daemonRetries_,
                        "Retry attempts for transient daemon failures")
            ->default_val(3)
            ->check(CLI::Range(0, 10));
        cmd->add_option("--daemon-backoff-ms", daemonBackoffMs_,
                        "Initial backoff between retries (ms); doubles each attempt")
            ->default_val(250);
        cmd->add_option("--daemon-ready-timeout-ms", daemonReadyTimeoutMs_,
                        "Max time to wait for daemon readiness (ms, 0 to skip)")
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

                SpinnerThread daemonSpinner;
                if (!cli_->getJsonOutput() && isStdoutTty()) {
                    daemonSpinner.start("Preparing add", false);
                }

                // Get active session for session-isolated memory (PBI-082)
                // Use initContext=false to avoid expensive storage initialization in daemon path
                // Session ID is read from YAMS_SESSION_CURRENT env var (set by `yams session use`)
                std::string activeSessionId = getActiveSessionId(cli_, bypassSession_, false);

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
                    const auto readyTimeout =
                        std::chrono::milliseconds(std::max(0, daemonReadyTimeoutMs_));
                    if (readyTimeout.count() > 0) {
                        const auto deadline = std::chrono::steady_clock::now() + readyTimeout;
                        auto sleepFor = std::chrono::milliseconds(50);
                        bool ready = false;
                        while (std::chrono::steady_clock::now() < deadline) {
                            if (yams::daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
                                ready = true;
                                break;
                            }
                            auto now = std::chrono::steady_clock::now();
                            if (now >= deadline) {
                                break;
                            }
                            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                                deadline - now);
                            std::this_thread::sleep_for(std::min(sleepFor, remaining));
                            sleepFor = std::min(sleepFor * 2, std::chrono::milliseconds(500));
                        }
                        if (!ready) {
                            return Error{ErrorCode::Timeout, "Daemon did not become ready in time"};
                        }
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
                // Include patterns validated but not directly used here; daemon applies them
                (void)sanitizedIncludeRes.value();

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
                size_t successfulRequests = 0;
                std::vector<std::pair<std::filesystem::path, Error>> daemonFailures;
                size_t totalAdded = 0, totalUpdated = 0, totalSkipped = 0;
                const size_t totalDaemonRequests = singleFiles.size() + groupedPaths.size();
                size_t completedRequests = 0;
                if (daemonSpinner.enabled() && totalDaemonRequests > 0) {
                    daemonSpinner.setShowCount(true);
                    daemonSpinner.setMessage("Adding via daemon");
                    daemonSpinner.setCounts(0, totalDaemonRequests);
                }
                auto pauseProgress = [&]() { daemonSpinner.pause(); };
                auto resumeProgress = [&]() {
                    if (daemonSpinner.enabled() && completedRequests < totalDaemonRequests) {
                        daemonSpinner.resume();
                    }
                };

                auto render = [&](const yams::daemon::AddDocumentResponse& resp,
                                  const std::filesystem::path& path) -> void {
                    pauseProgress();
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
                               (resp.message.find("asynchronous") != std::string::npos ||
                                resp.message.find("deferred") != std::string::npos)) {
                        // Directory adds / deferred adds are processed asynchronously - show
                        // acknowledgment
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
                    resumeProgress();
                    // Note: Daemon handles FTS5/embedding indexing via PostIngestQueue.
                    // No local lightIndexForHash needed - it would trigger expensive
                    // local storage initialization (40+ seconds) for no benefit.
                };

                // Create a shared client and ingestion service for all adds
                auto sharedClientCfg = yams::daemon::ClientConfig{};
                if (cli_->hasExplicitDataDir()) {
                    sharedClientCfg.dataDir = cli_->getDataPath();
                }
                sharedClientCfg.enableChunkedResponses = false;
                sharedClientCfg.singleUseConnections = false;
                sharedClientCfg.requestTimeout =
                    std::chrono::milliseconds(std::max(1, daemonTimeoutMs_));
                auto sharedClient = std::make_shared<yams::daemon::DaemonClient>(sharedClientCfg);
                yams::app::services::DocumentIngestionService ing(sharedClient);

                // Helper to populate common AddOptions fields
                auto makeBaseOpts = [&]() -> yams::app::services::AddOptions {
                    yams::app::services::AddOptions aopts;
                    aopts.tags = sanitizedTags;
                    aopts.metadata = sanitizedMetadata;
                    aopts.excludePatterns = sanitizedExclude;
                    aopts.collection = sanitizedCollection;
                    aopts.snapshotId = sanitizedSnapshotId;
                    aopts.snapshotLabel = sanitizedSnapshotLabel;
                    aopts.sessionId = activeSessionId;
                    if (!sanitizedMimeType.empty())
                        aopts.mimeType = sanitizedMimeType;
                    aopts.disableAutoMime = disableAutoMime_;
                    aopts.noEmbeddings = noEmbeddings_;
                    aopts.waitForProcessing = waitForProcessing_;
                    aopts.waitTimeoutSeconds = waitTimeoutSeconds_;
                    if (cli_->hasExplicitDataDir())
                        aopts.explicitDataDir = cli_->getDataPath();
                    aopts.timeoutMs = daemonTimeoutMs_;
                    aopts.retries = daemonRetries_;
                    aopts.backoffMs = daemonBackoffMs_;
                    aopts.verify = verify_;
                    aopts.noGitignore = noGitignore_;
                    return aopts;
                };

                // Validate document name once
                std::string sanitizedName = trimCopy(documentName_);
                if (sanitizedName.size() > kMaxNameLength) {
                    return Error{ErrorCode::InvalidArgument,
                                 "Document name exceeds maximum length"};
                }
                if (hasUnsupportedControlChars(sanitizedName)) {
                    return Error{ErrorCode::InvalidArgument,
                                 "Document name contains unsupported control characters"};
                }

                // Build batch of AddOptions for single files
                std::vector<yams::app::services::AddOptions> fileBatch;
                fileBatch.reserve(singleFiles.size());
                for (const auto& filePath : singleFiles) {
                    auto aopts = makeBaseOpts();
                    aopts.path = filePath.string();
                    aopts.recursive = false;
                    aopts.name = sanitizedName;
                    fileBatch.push_back(std::move(aopts));
                }

                // Process single files in parallel batch
                if (!fileBatch.empty()) {
                    auto batchResult = ing.addBatch(fileBatch, 4);
                    for (size_t i = 0; i < batchResult.results.size(); ++i) {
                        completedRequests++;
                        const auto& result = batchResult.results[i];
                        if (result) {
                            totalAdded += result.value().documentsAdded;
                            totalUpdated += result.value().documentsUpdated;
                            totalSkipped += result.value().documentsSkipped;
                            render(result.value(), singleFiles[i]);
                            successfulRequests++;
                        } else {
                            pauseProgress();
                            const auto err = result.error();
                            const auto msg = scrubDaemonLoadMessage(err.message);
                            spdlog::warn("Daemon add failed for file '{}': {}",
                                         singleFiles[i].string(), msg);
                            resumeProgress();
                            daemonFailures.emplace_back(singleFiles[i],
                                                        Error{err.code, msg});
                        }
                        if (daemonSpinner.enabled()) {
                            daemonSpinner.setCounts(completedRequests, totalDaemonRequests);
                        }
                    }
                }

                // Build batch of AddOptions for directories
                std::vector<yams::app::services::AddOptions> dirBatch;
                std::vector<std::filesystem::path> dirPaths;
                for (const auto& [dir, files] : groupedPaths) {
                    if (dir.string() == "-") {
                        // Route stdin through daemon instead of falling back to
                        // expensive local service initialization (~40s).
                        std::string stdinContent((std::istreambuf_iterator<char>(std::cin)),
                                                 std::istreambuf_iterator<char>());
                        if (stdinContent.empty()) {
                            return Error{ErrorCode::InvalidArgument,
                                         "No content received from stdin"};
                        }
                        auto aopts = makeBaseOpts();
                        aopts.content = std::move(stdinContent);
                        aopts.name = sanitizedName.empty() ? "stdin" : sanitizedName;
                        auto result = ing.addViaDaemon(aopts);
                        if (result) {
                            totalAdded += result.value().documentsAdded;
                            totalUpdated += result.value().documentsUpdated;
                            totalSkipped += result.value().documentsSkipped;
                            render(result.value(), std::filesystem::path("-"));
                            successfulRequests++;
                        } else {
                            const auto err = result.error();
                            const auto msg = scrubDaemonLoadMessage(err.message);
                            daemonFailures.emplace_back(std::filesystem::path("-"),
                                                        Error{err.code, msg});
                        }
                        continue;
                    }
                    auto aopts = makeBaseOpts();
                    aopts.path = dir.string();
                    aopts.recursive = true;
                    aopts.includePatterns = files;
                    aopts.name = sanitizedName;
                    dirBatch.push_back(std::move(aopts));
                    dirPaths.push_back(dir);
                }

                // Process directories in parallel batch
                if (!dirBatch.empty()) {
                    auto batchResult = ing.addBatch(dirBatch, 4);
                    for (size_t i = 0; i < batchResult.results.size(); ++i) {
                        completedRequests++;
                        const auto& result = batchResult.results[i];
                        if (result) {
                            totalAdded += result.value().documentsAdded;
                            totalUpdated += result.value().documentsUpdated;
                            totalSkipped += result.value().documentsSkipped;
                            render(result.value(), dirPaths[i]);
                            successfulRequests++;
                        } else {
                            pauseProgress();
                            const auto err = result.error();
                            const auto msg = scrubDaemonLoadMessage(err.message);
                            spdlog::warn("Daemon add failed for directory '{}': {}",
                                         dirPaths[i].string(), msg);
                            resumeProgress();
                            daemonFailures.emplace_back(dirPaths[i], Error{err.code, msg});
                        }
                        if (daemonSpinner.enabled()) {
                            daemonSpinner.setCounts(completedRequests, totalDaemonRequests);
                        }
                    }
                }
                daemonSpinner.pause();

                const size_t daemonRequestsAttempted = successfulRequests + daemonFailures.size();
                if (daemonRequestsAttempted > 0) {
                    std::cout << "Summary: added=" << totalAdded << ", updated=" << totalUpdated
                              << ", skipped=" << totalSkipped;
                    if (!daemonFailures.empty()) {
                        std::cout << ", failed=" << daemonFailures.size();
                    }
                    std::cout << std::endl;
                }
                if (!daemonFailures.empty()) {
                    const auto& [failedPath, failure] = daemonFailures.front();
                    std::ostringstream oss;
                    oss << "Failed to add '" << failedPath.string() << "': " << failure.message;
                    return Error{failure.code, oss.str()};
                }

                if (daemonRequestsAttempted > 0) {
                    return Result<void>();
                }
            }

            // Fall back to service-based execution (no stdin — that's handled via daemon above)
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
        req.noEmbeddings = noEmbeddings_;

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
                std::cout << "  Bytes stored: " << ui::format_bytes(result.bytesStored)
                          << std::endl;
                std::cout << "  Bytes deduped: " << ui::format_bytes(result.bytesDeduped)
                          << std::endl;
            }
        }
    }

    // Member variables
    YamsCLI* cli_ = nullptr;

    // Sync extraction wait options (must be declared before registerCommand uses them)
    bool waitForProcessing_ = false;
    int waitTimeoutSeconds_ = 30;

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
    int daemonTimeoutMs_ = 5000; // AddDocument just pushes to queue, 5s is plenty
    int daemonRetries_ = 3;
    int daemonBackoffMs_ = 250;
    int daemonReadyTimeoutMs_ = 10000;
};

// Factory function
std::unique_ptr<ICommand> createAddCommand() {
    return std::make_unique<AddCommand>();
}

} // namespace yams::cli
