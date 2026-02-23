#include <spdlog/spdlog.h>
#include <yams/app/services/factory.hpp>
#include <yams/app/services/list_input_resolver.hpp>
#include <yams/app/services/retrieval_service.h>
#include <yams/app/services/services.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/time_parser.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/detection/file_type_detector.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#if defined(YAMS_HAS_STD_FORMAT) && YAMS_HAS_STD_FORMAT
#include <format>
namespace yamsfmt = std;
#else
#include <spdlog/fmt/fmt.h>
namespace yamsfmt = fmt;
#endif
#include <nlohmann/json.hpp>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace yams::cli {

namespace fs = std::filesystem;
using json = nlohmann::json;
using yams::app::services::utils::normalizeLookupPath;

class ListCommand : public ICommand {
public:
    std::string getName() const override { return "list"; }

    std::string getDescription() const override { return "List stored documents"; }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("list", getDescription());
        cmd->alias("ls"); // Add ls as alias for list

        cmd->add_option("--name,-p,--pattern", namePattern_,
                        "Filter by name/path pattern (supports wildcards: * and ?)");
        // Positional pattern argument (uses same variable as --name/--pattern)
        auto* positional = cmd->add_option("positional_pattern", positionalName_,
                                           "Name or path pattern (positional argument)");
        positional->type_name("PATTERN");

        cmd->add_option("--format", format_, "Output format: table, json, csv, minimal")
            ->default_val("table")
            ->check(CLI::IsMember({"table", "json", "csv", "minimal"}));
        cmd->add_flag("--json", jsonFlag_, "Output in JSON format (shorthand for --format json)");

        cmd->add_option("--sort", sortBy_, "Sort by: name, size, date, hash")
            ->default_val("date")
            ->check(CLI::IsMember({"name", "size", "date", "hash"}));

        cmd->add_flag("--reverse,-r", reverse_, "Reverse sort order");
        cmd->add_option("--limit,-n", limit_, "Limit number of results")->default_val(100);
        cmd->add_option("--offset", offset_, "Offset for pagination")->default_val(0);
        cmd->add_option("--recent", recentCount_, "Show N most recent documents");
        cmd->add_flag("-v,--verbose", verbose_, "Show detailed information");
        cmd->add_flag("--paths-only", pathsOnly_,
                      "Output only file paths, one per line (useful for scripting)");

        // New options for enhanced metadata display
        cmd->add_flag("--show-snippets", showSnippets_, "Show content previews (default: true)")
            ->default_val(true);
        cmd->add_flag("--show-metadata", showMetadata_, "Show all metadata for each document");
        cmd->add_option("--metadata-fields", metadataFieldsRaw_,
                        "Metadata fields to show as columns (comma-separated, default: "
                        "task,pbi,phase,owner,source)")
            ->default_val(metadataFieldsRaw_);
        cmd->add_flag("--no-metadata-fields", noMetadataFields_,
                      "Hide metadata columns from list output");
        cmd->add_flag("--show-tags", showTags_, "Show document tags (default: true)")
            ->default_val(true);
        cmd->add_flag("--group-by-session", groupBySession_, "Group documents by time periods");
        cmd->add_option("--snippet-length", snippetLength_, "Length of content snippets")
            ->default_val(50);
        cmd->add_flag("--no-snippets", noSnippets_, "Disable content previews");
        cmd->add_option("--metadata-values", metadataValuesRaw_,
                        "Show unique metadata values with counts for keys (comma-separated)");
        cmd->add_option("--metadata-values-limit", metadataValuesLimit_,
                        "Maximum number of metadata values to show per key (default: 10000)")
            ->default_val(10000);
        cmd->add_flag("--all-metadata-values", allMetadataValues_,
                      "Show all metadata values (override default limit)");

        // Metadata filters (key/value)
        cmd->add_option("--pbi", pbiFilters_, "Filter by PBI (metadata key 'pbi')")->take_all();
        cmd->add_option("--task", taskFilters_, "Filter by task (metadata key 'task')")->take_all();
        cmd->add_option("--phase", phaseFilters_, "Filter by phase (metadata key 'phase')")
            ->take_all();
        cmd->add_option("--owner", ownerFilters_, "Filter by owner (metadata key 'owner')")
            ->take_all();
        cmd->add_option("--metadata", metadataFiltersRaw_,
                        "Additional metadata filters key=value (repeatable)")
            ->take_all();
        cmd->add_flag("--match-any-metadata", matchAnyMetadata_,
                      "Use OR across metadata filters (default AND)");

        // File type filters
        cmd->add_option("--type", fileType_,
                        "Filter by file type (image, document, archive, audio, video, text, "
                        "executable, binary)");
        cmd->add_option("--mime", mimeType_,
                        "Filter by MIME type (e.g., image/jpeg, application/pdf)");
        cmd->add_option("--extension", extensions_,
                        "Filter by file extension(s), comma-separated (e.g., .jpg,.png)");
        cmd->add_flag("--binary", binaryOnly_, "Show only binary files");
        cmd->add_flag("--text", textOnly_, "Show only text files");

        // Tag filtering
        cmd->add_option("--tags,-t", filterTags_,
                        "Filter by tags (comma-separated, e.g., 'task,important')");
        cmd->add_flag("--match-all-tags", matchAllTags_,
                      "Require all specified tags to match (default: match any)");

        // Session filtering
        cmd->add_option("--session,-s", sessionFilter_,
                        "Filter by session ID (documents added during that session)");

        // Time filters
        cmd->add_option("--created-after", createdAfter_,
                        "Show files created after this time (ISO 8601, relative like '7d', or "
                        "natural like 'yesterday')");
        cmd->add_option("--created-before", createdBefore_, "Show files created before this time");
        cmd->add_option("--modified-after", modifiedAfter_, "Show files modified after this time");
        cmd->add_option("--modified-before", modifiedBefore_,
                        "Show files modified before this time");
        cmd->add_option("--indexed-after", indexedAfter_, "Show files indexed after this time");
        cmd->add_option("--indexed-before", indexedBefore_, "Show files indexed before this time");

        // Change tracking options
        cmd->add_flag("--changes", showChanges_,
                      "Show documents with recent modifications (last 24h)");
        cmd->add_option("--since", sinceTime_,
                        "Show documents changed since specified time (ISO 8601, relative like "
                        "'7d', or natural like 'yesterday')");
        cmd->add_flag("--diff-tags", showDiffTags_,
                      "Show documents grouped by change type (added, modified, deleted)");
        cmd->add_flag("--show-deleted", showDeleted_,
                      "Include documents that have been deleted from filesystem");
        cmd->add_option("--change-window", changeWindow_,
                        "Time window for considering files as 'recently changed' (default: 24h)")
            ->default_val("24h");

        // Snapshot operations (Task 043-05b)
        cmd->add_flag("--snapshots", listSnapshots_, "List all available snapshots");
        cmd->add_option("--snapshot-id", snapshotId_,
                        "Filter by snapshot ID or show file at snapshot");
        cmd->add_option("--compare-to", compareTo_,
                        "Compare file with another snapshot (requires --snapshot-id)");

        cmd->add_flag(
            "--no-streaming",
            [this](bool v) {
                if (v)
                    enableStreaming_ = false;
            },
            "Disable streaming responses from daemon");

        cmd->callback([this]() {
            // Handle snippet flag logic
            if (noSnippets_) {
                showSnippets_ = false;
            }

            if (namePattern_.empty() && !positionalName_.empty()) {
                namePattern_ = positionalName_;
            }
            if (!namePattern_.empty()) {
                auto normalized = normalizeLookupPath(namePattern_);
                if (normalized.changed && !normalized.hasWildcards) {
                    namePattern_ = normalized.normalized;
                    namePatternWasNormalized_ = true;
                }
            }

            auto result = execute();
            if (!result) {
                spdlog::error("List failed: {}", result.error().message);
                throw CLI::RuntimeError(1);
            }
        });
    }

    Result<void> execute() override {
        try {
            if (jsonFlag_)
                format_ = "json";
            updateMetadataFields();
            if (!metadataValuesRaw_.empty()) {
                return listMetadataValues();
            }
            // Task 043-05b: Smart snapshot operations
            if (listSnapshots_) {
                return listAllSnapshots();
            }

            // Detect if pattern looks like a file path
            bool isFilePath = isFilePathPattern(namePattern_);

            if (isFilePath && !snapshotId_.empty() && !compareTo_.empty()) {
                return showFileDiff(namePattern_, snapshotId_, compareTo_);
            }

            if (isFilePath && !snapshotId_.empty()) {
                return showFileAtSnapshot(namePattern_, snapshotId_);
            }

            if (isFilePath) {
                // Check if the path is a directory
                std::filesystem::path p(namePattern_);
                std::error_code ec;
                if (std::filesystem::exists(p, ec) && std::filesystem::is_directory(p, ec)) {
                    // It's a directory - use it as a path prefix pattern for listing
                    // Don't call showFileHistory for directories
                    isFilePath = false;
                    // Convert directory path to a pattern for filtering
                    std::filesystem::path absPath = std::filesystem::absolute(p, ec);
                    if (!ec) {
                        namePattern_ = absPath.string() + "/";
                    }
                } else {
                    // It's a file path - show history
                    return showFileHistory(namePattern_);
                }
            }

            if (!snapshotId_.empty()) {
                return listFilesInSnapshot(snapshotId_);
            }

            // Always try daemon-first approach with full protocol mapping for PBI-001 compliance
            yams::app::services::ListOptions dreq;

            // Map all CLI options to daemon protocol fields
            // Basic pagination and sorting
            dreq.limit = limit_;
            dreq.offset = offset_;
            dreq.recentCount = recentCount_;
            dreq.recent = (recentCount_ > 0) || (limit_ <= 100); // backward compatibility

            // Format and display options
            dreq.format = format_;
            dreq.sortBy = sortBy_;
            dreq.reverse = reverse_;
            dreq.verbose = verbose_ || cli_->getVerbose();
            dreq.pathsOnly = pathsOnly_;
            dreq.showSnippets = showSnippets_ && !noSnippets_;
            dreq.snippetLength = snippetLength_;
            dreq.showMetadata = showMetadata_ || !metadataFields_.empty();
            dreq.showTags = showTags_;
            dreq.groupBySession = groupBySession_;
            dreq.noSnippets = noSnippets_;

            // File type filters
            dreq.fileType = fileType_;
            dreq.mimeType = mimeType_;
            dreq.extensions = extensions_;
            dreq.binaryOnly = binaryOnly_;
            dreq.textOnly = textOnly_;

            // Time filters
            dreq.createdAfter = createdAfter_;
            dreq.createdBefore = createdBefore_;
            dreq.modifiedAfter = modifiedAfter_;
            dreq.modifiedBefore = modifiedBefore_;
            dreq.indexedAfter = indexedAfter_;
            dreq.indexedBefore = indexedBefore_;

            // Change tracking
            dreq.showChanges = showChanges_;
            dreq.sinceTime = sinceTime_;
            dreq.showDiffTags = showDiffTags_;
            dreq.showDeleted = showDeleted_;
            dreq.changeWindow = changeWindow_;

            // Tag filtering
            dreq.tags = {}; // Keep empty for backward compatibility
            dreq.filterTags = filterTags_;
            dreq.matchAllTags = matchAllTags_;

            // Metadata filters (pbi/task/phase/owner + arbitrary key=value)
            auto setLast = [](const std::vector<std::string>& src, const std::string& key,
                              std::map<std::string, std::string>& dest) {
                if (!src.empty()) {
                    dest[key] = src.back();
                }
            };

            std::map<std::string, std::string> metadataFilters;
            setLast(pbiFilters_, "pbi", metadataFilters);
            setLast(taskFilters_, "task", metadataFilters);
            setLast(phaseFilters_, "phase", metadataFilters);
            setLast(ownerFilters_, "owner", metadataFilters);

            for (const auto& kv : metadataFiltersRaw_) {
                auto pos = kv.find('=');
                if (pos == std::string::npos) {
                    return Error{ErrorCode::InvalidArgument, "Metadata filters must be key=value"};
                }
                std::string key = kv.substr(0, pos);
                std::string value = kv.substr(pos + 1);
                config::trim(key);
                config::trim(value);
                if (key.empty()) {
                    return Error{ErrorCode::InvalidArgument, "Metadata key cannot be empty"};
                }
                metadataFilters[key] = value;
            }

            dreq.metadataFilters = std::move(metadataFilters);
            dreq.matchAllMetadata = !matchAnyMetadata_;

            // Session filtering
            if (!sessionFilter_.empty()) {
                dreq.sessionId = sessionFilter_;
            }

            // Name pattern filtering (detect local file path and normalize)
            if (!namePattern_.empty()) {
                auto resolved = yams::app::services::resolveNameToPatternIfLocalFile(namePattern_);
                dreq.namePattern = resolved.pattern;
                resolvedLocalFilePath_ = resolved.isLocalFile ? resolved.absPath : std::nullopt;
            }

            bool showSpinner = !cli_->getJsonOutput() && !pathsOnly_ && format_ != "json" &&
                               format_ != "csv" && format_ != "minimal";
            std::optional<ui::SpinnerRunner> spinner;
            if (showSpinner) {
                spinner.emplace();
                spinner->start("Listing documents...");
            }

            auto render = [&](const yams::daemon::ListResponse& resp) -> Result<void> {
                if (spinner) {
                    spinner->stop();
                }
                // Handle paths-only output
                if (pathsOnly_) {
                    for (const auto& e : resp.items) {
                        std::cout << e.path << std::endl;
                    }
                    return Result<void>();
                }

                // Convert daemon response to EnhancedDocumentInfo for display compatibility
                std::vector<EnhancedDocumentInfo> documents;

                for (const auto& e : resp.items) {
                    EnhancedDocumentInfo doc;

                    // Map daemon ListEntry to EnhancedDocumentInfo
                    doc.info.fileName = e.fileName.empty() ? e.name : e.fileName;
                    doc.info.filePath = e.path;
                    doc.info.sha256Hash = e.hash;
                    doc.info.fileExtension = e.extension;
                    doc.info.fileSize = e.size;
                    doc.info.mimeType = e.mimeType;

                    // Convert timestamps (seconds precision)
                    doc.info.createdTime =
                        std::chrono::sys_seconds{std::chrono::seconds{e.created}};
                    doc.info.modifiedTime =
                        std::chrono::sys_seconds{std::chrono::seconds{e.modified}};
                    doc.info.indexedTime =
                        std::chrono::sys_seconds{std::chrono::seconds{e.indexed}};

                    // Content and metadata
                    doc.contentSnippet = e.snippet;
                    doc.language = e.language;
                    doc.extractionMethod = e.extractionMethod;
                    doc.extractionStatus = e.extractionStatus;
                    doc.hasContent = !e.snippet.empty();

                    // Handle metadata and tags
                    for (const auto& [key, value] : e.metadata) {
                        metadata::MetadataValue metaVal;
                        metaVal.value = value;
                        doc.metadata[key] = metaVal;
                    }
                    doc.tags = e.tags;

                    documents.push_back(doc);
                }

                // Handle diff-tags grouping if requested
                if (showDiffTags_) {
                    outputDiffTags(documents);
                    return Result<void>();
                }

                // Output results - respect global --json flag
                std::string effectiveFormat = format_;
                if (effectiveFormat == "table" && cli_->getJsonOutput()) {
                    effectiveFormat = "json";
                }

                if (effectiveFormat == "json") {
                    outputJson(documents);
                } else if (effectiveFormat == "csv") {
                    outputCsv(documents);
                } else if (effectiveFormat == "minimal") {
                    outputMinimal(documents);
                } else {
                    outputTable(documents);
                }

                return Result<void>();
            };

            // Use RetrievalService (daemon-first). On failure, fallback to service path
            {
                yams::app::services::RetrievalService rsvc;
                yams::app::services::RetrievalOptions ropts;
                if (cli_->hasExplicitDataDir()) {
                    ropts.explicitDataDir = cli_->getDataPath();
                }
                ropts.enableStreaming = enableStreaming_;
                ropts.progressiveOutput = false;
                ropts.singleUseConnections = false;
                ropts.requestTimeoutMs = 30000;
                ropts.headerTimeoutMs = 30000;
                ropts.bodyTimeoutMs = 120000;
                auto res = rsvc.list(dreq, ropts);
                if (res) {
                    auto r = render(res.value());
                    if (!r)
                        return r;
                    return Result<void>();
                }
                if (spinner) {
                    spinner->stop();
                }
                spdlog::warn("list: daemon path failed ({}); using local services",
                             res.error().message);
            }

            return executeWithServices(&spinner);

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

private:
    // Track normalized local file path if user passed a concrete file via --name
    std::optional<std::string> resolvedLocalFilePath_;

    /**
     * Check if pattern looks like a file path (contains / or . or known prefixes)
     * Excludes glob patterns that start with wildcards like *.md
     */
    bool isFilePathPattern(const std::string& pattern) {
        if (pattern.empty())
            return false;

        if (pattern.starts_with("*") || pattern.starts_with("?")) {
            return false;
        }

        bool hasPathSeparator =
            pattern.find('/') != std::string::npos || pattern.find('\\') != std::string::npos;
        bool hasExtension = pattern.find('.') != std::string::npos;
        bool hasKnownPrefix = pattern.starts_with("src/") || pattern.starts_with("include/") ||
                              pattern.starts_with("docs/") || pattern.starts_with("tests/");

        if (hasPathSeparator || hasKnownPrefix) {
            return true;
        }

        if (hasExtension && !pattern.starts_with(".") && pattern.find('*') == std::string::npos &&
            pattern.find('?') == std::string::npos) {
            return true;
        }

        return false;
    }

    /**
     * List all available snapshots from both tree_snapshots and document metadata tables.
     * This merges snapshots from two sources:
     * 1. tree_snapshots - rich metadata from directory indexing (yams add --recursive)
     * 2. document metadata - snapshot_id from daemon file ingestion (yams add <files>)
     */
    Result<void> listAllSnapshots() {
        auto appContext = cli_->getAppContext();
        if (!appContext) {
            return Error{ErrorCode::NotInitialized, "App context not available"};
        }

        try {
            auto& metaRepo = appContext->metadataRepo;

            // Get rich tree snapshots first
            auto treeResult = metaRepo->listTreeSnapshots(limit_ > 0 ? limit_ : 100);
            if (!treeResult)
                return treeResult.error();

            std::vector<metadata::TreeSnapshotRecord> snapshots = std::move(treeResult.value());

            // Build set of known snapshot IDs from tree_snapshots
            std::unordered_set<std::string> knownIds;
            for (const auto& rec : snapshots) {
                knownIds.insert(rec.snapshotId);
            }

            // Get additional snapshots from document metadata table
            auto metaSnapshotsResult = metaRepo->getSnapshots();
            if (metaSnapshotsResult) {
                // Collect unknown snapshot IDs for batch query (eliminates N+1 pattern)
                std::vector<std::string> unknownIds;
                for (const auto& snapshotId : metaSnapshotsResult.value()) {
                    if (knownIds.find(snapshotId) == knownIds.end()) {
                        unknownIds.push_back(snapshotId);
                    }
                }

                // Single batch query instead of N individual calls
                if (!unknownIds.empty()) {
                    auto batchResult = metaRepo->batchGetSnapshotInfo(unknownIds);
                    if (batchResult) {
                        for (const auto& [snapshotId, info] : batchResult.value()) {
                            metadata::TreeSnapshotRecord derived;
                            derived.snapshotId = snapshotId;
                            derived.metadata["directory_path"] = info.directoryPath;
                            derived.metadata["snapshot_label"] = info.label;
                            derived.metadata["git_commit"] = info.gitCommit;
                            derived.fileCount = info.fileCount;
                            derived.createdTime = info.createdTime;
                            snapshots.push_back(std::move(derived));
                            knownIds.insert(snapshotId);
                        }
                    } else {
                        // Fallback: create empty records for unknown snapshots
                        for (const auto& snapshotId : unknownIds) {
                            metadata::TreeSnapshotRecord derived;
                            derived.snapshotId = snapshotId;
                            derived.metadata["directory_path"] = "";
                            derived.fileCount = 0;
                            derived.createdTime = 0;
                            snapshots.push_back(std::move(derived));
                            knownIds.insert(snapshotId);
                        }
                    }
                }
            }

            // Sort by created time descending (most recent first)
            std::sort(
                snapshots.begin(), snapshots.end(),
                [](const metadata::TreeSnapshotRecord& a, const metadata::TreeSnapshotRecord& b) {
                    return a.createdTime > b.createdTime;
                });

            // Apply limit after merging
            if (limit_ > 0 && snapshots.size() > static_cast<size_t>(limit_)) {
                snapshots.resize(static_cast<size_t>(limit_));
            }

            // Output snapshots
            if (format_ == "json" || cli_->getJsonOutput()) {
                json j = json::array();
                for (const auto& rec : snapshots) {
                    json snap;
                    snap["snapshot_id"] = rec.snapshotId;
                    snap["directory_path"] = rec.metadata.count("directory_path")
                                                 ? rec.metadata.at("directory_path")
                                                 : "";
                    std::string label = rec.metadata.count("snapshot_label")
                                            ? rec.metadata.at("snapshot_label")
                                            : "";
                    if (!label.empty())
                        snap["label"] = label;
                    std::string commit =
                        rec.metadata.count("git_commit") ? rec.metadata.at("git_commit") : "";
                    if (!commit.empty())
                        snap["git_commit"] = commit;
                    std::string branch =
                        rec.metadata.count("git_branch") ? rec.metadata.at("git_branch") : "";
                    if (!branch.empty())
                        snap["git_branch"] = branch;
                    snap["files_count"] = rec.fileCount;
                    snap["created_at"] = rec.createdTime;
                    j.push_back(snap);
                }
                std::cout << j.dump(2) << std::endl;
            } else {
                // Table format
                std::cout << yamsfmt::format("{:<28} {:<40} {:<20} {:<10} {:<8}\n", "SNAPSHOT ID",
                                             "DIRECTORY", "LABEL", "GIT", "FILES");
                std::cout << std::string(110, '-') << "\n";

                for (const auto& rec : snapshots) {
                    std::string path = rec.metadata.count("directory_path")
                                           ? rec.metadata.at("directory_path")
                                           : "";
                    std::string label = rec.metadata.count("snapshot_label")
                                            ? rec.metadata.at("snapshot_label")
                                            : "";
                    std::string commit =
                        rec.metadata.count("git_commit") ? rec.metadata.at("git_commit") : "";

                    std::string shortCommit = commit.empty() ? "-" : commit.substr(0, 8);
                    std::string displayLabel = label.empty() ? "-" : label;
                    std::string shortPath =
                        path.length() > 38 ? "..." + path.substr(path.length() - 35) : path;

                    std::cout << yamsfmt::format(
                        "{:<28} {:<40} {:<20} {:<10} {:>8}\n", rec.snapshotId.substr(0, 26),
                        shortPath, displayLabel.substr(0, 18), shortCommit, rec.fileCount);
                }
            }

            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         std::string("Failed to list snapshots: ") + e.what()};
        }
    }

    /**
     * Show file history across all snapshots using daemon protocol
     */
    Result<void> showFileHistory(const std::string& filepath) {
        spdlog::info("Showing file history for: {}", filepath);

        try {
            // Create daemon client
            daemon::ClientConfig config;
            if (cli_ && cli_->hasExplicitDataDir()) {
                config.dataDir = cli_->getDataPath();
            }
            daemon::DaemonClient client(config);

            // Prepare request
            daemon::FileHistoryRequest req;
            req.filepath = filepath;

            // Execute via co_spawn with promise/future
            std::promise<Result<daemon::FileHistoryResponse>> promise;
            auto future = promise.get_future();

            boost::asio::co_spawn(
                getExecutor(),
                [&client, req,
                 promise = std::move(promise)]() mutable -> boost::asio::awaitable<void> {
                    auto result = co_await client.fileHistory(req);
                    promise.set_value(std::move(result));
                    co_return;
                },
                boost::asio::detached);

            // Wait for result with timeout
            if (future.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
                return Error{ErrorCode::Timeout, "File history request timed out"};
            }

            auto response = future.get();
            if (!response) {
                return Error{response.error().code,
                             "File history failed: " + response.error().message};
            }

            const auto& history = response.value();

            // Extract filename for display
            std::filesystem::path p(history.filepath);
            std::string filename = p.filename().string();

            // Display results with enhanced UI
            std::cout << ui::section_header("File History: " + filename) << "\n";
            std::cout << ui::key_value("Path", history.filepath, ui::Ansi::CYAN, 12) << "\n\n";

            if (!history.found || history.versions.empty()) {
                std::cout << ui::status_warning(history.message.empty()
                                                    ? "File not found in any snapshot."
                                                    : history.message)
                          << "\n\n";

                std::cout << ui::colorize("Tip:", ui::Ansi::BOLD)
                          << " Add file to a snapshot with:\n";
                std::cout << ui::indent("yams add --snapshot-id <id> " + filepath, 2) << "\n";
                return Result<void>();
            }

            // Show count with color
            std::string countMsg = "Found " + ui::format_number(history.totalVersions) +
                                   " version" + (history.totalVersions > 1 ? "s" : "") +
                                   " across snapshots";
            std::cout << ui::status_ok(countMsg) << "\n\n";

            // Enhanced table with ui_helpers
            ui::Table table;
            table.headers = {"VERSION", "SNAPSHOT", "HASH", "SIZE", "INDEXED"};
            table.has_header = true;

            for (size_t i = 0; i < history.versions.size(); ++i) {
                const auto& version = history.versions[i];

                // Version number (most recent = 1)
                std::string versionNum =
                    i == 0
                        ? ui::colorize("#" + std::to_string(i + 1) + " (current)", ui::Ansi::GREEN)
                        : ui::colorize("#" + std::to_string(i + 1), ui::Ansi::DIM);

                // Truncate snapshot ID
                std::string snapDisplay = version.snapshotId.length() > 22
                                              ? version.snapshotId.substr(0, 22) + "..."
                                              : version.snapshotId;

                // Truncate hash for display
                std::string hashDisplay =
                    version.hash.length() > 14 ? version.hash.substr(0, 14) + "..." : version.hash;

                // Format size
                std::string sizeStr = ui::format_bytes(version.size);

                // Format timestamp
                std::time_t tt = version.indexedTimestamp;
                std::tm* tm = std::localtime(&tt);
                char timeBuffer[32];
                std::strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M", tm);

                table.add_row(
                    {versionNum, snapDisplay, hashDisplay, sizeStr, std::string(timeBuffer)});
            }

            // Render table using ui_helpers
            ui::render_table(std::cout, table);

            // Show helpful tips
            std::cout << "\n";
            std::cout << ui::colorize("Tips:", ui::Ansi::BOLD) << "\n";
            std::cout << ui::bullet("Retrieve a specific version: " +
                                        ui::colorize("yams get --hash <hash>", ui::Ansi::CYAN),
                                    2)
                      << "\n";
            std::cout << ui::bullet("Compare versions: " +
                                        ui::colorize("yams diff <hash1> <hash2>", ui::Ansi::CYAN),
                                    2)
                      << "\n";

            if (!history.message.empty() &&
                history.message.find("showing first") != std::string::npos) {
                std::cout << "\n" << ui::status_info(history.message) << "\n";
            }

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         "Failed to get file history: " + std::string(e.what())};
        }
    }

    /**
     * Show file at specific snapshot
     */
    Result<void> showFileAtSnapshot(const std::string& filepath, const std::string& snapshotId) {
        spdlog::info("Showing file {} at snapshot {}", filepath, snapshotId);
        std::cout << "File-at-snapshot feature coming soon!\n";
        std::cout << "This will show: " << filepath << " at snapshot " << snapshotId << "\n";
        return Result<void>();
    }

    /**
     * Show inline diff between two snapshots for a file
     */
    Result<void> showFileDiff(const std::string& filepath, const std::string& snapshotA,
                              const std::string& snapshotB) {
        spdlog::info("Showing diff for {} between {} and {}", filepath, snapshotA, snapshotB);
        std::cout << "File diff feature coming soon!\n";
        std::cout << "This will show changes in: " << filepath << "\n";
        std::cout << "Between: " << snapshotA << " and " << snapshotB << "\n";
        return Result<void>();
    }

    /**
     * List all files in a specific snapshot
     */
    Result<void> listFilesInSnapshot(const std::string& snapshotId) {
        spdlog::info("Listing files in snapshot: {}", snapshotId);
        std::cout << "List-files-in-snapshot feature coming soon!\n";
        std::cout << "This will show all files in snapshot: " << snapshotId << "\n";
        return Result<void>();
    }

    Result<void> executeWithServices(std::optional<ui::SpinnerRunner>* spinner = nullptr) {
        try {
            auto ensured = cli_->ensureStorageInitialized();
            if (!ensured) {
                return ensured;
            }

            auto appContext = cli_->getAppContext();
            if (!appContext) {
                return Error{ErrorCode::NotInitialized, "App context not available"};
            }
            auto documentService = app::services::makeDocumentService(*appContext);
            if (!documentService) {
                return Error{ErrorCode::NotInitialized, "Document service not available"};
            }

            // Map CLI options to service request
            app::services::ListDocumentsRequest serviceReq;

            // Basic options
            serviceReq.limit = limit_;
            serviceReq.offset = offset_;
            if (recentCount_ > 0) {
                serviceReq.recent = recentCount_;
            }

            // Name pattern filter (reuse normalized path if detected)
            if (!namePattern_.empty()) {
                auto resolved = yams::app::services::resolveNameToPatternIfLocalFile(namePattern_);
                serviceReq.pattern = resolved.pattern;
                if (!resolvedLocalFilePath_.has_value())
                    resolvedLocalFilePath_ = resolved.isLocalFile ? resolved.absPath : std::nullopt;
            }

            // File type filters
            serviceReq.type = fileType_;
            serviceReq.mime = mimeType_;
            serviceReq.extension = extensions_;
            serviceReq.binary = binaryOnly_;
            serviceReq.text = textOnly_;

            // Time filters
            serviceReq.createdAfter = createdAfter_;
            serviceReq.createdBefore = createdBefore_;
            serviceReq.modifiedAfter = modifiedAfter_;
            serviceReq.modifiedBefore = modifiedBefore_;
            serviceReq.indexedAfter = indexedAfter_;
            serviceReq.indexedBefore = indexedBefore_;

            // Change tracking
            serviceReq.changes = showChanges_;
            serviceReq.since = sinceTime_;
            serviceReq.diffTags = showDiffTags_;
            serviceReq.showDeleted = showDeleted_;
            serviceReq.changeWindow = changeWindow_;

            // Tag filtering - parse comma-separated tags into vector
            if (!filterTags_.empty()) {
                std::istringstream ss(filterTags_);
                std::string tag;
                while (std::getline(ss, tag, ',')) {
                    tag.erase(0, tag.find_first_not_of(" \t"));
                    tag.erase(tag.find_last_not_of(" \t") + 1);
                    if (!tag.empty()) {
                        serviceReq.tags.push_back(tag);
                    }
                }
            }
            serviceReq.matchAllTags = matchAllTags_;

            // Session filtering
            if (!sessionFilter_.empty()) {
                serviceReq.sessionId = sessionFilter_;
            }

            // Display options
            serviceReq.format = format_;
            serviceReq.showSnippets = showSnippets_ && !noSnippets_;
            serviceReq.snippetLength = snippetLength_;
            serviceReq.showMetadata = showMetadata_ || !metadataFields_.empty();
            serviceReq.showTags = showTags_;
            serviceReq.groupBySession = groupBySession_;
            serviceReq.verbose = verbose_ || cli_->getVerbose();

            // Metadata filters
            {
                auto setLast = [](const std::vector<std::string>& src, const std::string& key,
                                  std::map<std::string, std::string>& dest) {
                    if (!src.empty()) {
                        dest[key] = src.back();
                    }
                };

                std::map<std::string, std::string> metadataFilters;
                setLast(pbiFilters_, "pbi", metadataFilters);
                setLast(taskFilters_, "task", metadataFilters);
                setLast(phaseFilters_, "phase", metadataFilters);
                setLast(ownerFilters_, "owner", metadataFilters);

                for (const auto& kv : metadataFiltersRaw_) {
                    auto pos = kv.find('=');
                    if (pos == std::string::npos) {
                        return Error{ErrorCode::InvalidArgument,
                                     "Metadata filters must be key=value"};
                    }
                    std::string key = kv.substr(0, pos);
                    std::string value = kv.substr(pos + 1);
                    config::trim(key);
                    config::trim(value);
                    if (key.empty()) {
                        return Error{ErrorCode::InvalidArgument, "Metadata key cannot be empty"};
                    }
                    metadataFilters[key] = value;
                }

                serviceReq.metadataFilters = std::move(metadataFilters);
                serviceReq.matchAllMetadata = !matchAnyMetadata_;
            }

            // Sorting
            serviceReq.sortBy = sortBy_;
            serviceReq.reverse = reverse_;

            // Call service
            auto result = documentService->list(serviceReq);
            if (!result) {
                spdlog::warn("Service failed, falling back to filesystem scanning: {}",
                             result.error().message);
                if (spinner && *spinner) {
                    (*spinner)->stop();
                }
                return fallbackToFilesystemScanning();
            }

            const auto& serviceResponse = result.value();

            if (spinner && *spinner) {
                (*spinner)->stop();
            }

            // Convert service response to legacy EnhancedDocumentInfo for display
            std::vector<EnhancedDocumentInfo> documents;
            for (const auto& docEntry : serviceResponse.documents) {
                EnhancedDocumentInfo doc;

                // Convert DocumentEntry to DocumentInfo
                doc.info.fileName = docEntry.fileName;
                doc.info.filePath = docEntry.path;
                doc.info.sha256Hash = docEntry.hash;
                doc.info.fileExtension = docEntry.extension;
                doc.info.fileSize = docEntry.size;
                doc.info.mimeType = docEntry.mimeType;

                // Convert timestamps (seconds precision)
                doc.info.createdTime =
                    std::chrono::sys_seconds{std::chrono::seconds{docEntry.created}};
                doc.info.modifiedTime =
                    std::chrono::sys_seconds{std::chrono::seconds{docEntry.modified}};
                doc.info.indexedTime =
                    std::chrono::sys_seconds{std::chrono::seconds{docEntry.indexed}};

                // Handle metadata and tags
                if (!docEntry.metadata.empty()) {
                    for (const auto& [key, value] : docEntry.metadata) {
                        metadata::MetadataValue metaVal;
                        metaVal.value = value;
                        doc.metadata[key] = metaVal;
                    }
                }
                doc.tags = docEntry.tags;

                // Handle content snippet and extraction status
                doc.extractionStatus = docEntry.extractionStatus;
                if (docEntry.snippet) {
                    doc.contentSnippet = docEntry.snippet.value();
                    doc.hasContent = true;
                }

                documents.push_back(doc);
            }

            // Handle paths-only output
            if (pathsOnly_) {
                for (const auto& doc : documents) {
                    std::cout << doc.info.filePath << std::endl;
                }
                return Result<void>();
            }

            // Handle diff-tags grouping if requested
            if (showDiffTags_) {
                outputDiffTags(documents);
                return Result<void>();
            }

            // Output results - respect global --json flag
            std::string effectiveFormat = format_;
            if (effectiveFormat == "table" && cli_->getJsonOutput()) {
                effectiveFormat = "json";
            }

            if (effectiveFormat == "json") {
                outputJson(documents);
            } else if (effectiveFormat == "csv") {
                outputCsv(documents);
            } else if (effectiveFormat == "minimal") {
                outputMinimal(documents);
            } else {
                outputTable(documents);
            }

            // If a concrete file path was provided, try to show a diff against indexed
            (void)printPathDiffIfApplicable();

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

    struct EnhancedDocumentInfo {
        metadata::DocumentInfo info;
        std::unordered_map<std::string, metadata::MetadataValue> metadata;
        std::string contentSnippet;
        std::string language;
        std::string extractionMethod;
        std::string extractionStatus;
        std::vector<std::string> tags;
        bool hasContent = false;

        std::string getFormattedSize() const {
            return ui::format_bytes(static_cast<uint64_t>(info.fileSize));
        }

        std::string getFormattedDate() const {
            auto time_t = std::chrono::system_clock::to_time_t(info.indexedTime);
            std::tm* tm = std::localtime(&time_t);
            char buffer[100];
            std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", tm);
            return std::string(buffer);
        }

        std::string getRelativeTime() const {
            auto now = std::chrono::system_clock::now();
            auto diff = std::chrono::duration_cast<std::chrono::seconds>(now - info.indexedTime);

            if (diff.count() < 60) {
                return "just now";
            } else if (diff.count() < 3600) {
                return std::to_string(diff.count() / 60) + "m ago";
            } else if (diff.count() < 86400) {
                return std::to_string(diff.count() / 3600) + "h ago";
            } else if (diff.count() < 86400 * 7) {
                return std::to_string(diff.count() / 86400) + "d ago";
            } else {
                return getFormattedDate();
            }
        }

        std::string getTags() const {
            if (tags.size() > 3) {
                return tags[0] + "," + tags[1] + "," + tags[2] + ",+" +
                       std::to_string(tags.size() - 3);
            }

            std::string result;
            for (size_t i = 0; i < tags.size(); ++i) {
                if (i > 0)
                    result += ",";
                result += tags[i];
            }
            return result;
        }

        std::string getFileType() const {
            if (!info.fileExtension.empty()) {
                return info.fileExtension.substr(1); // Remove leading dot
            }
            if (info.mimeType.find("text/") == 0)
                return "text";
            if (info.mimeType.find("image/") == 0)
                return "image";
            if (info.mimeType.find("video/") == 0)
                return "video";
            if (info.mimeType.find("audio/") == 0)
                return "audio";
            if (info.mimeType.find("application/pdf") == 0)
                return "pdf";
            return "binary";
        }
    };

    static void
    mergeTagsIntoMetadata(const std::vector<std::string>& tags,
                          std::unordered_map<std::string, metadata::MetadataValue>& metadata) {
        if (tags.empty())
            return;

        for (const auto& tag : tags) {
            if (tag.empty())
                continue;
            std::string key = tag.starts_with("tag:") ? tag : ("tag:" + tag);
            if (metadata.find(key) != metadata.end())
                continue;
            metadata::MetadataValue metaVal;
            metaVal.value = tag;
            metadata.emplace(std::move(key), std::move(metaVal));
        }
    }

    void updateMetadataFields() {
        metadataFields_ = parseMetadataFields(metadataFieldsRaw_);
        if (noMetadataFields_) {
            metadataFields_.clear();
        }
    }

    Result<void> listMetadataValues() {
        // Start spinner early (before any blocking operations)
        bool showSpinner =
            !cli_->getJsonOutput() && format_ != "json" && format_ != "csv" && format_ != "minimal";
        std::shared_ptr<ui::SpinnerRunner> spinner =
            showSpinner ? std::make_shared<ui::SpinnerRunner>() : nullptr;
        if (spinner) {
            spinner->start("Loading metadata values...");
        }
        auto stopSpinner = [&]() {
            if (spinner) {
                spinner->stop();
            }
        };

        auto keys = parseMetadataFields(metadataValuesRaw_);
        if (keys.empty()) {
            stopSpinner();
            return Error{ErrorCode::InvalidData, "No metadata keys provided"};
        }

        // Check if we can use the fast path (no document filters)
        bool canUseFastPath =
            namePattern_.empty() && extensions_.empty() && mimeType_.empty() && !textOnly_ &&
            !binaryOnly_ && filterTags_.empty() && createdAfter_.empty() &&
            createdBefore_.empty() && modifiedAfter_.empty() && modifiedBefore_.empty() &&
            indexedAfter_.empty() && indexedBefore_.empty() && !showChanges_ && sinceTime_.empty();

        Result<std::unordered_map<std::string, std::vector<metadata::MetadataValueCount>>> metaRes;

        if (canUseFastPath) {
            // Fast path: query directly without full storage initialization
            metaRes =
                cli_->fastMetadataValueCounts(keys, allMetadataValues_ ? 0 : metadataValuesLimit_);
        } else {
            // Full path: initialize storage and use metadata repository
            return Error{ErrorCode::InvalidArgument,
                         "--metadata-values does not support document filters"};
        }

        if (!metaRes) {
            stopSpinner();
            return metaRes.error();
        }
        stopSpinner();
        if (!metaRes) {
            stopSpinner();
            return metaRes.error();
        }
        stopSpinner();

        auto makeRows = [&](const std::string& key) -> std::vector<metadata::MetadataValueCount> {
            auto it = metaRes.value().find(key);
            if (it == metaRes.value().end()) {
                return {};
            }
            return it->second;
        };

        if (format_ == "json" || cli_->getJsonOutput()) {
            json out;
            for (const auto& key : keys) {
                json arr = json::array();
                for (const auto& [value, count] : makeRows(key)) {
                    arr.push_back({{"value", value}, {"count", count}});
                }
                out[key] = arr;
            }
            std::cout << out.dump(2) << std::endl;
            return Result<void>();
        }

        if (format_ == "csv") {
            std::cout << "key,value,count\n";
            for (const auto& key : keys) {
                for (const auto& [value, count] : makeRows(key)) {
                    std::cout << key << "," << value << "," << count << "\n";
                }
            }
            return Result<void>();
        }

        if (format_ == "minimal") {
            for (const auto& key : keys) {
                for (const auto& [value, count] : makeRows(key)) {
                    std::cout << key << "\t" << value << "\t" << count << "\n";
                }
            }
            return Result<void>();
        }

        // Streaming table output with fixed column widths
        constexpr size_t kValueWidth = 48;
        constexpr size_t kCountWidth = 10;

        for (const auto& key : keys) {
            std::cout << ui::section_header("Metadata Values: " + key) << "\n\n";

            auto rows = makeRows(key);
            if (rows.empty()) {
                std::cout << ui::status_info("No values found.") << "\n\n";
                continue;
            }

            // Print header with fixed widths
            std::cout << "  " << ui::pad_right("VALUE", kValueWidth) << "  "
                      << ui::pad_left("COUNT", kCountWidth) << "\n";
            std::cout << "  " << std::string(kValueWidth, '-') << "  "
                      << std::string(kCountWidth, '-') << "\n";

            // Stream rows with fixed column widths
            for (const auto& [value, count] : rows) {
                std::string formattedValue = ui::truncate_to_width(value, kValueWidth);
                std::string formattedCount = ui::format_number(count);
                std::cout << "  " << ui::pad_right(formattedValue, kValueWidth) << "  "
                          << ui::pad_left(formattedCount, kCountWidth) << "\n";
            }

            // Show truncation notice if we hit the limit
            if (!allMetadataValues_ && static_cast<int>(rows.size()) >= metadataValuesLimit_) {
                std::cout << "\n"
                          << ui::status_info("Showing top " + std::to_string(metadataValuesLimit_) +
                                             " values. Use --all-metadata-values to see all.")
                          << "\n";
            }

            std::cout << "\n";
        }

        return Result<void>();
    }

    std::vector<std::string> parseMetadataFields(std::string_view raw) const {
        std::vector<std::string> fields;
        if (raw.empty()) {
            return fields;
        }
        std::string input(raw);
        std::istringstream ss(input);
        std::string token;
        while (std::getline(ss, token, ',')) {
            yams::config::trim(token);
            if (!token.empty()) {
                fields.push_back(token);
            }
        }
        return fields;
    }

    static std::string globToSqlLikePattern(const std::string& glob) {
        std::string result;
        result.reserve(glob.size());

        for (size_t i = 0; i < glob.size(); ++i) {
            char c = glob[i];
            if (c == '*') {
                if (i + 1 < glob.size() && glob[i + 1] == '*') {
                    result += '%';
                    i++;
                    if (i + 1 < glob.size() && glob[i + 1] == '/') {
                        i++;
                    }
                } else {
                    result += '%';
                }
            } else if (c == '?') {
                result += '_';
            } else if (c == '%' || c == '_') {
                result += '\\';
                result += c;
            } else {
                result += c;
            }
        }

        return result;
    }

    static std::string toLower(std::string_view input) {
        std::string out;
        out.reserve(input.size());
        for (char ch : input) {
            out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
        }
        return out;
    }

    std::optional<std::string> findMetadataValue(const EnhancedDocumentInfo& doc,
                                                 std::string_view field) const {
        if (field.empty()) {
            return std::nullopt;
        }
        auto direct = doc.metadata.find(std::string(field));
        if (direct != doc.metadata.end()) {
            return direct->second.value;
        }
        std::string target = toLower(field);
        for (const auto& [key, value] : doc.metadata) {
            if (toLower(key) == target) {
                return value.value;
            }
        }
        return std::nullopt;
    }

    std::string formatSnippetForDisplay(std::string_view snippet, size_t maxLength) const {
        if (snippet.empty()) {
            return {};
        }
        if (auto formatted = ui::format_text_snippet(snippet, maxLength)) {
            return *formatted;
        }
        return "[binary] no text preview";
    }

    void outputTable(const std::vector<EnhancedDocumentInfo>& documents) {
        if (documents.empty()) {
            std::cout << ui::colorize("No documents found.", ui::Ansi::DIM) << "\n";
            return;
        }

        struct ColumnSpec {
            std::string header;
            size_t width;
            size_t min_width;
            bool align_right;
        };

        constexpr int kGap = 2;
        bool isVerbose = verbose_ || cli_->getVerbose();
        std::vector<ColumnSpec> columns;
        columns.reserve(6);
        columns.push_back({"NAME", 24, 12, false});
        columns.push_back({"TYPE", 8, 6, false});
        columns.push_back({"SIZE", 8, 6, true});
        size_t snippetIndex = std::numeric_limits<size_t>::max();
        if (showSnippets_ && !noSnippets_) {
            snippetIndex = columns.size();
            columns.push_back({"SNIPPET", 36, 12, false});
        }
        size_t tagsIndex = std::numeric_limits<size_t>::max();
        if (showTags_) {
            tagsIndex = columns.size();
            columns.push_back({"TAGS", 12, 8, false});
        }
        std::vector<size_t> metadataIndices;
        metadataIndices.reserve(metadataFields_.size());
        for (const auto& field : metadataFields_) {
            std::string header = field;
            for (auto& ch : header) {
                ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
            }
            const size_t width = std::max<size_t>(10, std::min<size_t>(18, header.size() + 2));
            metadataIndices.push_back(columns.size());
            columns.push_back({header, width, 8, false});
        }
        columns.push_back(
            {isVerbose ? "INDEXED" : "WHEN", isVerbose ? 19u : 12u, isVerbose ? 16u : 8u, false});

        size_t totalWidth = 0;
        for (const auto& col : columns) {
            totalWidth += col.width;
        }
        if (!columns.empty()) {
            totalWidth += static_cast<size_t>(kGap) * (columns.size() - 1);
        }

        const int termWidth = ui::terminal_width();
        auto reduceColumn = [&](size_t idx) {
            if (idx >= columns.size())
                return;
            if (totalWidth <= static_cast<size_t>(termWidth))
                return;
            auto& col = columns[idx];
            if (col.width <= col.min_width)
                return;
            size_t over = totalWidth - static_cast<size_t>(termWidth);
            size_t reducible = col.width - col.min_width;
            size_t delta = std::min(over, reducible);
            col.width -= delta;
            totalWidth -= delta;
        };

        reduceColumn(snippetIndex);
        reduceColumn(0); // name
        reduceColumn(tagsIndex);
        for (auto idx : metadataIndices) {
            reduceColumn(idx);
        }
        reduceColumn(1);                  // type
        reduceColumn(2);                  // size
        reduceColumn(columns.size() - 1); // date

        // Header
        for (size_t i = 0; i < columns.size(); ++i) {
            if (i > 0)
                std::cout << std::string(kGap, ' ');
            std::cout << ui::pad_right(columns[i].header, columns[i].width);
        }
        std::cout << "\n";

        // Separator
        for (size_t i = 0; i < columns.size(); ++i) {
            if (i > 0)
                std::cout << std::string(kGap, ' ');
            std::cout << ui::repeat('-', columns[i].width);
        }
        std::cout << "\n";

        // Rows
        for (const auto& doc : documents) {
            std::vector<std::string> cells;
            cells.reserve(columns.size());
            cells.push_back(doc.info.fileName);
            cells.push_back(doc.getFileType());
            cells.push_back(doc.getFormattedSize());
            if (snippetIndex != std::numeric_limits<size_t>::max()) {
                std::string snippetDisplay;
                if (!doc.contentSnippet.empty()) {
                    snippetDisplay = formatSnippetForDisplay(doc.contentSnippet, snippetLength_);
                    if (snippetDisplay.empty())
                        snippetDisplay = "-";
                } else if (doc.extractionStatus == "pending") {
                    snippetDisplay = "[pending]";
                } else if (doc.extractionStatus == "failed") {
                    snippetDisplay = "[failed]";
                } else {
                    snippetDisplay = "-";
                }
                cells.push_back(snippetDisplay);
            }
            if (tagsIndex != std::numeric_limits<size_t>::max()) {
                cells.push_back(doc.getTags());
            }
            for (const auto& field : metadataFields_) {
                auto value = findMetadataValue(doc, field);
                cells.push_back(value.value_or("-"));
            }
            cells.push_back(isVerbose ? doc.getFormattedDate() : doc.getRelativeTime());

            for (size_t i = 0; i < columns.size(); ++i) {
                if (i > 0)
                    std::cout << std::string(kGap, ' ');
                std::string cell = i < cells.size() ? cells[i] : "";
                cell = ui::truncate_to_width(cell, columns[i].width);
                if (columns[i].align_right) {
                    std::cout << ui::pad_left(cell, columns[i].width);
                } else {
                    std::cout << ui::pad_right(cell, columns[i].width);
                }
            }
            std::cout << "\n";

            if (isVerbose) {
                std::cout << "    Hash: " << doc.info.sha256Hash << "\n";
                std::cout << "    Path: " << doc.info.filePath << "\n";
                std::cout << "    MIME: " << doc.info.mimeType << "\n";

                if (doc.hasContent) {
                    std::string snippetDisplay =
                        formatSnippetForDisplay(doc.contentSnippet, snippetLength_);
                    if (!snippetDisplay.empty()) {
                        std::cout << "    Content: " << snippetDisplay << "\n";
                    }
                    if (!doc.language.empty()) {
                        std::cout << "    Language: " << doc.language << "\n";
                    }
                }

                if (showMetadata_ && !doc.metadata.empty()) {
                    std::cout << "    Metadata:\n";
                    for (const auto& [key, value] : doc.metadata) {
                        std::cout << "      " << key << ": " << value.value << "\n";
                    }
                }

                std::cout << "\n";
            } else if (showMetadata_ && !doc.metadata.empty()) {
                std::cout << "    Metadata:\n";
                for (const auto& [key, value] : doc.metadata) {
                    std::cout << "      " << key << ": " << value.value << "\n";
                }
                std::cout << "\n";
            }
        }

        std::string total = "Total: " + std::to_string(documents.size()) + " document(s)";
        std::cout << "\n" << ui::colorize(total, ui::Ansi::DIM) << "\n";
    }

    // Attempt to show a simple line diff between local file content and indexed content
    // when the user provided a concrete file path via --name.
    Result<void> printPathDiffIfApplicable() {
        try {
            if (!resolvedLocalFilePath_.has_value())
                return Result<void>();
            fs::path p{*resolvedLocalFilePath_};
            if (!fs::exists(p) || !fs::is_regular_file(p))
                return Result<void>();

            // Resolve absolute path and attempt to find corresponding indexed document
            std::error_code ec;
            fs::path abs = fs::weakly_canonical(p, ec);
            if (ec) {
                std::error_code ec2;
                fs::path tmp = fs::absolute(p, ec2);
                abs = ec2 ? p : tmp;
            }

            auto appContext = cli_->getAppContext();
            if (!appContext)
                return Result<void>();
            auto documentService = app::services::makeDocumentService(*appContext);
            if (!documentService)
                return Result<void>();

            auto findByPattern =
                [&](const std::string& pat) -> std::vector<app::services::DocumentEntry> {
                app::services::ListDocumentsRequest req;
                req.pattern = pat;
                req.limit = 1000;
                req.pathsOnly = false;
                auto lr = documentService->list(req);
                if (lr && !lr.value().documents.empty())
                    return lr.value().documents;
                return {};
            };

            std::vector<app::services::DocumentEntry> matches;
            // Try exact path first
            matches = findByPattern(abs.string());
            // Try suffix match if empty
            if (matches.empty()) {
                matches = findByPattern(std::string("%/") + abs.string());
            }
            // Try basename anywhere as last resort
            if (matches.empty()) {
                matches = findByPattern(std::string("%") + abs.filename().string() + "%");
            }
            if (matches.empty())
                return Result<void>();

            // Pick newest by indexed time
            const app::services::DocumentEntry* chosen = &matches.front();
            for (const auto& d : matches) {
                if (d.indexed > chosen->indexed)
                    chosen = &d;
            }

            // Retrieve indexed content
            yams::app::services::RetrievalService rsvc;
            yams::app::services::RetrievalOptions ropts;
            if (cli_->hasExplicitDataDir())
                ropts.explicitDataDir = cli_->getDataPath();
            yams::app::services::GetOptions greq;
            greq.hash = chosen->hash;
            greq.metadataOnly = false;
            auto gr = rsvc.get(greq, ropts);
            if (!gr)
                return Result<void>();
            const auto& indexed = gr.value();

            // Read local file (limit size for safety)
            std::ifstream ifs(abs);
            if (!ifs)
                return Result<void>();
            std::string localContent((std::istreambuf_iterator<char>(ifs)),
                                     std::istreambuf_iterator<char>());

            // If identical, print a short note and return
            if (indexed.content == localContent) {
                if (!cli_->getJsonOutput()) {
                    std::cout << "\nNo differences: local file matches indexed content ("
                              << abs.string() << ")\n";
                }
                return Result<void>();
            }

            // Produce a simple line-based diff (first 200 differing lines)
            auto toLines = [](const std::string& s) {
                std::vector<std::string> lines;
                std::stringstream ss(s);
                std::string line;
                while (std::getline(ss, line))
                    lines.push_back(line);
                return lines;
            };
            auto a = toLines(localContent);
            auto b = toLines(indexed.content);
            size_t i = 0, j = 0;
            size_t shown = 0, maxShown = 200;
            if (!cli_->getJsonOutput()) {
                std::cout << "\n=== Diff (local vs indexed) for: " << abs.string() << " ===\n";
            }
            while ((i < a.size() || j < b.size()) && shown < maxShown) {
                const std::string* la = (i < a.size()) ? &a[i] : nullptr;
                const std::string* lb = (j < b.size()) ? &b[j] : nullptr;
                if (la && lb && *la == *lb) {
                    ++i;
                    ++j;
                    continue;
                }
                if (la) {
                    if (!cli_->getJsonOutput())
                        std::cout << "- " << *la << "\n";
                    ++shown;
                    ++i;
                }
                if (lb && shown < maxShown) {
                    if (!cli_->getJsonOutput())
                        std::cout << "+ " << *lb << "\n";
                    ++shown;
                    ++j;
                }
            }
            if (!cli_->getJsonOutput() && (i < a.size() || j < b.size())) {
                std::cout << "... (diff truncated)\n";
            }
            return Result<void>();
        } catch (...) {
            return Result<void>();
        }
    }

    void outputDiffTags(const std::vector<EnhancedDocumentInfo>& documents) {
        // Categorize documents by change type
        std::vector<EnhancedDocumentInfo> addedDocs;
        std::vector<EnhancedDocumentInfo> modifiedDocs;
        std::vector<EnhancedDocumentInfo> deletedDocs;

        auto now = std::chrono::system_clock::now();
        auto windowTime = now - std::chrono::hours(24); // Default 24h window

        // Parse custom change window if specified
        if (!changeWindow_.empty()) {
            auto parsedWindow = TimeParser::parse(changeWindow_);
            if (parsedWindow) {
                windowTime = parsedWindow.value();
            }
        }

        for (const auto& doc : documents) {
            // Check if file exists on filesystem
            bool fileExists = std::filesystem::exists(doc.info.filePath);

            if (!fileExists) {
                deletedDocs.push_back(doc);
            } else {
                // Consider "added" if recently created (within window)
                if (doc.info.createdTime >= windowTime) {
                    addedDocs.push_back(doc);
                }
                // Consider "modified" if modified recently but not newly created
                else if (doc.info.modifiedTime >= windowTime ||
                         doc.info.indexedTime >= windowTime) {
                    modifiedDocs.push_back(doc);
                }
            }
        }

        std::cout << ui::section_header("Documents grouped by change type") << "\n";
        std::cout << ui::colorize("Window: " + changeWindow_, ui::Ansi::DIM) << "\n\n";

        auto renderGroup = [&](const char* label, const char* marker, const char* color,
                               const std::vector<EnhancedDocumentInfo>& group) {
            if (group.empty())
                return;
            std::string header = std::string(marker) + " " + label + " (" +
                                 std::to_string(group.size()) + " documents)";
            std::cout << ui::colorize(header, color) << "\n";
            std::cout << ui::horizontal_rule() << "\n";
            for (const auto& doc : group) {
                std::string fileType = getFileTypeIndicator(doc);
                std::string prefix = "  " + ui::colorize(marker, color);
                std::cout << prefix << " " << doc.info.fileName << " " << fileType << " ("
                          << doc.getFormattedSize() << ", " << doc.getRelativeTime() << ")\n";
            }
            std::cout << "\n";
        };

        renderGroup("[+]", "ADDED", ui::Ansi::GREEN, addedDocs);
        renderGroup("[M]", "MODIFIED", ui::Ansi::YELLOW, modifiedDocs);
        renderGroup("[D]", "DELETED", ui::Ansi::RED, deletedDocs);

        if (addedDocs.empty() && modifiedDocs.empty() && deletedDocs.empty()) {
            std::cout << ui::colorize("No recent changes found in the specified time window.",
                                      ui::Ansi::DIM)
                      << "\n";
        }

        std::string total = "Total: " + std::to_string(documents.size()) + " document(s) (" +
                            std::to_string(addedDocs.size()) + " added, " +
                            std::to_string(modifiedDocs.size()) + " modified, " +
                            std::to_string(deletedDocs.size()) + " deleted)";
        std::cout << ui::colorize(total, ui::Ansi::DIM) << "\n";
    }

    void outputJson(const std::vector<EnhancedDocumentInfo>& documents) {
        json output;
        json docs = json::array();

        for (const auto& doc : documents) {
            json d;
            d["hash"] = doc.info.sha256Hash;
            d["name"] = doc.info.fileName;
            d["path"] = doc.info.filePath;
            d["extension"] = doc.info.fileExtension;
            d["size"] = doc.info.fileSize;
            d["size-formatted"] = doc.getFormattedSize();
            d["mime_type"] = doc.info.mimeType;
            d["created"] = std::chrono::duration_cast<std::chrono::seconds>(
                               doc.info.createdTime.time_since_epoch())
                               .count();
            d["modified"] = std::chrono::duration_cast<std::chrono::seconds>(
                                doc.info.modifiedTime.time_since_epoch())
                                .count();
            d["indexed"] = std::chrono::duration_cast<std::chrono::seconds>(
                               doc.info.indexedTime.time_since_epoch())
                               .count();
            d["indexed_formatted"] = doc.getFormattedDate();
            d["relative_time"] = doc.getRelativeTime();

            if (doc.hasContent) {
                d["content_snippet"] = doc.contentSnippet;
                d["language"] = doc.language;
                d["extraction_method"] = doc.extractionMethod;
            }

            json metadata_obj = json::object();
            for (const auto& [key, value] : doc.metadata) {
                if (key == "tag" || key.starts_with("tag:")) {
                    continue;
                }
                metadata_obj[key] = value.value;
            }
            d["metadata"] = metadata_obj;

            if (!metadataFields_.empty()) {
                json selected = json::object();
                for (const auto& field : metadataFields_) {
                    auto value = findMetadataValue(doc, field);
                    if (value) {
                        selected[field] = *value;
                    } else {
                        selected[field] = nullptr;
                    }
                }
                d["metadata_fields"] = selected;
            }

            d["tags"] = doc.tags;
            docs.push_back(d);
        }

        output["documents"] = docs;
        output["total"] = documents.size();

        std::cout << output.dump(2) << std::endl;
    }

    void outputCsv(const std::vector<EnhancedDocumentInfo>& documents) {
        // CSV header
        std::cout << "hash,name,size,type,snippet,tags";
        for (const auto& field : metadataFields_) {
            std::cout << "," << field;
        }
        std::cout << ",indexed\n";

        for (const auto& doc : documents) {
            auto writeField = [](std::string value) {
                std::replace(value.begin(), value.end(), '"', '\'');
                std::replace(value.begin(), value.end(), '\n', ' ');
                std::cout << "\"" << value << "\"";
            };
            std::cout << doc.info.sha256Hash << ",";
            writeField(doc.info.fileName);
            std::cout << ",";
            std::cout << doc.info.fileSize << ",";
            writeField(doc.getFileType());
            std::cout << ",";

            std::string snippet = doc.contentSnippet;
            writeField(snippet);
            std::cout << ",";

            writeField(doc.getTags());
            for (const auto& field : metadataFields_) {
                auto value = findMetadataValue(doc, field);
                std::cout << ",";
                writeField(value.value_or(""));
            }
            std::cout << "," << doc.getFormattedDate() << "\n";
        }
    }

    void outputMinimal(const std::vector<EnhancedDocumentInfo>& documents) {
        // Just output hashes, one per line (useful for piping)
        for (const auto& doc : documents) {
            std::cout << doc.info.sha256Hash << "\n";
        }
    }

    std::string extractSnippet(const std::string& content, int maxLength) {
        if (content.empty())
            return "";

        std::string snippet = content;

        // Remove excessive whitespace and newlines for better display
        std::string result;
        bool lastWasSpace = false;
        for (char c : snippet) {
            if (std::isspace(c)) {
                if (!lastWasSpace) {
                    result += ' ';
                    lastWasSpace = true;
                }
            } else {
                result += c;
                lastWasSpace = false;
            }
        }

        if (result.length() > static_cast<size_t>(maxLength)) {
            return result.substr(0, static_cast<size_t>(maxLength - 3)) + "...";
        }

        return result;
    }

    Result<void> fallbackToFilesystemScanning() {
        // Minimal fallback - just show that metadata repo is not available
        if (format_ == "json" || cli_->getJsonOutput()) {
            json output;
            output["error"] = "Metadata repository not available";
            output["fallback"] = true;
            output["documents"] = json::array();
            output["total"] = 0;
            std::cout << output.dump(2) << std::endl;
        } else {
            std::cout << "No documents found. Metadata repository not initialized.\n";
            std::cout << "Try running: yams init\n";
        }
        return Result<void>();
    }

    bool applyTimeFilters(const metadata::DocumentInfo& doc) {
        // Parse and apply created time filters
        if (!createdAfter_.empty()) {
            auto afterTime = TimeParser::parse(createdAfter_);
            if (!afterTime) {
                spdlog::warn("Invalid created-after time: {}", createdAfter_);
                return true; // Don't filter on invalid input
            }
            if (doc.createdTime < afterTime.value()) {
                return false;
            }
        }

        if (!createdBefore_.empty()) {
            auto beforeTime = TimeParser::parse(createdBefore_);
            if (!beforeTime) {
                spdlog::warn("Invalid created-before time: {}", createdBefore_);
                return true;
            }
            if (doc.createdTime > beforeTime.value()) {
                return false;
            }
        }

        // Parse and apply modified time filters
        if (!modifiedAfter_.empty()) {
            auto afterTime = TimeParser::parse(modifiedAfter_);
            if (!afterTime) {
                spdlog::warn("Invalid modified-after time: {}", modifiedAfter_);
                return true;
            }
            if (doc.modifiedTime < afterTime.value()) {
                return false;
            }
        }

        if (!modifiedBefore_.empty()) {
            auto beforeTime = TimeParser::parse(modifiedBefore_);
            if (!beforeTime) {
                spdlog::warn("Invalid modified-before time: {}", modifiedBefore_);
                return true;
            }
            if (doc.modifiedTime > beforeTime.value()) {
                return false;
            }
        }

        // Parse and apply indexed time filters
        if (!indexedAfter_.empty()) {
            auto afterTime = TimeParser::parse(indexedAfter_);
            if (!afterTime) {
                spdlog::warn("Invalid indexed-after time: {}", indexedAfter_);
                return true;
            }
            if (doc.indexedTime < afterTime.value()) {
                return false;
            }
        }

        if (!indexedBefore_.empty()) {
            auto beforeTime = TimeParser::parse(indexedBefore_);
            if (!beforeTime) {
                spdlog::warn("Invalid indexed-before time: {}", indexedBefore_);
                return true;
            }
            if (doc.indexedTime > beforeTime.value()) {
                return false;
            }
        }

        return true;
    }

    bool applyChangeFilters(const metadata::DocumentInfo& doc) {
        // If no change filters are specified, include all documents
        if (!showChanges_ && sinceTime_.empty() && !showDeleted_) {
            return true;
        }

        auto now = std::chrono::system_clock::now();

        // Handle --changes flag (show recent modifications in last 24h or specified window)
        if (showChanges_) {
            auto windowTime = TimeParser::parse(changeWindow_);
            if (!windowTime) {
                // Default to 24 hours if parsing fails
                windowTime = now - std::chrono::hours(24);
            }

            // Check if document was modified, created, or indexed recently
            bool recentlyChanged = (doc.modifiedTime >= windowTime.value()) ||
                                   (doc.createdTime >= windowTime.value()) ||
                                   (doc.indexedTime >= windowTime.value());

            if (!recentlyChanged) {
                return false;
            }
        }

        // Handle --since filter
        if (!sinceTime_.empty()) {
            auto sinceTimePoint = TimeParser::parse(sinceTime_);
            if (!sinceTimePoint) {
                spdlog::warn("Invalid since time: {}", sinceTime_);
                return true; // Don't filter on invalid input
            }

            // Check if any timestamp is after the since time
            bool changedSince = (doc.modifiedTime >= sinceTimePoint.value()) ||
                                (doc.createdTime >= sinceTimePoint.value()) ||
                                (doc.indexedTime >= sinceTimePoint.value());

            if (!changedSince) {
                return false;
            }
        }

        // Handle --show-deleted flag
        if (!showDeleted_) {
            // Check if file still exists on filesystem
            if (!std::filesystem::exists(doc.filePath)) {
                return false; // Filter out deleted files unless explicitly requested
            }
        }

        return true;
    }

    bool applyFileTypeFilters(const metadata::DocumentInfo& doc) {
        // Extension filter
        if (!extensions_.empty()) {
            std::string ext = doc.fileExtension;
            if (ext.empty() && !doc.fileName.empty()) {
                auto pos = doc.fileName.rfind('.');
                if (pos != std::string::npos) {
                    ext = doc.fileName.substr(pos);
                }
            }

            // Parse comma-separated extensions
            std::istringstream ss(extensions_);
            std::string token;
            bool found = false;
            while (std::getline(ss, token, ',')) {
                yams::config::trim(token);

                // Add dot if not present
                if (!token.empty() && token[0] != '.') {
                    token = "." + token;
                }

                if (ext == token) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                return false;
            }
        }

        // MIME type filter
        if (!mimeType_.empty()) {
            if (doc.mimeType != mimeType_) {
                // Also check if it's a wildcard match (e.g., "image/*")
                if (mimeType_.back() == '*' && mimeType_.size() > 1) {
                    std::string prefix = mimeType_.substr(0, mimeType_.size() - 1);
                    if (doc.mimeType.find(prefix) != 0) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }

        // File type category filter or binary/text filter
        if (!fileType_.empty() || binaryOnly_ || textOnly_) {
            // Detect file type if not already in metadata
            detection::FileSignature sig;

            if (!doc.mimeType.empty()) {
                sig.mimeType = doc.mimeType;
                sig.fileType =
                    detection::FileTypeDetector::instance().getFileTypeCategory(doc.mimeType);
                sig.isBinary =
                    detection::FileTypeDetector::instance().isBinaryMimeType(doc.mimeType);
            } else {
                // Try to detect from file path if available
                fs::path filePath = doc.filePath;
                if (fs::exists(filePath)) {
                    auto detectResult =
                        detection::FileTypeDetector::instance().detectFromFile(filePath);
                    if (detectResult) {
                        sig = detectResult.value();
                    } else {
                        // Fall back to extension-based detection
                        std::string ext = filePath.extension().string();
                        sig.mimeType = detection::FileTypeDetector::getMimeTypeFromExtension(ext);
                        sig.fileType = detection::FileTypeDetector::instance().getFileTypeCategory(
                            sig.mimeType);
                        sig.isBinary =
                            detection::FileTypeDetector::instance().isBinaryMimeType(sig.mimeType);
                    }
                } else {
                    // Use extension only
                    std::string ext = doc.fileExtension;
                    if (ext.empty() && !doc.fileName.empty()) {
                        auto pos = doc.fileName.rfind('.');
                        if (pos != std::string::npos) {
                            ext = doc.fileName.substr(pos);
                        }
                    }
                    sig.mimeType = detection::FileTypeDetector::getMimeTypeFromExtension(ext);
                    sig.fileType =
                        detection::FileTypeDetector::instance().getFileTypeCategory(sig.mimeType);
                    sig.isBinary =
                        detection::FileTypeDetector::instance().isBinaryMimeType(sig.mimeType);
                }
            }

            // Apply file type category filter
            if (!fileType_.empty()) {
                if (sig.fileType != fileType_) {
                    return false;
                }
            }

            // Apply binary/text filter
            if (binaryOnly_ && !sig.isBinary) {
                return false;
            }
            if (textOnly_ && sig.isBinary) {
                return false;
            }
        }

        return true;
    }

    YamsCLI* cli_ = nullptr;
    std::string namePattern_; // Filter by name pattern
    std::string positionalName_;
    bool namePatternWasNormalized_ = false;
    std::string format_;
    bool jsonFlag_ = false; // --json shorthand for --format json
    std::string sortBy_;
    bool reverse_ = false;
    int limit_ = 100;
    bool verbose_ = false;
    bool pathsOnly_ = false;
    int offset_ = 0;
    int recentCount_ = 0; // 0 means not set, show all

    // New enhanced display options
    bool showSnippets_ = true;
    bool showMetadata_ = false;
    std::string metadataFieldsRaw_ = "task,pbi,phase,owner,source";
    bool noMetadataFields_ = false;
    std::vector<std::string> metadataFields_;
    bool showTags_ = true;
    bool groupBySession_ = false;
    int snippetLength_ = 50;
    bool noSnippets_ = false;
    std::string metadataValuesRaw_{};
    int metadataValuesLimit_ = 10000;
    bool allMetadataValues_ = false;
    std::vector<std::string> pbiFilters_;
    std::vector<std::string> taskFilters_;
    std::vector<std::string> phaseFilters_;
    std::vector<std::string> ownerFilters_;
    std::vector<std::string> metadataFiltersRaw_;
    bool matchAnyMetadata_ = false;

    // File type filters
    std::string fileType_;
    std::string mimeType_;
    std::string extensions_;
    bool binaryOnly_ = false;
    bool textOnly_ = false;

    // Time filters
    std::string createdAfter_;
    std::string createdBefore_;
    std::string modifiedAfter_;
    std::string modifiedBefore_;
    std::string indexedAfter_;
    std::string indexedBefore_;

    // Change tracking options
    bool showChanges_ = false;
    std::string sinceTime_;
    bool showDiffTags_ = false;
    bool showDeleted_ = false;
    std::string changeWindow_;

    // Tag filtering
    std::string filterTags_;
    bool matchAllTags_ = false;

    // Session filtering
    std::string sessionFilter_;

    // Snapshot operations (Task 043-05b)
    bool listSnapshots_ = false;
    std::string snapshotId_;
    std::string compareTo_;

    // Streaming configuration
    bool enableStreaming_ = true;

    std::string getFileTypeIndicator(const EnhancedDocumentInfo& doc) {
        std::string indicator = "[";

        // Add extension
        if (!doc.info.fileExtension.empty()) {
            indicator += doc.info.fileExtension;
        } else {
            indicator += "no-ext";
        }

        // Add binary/text indicator
        if (!doc.info.mimeType.empty()) {
            bool isBinary =
                detection::FileTypeDetector::instance().isBinaryMimeType(doc.info.mimeType);
            indicator += isBinary ? "|bin" : "|txt";

            // Add general file type category
            std::string category =
                detection::FileTypeDetector::instance().getFileTypeCategory(doc.info.mimeType);
            if (!category.empty() && category != "unknown") {
                indicator += "|" + category;
            }
        }

        indicator += "]";
        return indicator;
    }
};

// Factory function
std::unique_ptr<ICommand> createListCommand() {
    return std::make_unique<ListCommand>();
}

} // namespace yams::cli
