#include <spdlog/spdlog.h>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/time_parser.h>
#include <yams/cli/yams_cli.h>
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
#include <iomanip>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <vector>

namespace yams::cli {

namespace fs = std::filesystem;
using json = nlohmann::json;

class ListCommand : public ICommand {
public:
    std::string getName() const override { return "list"; }

    std::string getDescription() const override { return "List stored documents"; }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("list", getDescription());
        cmd->alias("ls"); // Add ls as alias for list

        cmd->add_option("--format", format_, "Output format: table, json, csv, minimal")
            ->default_val("table")
            ->check(CLI::IsMember({"table", "json", "csv", "minimal"}));

        cmd->add_option("--sort", sortBy_, "Sort by: name, size, date, hash")
            ->default_val("date")
            ->check(CLI::IsMember({"name", "size", "date", "hash"}));

        cmd->add_flag("--reverse,-r", reverse_, "Reverse sort order");
        cmd->add_option("--limit,-n", limit_, "Limit number of results")->default_val(100);
        cmd->add_option("--offset", offset_, "Offset for pagination")->default_val(0);
        cmd->add_option("--recent", recentCount_, "Show N most recent documents");
        cmd->add_flag("-v,--verbose", verbose_, "Show detailed information");

        // New options for enhanced metadata display
        cmd->add_flag("--show-snippets", showSnippets_, "Show content previews (default: true)")
            ->default_val(true);
        cmd->add_flag("--show-metadata", showMetadata_, "Show all metadata for each document");
        cmd->add_flag("--show-tags", showTags_, "Show document tags (default: true)")
            ->default_val(true);
        cmd->add_flag("--group-by-session", groupBySession_, "Group documents by time periods");
        cmd->add_option("--snippet-length", snippetLength_, "Length of content snippets")
            ->default_val(50);
        cmd->add_flag("--no-snippets", noSnippets_, "Disable content previews");

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

        // Filter options
        cmd->add_option("--tags", filterTags_,
                        "Filter documents by tags (comma-separated, e.g., 'work,important')");

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

        cmd->callback([this]() {
            // Handle snippet flag logic
            if (noSnippets_) {
                showSnippets_ = false;
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
            // Try daemon-first for simple list (no heavy filters)
            if (!binaryOnly_ && !textOnly_ && fileType_.empty() && mimeType_.empty() &&
                extensions_.empty() && createdAfter_.empty() && createdBefore_.empty() &&
                modifiedAfter_.empty() && modifiedBefore_.empty() && indexedAfter_.empty() &&
                indexedBefore_.empty() && !showChanges_ && sinceTime_.empty() && !showDiffTags_ &&
                !showDeleted_) {
                yams::daemon::ListRequest dreq;
                dreq.limit = static_cast<size_t>(recentCount_ > 0 ? recentCount_ : limit_);
                dreq.recent = true; // respect recent ordering
                auto render = [&](const yams::daemon::ListResponse& resp) -> Result<void> {
                    if (format_ == "json") {
                        nlohmann::json j;
                        j["total"] = resp.totalCount;
                        nlohmann::json items = nlohmann::json::array();
                        for (const auto& e : resp.items) {
                            items.push_back({{"hash", e.hash},
                                             {"path", e.path},
                                             {"name", e.name},
                                             {"size", e.size}});
                        }
                        j["items"] = items;
                        std::cout << j.dump(2) << std::endl;
                    } else {
                        for (const auto& e : resp.items) {
                            if (format_ == "minimal") {
                                std::cout << e.path << "\n";
                            } else {
                                std::cout << e.name << "\t" << e.size << "\t"
                                          << e.hash.substr(0, 12) << "\t" << e.path << "\n";
                            }
                        }
                    }
                    return Result<void>();
                };
                auto fallback = [&]() -> Result<void> {
                    return Error{ErrorCode::NotImplemented, "local"};
                };
                if (auto d = daemon_first(dreq, fallback, render); d)
                    return Result<void>();
            }
            auto ensured = cli_->ensureStorageInitialized();
            if (!ensured) {
                return ensured;
            }

            auto store = cli_->getContentStore();
            if (!store) {
                return Error{ErrorCode::NotInitialized, "Content store not initialized"};
            }

            // Get metadata repository
            auto metadataRepo = cli_->getMetadataRepository();
            if (!metadataRepo) {
                spdlog::warn(
                    "Metadata repository not available, falling back to filesystem scanning");
                return fallbackToFilesystemScanning();
            }

            // Get all documents from metadata repository
            auto documentsResult = metadataRepo->findDocumentsByPath("%");
            if (!documentsResult) {
                spdlog::warn("Failed to query documents from metadata repository: {}",
                             documentsResult.error().message);
                return fallbackToFilesystemScanning();
            }

            spdlog::debug("Found {} documents in metadata repository",
                          documentsResult.value().size());

            std::vector<EnhancedDocumentInfo> documents;

            // Initialize file type detector if needed for filtering
            bool needFileTypeDetection =
                !fileType_.empty() || !mimeType_.empty() || binaryOnly_ || textOnly_;
            if (needFileTypeDetection) {
                detection::FileTypeDetectorConfig config;
                config.patternsFile = YamsCLI::findMagicNumbersFile();
                config.useCustomPatterns = !config.patternsFile.empty();
                detection::FileTypeDetector::instance().initialize(config);
            }

            // Process each document and enrich with metadata and content
            for (const auto& docInfo : documentsResult.value()) {
                // Apply time filters
                if (!applyTimeFilters(docInfo)) {
                    continue;
                }

                // Apply change tracking filters (skip if showing diff-tags, since it needs all
                // docs)
                if (!showDiffTags_ && !applyChangeFilters(docInfo)) {
                    continue;
                }

                // Apply file type filters
                if (!applyFileTypeFilters(docInfo)) {
                    continue;
                }

                EnhancedDocumentInfo doc;
                doc.info = docInfo;

                // Get additional metadata
                if (showMetadata_ || showTags_ || !filterTags_.empty()) {
                    auto metadataResult = metadataRepo->getAllMetadata(docInfo.id);
                    if (metadataResult) {
                        doc.metadata = metadataResult.value();
                    }
                }

                // Apply tag filter if specified
                if (!filterTags_.empty()) {
                    // Parse comma-separated tags
                    std::vector<std::string> requiredTags;
                    std::stringstream ss(filterTags_);
                    std::string tag;
                    while (std::getline(ss, tag, ',')) {
                        // Trim whitespace
                        tag.erase(0, tag.find_first_not_of(" \t"));
                        tag.erase(tag.find_last_not_of(" \t") + 1);
                        if (!tag.empty()) {
                            requiredTags.push_back(tag);
                        }
                    }

                    // Check if document has any of the required tags
                    bool hasTag = false;
                    for (const auto& [key, value] : doc.metadata) {
                        // Tags are stored with key="tag" as comma-separated values
                        if (key == "tag") {
                            // Parse the stored tags
                            std::stringstream storedTagStream(value.value);
                            std::string storedTag;
                            while (std::getline(storedTagStream, storedTag, ',')) {
                                // Trim whitespace from stored tag
                                storedTag.erase(0, storedTag.find_first_not_of(" \t"));
                                storedTag.erase(storedTag.find_last_not_of(" \t") + 1);

                                // Check if this stored tag matches any required tag
                                for (const auto& reqTag : requiredTags) {
                                    if (storedTag == reqTag) {
                                        hasTag = true;
                                        break;
                                    }
                                }
                                if (hasTag)
                                    break;
                            }
                            if (hasTag)
                                break;
                        }
                    }

                    if (!hasTag) {
                        continue; // Skip this document
                    }
                }

                // Get content snippet if requested
                bool isVerbose = verbose_ || cli_->getVerbose();
                if ((showSnippets_ && !noSnippets_) || isVerbose) {
                    auto contentResult = metadataRepo->getContent(docInfo.id);
                    if (contentResult && contentResult.value()) {
                        const auto& content = contentResult.value().value();
                        doc.contentSnippet = extractSnippet(content.contentText, snippetLength_);
                        doc.language = content.language;
                        doc.extractionMethod = content.extractionMethod;
                        doc.hasContent = true;
                    }
                }

                documents.push_back(doc);
            }

            // Sort documents
            if (sortBy_ == "name") {
                std::sort(documents.begin(), documents.end(),
                          [](const EnhancedDocumentInfo& a, const EnhancedDocumentInfo& b) {
                              return a.info.fileName < b.info.fileName;
                          });
            } else if (sortBy_ == "size") {
                std::sort(documents.begin(), documents.end(),
                          [](const EnhancedDocumentInfo& a, const EnhancedDocumentInfo& b) {
                              return a.info.fileSize < b.info.fileSize;
                          });
            } else if (sortBy_ == "hash") {
                std::sort(documents.begin(), documents.end(),
                          [](const EnhancedDocumentInfo& a, const EnhancedDocumentInfo& b) {
                              return a.info.sha256Hash < b.info.sha256Hash;
                          });
            } else { // date
                std::sort(documents.begin(), documents.end(),
                          [](const EnhancedDocumentInfo& a, const EnhancedDocumentInfo& b) {
                              return a.info.indexedTime < b.info.indexedTime;
                          });
            }

            if (reverse_) {
                std::reverse(documents.begin(), documents.end());
            }

            // If --recent is specified, sort by date (most recent first) and take N most recent
            if (recentCount_ > 0) {
                // Sort by date descending (most recent first) regardless of original sort
                std::sort(documents.begin(), documents.end(),
                          [](const EnhancedDocumentInfo& a, const EnhancedDocumentInfo& b) {
                              return a.info.indexedTime >
                                     b.info.indexedTime; // Note: > for descending
                          });

                // Take only the N most recent
                if (documents.size() > static_cast<size_t>(recentCount_)) {
                    documents.resize(static_cast<size_t>(recentCount_));
                }

                // Re-apply the original sort if it wasn't date
                if (sortBy_ != "date") {
                    if (sortBy_ == "name") {
                        std::sort(documents.begin(), documents.end(),
                                  [](const EnhancedDocumentInfo& a, const EnhancedDocumentInfo& b) {
                                      return a.info.fileName < b.info.fileName;
                                  });
                    } else if (sortBy_ == "size") {
                        std::sort(documents.begin(), documents.end(),
                                  [](const EnhancedDocumentInfo& a, const EnhancedDocumentInfo& b) {
                                      return a.info.fileSize < b.info.fileSize;
                                  });
                    } else if (sortBy_ == "hash") {
                        std::sort(documents.begin(), documents.end(),
                                  [](const EnhancedDocumentInfo& a, const EnhancedDocumentInfo& b) {
                                      return a.info.sha256Hash < b.info.sha256Hash;
                                  });
                    }
                    if (reverse_) {
                        std::reverse(documents.begin(), documents.end());
                    }
                }
            }

            // Apply offset then limit (but only if --recent wasn't used, or for additional
            // limiting)
            if (offset_ > 0) {
                if (documents.size() > static_cast<size_t>(offset_)) {
                    documents.erase(documents.begin(),
                                    documents.begin() + static_cast<size_t>(offset_));
                } else {
                    documents.clear();
                }
            }
            // Only apply limit if --recent wasn't specified, or if limit is smaller than recent
            if (limit_ > 0 && recentCount_ == 0 && documents.size() > static_cast<size_t>(limit_)) {
                documents.resize(static_cast<size_t>(limit_));
            } else if (limit_ > 0 && recentCount_ > 0 && limit_ < recentCount_ &&
                       documents.size() > static_cast<size_t>(limit_)) {
                documents.resize(static_cast<size_t>(limit_));
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

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

private:
    struct EnhancedDocumentInfo {
        metadata::DocumentInfo info;
        std::unordered_map<std::string, metadata::MetadataValue> metadata;
        std::string contentSnippet;
        std::string language;
        std::string extractionMethod;
        bool hasContent = false;

        std::string getFormattedSize() const {
            auto size = static_cast<size_t>(info.fileSize);
            if (size < 1024) {
                return std::to_string(size) + " B";
            } else if (size < 1024 * 1024) {
                return std::to_string(size / 1024) + " KB";
            } else if (size < 1024 * 1024 * 1024) {
                return yamsfmt::format("{:.1f} MB", size / (1024.0 * 1024.0));
            } else {
                return yamsfmt::format("{:.1f} GB", size / (1024.0 * 1024.0 * 1024.0));
            }
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
            std::vector<std::string> tags;
            for (const auto& [key, value] : metadata) {
                if (key == "tag" || key.starts_with("tag:")) {
                    tags.push_back(value.value.empty() ? key : value.value);
                }
            }
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

    void outputTable(const std::vector<EnhancedDocumentInfo>& documents) {
        if (documents.empty()) {
            std::cout << "No documents found.\n";
            return;
        }

        // Calculate column widths based on options
        size_t nameWidth = 24;
        size_t typeWidth = 8;
        size_t sizeWidth = 8;
        size_t snippetWidth = showSnippets_ && !noSnippets_ ? 36 : 0;
        size_t tagsWidth = showTags_ ? 12 : 0;
        bool isVerbose = verbose_ || cli_->getVerbose();
        size_t dateWidth = isVerbose ? 19 : 12;

        // Header
        std::cout << std::left;
        std::cout << std::setw(static_cast<int>(nameWidth)) << "NAME" << "  ";
        std::cout << std::setw(static_cast<int>(typeWidth)) << "TYPE" << "  ";
        std::cout << std::setw(static_cast<int>(sizeWidth)) << "SIZE" << "  ";

        if (snippetWidth > 0) {
            std::cout << std::setw(static_cast<int>(snippetWidth)) << "SNIPPET" << "  ";
        }

        if (tagsWidth > 0) {
            std::cout << std::setw(static_cast<int>(tagsWidth)) << "TAGS" << "  ";
        }

        std::cout << std::setw(static_cast<int>(dateWidth)) << (isVerbose ? "INDEXED" : "WHEN")
                  << "\n";

        // Separator
        std::cout << std::string(nameWidth, '-') << "  ";
        std::cout << std::string(typeWidth, '-') << "  ";
        std::cout << std::string(sizeWidth, '-') << "  ";

        if (snippetWidth > 0) {
            std::cout << std::string(snippetWidth, '-') << "  ";
        }

        if (tagsWidth > 0) {
            std::cout << std::string(tagsWidth, '-') << "  ";
        }

        std::cout << std::string(dateWidth, '-') << "\n";

        // Rows
        for (const auto& doc : documents) {
            std::string nameDisplay = doc.info.fileName;
            if (nameDisplay.length() > nameWidth) {
                nameDisplay = nameDisplay.substr(0, nameWidth - 3) + "...";
            }

            std::string typeDisplay = doc.getFileType();
            if (typeDisplay.length() > typeWidth) {
                typeDisplay = typeDisplay.substr(0, typeWidth - 1);
            }

            std::cout << std::setw(static_cast<int>(nameWidth)) << nameDisplay << "  ";
            std::cout << std::setw(static_cast<int>(typeWidth)) << typeDisplay << "  ";
            std::cout << std::setw(static_cast<int>(sizeWidth)) << doc.getFormattedSize() << "  ";

            if (snippetWidth > 0) {
                std::string snippetDisplay = doc.contentSnippet;
                if (snippetDisplay.length() > snippetWidth) {
                    snippetDisplay = snippetDisplay.substr(0, snippetWidth - 3) + "...";
                }
                // Replace newlines with spaces for display
                std::replace(snippetDisplay.begin(), snippetDisplay.end(), '\n', ' ');
                std::cout << std::setw(static_cast<int>(snippetWidth)) << snippetDisplay << "  ";
            }

            if (tagsWidth > 0) {
                std::string tagsDisplay = doc.getTags();
                if (tagsDisplay.length() > tagsWidth) {
                    tagsDisplay = tagsDisplay.substr(0, tagsWidth - 1);
                }
                std::cout << std::setw(static_cast<int>(tagsWidth)) << tagsDisplay << "  ";
            }

            std::cout << std::setw(static_cast<int>(dateWidth))
                      << (isVerbose ? doc.getFormattedDate() : doc.getRelativeTime()) << "\n";

            if (isVerbose) {
                std::cout << "    Hash: " << doc.info.sha256Hash << "\n";
                std::cout << "    Path: " << doc.info.filePath << "\n";
                std::cout << "    MIME: " << doc.info.mimeType << "\n";

                if (doc.hasContent) {
                    std::cout << "    Content: " << doc.contentSnippet << "\n";
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
            }
        }

        std::cout << "\nTotal: " << documents.size() << " document(s)\n";
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

        // Output grouped results with colored headers
        std::cout << "Documents grouped by change type (window: " << changeWindow_ << ")\n";
        std::cout << "═══════════════════════════════════════════════\n\n";

        if (!addedDocs.empty()) {
            std::cout << "[+] ADDED (" << addedDocs.size() << " documents)\n";
            std::cout << "────────────────────────────────────────\n";
            for (const auto& doc : addedDocs) {
                std::string fileType = getFileTypeIndicator(doc);
                std::cout << "  + " << doc.info.fileName << " " << fileType << " ("
                          << doc.getFormattedSize() << ", " << doc.getRelativeTime() << ")\n";
            }
            std::cout << "\n";
        }

        if (!modifiedDocs.empty()) {
            std::cout << "[M] MODIFIED (" << modifiedDocs.size() << " documents)\n";
            std::cout << "────────────────────────────────────────\n";
            for (const auto& doc : modifiedDocs) {
                std::string fileType = getFileTypeIndicator(doc);
                std::cout << "  M " << doc.info.fileName << " " << fileType << " ("
                          << doc.getFormattedSize() << ", " << doc.getRelativeTime() << ")\n";
            }
            std::cout << "\n";
        }

        if (!deletedDocs.empty()) {
            std::cout << "[D] DELETED (" << deletedDocs.size() << " documents)\n";
            std::cout << "────────────────────────────────────────\n";
            for (const auto& doc : deletedDocs) {
                std::string fileType = getFileTypeIndicator(doc);
                std::cout << "  D " << doc.info.fileName << " " << fileType << " ("
                          << doc.getFormattedSize() << ", " << doc.getRelativeTime() << ")\n";
            }
            std::cout << "\n";
        }

        if (addedDocs.empty() && modifiedDocs.empty() && deletedDocs.empty()) {
            std::cout << "No recent changes found in the specified time window.\n";
        }

        std::cout << "Total: " << documents.size() << " document(s) (" << addedDocs.size()
                  << " added, " << modifiedDocs.size() << " modified, " << deletedDocs.size()
                  << " deleted)\n";
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
            d["size_formatted"] = doc.getFormattedSize();
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

            if (!doc.metadata.empty()) {
                json metadata_obj;
                for (const auto& [key, value] : doc.metadata) {
                    metadata_obj[key] = value.value;
                }
                d["metadata"] = metadata_obj;
            }

            d["tags"] = doc.getTags();
            docs.push_back(d);
        }

        output["documents"] = docs;
        output["total"] = documents.size();

        std::cout << output.dump(2) << std::endl;
    }

    void outputCsv(const std::vector<EnhancedDocumentInfo>& documents) {
        // CSV header
        std::cout << "hash,name,size,type,snippet,tags,indexed\n";

        for (const auto& doc : documents) {
            std::cout << doc.info.sha256Hash << ",";
            std::cout << "\"" << doc.info.fileName << "\",";
            std::cout << doc.info.fileSize << ",";
            std::cout << "\"" << doc.getFileType() << "\",";

            std::string snippet = doc.contentSnippet;
            std::replace(snippet.begin(), snippet.end(), '"', '\'');
            std::replace(snippet.begin(), snippet.end(), '\n', ' ');
            std::cout << "\"" << snippet << "\",";

            std::cout << "\"" << doc.getTags() << "\",";
            std::cout << doc.getFormattedDate() << "\n";
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

private:
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
                // Trim whitespace
                token.erase(0, token.find_first_not_of(" \t"));
                token.erase(token.find_last_not_of(" \t") + 1);

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
    std::string format_;
    std::string sortBy_;
    bool reverse_ = false;
    int limit_ = 100;
    bool verbose_ = false;
    int offset_ = 0;
    int recentCount_ = 0;    // 0 means not set, show all
    std::string filterTags_; // Comma-separated tags to filter by

    // New enhanced display options
    bool showSnippets_ = true;
    bool showMetadata_ = false;
    bool showTags_ = true;
    bool groupBySession_ = false;
    int snippetLength_ = 50;
    bool noSnippets_ = false;

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

    // bool matchAllTags_ = false;  // Currently unused - reserved for future tag matching logic

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
