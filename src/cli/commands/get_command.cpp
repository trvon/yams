#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <sstream>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/system_executor.hpp>
#include <yams/app/services/services.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/session_store.h>
#include <yams/cli/time_parser.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/response_of.hpp>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/text_extractor.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/profiling.h>
#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace yams::cli {

using json = nlohmann::json;

class GetCommand : public ICommand {
public:
    std::string getName() const override { return "get"; }

    std::string getDescription() const override {
        return "Retrieve a document from the content store";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("get", getDescription());

        // Create option group for retrieval methods (only one can be used at a time)
        auto* group = cmd->add_option_group("retrieval_method");
        group->add_option("hash", hash_, "Hash of the document to retrieve");
        group->add_option("--name", name_, "Name of the document to retrieve");

        // File type filters
        group->add_option("--type", fileType_,
                          "Filter by file type (image, document, archive, audio, video, text, "
                          "executable, binary)");
        group->add_option("--mime", mimeType_,
                          "Filter by MIME type (e.g., image/jpeg, application/pdf)");
        group->add_option("--extension", extension_, "Filter by file extension (e.g., .jpg, .pdf)");
        group->add_flag("--binary", binaryOnly_, "Get only binary files");
        group->add_flag("--text", textOnly_, "Get only text files");

        // Time filters
        group->add_option("--created-after", createdAfter_, "Get files created after this time");
        group->add_option("--created-before", createdBefore_, "Get files created before this time");
        group->add_option("--modified-after", modifiedAfter_, "Get files modified after this time");
        group->add_option("--modified-before", modifiedBefore_,
                          "Get files modified before this time");
        group->add_option("--indexed-after", indexedAfter_, "Get files indexed after this time");
        group->add_option("--indexed-before", indexedBefore_, "Get files indexed before this time");

        // Require at least one filter/selector
        group->require_option(1);

        cmd->add_option("-o,--output", outputPath_, "Output file path (default: stdout)");
        cmd->add_flag("--metadata-only", metadataOnly_, "Return only metadata (no bytes)");
        cmd->add_option("--max-bytes", maxBytes_, "Maximum bytes to transfer (0 = unlimited)")
            ->default_val(0);
        cmd->add_option("--chunk-size", chunkSize_, "Streaming chunk size in bytes")
            ->default_val(262144);
        cmd->add_flag("-v,--verbose", verbose_, "Enable verbose output");
        cmd->add_flag("--latest", getLatest_, "Get the most recently indexed matching document");
        cmd->add_flag("--oldest", getOldest_, "Get the oldest indexed matching document");

        // Session scoping controls
        cmd->add_option("--session", sessionOverride_, "Use this session for scoping");
        cmd->add_flag("--no-session", noSession_, "Bypass session scoping");

        // Text extraction options
        cmd->add_flag("--raw", raw_, "Output raw content without text extraction");
        cmd->add_flag("--extract", extract_, "Force text extraction even when piping to file");

        // Knowledge graph options
        cmd->add_flag("--graph", showGraph_, "Show related documents from knowledge graph");
        cmd->add_option("--depth", graphDepth_, "Depth of graph traversal (default: 1)")
            ->default_val(1)
            ->check(CLI::Range(1, 5));

        cmd->callback([this]() {
            if (!noSession_) {
                sessionPatterns_ =
                    yams::cli::session_store::active_include_patterns(sessionOverride_);
            } else {
                sessionPatterns_.clear();
            }
            cli_->setPendingCommand(this);
        });
    }

    Result<void> execute() override {
        std::promise<Result<void>> prom;
        auto fut = prom.get_future();
        boost::asio::co_spawn(
            boost::asio::system_executor{},
            [this, &prom]() -> boost::asio::awaitable<void> {
                auto r = co_await this->executeAsync();
                prom.set_value(std::move(r));
                co_return;
            },
            boost::asio::detached);
        return fut.get();
    }

    boost::asio::awaitable<Result<void>> executeAsync() override {
        YAMS_ZONE_SCOPED_N("GetCommand::execute");
        try {
            // Create enhanced daemon request with ALL CLI options mapped
            yams::daemon::GetRequest dreq;

            // Target selection
            dreq.hash = hash_;
            dreq.name = name_;
            dreq.byName = !name_.empty();

            // File type filters
            dreq.fileType = fileType_;
            dreq.mimeType = mimeType_;
            dreq.extension = extension_;
            dreq.binaryOnly = binaryOnly_;
            dreq.textOnly = textOnly_;

            // Time filters
            dreq.createdAfter = createdAfter_;
            dreq.createdBefore = createdBefore_;
            dreq.modifiedAfter = modifiedAfter_;
            dreq.modifiedBefore = modifiedBefore_;
            dreq.indexedAfter = indexedAfter_;
            dreq.indexedBefore = indexedBefore_;

            // Selection options
            dreq.latest = getLatest_;
            dreq.oldest = getOldest_;

            // Output options
            dreq.outputPath = outputPath_;
            dreq.metadataOnly = metadataOnly_;
            dreq.maxBytes = maxBytes_;
            dreq.chunkSize = static_cast<uint32_t>(chunkSize_);

            // Content options
            dreq.raw = raw_;
            dreq.extract = extract_;

            // Knowledge graph options
            dreq.showGraph = showGraph_;
            dreq.graphDepth = graphDepth_;

            // Display options
            dreq.verbose = verbose_;

            spdlog::debug("GetCommand: Created enhanced daemon request (hash='{}', name='{}', "
                          "filters={}, graph={})",
                          dreq.hash, dreq.name, hasFilters(), dreq.showGraph);

            // Define render function for daemon response
            auto render = [&](const yams::daemon::GetResponse& resp) -> Result<void> {
                spdlog::debug("GetCommand: Processing daemon response (hash='{}', hasContent={}, "
                              "graphRelated={})",
                              resp.hash, resp.hasContent, resp.related.size());

                if (metadataOnly_) {
                    // Metadata-only display
                    if (verbose_) {
                        std::cerr << "Document: " << resp.fileName << std::endl;
                        std::cerr << "Hash: " << resp.hash << std::endl;
                        std::cerr << "Path: " << resp.path << std::endl;
                    }
                    std::cerr << "Size: " << resp.size << " bytes" << std::endl;
                    std::cerr << "Type: " << resp.mimeType << std::endl;

                    // Display metadata
                    for (const auto& [key, value] : resp.metadata) {
                        std::cerr << key << ": " << value << std::endl;
                    }

                    // Display knowledge graph if enabled
                    if (resp.graphEnabled && !resp.related.empty()) {
                        std::cerr << "\nRelated documents (depth " << graphDepth_
                                  << "):" << std::endl;
                        for (const auto& rel : resp.related) {
                            std::cerr << "  - " << rel.name << " (" << rel.relationship
                                      << ", distance=" << rel.distance << ")" << std::endl;
                        }
                    }

                } else if (resp.hasContent) {
                    // Content retrieval
                    if (outputPath_.empty() || outputPath_ == "-") {
                        // Output to stdout
                        std::cout << resp.content;
                    } else {
                        // Write to file
                        std::ofstream outFile(outputPath_, std::ios::binary);
                        if (!outFile) {
                            return Error{ErrorCode::WriteError,
                                         "Cannot open output file: " + outputPath_.string()};
                        }
                        outFile.write(resp.content.data(), resp.content.size());
                        outFile.close();

                        if (verbose_) {
                            std::cerr << "Document retrieved successfully!" << std::endl;
                            std::cerr << "Output: " << outputPath_ << std::endl;
                            std::cerr << "Size: " << resp.totalBytes << " bytes" << std::endl;
                        }
                    }

                    // Display knowledge graph if enabled
                    if (resp.graphEnabled && !resp.related.empty()) {
                        std::cerr << "\nRelated documents (depth " << graphDepth_
                                  << "):" << std::endl;
                        for (const auto& rel : resp.related) {
                            std::cerr << "  - " << rel.name << " (" << rel.relationship
                                      << ", distance=" << rel.distance << ")" << std::endl;
                        }
                    }
                } else {
                    return Error{ErrorCode::NotFound, "Document content not available"};
                }

                return Result<void>();
            };

            // Define service fallback using DocumentService
            auto fallback = [&]() -> Result<void> {
                spdlog::debug("GetCommand: Using service fallback");

                // Get application context and DocumentService
                auto appContext = cli_->getAppContext();
                auto documentService = app::services::makeDocumentService(*appContext);

                // Map CLI options to service RetrieveDocumentRequest
                app::services::RetrieveDocumentRequest serviceReq;

                // Target selection
                serviceReq.hash = hash_;
                serviceReq.name = name_;

                // File type filters
                serviceReq.fileType = fileType_;
                serviceReq.mimeType = mimeType_;
                serviceReq.extension = extension_;

                // Selection options
                serviceReq.latest = getLatest_;
                serviceReq.oldest = getOldest_;

                // Output options
                serviceReq.outputPath = outputPath_;
                serviceReq.metadataOnly = metadataOnly_;
                serviceReq.maxBytes = maxBytes_;
                serviceReq.chunkSize = chunkSize_;

                // Content options
                serviceReq.includeContent = !metadataOnly_;
                serviceReq.raw = raw_;
                serviceReq.extract = extract_;

                // Knowledge graph options
                serviceReq.graph = showGraph_;
                serviceReq.depth = graphDepth_;

                // Call DocumentService
                auto result = documentService->retrieve(serviceReq);
                if (!result) {
                    return result.error();
                }

                const auto& serviceResp = result.value();

                // Convert service response to daemon response and render
                yams::daemon::GetResponse daemonResp;

                if (serviceResp.document.has_value()) {
                    const auto& doc = serviceResp.document.value();
                    daemonResp.hash = doc.hash;
                    daemonResp.path = doc.path;
                    daemonResp.name = doc.name;
                    daemonResp.fileName = doc.fileName;
                    daemonResp.size = doc.size;
                    daemonResp.mimeType = doc.mimeType;
                    daemonResp.fileType = doc.fileType;
                    daemonResp.created = doc.created;
                    daemonResp.modified = doc.modified;
                    daemonResp.indexed = doc.indexed;

                    if (doc.content.has_value()) {
                        daemonResp.content = doc.content.value();
                        daemonResp.hasContent = true;
                    }

                    // Convert metadata
                    for (const auto& [key, value] : doc.metadata) {
                        daemonResp.metadata[key] = value;
                    }

                } else if (!serviceResp.documents.empty()) {
                    const auto& doc = serviceResp.documents[0];
                    daemonResp.hash = doc.hash;
                    daemonResp.path = doc.path;
                    daemonResp.name = doc.name;
                    daemonResp.fileName = doc.fileName;
                    daemonResp.size = doc.size;
                    daemonResp.mimeType = doc.mimeType;
                    daemonResp.fileType = doc.fileType;
                    daemonResp.created = doc.created;
                    daemonResp.modified = doc.modified;
                    daemonResp.indexed = doc.indexed;

                    if (doc.content.has_value()) {
                        daemonResp.content = doc.content.value();
                        daemonResp.hasContent = true;
                    }

                    for (const auto& [key, value] : doc.metadata) {
                        daemonResp.metadata[key] = value;
                    }
                } else {
                    return Error{ErrorCode::NotFound, "No documents found matching criteria"};
                }

                // Knowledge graph results
                daemonResp.graphEnabled = serviceResp.graphEnabled;
                if (serviceResp.graphEnabled) {
                    for (const auto& rel : serviceResp.related) {
                        yams::daemon::RelatedDocumentEntry entry;
                        entry.hash = rel.hash;
                        entry.path = rel.path;
                        entry.name = rel.name;
                        entry.relationship = rel.relationship.value_or("unknown");
                        entry.distance = rel.distance;
                        entry.relevanceScore = rel.relevanceScore;
                        daemonResp.related.push_back(entry);
                    }
                }

                daemonResp.totalBytes = serviceResp.totalBytes;
                daemonResp.outputWritten = serviceResp.outputPath.has_value();

                // Render using the same logic as daemon path
                return render(daemonResp);
            };

            // Best-effort: direct daemon call; fallback to services on error
            yams::daemon::ClientConfig cfg;
            if (cli_) {
                auto dp = cli_->getDataPath();
                if (!dp.empty()) {
                    cfg.dataDir = dp;
#ifndef _WIN32
                    ::setenv("YAMS_STORAGE", cfg.dataDir.string().c_str(), 1);
                    ::setenv("YAMS_DATA_DIR", cfg.dataDir.string().c_str(), 1);
#endif
                }
            }
            cfg.headerTimeout = std::chrono::milliseconds(30000);
            cfg.bodyTimeout = std::chrono::milliseconds(120000);
            yams::daemon::DaemonClient::setTimeoutEnvVars(cfg.headerTimeout, cfg.bodyTimeout);
            cfg.enableChunkedResponses = true;
            cfg.singleUseConnections = false;
            cfg.requestTimeout = std::chrono::milliseconds(30000);
            yams::daemon::DaemonClient client(cfg);
            client.setStreamingEnabled(cfg.enableChunkedResponses);
            auto dres = co_await client.call(dreq);
            if (dres) {
                auto r = render(dres.value());
                if (!r)
                    co_return r.error();
                co_return Result<void>();
            }

            co_return fallback();

        } catch (const std::exception& e) {
            co_return Error{ErrorCode::InternalError,
                            std::string("GetCommand failed: ") + e.what()};
        }
    }

private:
    Result<void> displayKnowledgeGraph(const std::string& hash) {
        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
        }

        // Find document by hash
        auto docResult = metadataRepo->getDocumentByHash(hash);
        if (!docResult || !docResult.value().has_value()) {
            return Error{ErrorCode::NotFound, "Document not found in metadata"};
        }

        auto& doc = docResult.value().value();

        std::cerr << "\n=== Knowledge Graph for " << doc.fileName << " ===\n";
        std::cerr << "Hash: " << hash.substr(0, 12) << "...\n";
        std::cerr << "Path: " << doc.filePath << "\n";

        // Display related documents (simulated - would need actual graph implementation)
        std::cerr << "\nRelated Documents (depth " << graphDepth_ << "):\n";

        // Find documents in same directory
        std::filesystem::path dirPath = std::filesystem::path(doc.filePath).parent_path();
        auto relatedResult = metadataRepo->findDocumentsByPath(dirPath.string() + "/%");

        if (relatedResult && !relatedResult.value().empty()) {
            int count = 0;
            for (const auto& related : relatedResult.value()) {
                if (related.sha256Hash != hash && count < 10) {
                    std::cerr << "  - " << related.fileName << " ("
                              << related.sha256Hash.substr(0, 8) << "...)\n";
                    count++;
                }
            }
        }

        // Find documents with similar extension
        std::cerr << "\nSimilar documents by extension:\n";
        auto extResult = metadataRepo->findDocumentsByPath("%." + doc.fileExtension);
        if (extResult && !extResult.value().empty()) {
            int count = 0;
            for (const auto& similar : extResult.value()) {
                if (similar.sha256Hash != hash && count < 5) {
                    std::cerr << "  - " << similar.fileName << " (" << similar.fileExtension
                              << ")\n";
                    count++;
                }
            }
        }

        std::cerr << "\n";
        return Result<void>();
    }

    bool hasFilters() const {
        return !fileType_.empty() || !mimeType_.empty() || !extension_.empty() || binaryOnly_ ||
               textOnly_ || !createdAfter_.empty() || !createdBefore_.empty() ||
               !modifiedAfter_.empty() || !modifiedBefore_.empty() || !indexedAfter_.empty() ||
               !indexedBefore_.empty();
    }

    Result<std::string> resolveByFilters() {
        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
        }

        // Get all documents
        auto documentsResult = metadataRepo->findDocumentsByPath("%");
        if (!documentsResult) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to query documents: " + documentsResult.error().message};
        }

        auto documents = documentsResult.value();

        // Initialize file type detector if needed
        bool needFileTypeDetection = !fileType_.empty() || !mimeType_.empty() ||
                                     !extension_.empty() || binaryOnly_ || textOnly_;
        if (needFileTypeDetection) {
            detection::FileTypeDetectorConfig config;
            config.patternsFile = YamsCLI::findMagicNumbersFile();
            config.useCustomPatterns = !config.patternsFile.empty();
            detection::FileTypeDetector::instance().initialize(config);
        }

        // Apply filters
        std::vector<metadata::DocumentInfo> filtered;
        for (const auto& doc : documents) {
            if (applyTimeFilters(doc) && applyFileTypeFilters(doc)) {
                filtered.push_back(doc);
            }
        }

        if (filtered.empty()) {
            return Error{ErrorCode::NotFound, "No documents match the specified filters"};
        }

        // Sort by indexed time if needed
        if (getLatest_ || getOldest_) {
            std::sort(filtered.begin(), filtered.end(),
                      [this](const metadata::DocumentInfo& a, const metadata::DocumentInfo& b) {
                          return getLatest_ ? (a.indexedTime > b.indexedTime)
                                            : (a.indexedTime < b.indexedTime);
                      });
        }

        if (filtered.size() > 1 && !getLatest_ && !getOldest_) {
            std::cerr << "Multiple documents match the filters (" << filtered.size()
                      << " found):" << std::endl;
            for (size_t i = 0; i < std::min(size_t(5), filtered.size()); ++i) {
                const auto& doc = filtered[i];
                std::cerr << "  " << doc.sha256Hash.substr(0, 12) << "... - " << doc.fileName
                          << " (" << doc.fileSize << " bytes)" << std::endl;
            }
            if (filtered.size() > 5) {
                std::cerr << "  ... and " << (filtered.size() - 5) << " more" << std::endl;
            }
            std::cerr << "Use --latest or --oldest to select one, or refine your filters."
                      << std::endl;
            return Error{
                ErrorCode::InvalidOperation,
                "Multiple documents match. Please refine filters or use --latest/--oldest."};
        }

        return filtered[0].sha256Hash;
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

    bool applyFileTypeFilters(const metadata::DocumentInfo& doc) {
        // Extension filter
        if (!extension_.empty()) {
            std::string ext = doc.fileExtension;
            if (ext.empty() && !doc.fileName.empty()) {
                auto pos = doc.fileName.rfind('.');
                if (pos != std::string::npos) {
                    ext = doc.fileName.substr(pos);
                }
            }

            // Add dot if not present
            std::string targetExt = extension_;
            if (!targetExt.empty() && targetExt[0] != '.') {
                targetExt = "." + targetExt;
            }

            if (ext != targetExt) {
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
                std::filesystem::path filePath = doc.filePath;
                if (std::filesystem::exists(filePath)) {
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

    Result<std::string> resolveNameToHash(const std::string& name) {
        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
        }

        // Try as partial hash first (if it looks like hex and is 6+ characters)
        if (isValidHashPrefix(name)) {
            auto hashResult = resolvePartialHash(name);
            if (hashResult) {
                return hashResult.value();
            }
            // If hash resolution fails, continue with name-based resolution
            if (verbose_) {
                std::cerr << "No document found with hash prefix '" << name
                          << "', trying name-based resolution..." << std::endl;
            }
        }

        // Check if name contains wildcards (* or ?)
        bool hasWildcards =
            name.find('*') != std::string::npos || name.find('?') != std::string::npos;

        // First try as a path suffix (for real files)
        auto documentsResult = metadataRepo->findDocumentsByPath("%/" + name);
        if (documentsResult && !documentsResult.value().empty()) {
            auto results = documentsResult.value();
            if (!sessionPatterns_.empty()) {
                std::vector<metadata::DocumentInfo> filtered;
                for (const auto& d : results) {
                    bool match = false;
                    for (const auto& pat : sessionPatterns_) {
                        if (d.filePath.find(pat) != std::string::npos ||
                            d.fileName.find(pat) != std::string::npos) {
                            match = true;
                            break;
                        }
                    }
                    if (match)
                        filtered.push_back(d);
                }
                if (!filtered.empty())
                    results.swap(filtered);
            }
            if (results.size() > 1) {
                if (hasWildcards || getLatest_ || getOldest_) {
                    // For wildcards or explicit latest/oldest flags, select automatically
                    auto sorted = results;
                    std::sort(
                        sorted.begin(), sorted.end(),
                        [this](const metadata::DocumentInfo& a, const metadata::DocumentInfo& b) {
                            return getOldest_ ? (a.indexedTime < b.indexedTime)
                                              : (a.indexedTime > b.indexedTime);
                        });
                    if (verbose_) {
                        std::cerr << "Found " << results.size() << " matches for '" << name
                                  << "', returning " << (getOldest_ ? "oldest" : "newest") << ": "
                                  << sorted[0].filePath << std::endl;
                    }
                    return sorted[0].sha256Hash;
                } else {
                    std::cerr << "Multiple documents found with name '" << name
                              << "':" << std::endl;
                    for (const auto& doc : results) {
                        std::cerr << "  " << doc.sha256Hash.substr(0, 12) << "... - "
                                  << doc.filePath << std::endl;
                    }
                    return Error{ErrorCode::InvalidOperation,
                                 "Multiple documents with the same name. Please use hash to "
                                 "specify which one or use --latest/--oldest."};
                }
            }
            return results[0].sha256Hash;
        }

        // Try exact path match
        documentsResult = metadataRepo->findDocumentsByPath(name);
        if (documentsResult && !documentsResult.value().empty()) {
            auto docs = documentsResult.value();
            if (!sessionPatterns_.empty()) {
                std::vector<metadata::DocumentInfo> filtered;
                for (const auto& d : docs) {
                    bool match = false;
                    for (const auto& pat : sessionPatterns_) {
                        if (d.filePath.find(pat) != std::string::npos ||
                            d.fileName.find(pat) != std::string::npos) {
                            match = true;
                            break;
                        }
                    }
                    if (match)
                        filtered.push_back(d);
                }
                if (!filtered.empty())
                    docs.swap(filtered);
            }
            if (docs.size() > 1 && (hasWildcards || getLatest_ || getOldest_)) {
                auto sorted = docs;
                std::sort(sorted.begin(), sorted.end(),
                          [this](const metadata::DocumentInfo& a, const metadata::DocumentInfo& b) {
                              return getOldest_ ? (a.indexedTime < b.indexedTime)
                                                : (a.indexedTime > b.indexedTime);
                          });
                if (verbose_) {
                    std::cerr << "Found " << documentsResult.value().size()
                              << " exact matches for '" << name << "', returning "
                              << (getOldest_ ? "oldest" : "newest") << ": " << sorted[0].filePath
                              << std::endl;
                }
                return sorted[0].sha256Hash;
            }
            return docs[0].sha256Hash;
        }

        // For wildcard patterns, try wildcard matching before fuzzy
        if (hasWildcards) {
            auto wildcardResult = resolveWildcardPath(name);
            if (wildcardResult) {
                return wildcardResult.value();
            }
        }

        // Try fuzzy path matching (partial path components)
        auto fuzzyResult = resolveFuzzyPath(name);
        if (fuzzyResult) {
            return fuzzyResult.value();
        }

        // For stdin documents or when path search fails, use search
        auto searchResult = metadataRepo->search(name, 100, 0);
        if (searchResult) {
            std::vector<std::string> matchingHashes;
            std::vector<std::string> matchingPaths;

            for (const auto& result : searchResult.value().results) {
                // SearchResult contains document directly
                const auto& doc = result.document;
                // Check if fileName matches exactly
                if (doc.fileName == name) {
                    matchingHashes.push_back(doc.sha256Hash);
                    matchingPaths.push_back(doc.filePath);
                }
            }

            if (!matchingHashes.empty()) {
                if (matchingHashes.size() > 1) {
                    std::cerr << "Multiple documents found with name '" << name
                              << "':" << std::endl;
                    for (size_t i = 0; i < matchingHashes.size(); ++i) {
                        std::cerr << "  " << matchingHashes[i].substr(0, 12) << "... - "
                                  << matchingPaths[i] << std::endl;
                    }
                    return Error{ErrorCode::InvalidOperation,
                                 "Multiple documents with the same name. Please use hash to "
                                 "specify which one."};
                }
                return matchingHashes[0];
            }
        }

        return Error{ErrorCode::NotFound, "No document found with name: " + name};
    }

    bool isValidHashPrefix(const std::string& input) const {
        // Must be at least 6 characters for unambiguous hash prefixes
        if (input.length() < 6 || input.length() > 64) {
            return false;
        }

        // Must contain only hexadecimal characters (case insensitive)
        return std::all_of(input.begin(), input.end(), [](char c) { return std::isxdigit(c); });
    }

    Result<std::string> resolvePartialHash(const std::string& hashPrefix) {
        YAMS_ZONE_SCOPED_N("GetCommand::resolvePartialHash");

        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
        }

        // Convert to lowercase for consistent comparison
        std::string lowerPrefix = hashPrefix;
        std::transform(lowerPrefix.begin(), lowerPrefix.end(), lowerPrefix.begin(), ::tolower);

        // Get all documents and find matches
        auto documentsResult = metadataRepo->findDocumentsByPath("%");
        if (!documentsResult) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to query documents: " + documentsResult.error().message};
        }

        std::vector<std::string> matchingHashes;
        std::vector<std::string> matchingPaths;

        for (const auto& doc : documentsResult.value()) {
            std::string docHash = doc.sha256Hash;
            std::transform(docHash.begin(), docHash.end(), docHash.begin(), ::tolower);

            if (docHash.substr(0, lowerPrefix.length()) == lowerPrefix) {
                matchingHashes.push_back(doc.sha256Hash);
                matchingPaths.push_back(doc.filePath);
            }
        }

        if (matchingHashes.empty()) {
            return Error{ErrorCode::NotFound, "No document found with hash prefix: " + hashPrefix};
        }

        if (matchingHashes.size() > 1) {
            std::cerr << "Ambiguous hash prefix '" << hashPrefix << "' matches "
                      << matchingHashes.size() << " documents:" << std::endl;
            for (size_t i = 0; i < std::min(size_t(5), matchingHashes.size()); ++i) {
                std::cerr << "  " << matchingHashes[i].substr(0, 12) << "... - " << matchingPaths[i]
                          << std::endl;
            }
            if (matchingHashes.size() > 5) {
                std::cerr << "  ... and " << (matchingHashes.size() - 5) << " more" << std::endl;
            }
            std::cerr << "Please use a longer hash prefix to disambiguate." << std::endl;
            return Error{ErrorCode::InvalidOperation, "Ambiguous hash prefix. Use longer prefix."};
        }

        if (verbose_) {
            std::cerr << "Resolved hash prefix '" << hashPrefix << "' to: " << matchingHashes[0]
                      << std::endl;
        }

        return matchingHashes[0];
    }

    Result<std::string> resolveFuzzyPath(const std::string& pathQuery) {
        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
        }

        // Get all documents
        auto documentsResult = metadataRepo->findDocumentsByPath("%");
        if (!documentsResult) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to query documents: " + documentsResult.error().message};
        }

        std::vector<std::pair<std::string, int>> candidatesWithScores;
        std::vector<metadata::DocumentInfo> docs = documentsResult.value();
        if (!sessionPatterns_.empty()) {
            std::vector<metadata::DocumentInfo> filtered;
            for (const auto& d : docs) {
                bool match = false;
                for (const auto& pat : sessionPatterns_) {
                    if (d.filePath.find(pat) != std::string::npos ||
                        d.fileName.find(pat) != std::string::npos) {
                        match = true;
                        break;
                    }
                }
                if (match)
                    filtered.push_back(d);
            }
            if (!filtered.empty())
                docs.swap(filtered);
        }

        // Split query into path components
        std::vector<std::string> queryComponents;
        std::istringstream ss(pathQuery);
        std::string component;
        while (std::getline(ss, component, '/')) {
            if (!component.empty()) {
                queryComponents.push_back(component);
            }
        }

        if (queryComponents.empty()) {
            return Error{ErrorCode::InvalidArgument, "Empty path query"};
        }

        // Score each document path
        for (const auto& doc : docs) {
            std::filesystem::path docPath(doc.filePath);

            // Split document path into components
            std::vector<std::string> docComponents;
            for (const auto& part : docPath) {
                if (part != "/" && !part.string().empty()) {
                    docComponents.push_back(part.string());
                }
            }

            // Calculate fuzzy match score
            int score = calculateFuzzyPathScore(queryComponents, docComponents);
            if (score > 0) {
                candidatesWithScores.emplace_back(doc.sha256Hash, score);
            }
        }

        if (candidatesWithScores.empty()) {
            return Error{ErrorCode::NotFound, "No documents match fuzzy path: " + pathQuery};
        }

        // Sort by score (highest first)
        std::sort(candidatesWithScores.begin(), candidatesWithScores.end(),
                  [](const auto& a, const auto& b) { return a.second > b.second; });

        // If multiple matches with same top score, check if this is a wildcard pattern
        bool hasWildcards =
            pathQuery.find('*') != std::string::npos || pathQuery.find('?') != std::string::npos;

        if (candidatesWithScores.size() > 1 &&
            candidatesWithScores[0].second == candidatesWithScores[1].second) {
            if (hasWildcards || getLatest_ || getOldest_) {
                // For wildcards or explicit flags, resolve by selecting newest/oldest
                std::vector<metadata::DocumentInfo> tiedMatches;
                for (size_t i = 0; i < candidatesWithScores.size() &&
                                   candidatesWithScores[i].second == candidatesWithScores[0].second;
                     ++i) {
                    // Find the document for this hash
                    for (const auto& doc : documentsResult.value()) {
                        if (doc.sha256Hash == candidatesWithScores[i].first) {
                            tiedMatches.push_back(doc);
                            break;
                        }
                    }
                }

                // Sort by indexed time
                std::sort(tiedMatches.begin(), tiedMatches.end(),
                          [this](const metadata::DocumentInfo& a, const metadata::DocumentInfo& b) {
                              return getOldest_ ? (a.indexedTime < b.indexedTime)
                                                : (a.indexedTime > b.indexedTime);
                          });

                if (verbose_) {
                    std::cerr << "Fuzzy path '" << pathQuery << "' matched " << tiedMatches.size()
                              << " documents with equal scores, returning "
                              << (getOldest_ ? "oldest" : "newest") << ": "
                              << tiedMatches[0].filePath << std::endl;
                }

                return tiedMatches[0].sha256Hash;
            } else {
                std::cerr << "Ambiguous fuzzy path '" << pathQuery
                          << "' matches multiple documents:" << std::endl;

                // Find document paths for display
                for (size_t i = 0; i < std::min(size_t(5), candidatesWithScores.size()) &&
                                   candidatesWithScores[i].second == candidatesWithScores[0].second;
                     ++i) {
                    // Find the document path for this hash
                    for (const auto& doc : documentsResult.value()) {
                        if (doc.sha256Hash == candidatesWithScores[i].first) {
                            std::cerr << "  " << candidatesWithScores[i].first.substr(0, 12)
                                      << "... - " << doc.filePath
                                      << " (score: " << candidatesWithScores[i].second << ")"
                                      << std::endl;
                            break;
                        }
                    }
                }

                return Error{ErrorCode::InvalidOperation,
                             "Ambiguous fuzzy path match. Please be more specific or use "
                             "--latest/--oldest."};
            }
        }

        if (verbose_) {
            // Find the document path for display
            for (const auto& doc : documentsResult.value()) {
                if (doc.sha256Hash == candidatesWithScores[0].first) {
                    std::cerr << "Fuzzy path '" << pathQuery << "' matched: " << doc.filePath
                              << " (score: " << candidatesWithScores[0].second << ")" << std::endl;
                    break;
                }
            }
        }

        return candidatesWithScores[0].first;
    }

    Result<std::string> resolveWildcardPath(const std::string& pattern) {
        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
        }

        // Get all documents
        auto documentsResult = metadataRepo->findDocumentsByPath("%");
        if (!documentsResult) {
            return Error{ErrorCode::DatabaseError,
                         "Failed to query documents: " + documentsResult.error().message};
        }

        std::vector<metadata::DocumentInfo> matches;

        // Match documents against wildcard pattern
        for (const auto& doc : documentsResult.value()) {
            if (matchesWildcardPattern(doc.filePath, pattern)) {
                matches.push_back(doc);
            }
        }

        if (matches.empty()) {
            return Error{ErrorCode::NotFound, "No documents match wildcard pattern: " + pattern};
        }

        // Sort by indexed time (newest first by default, oldest first if --oldest flag is set)
        std::sort(matches.begin(), matches.end(),
                  [this](const metadata::DocumentInfo& a, const metadata::DocumentInfo& b) {
                      return getOldest_ ? (a.indexedTime < b.indexedTime)
                                        : (a.indexedTime > b.indexedTime);
                  });

        if (verbose_) {
            std::cerr << "Wildcard pattern '" << pattern << "' matched " << matches.size()
                      << " documents, returning " << (getOldest_ ? "oldest" : "newest") << ": "
                      << matches[0].filePath << std::endl;
        }

        return matches[0].sha256Hash;
    }

    bool matchesWildcardPattern(const std::string& path, const std::string& pattern) {
        // Simple wildcard matching for * and ? characters
        // This is a basic implementation - could be enhanced with more sophisticated glob matching

        size_t patternIdx = 0;
        size_t pathIdx = 0;
        size_t starIdx = std::string::npos;
        size_t matchIdx = 0;

        while (pathIdx < path.length()) {
            if (patternIdx < pattern.length() && pattern[patternIdx] == '*') {
                starIdx = patternIdx;
                matchIdx = pathIdx;
                patternIdx++;
            } else if (patternIdx < pattern.length() &&
                       (pattern[patternIdx] == '?' || pattern[patternIdx] == path[pathIdx])) {
                patternIdx++;
                pathIdx++;
            } else if (starIdx != std::string::npos) {
                patternIdx = starIdx + 1;
                matchIdx++;
                pathIdx = matchIdx;
            } else {
                return false;
            }
        }

        // Skip any trailing stars in pattern
        while (patternIdx < pattern.length() && pattern[patternIdx] == '*') {
            patternIdx++;
        }

        return patternIdx == pattern.length();
    }

    int calculateFuzzyPathScore(const std::vector<std::string>& queryComponents,
                                const std::vector<std::string>& docComponents) {
        int score = 0;

        // Exact suffix match gets highest score
        if (queryComponents.size() <= docComponents.size()) {
            bool exactSuffixMatch = true;
            for (size_t i = 0; i < queryComponents.size(); ++i) {
                size_t queryIdx = queryComponents.size() - 1 - i;
                size_t docIdx = docComponents.size() - 1 - i;
                if (queryComponents[queryIdx] != docComponents[docIdx]) {
                    exactSuffixMatch = false;
                    break;
                }
            }
            if (exactSuffixMatch) {
                return 1000 + static_cast<int>(queryComponents.size());
            }
        }

        // Partial matches - give points for each matching component
        for (const auto& queryComp : queryComponents) {
            for (const auto& docComp : docComponents) {
                if (queryComp == docComp) {
                    score += 100; // Exact component match
                } else if (docComp.find(queryComp) != std::string::npos) {
                    score += 50; // Substring match
                } else if (queryComp.find(docComp) != std::string::npos) {
                    score += 30; // Query contains doc component
                } else {
                    // Check for case-insensitive match
                    std::string lowerQuery = queryComp;
                    std::string lowerDoc = docComp;
                    std::transform(lowerQuery.begin(), lowerQuery.end(), lowerQuery.begin(),
                                   ::tolower);
                    std::transform(lowerDoc.begin(), lowerDoc.end(), lowerDoc.begin(), ::tolower);

                    if (lowerQuery == lowerDoc) {
                        score += 80; // Case-insensitive exact match
                    } else if (lowerDoc.find(lowerQuery) != std::string::npos) {
                        score += 40; // Case-insensitive substring match
                    }
                }
            }
        }

        // Bonus for matching last component (filename)
        if (!queryComponents.empty() && !docComponents.empty()) {
            const auto& lastQuery = queryComponents.back();
            const auto& lastDoc = docComponents.back();

            if (lastQuery == lastDoc) {
                score += 200; // Exact filename match
            } else if (lastDoc.find(lastQuery) != std::string::npos) {
                score += 100; // Filename contains query
            }
        }

        return score;
    }

    YamsCLI* cli_ = nullptr;
    std::string hash_;
    std::string name_;
    std::filesystem::path outputPath_;
    bool verbose_ = false;
    bool getLatest_ = false;
    bool getOldest_ = false;

    // File type filters
    std::string fileType_;
    std::string mimeType_;
    std::string extension_;
    bool binaryOnly_ = false;
    bool textOnly_ = false;
    bool raw_ = false;     // Output raw content without text extraction
    bool extract_ = false; // Force text extraction even when piping

    // Time filters
    std::string createdAfter_;
    std::string createdBefore_;
    std::string modifiedAfter_;
    std::string modifiedBefore_;
    std::string indexedAfter_;
    std::string indexedBefore_;

    // Knowledge graph options
    bool showGraph_ = false;
    int graphDepth_ = 1;

    // Session scoping
    std::optional<std::string> sessionOverride_{};
    bool noSession_{false};
    std::vector<std::string> sessionPatterns_;

    // Daemon streaming options
    bool metadataOnly_ = false;
    uint64_t maxBytes_ = 0;     // 0 = unlimited
    size_t chunkSize_ = 262144; // 256KB default
};

// Factory function
std::unique_ptr<ICommand> createGetCommand() {
    return std::make_unique<GetCommand>();
}

} // namespace yams::cli
