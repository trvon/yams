#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <yams/cli/command.h>
#include <yams/cli/result_renderer.h>
#include <yams/cli/yams_cli.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/profiling.h>
#include <yams/search/search_engine_builder.h>
#include <yams/search/search_executor.h>
#include <yams/vector/vector_index_manager.h>
// Daemon client API for daemon-first search
#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/response_of.hpp>

namespace yams::cli {

using json = nlohmann::json;

class SearchCommand : public ICommand {
public:
    std::string getName() const override { return "search"; }

    std::string getDescription() const override { return "Search documents by query"; }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("search", getDescription());
        cmd->add_option("query", query_, "Search query")->required();
        cmd->add_option("path", pathFilter_, "Filter results by path pattern (optional)");

        cmd->add_option("-l,--limit", limit_, "Maximum number of results")->default_val(20);

        cmd->add_option("-t,--type", searchType_, "Search type (keyword, semantic, hybrid)")
            ->default_val("keyword");

        cmd->add_flag("-f,--fuzzy", fuzzySearch_, "Enable fuzzy search for approximate matching");
        cmd->add_option("--similarity", minSimilarity_,
                        "Minimum similarity for fuzzy search (0.0-1.0)")
            ->default_val(0.7f);

        cmd->add_flag("--paths-only", pathsOnly_,
                      "Output only file paths, one per line (useful for scripting)");
        cmd->add_flag("--literal-text", literalText_,
                      "Treat query as literal text, not regex (escapes special characters)");
        cmd->add_flag("--show-hash", showHash_, "Show document hashes in results");
        cmd->add_flag("-v,--verbose", verbose_, "Show detailed information including full hashes");
        cmd->add_flag("--json", jsonOutput_, "Output results in JSON format");

        // Line-level search options
        cmd->add_flag("-n,--line-numbers", showLineNumbers_, "Show line numbers with matches");
        cmd->add_option("-A,--after", afterContext_, "Show N lines after match")->default_val(0);
        cmd->add_option("-B,--before", beforeContext_, "Show N lines before match")->default_val(0);
        cmd->add_option("-C,--context", context_, "Show N lines before and after match")
            ->default_val(0);

        cmd->add_option("--hash", hashQuery_,
                        "Search by file hash (full or partial, minimum 8 characters)");

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Command failed: {}", result.error().message);
                throw CLI::RuntimeError(1);
            }
        });
    }

    Result<void> execute() override {
        YAMS_ZONE_SCOPED_N("SearchCommand::execute");

        try {
            // Attempt daemon search first via generic helper; fall back to local on failure
            {
                yams::daemon::SearchRequest dreq;
                dreq.query = query_;
                dreq.limit = static_cast<size_t>(limit_);
                dreq.fuzzy = fuzzySearch_;
                dreq.literalText = literalText_;
                dreq.similarity = static_cast<double>(minSimilarity_);

                auto render = [&](const yams::daemon::SearchResponse& resp) -> Result<void> {
                    const auto& items = resp.results;
                    if (pathsOnly_) {
                        for (const auto& r : items) {
                            auto itPath = r.metadata.find("path");
                            if (itPath != r.metadata.end()) {
                                std::cout << itPath->second << std::endl;
                            } else if (!r.path.empty()) {
                                std::cout << r.path << std::endl;
                            } else {
                                std::cout << r.id << std::endl;
                            }
                        }
                        return Result<void>();
                    }
                    if (jsonOutput_ || verbose_) {
                        json output;
                        output["query"] = query_;
                        output["method"] = "daemon";
                        output["total_results"] = items.size();
                        output["returned"] = items.size();
                        json results = json::array();
                        for (const auto& r : items) {
                            json doc;
                            doc["id"] = r.id;
                            if (!r.title.empty())
                                doc["title"] = r.title;
                            if (!r.path.empty())
                                doc["path"] = r.path;
                            doc["score"] = r.score;
                            if (!r.snippet.empty())
                                doc["snippet"] = truncateSnippet(r.snippet, 200);
                            results.push_back(doc);
                        }
                        output["results"] = results;
                        std::cout << output.dump(2) << std::endl;
                    } else {
                        for (const auto& r : items) {
                            std::cout << r.score << "  ";
                            if (!r.path.empty()) {
                                std::cout << r.path;
                            } else if (!r.title.empty()) {
                                std::cout << r.title;
                            } else {
                                std::cout << r.id;
                            }
                            if (!r.snippet.empty()) {
                                std::cout << "\n    " << truncateSnippet(r.snippet, 200);
                            }
                            std::cout << "\n";
                        }
                    }
                    return Result<void>();
                };
                auto fallback = [&]() -> Result<void> {
                    // Signal to caller to use local fallback only for specific daemon errors
                    return Error{ErrorCode::NotImplemented, "daemon_unavailable"};
                };
                auto daemonResult = daemon_first(dreq, fallback, render);
                if (daemonResult) {
                    // Daemon path succeeded; avoid heavy local initialization
                    return Result<void>();
                }

                // Check if we should fallback to local search or propagate the error
                if (daemonResult.error().code == ErrorCode::NotImplemented &&
                    daemonResult.error().message == "daemon_unavailable") {
                    // Continue to local fallback for daemon unavailable
                    spdlog::debug("Daemon unavailable, falling back to local search");
                } else {
                    // Other daemon errors should be propagated without heavy local initialization
                    return daemonResult.error();
                }
            }
            // Daemon-first failed with retriable condition, use local fallback
            auto ensured = cli_->ensureStorageInitialized();
            if (!ensured) {
                return ensured;
            }

            // Handle hash search if --hash flag is provided
            if (!hashQuery_.empty()) {
                YAMS_ZONE_SCOPED_N("SearchCommand::hashSearch");
                if (!isValidHash(hashQuery_)) {
                    return Error{ErrorCode::InvalidArgument,
                                 "Invalid hash format. Must be 8-64 hexadecimal characters."};
                }
                searchByHash(hashQuery_);
                return Result<void>();
            }

            // Auto-detect hash format in query for backward compatibility
            if (query_.length() >= 8 && query_.length() <= 64 && isValidHash(query_)) {
                searchByHash(query_);
                return Result<void>();
            }

            // Pass raw query string - escaping happens at the search engine level
            std::string processedQuery = query_;

            // Check if we should use fuzzy search via metadata repository
            if (fuzzySearch_) {
                YAMS_ZONE_SCOPED_N("SearchCommand::fuzzySearch");
                YAMS_PLOT("SearchType", static_cast<int64_t>(1)); // 1 = fuzzy

                auto metadataRepo = cli_->getMetadataRepository();
                if (!metadataRepo) {
                    return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
                }

                // Handle context options
                if (context_ > 0) {
                    beforeContext_ = afterContext_ = context_;
                }

                // Execute fuzzy search
                auto searchResult =
                    metadataRepo->fuzzySearch(processedQuery, minSimilarity_, limit_);
                if (!searchResult) {
                    return Error{searchResult.error().code, searchResult.error().message};
                }

                // Apply tag filter if specified
                auto filteredResults = filterResultsByTags(searchResult.value(), metadataRepo);

                YAMS_PLOT("SearchResultCount",
                          static_cast<int64_t>(filteredResults.results.size()));
                outputFuzzyResults(filteredResults);
                return Result<void>();
            }

            // Prefer hybrid engine with KG enabled by default; fail open to metadata/FTS.
            auto metadataRepo = cli_->getMetadataRepository();
            if (metadataRepo) {
                // Build embedded HybridSearchEngine (keyword via MetadataRepository; KG if
                // available)
                try {
                    YAMS_ZONE_SCOPED_N("SearchCommand::hybridSearch");
                    YAMS_PLOT("SearchType", static_cast<int64_t>(2)); // 2 = hybrid

                    // Use shared VectorIndexManager from CLI instead of creating new instance
                    auto vecMgr = cli_->getVectorIndexManager();
                    if (!vecMgr) {
                        spdlog::warn(
                            "VectorIndexManager not available, falling back to metadata search");
                        // Fall through to metadata-based search below
                    } else {
                        // Avoid initializing a new heavy embedding pipeline here; reuse shared
                        // generator only if already available.
                        yams::search::SearchEngineBuilder builder;
                        builder.withVectorIndex(vecMgr)
                            .withMetadataRepo(metadataRepo)
                            .withKGStore(cli_->getKnowledgeGraphStore());
                        // Only use embedding generator if it already exists to avoid heavy
                        // initialization
                        if (cli_->hasEmbeddingGenerator()) {
                            if (auto gen = cli_->getEmbeddingGenerator()) {
                                builder.withEmbeddingGenerator(gen);
                            }
                        }

                        auto opts = yams::search::SearchEngineBuilder::BuildOptions::makeDefault();
                        opts.hybrid.final_top_k = static_cast<size_t>(limit_);
                        // Show method and score breakdown only when verbose/json requested
                        opts.hybrid.generate_explanations = verbose_ || jsonOutput_;

                        auto engRes = builder.buildEmbedded(opts);
                        if (engRes) {
                            auto eng = engRes.value();
                            auto hres = eng->search(processedQuery, opts.hybrid.final_top_k);
                            if (hres) {
                                const auto& items = hres.value();
                                YAMS_PLOT("SearchResultCount", static_cast<int64_t>(items.size()));

                                // Handle paths-only output first
                                if (pathsOnly_) {
                                    for (const auto& r : items) {
                                        auto itPath = r.metadata.find("path");
                                        if (itPath != r.metadata.end()) {
                                            std::cout << itPath->second << std::endl;
                                        } else {
                                            std::cout << r.id << std::endl;
                                        }
                                    }
                                    return Result<void>();
                                }

                                // Output in JSON if --json flag is set, or if --verbose is set
                                if (jsonOutput_ || verbose_) {
                                    json output;
                                    output["query"] = query_;
                                    output["method"] = "hybrid";
                                    output["kg_enabled"] = eng->getConfig().enable_kg;
                                    output["total_results"] = items.size();
                                    output["returned"] = items.size();

                                    json results = json::array();
                                    for (const auto& r : items) {
                                        json doc;
                                        doc["id"] = r.id;
                                        // Prefer adapter-provided metadata
                                        auto itTitle = r.metadata.find("title");
                                        if (itTitle != r.metadata.end())
                                            doc["title"] = itTitle->second;
                                        auto itPath = r.metadata.find("path");
                                        if (itPath != r.metadata.end())
                                            doc["path"] = itPath->second;
                                        doc["score"] = r.hybrid_score;
                                        if (!r.content.empty()) {
                                            doc["snippet"] = truncateSnippet(r.content, 200);
                                        }
                                        if (verbose_) {
                                            json breakdown;
                                            breakdown["vector_score"] = r.vector_score;
                                            breakdown["keyword_score"] = r.keyword_score;
                                            breakdown["kg_entity_score"] = r.kg_entity_score;
                                            breakdown["structural_score"] = r.structural_score;
                                            doc["score_breakdown"] = breakdown;
                                        }
                                        results.push_back(doc);
                                    }
                                    output["results"] = results;
                                    std::cout << output.dump(2) << std::endl;
                                } else {
                                    // Simple, concise output by default
                                    if (items.empty()) {
                                        std::cout << "No results found for: " << query_
                                                  << std::endl;
                                    } else {
                                        std::cout << "Found " << items.size()
                                                  << " result(s) for: " << query_ << std::endl;
                                        std::cout << std::endl;

                                        for (size_t i = 0; i < items.size(); i++) {
                                            const auto& r = items[i];
                                            std::cout << (i + 1) << ". ";
                                            auto itTitle = r.metadata.find("title");
                                            if (itTitle != r.metadata.end()) {
                                                std::cout << itTitle->second;
                                            } else {
                                                std::cout << r.id;
                                            }
                                            auto itPath = r.metadata.find("path");
                                            if (itPath != r.metadata.end()) {
                                                std::cout << " (" << itPath->second << ")";
                                            }
                                            std::cout << std::endl;

                                            if (!r.content.empty()) {
                                                std::cout << "   "
                                                          << truncateSnippet(r.content, 200)
                                                          << std::endl;
                                            }
                                            std::cout << std::endl;
                                        }
                                    }
                                }

                                return Result<void>();
                            } else {
                                spdlog::warn("Hybrid search failed: {}", hres.error().message);
                            }
                        } else {
                            spdlog::warn("Hybrid engine initialization failed: {}",
                                         engRes.error().message);
                        }
                    } // end of vecMgr check
                } catch (const std::exception& e) {
                    spdlog::warn("Hybrid engine error (fallback to metadata): {}", e.what());
                }

                // Fallback: Execute metadata-based search
                YAMS_ZONE_SCOPED_N("SearchCommand::metadataSearch");
                YAMS_PLOT("SearchType", static_cast<int64_t>(3)); // 3 = metadata/FTS

                auto searchResult = metadataRepo->search(query_, limit_, 0);
                if (!searchResult) {
                    return Error{searchResult.error().code, searchResult.error().message};
                }

                YAMS_PLOT("SearchResultCount",
                          static_cast<int64_t>(searchResult.value().results.size()));
                outputMetadataResults(searchResult.value());
                return Result<void>();
            }

            // Final fallback to SearchExecutor (or primary path when --literal-text)
            auto searchExecutor = cli_->getSearchExecutor();
            if (!searchExecutor) {
                return Error{ErrorCode::NotInitialized, "Search executor not initialized"};
            }

            // Build search request
            search::SearchRequest request;
            request.query = query_;
            request.limit = limit_;
            request.offset = 0;
            request.includeHighlights = true;
            request.includeSnippets = true;
            request.literalText = literalText_; // Pass the literal text flag

            // Execute search
            auto result = searchExecutor->search(request);
            if (!result) {
                return Error{result.error().code, result.error().message};
            }

            auto& response = result.value();

            // Output results using helper
            outputSearchResults(response);

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
        }
    }

private:
    void displayLineContext(const metadata::DocumentInfo& doc) {
        // Retrieve document content for line-level display
        auto store = cli_->getContentStore();
        if (!store) {
            return;
        }

        auto contentResult = store->retrieveBytes(doc.sha256Hash);
        if (!contentResult) {
            return;
        }

        std::string content(reinterpret_cast<const char*>(contentResult.value().data()),
                            contentResult.value().size());

        // Find matches and display with context
        std::istringstream stream(content);
        std::string line;
        std::vector<std::string> lines;

        // Store all lines
        while (std::getline(stream, line)) {
            lines.push_back(line);
        }

        // Find matching lines
        for (size_t i = 0; i < lines.size(); ++i) {
            if (lines[i].find(query_) != std::string::npos) {
                // Show context before
                size_t startLine = (i > beforeContext_) ? i - beforeContext_ : 0;
                size_t endLine = std::min(i + afterContext_ + 1, lines.size());

                for (size_t j = startLine; j < endLine; ++j) {
                    if (showLineNumbers_) {
                        std::cout << std::setw(6) << (j + 1) << ": ";
                    }

                    // Highlight matching line
                    if (j == i) {
                        std::cout << "\033[1;32m" << lines[j] << "\033[0m" << std::endl;
                    } else {
                        std::cout << lines[j] << std::endl;
                    }
                }

                if (endLine < lines.size() - 1 && i + afterContext_ + 1 < lines.size()) {
                    std::cout << "--" << std::endl;
                }
            }
        }
    }

    void outputSearchResults(const search::SearchResults& response) {
        auto format = pathsOnly_ ? OutputFormat::PathsOnly
                                 : (jsonOutput_ ? OutputFormat::JSON : OutputFormat::Verbose);
        auto renderer = createHybridSearchResultRenderer(pathFilter_, format, showHash_, verbose_);
        renderer.render(response.getStatistics().originalQuery, "hybrid", response.getItems(),
                        response.getStatistics().totalResults,
                        response.getStatistics().totalTime.count());
    }

    // Helper function to detect if a string is a SHA256 hash
    bool isValidHash(const std::string& str) {
        // Must be 8-64 hex characters
        if (str.length() < 8 || str.length() > 64) {
            return false;
        }

        // Check if all characters are hex
        for (char c : str) {
            if (!std::isxdigit(c)) {
                return false;
            }
        }

        return true;
    }

    // Helper function to search by hash with support for partial matching
    void searchByHash(const std::string& hash) {
        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            std::cerr << "Error: Metadata repository not initialized" << std::endl;
            return;
        }

        // If it's a full 64-character hash, do exact lookup
        if (hash.length() == 64) {
            auto docResult = metadataRepo->getDocumentByHash(hash);
            if (!docResult) {
                std::cerr << "Error: " << docResult.error().message << std::endl;
                return;
            }

            if (!docResult.value()) {
                std::cout << "No document found with hash: " << hash << std::endl;
                return;
            }

            // Display the single result
            const auto& doc = docResult.value().value();

            // Handle paths-only output
            if (pathsOnly_) {
                std::cout << (!doc.filePath.empty() && doc.filePath != "stdin" ? doc.filePath
                                                                               : doc.fileName)
                          << std::endl;
                return;
            }

            std::cout << "Found document for hash: " << hash.substr(0, 8) << "..." << std::endl;
            std::cout << std::endl;

            if (verbose_) {
                std::cout << "1. " << doc.fileName << std::endl;
                std::cout << "   Path: " << doc.filePath << std::endl;
                std::cout << "   Hash: " << doc.sha256Hash << std::endl;
                std::cout
                    << "   Size: "
                    << yams::cli::ResultRenderer<yams::cli::DocumentInfoResultAdapter>::formatSize(
                           doc.fileSize)
                    << " | Type: " << doc.mimeType << std::endl;
            } else {
                std::cout << "1. " << doc.fileName << std::endl;
                std::cout << "   " << doc.sha256Hash.substr(0, 16) << "..." << std::endl;
            }
            std::cout << std::endl;
        } else {
            // Partial hash search - need to search all documents
            auto queryResult = metadataRepo->findDocumentsByPath("%");
            if (!queryResult) {
                std::cerr << "Error: " << queryResult.error().message << std::endl;
                return;
            }

            std::vector<metadata::DocumentInfo> matches;
            for (const auto& doc : queryResult.value()) {
                if (doc.sha256Hash.substr(0, hash.length()) == hash) {
                    matches.push_back(doc);
                    if (matches.size() >= static_cast<size_t>(limit_))
                        break; // Respect limit
                }
            }

            if (matches.empty()) {
                std::cout << "No documents found with hash prefix: " << hash << std::endl;
                return;
            }

            // Handle paths-only output
            if (pathsOnly_) {
                for (const auto& doc : matches) {
                    std::cout << (!doc.filePath.empty() && doc.filePath != "stdin" ? doc.filePath
                                                                                   : doc.fileName)
                              << std::endl;
                }
                return;
            }

            std::cout << "Found " << matches.size() << " document(s) with hash prefix: " << hash
                      << std::endl;
            std::cout << std::endl;

            for (size_t i = 0; i < matches.size(); i++) {
                const auto& doc = matches[i];
                if (verbose_) {
                    std::cout << (i + 1) << ". " << doc.fileName << std::endl;
                    std::cout << "   Path: " << doc.filePath << std::endl;
                    std::cout << "   Hash: " << doc.sha256Hash << std::endl;
                    std::cout << "   Size: "
                              << yams::cli::ResultRenderer<
                                     yams::cli::DocumentInfoResultAdapter>::formatSize(doc.fileSize)
                              << " | Type: " << doc.mimeType << std::endl;
                } else {
                    std::cout << (i + 1) << ". " << doc.fileName << std::endl;
                    std::cout << "   " << doc.sha256Hash.substr(0, 16) << "..." << std::endl;
                }
                std::cout << std::endl;
            }
        }
    }

    // Helper function to truncate snippet to a maximum length at word boundary
    std::string truncateSnippet(const std::string& snippet, size_t maxLength) {
        // Remove HTML tags and clean up the snippet
        std::string cleaned;
        bool inTag = false;
        for (char c : snippet) {
            if (c == '<') {
                inTag = true;
            } else if (c == '>') {
                inTag = false;
            } else if (!inTag) {
                // Replace newlines and multiple spaces with single space
                if (c == '\n' || c == '\r' || c == '\t') {
                    if (!cleaned.empty() && cleaned.back() != ' ') {
                        cleaned += ' ';
                    }
                } else {
                    cleaned += c;
                }
            }
        }

        // Remove multiple consecutive spaces
        std::string result;
        bool lastWasSpace = false;
        for (char c : cleaned) {
            if (c == ' ') {
                if (!lastWasSpace) {
                    result += c;
                    lastWasSpace = true;
                }
            } else {
                result += c;
                lastWasSpace = false;
            }
        }

        // Trim to max length at word boundary
        if (result.length() <= maxLength) {
            return result;
        }

        // Find last space before maxLength
        size_t lastSpace = result.rfind(' ', maxLength);
        if (lastSpace != std::string::npos && lastSpace > maxLength * 0.7) {
            // Found a reasonable word boundary
            return result.substr(0, lastSpace);
        }

        // No good word boundary, just truncate
        return result.substr(0, maxLength);
    }

    [[nodiscard]] constexpr bool matchesPathFilter(std::string_view path) const noexcept {
        if (pathFilter_.empty()) {
            return true; // No filter, include all
        }
        return path.find(pathFilter_) != std::string_view::npos;
    }

    [[nodiscard]] constexpr std::string_view
    getDisplayPath(const metadata::DocumentInfo& doc) const noexcept {
        return (!doc.filePath.empty() && doc.filePath != "stdin") ? std::string_view{doc.filePath}
                                                                  : std::string_view{doc.fileName};
    }

    void outputFuzzyResults(const metadata::SearchResults& results) {
        // Handle error case
        if (!results.errorMessage.empty()) {
            std::cerr << "Fuzzy search error: " << results.errorMessage << std::endl;
            return;
        }

        auto format = pathsOnly_ ? OutputFormat::PathsOnly
                                 : (jsonOutput_ ? OutputFormat::JSON : OutputFormat::Verbose);
        auto renderer = createSearchResultRenderer(pathFilter_, format, showHash_, verbose_);
        renderer.render(results.query, "fuzzy", results.results, results.totalCount,
                        results.executionTimeMs);
    }

    void outputMetadataResults(const metadata::SearchResults& results) {
        // Handle error case
        if (!results.errorMessage.empty()) {
            std::cerr << "Search error: " << results.errorMessage << std::endl;
            return;
        }

        auto format = pathsOnly_ ? OutputFormat::PathsOnly
                                 : (jsonOutput_ ? OutputFormat::JSON : OutputFormat::Verbose);
        auto renderer = createSearchResultRenderer(pathFilter_, format, showHash_, verbose_);
        renderer.render(results.query, "full-text", results.results, results.totalCount,
                        results.executionTimeMs);
    }

private:
    YamsCLI* cli_ = nullptr;
    std::string query_;
    std::string pathFilter_;
    size_t limit_ = 20;
    std::string searchType_ = "keyword";
    bool fuzzySearch_ = false;
    float minSimilarity_ = 0.7f;
    bool pathsOnly_ = false;
    bool literalText_ = false;
    bool showHash_ = false;
    bool verbose_ = false;
    bool jsonOutput_ = false;
    std::string hashQuery_;

    // Line-level search options
    bool showLineNumbers_ = false;
    size_t beforeContext_ = 0;
    size_t afterContext_ = 0;
    size_t context_ = 0;

    // Tag filtering
    std::string filterTags_;
    bool matchAllTags_ = false;

    // Helper method to filter search results by tags
    metadata::SearchResults
    filterResultsByTags(const metadata::SearchResults& results,
                        std::shared_ptr<metadata::IMetadataRepository> metadataRepo) {
        if (filterTags_.empty()) {
            return results;
        }

        // Parse comma-separated tags
        std::vector<std::string> tags;
        std::stringstream ss(filterTags_);
        std::string tag;
        while (std::getline(ss, tag, ',')) {
            // Trim whitespace
            tag.erase(0, tag.find_first_not_of(" \t"));
            tag.erase(tag.find_last_not_of(" \t") + 1);
            if (!tag.empty()) {
                tags.push_back(tag);
            }
        }

        if (tags.empty()) {
            return results;
        }

        // Filter results
        metadata::SearchResults filtered = results;
        filtered.results.clear();

        for (const auto& result : results.results) {
            // Get tags for this document
            auto docTagsResult = metadataRepo->getDocumentTags(result.document.id);
            if (!docTagsResult) {
                continue; // Skip documents we can't get tags for
            }

            const auto& docTags = docTagsResult.value();
            bool matches = false;

            if (matchAllTags_) {
                // Check if document has all required tags
                matches = true;
                for (const auto& requiredTag : tags) {
                    if (std::find(docTags.begin(), docTags.end(), requiredTag) == docTags.end()) {
                        matches = false;
                        break;
                    }
                }
            } else {
                // Check if document has any of the required tags
                for (const auto& requiredTag : tags) {
                    if (std::find(docTags.begin(), docTags.end(), requiredTag) != docTags.end()) {
                        matches = true;
                        break;
                    }
                }
            }

            if (matches) {
                filtered.results.push_back(result);
            }
        }

        filtered.totalCount = filtered.results.size();
        return filtered;
    }
};

// Factory function
std::unique_ptr<ICommand> createSearchCommand() {
    return std::make_unique<SearchCommand>();
}

} // namespace yams::cli
