#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <yams/app/services/services.hpp>
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
#include <yams/cli/async_bridge.h>

namespace yams::cli {

using json = nlohmann::json;

class SearchCommand : public ICommand {
private:
    // Streaming configuration
    bool enableStreaming_ = true;
    int headerTimeoutMs_ = 30000;
    int bodyTimeoutMs_ = 60000;
    int chunkSize_ = 64 * 1024;
    // Default to non-streaming for best-effort reliability; users can opt back in
    bool disableStreaming_ = true;

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

    // Include globs filtering
    std::vector<std::string> includeGlobs_;

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
                if (c == ' ' || c == ' ' || c == '\t') {
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
            return result.substr(0, lastSpace) + "...";
        }

        // Fallback to simple truncation
        return result.substr(0, maxLength - 3) + "...";
    }

    static std::string trim(const std::string& s) {
        auto start = s.find_first_not_of(" \t\n\r");
        if (start == std::string::npos)
            return std::string();
        auto end = s.find_last_not_of(" \t\n\r");
        return s.substr(start, end - start + 1);
    }

    static std::vector<std::string> splitCommaPatterns(const std::vector<std::string>& inputs) {
        std::vector<std::string> out;
        for (const auto& entry : inputs) {
            std::stringstream ss(entry);
            std::string tok;
            while (std::getline(ss, tok, ',')) {
                auto t = trim(tok);
                if (!t.empty())
                    out.push_back(t);
            }
        }
        return out;
    }

    static bool matchAnyGlob(const std::string& path, const std::vector<std::string>& globs) {
        if (globs.empty())
            return true;
        // Use existing glob matcher from services utils
        for (const auto& g : globs) {
            if (yams::app::services::utils::matchGlob(path, g))
                return true;
        }
        return false;
    }

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

        cmd->add_option("--header-timeout", headerTimeoutMs_,
                        "Timeout for receiving response headers (milliseconds)")
            ->default_val(30000);

        cmd->add_option("--body-timeout", bodyTimeoutMs_,
                        "Timeout for receiving response body (milliseconds)")
            ->default_val(60000);

        cmd->add_flag("--no-streaming", disableStreaming_,
                      "Disable streaming responses (progressive output)")
            ->default_val(false);
        cmd->add_option("--chunk-size", chunkSize_,
                        "Size of chunks for streaming responses (bytes)")
            ->default_val(64 * 1024);
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

        // Tag filtering options
        cmd->add_option("--tags", filterTags_,
                        "Filter results by tags (comma-separated, e.g., work,important)");
        cmd->add_flag("--match-all-tags", matchAllTags_,
                      "Require all specified tags (default: match any)");

        // Include globs
        cmd->add_option("--include", includeGlobs_,
                        "Limit results to paths matching globs (comma-separated or repeated). "
                        "Examples: \"*.cpp\", \"*.{cpp,hpp}\", \"src/**/*.cpp,include/**/*.hpp\"");

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
            // Normalize include globs (split commas)
            auto includeGlobsExpanded = splitCommaPatterns(includeGlobs_);
            if (includeGlobsExpanded.empty() && !pathFilter_.empty()) {
                includeGlobsExpanded.push_back(pathFilter_);
            }

            // Attempt daemon-first using a single DaemonClient instance (no pool).
            // Keep it simple and deterministic for reliability.
            yams::daemon::DaemonClient::setTimeoutEnvVars(
                std::chrono::milliseconds(headerTimeoutMs_),
                std::chrono::milliseconds(bodyTimeoutMs_));

            yams::daemon::ClientConfig clientConfig;
            clientConfig.headerTimeout = std::chrono::milliseconds(headerTimeoutMs_);
            clientConfig.bodyTimeout = std::chrono::milliseconds(bodyTimeoutMs_);
            clientConfig.requestTimeout = std::chrono::milliseconds(30000);
            clientConfig.enableChunkedResponses = !disableStreaming_ && enableStreaming_;
            clientConfig.progressiveOutput = true;
            clientConfig.maxChunkSize = chunkSize_;
            clientConfig.singleUseConnections = true; // close after each request
            yams::daemon::DaemonClient client(clientConfig);
            client.setStreamingEnabled(clientConfig.enableChunkedResponses);

            // Create search request
            yams::daemon::SearchRequest dreq;
            dreq.query = query_;
            dreq.limit = static_cast<size_t>(limit_);
            dreq.fuzzy = fuzzySearch_;
            dreq.literalText = literalText_;
            dreq.similarity = static_cast<double>(minSimilarity_);
            dreq.pathsOnly = pathsOnly_;
            dreq.searchType = searchType_;
            dreq.jsonOutput = jsonOutput_;
            dreq.showHash = showHash_;
            dreq.verbose = verbose_;
            dreq.showLineNumbers = showLineNumbers_;
            // Note: daemon protocol currently lacks includeGlobs; apply client-side filtering below

            auto render = [&](const yams::daemon::SearchResponse& resp) -> Result<void> {
                // Apply client-side filtering if include globs present
                std::vector<yams::daemon::SearchResult> items;
                items.reserve(resp.results.size());
                if (!includeGlobsExpanded.empty()) {
                    for (const auto& r : resp.results) {
                        std::string path = !r.path.empty()
                                               ? r.path
                                               : (r.metadata.count("path") ? r.metadata.at("path")
                                                                           : std::string());
                        if (matchAnyGlob(path, includeGlobsExpanded)) {
                            items.push_back(r);
                        }
                    }
                } else {
                    items = resp.results;
                }

                if (pathsOnly_) {
                    if (!enableStreaming_) {
                        if (items.empty()) {
                            std::cout << "(no results)" << std::endl;
                        } else {
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
                    // Enrich with extraction progress indicator
                    try {
                        yams::daemon::GetStatsRequest sreq;
                        auto sres = run_sync(client.getStats(sreq), std::chrono::seconds(5));
                        if (sres) {
                            const auto& s = sres.value();
                            size_t total = s.totalDocuments;
                            // Prefer detailed additionalStats if present
                            size_t indexed = 0;
                            if (s.additionalStats.count("meta_indexed_documents")) {
                                indexed = std::stoull(s.additionalStats.at("meta_indexed_documents"));
                            } else {
                                indexed = s.indexedDocuments;
                            }
                            if (total >= indexed) {
                                output["extraction_pending"] = static_cast<uint64_t>(total - indexed);
                                output["documents_total"] = static_cast<uint64_t>(total);
                                output["documents_extracted"] = static_cast<uint64_t>(indexed);
                            }
                        }
                    } catch (...) {
                        // Best-effort diagnostics only
                    }
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

                    if (!items.empty()) {
                        std::cout << "Found " << resp.totalCount << " results in "
                                  << resp.elapsed.count() << "ms" << std::endl;
                    }
                }
                return Result<void>();
            };
            auto fallback = [&]() -> Result<void> {
                // Use app services for local fallback
                auto appContext = cli_->getAppContext();
                if (!appContext) {
                    return Error{ErrorCode::NotInitialized, "Failed to initialize app context"};
                }

                auto searchService = app::services::makeSearchService(*appContext);
                if (!searchService) {
                    return Error{ErrorCode::NotInitialized, "Failed to create search service"};
                }

                // Map CLI arguments to service request
                app::services::SearchRequest sreq;
                sreq.query = query_;
                sreq.limit = limit_;
                sreq.fuzzy = fuzzySearch_;
                sreq.similarity = minSimilarity_;
                sreq.hash = hashQuery_;
                sreq.type = searchType_;
                sreq.verbose = verbose_;
                sreq.literalText = literalText_;
                sreq.showHash = showHash_;
                sreq.pathsOnly = pathsOnly_;
                sreq.showLineNumbers = showLineNumbers_;
                sreq.beforeContext = static_cast<int>(beforeContext_);
                sreq.afterContext = static_cast<int>(afterContext_);
                sreq.context = static_cast<int>(context_);
                sreq.pathPattern = pathFilter_;

                // Parse tags from comma-separated string
                if (!filterTags_.empty()) {
                    std::stringstream ss(filterTags_);
                    std::string tag;
                    while (std::getline(ss, tag, ',')) {
                        // Trim whitespace
                        tag.erase(0, tag.find_first_not_of(" \t"));
                        tag.erase(tag.find_last_not_of(" \t") + 1);
                        if (!tag.empty()) {
                            sreq.tags.push_back(tag);
                        }
                    }
                    sreq.matchAllTags = matchAllTags_;
                }

                auto result = searchService->search(sreq);
                if (!result) {
                    return result.error();
                }

                auto resp = result.value();

                // Apply client-side include filtering if needed
                if (!includeGlobsExpanded.empty()) {
                    if (pathsOnly_) {
                        std::vector<std::string> filtered;
                        for (const auto& p : resp.paths) {
                            if (matchAnyGlob(p, includeGlobsExpanded))
                                filtered.push_back(p);
                        }
                        resp.paths.swap(filtered);
                    } else {
                        std::vector<app::services::SearchItem> filtered;
                        for (const auto& it : resp.results) {
                            if (matchAnyGlob(it.path, includeGlobsExpanded))
                                filtered.push_back(it);
                        }
                        resp.results.swap(filtered);
                    }
                }

                // Handle paths-only output
                if (pathsOnly_) {
                    for (const auto& path : resp.paths) {
                        std::cout << path << std::endl;
                    }
                    return Result<void>();
                }

                // Output results (JSON or table format)
                if (jsonOutput_ || verbose_) {
                    json output;
                    output["query"] = query_;
                    output["method"] = resp.type;
                    output["total_results"] = resp.total;
                    output["execution_time_ms"] = resp.executionTimeMs;
                    // Extraction progress indicator from stats
                    try {
                        yams::daemon::GetStatsRequest sreq;
                        auto sres = run_sync(client.getStats(sreq), std::chrono::seconds(5));
                        if (sres) {
                            const auto& s = sres.value();
                            size_t total = s.totalDocuments;
                            size_t indexed = 0;
                            if (s.additionalStats.count("meta_indexed_documents")) {
                                indexed = std::stoull(s.additionalStats.at("meta_indexed_documents"));
                            } else {
                                indexed = s.indexedDocuments;
                            }
                            if (total >= indexed) {
                                output["extraction_pending"] = static_cast<uint64_t>(total - indexed);
                                output["documents_total"] = static_cast<uint64_t>(total);
                                output["documents_extracted"] = static_cast<uint64_t>(indexed);
                            }
                        }
                    } catch (...) {
                    }

                    json results = json::array();
                    for (const auto& item : resp.results) {
                        json doc;
                        doc["id"] = item.id;
                        if (!item.title.empty())
                            doc["title"] = item.title;
                        if (!item.path.empty())
                            doc["path"] = item.path;
                        if (showHash_ && !item.hash.empty())
                            doc["hash"] = item.hash;
                        doc["score"] = item.score;
                        if (!item.snippet.empty())
                            doc["snippet"] = item.snippet;

                        if (verbose_ && (item.vectorScore || item.keywordScore ||
                                         item.kgEntityScore || item.structuralScore)) {
                            json breakdown;
                            if (item.vectorScore)
                                breakdown["vector_score"] = *item.vectorScore;
                            if (item.keywordScore)
                                breakdown["keyword_score"] = *item.keywordScore;
                            if (item.kgEntityScore)
                                breakdown["kg_entity_score"] = *item.kgEntityScore;
                            if (item.structuralScore)
                                breakdown["structural_score"] = *item.structuralScore;
                            doc["score_breakdown"] = breakdown;
                        }
                        results.push_back(doc);
                    }
                    output["results"] = results;
                    std::cout << output.dump(2) << std::endl;
                } else {
                    // Simple output
                    for (const auto& item : resp.results) {
                        std::cout << item.score << "  ";
                        if (!item.path.empty()) {
                            std::cout << item.path;
                        } else if (!item.title.empty()) {
                            std::cout << item.title;
                        } else {
                            std::cout << item.id;
                        }
                        if (showHash_ && !item.hash.empty()) {
                            std::cout << " [" << item.hash.substr(0, 8) << "]";
                        }
                        if (!item.snippet.empty()) {
                            std::cout << "\n    " << item.snippet;
                        }
                        std::cout << "\n";
                    }
                }

                return Result<void>();
            };
            // Call daemon directly; stream when enabled, otherwise unary.
            auto daemonResult = clientConfig.enableChunkedResponses
                                    ? run_sync(client.streamingSearch(dreq), std::chrono::seconds(30))
                                    : run_sync(client.call(dreq), std::chrono::seconds(30));
            
            if (!daemonResult && clientConfig.enableChunkedResponses) {
                // Transitional fallback: if streaming timed out waiting for header/chunks,
                // retry a unary call with a longer header timeout to allow full compute.
                const auto& err = daemonResult.error();
                if (err.code == ErrorCode::Timeout && err.message.find("Read timeout") != std::string::npos) {
                    spdlog::warn("Streaming search timed out; retrying unary path with extended header timeout");
                    // Bump header timeout to body timeout for unary retry
                    client.setHeaderTimeout(std::chrono::milliseconds(bodyTimeoutMs_));
                    auto unaryRetry = run_sync(client.call(dreq), std::chrono::seconds(60));
                    if (unaryRetry) {
                        auto r = render(unaryRetry.value());
                        if (!r) return r.error();
                        return Result<void>();
                    }
                }
            }
            if (daemonResult) {
                auto r = render(daemonResult.value());
                if (!r) return r.error();
                return Result<void>();
            }
            // Fallback to local on error
            auto fb = fallback();
            if (!fb) return fb.error();
            return Result<void>();
            
        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
        }
    }
};

// Factory function
std::unique_ptr<ICommand> createSearchCommand() {
    return std::make_unique<SearchCommand>();
}

} // namespace yams::cli
