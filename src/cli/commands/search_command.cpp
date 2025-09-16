#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <yams/app/services/services.hpp>
#include <yams/cli/command.h>
#include <yams/cli/result_renderer.h>
#include <yams/cli/session_store.h>
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
// Async helpers (interim bridge-free execution)
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/system_executor.hpp>
// Timers and coroutine executor helpers for guard race
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>

// Hot/Cold mode helpers (env-driven)
#include <future>
#include "hot_cold_utils.h"

namespace yams::cli {

using json = nlohmann::json;

class SearchCommand : public ICommand {
private:
    // Streaming configuration
    bool enableStreaming_ = true;
    int headerTimeoutMs_ = 15000;
    int bodyTimeoutMs_ = 60000;
    int chunkSize_ = 64 * 1024;
    // Default to streaming; users can opt out with --no-streaming
    bool disableStreaming_ = false;

    YamsCLI* cli_ = nullptr;
    std::string query_;
    bool readStdin_ = false;
    std::string queryFile_;
    std::vector<std::string> extraArgs_;
    std::string pathFilter_;
    size_t limit_ = 20;
    std::string searchType_ = "hybrid";
    // Default list mode derived from env; currently not used directly here but kept for consistency
    yams::cli::HotColdMode listMode_ = yams::cli::getListMode();
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

    // File-type and MIME filtering
    std::string extension_;
    std::string mimeType_;
    std::string fileType_;
    bool textOnly_{false};
    bool binaryOnly_{false};

    // Time filters
    std::string createdAfter_;
    std::string createdBefore_;
    std::string modifiedAfter_;
    std::string modifiedBefore_;
    std::string indexedAfter_;
    std::string indexedBefore_;

    // Session scoping controls
    std::optional<std::string> sessionOverride_{};
    bool noSession_{false};
    // Force thorough, non-streaming execution (disables streaming guard)
    bool cold_{false};

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
        // Removed positional query to avoid option-name collision; prefer -q/--query or extras
        // Provide flagged alias to safely pass queries beginning with '-'
        cmd->add_option("-q,--query", query_, "Search query (use when the query starts with '-')");
        cmd->add_option("--path", pathFilter_, "Filter results by path pattern (optional)");
        // Query input sources
        cmd->add_flag("--stdin", readStdin_, "Read query from STDIN if not provided");
        cmd->add_option("--query-file", queryFile_,
                        "Read query from file path (use '-' to read from STDIN)");
        cmd->allow_extras();

        cmd->add_option("-l,--limit", limit_, "Maximum number of results")->default_val(20);

        cmd->add_option("-t,--type", searchType_, "Search type (keyword, semantic, hybrid)")
            ->default_val("hybrid");

        cmd->add_flag("-f,--fuzzy", fuzzySearch_, "Enable fuzzy search for approximate matching");
        cmd->add_option("--similarity", minSimilarity_,
                        "Minimum similarity for fuzzy search (0.0-1.0)")
            ->default_val(0.7f);

        cmd->add_flag("--paths-only", pathsOnly_,
                      "Output only file paths, one per line (useful for scripting)");

        // Session scoping controls
        cmd->add_option("--session", sessionOverride_, "Use this session for scoping");
        cmd->add_flag("--no-session", noSession_, "Bypass session scoping");

        cmd->add_option("--header-timeout", headerTimeoutMs_,
                        "Timeout for receiving response headers (milliseconds)")
            ->default_val(15000);

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

        // Execution mode
        cmd->add_flag("--cold", cold_, "Force thorough (non-streaming) execution");

        // Tag filtering options
        cmd->add_option("--tags", filterTags_,
                        "Filter results by tags (comma-separated, e.g., work,important)");
        cmd->add_flag("--match-all-tags", matchAllTags_,
                      "Require all specified tags (default: match any)");

        // Include globs
        cmd->add_option("--include", includeGlobs_,
                        "Limit results to paths matching globs (comma-separated or repeated). "
                        "Examples: \"*.cpp\", \"*.{cpp,hpp}\", \"src/**/*.cpp,include/**/*.hpp\"");

        // File-type and MIME filters
        cmd->add_option("--ext,--extension", extension_,
                        "Filter by file extension (e.g., .cpp or cpp)");
        cmd->add_option("--mime,--mime-type", mimeType_, "Filter by MIME type (e.g., text/plain)");
        cmd->add_option("--file-type", fileType_,
                        "Filter by file type (text, binary, image, document, archive, audio, "
                        "video, executable)");
        cmd->add_flag("--text-only", textOnly_, "Limit results to text documents");
        cmd->add_flag("--binary-only", binaryOnly_, "Limit results to binary documents");

        // Time filters (ISO 8601, relative like 7d, or natural like 'yesterday')
        cmd->add_option("--created-after", createdAfter_, "Created after (ISO/relative/natural)");
        cmd->add_option("--created-before", createdBefore_,
                        "Created before (ISO/relative/natural)");
        cmd->add_option("--modified-after", modifiedAfter_,
                        "Modified after (ISO/relative/natural)");
        cmd->add_option("--modified-before", modifiedBefore_,
                        "Modified before (ISO/relative/natural)");
        cmd->add_option("--indexed-after", indexedAfter_, "Indexed after (ISO/relative/natural)");
        cmd->add_option("--indexed-before", indexedBefore_,
                        "Indexed before (ISO/relative/natural)");

        cmd->callback([this, cmd]() {
            // Capture extras for folding into the query string
            this->extraArgs_ = cmd->remaining();
            cli_->setPendingCommand(this);
            if (cold_) {
                // When --cold is provided, prefer non-streaming thorough execution
                disableStreaming_ = true;
            }
        });
    }

    Result<void> execute() override {
        YAMS_ZONE_SCOPED_N("SearchCommand::execute");

        try {
            // Resolve base query from flags/stdin/file
            if (query_.empty()) {
                if (!queryFile_.empty() && queryFile_ != "-") {
                    std::ifstream fin(queryFile_, std::ios::in | std::ios::binary);
                    if (!fin.good()) {
                        return Error{ErrorCode::InvalidArgument,
                                     std::string("Failed to open query file: ") + queryFile_};
                    }
                    std::ostringstream ss;
                    ss << fin.rdbuf();
                    query_ = trim(ss.str());
                } else if (readStdin_ || (!queryFile_.empty() && queryFile_ == "-")) {
                    std::ostringstream ss;
                    ss << std::cin.rdbuf();
                    query_ = trim(ss.str());
                }
            }
            // Fold any extra positional tokens into the final query
            if (!extraArgs_.empty()) {
                std::ostringstream joiner;
                for (size_t i = 0; i < extraArgs_.size(); ++i) {
                    if (i)
                        joiner << ' ';
                    joiner << extraArgs_[i];
                }
                auto extras = trim(joiner.str());
                if (!extras.empty()) {
                    if (query_.empty()) {
                        query_ = extras;
                    } else {
                        query_ += ' ';
                        query_ += extras;
                    }
                }
            }
            // Final validation
            if (query_.empty() && extraArgs_.empty() && queryFile_.empty() && !readStdin_) {
                std::ostringstream help;
                help << "Query not provided.\n"
                     << "Tips:\n"
                     << "  - Provide a query as free args (default): yams search lzma\n"
                     << "  - Use -q or --query when the query starts with '-': yams search -q "
                        "\"--start-group\"\n"
                     << "  - Or use -- to stop option parsing: yams search -- \"--start-group "
                        "--whole-archive\"\n"
                     << "  - Or provide the query via --stdin or --query-file.\n";
                return Error{ErrorCode::InvalidArgument, help.str()};
            }
            // Auto-enable literal-text for code-like queries unless user explicitly set it
            {
                const std::string& q = query_;
                auto contains = [&](const char* s) { return q.find(s) != std::string::npos; };
                bool punct = false;
                for (char c : q) {
                    if (c == '(' || c == ')' || c == '[' || c == ']' || c == '{' || c == '}' ||
                        c == '"' || c == '\'' || c == '\\' || c == '`' || c == ';') {
                        punct = true;
                        break;
                    }
                }
                int dq = static_cast<int>(std::count(q.begin(), q.end(), '\"'));
                int lp = static_cast<int>(std::count(q.begin(), q.end(), '('));
                int rp = static_cast<int>(std::count(q.begin(), q.end(), ')'));
                int lb = static_cast<int>(std::count(q.begin(), q.end(), '['));
                int rb = static_cast<int>(std::count(q.begin(), q.end(), ']'));
                int lc = static_cast<int>(std::count(q.begin(), q.end(), '{'));
                int rc = static_cast<int>(std::count(q.begin(), q.end(), '}'));
                bool unbalanced = (dq % 2 != 0) || (lp != rp) || (lb != rb) || (lc != rc);
                bool codeSeq = contains("::") || contains("->") || contains("#include") ||
                               contains("template<") || contains("std::");
                if (!literalText_ && !fuzzySearch_ && (codeSeq || punct || unbalanced)) {
                    literalText_ = true;
                }
            }
            // Normalize include globs (split commas)
            auto includeGlobsExpanded = splitCommaPatterns(includeGlobs_);
            // Session-aware scoping: merge active session include patterns unless disabled
            if (!noSession_) {
                auto sessionName = (sessionOverride_ ? std::optional<std::string>(*sessionOverride_)
                                                     : std::optional<std::string>{});
                auto sessPatterns = yams::cli::session_store::active_include_patterns(sessionName);
                includeGlobsExpanded.insert(includeGlobsExpanded.end(), sessPatterns.begin(),
                                            sessPatterns.end());
            }
            if (includeGlobsExpanded.empty() && !pathFilter_.empty()) {
                includeGlobsExpanded.push_back(pathFilter_);
            }

            yams::daemon::ClientConfig clientConfig;
            clientConfig.headerTimeout = std::chrono::milliseconds(headerTimeoutMs_);
            clientConfig.bodyTimeout = std::chrono::milliseconds(bodyTimeoutMs_);
            clientConfig.requestTimeout = std::chrono::milliseconds(30000);
            // Allow streaming by default; CLI will retry unary on timeout
            clientConfig.enableChunkedResponses = !disableStreaming_ && enableStreaming_;
            clientConfig.progressiveOutput = true;
            clientConfig.maxChunkSize = chunkSize_;
            clientConfig.singleUseConnections = false;
            // Ensure daemon uses explicit CLI storage only when user overrode it
            if (cli_ && cli_->hasExplicitDataDir()) {
                auto dp = cli_->getDataPath();
                if (!dp.empty()) {
                    clientConfig.dataDir = dp;
                }
            }
            // Seed env aliases for any subprocess-based startup paths
            if (clientConfig.dataDir != std::filesystem::path{}) {
#ifndef _WIN32
                ::setenv("YAMS_STORAGE", clientConfig.dataDir.string().c_str(), 1);
                ::setenv("YAMS_DATA_DIR", clientConfig.dataDir.string().c_str(), 1);
#endif
            }
            yams::daemon::DaemonClient client(clientConfig);
            client.setStreamingEnabled(clientConfig.enableChunkedResponses);

            // Create search request
            yams::daemon::SearchRequest dreq;
            dreq.query = query_;
            dreq.limit = static_cast<size_t>(limit_);
            dreq.fuzzy = true; // favor resilient defaults; CLI still refines client-side
            dreq.literalText = literalText_;
            dreq.similarity = (minSimilarity_ > 0.0f) ? static_cast<double>(minSimilarity_) : 0.7;
            dreq.pathsOnly = pathsOnly_;
            dreq.searchType = searchType_;
            dreq.jsonOutput = jsonOutput_;
            dreq.showHash = showHash_;
            dreq.verbose = verbose_;
            dreq.showLineNumbers = showLineNumbers_;
            // Engine-level filters
            dreq.pathPattern = pathFilter_;
            if (!filterTags_.empty()) {
                std::stringstream ss(filterTags_);
                std::string tag;
                while (std::getline(ss, tag, ',')) {
                    // trim
                    auto s = trim(tag);
                    if (!s.empty())
                        dreq.tags.push_back(s);
                }
                dreq.matchAllTags = matchAllTags_;
            }
            dreq.extension = extension_;
            dreq.mimeType = mimeType_;
            dreq.fileType = fileType_;
            dreq.textOnly = textOnly_;
            dreq.binaryOnly = binaryOnly_;
            dreq.createdAfter = createdAfter_;
            dreq.createdBefore = createdBefore_;
            dreq.modifiedAfter = modifiedAfter_;
            dreq.modifiedBefore = modifiedBefore_;
            dreq.indexedAfter = indexedAfter_;
            dreq.indexedBefore = indexedBefore_;
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
                    const bool enableStreamEffective = clientConfig.enableChunkedResponses;
                    if (!enableStreamEffective) {
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
                    } else {
                        // Streaming prints progressively; add explicit marker when empty
                        if (resp.totalCount == 0) {
                            std::cout << "(no results)" << std::endl;
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
                    if (items.empty()) {
                        std::cout << "(no results)" << std::endl;
                        return Result<void>();
                    }
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
                    std::cout << "Found " << resp.totalCount << " results in "
                              << resp.elapsed.count() << "ms" << std::endl;
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
                sreq.extension = extension_;
                sreq.mimeType = mimeType_;
                sreq.fileType = fileType_;
                sreq.textOnly = textOnly_;
                sreq.binaryOnly = binaryOnly_;
                sreq.createdAfter = createdAfter_;
                sreq.createdBefore = createdBefore_;
                sreq.modifiedAfter = modifiedAfter_;
                sreq.modifiedBefore = modifiedBefore_;
                sreq.indexedAfter = indexedAfter_;
                sreq.indexedBefore = indexedBefore_;

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

                std::promise<Result<app::services::SearchResponse>> prom;
                auto fut = prom.get_future();
                boost::asio::co_spawn(
                    boost::asio::system_executor{},
                    [&]() -> boost::asio::awaitable<void> {
                        auto r = co_await searchService->search(sreq);
                        prom.set_value(std::move(r));
                        co_return;
                    },
                    boost::asio::detached);
                if (fut.wait_for(std::chrono::seconds(30)) != std::future_status::ready) {
                    return Error{ErrorCode::Timeout, "Local search timed out"};
                }
                auto rlocal = fut.get();
                if (!rlocal) {
                    return rlocal.error();
                }
                auto resp = rlocal.value();

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
                    // Best-effort extraction stats removed; hybrid engine surfaces progress

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
            // Fully async daemon path with a single co_spawn and promise completion
            std::promise<Result<void>> done;
            auto fut = done.get_future();
            auto work = [&, dreq, enableStream = clientConfig.enableChunkedResponses,
                         bodyTimeoutMs = bodyTimeoutMs_, fuzzyFlag = fuzzySearch_,
                         literalFlag = literalText_]() -> boost::asio::awaitable<void> {
                auto callOnce = [&](const yams::daemon::SearchRequest& rq)
                    -> boost::asio::awaitable<Result<yams::daemon::SearchResponse>> {
                    if (enableStream)
                        co_return co_await client.streamingSearch(rq);
                    co_return co_await client.call(rq);
                };
                auto r = co_await callOnce(dreq);
                if (!r && enableStream) {
                    const auto& err = r.error();
                    if (err.code == ErrorCode::Timeout &&
                        err.message.find("Read timeout") != std::string::npos) {
                        spdlog::warn("Streaming search timed out; retrying unary path with "
                                     "extended header timeout");
                        client.setHeaderTimeout(std::chrono::milliseconds(bodyTimeoutMs));
                        auto ur = co_await client.call(dreq);
                        if (ur) {
                            auto ok = render(ur.value());
                            done.set_value(ok ? Result<void>() : Result<void>(ok.error()));
                            co_return;
                        }
                    }
                }
                if (r) {
                    auto resp = r.value();
                    bool noResults = resp.results.empty() || resp.totalCount == 0;
                    if (noResults && !fuzzyFlag) {
                        auto retryReq = dreq;
                        retryReq.fuzzy = true;
                        auto fr = co_await callOnce(retryReq);
                        if (fr) {
                            auto ok = render(fr.value());
                            done.set_value(ok ? Result<void>() : Result<void>(ok.error()));
                            co_return;
                        }
                    }
                    auto ok = render(resp);
                    done.set_value(ok ? Result<void>() : Result<void>(ok.error()));
                    co_return;
                }
                // Heuristic retry with literal-text when parse-like failure
                const auto& derr = r.error();
                bool parseLike = derr.code == ErrorCode::InvalidArgument ||
                                 derr.message.find("syntax") != std::string::npos ||
                                 derr.message.find("FTS5") != std::string::npos ||
                                 derr.message.find("unbalanced") != std::string::npos ||
                                 derr.message.find("near") != std::string::npos ||
                                 derr.message.find("tokenize") != std::string::npos;
                if (!literalFlag && parseLike) {
                    // Silent retry with literal-text for better ergonomics
                    auto retryReq = dreq;
                    retryReq.literalText = true;
                    auto rr = co_await callOnce(retryReq);
                    if (rr) {
                        auto ok = render(rr.value());
                        done.set_value(ok ? Result<void>() : Result<void>(ok.error()));
                        co_return;
                    }
                }
                done.set_value(r.error());
                co_return;
            };
            boost::asio::co_spawn(boost::asio::system_executor{}, work(), boost::asio::detached);
            if (fut.wait_for(std::chrono::seconds(30)) != std::future_status::ready) {
                spdlog::warn("search: daemon call timed out; falling back to local execution");
                auto fb = fallback();
                if (!fb)
                    return fb.error();
                return Result<void>();
            }
            auto rv = fut.get();
            if (rv)
                return Result<void>();
            // Fallback to local when daemon path returns error
            {
                auto fb = fallback();
                if (!fb)
                    return fb.error();
                return Result<void>();
            }

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
        }
    }

    boost::asio::awaitable<Result<void>> executeAsync() override {
        try {
            // Resolve base query (reuse sync logic for assembling fields)
            // Defer to execute() for argument normalization, then re-run daemon path with co_await
            // If execute() succeeded synchronously (local), return its result; otherwise, we
            // proceed async.
        } catch (...) {
        }

        // Build normalized arguments (copy of execute() preamble)
        if (query_.empty()) {
            if (!queryFile_.empty() && queryFile_ != "-") {
                std::ifstream fin(queryFile_, std::ios::in | std::ios::binary);
                if (!fin.good()) {
                    co_return Error{ErrorCode::InvalidArgument,
                                    std::string("Failed to open query file: ") + queryFile_};
                }
                std::ostringstream ss;
                ss << fin.rdbuf();
                query_ = trim(ss.str());
            } else if (readStdin_ || (!queryFile_.empty() && queryFile_ == "-")) {
                std::ostringstream ss;
                ss << std::cin.rdbuf();
                query_ = trim(ss.str());
            }
        }
        // Fold extra positional tokens already captured in callback
        if (!extraArgs_.empty()) {
            std::ostringstream joiner;
            for (size_t i = 0; i < extraArgs_.size(); ++i) {
                if (i)
                    joiner << ' ';
                joiner << extraArgs_[i];
            }
            auto extras = trim(joiner.str());
            if (!extras.empty()) {
                if (query_.empty())
                    query_ = extras;
                else
                    query_ += std::string(" ") + extras;
            }
        }
        // Auto-enable literal text heuristics (reuse minimal subset)
        if (!literalText_ && !fuzzySearch_) {
            const std::string& q = query_;
            bool punct = false;
            for (char c : q) {
                if (c == '(' || c == ')' || c == '[' || c == ']' || c == '{' || c == '}' ||
                    c == '"' || c == '\'' || c == '\\' || c == '`' || c == ';') {
                    punct = true;
                    break;
                }
            }
            if (punct || q.find("::") != std::string::npos || q.find("->") != std::string::npos ||
                q.find("#include") != std::string::npos || q.find("std::") != std::string::npos) {
                literalText_ = true;
            }
        }

        // Includes/session scoping
        auto includeGlobsExpanded = splitCommaPatterns(includeGlobs_);
        if (!noSession_) {
            auto sess = yams::cli::session_store::active_include_patterns(sessionOverride_);
            includeGlobsExpanded.insert(includeGlobsExpanded.end(), sess.begin(), sess.end());
        }
        if (includeGlobsExpanded.empty() && !pathFilter_.empty())
            includeGlobsExpanded.push_back(pathFilter_);

        // Daemon client config
        yams::daemon::DaemonClient::setTimeoutEnvVars(std::chrono::milliseconds(headerTimeoutMs_),
                                                      std::chrono::milliseconds(bodyTimeoutMs_));
        yams::daemon::ClientConfig clientConfig;
        clientConfig.headerTimeout = std::chrono::milliseconds(headerTimeoutMs_);
        clientConfig.bodyTimeout = std::chrono::milliseconds(bodyTimeoutMs_);
        clientConfig.requestTimeout = std::chrono::milliseconds(30000);
        clientConfig.enableChunkedResponses = !disableStreaming_ && enableStreaming_;
        clientConfig.progressiveOutput = true;
        clientConfig.maxChunkSize = chunkSize_;
        clientConfig.singleUseConnections = true;
        if (cli_) {
            auto dp = cli_->getDataPath();
            if (!dp.empty())
                clientConfig.dataDir = dp;
        }

        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(clientConfig);
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;
        client.setStreamingEnabled(clientConfig.enableChunkedResponses);

        // Request + render
        yams::daemon::SearchRequest dreq;
        dreq.query = query_;
        dreq.limit = static_cast<size_t>(limit_);
        dreq.fuzzy = true;
        dreq.literalText = literalText_;
        dreq.similarity = (minSimilarity_ > 0.0f) ? static_cast<double>(minSimilarity_) : 0.7;
        dreq.pathsOnly = pathsOnly_;
        dreq.searchType = searchType_;
        dreq.jsonOutput = jsonOutput_;
        dreq.showHash = showHash_;
        dreq.verbose = verbose_;
        dreq.showLineNumbers = showLineNumbers_;
        // Engine-level filters
        dreq.pathPattern = pathFilter_;
        if (!filterTags_.empty()) {
            std::stringstream ss(filterTags_);
            std::string tag;
            while (std::getline(ss, tag, ',')) {
                auto s = trim(tag);
                if (!s.empty())
                    dreq.tags.push_back(s);
            }
            dreq.matchAllTags = matchAllTags_;
        }
        dreq.extension = extension_;
        dreq.mimeType = mimeType_;
        dreq.fileType = fileType_;
        dreq.textOnly = textOnly_;
        dreq.binaryOnly = binaryOnly_;
        dreq.createdAfter = createdAfter_;
        dreq.createdBefore = createdBefore_;
        dreq.modifiedAfter = modifiedAfter_;
        dreq.modifiedBefore = modifiedBefore_;
        dreq.indexedAfter = indexedAfter_;
        dreq.indexedBefore = indexedBefore_;

        auto render = [&](const yams::daemon::SearchResponse& resp) -> Result<void> {
            std::vector<yams::daemon::SearchResult> items;
            items.reserve(resp.results.size());
            if (!includeGlobsExpanded.empty()) {
                for (const auto& r : resp.results) {
                    std::string path =
                        !r.path.empty()
                            ? r.path
                            : (r.metadata.count("path") ? r.metadata.at("path") : std::string{});
                    if (matchAnyGlob(path, includeGlobsExpanded))
                        items.push_back(r);
                }
            } else {
                items = resp.results;
            }
            if (pathsOnly_) {
                if (items.empty()) {
                    std::cout << "(no results)" << std::endl;
                } else {
                    for (const auto& r : items) {
                        if (!r.path.empty())
                            std::cout << r.path << std::endl;
                        else if (auto it = r.metadata.find("path"); it != r.metadata.end())
                            std::cout << it->second << std::endl;
                        else
                            std::cout << r.id << std::endl;
                    }
                }
                return Result<void>();
            }
            if (jsonOutput_ || verbose_) {
                nlohmann::json output;
                output["query"] = query_;
                output["method"] = std::string("search");
                output["total_results"] = resp.totalCount;
                output["execution_time_ms"] = resp.elapsed.count();
                nlohmann::json results = nlohmann::json::array();
                for (const auto& it : items) {
                    nlohmann::json doc;
                    doc["id"] = it.id;
                    if (!it.title.empty())
                        doc["title"] = it.title;
                    if (!it.path.empty())
                        doc["path"] = it.path;
                    if (!it.snippet.empty())
                        doc["snippet"] = truncateSnippet(it.snippet, 200);
                    results.push_back(std::move(doc));
                }
                output["results"] = std::move(results);
                std::cout << output.dump(2) << std::endl;
                return Result<void>();
            }
            if (items.empty()) {
                std::cout << "(no results)" << std::endl;
                return Result<void>();
            }
            for (const auto& r : items) {
                if (!r.path.empty())
                    std::cout << r.path;
                else if (!r.title.empty())
                    std::cout << r.title;
                else
                    std::cout << r.id;
                if (!r.snippet.empty())
                    std::cout << "\n    " << truncateSnippet(r.snippet, 200);
                std::cout << "\n";
            }
            std::cout << "Found " << resp.totalCount << " results in " << resp.elapsed.count()
                      << "ms" << std::endl;
            return Result<void>();
        };

        // Async daemon request with retries
        auto callOnce = [&](const yams::daemon::SearchRequest& rq)
            -> boost::asio::awaitable<Result<yams::daemon::SearchResponse>> {
            if (clientConfig.enableChunkedResponses)
                co_return co_await client.streamingSearch(rq);
            co_return co_await client.call(rq);
        };

        // Prefer streaming by default with a guard: if no results arrive quickly,
        // race a unary call and use whichever finishes first. Skipped when --cold.
        Result<yams::daemon::SearchResponse> result_stream_or_unary(
            Error{ErrorCode::Unknown, "uninitialized"});
        if (clientConfig.enableChunkedResponses) {
            // Race: streaming vs delayed unary (2s). Whichever sets first wins.
            std::shared_ptr<std::atomic_bool> decided = std::make_shared<std::atomic_bool>(false);
            std::shared_ptr<std::promise<Result<yams::daemon::SearchResponse>>> prom =
                std::make_shared<std::promise<Result<yams::daemon::SearchResponse>>>();
            auto fut = prom->get_future();

            // Launch streaming
            boost::asio::co_spawn(
                boost::asio::system_executor{},
                [&, decided, prom, leaseHandle]() -> boost::asio::awaitable<void> {
                    auto& cliRef = **leaseHandle;
                    auto sr = co_await cliRef.streamingSearch(dreq);
                    if (!decided->exchange(true))
                        prom->set_value(std::move(sr));
                    co_return;
                },
                boost::asio::detached);

            // Launch delayed unary fallback (2 seconds after start)
            boost::asio::co_spawn(
                boost::asio::system_executor{},
                [&, decided, prom, leaseHandle]() -> boost::asio::awaitable<void> {
                    boost::asio::steady_timer t(co_await boost::asio::this_coro::executor);
                    t.expires_after(std::chrono::seconds(2));
                    co_await t.async_wait(boost::asio::use_awaitable);
                    if (!decided->load()) {
                        auto& cliRef = **leaseHandle;
                        auto ur = co_await cliRef.call(dreq);
                        if (!decided->exchange(true))
                            prom->set_value(std::move(ur));
                    }
                    co_return;
                },
                boost::asio::detached);

            // Wait up to body timeout for one of the paths to complete
            auto wait_ms = static_cast<int>(bodyTimeoutMs_ > 0 ? bodyTimeoutMs_ : 60000);
            if (fut.wait_for(std::chrono::milliseconds(wait_ms)) == std::future_status::ready) {
                result_stream_or_unary = fut.get();
            } else {
                result_stream_or_unary = Error{ErrorCode::Timeout, "Search timed out"};
            }
        } else {
            // Non-streaming path (cold): unary only
            result_stream_or_unary = co_await client.call(dreq);
        }

        auto r = result_stream_or_unary;
        if (!r && clientConfig.enableChunkedResponses) {
            const auto& err = r.error();
            if (err.code == ErrorCode::Timeout &&
                err.message.find("Read timeout") != std::string::npos) {
                // Silent retry with unary path and extended header timeout
                client.setHeaderTimeout(std::chrono::milliseconds(bodyTimeoutMs_));
                auto ur = co_await client.call(dreq);
                if (ur)
                    co_return render(ur.value());
                // If unary retry also failed, fallback to local
                if (!ur) {
                    if (auto appContext = cli_->getAppContext()) {
                        auto searchService = app::services::makeSearchService(*appContext);
                        if (searchService) {
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
                            sreq.extension = extension_;
                            sreq.mimeType = mimeType_;
                            sreq.fileType = fileType_;
                            sreq.textOnly = textOnly_;
                            sreq.binaryOnly = binaryOnly_;
                            sreq.createdAfter = createdAfter_;
                            sreq.createdBefore = createdBefore_;
                            sreq.modifiedAfter = modifiedAfter_;
                            sreq.modifiedBefore = modifiedBefore_;
                            sreq.indexedAfter = indexedAfter_;
                            sreq.indexedBefore = indexedBefore_;
                            auto local = co_await searchService->search(sreq);
                            if (local) {
                                yams::daemon::SearchResponse out;
                                for (const auto& it : local.value().results) {
                                    yams::daemon::SearchResult sr;
                                    sr.id = it.id;
                                    sr.title = it.title;
                                    sr.path = it.path;
                                    sr.score = it.score;
                                    sr.snippet = it.snippet;
                                    out.results.push_back(std::move(sr));
                                }
                                out.totalCount = out.results.size();
                                co_return render(out);
                            }
                        }
                    }
                }
            }
        }
        if (r) {
            auto resp = r.value();
            bool noResults = resp.results.empty() || resp.totalCount == 0;
            if (noResults && !fuzzySearch_) {
                auto retryReq = dreq;
                retryReq.fuzzy = true;
                auto fr = co_await callOnce(retryReq);
                if (fr)
                    co_return render(fr.value());
            }
            co_return render(resp);
        }
        // Parse-like retry
        const auto& derr = r.error();
        bool parseLike = derr.code == ErrorCode::InvalidArgument ||
                         derr.message.find("syntax") != std::string::npos ||
                         derr.message.find("FTS5") != std::string::npos ||
                         derr.message.find("unbalanced") != std::string::npos ||
                         derr.message.find("near") != std::string::npos ||
                         derr.message.find("tokenize") != std::string::npos;
        if (!literalText_ && parseLike) {
            // Silent retry with literal-text for better ergonomics
            auto retryReq = dreq;
            retryReq.literalText = true;
            auto rr = co_await callOnce(retryReq);
            if (rr)
                co_return render(rr.value());
        }

        // Fallback to local services via co_await
        if (auto appContext = cli_->getAppContext()) {
            auto searchService = app::services::makeSearchService(*appContext);
            if (searchService) {
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
                sreq.extension = extension_;
                sreq.mimeType = mimeType_;
                sreq.fileType = fileType_;
                sreq.textOnly = textOnly_;
                sreq.binaryOnly = binaryOnly_;
                sreq.createdAfter = createdAfter_;
                sreq.createdBefore = createdBefore_;
                sreq.modifiedAfter = modifiedAfter_;
                sreq.modifiedBefore = modifiedBefore_;
                sreq.indexedAfter = indexedAfter_;
                sreq.indexedBefore = indexedBefore_;
                if (!filterTags_.empty()) {
                    std::stringstream ss(filterTags_);
                    std::string tag;
                    while (std::getline(ss, tag, ',')) {
                        tag.erase(0, tag.find_first_not_of(" \t"));
                        tag.erase(tag.find_last_not_of(" \t") + 1);
                        if (!tag.empty())
                            sreq.tags.push_back(tag);
                    }
                    sreq.matchAllTags = matchAllTags_;
                }
                auto local = co_await searchService->search(sreq);
                if (!local)
                    co_return local.error();
                auto resp = local.value();
                if (!includeGlobsExpanded.empty()) {
                    if (pathsOnly_) {
                        std::vector<std::string> filtered;
                        for (const auto& p : resp.paths)
                            if (matchAnyGlob(p, includeGlobsExpanded))
                                filtered.push_back(p);
                        resp.paths.swap(filtered);
                    } else {
                        std::vector<app::services::SearchItem> filtered;
                        for (const auto& it : resp.results)
                            if (matchAnyGlob(it.path, includeGlobsExpanded))
                                filtered.push_back(it);
                        resp.results.swap(filtered);
                    }
                }
                if (pathsOnly_) {
                    for (const auto& p : resp.paths)
                        std::cout << p << std::endl;
                    co_return Result<void>();
                }
                if (jsonOutput_ || verbose_) {
                    nlohmann::json output;
                    output["query"] = query_;
                    output["method"] = resp.type;
                    output["total_results"] = resp.total;
                    output["execution_time_ms"] = resp.executionTimeMs;
                    nlohmann::json results = nlohmann::json::array();
                    for (const auto& item : resp.results) {
                        nlohmann::json doc;
                        doc["id"] = item.id;
                        if (!item.title.empty())
                            doc["title"] = item.title;
                        if (!item.path.empty())
                            doc["path"] = item.path;
                        results.push_back(std::move(doc));
                    }
                    output["results"] = std::move(results);
                    std::cout << output.dump(2) << std::endl;
                    co_return Result<void>();
                }
                for (const auto& item : resp.results) {
                    if (!item.path.empty())
                        std::cout << item.path;
                    else if (!item.title.empty())
                        std::cout << item.title;
                    else
                        std::cout << item.id;
                    std::cout << "\n";
                }
                co_return Result<void>();
            }
        }
        co_return Error{ErrorCode::Unknown, derr.message};
    }
};

// Factory function
std::unique_ptr<ICommand> createSearchCommand() {
    return std::make_unique<SearchCommand>();
}

} // namespace yams::cli
