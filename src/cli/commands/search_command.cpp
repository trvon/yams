#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <optional>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <yams/app/services/factory.hpp>
#include <yams/app/services/retrieval_service.h>
#include <yams/app/services/services.hpp>
#include <yams/cli/command.h>
#include <yams/cli/result_renderer.h>
#include <yams/cli/session_store.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/common/utf8_utils.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/profiling.h>
#include <yams/search/search_engine_builder.h>
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
// Timers and coroutine executor helpers for guard race
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>

#include <future>

namespace yams::cli {

using json = nlohmann::json;
using yams::app::services::utils::normalizeLookupPath;

class SearchCommand : public ICommand {
private:
    // Streaming configuration
#if defined(_WIN32)
    bool enableStreaming_ = false;
#else
    bool enableStreaming_ = true;
#endif
    int headerTimeoutMs_ = 15000;
    int bodyTimeoutMs_ = 60000;
    int chunkSize_ = 64 * 1024;

    YamsCLI* cli_ = nullptr;
    std::string query_;
    bool readStdin_ = false;
    std::string queryFile_;
    std::vector<std::string> extraArgs_;
    std::string pathFilter_;
    std::optional<std::string> resolvedLocalFilePath_;
    size_t limit_ = 20;
    std::string searchType_ = "hybrid";
    bool fuzzySearch_ = false;
    float minSimilarity_ = 0.7f;
    bool pathsOnly_ = false;
    bool literalText_ = false;
    bool showHash_ = false;
    bool verbose_ = false;
    bool jsonOutput_ = false;
    bool symbolRank_ = true; // Enable symbol ranking boost for code-like queries
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
    std::optional<std::string> sessionOverride_;
    bool noSession_{false};

    // CWD scoping
    bool scopeToCwd_{false};

    // Grouping of multiple versions per path (UI-only feature)
    bool groupVersions_{true};           // default: enabled
    std::string versionsMode_{"latest"}; // latest | all
    std::size_t versionsTopk_{3};        // cap per-path versions when showing all
    std::string versionsSort_{"score"};  // score | path | title
    bool showTools_{true};               // show per-version tool hints
    bool jsonGrouped_{false};            // emit grouped JSON instead of flat

    // Unified search result item for rendering (works with both daemon and local responses)
    struct UnifiedItem {
        std::string id;
        std::string path;
        std::string title;
        std::string snippet;
        std::string hash;
        double score{0.0};
        // Optional score breakdowns for verbose output
        std::optional<double> vectorScore;
        std::optional<double> keywordScore;
        std::optional<double> kgEntityScore;
        std::optional<double> structuralScore;

        // Convert from daemon SearchResult
        static UnifiedItem fromDaemon(const yams::daemon::SearchResult& r) {
            UnifiedItem item;
            item.id = r.id;
            item.path = r.path;
            item.title = r.title;
            item.snippet = r.snippet;
            item.score = r.score;
            if (auto it = r.metadata.find("hash"); it != r.metadata.end())
                item.hash = it->second;
            return item;
        }

        // Convert from local SearchItem
        static UnifiedItem fromLocal(const yams::app::services::SearchItem& r) {
            UnifiedItem item;
            item.id = std::to_string(r.id);
            item.path = r.path;
            item.title = r.title;
            item.snippet = r.snippet;
            item.hash = r.hash;
            item.score = r.score;
            item.vectorScore = r.vectorScore;
            item.keywordScore = r.keywordScore;
            item.kgEntityScore = r.kgEntityScore;
            item.structuralScore = r.structuralScore;
            return item;
        }

        std::string getDisplayPath() const {
            if (!path.empty())
                return path;
            if (!title.empty())
                return title;
            return id;
        }
    };

    struct RenderContext {
        std::string query;
        size_t totalCount{0};
        int64_t elapsedMs{0};
        std::string method{"search"};
    };

    bool shouldShowSpinner() const {
        bool jsonMode = jsonOutput_ || (cli_ && cli_->getJsonOutput());
        return !jsonMode && !pathsOnly_;
    }

    // Unified rendering function for search results
    Result<void> renderResults(std::vector<UnifiedItem>& items, const RenderContext& ctx) {
        auto emitTagHint = [&]() {
            if (!filterTags_.empty()) {
                std::cerr
                    << "Hint: tag filter returned no results; tags are stored as metadata keys "
                       "'tag:<name>'. Try searching without --tags."
                    << std::endl;
            }
        };

        // Deduplicate by path
        std::unordered_set<std::string> seenPaths;
        seenPaths.reserve(items.size());
        std::vector<UnifiedItem> deduplicated;
        deduplicated.reserve(std::min(items.size(), limit_));

        for (auto& item : items) {
            if (deduplicated.size() >= limit_)
                break;
            const std::string& key = item.path.empty() ? item.id : item.path;
            if (!key.empty() && seenPaths.count(key))
                continue;
            if (!key.empty())
                seenPaths.insert(key);
            deduplicated.push_back(std::move(item));
        }

        // Paths-only output
        if (pathsOnly_) {
            if (deduplicated.empty()) {
                std::cout << "(no results)" << std::endl;
                emitTagHint();
            } else {
                for (const auto& item : deduplicated) {
                    std::cout << item.getDisplayPath() << std::endl;
                }
            }
            return Result<void>();
        }

        // JSON output (flat)
        if ((jsonOutput_ && !jsonGrouped_) || (!groupVersions_ && (jsonOutput_ || verbose_))) {
            nlohmann::json output;
            output["query"] = ctx.query;
            output["method"] = ctx.method;
            output["total_results"] = deduplicated.size();
            output["execution_time_ms"] = ctx.elapsedMs;

            nlohmann::json results = nlohmann::json::array();
            for (const auto& item : deduplicated) {
                nlohmann::json doc;
                doc["id"] = item.id;
                if (!item.title.empty())
                    doc["title"] = item.title;
                if (!item.path.empty())
                    doc["path"] = item.path;
                if (showHash_ && !item.hash.empty())
                    doc["hash"] = item.hash;
                doc["score"] = item.score;
                if (!item.snippet.empty())
                    doc["snippet"] = truncateSnippet(item.snippet, 200);

                if (verbose_ && (item.vectorScore || item.keywordScore || item.kgEntityScore ||
                                 item.structuralScore)) {
                    nlohmann::json breakdown;
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
                results.push_back(std::move(doc));
            }
            output["results"] = std::move(results);
            std::cout << output.dump(2) << std::endl;
            return Result<void>();
        }

        // Human-readable output
        if (deduplicated.empty()) {
            std::cout << ui::colorize("(no results)", ui::Ansi::DIM) << std::endl;
            emitTagHint();
            return Result<void>();
        }

        if (!groupVersions_) {
            // Flat output
            for (const auto& item : deduplicated) {
                // File path in magenta
                std::string displayPath;
                if (!item.path.empty())
                    displayPath = item.path;
                else if (!item.title.empty())
                    displayPath = item.title;
                else
                    displayPath = item.id;

                std::cout << ui::colorize(displayPath, ui::Ansi::MAGENTA);

                // Score in green/yellow/dim based on relevance
                const char* scoreColor = ui::Ansi::DIM;
                if (item.score >= 0.8)
                    scoreColor = ui::Ansi::GREEN;
                else if (item.score >= 0.5)
                    scoreColor = ui::Ansi::YELLOW;
                std::cout << " "
                          << ui::colorize("[" + std::to_string(item.score).substr(0, 4) + "]",
                                          scoreColor);
                std::cout << "\n";

                // Snippet with line-number-style prefix
                if (!item.snippet.empty()) {
                    std::string snippet = truncateSnippet(item.snippet, 200);
                    std::cout << ui::colorize("  1:", ui::Ansi::DIM) << " " << snippet << "\n";
                }
                std::cout << "\n";
            }
        } else {
            // Group by path for version display
            std::unordered_map<std::string, std::vector<UnifiedItem>> groups;
            groups.reserve(deduplicated.size());
            for (auto& item : deduplicated) {
                std::string key = item.getDisplayPath();
                if (key.empty())
                    continue;
                groups[key].push_back(std::move(item));
            }

            if (jsonOutput_ && jsonGrouped_) {
                // Grouped JSON output
                nlohmann::json output;
                output["query"] = ctx.query;
                output["method"] = ctx.method;
                output["total_groups"] = groups.size();
                nlohmann::json groupsArr = nlohmann::json::array();
                for (auto& [path, vec] : groups) {
                    std::stable_sort(vec.begin(), vec.end(), [&](const auto& a, const auto& b) {
                        if (versionsSort_ == "path")
                            return a.path < b.path;
                        if (versionsSort_ == "title")
                            return a.title < b.title;
                        return a.score > b.score;
                    });
                    const auto& best = vec.front();
                    nlohmann::json g;
                    g["path"] = path;
                    g["total_versions"] = vec.size();
                    nlohmann::json bestJ;
                    bestJ["id"] = best.id;
                    bestJ["title"] = best.title;
                    bestJ["path"] = best.path;
                    bestJ["score"] = best.score;
                    if (showHash_ && !best.hash.empty())
                        bestJ["hash"] = best.hash;
                    if (!best.snippet.empty())
                        bestJ["snippet"] = truncateSnippet(best.snippet, 200);
                    g["best"] = bestJ;
                    nlohmann::json vers = nlohmann::json::array();
                    std::size_t cap = versionsMode_ == "all" ? versionsTopk_ : 1;
                    for (std::size_t i = 0; i < vec.size() && i < cap; ++i) {
                        const auto& v = vec[i];
                        nlohmann::json vj;
                        vj["id"] = v.id;
                        vj["title"] = v.title;
                        vj["path"] = v.path;
                        vj["score"] = v.score;
                        if (showHash_ && !v.hash.empty())
                            vj["hash"] = v.hash;
                        if (!v.snippet.empty())
                            vj["snippet"] = truncateSnippet(v.snippet, 200);
                        vers.push_back(vj);
                    }
                    g["versions"] = vers;
                    groupsArr.push_back(g);
                }
                output["groups"] = groupsArr;
                std::cout << output.dump(2) << std::endl;
            } else {
                // Grouped output
                for (auto& [path, vec] : groups) {
                    std::stable_sort(vec.begin(), vec.end(), [&](const auto& a, const auto& b) {
                        if (versionsSort_ == "path")
                            return a.path < b.path;
                        if (versionsSort_ == "title")
                            return a.title < b.title;
                        return a.score > b.score;
                    });
                    const auto& best = vec.front();

                    // File path in magenta
                    std::cout << ui::colorize(path, ui::Ansi::MAGENTA);

                    // Version count in dim
                    if (vec.size() > 1) {
                        std::cout << " "
                                  << ui::colorize("(" + std::to_string(vec.size()) + " versions)",
                                                  ui::Ansi::DIM);
                    }
                    std::cout << "\n";

                    std::size_t cap = (versionsMode_ == "all") ? versionsTopk_ : 1;
                    for (std::size_t i = 0; i < vec.size() && i < cap; ++i) {
                        const auto& v = vec[i];
                        std::string hash8;
                        if (!v.hash.empty() && v.hash.size() >= 8)
                            hash8 = v.hash.substr(0, 8);

                        if (vec.size() > 1 || versionsMode_ == "all") {
                            // Hash in cyan, score colored by relevance
                            std::string hashDisplay =
                                hash8.empty() ? "[--------]" : ("[" + hash8 + "]");
                            const char* scoreColor = ui::Ansi::DIM;
                            if (v.score >= 0.8)
                                scoreColor = ui::Ansi::GREEN;
                            else if (v.score >= 0.5)
                                scoreColor = ui::Ansi::YELLOW;

                            std::cout
                                << "  " << ui::colorize(hashDisplay, ui::Ansi::CYAN) << " "
                                << ui::colorize(std::to_string(v.score).substr(0, 4), scoreColor);
                            if (!v.title.empty() && v.title != path)
                                std::cout << "  " << v.title;
                            std::cout << "\n";
                        }

                        // Snippet with line-number-style prefix
                        if (!v.snippet.empty()) {
                            std::string snippet = truncateSnippet(v.snippet, 200);
                            std::string linePrefix = vec.size() > 1
                                                         ? ui::colorize("     1:", ui::Ansi::DIM)
                                                         : ui::colorize("  1:", ui::Ansi::DIM);
                            std::cout << linePrefix << " " << snippet << "\n";
                        }

                        if (showTools_ && !hash8.empty() && i == 0) {
                            std::string indent = vec.size() > 1 ? "       " : "     ";
                            std::cout << ui::colorize(indent + "yams cat --hash " + v.hash,
                                                      ui::Ansi::DIM);
                            std::cout << "\n";
                        }
                    }
                    if (versionsMode_ == "all" && vec.size() > cap) {
                        std::cout << ui::colorize("    (+" + std::to_string(vec.size() - cap) +
                                                      " more)",
                                                  ui::Ansi::DIM)
                                  << "\n";
                    }
                    std::cout << "\n";
                }
            }
        }

        // Summary
        std::cout << ui::colorize(std::to_string(deduplicated.size()) + " result" +
                                      (deduplicated.size() != 1 ? "s" : "") + " (" +
                                      std::to_string(ctx.elapsedMs) + "ms)",
                                  ui::Ansi::DIM)
                  << std::endl;
        return Result<void>();
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
                if (c == ' ' || c == '\t') {
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

    Result<void> printDiffForSearchResult(const yams::app::services::SearchResponse& resp) {
        if (!resolvedLocalFilePath_.has_value() || pathsOnly_ || cli_->getJsonOutput())
            return Result<void>();

        namespace fs = std::filesystem;
        fs::path abs{*resolvedLocalFilePath_};
        std::error_code ec;
        if (!fs::exists(abs, ec) || !fs::is_regular_file(abs, ec))
            return Result<void>();

        const yams::app::services::SearchItem* matched = nullptr;
        for (const auto& item : resp.results) {
            if (item.path == abs.string()) {
                matched = &item;
                break;
            }
        }
        std::string resolvedHash = matched ? matched->hash : std::string{};
        if (resolvedHash.empty()) {
            auto appContext = cli_->getAppContext();
            if (appContext) {
                auto documentService = yams::app::services::makeDocumentService(*appContext);
                if (documentService) {
                    auto hres = documentService->resolveNameToHash(abs.string());
                    if (hres)
                        resolvedHash = hres.value();
                }
            }
        }
        if (resolvedHash.empty())
            return Result<void>();

        yams::app::services::RetrievalService rsvc;
        yams::app::services::RetrievalOptions ropts;
        if (cli_->hasExplicitDataDir())
            ropts.explicitDataDir = cli_->getDataPath();

        yams::app::services::GetOptions greq;
        greq.hash = resolvedHash;
        greq.metadataOnly = false;
        auto gr = rsvc.get(greq, ropts);
        if (!gr)
            return Result<void>();

        const auto& indexed = gr.value();
        std::ifstream ifs(abs);
        if (!ifs)
            return Result<void>();
        std::string local((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
        if (local == indexed.content) {
            std::cout << "\nNo differences: local file matches indexed content (" << abs.string()
                      << ")\n";
            return Result<void>();
        }

        auto toLines = [](const std::string& s) {
            std::vector<std::string> lines;
            std::stringstream ss(s);
            std::string line;
            while (std::getline(ss, line))
                lines.push_back(line);
            return lines;
        };

        auto localLines = toLines(local);
        auto indexedLines = toLines(indexed.content);
        size_t i = 0, j = 0;
        size_t shown = 0;
        constexpr size_t kMaxLines = 200;

        std::cout << "\n=== Diff (local vs indexed) for: " << abs.string() << " ===\n";
        while ((i < localLines.size() || j < indexedLines.size()) && shown < kMaxLines) {
            const std::string* la = (i < localLines.size()) ? &localLines[i] : nullptr;
            const std::string* lb = (j < indexedLines.size()) ? &indexedLines[j] : nullptr;
            if (la && lb && *la == *lb) {
                ++i;
                ++j;
                continue;
            }
            if (la) {
                std::cout << "- " << *la << "\n";
                ++shown;
                ++i;
            }
            if (lb && shown < kMaxLines) {
                std::cout << "+ " << *lb << "\n";
                ++shown;
                ++j;
            }
        }

        if (i < localLines.size() || j < indexedLines.size()) {
            std::cout << "... diff truncated after " << kMaxLines << " lines ...\n";
        }

        return Result<void>();
    }

public:
    std::string getName() const override { return "search"; }
    std::string getDescription() const override { return "Search documents by query"; }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("search", getDescription());
        // Provide flagged alias to safely pass queries beginning with '-'
        cmd->add_option("-q,--query", query_, "Search query (use when the query starts with '-')");
        // Define a positional option for the query - this allows options anywhere in command line
        cmd->add_option("query_positional", extraArgs_, "Search query terms")
            ->expected(0, -1); // Accept 0 or more positional arguments
        cmd->add_option("--path", pathFilter_, "Filter results by path pattern (optional)");
        // Query input sources
        cmd->add_flag("--stdin", readStdin_, "Read query from STDIN if not provided");
        cmd->add_option("--query-file", queryFile_,
                        "Read query from file path (use '-' to read from STDIN)");

        auto* limitOpt =
            cmd->add_option("-l,--limit", limit_, "Maximum number of results")->default_val(20);

        cmd->add_option("-t,--type", searchType_, "Search type (keyword, semantic, hybrid)")
            ->default_val("hybrid");

        cmd->add_flag("-f,--fuzzy", fuzzySearch_, "Enable fuzzy search for approximate matching");
        cmd->add_option("--similarity", minSimilarity_,
                        "Minimum similarity for fuzzy search (0.0-1.0)")
            ->default_val(0.7f);

        cmd->add_flag("--paths-only", pathsOnly_,
                      "Output only file paths, one per line (useful for scripting)");

        // Grouping / versions presentation controls (default grouping enabled)
        bool noGroupVersions = false; // local flag to flip default-on grouping
        cmd->add_flag("--no-group-versions", noGroupVersions,
                      "Disable grouping of multiple versions per path");
        cmd->add_option("--versions", versionsMode_,
                        "Which versions to show when grouped: latest|all")
            ->check(CLI::IsMember({"latest", "all"}))
            ->default_val("latest");
        cmd->add_option("--versions-topk", versionsTopk_,
                        "Max versions to show per path when --versions=all")
            ->default_val(3);
        cmd->add_option("--versions-sort", versionsSort_, "Per-path version sort: score|path|title")
            ->check(CLI::IsMember({"score", "path", "title"}))
            ->default_val("score");
        bool noTools = false; // local flag to hide tool hints
        cmd->add_flag("--no-tools", noTools, "Hide per-version tool hints in grouped output");
        cmd->add_flag("--json-grouped", jsonGrouped_,
                      "Emit grouped JSON (preserves flat JSON unless this is set)");

        // Session scoping controls
        cmd->add_option("--session", sessionOverride_, "Search within specific session");
        cmd->add_flag("--global,--no-session", noSession_, "Search global memory (bypass session)");

        // CWD scoping
        cmd->add_flag("--cwd,--here", scopeToCwd_,
                      "Scope search to current working directory (adds CWD as path prefix filter)");

        cmd->add_option("--header-timeout", headerTimeoutMs_,
                        "Timeout for receiving response headers (milliseconds)")
            ->default_val(15000);

        cmd->add_option("--body-timeout", bodyTimeoutMs_,
                        "Timeout for receiving response body (milliseconds)")
            ->default_val(60000);

        cmd->add_flag(
            "--no-streaming",
            [this](bool v) {
                if (v)
                    enableStreaming_ = false;
            },
            "Disable streaming responses from daemon");
        cmd->add_option("--chunk-size", chunkSize_,
                        "Size of chunks for streaming responses (bytes)")
            ->default_val(64 * 1024);
        cmd->add_flag("-F,-Q,--fixed-strings,--literal-text", literalText_,
                      "Treat query as literal text, not regex (escapes special characters). "
                      "Use this for queries with special characters like ()[]{}.*+?. "
                      "Example: yams search -F \"MyClass::method()\" --include=\"src/**/*.cpp\"");
        cmd->add_flag("--show-hash", showHash_, "Show document hashes in results");
        cmd->add_flag("-v,--verbose", verbose_, "Show detailed information including full hashes");
        cmd->add_flag("--json", jsonOutput_, "Output results in JSON format");
        cmd->add_flag(
            "--symbol-rank,--no-symbol-rank", symbolRank_,
            "Enable/disable automatic symbol ranking boost for code-like queries (enabled by "
            "default)");

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

        // limitOpt is unused but kept for potential future diagnostics
        (void)limitOpt;
        cmd->callback([this, cmd]() {
            cli_->setPendingCommand(this);
            // Apply local inversion flags captured above
            // Note: CLI11 stores flag states; re-fetch via app to avoid capture issues
            // but here we use simple locals and rely on evaluation order pre-callback.
            // If these locals are optimized away, we can revisit.
            // For now, mirror intent: default ON, disable when --no-group-versions present;
            // default show tools, disable when --no-tools present.
            groupVersions_ = true;
            showTools_ = true;
            // These environment variables allow forcing defaults at runtime if needed
            if (const char* envNoGroup = std::getenv("YAMS_NO_GROUP_VERSIONS")) {
                std::string v(envNoGroup);
                std::transform(v.begin(), v.end(), v.begin(),
                               [](unsigned char c) { return (char)std::tolower(c); });
                if (v == "1" || v == "true" || v == "yes" || v == "on")
                    groupVersions_ = false;
            }
            if (const char* envNoTools = std::getenv("YAMS_NO_GROUP_TOOLS")) {
                std::string v(envNoTools);
                std::transform(v.begin(), v.end(), v.begin(),
                               [](unsigned char c) { return (char)std::tolower(c); });
                if (v == "1" || v == "true" || v == "yes" || v == "on")
                    showTools_ = false;
            }
            // CLI flags override env defaults when present
            if (cmd->count("--no-group-versions") > 0)
                groupVersions_ = false;
            if (cmd->count("--no-tools") > 0)
                showTools_ = false;
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
                bool hasWordConnectors = false; // code-ish identifiers like add_directory, foo-bar
                for (char c : q) {
                    if (c == '(' || c == ')' || c == '[' || c == ']' || c == '{' || c == '}' ||
                        c == '"' || c == '\'' || c == '\\' || c == '`' || c == ';') {
                        punct = true;
                        break;
                    }
                    if (c == '_' || c == '-' || c == '.' || c == '/') {
                        hasWordConnectors = true;
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
                // If the query has word-connector characters and no whitespace, treat literally
                bool singleTokenWithConnectors =
                    hasWordConnectors && (q.find_first_of(" \t\n\r") == std::string::npos);
                if (!literalText_ && !fuzzySearch_ &&
                    (codeSeq || punct || unbalanced || singleTokenWithConnectors)) {
                    literalText_ = true;
                }
            }

            resolvedLocalFilePath_.reset();
            if (!pathFilter_.empty()) {
                auto normalized = normalizeLookupPath(pathFilter_);
                if (normalized.changed) {
                    pathFilter_ = normalized.normalized;
                }
                if (!normalized.hasWildcards) {
                    namespace fs = std::filesystem;
                    std::error_code ec;
                    fs::path candidate{pathFilter_};
                    if (fs::exists(candidate, ec) && fs::is_regular_file(candidate, ec)) {
                        resolvedLocalFilePath_ = candidate.string();
                    }
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

            // CWD scoping: add current directory as a path prefix filter
            std::string cwdPrefix;
            if (scopeToCwd_) {
                std::error_code ec;
                auto cwd = std::filesystem::current_path(ec);
                if (!ec) {
                    cwdPrefix = cwd.string();
                    // Normalize path separators for Windows
                    std::replace(cwdPrefix.begin(), cwdPrefix.end(), '\\', '/');
                    if (!cwdPrefix.empty() && cwdPrefix.back() != '/') {
                        cwdPrefix += '/';
                    }
                    // Add glob pattern to match all files under CWD
                    includeGlobsExpanded.push_back(cwdPrefix + "**/*");
                    spdlog::debug("[CLI] Scoping search to CWD: {}", cwdPrefix);
                }
            }

            yams::daemon::ClientConfig clientConfig;
            clientConfig.headerTimeout = std::chrono::milliseconds(headerTimeoutMs_);
            clientConfig.bodyTimeout = std::chrono::milliseconds(bodyTimeoutMs_);
            clientConfig.requestTimeout = std::chrono::milliseconds(30000);
            clientConfig.enableChunkedResponses = enableStreaming_;
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
            // Sanitize query to ensure valid UTF-8 (required by Protocol Buffer)
            dreq.query = yams::common::sanitizeUtf8(query_);
            if (dreq.query != query_) {
                spdlog::debug(
                    "Search query contained invalid UTF-8; sanitized for IPC transmission");
            }
            // Request one extra result to account for potential deduplication
            dreq.limit = static_cast<size_t>(limit_) + 1;
            dreq.fuzzy = fuzzySearch_; // honor explicit CLI flag; fallback logic will retry
            dreq.literalText = literalText_;
            dreq.similarity = (minSimilarity_ > 0.0f) ? static_cast<double>(minSimilarity_) : 0.7;
            dreq.pathsOnly = false; // Always fetch full results; pathsOnly is a CLI display option
            dreq.searchType = searchType_;
            dreq.jsonOutput = jsonOutput_;
            dreq.showHash = showHash_;
            dreq.verbose = verbose_;
            dreq.showLineNumbers = showLineNumbers_;
            dreq.symbolRank = symbolRank_;
            // Engine-level filters
            if (!includeGlobsExpanded.empty()) {
                dreq.pathPatterns =
                    includeGlobsExpanded; // Send all patterns for server-side filtering
                spdlog::debug("[CLI] Setting {} path patterns for search",
                              includeGlobsExpanded.size());
            } else if (!pathFilter_.empty()) {
                dreq.pathPattern = pathFilter_;           // Legacy single pattern fallback
                dreq.pathPatterns.push_back(pathFilter_); // ALSO populate the vector
                spdlog::debug("[CLI] Using pathFilter fallback, set pathPatterns with 1 element");
            }
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

            dreq.globalSearch = noSession_;
            dreq.useSession = !noSession_;
            if (!noSession_ && sessionOverride_) {
                dreq.sessionName = *sessionOverride_;
            }

            std::shared_ptr<ui::SpinnerRunner> spinner =
                shouldShowSpinner() ? std::make_shared<ui::SpinnerRunner>() : nullptr;
            if (spinner) {
                spinner->start("Searching...");
            }
            auto stopSpinner = [&]() {
                if (spinner) {
                    spinner->stop();
                }
            };

            auto render = [&](const yams::daemon::SearchResponse& resp) -> Result<void> {
                stopSpinner();
                // Convert daemon results to unified items
                std::vector<UnifiedItem> items;
                items.reserve(resp.results.size());
                for (const auto& r : resp.results) {
                    items.push_back(UnifiedItem::fromDaemon(r));
                }

                RenderContext ctx;
                ctx.query = query_;
                ctx.totalCount = resp.totalCount;
                ctx.elapsedMs = resp.elapsed.count();
                ctx.method = "daemon";

                return renderResults(items, ctx);
            };
            auto fallback = [&]() -> Result<void> {
                stopSpinner();
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
                    getExecutor(),
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

                // Handle paths-only output (special case - uses resp.paths)
                if (pathsOnly_) {
                    std::unordered_set<std::string> seen;
                    for (const auto& path : resp.paths) {
                        if (seen.count(path))
                            continue;
                        seen.insert(path);
                        std::cout << path << std::endl;
                    }
                    return Result<void>();
                }

                // Convert to unified items and render
                std::vector<UnifiedItem> items;
                items.reserve(resp.results.size());
                for (const auto& r : resp.results) {
                    items.push_back(UnifiedItem::fromLocal(r));
                }

                RenderContext ctx;
                ctx.query = query_;
                ctx.totalCount = resp.total;
                ctx.elapsedMs = resp.executionTimeMs;
                ctx.method = resp.type;

                auto renderResult = renderResults(items, ctx);
                (void)printDiffForSearchResult(resp);
                return renderResult;
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
                const auto ipcStart = std::chrono::steady_clock::now();
                auto r = co_await callOnce(dreq);
                const auto ipcEnd = std::chrono::steady_clock::now();
                spdlog::info(
                    "[TIMING] IPC call completed in {}ms",
                    std::chrono::duration_cast<std::chrono::milliseconds>(ipcEnd - ipcStart)
                        .count());
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
                    const auto& resp = r.value();
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
            auto coroFut = boost::asio::co_spawn(getExecutor(), work(), boost::asio::use_future);
            if (fut.wait_for(std::chrono::seconds(30)) != std::future_status::ready) {
                spdlog::warn("search: daemon call timed out; falling back to local execution");
                auto fb = fallback();
                if (!fb)
                    return fb.error();
                return Result<void>();
            }
            auto rv = fut.get();
            // Ensure coroutine cleanup completes before destroying captured references
            if (coroFut.wait_for(std::chrono::milliseconds(100)) != std::future_status::ready) {
                spdlog::debug("search: coroutine cleanup still in progress");
            }
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

        // CWD scoping: add current directory as a path prefix filter
        if (scopeToCwd_) {
            std::error_code ec;
            auto cwd = std::filesystem::current_path(ec);
            if (!ec) {
                std::string cwdPrefix = cwd.string();
                std::replace(cwdPrefix.begin(), cwdPrefix.end(), '\\', '/');
                if (!cwdPrefix.empty() && cwdPrefix.back() != '/') {
                    cwdPrefix += '/';
                }
                includeGlobsExpanded.push_back(cwdPrefix + "**/*");
            }
        }

        std::shared_ptr<ui::SpinnerRunner> spinner =
            shouldShowSpinner() ? std::make_shared<ui::SpinnerRunner>() : nullptr;
        if (spinner) {
            spinner->start("Searching...");
        }
        auto stopSpinner = [spinner]() {
            if (spinner) {
                spinner->stop();
            }
        };

        auto localFallback = [&]() -> boost::asio::awaitable<Result<void>> {
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
                    stopSpinner();
                    if (!local)
                        co_return local.error();
                    auto resp = local.value();

                    // Handle paths-only output (special case - uses resp.paths)
                    if (pathsOnly_) {
                        std::unordered_set<std::string> seen;
                        for (const auto& p : resp.paths) {
                            if (!includeGlobsExpanded.empty() &&
                                !matchAnyGlob(p, includeGlobsExpanded))
                                continue;
                            if (seen.count(p))
                                continue;
                            seen.insert(p);
                            std::cout << p << std::endl;
                        }
                        co_return Result<void>();
                    }

                    // Convert to unified items and render
                    std::vector<UnifiedItem> localItems;
                    localItems.reserve(resp.results.size());
                    for (const auto& it : resp.results) {
                        if (!includeGlobsExpanded.empty() &&
                            !matchAnyGlob(it.path, includeGlobsExpanded))
                            continue;
                        localItems.push_back(UnifiedItem::fromLocal(it));
                    }

                    RenderContext localCtx;
                    localCtx.query = query_;
                    localCtx.totalCount = resp.total;
                    localCtx.elapsedMs = resp.executionTimeMs;
                    localCtx.method = resp.type;

                    co_return renderResults(localItems, localCtx);
                }
            }
            stopSpinner();
            co_return Error{ErrorCode::Unknown, "Failed to initialize local search services"};
        };

        // Daemon client config
        yams::daemon::DaemonClient::setTimeoutEnvVars(std::chrono::milliseconds(headerTimeoutMs_),
                                                      std::chrono::milliseconds(bodyTimeoutMs_));
        yams::daemon::ClientConfig clientConfig;
        clientConfig.headerTimeout = std::chrono::milliseconds(headerTimeoutMs_);
        clientConfig.bodyTimeout = std::chrono::milliseconds(bodyTimeoutMs_);
        clientConfig.requestTimeout = std::chrono::milliseconds(30000);
        clientConfig.enableChunkedResponses = enableStreaming_;
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
            spdlog::warn("search: unable to acquire daemon client: {}", leaseRes.error().message);
            if (!(jsonOutput_ || (cli_ && cli_->getJsonOutput()))) {
                std::cout << "Daemon unavailable; falling back to local search.\n";
            }
            co_return co_await localFallback();
        }
        auto leaseHandle = std::move(leaseRes.value());
        struct LeaseScopeExit {
            std::shared_ptr<yams::cli::DaemonClientPool::Lease>* handle;
            ~LeaseScopeExit() {
                if (handle) {
                    handle->reset();
                }
            }
        } leaseGuard{&leaseHandle};
        auto& client = **leaseHandle;
        client.setStreamingEnabled(clientConfig.enableChunkedResponses);

        // Request + render
        yams::daemon::SearchRequest dreq;
        // Sanitize query to ensure valid UTF-8 (required by Protocol Buffer)
        dreq.query = yams::common::sanitizeUtf8(query_);
        if (dreq.query != query_) {
            spdlog::debug("Search query contained invalid UTF-8; sanitized for IPC transmission");
        }
        // Request one extra result to account for potential deduplication
        dreq.limit = static_cast<size_t>(limit_) + 1;
        dreq.fuzzy = fuzzySearch_;
        dreq.literalText = literalText_;
        dreq.similarity = (minSimilarity_ > 0.0f) ? static_cast<double>(minSimilarity_) : 0.7;
        dreq.pathsOnly = false; // Always fetch full results; pathsOnly is a CLI display option
        dreq.searchType = searchType_;
        dreq.jsonOutput = jsonOutput_;
        dreq.showHash = showHash_;
        dreq.verbose = verbose_;
        dreq.showLineNumbers = showLineNumbers_;
        // Engine-level filters
        dreq.pathPattern = pathFilter_;
        if (!includeGlobsExpanded.empty()) {
            dreq.pathPatterns = includeGlobsExpanded; // Send all patterns for server-side filtering
        } else if (!pathFilter_.empty()) {
            dreq.pathPatterns.push_back(pathFilter_); // Fallback for single pattern
        }
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
            stopSpinner();
            // Convert daemon results to unified items
            std::vector<UnifiedItem> items;
            items.reserve(resp.results.size());
            for (const auto& r : resp.results) {
                // Apply glob filtering during conversion
                std::string path =
                    !r.path.empty()
                        ? r.path
                        : (r.metadata.count("path") ? r.metadata.at("path") : std::string{});
                if (!includeGlobsExpanded.empty() && !matchAnyGlob(path, includeGlobsExpanded))
                    continue;
                items.push_back(UnifiedItem::fromDaemon(r));
            }

            RenderContext ctx;
            ctx.query = query_;
            ctx.totalCount = resp.totalCount;
            ctx.elapsedMs = resp.elapsed.count();
            ctx.method = "daemon";

            return renderResults(items, ctx);
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

            auto timer = std::make_shared<boost::asio::steady_timer>(getExecutor());
            timer->expires_after(std::chrono::seconds(2));

            boost::asio::co_spawn(
                getExecutor(),
                [&, decided, prom, leaseHandle, timer]() -> boost::asio::awaitable<void> {
                    auto& cliRef = **leaseHandle;
                    auto sr = co_await cliRef.streamingSearch(dreq);
                    if (!decided->exchange(true)) {
                        prom->set_value(std::move(sr));
                        timer->cancel();
                    }
                    co_return;
                },
                boost::asio::detached);

            boost::asio::co_spawn(
                getExecutor(),
                [&, decided, prom, leaseHandle, timer]() -> boost::asio::awaitable<void> {
                    boost::system::error_code ec;
                    co_await timer->async_wait(
                        boost::asio::redirect_error(boost::asio::use_awaitable, ec));
                    if (!ec && !decided->load()) {
                        auto& cliRef = **leaseHandle;
                        auto ur = co_await cliRef.call(dreq);
                        if (!decided->exchange(true))
                            prom->set_value(std::move(ur));
                    }
                    co_return;
                },
                boost::asio::detached);

            auto wait_ms = static_cast<int>(bodyTimeoutMs_ > 0 ? bodyTimeoutMs_ : 60000);
            if (fut.wait_for(std::chrono::milliseconds(wait_ms)) == std::future_status::ready) {
                result_stream_or_unary = fut.get();
            } else {
                result_stream_or_unary = Error{ErrorCode::Timeout, "Search timed out"};
            }
            timer->cancel();
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
            const auto& resp = r.value();
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
        co_return co_await localFallback();
    }
};

// Factory function
std::unique_ptr<ICommand> createSearchCommand() {
    return std::make_unique<SearchCommand>();
}

} // namespace yams::cli
