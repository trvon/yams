#include <spdlog/spdlog.h>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <map>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <unistd.h>
#include <unordered_set>
#include <vector>
#include <yams/cli/command.h>
#include <yams/cli/session_store.h>
#include <yams/cli/yams_cli.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine_builder.h>
#include <yams/vector/vector_index_manager.h>
// Daemon client API for daemon-first grep
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/system_executor.hpp>
#include <boost/asio/this_coro.hpp>
// Timers for streaming guard
#include <future>
#include <boost/asio/steady_timer.hpp>
#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/response_of.hpp>
// Retrieval facade for daemon-first grep
#include <yams/app/services/retrieval_service.h>

namespace yams::cli {

class GrepCommand : public ICommand {
private:
    // Member variables
    YamsCLI* cli_ = nullptr;
    std::string pattern_;
    std::vector<std::string> paths_;
    std::string includePatterns_;
    size_t afterContext_ = 0;
    size_t beforeContext_ = 0;
    size_t context_ = 0;
    bool ignoreCase_ = false;
    bool wholeWord_ = false;
    bool invertMatch_ = false;
    bool showLineNumbers_ = false;
    bool showFilename_ = false;
    bool noFilename_ = false;
    bool countOnly_ = false;
    bool filesOnly_ = false;
    bool filesWithoutMatch_ = false;
    bool pathsOnly_ = false;
    bool literalText_ = false;
    bool regexOnly_ = false;
    // Default to streaming for quicker first results; user can opt out
    bool disableStreaming_ = false;
    // Force thorough (unary) mode: disables streaming and the guard
    bool cold_{false};
    size_t semanticLimit_ = 10;
    std::string filterTags_;
    bool matchAllTags_ = false;
    std::string colorMode_ = "auto";
    size_t maxCount_ = 20;
    // Session scoping
    std::optional<std::string> sessionOverride_{};
    bool noSession_{false};
    std::vector<std::string> sessionPatterns_;

public:
    std::string getName() const override { return "grep"; }

    std::string getDescription() const override {
        return "Search for regex patterns within file contents";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("grep", getDescription());

        // Positional pattern: make optional and provide friendly alternatives (-e/--pattern or --)
        cmd->add_option("pattern", pattern_,
                        "Regular expression pattern to search for (if it begins with '-', use -- "
                        "to end options or -e/--expr)");

        // Explicit pattern option to handle leading '-' patterns ergonomically
        cmd->add_option(
            "-e,--expr", pattern_,
            "Explicit pattern (use when pattern starts with '-' or to avoid ambiguity)");

        cmd->add_option("paths", paths_,
                        "Files or directories to search (default: all indexed files)");

        // Pattern filtering
        cmd->add_option("--include", includePatterns_,
                        "File patterns to include (e.g., '*.md,*.txt')");

        // Context options
        cmd->add_option("-A,--after", afterContext_, "Show N lines after match")->default_val(0);
        cmd->add_option("-B,--before", beforeContext_, "Show N lines before match")->default_val(0);
        cmd->add_option("-C,--context", context_, "Show N lines before and after match")
            ->default_val(0);

        // Search options
        cmd->add_flag("-i,--ignore-case", ignoreCase_, "Case-insensitive search");
        cmd->add_flag("-w,--word", wholeWord_, "Match whole words only");
        cmd->add_flag("-v,--invert", invertMatch_, "Invert match (show non-matching lines)");
        cmd->add_flag("-n,--line-numbers", showLineNumbers_, "Show line numbers");
        cmd->add_flag("-H,--with-filename", showFilename_, "Show filename with matches");
        cmd->add_flag("--no-filename", noFilename_, "Never show filename");
        cmd->add_flag("-c,--count", countOnly_, "Show only count of matching lines");
        cmd->add_flag("-l,--files-with-matches", filesOnly_, "Show only filenames with matches");
        cmd->add_flag("-L,--files-without-match", filesWithoutMatch_,
                      "Show only filenames without matches");
        cmd->add_flag("--paths-only", pathsOnly_, "Show only file paths (no content)");
        cmd->add_flag("--literal-text", literalText_,
                      "Treat pattern as literal text, not regex (escapes special characters)");

        // Hybrid search options
        cmd->add_flag("--regex-only", regexOnly_, "Disable semantic search, use regex only");
        cmd->add_option("--semantic-limit", semanticLimit_, "Number of semantic results to show")
            ->default_val(10);

        // Tag filtering options
        cmd->add_option("--tags", filterTags_,
                        "Filter results by tags (comma-separated, e.g., work,important)");
        cmd->add_flag("--match-all-tags", matchAllTags_,
                      "Require all specified tags (default: match any)");

        // Output options
        cmd->add_option("--color", colorMode_, "Color mode: always, never, auto")
            ->default_val("auto")
            ->check(CLI::IsMember({"always", "never", "auto"}));

        cmd->add_option("-m,--max-count", maxCount_, "Stop after N matches per file")
            ->default_val(20);

        cmd->add_option("--limit", maxCount_,
                        "Alias: stop after N matches per file (same as --max-count)")
            ->default_val(20);

        // Streaming control
        cmd->add_flag("--no-streaming", disableStreaming_,
                      "Disable streaming responses from daemon");
        // Thorough (non-streaming) mode
        cmd->add_flag("--cold", cold_, "Force thorough (non-streaming) execution");

        // Session scoping controls
        cmd->add_option("--session", sessionOverride_, "Use this session for scoping");
        cmd->add_flag("--no-session", noSession_, "Bypass session scoping");

        cmd->callback([this]() {
            // Validate presence of a pattern with helpful guidance
            if (pattern_.empty()) {
                throw CLI::ValidationError(
                    "pattern",
                    "Pattern not provided. Tip: if your pattern starts with '-' (e.g., "
                    "'--tags|foo'), use -- to end options: \n  yams grep -- \"--tags|knowledge "
                    "graph|kg\" --include=\"docs/**/*.md\"\nOr use the explicit option: \n  yams "
                    "grep -e \"--tags|knowledge graph|kg\" --include=\"docs/**/*.md\"");
            }
            // Normalize popular PCRE inline flags like (?i) to CLI flags and ECMAScript-compatible
            // regex Detect and strip all occurrences of (?i) while enabling -i when present
            bool modified = false;
            {
                std::string normalized = pattern_;
                const std::string needle = "(?i)";
                size_t pos = 0;
                while ((pos = normalized.find(needle, pos)) != std::string::npos) {
                    normalized.erase(pos, needle.size());
                    modified = true;
                }
                // Also handle the common anchored form (?i:...)
                // Replace leading "(?i:" with "(" and trailing matching ')' already present
                // remains.
                if (normalized.rfind("(?i:", 0) == 0) {
                    normalized.erase(2, 2); // remove 'i:' after '(?'
                    modified = true;
                }
                if (modified) {
                    if (!ignoreCase_)
                        ignoreCase_ = true;
                    pattern_ = normalized;
                    // Best-effort UX: print a proposed normalized command preview
                    std::ostringstream pcmd;
                    pcmd << "Proposed Command\n  └ yams grep \"" << pattern_ << "\"";
                    if (!includePatterns_.empty()) {
                        pcmd << " --include=\"" << includePatterns_ << "\"";
                    }
                    if (showFilename_)
                        pcmd << " -H";
                    if (showLineNumbers_)
                        pcmd << " -n";
                    if (regexOnly_)
                        pcmd << " --regex-only";
                    if (ignoreCase_)
                        pcmd << " -i";
                    if (pathsOnly_)
                        pcmd << " --paths-only";
                    std::cerr << pcmd.str() << std::endl;
                }
            }
            if (!noSession_) {
                sessionPatterns_ =
                    yams::cli::session_store::active_include_patterns(sessionOverride_);
            } else {
                sessionPatterns_.clear();
            }
            if (cold_) {
                disableStreaming_ = true;
            }
            auto result = execute();
            if (!result) {
                spdlog::error("Grep failed: {}", result.error().message);
                throw CLI::RuntimeError(1);
            }
        });
    }

    Result<void> execute() override {
        try {
            // Attempt daemon-first grep with complete protocol mapping
            {
                yams::daemon::GrepRequest dreq;
                dreq.pattern = pattern_;
                dreq.paths = paths_; // Use new paths field for multiple paths
                if (dreq.paths.empty() && !sessionPatterns_.empty()) {
                    dreq.paths = sessionPatterns_;
                }
                dreq.caseInsensitive = ignoreCase_;
                dreq.invertMatch = invertMatch_;

                // Handle context options with proper precedence
                if (context_ > 0) {
                    dreq.contextLines = static_cast<int>(context_);
                    dreq.beforeContext = static_cast<int>(context_);
                    dreq.afterContext = static_cast<int>(context_);
                } else {
                    dreq.contextLines = 0;
                    dreq.beforeContext = static_cast<int>(beforeContext_);
                    dreq.afterContext = static_cast<int>(afterContext_);
                }

                // Map all CLI options to daemon protocol
                dreq.includePatterns = parseCommaSeparated(includePatterns_);
                dreq.recursive = true; // Default to recursive
                dreq.wholeWord = wholeWord_;
                dreq.showLineNumbers = showLineNumbers_;
                dreq.showFilename = showFilename_;
                dreq.noFilename = noFilename_;
                dreq.countOnly = countOnly_;
                dreq.filesOnly = filesOnly_;
                dreq.filesWithoutMatch = filesWithoutMatch_;
                dreq.pathsOnly = pathsOnly_;
                dreq.literalText = literalText_;
                dreq.regexOnly = regexOnly_;
                dreq.semanticLimit = semanticLimit_;
                dreq.filterTags = parseCommaSeparated(filterTags_);
                dreq.matchAllTags = matchAllTags_;
                dreq.colorMode = colorMode_;
                dreq.maxMatches = maxCount_;

                auto render = [&](const yams::daemon::GrepResponse& resp) -> Result<void> {
                    // Informative note: reflect the normalized command actually executed
                    {
                        std::ostringstream ran;
                        ran << "Ran yams grep \"" << pattern_ << "\"";
                        if (!includePatterns_.empty())
                            ran << " --include=\"" << includePatterns_ << "\"";
                        if (showFilename_)
                            ran << " -H";
                        if (showLineNumbers_)
                            ran << " -n";
                        if (regexOnly_)
                            ran << " --regex-only";
                        if (ignoreCase_)
                            ran << " -i";
                        if (pathsOnly_)
                            ran << " --paths-only";
                        std::cerr << ran.str() << std::endl;
                    }
                    // Handle different output modes
                    if (pathsOnly_ || filesOnly_) {
                        // Show files from regex and semantic results (semantic marked with
                        // confidence when no regex)
                        std::set<std::string> files;
                        std::map<std::string, bool> hasRegex;
                        std::map<std::string, double> semOnlyConf;

                        for (const auto& match : resp.matches) {
                            files.insert(match.file);
                            if (match.matchType == "semantic") {
                                auto it = semOnlyConf.find(match.file);
                                if (it == semOnlyConf.end() || match.confidence > it->second) {
                                    semOnlyConf[match.file] = match.confidence;
                                }
                            } else {
                                hasRegex[match.file] = true;
                            }
                        }

                        if (files.empty()) {
                            std::cout << "(no results)" << std::endl;
                        } else {
                            for (const auto& file : files) {
                                auto itR = hasRegex.find(file);
                                auto itS = semOnlyConf.find(file);
                                if ((itR == hasRegex.end() || !itR->second) &&
                                    itS != semOnlyConf.end()) {
                                    std::cout << "[S:" << std::fixed << std::setprecision(2)
                                              << itS->second << "] " << file << std::endl;
                                } else {
                                    std::cout << file << std::endl;
                                }
                            }
                        }
                    } else if (countOnly_) {
                        // Count regex matches per file; also surface semantic suggestions
                        // separately
                        std::map<std::string, size_t> fileCounts;
                        std::set<std::string> regexFiles;
                        std::map<std::string, double> semanticOnly;

                        for (const auto& match : resp.matches) {
                            if (match.matchType == "semantic") {
                                if (regexFiles.find(match.file) == regexFiles.end()) {
                                    auto it = semanticOnly.find(match.file);
                                    if (it == semanticOnly.end() || match.confidence > it->second) {
                                        semanticOnly[match.file] = match.confidence;
                                    }
                                }
                            } else {
                                regexFiles.insert(match.file);
                                fileCounts[match.file]++;
                                // If this file was previously marked as semantic-only, it is no
                                // longer semantic-only
                                semanticOnly.erase(match.file);
                            }
                        }

                        if (fileCounts.empty() && semanticOnly.empty()) {
                            std::cout << "(no results)" << std::endl;
                        } else {
                            for (const auto& [file, count] : fileCounts) {
                                if (showFilename_ || fileCounts.size() > 1) {
                                    std::cout << file << ":";
                                }
                                std::cout << count << std::endl;
                            }
                            if (!semanticOnly.empty()) {
                                std::cout << "Semantic suggestions:" << std::endl;
                                for (const auto& [file, conf] : semanticOnly) {
                                    std::cout << "[S:" << std::fixed << std::setprecision(2) << conf
                                              << "] " << file << std::endl;
                                }
                            }
                        }
                    } else {
                        if (resp.matches.empty()) {
                            std::cout << "(no results)" << std::endl;
                            return Result<void>();
                        }
                        // Full match output with match type indicators
                        size_t regexCount = 0, semanticCount = 0;

                        for (const auto& match : resp.matches) {
                            // Track match types for summary
                            if (match.matchType == "semantic") {
                                semanticCount++;
                            } else {
                                regexCount++;
                            }

                            // Build output line with match type indicator
                            if (showFilename_ || resp.matches.size() > 1) {
                                std::cout << match.file << ":";
                            }
                            if (showLineNumbers_) {
                                std::cout << match.lineNumber << ":";
                            }

                            // Add match type indicator
                            if (match.matchType == "semantic") {
                                // Show semantic match with confidence
                                std::cout << "[S:" << std::fixed << std::setprecision(2)
                                          << match.confidence << "] ";
                            } else if (match.matchType == "hybrid") {
                                std::cout << "[H] ";
                            } else {
                                // Regex match - only show indicator if we have mixed results
                                if (!regexOnly_ && semanticLimit_ > 0) {
                                    std::cout << "[R] ";
                                }
                            }

                            std::cout << match.line << std::endl;

                            // Show context lines if any
                            for (const auto& ctx : match.contextBefore) {
                                std::cout << "  " << ctx << std::endl;
                            }
                            for (const auto& ctx : match.contextAfter) {
                                std::cout << "  " << ctx << std::endl;
                            }
                        }

                        // Show summary if we have mixed match types
                        if (regexCount > 0 && semanticCount > 0) {
                            std::cout << "\n[Summary: " << regexCount << " regex matches, "
                                      << semanticCount << " semantic matches]" << std::endl;
                        }
                    }

                    if (resp.totalMatches > 0) {
                        spdlog::debug("Found {} matches in {} files", resp.totalMatches,
                                      resp.matches.size());
                    }
                    return Result<void>();
                };

                // Use RetrievalService facade (daemon-first)
                yams::app::services::RetrievalService rsvc;
                yams::app::services::RetrievalOptions ropts;
                if (cli_ && cli_->hasExplicitDataDir()) {
                    ropts.explicitDataDir = cli_->getDataPath();
                }
                ropts.enableStreaming = !disableStreaming_;
                ropts.headerTimeoutMs = 30000;
                ropts.bodyTimeoutMs = 120000;
                ropts.requestTimeoutMs = 30000;
                auto gres = rsvc.grep(dreq, ropts);
                if (!gres) {
                    if (gres.error().code == ErrorCode::Timeout) {
                        spdlog::warn(
                            "grep: daemon call timed out; falling back to local execution");
                        return executeLocal();
                    }
                    return gres.error();
                }
                auto rr = render(gres.value());
                if (!rr)
                    return rr.error();
                return Result<void>();
            }

            // Fall back to local execution if daemon failed
            return executeLocal();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string("Unexpected error: ") + e.what()};
        }
    }

private:
    // Helper function to parse comma-separated strings into vector
    std::vector<std::string> parseCommaSeparated(const std::string& input) {
        std::vector<std::string> result;
        if (input.empty())
            return result;

        std::stringstream ss(input);
        std::string item;
        while (std::getline(ss, item, ',')) {
            // Trim whitespace
            item.erase(0, item.find_first_not_of(" \t"));
            item.erase(item.find_last_not_of(" \t") + 1);
            if (!item.empty()) {
                result.push_back(item);
            }
        }
        return result;
    }

    Result<void> executeLocal() {
        auto ensured = cli_->ensureStorageInitialized();
        if (!ensured) {
            return ensured;
        }

        auto metadataRepo = cli_->getMetadataRepository();
        if (!metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not initialized"};
        }

        auto store = cli_->getContentStore();
        if (!store) {
            return Error{ErrorCode::NotInitialized, "Content store not initialized"};
        }

        // Handle context options
        if (context_ > 0) {
            beforeContext_ = afterContext_ = context_;
        }

        // Determine if we should show filenames
        bool multipleFiles = paths_.size() != 1;
        if (!noFilename_ && (showFilename_ || multipleFiles)) {
            showFilename_ = true;
        }

        // Build regex pattern for local grep matching
        std::regex_constants::syntax_option_type flags = std::regex_constants::ECMAScript;
        if (ignoreCase_) {
            flags |= std::regex_constants::icase;
        }

        std::string regexPattern = pattern_;

        // For local regex matching, we still need to escape for grep functionality
        // But for search engine queries, we'll pass literalText_ flag separately
        if (literalText_) {
            std::string escaped;
            for (char c : pattern_) {
                if (c == '.' || c == '*' || c == '?' || c == '+' || c == '[' || c == ']' ||
                    c == '(' || c == ')' || c == '{' || c == '}' || c == '^' || c == '$' ||
                    c == '|' || c == '\\') {
                    escaped += "\\";
                }
                escaped += c;
            }
            regexPattern = escaped;
        }

        if (wholeWord_) {
            regexPattern = "\\b" + regexPattern + "\\b";
        }

        std::regex regex;
        try {
            regex = std::regex(regexPattern, flags);
        } catch (const std::regex_error& e) {
            return Error{ErrorCode::InvalidArgument,
                         "Invalid regex pattern: " + std::string(e.what())};
        }

        // Get documents to search
        std::vector<metadata::DocumentInfo> documents;

        if (paths_.empty()) {
            // Apply tag filter if specified
            if (!filterTags_.empty()) {
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

                if (!tags.empty()) {
                    auto docsResult = metadataRepo->findDocumentsByTags(tags, matchAllTags_);
                    if (!docsResult) {
                        return Error{ErrorCode::DatabaseError,
                                     "Failed to query documents by tags: " +
                                         docsResult.error().message};
                    }
                    documents = docsResult.value();
                } else {
                    // No valid tags, search all files
                    auto docsResult = metadataRepo->findDocumentsByPath("%");
                    if (!docsResult) {
                        return Error{ErrorCode::DatabaseError,
                                     "Failed to query documents: " + docsResult.error().message};
                    }
                    documents = docsResult.value();
                }
            } else {
                // Search all indexed files
                auto docsResult = metadataRepo->findDocumentsByPath("%");
                if (!docsResult) {
                    return Error{ErrorCode::DatabaseError,
                                 "Failed to query documents: " + docsResult.error().message};
                }
                documents = docsResult.value();
            }
        } else {
            // Search specific paths
            for (const auto& path : paths_) {
                std::filesystem::path fsPath(path);

                // Check if path is a directory
                if (std::filesystem::exists(fsPath) && std::filesystem::is_directory(fsPath)) {
                    // For directories, search all files within
                    std::string pattern = path;
                    if (pattern.back() != '/') {
                        pattern += '/';
                    }
                    pattern += '%';

                    auto docsResult = metadataRepo->findDocumentsByPath(pattern);
                    if (docsResult) {
                        for (const auto& doc : docsResult.value()) {
                            documents.push_back(doc);
                        }
                    }
                } else {
                    // For files or patterns, try exact match first
                    auto docsResult = metadataRepo->findDocumentsByPath(path);
                    if (docsResult) {
                        for (const auto& doc : docsResult.value()) {
                            documents.push_back(doc);
                        }
                    }

                    // Also try as a suffix pattern if no exact match
                    if (!docsResult || docsResult.value().empty()) {
                        auto suffixResult = metadataRepo->findDocumentsByPath("%/" + path);
                        if (suffixResult && !suffixResult.value().empty()) {
                            for (const auto& doc : suffixResult.value()) {
                                documents.push_back(doc);
                            }
                        }
                    }
                }
            }
        }

        // Apply include pattern filtering if specified
        if (!includePatterns_.empty()) {
            std::vector<metadata::DocumentInfo> filteredDocs;
            auto expandedPatterns = splitPatterns({includePatterns_});

            for (const auto& doc : documents) {
                bool shouldInclude = false;

                for (const auto& pattern : expandedPatterns) {
                    // Check if pattern contains path components
                    if (pattern.find('/') != std::string::npos) {
                        if (matchesPattern(doc.filePath, pattern)) {
                            shouldInclude = true;
                            break;
                        }
                    } else {
                        std::string fileName =
                            std::filesystem::path(doc.filePath).filename().string();
                        if (matchesPattern(fileName, pattern)) {
                            shouldInclude = true;
                            break;
                        }
                    }
                }

                if (shouldInclude) {
                    filteredDocs.push_back(doc);
                }
            }

            documents = filteredDocs;
        }

        // Optional FTS prefilter: when literal/word-style queries, use the SQLite FTS index
        // to narrow the candidate document set before content scanning.
        if (!documents.empty() && (literalText_ || wholeWord_)) {
            try {
                auto sRes = metadataRepo->search(pattern_, /*limit*/ 2000, /*offset*/ 0);
                if (sRes && sRes.value().isSuccess() && !sRes.value().results.empty()) {
                    std::unordered_set<int64_t> allow;
                    allow.reserve(sRes.value().results.size());
                    for (const auto& r : sRes.value().results)
                        allow.insert(r.document.id);
                    std::vector<metadata::DocumentInfo> filtered;
                    filtered.reserve(documents.size());
                    for (auto& d : documents) {
                        if (allow.find(d.id) != allow.end())
                            filtered.push_back(std::move(d));
                    }
                    if (!filtered.empty())
                        documents.swap(filtered);
                }
            } catch (...) {
                // Best-effort optimization; ignore failures
            }
        }

        if (documents.empty()) {
            std::cerr << "No files to search" << std::endl;
            return Result<void>();
        }

        // Process each document for regex matches
        // size_t totalMatches = 0;  // Currently only incremented but not used
        std::vector<std::string> matchingFiles;
        std::vector<std::string> nonMatchingFiles;
        std::map<std::string, std::vector<Match>> allRegexMatches;

        for (const auto& doc : documents) {
            // Retrieve document content
            auto contentResult = store->retrieveBytes(doc.sha256Hash);
            if (!contentResult) {
                continue; // Skip if can't retrieve
            }

            std::string content(reinterpret_cast<const char*>(contentResult.value().data()),
                                contentResult.value().size());

            // Process the file for regex matches
            auto matches = processFile(doc.filePath, content, regex);

            if (!matches.empty()) {
                allRegexMatches[doc.filePath] = matches;
                matchingFiles.push_back(doc.filePath);
            } else {
                nonMatchingFiles.push_back(doc.filePath);
            }
        }

        // If not regex-only mode, also perform semantic search (even in files-only/paths-only/count
        // modes)
        std::vector<search::HybridSearchResult> semanticResults;
        if (!regexOnly_) {
            try {
                // Build HybridSearchEngine for semantic search
                auto vecMgr = std::make_shared<yams::vector::VectorIndexManager>();
                yams::search::SearchEngineBuilder builder;
                builder.withVectorIndex(vecMgr)
                    .withMetadataRepo(metadataRepo)
                    .withKGStore(cli_->getKnowledgeGraphStore());

                auto opts = yams::search::SearchEngineBuilder::BuildOptions::makeDefault();
                // Prefer fast, exact FTS5 keyword results over vector similarity for grep-like use
                opts.hybrid.final_top_k = semanticLimit_ * 3; // Get more results to filter
                opts.hybrid.vector_weight = 0.40f;
                opts.hybrid.keyword_weight = 0.60f;
                opts.hybrid.enable_kg = false; // Disable KG for grep scenarios

                auto engRes = builder.buildEmbedded(opts);
                if (engRes) {
                    auto eng = engRes.value();
                    auto hres = eng->search(pattern_, semanticLimit_ * 3);
                    if (hres) {
                        // Filter semantic results to only include files from searched paths
                        if (!paths_.empty()) {
                            std::vector<search::HybridSearchResult> filteredResults;
                            for (const auto& result : hres.value()) {
                                auto pathIt = result.metadata.find("path");
                                if (pathIt != result.metadata.end()) {
                                    std::string resultPath = pathIt->second;
                                    // Check if this result is within any of the specified paths
                                    bool inSearchPath = false;
                                    for (const auto& searchPath : paths_) {
                                        if (resultPath.find(searchPath) != std::string::npos) {
                                            inSearchPath = true;
                                            break;
                                        }
                                    }
                                    if (inSearchPath) {
                                        filteredResults.push_back(result);
                                        if (filteredResults.size() >= semanticLimit_) {
                                            break;
                                        }
                                    }
                                }
                            }
                            semanticResults = filteredResults;
                        } else {
                            // No path filter, take top results
                            semanticResults = hres.value();
                            if (semanticResults.size() > semanticLimit_) {
                                semanticResults.resize(semanticLimit_);
                            }
                        }
                    }
                }
            } catch (const std::exception& e) {
                spdlog::debug("Semantic search failed (falling back to regex only): {}", e.what());
            }
        }

        // Output results based on mode
        if (filesOnly_ || pathsOnly_) {
            // Show files from regex and semantic results (semantic marked with confidence when no
            // regex)
            std::set<std::string> files(matchingFiles.begin(), matchingFiles.end());
            std::map<std::string, double> semOnlyConf;

            // Merge semantic paths (only when not already matched by regex)
            for (const auto& result : semanticResults) {
                auto pathIt = result.metadata.find("path");
                if (pathIt != result.metadata.end()) {
                    const std::string& p = pathIt->second;
                    if (files.find(p) == files.end()) {
                        double conf =
                            result.hybrid_score > 0 ? result.hybrid_score : result.vector_score;
                        auto itc = semOnlyConf.find(p);
                        if (itc == semOnlyConf.end() || conf > itc->second) {
                            semOnlyConf[p] = conf;
                        }
                        files.insert(p);
                    }
                }
            }

            if (files.empty()) {
                std::cout << "(no results)" << std::endl;
            } else {
                for (const auto& file : files) {
                    auto itc = semOnlyConf.find(file);
                    if (itc != semOnlyConf.end()) {
                        std::cout << "[S:" << std::fixed << std::setprecision(2) << itc->second
                                  << "] " << file << std::endl;
                    } else {
                        std::cout << file << std::endl;
                    }
                }
            }
        } else if (countOnly_) {
            // Output counts
            for (const auto& [filePath, matches] : allRegexMatches) {
                if (showFilename_) {
                    std::cout << filePath << ":";
                }
                std::cout << matches.size() << std::endl;
            }
        } else if (filesWithoutMatch_) {
            // Handle files-without-match option
            for (const auto& file : nonMatchingFiles) {
                std::cout << file << std::endl;
            }
        } else {
            // Hybrid output mode - show both regex and semantic results
            bool hasRegexMatches = !allRegexMatches.empty();
            bool hasSemanticResults = !semanticResults.empty() && !regexOnly_;

            if (hasRegexMatches) {
                if (hasSemanticResults) {
                    std::cout << "=== Text Matches ===" << std::endl;
                    std::cout << std::endl;
                }

                // Show top regex matches (limit to 3 files for balance)
                size_t fileCount = 0;
                for (const auto& [filePath, matches] : allRegexMatches) {
                    if (fileCount >= 3 && hasSemanticResults)
                        break; // Limit when showing both

                    // Retrieve content for printing
                    auto doc =
                        std::find_if(documents.begin(), documents.end(),
                                     [&filePath](const auto& d) { return d.filePath == filePath; });

                    if (doc != documents.end()) {
                        auto contentResult = store->retrieveBytes(doc->sha256Hash);
                        if (contentResult) {
                            std::string content(
                                reinterpret_cast<const char*>(contentResult.value().data()),
                                contentResult.value().size());

                            // Apply per-file limit
                            auto limitedMatches = matches;
                            if (maxCount_ > 0 &&
                                limitedMatches.size() > static_cast<size_t>(maxCount_)) {
                                limitedMatches.resize(maxCount_);
                            }

                            printMatches(filePath, content, limitedMatches);
                        }
                    }
                    fileCount++;
                }
            }

            if (hasSemanticResults) {
                if (hasRegexMatches) {
                    std::cout << std::endl;
                }
                std::cout << "=== Semantic Matches ===" << std::endl;
                std::cout << std::endl;

                // Show semantic results
                for (size_t i = 0; i < semanticResults.size() && i < semanticLimit_; i++) {
                    const auto& result = semanticResults[i];

                    // Extract path from metadata
                    auto pathIt = result.metadata.find("path");
                    std::string path = (pathIt != result.metadata.end()) ? pathIt->second : "";

                    // Skip if this file already shown in regex matches
                    if (allRegexMatches.find(path) != allRegexMatches.end()) {
                        continue;
                    }

                    std::cout << (i + 1) << ". ";
                    auto titleIt = result.metadata.find("title");
                    if (titleIt != result.metadata.end()) {
                        std::cout << titleIt->second;
                    } else {
                        std::cout << result.id;
                    }

                    if (!path.empty()) {
                        std::cout << " (" << path << ")";
                    }
                    std::cout << std::endl;

                    // Show snippet if available
                    if (!result.content.empty()) {
                        std::string snippet = truncateSnippet(result.content, 200);
                        std::cout << "   " << snippet << std::endl;
                    }
                    std::cout << std::endl;
                }
            }

            if (!hasRegexMatches && !hasSemanticResults) {
                std::cout << "No matches found for pattern: " << pattern_ << std::endl;
            }

            return Result<void>();
        }
        return Result<void>();
    }

private:
    struct Match {
        size_t lineNumber;
        size_t columnStart;
        size_t columnEnd;
        std::string line;
    };

    std::vector<Match> processFile(const std::string& /*filename*/, const std::string& content,
                                   const std::regex& regex) {
        std::vector<Match> matches;
        std::istringstream stream(content);
        std::string line;
        size_t lineNumber = 1;

        while (std::getline(stream, line)) {
            bool hasMatch = false;
            std::smatch match;
            std::string searchLine = line;
            size_t columnOffset = 0;

            while (std::regex_search(searchLine, match, regex)) {
                if (!invertMatch_) {
                    Match m;
                    m.lineNumber = lineNumber;
                    m.columnStart = columnOffset + match.position();
                    m.columnEnd = m.columnStart + match.length();
                    m.line = line;
                    matches.push_back(m);
                    hasMatch = true;
                }

                columnOffset += match.position() + match.length();
                searchLine = match.suffix();

                // For count/files only modes, one match per line is enough
                if (countOnly_ || filesOnly_ || filesWithoutMatch_) {
                    break;
                }
            }

            // Handle inverted match
            if (invertMatch_ && !hasMatch) {
                Match m;
                m.lineNumber = lineNumber;
                m.columnStart = 0;
                m.columnEnd = 0;
                m.line = line;
                matches.push_back(m);
            }

            lineNumber++;
        }

        return matches;
    }

    void printMatches(const std::string& filename, const std::string& content,
                      const std::vector<Match>& matches) {
        // Split content into lines for context printing
        std::vector<std::string> lines;
        std::istringstream stream(content);
        std::string line;
        while (std::getline(stream, line)) {
            lines.push_back(line);
        }

        // Track which lines we've already printed (for context overlap)
        std::set<size_t> printedLines;

        for (const auto& match : matches) {
            // Calculate context range
            size_t startLine =
                (match.lineNumber > beforeContext_) ? match.lineNumber - beforeContext_ : 1;
            size_t endLine = std::min(match.lineNumber + afterContext_, lines.size());

            // Print separator if needed
            if (!printedLines.empty() && startLine > *printedLines.rbegin() + 1) {
                std::cout << "--" << std::endl;
            }

            // Print context and match
            for (size_t i = startLine; i <= endLine; ++i) {
                if (printedLines.count(i) > 0) {
                    continue; // Already printed this line
                }
                printedLines.insert(i);

                if (i - 1 >= lines.size()) {
                    continue;
                }

                // Print filename if needed
                if (showFilename_) {
                    std::cout << filename << ":";
                }

                // Print line number if needed
                if (showLineNumbers_) {
                    std::cout << std::setw(6) << i << ":";
                }

                // Print the line with highlighting if it's a match line
                if (i == match.lineNumber && !invertMatch_) {
                    printHighlightedLine(lines[i - 1], match);
                } else {
                    std::cout << lines[i - 1] << std::endl;
                }
            }
        }
    }

    void printHighlightedLine(const std::string& line, const Match& match) {
        // Simple highlighting with color codes if enabled
        bool useColor =
            (colorMode_ == "always") || (colorMode_ == "auto" && isatty(fileno(stdout)));

        if (useColor && match.columnEnd > match.columnStart) {
            std::cout << line.substr(0, match.columnStart);
            std::cout << "\033[1;31m"; // Bold red
            std::cout << line.substr(match.columnStart, match.columnEnd - match.columnStart);
            std::cout << "\033[0m"; // Reset
            std::cout << line.substr(match.columnEnd);
        } else {
            std::cout << line;
        }
        std::cout << std::endl;
    }

    std::vector<std::string> splitPatterns(const std::vector<std::string>& patterns) {
        std::vector<std::string> result;
        for (const auto& pattern : patterns) {
            std::stringstream ss(pattern);
            std::string item;
            while (std::getline(ss, item, ',')) {
                // Trim whitespace
                item.erase(0, item.find_first_not_of(" \t"));
                item.erase(item.find_last_not_of(" \t") + 1);
                if (!item.empty()) {
                    result.push_back(item);
                }
            }
        }
        return result;
    }

    // Helper function to truncate snippet to a maximum length at word boundary
    std::string truncateSnippet(const std::string& snippet, size_t maxLength) {
        // Remove newlines and multiple spaces
        std::string cleaned;
        bool lastWasSpace = false;
        for (char c : snippet) {
            if (c == '\n' || c == '\r' || c == '\t') {
                if (!lastWasSpace) {
                    cleaned += ' ';
                    lastWasSpace = true;
                }
            } else if (c == ' ') {
                if (!lastWasSpace) {
                    cleaned += c;
                    lastWasSpace = true;
                }
            } else {
                cleaned += c;
                lastWasSpace = false;
            }
        }

        // Trim to max length at word boundary
        if (cleaned.length() <= maxLength) {
            return cleaned;
        }

        // Find last space before maxLength
        size_t lastSpace = cleaned.rfind(' ', maxLength);
        if (lastSpace != std::string::npos && lastSpace > maxLength * 0.7) {
            return cleaned.substr(0, lastSpace) + "...";
        }

        // No good word boundary, just truncate
        return cleaned.substr(0, maxLength) + "...";
    }

    bool matchesPattern(const std::string& text, const std::string& pattern) {
        // Simple wildcard matching (* and ?)
        std::string regexString = pattern;

        // Escape regex special characters except * and ?
        std::string escaped;
        for (char c : regexString) {
            if (c == '*') {
                escaped += ".*";
            } else if (c == '?') {
                escaped += ".";
            } else if (c == '.' || c == '[' || c == ']' || c == '(' || c == ')' || c == '{' ||
                       c == '}' || c == '+' || c == '^' || c == '$' || c == '|' || c == '\\') {
                escaped += "\\";
                escaped += c;
            } else {
                escaped += c;
            }
        }

        try {
            std::regex regexPattern(escaped, std::regex_constants::icase);
            return std::regex_match(text, regexPattern);
        } catch (const std::regex_error&) {
            // If regex fails, fall back to simple string comparison
            return text == pattern;
        }
    }
};

// Factory function
std::unique_ptr<ICommand> createGrepCommand() {
    return std::make_unique<GrepCommand>();
}

} // namespace yams::cli
