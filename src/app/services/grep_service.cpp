#include "../../cli/hot_cold_utils.h"
#include <yams/app/services/literal_extractor.hpp>
#include <yams/app/services/simd_newline_scanner.hpp>
#include <yams/app/services/grep_mode_tls.h>
#include <yams/app/services/services.hpp>
#include <yams/common/utf8_utils.h>
#include <yams/core/cpp23_features.hpp>
#include <yams/core/magic_numbers.hpp>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>
#include <yams/search/search_engine_builder.h>
#include <yams/vector/vector_index_manager.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <mutex>
#include <thread>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <map>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

namespace yams::app::services {

namespace {

static constexpr std::string_view kRegexSpecialChars = "\\^$.|?*+()[]{}";

#if __cpp_lib_string_contains >= 202011L
template <typename StringType, typename SubType>
constexpr bool string_contains(const StringType& str, const SubType& substr) noexcept {
    return str.contains(substr);
}
#else
template <typename StringType, typename SubType>
constexpr bool string_contains(const StringType& str, const SubType& substr) noexcept {
    return str.find(substr) != StringType::npos;
}
#endif

static std::string escapeRegex(std::string_view text) {
    std::string escaped;
    escaped.reserve(text.size() * 2);
    for (char c : text) {
        if (kRegexSpecialChars.find(c) != std::string_view::npos) {
            escaped += '\\';
        }
        escaped += c;
    }
    return escaped;
}

struct PathTreeConfigSettings {
    bool enabled{false};
    std::string mode{"fallback"};
};

static constexpr auto toLower = [](unsigned char c) constexpr noexcept {
    return static_cast<char>(std::tolower(c));
};

static std::string toLowerCopy(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), toLower);
    return value;
}

static std::filesystem::path resolveConfigPath() {
    if (const char* explicitPath = std::getenv("YAMS_CONFIG_PATH")) {
        std::filesystem::path p{explicitPath};
        if (std::filesystem::exists(p))
            return p;
    }
    if (const char* xdg = std::getenv("XDG_CONFIG_HOME")) {
        std::filesystem::path p = std::filesystem::path(xdg) / "yams" / "config.toml";
        if (std::filesystem::exists(p))
            return p;
    }
    if (const char* home = std::getenv("HOME")) {
        std::filesystem::path p = std::filesystem::path(home) / ".config" / "yams" / "config.toml";
        if (std::filesystem::exists(p))
            return p;
    }
    return {};
}

static std::map<std::string, std::string> parseConfigForKeys(const std::filesystem::path& path) {
    std::map<std::string, std::string> out;
    std::ifstream file(path);
    if (!file)
        return out;

    std::string line;
    std::string currentSection;

    auto trim = [](std::string s) {
        auto issp = [](unsigned char c) { return std::isspace(c) != 0; };
        while (!s.empty() && issp(static_cast<unsigned char>(s.front())))
            s.erase(s.begin());
        while (!s.empty() && issp(static_cast<unsigned char>(s.back())))
            s.pop_back();
        return s;
    };

    while (std::getline(file, line)) {
        auto comment = line.find('#');
        if (comment != std::string::npos)
            line = line.substr(0, comment);
        line = trim(line);
        if (line.empty())
            continue;

        if (line.front() == '[' && line.back() == ']') {
            currentSection = line.substr(1, line.size() - 2);
            continue;
        }

        auto eq = line.find('=');
        if (eq == std::string::npos)
            continue;
        std::string key = trim(line.substr(0, eq));
        std::string value = trim(line.substr(eq + 1));
        if (!value.empty() && value.front() == '"' && value.back() == '"') {
            value = value.substr(1, value.size() - 2);
        }
        if (!currentSection.empty()) {
            out[currentSection + "." + key] = value;
        } else {
            out[key] = value;
        }
    }
    return out;
}

static PathTreeConfigSettings loadPathTreeConfigSettings() {
    PathTreeConfigSettings cfg;
    if (auto cfgPath = resolveConfigPath(); !cfgPath.empty()) {
        auto values = parseConfigForKeys(cfgPath);
        if (auto it = values.find("search.path_tree.enable"); it != values.end()) {
            auto v = toLowerCopy(it->second);
            cfg.enabled = (v == "1" || v == "true" || v == "yes" || v == "on");
        }
        if (auto it = values.find("search.path_tree.mode"); it != values.end()) {
            auto v = toLowerCopy(it->second);
            if (v == "preferred" || v == "fallback")
                cfg.mode = v;
        }
    }

    if (const char* envEnable = std::getenv("YAMS_GREP_PATH_TREE")) {
        auto v = toLowerCopy(envEnable);
        if (v == "0" || v == "false" || v == "off" || v == "no") {
            cfg.enabled = false;
        } else {
            cfg.enabled = true;
            if (v == "preferred" || v == "fallback")
                cfg.mode = v;
        }
    }
    if (const char* envMode = std::getenv("YAMS_GREP_PATH_TREE_MODE")) {
        auto v = toLowerCopy(envMode);
        if (v == "preferred" || v == "fallback")
            cfg.mode = v;
    }
    return cfg;
}

static constexpr bool hasWildcard(std::string_view s) noexcept {
    return s.find('*') != std::string_view::npos || s.find('?') != std::string_view::npos;
}

static std::string normalizePathForCompare(const std::string& path) {
    if (path.empty())
        return {};
    std::filesystem::path fs(path);
    std::error_code ec;
    auto normalized = fs.lexically_normal();
    std::string out = normalized.generic_string();
    if (out.empty()) {
        out = fs.generic_string();
    }
#if defined(__APPLE__)
    // Canonicalize common macOS path aliases so comparisons/globs are consistent
    auto canonApple = [](const std::string& s) -> std::string {
        if (s.rfind("/private/var/", 0) == 0 || s == "/private/var")
            return s; // already canonical
        if (s.rfind("/private/tmp/", 0) == 0 || s == "/private/tmp")
            return s; // already canonical
        if (s.rfind("/var/", 0) == 0)
            return std::string("/private") + s; // "/var/..." -> "/private/var/..."
        if (s == "/var")
            return std::string("/private/var");
        if (s.rfind("/tmp/", 0) == 0)
            return std::string("/private") + s; // "/tmp/..." -> "/private/tmp/..."
        if (s == "/tmp")
            return std::string("/private/tmp");
        return s;
    };
    out = canonApple(out);
#endif
#if defined(_WIN32) || defined(__APPLE__)
    std::transform(out.begin(), out.end(), out.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
#endif
    return out;
}

static std::string normalizeForGlobMatch(const std::string& value) {
    std::string out = value;
#if defined(_WIN32)
    std::replace(out.begin(), out.end(), '\\', '/');
#endif
#if defined(__APPLE__)
    // Canonicalize macOS path aliases for consistent glob matching
    auto canonApple = [](const std::string& s) -> std::string {
        if (s.rfind("/private/var/", 0) == 0 || s == "/private/var")
            return s;
        if (s.rfind("/private/tmp/", 0) == 0 || s == "/private/tmp")
            return s;
        if (s.rfind("/var/", 0) == 0)
            return std::string("/private") + s;
        if (s == "/var")
            return std::string("/private/var");
        if (s.rfind("/tmp/", 0) == 0)
            return std::string("/private") + s;
        if (s == "/tmp")
            return std::string("/private/tmp");
        return s;
    };
    out = canonApple(out);
#endif
#if defined(_WIN32) || defined(__APPLE__)
    std::transform(out.begin(), out.end(), out.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
#endif
    return out;
}

static bool isTransientMetadataError(const Error& err) noexcept {
    switch (err.code) {
        case ErrorCode::NotInitialized:
        case ErrorCode::DatabaseError:
        case ErrorCode::ResourceBusy:
        case ErrorCode::OperationInProgress:
        case ErrorCode::Timeout:
            return true;
        case ErrorCode::InternalError: {
            const auto& msg = err.message;
            return string_contains(msg, "database is locked") || string_contains(msg, "readonly") ||
                   string_contains(msg, "busy");
        }
        default:
            return false;
    }
}

struct MetadataTelemetry {
    std::atomic<std::uint64_t> operations{0};
    std::atomic<std::uint64_t> retries{0};
    std::atomic<std::uint64_t> transientFailures{0};
};

template <typename Fn>
auto retryMetadataOp(Fn&& fn, std::size_t maxAttempts = 4,
                     std::chrono::milliseconds initialDelay = std::chrono::milliseconds(25),
                     MetadataTelemetry* telemetry = nullptr) -> decltype(fn()) {
    if (telemetry)
        telemetry->operations.fetch_add(1, std::memory_order_relaxed);
    auto attempt = fn();
    if (attempt)
        return attempt;

    auto delay = initialDelay;
    bool transient = isTransientMetadataError(attempt.error());
    if (telemetry && transient)
        telemetry->transientFailures.fetch_add(1, std::memory_order_relaxed);

    for (std::size_t i = 1; i < maxAttempts && transient; ++i) {
        if (telemetry)
            telemetry->retries.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::sleep_for(delay);
        delay = std::min(delay * 2, std::chrono::milliseconds(250));
        attempt = fn();
        if (attempt)
            return attempt;
        transient = isTransientMetadataError(attempt.error());
        if (telemetry && transient)
            telemetry->transientFailures.fetch_add(1, std::memory_order_relaxed);
    }
    return attempt;
}

static bool pathFilterMatch(const std::string& filePath, const std::vector<std::string>& filters) {
    if (filters.empty())
        return true;
    const std::string normalizedDoc = normalizePathForCompare(filePath);
    const std::string globDoc = normalizeForGlobMatch(filePath);
    for (const auto& f : filters) {
        if (f.empty())
            continue;
        if (hasWildcard(f)) {
            // First try a straight glob match (supports '*' and '?')
            if (yams::app::services::utils::matchGlob(globDoc, normalizeForGlobMatch(f)))
                return true;
            // Extra robustness: support directory-style patterns like "/path/**" by
            // treating the portion before "**" as a normalized directory prefix.
            // This covers cases where a double-star may not be interpreted as intended by
            // the minimal glob matcher.
            auto dd = f.find("**");
            if (dd != std::string::npos) {
                std::string prefix = f.substr(0, dd);
                auto normPrefix = normalizePathForCompare(prefix);
                if (!normPrefix.empty()) {
                    if (!normPrefix.empty() && normPrefix.back() != '/')
                        normPrefix.push_back('/');
                    if (normalizedDoc.size() >= normPrefix.size() &&
                        normalizedDoc.compare(0, normPrefix.size(), normPrefix) == 0)
                        return true;
                }
            }
        } else {
            auto normalizedFilter = normalizePathForCompare(f);
            if (normalizedFilter.empty())
                continue;
            if (string_contains(normalizedDoc, normalizedFilter))
                return true;
        }
    }
    return false;
}

// Check if pattern is likely to work well with FTS5 tokenization
// FTS5 works best with natural language words, not regex patterns or special chars
static bool isLikelyFtsPattern(const std::string& pattern) {
    if (pattern.empty() || pattern.length() < 3) {
        return false; // Too short for FTS5 to be useful
    }

    // Check for regex metacharacters that indicate this is a regex pattern
    const std::string regexChars = "[](){}*+?|^$\\.";
    for (char c : pattern) {
        if (regexChars.find(c) != std::string::npos) {
            return false; // Contains regex special chars, use regex scan instead
        }
    }

    // Check if pattern contains only word characters, spaces, and basic punctuation
    // This heuristic identifies natural language queries that FTS5 handles well
    for (char c : pattern) {
        if (!std::isalnum(static_cast<unsigned char>(c)) && c != ' ' && c != '_' && c != '-') {
            return false;
        }
    }

    return true; // Looks like a good candidate for FTS5
}

} // namespace

class GrepServiceImpl final : public IGrepService {
public:
    explicit GrepServiceImpl(const AppContext& ctx) : ctx_(ctx) {
        auto cfg = loadPathTreeConfigSettings();
        pathTreeEnabled_ = cfg.enabled;
        pathTreeMode_ = cfg.mode;
        pathTreePreferred_ = pathTreeEnabled_ && pathTreeMode_ == "preferred";
        spdlog::debug("[GrepService] path-tree config: enabled={} mode={}", pathTreeEnabled_,
                      pathTreeMode_);
    }

    Result<GrepResponse> grep(const GrepRequest& req) override {
        auto grep_start_time = std::chrono::steady_clock::now();
        spdlog::debug("[GrepTrace] GrepServiceImpl::grep started.");
        if (!ctx_.metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }
        if (!ctx_.store) {
            return Error{ErrorCode::NotInitialized, "Content store not available"};
        }
        if (req.pattern.empty()) {
            return Error{ErrorCode::InvalidArgument, "Pattern is required"};
        }

        // Prepare pattern variants
        const std::string rawPattern = req.pattern;
        std::string regexPattern = req.pattern;
        if (req.literalText) {
            regexPattern = escapeRegex(regexPattern);
        }

        // Extract literals for two-phase matching (ripgrep strategy)
        std::unique_ptr<BMHSearcher> bmhSearcher;
        LiteralExtractor::ExtractionResult literalExtraction;
        
        if (!req.literalText) {
            literalExtraction = LiteralExtractor::extract(req.pattern, req.ignoreCase);
            if (literalExtraction.longestLength >= 3) {
                bmhSearcher = std::make_unique<BMHSearcher>(
                    literalExtraction.longest(), req.ignoreCase);
                spdlog::debug("[GrepService] Using BMH pre-filter with literal: '{}'",
                              literalExtraction.longest());
            }
        } else if (rawPattern.size() >= 3 && !req.ignoreCase) {
            bmhSearcher = std::make_unique<BMHSearcher>(rawPattern, false);
            spdlog::debug("[GrepService] Using BMH for literal pattern: '{}'", rawPattern);
        }

        std::regex_constants::syntax_option_type flags = std::regex::ECMAScript;
        if (req.ignoreCase)
            flags |= std::regex::icase;

        std::regex re;
        try {
            re = std::regex(regexPattern, flags);
        } catch (const std::regex_error& e) {
            return Error{ErrorCode::InvalidArgument, std::string("Invalid regex: ") + e.what()};
        }

        // Resolve context windows
        int beforeContext = req.context > 0 ? req.context : req.beforeContext;
        int afterContext = req.context > 0 ? req.context : req.afterContext;
        if (beforeContext < 0)
            beforeContext = 0;
        if (afterContext < 0)
            afterContext = 0;

        // Stage caps and timeouts (env-overridable)
        auto getenv_int = [](const char* k, int def) -> int {
            if (const char* v = std::getenv(k)) {
                try {
                    return std::max(0, std::stoi(v));
                } catch (...) {
                }
            }
            return def;
        };
        // PERFORMANCE: Lower default limits to prevent timeouts on large repos
        // Users can override with environment variables if needed
        const int max_docs_hot = getenv_int("YAMS_GREP_MAX_DOCS_HOT", 500);
        const int max_docs_cold = getenv_int("YAMS_GREP_MAX_DOCS_COLD", 50);
        const int budget_ms = getenv_int("YAMS_GREP_TIME_BUDGET_MS", 10000);
        const int max_total_results = getenv_int("YAMS_GREP_MAX_RESULTS", 1000);
        MetadataTelemetry metadataTelemetry;
        auto start_time = std::chrono::steady_clock::now();

        // --- Candidate Document Discovery ---
        std::vector<metadata::DocumentInfo> docs;
        std::unordered_set<int64_t> seenDocIds;

        auto addDocs = [&](std::vector<metadata::DocumentInfo>&& newDocs) {
            for (auto& d : newDocs) {
                if (seenDocIds.insert(d.id).second) {
                    docs.push_back(std::move(d));
                }
            }
        };

        bool usedFtsForInitialCandidates = false;

        if (!req.tags.empty()) {
            auto tRes = retryMetadataOp(
                [&]() {
                    return ctx_.metadataRepo->findDocumentsByTags(req.tags, req.matchAllTags);
                },
                4, std::chrono::milliseconds(25), &metadataTelemetry);
            if (tRes) {
                addDocs(std::move(tRes.value()));
            }
        } else if (!req.pattern.empty() && isLikelyFtsPattern(req.pattern)) {
            // Try FTS5 first for patterns that are likely to work well with FTS tokenization
            auto sRes = retryMetadataOp(
                [&]() { return ctx_.metadataRepo->search(req.pattern, max_docs_hot * 2); }, 4,
                std::chrono::milliseconds(25), &metadataTelemetry);
            if (sRes && sRes.value().isSuccess() && !sRes.value().results.empty()) {
                std::vector<metadata::DocumentInfo> ftsHits;
                ftsHits.reserve(sRes.value().results.size());
                for (const auto& r : sRes.value().results) {
                    ftsHits.push_back(r.document);
                }
                addDocs(std::move(ftsHits));
                usedFtsForInitialCandidates = true;
            } else {
                // FTS5 failed or empty - fall back to document scan
                if (!req.includePatterns.empty()) {
                    auto patternDocsRes = retryMetadataOp(
                        [&]() {
                            return metadata::queryDocumentsByGlobPatterns(*ctx_.metadataRepo,
                                                                          req.includePatterns, 0);
                        },
                        4, std::chrono::milliseconds(25), &metadataTelemetry);
                    if (patternDocsRes) {
                        addDocs(std::move(patternDocsRes.value()));
                    }
                } else {
                    // Safe FTS-like pattern with no path filter - do full scan as fallback
                    auto allDocsRes = retryMetadataOp(
                        [&]() {
                            return metadata::queryDocumentsByPattern(*ctx_.metadataRepo, "%");
                        },
                        4, std::chrono::milliseconds(25), &metadataTelemetry);
                    if (allDocsRes) {
                        addDocs(std::move(allDocsRes.value()));
                    }
                }
            }
        } else {
            // Pattern doesn't look like FTS5 will work - require includePatterns for scoping
            if (!req.includePatterns.empty()) {
                auto patternDocsRes = retryMetadataOp(
                    [&]() {
                        return metadata::queryDocumentsByGlobPatterns(*ctx_.metadataRepo,
                                                                      req.includePatterns, 0);
                    },
                    4, std::chrono::milliseconds(25), &metadataTelemetry);
                if (patternDocsRes) {
                    addDocs(std::move(patternDocsRes.value()));
                    spdlog::debug(
                        "[GrepService] Filtered at SQL level by {} include patterns: {} docs",
                        req.includePatterns.size(), docs.size());
                }
            } else {
                // No filters at all - scan all documents
                auto allDocsRes = retryMetadataOp(
                    [&]() { return metadata::queryDocumentsByPattern(*ctx_.metadataRepo, "%"); }, 4,
                    std::chrono::milliseconds(25), &metadataTelemetry);
                if (allDocsRes) {
                    addDocs(std::move(allDocsRes.value()));
                }
            }
        }

        // Stage 2: Further FTS filtering if not already used for initial candidates
        // PERFORMANCE: Skip redundant FTS search if we already used it in Stage 1
        bool literalFtsFallback = false;

        if (req.literalText && !req.pattern.empty() && !docs.empty() &&
            !usedFtsForInitialCandidates) {
            auto sRes = retryMetadataOp(
                [&]() { return ctx_.metadataRepo->search(req.pattern, max_docs_hot); }, 4,
                std::chrono::milliseconds(25), &metadataTelemetry);

            if (sRes && sRes.value().isSuccess()) {
                if (!sRes.value().results.empty()) {
                    // Filter the current document list by FTS results.
                    std::unordered_set<int64_t> fts_doc_ids;
                    fts_doc_ids.reserve(sRes.value().results.size());
                    for (const auto& r : sRes.value().results) {
                        fts_doc_ids.insert(r.document.id);
                    }

                    std::vector<metadata::DocumentInfo> filtered_docs;
                    filtered_docs.reserve(docs.size());
                    for (const auto& doc : docs) {
                        if (fts_doc_ids.count(doc.id) > 0) {
                            filtered_docs.push_back(doc);
                        }
                    }
                    docs = std::move(filtered_docs);
                } else {
                    literalFtsFallback = true;
                }
            }
            // If FTS search fails, we proceed with the larger candidate list from Stage 1.
        }

        // If no candidates were found by any method, return empty-handed.
        if (docs.empty()) {
            spdlog::debug("[GrepService] No candidate documents found after initial discovery.");
        }

        if (!req.paths.empty() || !req.includePatterns.empty()) {
            std::vector<metadata::DocumentInfo> filtered;
            filtered.reserve(docs.size());

            for (auto& doc : docs) {
                // Check paths filter
                if (!pathFilterMatch(doc.filePath, req.paths))
                    continue;

                // Check include patterns
                if (!req.includePatterns.empty()) {
                    bool matches = false;
                    const std::string docGlobPath = normalizeForGlobMatch(doc.filePath);
                    for (const auto& pattern : req.includePatterns) {
                        if (hasWildcard(pattern)) {
                            if (yams::app::services::utils::matchGlob(
                                    docGlobPath, normalizeForGlobMatch(pattern))) {
                                matches = true;
                                break;
                            }
                        } else {
                            const auto normalizedPattern = normalizeForGlobMatch(pattern);
                            if (!normalizedPattern.empty() &&
                                string_contains(docGlobPath, normalizedPattern)) {
                                matches = true;
                                break;
                            }
                        }
                    }
                    if (!matches)
                        continue;
                }

                filtered.push_back(std::move(doc));
            }

            docs = std::move(filtered);
            spdlog::debug("[GrepService] After path/include filtering: {} documents remain",
                          docs.size());
        }

        if (docs.empty() && pathTreeEnabled_ && !req.paths.empty()) {
            collectDocsFromPathTree(req, addDocs, metadataTelemetry);
            if (!docs.empty()) {
                spdlog::debug("[GrepService] Path-tree traversal supplied {} candidate docs",
                              docs.size());
            }
        }

        // Prefer hot docs (extracted/text) then cold; cap both sets
        std::vector<metadata::DocumentInfo> hotDocs;
        std::vector<metadata::DocumentInfo> coldDocs;
        hotDocs.reserve(docs.size());
        coldDocs.reserve(docs.size());
        
        // Parallel candidate filtering with magic_numbers.hpp for binary detection
        const size_t MAX_FILE_SIZE = 100 * 1024 * 1024;
        size_t filesSkippedType = 0;
        size_t filesSkippedSize = 0;
        
        auto shouldSkipFile = [](const metadata::DocumentInfo& doc) -> bool {
            std::string ext;
            auto dotPos = doc.filePath.rfind('.');
            if (dotPos != std::string::npos && dotPos < doc.filePath.size() - 1) {
                ext = doc.filePath.substr(dotPos + 1);
            }
            
            auto category = yams::magic::getPruneCategory(doc.filePath, ext);
            if (category == yams::magic::PruneCategory::BuildObject ||
                category == yams::magic::PruneCategory::BuildLibrary ||
                category == yams::magic::PruneCategory::BuildExecutable ||
                category == yams::magic::PruneCategory::BuildArchive ||
                category == yams::magic::PruneCategory::Packages) {
                return true;
            }
            
            if (doc.mimeType.find("image/") == 0 ||
                doc.mimeType.find("video/") == 0 ||
                doc.mimeType.find("audio/") == 0 ||
                doc.mimeType == "application/pdf" ||
                doc.mimeType == "application/zip" ||
                doc.mimeType == "application/x-tar" ||
                doc.mimeType == "application/x-gzip" ||
                doc.mimeType == "application/x-bzip2" ||
                doc.mimeType == "application/octet-stream") {
                return true;
            }
            
            return false;
        };
        
        // Parallel filtering using C++20 async for large candidate sets
        // For large candidate sets (>100 files), use parallel filtering
        if (docs.size() > 100) {
            std::vector<std::future<std::pair<std::vector<metadata::DocumentInfo>,
                                               std::vector<metadata::DocumentInfo>>>> futures;
            
            // Split candidates into chunks for parallel processing
            const size_t chunkSize = std::max<size_t>(10, docs.size() / std::thread::hardware_concurrency());
            for (size_t i = 0; i < docs.size(); i += chunkSize) {
                size_t end = std::min(i + chunkSize, docs.size());
                
                futures.push_back(std::async(std::launch::async, [&, i, end]() {
                    std::vector<metadata::DocumentInfo> hot, cold;
                    for (size_t j = i; j < end; ++j) {
                        auto& d = docs[j];
                        
                        // Skip binary/unsuitable files
                        if (shouldSkipFile(d)) {
                            continue;
                        }
                        
                        // Skip oversized files
                        if (d.fileSize > MAX_FILE_SIZE) {
                            continue;
                        }
                        
                        // Classify as hot or cold
                        bool isText = !d.mimeType.empty() && d.mimeType.rfind("text/", 0) == 0;
                        if (d.contentExtracted || isText) {
                            hot.push_back(std::move(d));
                        } else {
                            cold.push_back(std::move(d));
                        }
                    }
                    return std::make_pair(std::move(hot), std::move(cold));
                }));
            }
            
            // Collect results from parallel filters
            for (auto& fut : futures) {
                auto [hot, cold] = fut.get();
                hotDocs.insert(hotDocs.end(), std::make_move_iterator(hot.begin()),
                               std::make_move_iterator(hot.end()));
                coldDocs.insert(coldDocs.end(), std::make_move_iterator(cold.begin()),
                                std::make_move_iterator(cold.end()));
            }
            
            filesSkippedType = docs.size() - (hotDocs.size() + coldDocs.size());
            spdlog::debug("[GrepService] Parallel filtering (magic_numbers): {} docs -> {} hot + {} cold ({} skipped)",
                          docs.size(), hotDocs.size(), coldDocs.size(), filesSkippedType);
        } else {
            // Sequential filtering for small candidate sets
            for (auto& d : docs) {
                // Skip binary/unsuitable files
                if (shouldSkipFile(d)) {
                    filesSkippedType++;
                    continue;
                }
                
                // Skip oversized files
                if (d.fileSize > MAX_FILE_SIZE) {
                    filesSkippedSize++;
                    continue;
                }
                
                bool isText = !d.mimeType.empty() && d.mimeType.rfind("text/", 0) == 0;
                if (d.contentExtracted || isText) {
                    hotDocs.push_back(std::move(d));
                } else {
                    coldDocs.push_back(std::move(d));
                }
            }
        }
        
        if (filesSkippedType > 0 || filesSkippedSize > 0) {
            spdlog::debug("[GrepService] Skipped {} unsuitable files ({} binary/artifacts, {} oversized)",
                          filesSkippedType + filesSkippedSize, filesSkippedType, filesSkippedSize);
        }
        
        if (max_docs_hot >= 0 && hotDocs.size() > static_cast<size_t>(max_docs_hot))
            hotDocs.resize(static_cast<size_t>(max_docs_hot));
        if (max_docs_cold >= 0 && coldDocs.size() > static_cast<size_t>(max_docs_cold))
            coldDocs.resize(static_cast<size_t>(max_docs_cold));

        docs.clear();
        docs.insert(docs.end(), hotDocs.begin(), hotDocs.end());
        docs.insert(docs.end(), coldDocs.begin(), coldDocs.end());

        // Stage 3: Batch-fetch metadata for all candidates (CRITICAL PERFORMANCE FIX)
        // Use existing getMetadataForDocuments() to replace per-document getAllMetadata() calls
        // in workers (8K-80K queries → 1 query)
        std::unordered_map<int64_t, std::unordered_map<std::string, metadata::MetadataValue>>
            docMetadata;
        if (!docs.empty() && ctx_.metadataRepo) {
            std::vector<int64_t> docIds;
            docIds.reserve(docs.size());
            for (const auto& d : docs) {
                docIds.push_back(d.id);
            }
            auto batchResult = retryMetadataOp(
                [&]() { return ctx_.metadataRepo->getMetadataForDocuments(docIds); }, 4,
                std::chrono::milliseconds(25), &metadataTelemetry);
            if (batchResult) {
                docMetadata = std::move(batchResult.value());
                spdlog::debug("[GrepService] Batch-fetched metadata for {} documents",
                              docMetadata.size());
            } else {
                spdlog::warn("[GrepService] Failed to batch-fetch metadata: code={} msg={}",
                             static_cast<int>(batchResult.error().code),
                             batchResult.error().message);
                if (!req.tags.empty()) {
                    return batchResult.error();
                }
            }
        }

        GrepResponse response;
        if (literalFtsFallback) {
            response.searchStats["fts_literal_fallback"] = "true";
        }
        response.totalMatches = 0;
        response.regexMatches = 0;
        response.semanticMatches = 0;

        response.filesSearched = docs.size();

        // Mode selection for tiered grep
        enum class Mode { Auto, HotOnly, ColdOnly };
        Mode mode = Mode::Auto;

        // POLICY: Hot path (extracted text) is ONLY used within session context.
        // For general queries, always use cold path (CAS) for comprehensive results.
        if (!req.useSession) {
            mode = Mode::ColdOnly;
            spdlog::debug("[GrepService] Non-session query → forcing ColdOnly mode (CAS scan)");
        } else {
            // Session-scoped query: use Auto mode (allows hot path for warmed docs)
            mode = Mode::Auto;
            spdlog::debug(
                "[GrepService] Session-scoped query → using Auto mode (hot path enabled)");
        }

        // Dynamic, bounded parallelism (config-free): cap to a conservative small number
        size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
        size_t rec = hw > 1 ? (hw - 1) : 1;
#if !defined(_WIN32) && !defined(__ANDROID__)
        double loads[3] = {0, 0, 0};
        if (getloadavg(loads, 3) == 3) {
            double load1 = loads[0];
            double capacity = std::max(0.0, static_cast<double>(hw) - load1);
            rec = static_cast<size_t>(std::clamp(capacity, 1.0, static_cast<double>(hw)));
        }
#endif
        // Keep grep background-friendly by default: at most 4 workers (or fewer if docs < 4)
        size_t workers = std::min<size_t>({rec, hw, static_cast<size_t>(4)});
        workers = std::min(workers, docs.size() > 0 ? docs.size() : size_t{1});

        spdlog::debug("[GrepService] starting worker scan: docs={} tags={} paths={} includes={}",
                      docs.size(), req.tags.size(), req.paths.size(), req.includePatterns.size());
        auto before_workers_time = std::chrono::steady_clock::now();
        std::atomic<size_t> next{0};
        std::mutex outMutex;
        std::vector<GrepFileResult> outResults;
        outResults.reserve(docs.size());
        std::vector<std::string> filesWith;
        std::vector<std::string> filesWithout;
        std::atomic<size_t> totalMatches{0};
        std::atomic<size_t> regexMatches{0};
        std::atomic<bool> stop{false};
        std::mutex errorMutex;
        std::vector<Error> workerErrors;
        workerErrors.reserve(workers);

        auto worker = [&]() {
            while (true) {
                if (stop.load(std::memory_order_relaxed))
                    break;
                if (budget_ms > 0) {
                    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_time);
                    if (elapsed.count() >= budget_ms) {
                        stop.store(true, std::memory_order_relaxed);
                        break;
                    }
                }
                size_t i = next.fetch_add(1);
                if (i >= docs.size())
                    break;
                const auto& doc = docs[i];

                if (!pathFilterMatch(doc.filePath, req.paths))
                    continue;
                if (!req.includePatterns.empty()) {
                    bool ok = false;
                    const std::string docGlobPath = normalizeForGlobMatch(doc.filePath);
                    for (const auto& pattern : req.includePatterns) {
                        if (hasWildcard(pattern)) {
                            if (yams::app::services::utils::matchGlob(
                                    docGlobPath, normalizeForGlobMatch(pattern))) {
                                ok = true;
                                break;
                            }
                            // Extra: treat "/dir/**" as a directory prefix include
                            auto dd = pattern.find("**");
                            if (!ok && dd != std::string::npos) {
                                std::string prefix = pattern.substr(0, dd);
                                auto normPrefix = normalizePathForCompare(prefix);
                                if (!normPrefix.empty()) {
                                    if (normPrefix.back() != '/')
                                        normPrefix.push_back('/');
                                    auto normDoc = normalizePathForCompare(doc.filePath);
                                    if (normDoc.size() >= normPrefix.size() &&
                                        normDoc.compare(0, normPrefix.size(), normPrefix) == 0) {
                                        ok = true;
                                        break;
                                    }
                                }
                            }
                        } else {
                            const auto normalizedPattern = normalizeForGlobMatch(pattern);
                            if (!normalizedPattern.empty() &&
                                string_contains(docGlobPath, normalizedPattern)) {
                                ok = true;
                                break;
                            }
                        }
                    }
                    if (!ok)
                        continue;
                }

                bool forceCold = false;
                auto metadataIt = docMetadata.find(doc.id);
                if (metadataIt != docMetadata.end()) {
                    const auto& metadataSnapshot = metadataIt->second;
                    auto it = metadataSnapshot.find("force_cold");
                    if (it != metadataSnapshot.end()) {
                        auto v = it->second.asString();
                        std::string lv = v;
                        std::transform(lv.begin(), lv.end(), lv.begin(), ::tolower);
                        forceCold = (lv == "1" || lv == "true" || lv == "yes");
                    }
                    if (!forceCold &&
                        metadataSnapshot.find("tag:force_cold") != metadataSnapshot.end()) {
                        forceCold = true;
                    }
                }

                auto isWordCharExtended = [](char c) -> bool {
                    // Treat '-' as part of a word to avoid counting 'foo' in 'foo-bar'
                    return std::isalnum(static_cast<unsigned char>(c)) || c == '-';
                };
                auto boundaryOk = [&](const std::string& line, size_t pos, size_t len) -> bool {
                    if (!req.word)
                        return true;
                    bool beforeOk = (pos == 0) || !isWordCharExtended(line[pos - 1]);
                    bool afterOk =
                        (pos + len >= line.size()) || !isWordCharExtended(line[pos + len]);
                    return beforeOk && afterOk;
                };
                auto countMatches = [&](const std::string& line) -> size_t {
                    size_t count = 0;
                    
                    // Two-phase matching: BMH literal pre-filter → regex validation
                    if (bmhSearcher && !req.literalText) {
                        if (bmhSearcher->find(line) == std::string::npos) {
                            return 0;
                        }
                    }
                    
                    if (req.literalText && bmhSearcher) {
                        size_t from = 0;
                        while (true) {
                            auto pos = bmhSearcher->find(line, from);
                            if (pos == std::string::npos)
                                break;
                            if (boundaryOk(line, pos, rawPattern.size()))
                                ++count;
                            from = pos + 1;
                        }
                        return count;
                    }
                    
                    // Fast path fallback: pure literal < 3 chars, use std::string::find
                    if (req.literalText && !req.ignoreCase) {
                        size_t from = 0;
                        while (true) {
                            auto pos = line.find(rawPattern, from);
                            if (pos == std::string::npos)
                                break;
                            if (boundaryOk(line, pos, rawPattern.size()))
                                ++count;
                            from = pos + 1;
                        }
                        return count;
                    }
                    
                    // Regex path: full pattern matching (after literal pre-filter if applicable)
                    std::cmatch cm;
                    const char* start = line.c_str();
                    const char* end = start + line.size();
                    while (std::regex_search(start, end, cm, re)) {
                        auto pos = static_cast<size_t>(cm.position(0) + (start - line.c_str()));
                        auto len = static_cast<size_t>(cm.length(0));
                        if (boundaryOk(line, pos, len))
                            ++count;
                        start = cm.suffix().first;
                    }
                    return count;
                };

                GrepFileResult fileResult;
                fileResult.file = doc.filePath;
                fileResult.fileName = std::filesystem::path(doc.filePath).filename().string();
                fileResult.matchCount = 0;
                size_t ln_counter = 0;
                auto onLine = [&](const std::string& line) {
                    ++ln_counter;
                    size_t n = countMatches(line);
                    bool matched = (n > 0);
                    if (req.invert)
                        matched = !matched;
                    if (!matched)
                        return;
                    fileResult.matchCount += n;
                    if (req.count)
                        return;
                    GrepMatch gm;
                    gm.matchType = req.literalText ? std::string("literal") : std::string("regex");
                    gm.confidence = 1.0;
                    if (req.lineNumbers)
                        gm.lineNumber = ln_counter;
                    gm.line = yams::common::sanitizeUtf8(line);
                    if (!req.invert && !req.literalText) {
                        std::smatch sm;
                        if (std::regex_search(line, sm, re)) {
                            gm.columnStart = static_cast<size_t>(sm.position()) + 1;
                            gm.columnEnd = gm.columnStart + static_cast<size_t>(sm.length());
                        }
                    } else if (!req.invert && req.literalText) {
                        auto pos = line.find(rawPattern);
                        if (pos != std::string::npos) {
                            gm.columnStart = pos + 1;
                            gm.columnEnd = gm.columnStart + rawPattern.size();
                        }
                    }
                    fileResult.matches.push_back(std::move(gm));
                };

                // Hot path: process extracted text line-by-line without touching CAS
                if (mode == Mode::HotOnly && !forceCold) {
                    if (ctx_.metadataRepo) {
                        auto c =
                            retryMetadataOp([&]() { return ctx_.metadataRepo->getContent(doc.id); },
                                            4, std::chrono::milliseconds(25), &metadataTelemetry);
                        if (c && c.value().has_value()) {
                            std::istringstream iss(c.value()->contentText);
                            std::string line;
                            while (std::getline(iss, line)) {
                                if (!line.empty() && line.back() == '\r')
                                    line.pop_back();
                                onLine(line);
                                if (req.maxCount > 0 &&
                                    static_cast<int>(fileResult.matchCount) >= req.maxCount)
                                    break;
                                if (budget_ms > 0) {
                                    auto e2 = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - start_time);
                                    if (e2.count() >= budget_ms) {
                                        stop.store(true, std::memory_order_relaxed);
                                        break;
                                    }
                                }
                            }
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                } else {
                    // Cold path: stream content and scan lines incrementally
                    struct LineScanBuf : public std::streambuf {
                        std::string buffer;
                        std::function<void(const std::string&)> cb;
                        explicit LineScanBuf(std::function<void(const std::string&)> f)
                            : cb(std::move(f)) {}
                        int overflow(int ch) override {
                            if (ch == traits_type::eof())
                                return 0;
                            char c = static_cast<char>(ch);
                            if (c == '\n') {
                                cb(buffer);
                                buffer.clear();
                            } else if (c != '\r') {
                                buffer.push_back(c);
                            }
                            return ch;
                        }
                        std::streamsize xsputn(const char* s, std::streamsize n) override {
                            // SIMD vectorized newline scanning
                            const char* p = s;
                            const char* end = s + n;
                            while (p < end) {
                                size_t remaining = static_cast<size_t>(end - p);
                                size_t nlOffset = SimdNewlineScanner::findNewline(p, remaining);
                                
                                if (nlOffset >= remaining) {
                                    // No newline found in remaining data
                                    if (memchr(p, '\r', remaining) != nullptr) {
                                        // handle CR occurrences by copying segments
                                        const char* q = p;
                                        while (q < end) {
                                            const char* r = static_cast<const char*>(
                                                memchr(q, '\r', static_cast<size_t>(end - q)));
                                            if (!r) {
                                                buffer.append(q, end);
                                                break;
                                            }
                                            buffer.append(q, r);
                                            q = r + 1; // skip CR
                                        }
                                    } else {
                                        buffer.append(p, end);
                                    }
                                    return n;
                                }
                                
                                const char* nlc = p + nlOffset;
                                // append up to newline, skipping CRs
                                if (memchr(p, '\r', static_cast<size_t>(nlc - p)) != nullptr) {
                                    const char* q = p;
                                    while (q < nlc) {
                                        const char* r = static_cast<const char*>(
                                            memchr(q, '\r', static_cast<size_t>(nlc - q)));
                                        if (!r) {
                                            buffer.append(q, nlc);
                                            break;
                                        }
                                        buffer.append(q, r);
                                        q = r + 1; // skip CR
                                    }
                                } else {
                                    buffer.append(p, nlc);
                                }
                                // deliver line
                                cb(buffer);
                                buffer.clear();
                                p = nlc + 1; // skip LF
                            }
                            return n;
                        }
                    };
                    LineScanBuf sb(onLine);
                    std::ostream os(&sb);
                    if (budget_ms > 0) {
                        auto e3 = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - start_time);
                        if (e3.count() >= budget_ms) {
                            stop.store(true, std::memory_order_relaxed);
                            break;
                        }
                    }
                    auto rs = ctx_.store->retrieveStream(doc.sha256Hash, os, nullptr);
                    if (!rs)
                        continue;
                    // Flush any remaining buffered content as a final line
                    if (!sb.buffer.empty()) {
                        std::string tail = std::move(sb.buffer);
                        sb.buffer.clear();
                        // Normalize potential Windows CRLF by stripping trailing \r
                        if (!tail.empty() && tail.back() == '\r')
                            tail.pop_back();
                        onLine(tail);
                    }
                }

                // Early exit shaping for files-only/paths-only
                if (req.filesWithMatches || req.pathsOnly || req.filesWithoutMatch) {
                    // Defer formatting to caller; we just track counts and file sets below
                }

                std::lock_guard<std::mutex> lk(outMutex);
                if (fileResult.matchCount > 0) {
                    spdlog::debug("[GrepService] matched '{}' count={}", fileResult.file,
                                  fileResult.matchCount);
                    totalMatches += static_cast<size_t>(fileResult.matchCount);
                    regexMatches += static_cast<size_t>(fileResult.matchCount);
                    filesWith.push_back(fileResult.file);
                    if (!req.filesWithMatches && !req.pathsOnly)
                        outResults.push_back(std::move(fileResult));

                    // PERFORMANCE: Stop early if we've hit the result limit
                    if (max_total_results > 0 &&
                        totalMatches >= static_cast<size_t>(max_total_results)) {
                        spdlog::info("[GrepService] Hit result limit ({} matches), stopping early",
                                     totalMatches.load());
                        stop.store(true, std::memory_order_relaxed);
                    }
                } else {
                    filesWithout.push_back(doc.filePath);
                }
            }
        };

        // Wrap lambda in std::function to avoid C++20 immediate function evaluation issues
        std::function<void()> workerFunc = worker;

        std::vector<std::thread> ths;
        ths.reserve(workers);
        for (size_t t = 0; t < workers; ++t)
            ths.emplace_back(workerFunc);
        for (auto& th : ths)
            th.join();

        auto workers_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now() - before_workers_time)
                                    .count();
        spdlog::debug("[GrepTrace] Regex matching across {} files took {}ms.",
                      response.filesSearched, workers_duration);

        {
            std::lock_guard<std::mutex> lk(errorMutex);
            if (!workerErrors.empty())
                return workerErrors.front();
        }

        response.results = std::move(outResults);
        response.filesWith = std::move(filesWith);
        response.filesWithout = std::move(filesWithout);
        if (req.pathsOnly) {
            response.pathsOnly = req.invert ? response.filesWithout : response.filesWith;
        }
        spdlog::debug(
            "[GrepService] filesWith={} filesWithout={} results={} pathsOnly={} totalMatches={}",
            response.filesWith.size(), response.filesWithout.size(), response.results.size(),
            response.pathsOnly.size(), response.totalMatches);
        response.totalMatches = totalMatches.load();
        response.regexMatches = regexMatches.load();
        response.queryInfo = "grep: parallel regex scan";
        response.searchStats["workers"] = std::to_string(workers);
        response.searchStats["files_scanned"] = std::to_string(response.filesSearched);

        // Perform semantic search unless disabled; allow even in count/files-only/paths-only modes
        if (!req.regexOnly && req.semanticLimit > 0) {
            try {
                auto semantic_start_time = std::chrono::steady_clock::now();
                size_t topk = static_cast<size_t>(std::max(1, req.semanticLimit)) * 3;

                // Use cached engine if available, build fresh if needed
                std::shared_ptr<yams::search::HybridSearchEngine> eng = ctx_.hybridEngine;
                if (!eng) {
                    auto build_start_time = std::chrono::steady_clock::now();
                    auto vecMgr = std::make_shared<yams::vector::VectorIndexManager>();
                    yams::search::SearchEngineBuilder builder;
                    builder.withVectorIndex(vecMgr).withMetadataRepo(ctx_.metadataRepo);
                    auto opts = yams::search::SearchEngineBuilder::BuildOptions::makeDefault();
                    opts.hybrid.final_top_k = topk;
                    auto engRes = builder.buildEmbedded(opts);
                    if (engRes) {
                        eng = engRes.value();
                    }
                    auto build_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                                              std::chrono::steady_clock::now() - build_start_time)
                                              .count();
                    spdlog::debug("[GrepTrace] Built fresh HybridSearchEngine for semantic "
                                  "search in {}ms.",
                                  build_duration);
                }

                if (eng) {
                    auto hres = eng->search(req.pattern, topk);
                    if (hres) {
                        std::set<std::string> regexFiles;
                        for (const auto& fr : response.results)
                            regexFiles.insert(fr.file);
                        std::vector<yams::search::HybridSearchResult> sem = hres.value();
                        int taken = 0;
                        for (const auto& r : sem) {
                            auto itPath = r.metadata.find("path");
                            if (itPath == r.metadata.end())
                                continue;
                            const std::string& path = itPath->second;
                            if (!pathFilterMatch(path, req.paths))
                                continue;
                            if (!req.includePatterns.empty()) {
                                bool ok = false;
                                const std::string pathGlob = normalizeForGlobMatch(path);
                                for (const auto& p : req.includePatterns) {
                                    if (hasWildcard(p)) {
                                        if (yams::app::services::utils::matchGlob(
                                                pathGlob, normalizeForGlobMatch(p))) {
                                            ok = true;
                                            break;
                                        }
                                        // Extra: support directory prefix semantics for "**"
                                        auto dd = p.find("**");
                                        if (!ok && dd != std::string::npos) {
                                            std::string prefix = p.substr(0, dd);
                                            auto normPrefix = normalizePathForCompare(prefix);
                                            if (!normPrefix.empty()) {
                                                if (normPrefix.back() != '/')
                                                    normPrefix.push_back('/');
                                                auto normDoc = normalizePathForCompare(path);
                                                if (normDoc.size() >= normPrefix.size() &&
                                                    normDoc.compare(0, normPrefix.size(),
                                                                    normPrefix) == 0) {
                                                    ok = true;
                                                    break;
                                                }
                                            }
                                        }
                                    } else {
                                        const auto normalized = normalizeForGlobMatch(p);
                                        if (!normalized.empty() &&
                                            string_contains(pathGlob, normalized)) {
                                            ok = true;
                                            break;
                                        }
                                    }
                                }
                                if (!ok)
                                    continue;
                            }
                            if (regexFiles.find(path) != regexFiles.end())
                                continue;
                            GrepFileResult fr;
                            fr.file = path;
                            fr.fileName = std::filesystem::path(path).filename().string();
                            GrepMatch gm;
                            gm.matchType = "semantic";
                            float conf = r.hybrid_score > 0.0f ? r.hybrid_score : r.vector_score;
                            if (conf < 0.0f)
                                conf = 0.0f;
                            if (conf > 1.0f)
                                conf = 1.0f;
                            gm.confidence = conf;
                            gm.lineNumber = 0;
                            if (!r.content.empty())
                                gm.line = yams::common::sanitizeUtf8(r.content);
                            fr.matches.push_back(std::move(gm));
                            fr.matchCount = 1;
                            fr.wasSemanticSearch = true;
                            response.results.push_back(std::move(fr));
                            response.semanticMatches += 1;
                            response.totalMatches += 0; // semantic items are not line hits
                            taken++;
                            if (taken >= req.semanticLimit)
                                break;
                        }
                    }
                }
                auto semantic_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                                             std::chrono::steady_clock::now() - semantic_start_time)
                                             .count();
                spdlog::debug("[GrepTrace] Semantic search took {}ms.", semantic_duration);
            } catch (...) {
            }
        }

        auto totalElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - start_time)
                                .count();
        response.executionTimeMs = static_cast<std::int64_t>(totalElapsed);
        response.searchStats["latency_ms"] = std::to_string(totalElapsed);
        response.searchStats["metadata_operations"] =
            std::to_string(metadataTelemetry.operations.load(std::memory_order_relaxed));
        response.searchStats["metadata_retries"] =
            std::to_string(metadataTelemetry.retries.load(std::memory_order_relaxed));
        response.searchStats["metadata_transient_failures"] =
            std::to_string(metadataTelemetry.transientFailures.load(std::memory_order_relaxed));
        if (budget_ms > 0) {
            response.searchStats["budget_ms"] = std::to_string(budget_ms);
            response.searchStats["timeout_triggered"] =
                stop.load(std::memory_order_relaxed) ? "1" : "0";
        }

        auto total_grep_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                                       std::chrono::steady_clock::now() - grep_start_time)
                                       .count();
        spdlog::debug("[GrepTrace] GrepServiceImpl::grep finished in {}ms.", total_grep_duration);

        return Result<GrepResponse>(std::move(response));
    }

private:
    AppContext ctx_;
    bool pathTreeEnabled_{false};
    std::string pathTreeMode_{"fallback"};
    bool pathTreePreferred_{false};

    template <typename AddFn>
    void collectDocsFromPathTree(const GrepRequest& req, AddFn&& addFn,
                                 MetadataTelemetry& telemetry) {
        if (!pathTreeEnabled_ || !ctx_.metadataRepo)
            return;

        constexpr std::size_t kRootFanout = 32;
        auto repo = ctx_.metadataRepo;

        auto canonicalize = [](std::string path) -> std::string {
            if (path.empty())
                return path;
            try {
                std::filesystem::path p(path);
                if (!p.is_absolute())
                    p = std::filesystem::path("/") / p;
                p = p.lexically_normal();
                std::string norm = p.generic_string();
                if (norm.empty())
                    norm = "/";
                if (norm.size() > 1 && norm.back() == '/')
                    norm.pop_back();
                return norm;
            } catch (...) {
                if (path.front() != '/')
                    path.insert(path.begin(), '/');
                if (path.size() > 1 && path.back() == '/')
                    path.pop_back();
                return path;
            }
        };

        auto fetchPrefix = [&](const std::string& prefix) {
            if (prefix.empty() || prefix == "/") {
                auto res =
                    retryMetadataOp([&]() { return metadata::queryDocumentsByPattern(*repo, "%"); },
                                    4, std::chrono::milliseconds(25), &telemetry);
                if (res)
                    addFn(std::move(res.value()));
                return;
            }
            metadata::DocumentQueryOptions opts;
            opts.pathPrefix = prefix;
            opts.prefixIsDirectory = true;
            opts.includeSubdirectories = true;
            opts.orderByNameAsc = true;
            auto res = retryMetadataOp([&]() { return repo->queryDocuments(opts); }, 4,
                                       std::chrono::milliseconds(25), &telemetry);
            if (res)
                addFn(std::move(res.value()));
        };

        std::vector<std::string> prefixes;
        for (const auto& raw : req.paths) {
            if (hasWildcard(raw))
                continue;
            auto norm = canonicalize(raw);
            if (!norm.empty())
                prefixes.push_back(norm);
        }

        if (!prefixes.empty()) {
            for (const auto& prefix : prefixes) {
                auto nodeRes = repo->findPathTreeNodeByFullPath(prefix);
                if (nodeRes && nodeRes.value()) {
                    fetchPrefix(prefix);
                } else if (pathTreePreferred_) {
                    fetchPrefix(prefix);
                }
            }
            return;
        }

        auto childrenRes = repo->listPathTreeChildren("/", kRootFanout);
        if (!childrenRes) {
            if (pathTreePreferred_)
                fetchPrefix("/");
            return;
        }
        const auto& children = childrenRes.value();
        if (children.empty()) {
            if (pathTreePreferred_)
                fetchPrefix("/");
            return;
        }
        for (const auto& child : children) {
            fetchPrefix(child.fullPath);
        }
    }
};

// Optional separate factory for direct construction (callers may wire this in a factory unit).
std::shared_ptr<IGrepService> makeGrepService(const AppContext& ctx) {
    return std::make_shared<GrepServiceImpl>(ctx);
}

} // namespace yams::app::services
