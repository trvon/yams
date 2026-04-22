#include "../../cli/hot_cold_utils.h"
#include <yams/app/services/grep_mode_tls.h>
#include <yams/app/services/grep_regex.hpp>
#include <yams/app/services/literal_extractor.hpp>
#include <yams/app/services/services.hpp>
#include <yams/app/services/simd_newline_scanner.hpp>
#include <yams/common/utf8_utils.h>
#include <yams/core/cpp23_features.hpp>
#include <yams/core/magic_numbers.hpp>
#include <yams/metadata/kg_relation_summary.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>
#include <yams/profiling.h>
#include <yams/search/query_router.h>
#include <yams/search/search_engine_builder.h>

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
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace yams::app::services {

namespace {

std::atomic<int> g_activeGrepRequests{0};

class ActiveGrepRequestGuard {
public:
    explicit ActiveGrepRequestGuard(std::atomic<int>& counter)
        : counter_(counter), active_(counter_.fetch_add(1, std::memory_order_acq_rel) + 1) {}

    ~ActiveGrepRequestGuard() { counter_.fetch_sub(1, std::memory_order_acq_rel); }

    int active() const { return active_; }

private:
    std::atomic<int>& counter_;
    int active_{1};
};

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

static auto toLower = [](unsigned char c) noexcept { return static_cast<char>(std::tolower(c)); };

static std::string toLowerCopy(std::string_view value) {
    std::string lowered(value);
    std::transform(lowered.begin(), lowered.end(), lowered.begin(), toLower);
    return lowered;
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
            line.resize(comment);
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

using GrepClock = std::chrono::steady_clock;

static void recordPhaseMetric(std::map<std::string, std::int64_t>& phaseTimings,
                              const char* statKey, const char* plotKey,
                              const GrepClock::time_point start) {
    (void)plotKey;
    const auto elapsedMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(GrepClock::now() - start).count();
    phaseTimings[statKey] = elapsedMs;
    YAMS_PLOT(plotKey, elapsedMs);
}

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

static std::size_t annotateResultRelations(std::vector<GrepFileResult>& results,
                                           metadata::KnowledgeGraphStore* kgStore,
                                           std::size_t maxFiles = 64) {
    if (!kgStore || results.empty()) {
        return 0;
    }

    const std::size_t count = std::min(results.size(), maxFiles);
    std::size_t enriched = 0;
    for (std::size_t i = 0; i < count; ++i) {
        auto summary = metadata::collectFileRelationSummary(kgStore, results[i].file);
        if (!summary.has_value()) {
            continue;
        }
        results[i].metadata["relation_count"] = std::to_string(summary->totalEdges);
        results[i].metadata["relation_types"] =
            metadata::formatRelationSummary(summary->topRelations, false /*humanReadable*/);
        results[i].metadata["relation_summary"] =
            metadata::formatRelationSummary(summary->topRelations, true /*humanReadable*/);
        if (!summary->topRelations.empty()) {
            results[i].metadata["primary_relation"] = summary->topRelations.front().first;
        }
        ++enriched;
    }
    return enriched;
}

} // namespace

class GrepServiceImpl final : public IGrepService {
public:
    explicit GrepServiceImpl(const AppContext& ctx) : ctx_(ctx) {
        auto cfg = loadPathTreeConfigSettings();
        pathTreeEnabled_ = cfg.enabled;
        pathTreeMode_ = std::move(cfg.mode);
        pathTreePreferred_ = pathTreeEnabled_ && pathTreeMode_ == "preferred";
        spdlog::debug("[GrepService] path-tree config: enabled={} mode={}", pathTreeEnabled_,
                      pathTreeMode_);
    }

    Result<GrepResponse> grep(const GrepRequest& req) override {
        YAMS_ZONE_SCOPED_N("grep_service::grep");
        ActiveGrepRequestGuard activeGuard(g_activeGrepRequests);
        auto grep_start_time = GrepClock::now();
        YAMS_PLOT("grep::active_requests", activeGuard.active());
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

        std::map<std::string, std::int64_t> phaseTimings;
        const search::QueryRouter queryRouter;
        const auto routeDecision = queryRouter.route(req.pattern);
        const auto retrievalMode = routeDecision.retrievalMode.label;

        const auto patternPrepStart = GrepClock::now();
        const std::string rawPattern = req.pattern;
        std::string regexPattern = req.pattern;
        std::unique_ptr<BMHSearcher> bmhSearcher;
        LiteralExtractor::ExtractionResult literalExtraction;
        {
            YAMS_ZONE_SCOPED_N("grep_service::pattern_prep");
            if (req.literalText) {
                regexPattern = escapeRegex(regexPattern);
            }

            // Extract literals for two-phase matching (ripgrep strategy)
            if (!req.literalText) {
                literalExtraction = LiteralExtractor::extract(req.pattern, req.ignoreCase);
                if (literalExtraction.longestLength >= 3) {
                    bmhSearcher =
                        std::make_unique<BMHSearcher>(literalExtraction.longest(), req.ignoreCase);
                    spdlog::debug("[GrepService] Using BMH pre-filter with literal: '{}'",
                                  literalExtraction.longest());
                }
            } else if (rawPattern.size() >= 3 && !req.ignoreCase) {
                bmhSearcher = std::make_unique<BMHSearcher>(rawPattern, false);
                spdlog::debug("[GrepService] Using BMH for literal pattern: '{}'", rawPattern);
            }
        }
        recordPhaseMetric(phaseTimings, "phase_pattern_prep_ms", "grep::phase::pattern_prep_ms",
                          patternPrepStart);

        std::optional<GrepRegex> grepRe;
        const auto regexCompileStart = GrepClock::now();
        {
            YAMS_ZONE_SCOPED_N("grep_service::regex_compile");
            grepRe = GrepRegex::compile(regexPattern, req.ignoreCase);
            if (!grepRe) {
                return Error{ErrorCode::InvalidArgument,
                             std::string("Invalid regex: ") + regexPattern};
            }
            if (!grepRe->fallbackReason().empty()) {
                spdlog::debug("[GrepService] RE2 fallback for '{}': {}", regexPattern,
                              grepRe->fallbackReason());
            }
        }
        recordPhaseMetric(phaseTimings, "phase_regex_compile_ms", "grep::phase::regex_compile_ms",
                          regexCompileStart);

        // Alias for the compiled regex (used by worker lambdas).
        const auto& re = *grepRe;

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
        int max_docs_hot = getenv_int("YAMS_GREP_MAX_DOCS_HOT", 500);
        int max_docs_cold = getenv_int("YAMS_GREP_MAX_DOCS_COLD", 50);
        int budget_ms = getenv_int("YAMS_GREP_TIME_BUDGET_MS", 10000);
        const int max_total_results = getenv_int("YAMS_GREP_MAX_RESULTS", 1000);

        if (!req.useSession) {
            max_docs_hot = std::min(max_docs_hot, 128);
            max_docs_cold = std::min(max_docs_cold, 20);
            budget_ms = std::min(budget_ms, 3000);
        }

        const int activeRequests = std::clamp(activeGuard.active(), 1, 4);
        if (activeRequests > 1) {
            max_docs_hot = std::max(64, max_docs_hot / activeRequests);
            max_docs_cold = std::max(16, max_docs_cold / activeRequests);
            budget_ms = std::max(2000, budget_ms / activeRequests);
        }
        MetadataTelemetry metadataTelemetry;
        auto start_time = GrepClock::now();

        // --- Candidate Document Discovery ---
        std::vector<metadata::GrepCandidateProjection> docs;
        std::unordered_set<int64_t> seenDocIds;

        auto addDocs = [&](std::vector<metadata::GrepCandidateProjection>&& newDocs) {
            for (auto& d : newDocs) {
                if (seenDocIds.insert(d.id).second) {
                    docs.push_back(std::move(d));
                }
            }
        };

        // Helper to convert DocumentInfo results (from FTS/tags) to GrepCandidateProjection
        auto convertToGrepCandidates = [](std::vector<metadata::DocumentInfo>&& infos)
            -> std::vector<metadata::GrepCandidateProjection> {
            std::vector<metadata::GrepCandidateProjection> out;
            out.reserve(infos.size());
            for (auto& d : infos) {
                metadata::GrepCandidateProjection p;
                p.id = d.id;
                p.filePath = std::move(d.filePath);
                p.fileSize = d.fileSize;
                p.sha256Hash = std::move(d.sha256Hash);
                p.mimeType = std::move(d.mimeType);
                p.contentExtracted = d.contentExtracted;
                out.push_back(std::move(p));
            }
            return out;
        };

        bool usedFtsForInitialCandidates = false;
        const auto candidateDiscoveryStart = GrepClock::now();
        {
            YAMS_ZONE_SCOPED_N("grep_service::candidate_discovery");
            if (!req.tags.empty()) {
                auto tRes = retryMetadataOp(
                    [&]() {
                        return ctx_.metadataRepo->findDocumentsByTags(req.tags, req.matchAllTags);
                    },
                    4, std::chrono::milliseconds(25), &metadataTelemetry);
                if (tRes) {
                    addDocs(convertToGrepCandidates(std::move(tRes.value())));
                }
            } else if (!req.pattern.empty() && isLikelyFtsPattern(req.pattern) &&
                       retrievalMode != search::QueryRetrievalMode::Literal &&
                       retrievalMode != search::QueryRetrievalMode::Path &&
                       !req.filesWithoutMatch) {
                auto sRes = retryMetadataOp(
                    [&]() { return ctx_.metadataRepo->search(req.pattern, max_docs_hot * 2); }, 4,
                    std::chrono::milliseconds(25), &metadataTelemetry);
                if (sRes && sRes.value().isSuccess() && !sRes.value().results.empty()) {
                    std::vector<metadata::DocumentInfo> ftsHits;
                    ftsHits.reserve(sRes.value().results.size());
                    for (const auto& r : sRes.value().results) {
                        ftsHits.push_back(r.document);
                    }
                    addDocs(convertToGrepCandidates(std::move(ftsHits)));
                    usedFtsForInitialCandidates = true;
                } else if (!req.includePatterns.empty()) {
                    auto patternDocsRes = retryMetadataOp(
                        [&]() {
                            return metadata::queryGrepCandidatesByGlobPatterns(
                                *ctx_.metadataRepo, req.includePatterns, 0);
                        },
                        4, std::chrono::milliseconds(25), &metadataTelemetry);
                    if (patternDocsRes) {
                        addDocs(std::move(patternDocsRes.value()));
                    }
                } else {
                    auto allDocsRes = retryMetadataOp(
                        [&]() {
                            return metadata::queryGrepCandidatesByPattern(*ctx_.metadataRepo, "%");
                        },
                        4, std::chrono::milliseconds(25), &metadataTelemetry);
                    if (allDocsRes) {
                        addDocs(std::move(allDocsRes.value()));
                    }
                }
            } else if (!req.includePatterns.empty()) {
                auto patternDocsRes = retryMetadataOp(
                    [&]() {
                        return metadata::queryGrepCandidatesByGlobPatterns(*ctx_.metadataRepo,
                                                                           req.includePatterns, 0);
                    },
                    4, std::chrono::milliseconds(25), &metadataTelemetry);
                if (patternDocsRes) {
                    addDocs(std::move(patternDocsRes.value()));
                    spdlog::debug("[GrepService] Filtered at SQL level by {} include patterns: {} "
                                  "docs",
                                  req.includePatterns.size(), docs.size());
                }
            } else {
                auto allDocsRes = retryMetadataOp(
                    [&]() {
                        return metadata::queryGrepCandidatesByPattern(*ctx_.metadataRepo, "%");
                    },
                    4, std::chrono::milliseconds(25), &metadataTelemetry);
                if (allDocsRes) {
                    addDocs(std::move(allDocsRes.value()));
                }
            }
        }
        recordPhaseMetric(phaseTimings, "phase_candidate_discovery_ms",
                          "grep::phase::candidate_discovery_ms", candidateDiscoveryStart);

        bool literalFtsFallback = false;
        const auto literalFtsFilterStart = GrepClock::now();
        {
            YAMS_ZONE_SCOPED_N("grep_service::literal_fts_filter");
            if (req.literalText && !req.pattern.empty() && !docs.empty() &&
                retrievalMode != search::QueryRetrievalMode::Literal &&
                retrievalMode != search::QueryRetrievalMode::Path && !usedFtsForInitialCandidates) {
                auto sRes = retryMetadataOp(
                    [&]() { return ctx_.metadataRepo->search(req.pattern, max_docs_hot); }, 4,
                    std::chrono::milliseconds(25), &metadataTelemetry);

                if (sRes && sRes.value().isSuccess()) {
                    if (!sRes.value().results.empty()) {
                        std::unordered_set<int64_t> ftsDocIds;
                        ftsDocIds.reserve(sRes.value().results.size());
                        for (const auto& r : sRes.value().results) {
                            ftsDocIds.insert(r.document.id);
                        }

                        std::vector<metadata::GrepCandidateProjection> filteredDocs;
                        filteredDocs.reserve(docs.size());
                        for (const auto& doc : docs) {
                            if (ftsDocIds.count(doc.id) > 0) {
                                filteredDocs.push_back(doc);
                            }
                        }
                        docs = std::move(filteredDocs);
                    } else {
                        literalFtsFallback = true;
                    }
                }
            }
        }
        recordPhaseMetric(phaseTimings, "phase_literal_fts_filter_ms",
                          "grep::phase::literal_fts_filter_ms", literalFtsFilterStart);

        const auto pathFilteringStart = GrepClock::now();
        {
            YAMS_ZONE_SCOPED_N("grep_service::path_filtering");
            if (docs.empty()) {
                spdlog::debug("[GrepService] No candidate documents found after initial "
                              "discovery.");
            }

            if (!req.paths.empty() || !req.includePatterns.empty()) {
                std::vector<metadata::GrepCandidateProjection> filtered;
                filtered.reserve(docs.size());

                for (auto& doc : docs) {
                    if (!pathFilterMatch(doc.filePath, req.paths))
                        continue;

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
        }
        recordPhaseMetric(phaseTimings, "phase_path_filtering_ms", "grep::phase::path_filtering_ms",
                          pathFilteringStart);

        const auto candidateClassificationStart = GrepClock::now();
        std::vector<metadata::GrepCandidateProjection> hotDocs;
        std::vector<metadata::GrepCandidateProjection> coldDocs;
        hotDocs.reserve(docs.size());
        coldDocs.reserve(docs.size());
        {
            YAMS_ZONE_SCOPED_N("grep_service::candidate_classification");
            const size_t maxFileSize = 100 * 1024 * 1024;
            size_t filesSkippedType = 0;
            size_t filesSkippedSize = 0;

            auto shouldSkipFile = [](const metadata::GrepCandidateProjection& doc) -> bool {
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

                if (doc.mimeType.find("image/") == 0 || doc.mimeType.find("video/") == 0 ||
                    doc.mimeType.find("audio/") == 0 || doc.mimeType == "application/pdf" ||
                    doc.mimeType == "application/zip" || doc.mimeType == "application/x-tar" ||
                    doc.mimeType == "application/x-gzip" || doc.mimeType == "application/x-bzip2" ||
                    doc.mimeType == "application/octet-stream") {
                    return true;
                }

                return false;
            };

            if (docs.size() > 100) {
                std::vector<std::future<std::pair<std::vector<metadata::GrepCandidateProjection>,
                                                  std::vector<metadata::GrepCandidateProjection>>>>
                    futures;

                const size_t chunkSize =
                    std::max<size_t>(10, docs.size() / std::thread::hardware_concurrency());
                futures.reserve((docs.size() + chunkSize - 1) / chunkSize);

                for (size_t i = 0; i < docs.size(); i += chunkSize) {
                    size_t end = std::min(i + chunkSize, docs.size());

                    futures.push_back(std::async(std::launch::async, [&, i, end]() {
                        std::vector<metadata::GrepCandidateProjection> hot;
                        std::vector<metadata::GrepCandidateProjection> cold;
                        for (size_t j = i; j < end; ++j) {
                            auto& d = docs[j];
                            if (shouldSkipFile(d)) {
                                continue;
                            }

                            if (static_cast<int64_t>(d.fileSize) >
                                static_cast<int64_t>(maxFileSize)) {
                                continue;
                            }

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

                for (auto& fut : futures) {
                    auto [hot, cold] = fut.get();
                    hotDocs.insert(hotDocs.end(), std::make_move_iterator(hot.begin()),
                                   std::make_move_iterator(hot.end()));
                    coldDocs.insert(coldDocs.end(), std::make_move_iterator(cold.begin()),
                                    std::make_move_iterator(cold.end()));
                }

                filesSkippedType = docs.size() - (hotDocs.size() + coldDocs.size());
                spdlog::debug("[GrepService] Parallel filtering (magic_numbers): {} docs -> {} "
                              "hot + {} cold ({} skipped)",
                              docs.size(), hotDocs.size(), coldDocs.size(), filesSkippedType);
            } else {
                for (auto& d : docs) {
                    if (shouldSkipFile(d)) {
                        filesSkippedType++;
                        continue;
                    }

                    if (static_cast<int64_t>(d.fileSize) > static_cast<int64_t>(maxFileSize)) {
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
                spdlog::debug("[GrepService] Skipped {} unsuitable files ({} binary/artifacts, {} "
                              "oversized)",
                              filesSkippedType + filesSkippedSize, filesSkippedType,
                              filesSkippedSize);
            }

            if (max_docs_hot >= 0 && hotDocs.size() > static_cast<size_t>(max_docs_hot))
                hotDocs.resize(static_cast<size_t>(max_docs_hot));
            if (max_docs_cold >= 0 && coldDocs.size() > static_cast<size_t>(max_docs_cold))
                coldDocs.resize(static_cast<size_t>(max_docs_cold));

            docs.clear();
            docs.insert(docs.end(), hotDocs.begin(), hotDocs.end());
            docs.insert(docs.end(), coldDocs.begin(), coldDocs.end());
        }
        recordPhaseMetric(phaseTimings, "phase_candidate_classification_ms",
                          "grep::phase::candidate_classification_ms", candidateClassificationStart);

        const auto metadataBatchStart = GrepClock::now();
        std::unordered_map<int64_t, std::unordered_map<std::string, metadata::MetadataValue>>
            docMetadata;
        const bool metadataForceColdEnabled = req.useSession;
        {
            YAMS_ZONE_SCOPED_N("grep_service::metadata_batch_fetch");
            if (metadataForceColdEnabled && !hotDocs.empty() && ctx_.metadataRepo) {
                std::vector<int64_t> docIds;
                docIds.reserve(hotDocs.size());
                for (const auto& d : hotDocs) {
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
        }
        recordPhaseMetric(phaseTimings, "phase_metadata_batch_fetch_ms",
                          "grep::phase::metadata_batch_fetch_ms", metadataBatchStart);

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
        size_t perRequestCap = 4;
        if (activeGuard.active() >= 4) {
            perRequestCap = 1;
        } else if (activeGuard.active() >= 2) {
            perRequestCap = 2;
        }
        size_t workers = std::min<size_t>({rec, hw, perRequestCap});
        workers = std::min(workers, docs.size() > 0 ? docs.size() : size_t{1});

        spdlog::debug("[GrepService] starting worker scan: docs={} tags={} paths={} includes={} "
                      "active={} hot_cap={} cold_cap={} budget_ms={}",
                      docs.size(), req.tags.size(), req.paths.size(), req.includePatterns.size(),
                      activeRequests, max_docs_hot, max_docs_cold, budget_ms);
        const auto workerScanStart = GrepClock::now();
        std::atomic<size_t> next{0};
        std::mutex outMutex;
        std::vector<GrepFileResult> outResults;
        outResults.reserve(docs.size());
        std::unordered_set<std::string> filesWith;
        std::unordered_set<std::string> filesWithout;
        std::atomic<size_t> totalMatches{0};
        std::atomic<size_t> regexMatches{0};
        std::atomic<std::uint64_t> docsScanned{0};
        std::atomic<std::uint64_t> linesScanned{0};
        std::atomic<std::uint64_t> bytesScanned{0};
        std::atomic<std::uint64_t> bmhPrefilterSkips{0};
        std::atomic<std::uint64_t> regexSearchCalls{0};
        std::atomic<std::uint64_t> regexScanNs{0};
        std::atomic<std::uint64_t> contentRetrievalMs{0};
        std::atomic<bool> stop{false};
        std::mutex errorMutex;
        std::vector<Error> workerErrors;
        workerErrors.reserve(workers);

        struct WorkerBreakdown {
            std::uint64_t totalMs{0};
            std::uint64_t retrievalMs{0};
            std::uint64_t regexMs{0};
            std::uint64_t otherMs{0};
            std::uint64_t docsScanned{0};
            std::uint64_t linesScanned{0};
        };
        std::mutex workerBreakdownMutex;
        std::vector<WorkerBreakdown> workerBreakdowns;
        workerBreakdowns.reserve(workers);

        constexpr size_t kHotContentBatchSize = 32;

        auto worker = [&]() {
            YAMS_ZONE_SCOPED_N("grep_service::worker_thread");
            std::uint64_t localDocsScanned = 0;
            std::uint64_t localLinesScanned = 0;
            std::uint64_t localBytesScanned = 0;
            std::uint64_t localBmhPrefilterSkips = 0;
            std::uint64_t localRegexSearchCalls = 0;
            std::uint64_t localRegexScanNs = 0;
            std::uint64_t localContentRetrievalMs = 0;
            const auto workerStart = GrepClock::now();
            std::vector<size_t> docBatchIndices;
            docBatchIndices.reserve(kHotContentBatchSize);

            while (true) {
                if (stop.load(std::memory_order_relaxed))
                    break;
                if (budget_ms > 0) {
                    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                        GrepClock::now() - start_time);
                    if (elapsed.count() >= budget_ms) {
                        stop.store(true, std::memory_order_relaxed);
                        break;
                    }
                }
                docBatchIndices.clear();
                while (docBatchIndices.size() < kHotContentBatchSize) {
                    size_t i = next.fetch_add(1);
                    if (i >= docs.size())
                        break;
                    docBatchIndices.push_back(i);
                }
                if (docBatchIndices.empty())
                    break;

                std::unordered_map<int64_t, metadata::DocumentContent> hotContentBatch;
                if ((mode == Mode::HotOnly || mode == Mode::Auto) && ctx_.metadataRepo) {
                    std::vector<int64_t> hotDocIds;
                    hotDocIds.reserve(docBatchIndices.size());
                    for (size_t docIndex : docBatchIndices) {
                        const auto& candidate = docs[docIndex];
                        const bool candidateIsHot = candidate.contentExtracted ||
                                                    (!candidate.mimeType.empty() &&
                                                     candidate.mimeType.rfind("text/", 0) == 0);
                        bool forceCold = false;
                        if (metadataForceColdEnabled) {
                            auto metadataIt = docMetadata.find(candidate.id);
                            if (metadataIt != docMetadata.end()) {
                                const auto& metadataSnapshot = metadataIt->second;
                                auto it = metadataSnapshot.find("force_cold");
                                if (it != metadataSnapshot.end()) {
                                    auto v = it->second.asString();
                                    std::string lv = v;
                                    std::transform(lv.begin(), lv.end(), lv.begin(), ::tolower);
                                    forceCold = (lv == "1" || lv == "true" || lv == "yes");
                                }
                                if (!forceCold && metadataSnapshot.find("tag:force_cold") !=
                                                      metadataSnapshot.end()) {
                                    forceCold = true;
                                }
                            }
                        }
                        if (candidateIsHot && !forceCold) {
                            hotDocIds.push_back(candidate.id);
                        }
                    }
                    if (!hotDocIds.empty()) {
                        const auto batchStart = GrepClock::now();
                        auto batchResult = retryMetadataOp(
                            [&]() { return ctx_.metadataRepo->batchGetContent(hotDocIds); }, 4,
                            std::chrono::milliseconds(25), &metadataTelemetry);
                        localContentRetrievalMs += static_cast<std::uint64_t>(
                            std::chrono::duration_cast<std::chrono::milliseconds>(GrepClock::now() -
                                                                                  batchStart)
                                .count());
                        if (batchResult) {
                            hotContentBatch = std::move(batchResult.value());
                        }
                    }
                }

                for (size_t docIndex : docBatchIndices) {
                    const auto& doc = docs[docIndex];
                    ++localDocsScanned;

                    // NOTE: path and include-pattern filtering already applied before
                    // docs entered the worker pool (lines ~663-702). No re-check needed.

                    bool forceCold = false;
                    if (metadataForceColdEnabled) {
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
                                ++localBmhPrefilterSkips;
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

                        // Regex path: full pattern matching (after literal pre-filter if
                        // applicable)
                        const char* lineData = line.c_str();
                        const size_t lineLen = line.size();
                        size_t searchOffset = 0;
                        const auto regexScanStart = GrepClock::now();
                        while (searchOffset <= lineLen) {
                            ++localRegexSearchCalls;
                            auto m = re.findNext(lineData, lineLen, searchOffset);
                            if (!m)
                                break;
                            if (boundaryOk(line, m->position, m->length))
                                ++count;
                            searchOffset = m->position + std::max<size_t>(m->length, 1);
                        }
                        localRegexScanNs += static_cast<std::uint64_t>(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(GrepClock::now() -
                                                                                 regexScanStart)
                                .count());
                        return count;
                    };

                    GrepFileResult fileResult;
                    fileResult.file = doc.filePath;
                    fileResult.fileName = std::filesystem::path(doc.filePath).filename().string();
                    fileResult.matchCount = 0;
                    size_t ln_counter = 0;
                    auto onLine = [&](const std::string& line) {
                        if (stop.load(std::memory_order_relaxed))
                            return;
                        ++ln_counter;
                        if (budget_ms > 0 && (ln_counter % 64 == 0)) {
                            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                GrepClock::now() - start_time);
                            if (elapsed.count() >= budget_ms) {
                                stop.store(true, std::memory_order_relaxed);
                                return;
                            }
                        }
                        size_t n = countMatches(line);
                        bool matched = (n > 0);
                        if (req.invert)
                            matched = !matched;
                        if (!matched)
                            return;

                        if (max_total_results > 0) {
                            const size_t limit = static_cast<size_t>(max_total_results);
                            size_t prev = totalMatches.fetch_add(n, std::memory_order_relaxed);
                            if (prev >= limit) {
                                totalMatches.fetch_sub(n, std::memory_order_relaxed);
                                stop.store(true, std::memory_order_relaxed);
                                return;
                            }
                            const size_t remaining = limit - prev;
                            if (n > remaining) {
                                const size_t overflow = n - remaining;
                                totalMatches.fetch_sub(overflow, std::memory_order_relaxed);
                                n = remaining;
                            }
                            if (n == 0) {
                                stop.store(true, std::memory_order_relaxed);
                                return;
                            }
                            if ((prev + n) >= limit) {
                                stop.store(true, std::memory_order_relaxed);
                            }
                        }

                        fileResult.matchCount += n;
                        if (req.count)
                            return;
                        GrepMatch gm;
                        gm.matchType =
                            req.literalText ? std::string("literal") : std::string("regex");
                        gm.confidence = 1.0;
                        if (req.lineNumbers)
                            gm.lineNumber = ln_counter;
                        gm.line = yams::common::sanitizeUtf8(line);
                        if (!req.invert && !req.literalText) {
                            const auto regexScanStart = GrepClock::now();
                            ++localRegexSearchCalls;
                            auto firstMatch = re.findFirst(line);
                            if (firstMatch) {
                                gm.columnStart = firstMatch->position + 1;
                                gm.columnEnd = gm.columnStart + firstMatch->length;
                            }
                            localRegexScanNs += static_cast<std::uint64_t>(
                                std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    GrepClock::now() - regexScanStart)
                                    .count());
                        } else if (!req.invert && req.literalText) {
                            auto pos = line.find(rawPattern);
                            if (pos != std::string::npos) {
                                gm.columnStart = pos + 1;
                                gm.columnEnd = gm.columnStart + rawPattern.size();
                            }
                        }
                        fileResult.matches.push_back(std::move(gm));
                    };

                    // Auto prefers metadata-backed hot content and falls back to CAS when needed.
                    auto hotIt = hotContentBatch.find(doc.id);
                    if ((mode == Mode::HotOnly || mode == Mode::Auto) && !forceCold &&
                        hotIt != hotContentBatch.end()) {
                        localBytesScanned += hotIt->second.contentText.size();
                        std::istringstream iss(hotIt->second.contentText);
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
                                    GrepClock::now() - start_time);
                                if (e2.count() >= budget_ms) {
                                    stop.store(true, std::memory_order_relaxed);
                                    break;
                                }
                            }
                        }
                    } else {
                        // Cold path: retrieve content into memory and scan directly
                        // (E) retrieveBytes avoids temp file create/write/read/delete per doc
                        // (F) Direct pointer-walking with SIMD newline scanner avoids
                        //     streambuf/ostream abstraction overhead entirely
                        if (budget_ms > 0) {
                            auto e3 = std::chrono::duration_cast<std::chrono::milliseconds>(
                                GrepClock::now() - start_time);
                            if (e3.count() >= budget_ms) {
                                stop.store(true, std::memory_order_relaxed);
                                break;
                            }
                        }
                        const auto retrievalStart = GrepClock::now();
                        auto bytesResult = ctx_.store->retrieveBytes(doc.sha256Hash);
                        localContentRetrievalMs += static_cast<std::uint64_t>(
                            std::chrono::duration_cast<std::chrono::milliseconds>(GrepClock::now() -
                                                                                  retrievalStart)
                                .count());
                        if (!bytesResult)
                            continue;
                        const auto& bytes = bytesResult.value();
                        const char* data = reinterpret_cast<const char*>(bytes.data());
                        const size_t dataSize = bytes.size();
                        localBytesScanned += dataSize;

                        // Walk the buffer with SIMD newline scanning, calling onLine per line
                        const char* p = data;
                        const char* end = data + dataSize;
                        std::string lineBuffer;
                        while (p < end) {
                            if (stop.load(std::memory_order_relaxed))
                                break;
                            size_t remaining = static_cast<size_t>(end - p);
                            size_t nlOffset = SimdNewlineScanner::findNewline(p, remaining);

                            const char* lineEnd = (nlOffset < remaining) ? (p + nlOffset) : end;
                            size_t lineLen = static_cast<size_t>(lineEnd - p);

                            // Strip trailing CR for CRLF line endings
                            size_t trimLen = lineLen;
                            if (trimLen > 0 && p[trimLen - 1] == '\r')
                                --trimLen;

                            // Check if any CRs remain (bare CRs in middle of line)
                            if (trimLen > 0 && memchr(p, '\r', trimLen) != nullptr) {
                                // Rare path: strip embedded CRs
                                lineBuffer.clear();
                                const char* q = p;
                                const char* segEnd = p + trimLen;
                                while (q < segEnd) {
                                    const char* r = static_cast<const char*>(
                                        memchr(q, '\r', static_cast<size_t>(segEnd - q)));
                                    if (!r) {
                                        lineBuffer.append(q, segEnd);
                                        break;
                                    }
                                    lineBuffer.append(q, r);
                                    q = r + 1;
                                }
                                onLine(lineBuffer);
                            } else {
                                // Fast path: zero-copy string_view → string for onLine
                                std::string line(p, trimLen);
                                onLine(line);
                            }

                            if (req.maxCount > 0 &&
                                static_cast<int>(fileResult.matchCount) >= req.maxCount)
                                break;

                            // Advance past LF (or to end if no newline found)
                            p = (nlOffset < remaining) ? (p + nlOffset + 1) : end;
                        }
                    }

                    // Early exit shaping for files-only/paths-only
                    if (req.filesWithMatches || req.pathsOnly || req.filesWithoutMatch) {
                        // Defer formatting to caller; we just track counts and file sets below
                    }

                    localLinesScanned += ln_counter;

                    std::lock_guard<std::mutex> lk(outMutex);
                    if (fileResult.matchCount > 0) {
                        spdlog::debug("[GrepService] matched '{}' count={}", fileResult.file,
                                      fileResult.matchCount);
                        if (max_total_results <= 0) {
                            totalMatches += static_cast<size_t>(fileResult.matchCount);
                        }
                        regexMatches += static_cast<size_t>(fileResult.matchCount);
                        filesWith.insert(fileResult.file);
                        if (!req.filesWithMatches && !req.pathsOnly)
                            outResults.push_back(std::move(fileResult));

                        // PERFORMANCE: Stop early if we've hit the result limit
                        if (max_total_results > 0 &&
                            totalMatches >= static_cast<size_t>(max_total_results)) {
                            spdlog::info(
                                "[GrepService] Hit result limit ({} matches), stopping early",
                                totalMatches.load());
                            stop.store(true, std::memory_order_relaxed);
                        }
                    } else {
                        filesWithout.insert(doc.filePath);
                    }
                }
            }

            docsScanned.fetch_add(localDocsScanned, std::memory_order_relaxed);
            linesScanned.fetch_add(localLinesScanned, std::memory_order_relaxed);
            bytesScanned.fetch_add(localBytesScanned, std::memory_order_relaxed);
            bmhPrefilterSkips.fetch_add(localBmhPrefilterSkips, std::memory_order_relaxed);
            regexSearchCalls.fetch_add(localRegexSearchCalls, std::memory_order_relaxed);
            regexScanNs.fetch_add(localRegexScanNs, std::memory_order_relaxed);
            contentRetrievalMs.fetch_add(localContentRetrievalMs, std::memory_order_relaxed);

            const auto workerTotalMs =
                static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                               GrepClock::now() - workerStart)
                                               .count());
            const auto workerRegexMs =
                static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                               std::chrono::nanoseconds(localRegexScanNs))
                                               .count());
            const auto accountedMs = workerRegexMs + localContentRetrievalMs;
            const auto workerOtherMs =
                (workerTotalMs > accountedMs) ? (workerTotalMs - accountedMs) : 0;

            {
                std::lock_guard<std::mutex> lk(workerBreakdownMutex);
                workerBreakdowns.push_back(WorkerBreakdown{.totalMs = workerTotalMs,
                                                           .retrievalMs = localContentRetrievalMs,
                                                           .regexMs = workerRegexMs,
                                                           .otherMs = workerOtherMs,
                                                           .docsScanned = localDocsScanned,
                                                           .linesScanned = localLinesScanned});
            }
        };

        // Wrap lambda in std::function to avoid C++20 immediate function evaluation issues
        std::function<void()> workerFunc = worker;

        {
            YAMS_ZONE_SCOPED_N("grep_service::worker_scan");
            std::vector<std::thread> ths;
            ths.reserve(workers);
            for (size_t t = 0; t < workers; ++t)
                ths.emplace_back(workerFunc);
            for (auto& th : ths)
                th.join();
        }

        auto workers_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    GrepClock::now() - workerScanStart)
                                    .count();
        recordPhaseMetric(phaseTimings, "phase_worker_scan_ms", "grep::phase::worker_scan_ms",
                          workerScanStart);
        spdlog::debug("[GrepTrace] Regex matching across {} files took {}ms.",
                      response.filesSearched, workers_duration);
        YAMS_PLOT("grep::docs_scanned",
                  static_cast<int64_t>(docsScanned.load(std::memory_order_relaxed)));
        YAMS_PLOT("grep::lines_scanned",
                  static_cast<int64_t>(linesScanned.load(std::memory_order_relaxed)));
        YAMS_PLOT("grep::bytes_scanned",
                  static_cast<int64_t>(bytesScanned.load(std::memory_order_relaxed)));
        YAMS_PLOT("grep::bmh_prefilter_skips",
                  static_cast<int64_t>(bmhPrefilterSkips.load(std::memory_order_relaxed)));
        YAMS_PLOT("grep::regex_search_calls",
                  static_cast<int64_t>(regexSearchCalls.load(std::memory_order_relaxed)));
        const auto regexScanMs = static_cast<int64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::nanoseconds(regexScanNs.load(std::memory_order_relaxed)))
                .count());
        YAMS_PLOT("grep::regex_scan_ms", regexScanMs);
        YAMS_PLOT("grep::content_retrieval_ms",
                  static_cast<int64_t>(contentRetrievalMs.load(std::memory_order_relaxed)));

        WorkerBreakdown criticalWorker;
        std::uint64_t workerTotalSumMs = 0;
        std::uint64_t workerRetrievalSumMs = 0;
        std::uint64_t workerRegexSumMs = 0;
        std::uint64_t workerOtherSumMs = 0;
        {
            std::lock_guard<std::mutex> lk(workerBreakdownMutex);
            for (const auto& worker : workerBreakdowns) {
                workerTotalSumMs += worker.totalMs;
                workerRetrievalSumMs += worker.retrievalMs;
                workerRegexSumMs += worker.regexMs;
                workerOtherSumMs += worker.otherMs;
                if (worker.totalMs > criticalWorker.totalMs) {
                    criticalWorker = worker;
                }
            }
        }

        YAMS_PLOT("grep::phase::worker_critical_total_ms",
                  static_cast<int64_t>(criticalWorker.totalMs));
        YAMS_PLOT("grep::phase::worker_critical_content_retrieval_ms",
                  static_cast<int64_t>(criticalWorker.retrievalMs));
        YAMS_PLOT("grep::phase::worker_critical_regex_scan_ms",
                  static_cast<int64_t>(criticalWorker.regexMs));
        YAMS_PLOT("grep::phase::worker_critical_other_ms",
                  static_cast<int64_t>(criticalWorker.otherMs));

        {
            std::lock_guard<std::mutex> lk(errorMutex);
            if (!workerErrors.empty())
                return workerErrors.front();
        }

        response.results = std::move(outResults);
        response.filesWith = std::vector<std::string>(filesWith.begin(), filesWith.end());
        response.filesWithout = std::vector<std::string>(filesWithout.begin(), filesWithout.end());
        if (req.pathsOnly) {
            response.pathsOnly = req.invert ? response.filesWithout : response.filesWith;
        }
        spdlog::debug("[GrepService] filesWith={} filesWithout={} results={} pathsOnly={} "
                      "totalMatches={}",
                      response.filesWith.size(), response.filesWithout.size(),
                      response.results.size(), response.pathsOnly.size(), response.totalMatches);
        response.totalMatches = totalMatches.load();
        response.regexMatches = regexMatches.load();
        response.queryInfo = "grep: parallel regex scan";
        response.searchStats["workers"] = std::to_string(workers);
        response.searchStats["files_scanned"] = std::to_string(response.filesSearched);
        const auto relationEnrichmentStart = GrepClock::now();
        {
            YAMS_ZONE_SCOPED_N("grep_service::relation_enrichment");
            if (!response.results.empty() && ctx_.kgStore) {
                const auto enriched = annotateResultRelations(response.results, ctx_.kgStore.get());
                response.searchStats["relation_results_enriched"] = std::to_string(enriched);
            }
        }
        recordPhaseMetric(phaseTimings, "phase_relation_enrichment_ms",
                          "grep::phase::relation_enrichment_ms", relationEnrichmentStart);

        const auto semanticPhaseStart = GrepClock::now();
        {
            YAMS_ZONE_SCOPED_N("grep_service::semantic_phase");
            if (!req.regexOnly && req.semanticLimit > 0 &&
                retrievalMode != search::QueryRetrievalMode::Literal &&
                retrievalMode != search::QueryRetrievalMode::Path) {
                try {
                    size_t topk = static_cast<size_t>(std::max(1, req.semanticLimit)) * 3;

                    std::shared_ptr<yams::search::SearchEngine> eng = ctx_.searchEngine;
                    if (!eng) {
                        const auto buildStart = GrepClock::now();
                        yams::search::SearchEngineBuilder builder;
                        builder.withMetadataRepo(ctx_.metadataRepo);
                        if (ctx_.vectorDatabase) {
                            builder.withVectorDatabase(ctx_.vectorDatabase);
                        }
                        auto opts = yams::search::SearchEngineBuilder::BuildOptions::makeDefault();
                        opts.config.maxResults = topk;
                        auto engRes = builder.buildEmbedded(opts);
                        if (engRes) {
                            eng = engRes.value();
                        }
                        const auto buildDuration =
                            std::chrono::duration_cast<std::chrono::milliseconds>(GrepClock::now() -
                                                                                  buildStart)
                                .count();
                        response.searchStats["semantic_engine_build_ms"] =
                            std::to_string(buildDuration);
                        YAMS_PLOT("grep::semantic_engine_build_ms", buildDuration);
                        spdlog::debug("[GrepTrace] Built fresh SearchEngine for semantic search in "
                                      "{}ms.",
                                      buildDuration);
                    }

                    if (eng) {
                        yams::search::SearchParams params;
                        params.limit = static_cast<int>(topk);
                        auto hres = eng->search(req.pattern, params);
                        if (hres) {
                            std::set<std::string> regexFiles;
                            for (const auto& fr : response.results)
                                regexFiles.insert(fr.file);
                            std::vector<yams::metadata::SearchResult> sem = hres.value();
                            int taken = 0;
                            for (const auto& r : sem) {
                                const std::string& path = r.document.filePath;
                                if (path.empty())
                                    continue;
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
                                float conf = static_cast<float>(r.score);
                                if (conf < 0.0f)
                                    conf = 0.0f;
                                if (conf > 1.0f)
                                    conf = 1.0f;
                                gm.confidence = conf;
                                gm.lineNumber = 0;
                                if (!r.snippet.empty())
                                    gm.line = yams::common::sanitizeUtf8(r.snippet);
                                fr.matches.push_back(std::move(gm));
                                fr.matchCount = 1;
                                fr.wasSemanticSearch = true;
                                response.results.push_back(std::move(fr));
                                response.semanticMatches += 1;
                                taken++;
                                if (taken >= req.semanticLimit)
                                    break;
                            }
                        }
                    }
                } catch (...) {
                }
            }
        }
        recordPhaseMetric(phaseTimings, "phase_semantic_ms", "grep::phase::semantic_ms",
                          semanticPhaseStart);

        auto totalElapsed =
            std::chrono::duration_cast<std::chrono::milliseconds>(GrepClock::now() - start_time)
                .count();
        response.executionTimeMs = static_cast<std::int64_t>(totalElapsed);
        response.searchStats["latency_ms"] = std::to_string(totalElapsed);
        response.searchStats["docs_scanned"] =
            std::to_string(docsScanned.load(std::memory_order_relaxed));
        response.searchStats["lines_scanned"] =
            std::to_string(linesScanned.load(std::memory_order_relaxed));
        response.searchStats["bytes_scanned"] =
            std::to_string(bytesScanned.load(std::memory_order_relaxed));
        response.searchStats["bmh_prefilter_skips"] =
            std::to_string(bmhPrefilterSkips.load(std::memory_order_relaxed));
        response.searchStats["regex_search_calls"] =
            std::to_string(regexSearchCalls.load(std::memory_order_relaxed));
        response.searchStats["regex_scan_ms"] = std::to_string(regexScanMs);
        response.searchStats["content_retrieval_ms"] =
            std::to_string(contentRetrievalMs.load(std::memory_order_relaxed));
        response.searchStats["worker_critical_total_ms"] = std::to_string(criticalWorker.totalMs);
        response.searchStats["worker_critical_content_retrieval_ms"] =
            std::to_string(criticalWorker.retrievalMs);
        response.searchStats["worker_critical_regex_scan_ms"] =
            std::to_string(criticalWorker.regexMs);
        response.searchStats["worker_critical_other_ms"] = std::to_string(criticalWorker.otherMs);
        response.searchStats["worker_critical_docs_scanned"] =
            std::to_string(criticalWorker.docsScanned);
        response.searchStats["worker_critical_lines_scanned"] =
            std::to_string(criticalWorker.linesScanned);
        response.searchStats["worker_total_sum_ms"] = std::to_string(workerTotalSumMs);
        response.searchStats["worker_retrieval_sum_ms"] = std::to_string(workerRetrievalSumMs);
        response.searchStats["worker_regex_sum_ms"] = std::to_string(workerRegexSumMs);
        response.searchStats["worker_other_sum_ms"] = std::to_string(workerOtherSumMs);
        response.searchStats["worker_breakdown_threads"] = std::to_string(workerBreakdowns.size());
        response.searchStats["metadata_operations"] =
            std::to_string(metadataTelemetry.operations.load(std::memory_order_relaxed));
        response.searchStats["metadata_retries"] =
            std::to_string(metadataTelemetry.retries.load(std::memory_order_relaxed));
        response.searchStats["metadata_transient_failures"] =
            std::to_string(metadataTelemetry.transientFailures.load(std::memory_order_relaxed));
        response.searchStats["query_intent"] =
            search::queryIntentToString(routeDecision.intent.label);
        response.searchStats["retrieval_mode"] =
            search::queryRetrievalModeToString(routeDecision.retrievalMode.label);
        response.searchStats["retrieval_mode_reason"] = routeDecision.retrievalMode.reason;
        response.searchStats["fts_candidate_mode"] =
            usedFtsForInitialCandidates ? "initial" : "off";
        response.searchStats["fts_literal_filter"] =
            (req.literalText && retrievalMode != search::QueryRetrievalMode::Literal &&
             retrievalMode != search::QueryRetrievalMode::Path)
                ? "eligible"
                : "off";
        for (const auto& [phaseKey, phaseValue] : phaseTimings) {
            response.searchStats[phaseKey] = std::to_string(phaseValue);
        }
        const auto metadataEnumerationMs = phaseTimings["phase_candidate_discovery_ms"] +
                                           phaseTimings["phase_path_filtering_ms"] +
                                           phaseTimings["phase_candidate_classification_ms"] +
                                           phaseTimings["phase_metadata_batch_fetch_ms"];
        response.searchStats["metadata_enumeration_ms"] = std::to_string(metadataEnumerationMs);
        const auto workerScanMs = phaseTimings["phase_worker_scan_ms"];
        const auto contentRetrievalTotalMs =
            static_cast<int64_t>(contentRetrievalMs.load(std::memory_order_relaxed));
        const auto decodeEstimateMs =
            std::max<int64_t>(0, workerScanMs - contentRetrievalTotalMs - regexScanMs);
        response.searchStats["content_decode_ms_estimate"] = std::to_string(decodeEstimateMs);
        YAMS_PLOT("grep::latency_ms", totalElapsed);
        if (budget_ms > 0) {
            response.searchStats["budget_ms"] = std::to_string(budget_ms);
            response.searchStats["timeout_triggered"] =
                stop.load(std::memory_order_relaxed) ? "1" : "0";
        }

        auto total_grep_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                                       GrepClock::now() - grep_start_time)
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
                auto res = retryMetadataOp(
                    [&]() { return metadata::queryGrepCandidatesByPattern(*repo, "%"); }, 4,
                    std::chrono::milliseconds(25), &telemetry);
                if (res)
                    addFn(std::move(res.value()));
                return;
            }
            metadata::DocumentQueryOptions opts;
            opts.pathPrefix = prefix;
            opts.prefixIsDirectory = true;
            opts.includeSubdirectories = true;
            opts.orderByNameAsc = true;
            auto res =
                retryMetadataOp([&]() { return repo->queryDocumentsForGrepCandidates(opts); }, 4,
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
