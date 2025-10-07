#include <yams/app/services/grep_mode_tls.h>
#include <yams/app/services/services.hpp>
#include <yams/common/utf8_utils.h>
// Hot/Cold mode helpers (env-driven)
#include "../../cli/hot_cold_utils.h"
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
#include <filesystem>
#include <future>
#include <mutex>
#include <thread>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

namespace yams::app::services {

namespace {

// Helper function to escape regex special characters
static std::string escapeRegex(const std::string& text) {
    static const std::string specialChars = "\\^$.|?*+()[]{}";
    std::string escaped;
    escaped.reserve(text.size() * 2);
    for (char c : text) {
        if (specialChars.find(c) != std::string::npos) {
            escaped += '\\';
        }
        escaped += c;
    }
    return escaped;
}

static bool hasWildcard(const std::string& s) {
    return s.find('*') != std::string::npos || s.find('?') != std::string::npos;
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

// Normalize path minimally for SQL LIKE against stored file_path.
// Do not alter macOS /var vs /private/var or case; only unify separators on Windows.
static std::string normalizePathForSqlLike(const std::string& path) {
    std::string out = path;
#if defined(_WIN32)
    std::replace(out.begin(), out.end(), '\\', '/');
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

// Helper to check if document has required tags
static bool isTransientMetadataError(const Error& err) {
    switch (err.code) {
        case ErrorCode::NotInitialized:
        case ErrorCode::DatabaseError:
        case ErrorCode::ResourceBusy:
        case ErrorCode::OperationInProgress:
        case ErrorCode::Timeout:
            return true;
        case ErrorCode::InternalError: {
            const auto& msg = err.message;
            return msg.find("database is locked") != std::string::npos ||
                   msg.find("readonly") != std::string::npos ||
                   msg.find("busy") != std::string::npos;
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
            if (normalizedDoc.find(normalizedFilter) != std::string::npos)
                return true;
        }
    }
    return false;
}

} // namespace

class GrepServiceImpl final : public IGrepService {
public:
    explicit GrepServiceImpl(const AppContext& ctx) : ctx_(ctx) {}

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
        const std::string rawPattern = req.pattern; // literal needle
        std::string regexPattern = req.pattern;     // regex source
        if (req.literalText) {
            // Escape regex special characters for the regex path only
            regexPattern = escapeRegex(regexPattern);
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
        const int max_docs_hot = getenv_int("YAMS_GREP_MAX_DOCS_HOT", 2000);
        const int max_docs_cold = getenv_int("YAMS_GREP_MAX_DOCS_COLD", 200);
        const int budget_ms = getenv_int("YAMS_GREP_TIME_BUDGET_MS", 15000);
        MetadataTelemetry metadataTelemetry;
        auto start_time = std::chrono::steady_clock::now();

        // --- Candidate Document Discovery ---
        // This logic is modeled after SearchService's multi-stage candidate retrieval.
        // It prioritizes smart, fast methods (hybrid search, FTS) to find a relevant
        // set of documents to grep, avoiding a full scan of all content when possible.
        std::vector<metadata::DocumentInfo> docs;
        std::unordered_set<int64_t> seenDocIds;

        auto addDocs = [&](std::vector<metadata::DocumentInfo>&& newDocs) {
            for (auto& d : newDocs) {
                if (seenDocIds.insert(d.id).second) {
                    docs.push_back(std::move(d));
                }
            }
        };

        // If explicit, restrictive filters (tags, paths) are provided, we start with them.
        // Otherwise, we use smart search to find an initial set of candidates.
        bool has_restrictive_filters =
            !req.tags.empty() || !req.paths.empty() || !req.includePatterns.empty();

        if (has_restrictive_filters) {
            if (!req.tags.empty()) {
                auto tRes = retryMetadataOp(
                    [&]() {
                        return ctx_.metadataRepo->findDocumentsByTags(req.tags, req.matchAllTags);
                    },
                    4, std::chrono::milliseconds(25), &metadataTelemetry);
                if (tRes)
                    addDocs(std::move(tRes.value()));
            }
            if (!req.paths.empty() || !req.includePatterns.empty()) {
                // Normalize include/path patterns to v13-indexed queries where possible:
                //  - "**/*.ext" or "*.ext"            => extension filter
                //  - "dir/**/*.ext"                    => pathPrefix=dir + extension filter
                //  - "**/*.{a,b,c}" (brace form)      => multiple extension filters
                //  - other patterns                     => fallback to LIKE

                auto trim = [](std::string s) {
                    auto issp = [](unsigned char c) { return std::isspace(c) != 0; };
                    while (!s.empty() && issp(static_cast<unsigned char>(s.front())))
                        s.erase(s.begin());
                    while (!s.empty() && issp(static_cast<unsigned char>(s.back())))
                        s.pop_back();
                    return s;
                };
                auto strip_leading_slash = [](std::string s) {
                    while (!s.empty() &&
                           (s.front() == '/' || (s.size() > 1 && s[0] == '.' && s[1] == '/'))) {
                        if (s.front() == '/')
                            s.erase(s.begin());
                        else
                            s.erase(s.begin(), s.begin() + 2);
                    }
                    return s;
                };

                auto collect_by_ext_and_prefix = [&](const std::string& maybePrefix,
                                                     const std::string& ext) {
                    if (ext.empty())
                        return;
                    if (maybePrefix.empty()) {
                        auto r = retryMetadataOp(
                            [&]() { return ctx_.metadataRepo->findDocumentsByExtension(ext); }, 4,
                            std::chrono::milliseconds(25), &metadataTelemetry);
                        if (r)
                            addDocs(std::move(r.value()));
                        return;
                    }
                    metadata::DocumentQueryOptions q;
                    q.pathPrefix = strip_leading_slash(maybePrefix);
                    q.prefixIsDirectory = true;
                    q.includeSubdirectories = true;
                    q.extension = ext;
                    q.orderByNameAsc = true;
                    auto r = retryMetadataOp([&]() { return ctx_.metadataRepo->queryDocuments(q); },
                                             4, std::chrono::milliseconds(25), &metadataTelemetry);
                    if (r)
                        addDocs(std::move(r.value()));
                };

                // Expand a single pattern that may include brace extension list like "*.{h,hpp}"
                auto expand_brace_exts = [](const std::string& pattern) {
                    struct Out {
                        std::string head;
                        std::vector<std::string> exts;
                    };
                    Out out;
                    auto lb = pattern.find('{');
                    auto rb =
                        (lb == std::string::npos) ? std::string::npos : pattern.find('}', lb + 1);
                    if (lb == std::string::npos || rb == std::string::npos || rb <= lb + 1) {
                        return out; // no braces
                    }
                    out.head = pattern.substr(0, lb);
                    std::string inner = pattern.substr(lb + 1, rb - lb - 1);
                    std::vector<std::string> list;
                    std::stringstream ss(inner);
                    std::string item;
                    while (std::getline(ss, item, ',')) {
                        // trim spaces
                        item.erase(0, item.find_first_not_of(" \t"));
                        if (!item.empty())
                            item.erase(item.find_last_not_of(" \t") + 1);
                        if (!item.empty())
                            list.push_back(item);
                    }
                    out.exts = std::move(list);
                    return out;
                };

                std::vector<std::string> patterns;
                patterns.reserve(req.paths.size() + req.includePatterns.size());
                patterns.insert(patterns.end(), req.paths.begin(), req.paths.end());
                patterns.insert(patterns.end(), req.includePatterns.begin(),
                                req.includePatterns.end());

                std::size_t likeFallbacks = 0;
                std::size_t indexedFastPaths = 0;
                for (const auto& raw : patterns) {
                    std::string p = trim(raw);
                    if (p.empty())
                        continue;

                    // Handle brace expansion variants first
                    auto brace = expand_brace_exts(p);
                    if (!brace.head.empty() && !brace.exts.empty()) {
                        // Cases: "**/*. {a,b}" or "dir/**/*.{a,b}" or "*.{a,b}"
                        if (brace.head == "**/*.") {
                            for (const auto& e : brace.exts)
                                collect_by_ext_and_prefix("", e);
                            indexedFastPaths += brace.exts.size();
                            continue;
                        }
                        // "*.{exts}"
                        if (brace.head == "*.") {
                            for (const auto& e : brace.exts)
                                collect_by_ext_and_prefix("", e);
                            indexedFastPaths += brace.exts.size();
                            continue;
                        }
                        // "dir/**/*.{exts}"
                        auto marker = brace.head.find("/**/");
                        if (marker != std::string::npos && brace.head.size() >= marker + 4 &&
                            brace.head.rfind("*.", std::string::npos) == std::string::npos) {
                            std::string prefix = brace.head.substr(0, marker);
                            for (const auto& e : brace.exts)
                                collect_by_ext_and_prefix(prefix, e);
                            indexedFastPaths += brace.exts.size();
                            continue;
                        }
                        // Fallthrough to LIKE if unrecognized form
                    }

                    // Case 1: "**/*.ext" or "*.ext"
                    if ((p.rfind("**/*.", 0) == 0 && p.size() > 5) ||
                        (p.rfind("*.", 0) == 0 && p.size() > 2)) {
                        const char* start = (p[0] == '*' && p.size() > 1 && p[1] == '.')
                                                ? p.c_str() + 2
                                                : (p.size() > 4 ? p.c_str() + 5 : p.c_str());
                        std::string ext(start);
                        collect_by_ext_and_prefix("", ext);
                        ++indexedFastPaths;
                        continue;
                    }

                    // Case 2: "dir/**/*.ext" -> prefix + ext
                    std::size_t starDot = p.rfind("*.");
                    std::size_t marker = p.find("/**/");
                    if (marker != std::string::npos && starDot != std::string::npos &&
                        starDot > marker) {
                        std::string ext = p.substr(starDot + 2);
                        std::string prefix = p.substr(0, marker);
                        collect_by_ext_and_prefix(prefix, ext);
                        ++indexedFastPaths;
                        continue;
                    }

                    // Fallback: LIKE pattern
                    std::string likePattern = normalizePathForSqlLike(p);
                    std::replace(likePattern.begin(), likePattern.end(), '*', '%');
                    auto r = retryMetadataOp(
                        [&]() {
                            return metadata::queryDocumentsByPattern(*ctx_.metadataRepo,
                                                                     likePattern);
                        },
                        4, std::chrono::milliseconds(25), &metadataTelemetry);
                    if (r)
                        addDocs(std::move(r.value()));
                    ++likeFallbacks;
                }
                spdlog::info(
                    "[GrepService] includePatterns= {}{}; indexedFastPaths={} likeFallbacks={} "
                    "candidates(before dedupe)={}",
                    (req.includePatterns.empty() ? std::string("<none>")
                                                 : req.includePatterns.front()),
                    (req.includePatterns.size() > 1
                         ? std::string(", …(") + std::to_string(req.includePatterns.size()) + ")"
                         : std::string()),
                    indexedFastPaths, likeFallbacks, docs.size());
            }
        } else {
            // No restrictive filters, so use smart search to find candidates.
            // 1. Hybrid Search (Semantic + Keyword)
            if (ctx_.hybridEngine) {
                auto hres = ctx_.hybridEngine->search(req.pattern, 1000);
                if (hres && !hres.value().empty()) {
                    std::vector<metadata::DocumentInfo> semanticDocs;
                    // This is inefficient, but necessary as hybrid search doesn't return doc IDs.
                    std::unordered_set<std::string> allowPaths;
                    for (const auto& r : hres.value()) {
                        if (auto it = r.metadata.find("path"); it != r.metadata.end())
                            allowPaths.insert(it->second);
                    }
                    if (!allowPaths.empty()) {
                        auto allDocsRes = retryMetadataOp([&]() {
                            return metadata::queryDocumentsByPattern(*ctx_.metadataRepo, "%");
                        });
                        if (allDocsRes) {
                            for (auto& d : allDocsRes.value()) {
                                if (allowPaths.count(d.filePath))
                                    semanticDocs.push_back(std::move(d));
                            }
                            addDocs(std::move(semanticDocs));
                        }
                    }
                }
            }
            // 2. FTS Search (Keyword)
            if (req.literalText || req.word) {
                auto sRes = retryMetadataOp(
                    [&]() { return ctx_.metadataRepo->search(req.pattern, 2000, 0); });
                if (sRes && sRes.value().isSuccess()) {
                    std::vector<metadata::DocumentInfo> ftsDocs;
                    for (const auto& r : sRes.value().results)
                        ftsDocs.push_back(r.document);
                    addDocs(std::move(ftsDocs));
                }
            }
        }

        // If no candidates were found by any method, return empty-handed.
        if (docs.empty()) {
            spdlog::debug("[GrepService] No candidate documents found after initial discovery.");
        }

        // Ensure candidate set includes any explicit req.paths selections by performing
        // directory-prefix LIKE scans or exact path matches before filtering. This helps
        // surface freshly ingested documents that may not yet appear in broad enumerations.
        if (!req.paths.empty()) {
            std::unordered_set<int64_t> have;
            have.reserve(docs.size());
            for (const auto& d : docs)
                have.insert(d.id);
            for (const auto& p : req.paths) {
                if (p.empty())
                    continue;
                std::string like;
                bool wild =
                    (p.find('*') != std::string::npos) || (p.find('?') != std::string::npos);
                auto dd = p.find("**");
                if (wild || dd != std::string::npos) {
                    std::string prefix = (dd != std::string::npos) ? p.substr(0, dd) : p;
                    auto lastSlash = prefix.find_last_of('/');
                    if (lastSlash != std::string::npos)
                        prefix = prefix.substr(0, lastSlash + 1);
                    auto normPrefix = normalizePathForSqlLike(prefix);
                    if (!normPrefix.empty() && normPrefix.back() != '/')
                        normPrefix.push_back('/');
                    like = normPrefix + "%";
                } else {
                    auto norm = normalizePathForSqlLike(p);
                    if (!norm.empty() && norm.back() == '/')
                        like = norm + "%";
                    else
                        like = norm;
                }
                if (like.empty())
                    continue;
                auto addRes = retryMetadataOp(
                    [&]() { return metadata::queryDocumentsByPattern(*ctx_.metadataRepo, like); },
                    4, std::chrono::milliseconds(25), &metadataTelemetry);
                if (addRes && !addRes.value().empty()) {
                    for (auto& d : addRes.value()) {
                        if (have.insert(d.id).second)
                            docs.push_back(std::move(d));
                    }
                }
            }
        }

        spdlog::debug("[GrepService] candidates pre-filter count={}", docs.size());

        // If tags were provided, ensure the candidate set is filtered by them.
        // This is because the candidate set could have been populated by FTS or other
        // means that do not account for tags.
        if (!req.tags.empty()) {
            auto tagDocsRes = retryMetadataOp(
                [&]() {
                    return ctx_.metadataRepo->findDocumentsByTags(req.tags, req.matchAllTags);
                },
                4, std::chrono::milliseconds(25), &metadataTelemetry);

            if (!tagDocsRes) {
                spdlog::debug("[GrepService] tag lookup failed: code={} message={}",
                              static_cast<int>(tagDocsRes.error().code),
                              tagDocsRes.error().message);
                return Result<GrepResponse>(tagDocsRes.error());
            }

            const auto& docsVec = tagDocsRes.value();
            if (!docsVec.empty()) {
                std::unordered_set<int64_t> allowedIds;
                allowedIds.reserve(docsVec.size());
                for (const auto& doc : docsVec) {
                    allowedIds.insert(doc.id);
                }

                std::vector<metadata::DocumentInfo> finalDocs;
                finalDocs.reserve(docs.size());
                for (const auto& doc : docs) {
                    if (allowedIds.count(doc.id)) {
                        finalDocs.push_back(doc);
                    }
                }
                docs = std::move(finalDocs);
                spdlog::debug("[GrepService] candidates after tag filter count={} (metadata ops={} "
                              "retries={} transient_failures={})",
                              docs.size(), metadataTelemetry.operations.load(),
                              metadataTelemetry.retries.load(),
                              metadataTelemetry.transientFailures.load());
            } else {
                spdlog::debug(
                    "[GrepService] tag repository returned no direct candidates; deferring tag "
                    "validation to per-document metadata (ops={} retries={} transient_failures={})",
                    metadataTelemetry.operations.load(), metadataTelemetry.retries.load(),
                    metadataTelemetry.transientFailures.load());
            }
        }

        // Apply early path/include filtering
        if (!req.paths.empty() || !req.includePatterns.empty()) {
            const size_t preCount = docs.size();
            std::vector<metadata::DocumentInfo> filtered;
            filtered.reserve(docs.size());
            for (auto& d : docs) {
                if (!req.paths.empty() && !pathFilterMatch(d.filePath, req.paths))
                    continue;
                if (!req.includePatterns.empty() &&
                    !pathFilterMatch(d.filePath, req.includePatterns))
                    continue;
                filtered.push_back(std::move(d));
            }
            docs = std::move(filtered);
            try {
                if (!req.includePatterns.empty()) {
                    std::ostringstream oss;
                    oss << "[GrepService] includePatterns= ";
                    for (size_t i = 0; i < req.includePatterns.size(); ++i) {
                        if (i)
                            oss << ", ";
                        oss << req.includePatterns[i];
                    }
                    oss << "; candidates after filter=" << docs.size() << "/" << preCount;
                    spdlog::info("{}", oss.str());
                }
                if (!req.paths.empty()) {
                    std::ostringstream oss2;
                    oss2 << "[GrepService] paths preselect size=" << preCount
                         << ", after filter=" << docs.size();
                    spdlog::debug("{}", oss2.str());
                }
            } catch (...) {
            }
        }

        // Prefer hot docs (extracted/text) then cold; cap both sets
        std::vector<metadata::DocumentInfo> hotDocs;
        std::vector<metadata::DocumentInfo> coldDocs;
        hotDocs.reserve(docs.size());
        coldDocs.reserve(docs.size());
        for (auto& d : docs) {
            bool isText = !d.mimeType.empty() && d.mimeType.rfind("text/", 0) == 0;
            if (d.contentExtracted || isText)
                hotDocs.push_back(std::move(d));
            else
                coldDocs.push_back(std::move(d));
        }
        if (max_docs_hot >= 0 && hotDocs.size() > static_cast<size_t>(max_docs_hot))
            hotDocs.resize(static_cast<size_t>(max_docs_hot));
        if (max_docs_cold >= 0 && coldDocs.size() > static_cast<size_t>(max_docs_cold))
            coldDocs.resize(static_cast<size_t>(max_docs_cold));

        docs.clear();
        docs.insert(docs.end(), hotDocs.begin(), hotDocs.end());
        docs.insert(docs.end(), coldDocs.begin(), coldDocs.end());

        GrepResponse response;
        response.totalMatches = 0;
        response.regexMatches = 0;
        response.semanticMatches = 0;

        response.filesSearched = docs.size();

        // Mode selection for tiered grep
        enum class Mode { Auto, HotOnly, ColdOnly };
        Mode mode = Mode::Auto;
        // Thread-local override from streaming fast/full execution, if set
        switch (get_grep_mode_tls()) {
            case GrepExecMode::HotOnly:
                mode = Mode::HotOnly;
                break;
            case GrepExecMode::ColdOnly:
                mode = Mode::ColdOnly;
                break;
            default: {
                yams::cli::HotColdMode grepMode = yams::cli::getGrepMode();
                switch (get_grep_mode_tls()) {
                    case GrepExecMode::HotOnly:
                        mode = Mode::HotOnly;
                        break;
                    case GrepExecMode::ColdOnly:
                        mode = Mode::ColdOnly;
                        break;
                    default:
                        if (grepMode == yams::cli::HotColdMode::HotOnly)
                            mode = Mode::HotOnly;
                        else if (grepMode == yams::cli::HotColdMode::ColdOnly)
                            mode = Mode::ColdOnly;
                        break;
                }
                break;
            }
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
                                docGlobPath.find(normalizedPattern) != std::string::npos) {
                                ok = true;
                                break;
                            }
                        }
                    }
                    if (!ok)
                        continue;
                }

                // Respect per-document force_cold (metadata key or tag)
                bool forceCold = false;
                std::optional<std::unordered_map<std::string, metadata::MetadataValue>>
                    metadataSnapshot;
                if (ctx_.metadataRepo) {
                    auto md =
                        retryMetadataOp([&]() { return ctx_.metadataRepo->getAllMetadata(doc.id); },
                                        4, std::chrono::milliseconds(25), &metadataTelemetry);
                    if (!md) {
                        spdlog::warn(
                            "[GrepService] metadata fetch failed for doc {}: code={} message={}",
                            doc.id, static_cast<int>(md.error().code), md.error().message);
                        {
                            std::lock_guard<std::mutex> lk(errorMutex);
                            workerErrors.push_back(md.error());
                        }
                        stop.store(true, std::memory_order_relaxed);
                        return;
                    }
                    metadataSnapshot = md.value();

                    if (!req.tags.empty()) {
                        size_t matchedTags = 0;
                        for (const auto& tag : req.tags) {
                            const std::string key = std::string("tag:") + tag;
                            const bool present =
                                metadataSnapshot->find(key) != metadataSnapshot->end();
                            if (present) {
                                ++matchedTags;
                                if (!req.matchAllTags)
                                    break;
                            } else if (req.matchAllTags) {
                                matchedTags = 0;
                                break;
                            }
                        }
                        const bool tagsSatisfied =
                            req.matchAllTags ? (matchedTags == req.tags.size()) : (matchedTags > 0);
                        if (!tagsSatisfied) {
                            continue;
                        }
                    }

                    auto it = metadataSnapshot->find("force_cold");
                    if (it != metadataSnapshot->end()) {
                        auto v = it->second.asString();
                        std::string lv = v;
                        std::transform(lv.begin(), lv.end(), lv.begin(), ::tolower);
                        forceCold = (lv == "1" || lv == "true" || lv == "yes");
                    }
                    if (!forceCold &&
                        metadataSnapshot->find("tag:force_cold") != metadataSnapshot->end()) {
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
                    // Fast path: pure literal, case-sensitive
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
                    // Regex/case-insensitive path: scan all occurrences and enforce boundaries when
                    // needed
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
                            // Process in bulk: split on '\n' without per-char callbacks
                            const char* p = s;
                            const char* end = s + n;
                            while (p < end) {
                                const void* nl = memchr(p, '\n', static_cast<size_t>(end - p));
                                if (!nl) {
                                    if (memchr(p, '\r', static_cast<size_t>(end - p)) != nullptr) {
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
                                const char* nlc = static_cast<const char*>(nl);
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
                } else {
                    filesWithout.push_back(doc.filePath);
                }
            }
        };

        std::vector<std::thread> ths;
        ths.reserve(workers);
        for (size_t t = 0; t < workers; ++t)
            ths.emplace_back(worker);
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
                std::shared_ptr<yams::search::HybridSearchEngine> eng = ctx_.hybridEngine;
                size_t topk = static_cast<size_t>(std::max(1, req.semanticLimit)) * 3;
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
                    spdlog::debug("[GrepTrace] Built temporary HybridSearchEngine for semantic "
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
                                            pathGlob.find(normalized) != std::string::npos) {
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
};

// Optional separate factory for direct construction (callers may wire this in a factory unit).
std::shared_ptr<IGrepService> makeGrepService(const AppContext& ctx) {
    return std::make_shared<GrepServiceImpl>(ctx);
}

} // namespace yams::app::services
