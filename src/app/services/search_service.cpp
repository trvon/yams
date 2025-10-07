#include <spdlog/spdlog.h>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/app/services/enhanced_search_executor.h>
#include <yams/app/services/services.hpp>
#include <yams/metadata/query_helpers.h>
#ifdef YAMS_ENABLE_DAEMON_FEATURES
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/resource/plugin_host.h>
#endif
#include <yams/plugins/search_provider_v1.h>
#include <yams/search/query_qualifiers.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <regex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#ifndef _WIN32
#include <unistd.h>
#endif

// Forward util for snippet creation
namespace yams::app::services::utils {
std::string createSnippet(const std::string& content, size_t maxLength, bool preserveWordBoundary);
}

namespace yams::app::services {

namespace {

// Returns true if s consists only of hex digits
bool isHex(const std::string& s) {
    return std::all_of(s.begin(), s.end(), [](unsigned char c) { return std::isxdigit(c) != 0; });
}

// Heuristic: treat as hash when it looks like a hex string of reasonable length (8-64)
bool looksLikeHash(const std::string& s) {
    if (s.size() < 8 || s.size() > 64)
        return false;
    return isHex(s);
}

static bool hasWildcard(const std::string& s) {
    return s.find('*') != std::string::npos || s.find('?') != std::string::npos;
}

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

// Converts a glob pattern to a regex string.
static std::string globToRegex(const std::string& glob) {
    std::string regex_str;
    regex_str.reserve(glob.size() * 2);
    for (size_t i = 0; i < glob.size(); ++i) {
        char c = glob[i];
        if (c == '*') {
            if (i + 1 < glob.size() && glob[i + 1] == '*') {
                // '**' matches any sequence of characters, including path separators
                regex_str += ".*";
                i++; // consume second '*'
            } else {
                // '*' matches any sequence of characters except path separators
                regex_str += "[^/]*";
            }
        } else if (c == '?') {
            regex_str += ".";
        } else if (c == '.' || c == '+' || c == '(' || c == ')' || c == '{' || c == '}' ||
                   c == '[' || c == ']' || c == '^' || c == '|' || c == '\\') {
            regex_str += '\\';
            regex_str += c;
        } else {
            regex_str += c;
        }
    }
    return regex_str;
}

// Robust glob matcher using regex, supporting '**'.
static bool wildcardMatch(const std::string& text, const std::string& pattern) {
    try {
        std::regex re(globToRegex(pattern));
        return std::regex_match(text, re);
    } catch (const std::regex_error& e) {
        spdlog::warn("Invalid glob pattern '{}' converted to regex: {}", pattern, e.what());
        // Fallback to simple string contains for invalid patterns
        return text.find(pattern) != std::string::npos;
    }
}

// Heuristic: treat as path/filename when the query contains a separator
// or looks like a single token with an extension and no spaces.
static bool looksLikePathQuery(const std::string& q) {
    if (q.find('/') != std::string::npos || q.find('\\') != std::string::npos)
        return true;
    if (q.find(' ') != std::string::npos)
        return false;
    auto dot = q.rfind('.');
    if (dot != std::string::npos && dot > 0 && dot + 1 < q.size()) {
        // Has an extension-like suffix
        return true;
    }
    // Wildcards also indicate a path-style intent
    return hasWildcard(q);
}

// Presence-based tag match using metadata repository
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
boost::asio::awaitable<decltype(std::declval<Fn>()())>
retryMetadataOp(Fn&& fn, std::size_t maxAttempts = 4,
                std::chrono::milliseconds initialDelay = std::chrono::milliseconds(25),
                MetadataTelemetry* telemetry = nullptr) {
    using ResultT = decltype(std::declval<Fn>()());

    if (telemetry) {
        telemetry->operations.fetch_add(1, std::memory_order_relaxed);
    }

    auto attempt = fn();
    if (attempt) {
        co_return attempt;
    }

    auto delay = initialDelay;
    bool transient = isTransientMetadataError(attempt.error());
    if (telemetry && transient) {
        telemetry->transientFailures.fetch_add(1, std::memory_order_relaxed);
    }

    for (std::size_t i = 1; i < maxAttempts && transient; ++i) {
        if (telemetry) {
            telemetry->retries.fetch_add(1, std::memory_order_relaxed);
        }

        boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
        timer.expires_after(delay);
        co_await timer.async_wait(boost::asio::use_awaitable);

        delay = std::min(delay * 2, std::chrono::milliseconds(250));

        attempt = fn();
        if (attempt) {
            co_return attempt;
        }

        transient = isTransientMetadataError(attempt.error());
        if (telemetry && transient) {
            telemetry->transientFailures.fetch_add(1, std::memory_order_relaxed);
        }
    }

    co_return attempt;
}

static boost::asio::awaitable<bool> metadataHasTags(metadata::MetadataRepository* repo,
                                                    int64_t docId,
                                                    const std::vector<std::string>& tags,
                                                    bool matchAll,
                                                    MetadataTelemetry* telemetry = nullptr) {
    if (!repo || tags.empty()) {
        co_return true;
    }

    auto md = co_await retryMetadataOp([&]() { return repo->getAllMetadata(docId); }, 4,
                                       std::chrono::milliseconds(25), telemetry);

    if (!md) {
        spdlog::debug("SearchService: metadata lookup failed for doc {}: {}", docId,
                      md.error().message);
        co_return false;
    }

    auto& all = md.value();

    auto hasTag = [&](const std::string& t) {
        auto it = all.find(t);
        if (it != all.end()) {
            return true;
        }
        for (const auto& [k, v] : all) {
            if (v.asString() == t) {
                return true;
            }
        }
        return false;
    };

    if (matchAll) {
        for (const auto& t : tags) {
            if (!hasTag(t)) {
                co_return false;
            }
        }
        co_return true;
    } else {
        for (const auto& t : tags) {
            if (hasTag(t)) {
                co_return true;
            }
        }
        co_return false;
    }
}

// Compute recommended worker count based on hardware, load and caps
static inline size_t recommendedWorkers(size_t items) {
    size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
    size_t rec = hw > 1 ? (hw - 1) : 1;
#if !defined(_WIN32) && !defined(__ANDROID__)
    double loads[3] = {0, 0, 0};
    if (getloadavg(loads, 3) == 3) {
        double capacity = std::max(0.0, static_cast<double>(hw) - loads[0]);
        rec = static_cast<size_t>(std::clamp(capacity, 1.0, static_cast<double>(hw)));
    }
#endif
    rec = std::max(rec, hw / 2);
    size_t workers = std::clamp<size_t>(rec, 1, hw);
    if (const char* gcap = std::getenv("YAMS_DAEMON_WORKERS_MAX"); gcap && *gcap) {
        try {
            auto cap = static_cast<size_t>(std::stoul(gcap));
            if (cap > 0)
                workers = std::min(workers, cap);
        } catch (...) {
        }
    }
    workers = std::min(workers, items > 0 ? items : size_t{1});
    return std::max<size_t>(1, workers);
}

// Normalize scores to be non-negative and sort-friendly for UI/tests.
// For full-text (FTS5 BM25), lower is better; convert to descending positive [0,1].
// For fuzzy/other modes, clamp to >= 0 but keep natural ordering.
static void normalizeScores(std::vector<SearchItem>& results, const std::string& type) {
    if (results.empty())
        return;
    const double eps = 1e-9;
    if (type == "full-text") {
        double minv = results.front().score;
        double maxv = results.front().score;
        for (const auto& r : results) {
            minv = std::min(minv, r.score);
            maxv = std::max(maxv, r.score);
        }
        if (std::abs(maxv - minv) < 1e-12) {
            for (auto& r : results)
                r.score = 1.0; // equal scores, set to 1.0 for all
            return;
        }
        // Invert so lower raw scores (better BM25) map to higher normalized scores
        const double span = (maxv - minv);
        for (auto& r : results) {
            double inv = (maxv - r.score) / span; // in [0,1]
            // Ensure strictly positive for tests expecting > 0
            r.score = std::max(inv + eps, 0.0);
        }
        // Keep the original DB order (already ordered by raw score). After inversion, the
        // sequence should be non-increasing.
        return;
    }
    // Other modes: clamp
    for (auto& r : results) {
        if (r.score < 0.0)
            r.score = 0.0;
    }
}

} // namespace

class SearchServiceImpl final : public ISearchService {
public:
    explicit SearchServiceImpl(const AppContext& ctx) : ctx_(ctx) {
        // Initialize degraded mode from AppContext repair flags (preferred), falling back to env
        // vars
        degraded_ = ctx_.searchRepairInProgress || (ctx_.hybridEngine == nullptr);
        if (const char* d = std::getenv("YAMS_SEARCH_DEGRADED")) {
            std::string v(d);
            std::transform(v.begin(), v.end(), v.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (v == "1" || v == "true" || v == "yes" || v == "on")
                degraded_ = true;
        }
        // Prefer details from AppContext; overrideable via env
        if (!ctx_.searchRepairDetails.empty()) {
            repairDetails_ = ctx_.searchRepairDetails;
        }
        if (const char* r = std::getenv("YAMS_SEARCH_DEGRADED_REASON")) {
            repairDetails_ = r;
        }

        // Enhanced search config (best-effort; off by default). Keep lightweight and safe.
        try {
            enhancedCfg_ = EnhancedSearchExecutor::loadConfigFromToml();
            if (enhancedCfg_.enable) {
                hotzones_ = std::make_shared<yams::search::HotzoneManager>(enhancedCfg_.hotzones);
                enhanced_.setHotzoneManager(hotzones_);
            }
        } catch (...) {
            // Ignore config errors; keep enhancements disabled.
            enhancedCfg_ = {};
        }
    }

    // Minimal JSON emission for LLM/CLI when requested.
    static void maybeEmitJson(const SearchRequest& req, SearchResponse& resp) {
        if (!req.jsonOutput)
            return;
        std::ostringstream os;
        os << "{";
        os << "\"type\":\"" << (resp.type.empty() ? (req.fuzzy ? "fuzzy" : "keyword") : resp.type)
           << "\",";
        os << "\"total\":" << resp.total << ",";
        if (req.pathsOnly) {
            os << "\"paths\":[";
            for (size_t i = 0; i < resp.paths.size(); ++i) {
                if (i)
                    os << ",";
                os << "\"" << resp.paths[i] << "\"";
            }
            os << "]";
        } else {
            os << "\"results\":[";
            for (size_t i = 0; i < resp.results.size(); ++i) {
                if (i)
                    os << ",";
                const auto& it = resp.results[i];
                os << "{\"path\":\"" << it.path << "\",\"title\":\"" << it.title
                   << "\",\"score\":" << (it.score < 0.0 ? 0.0 : it.score) << "}";
            }
            os << "]";
        }
        os << "}";
        resp.jsonOutput = os.str();
    }

    boost::asio::awaitable<Result<SearchResponse>> search(const SearchRequest& req) override {
        using namespace std::chrono;
        const auto t0 = steady_clock::now();
        MetadataTelemetry metadataTelemetry;

        // Validate dependencies
        if (!ctx_.metadataRepo) {
            co_return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }

        auto parsed = yams::search::parseQueryQualifiers(req.query);
        SearchRequest normalizedReq = req;
        normalizedReq.query = parsed.normalizedQuery;

        if (normalizedReq.query.empty() && normalizedReq.hash.empty()) {
            co_return Error{ErrorCode::InvalidArgument, "Query or hash is required"};
        }
        constexpr std::size_t kMaxReasonableLimit = 100000;
        if (normalizedReq.limit > kMaxReasonableLimit) {
            co_return Error{ErrorCode::InvalidArgument, "Limit is out of allowed range"};
        }

        if (!parsed.scope.name.empty()) {
            if (normalizedReq.pathPattern.empty()) {
                normalizedReq.pathPattern = parsed.scope.name;
            } else if (normalizedReq.pathPattern.find(parsed.scope.name) == std::string::npos) {
                normalizedReq.pathPattern += " " + parsed.scope.name;
            }
        }
        if (normalizedReq.extension.empty() && !parsed.scope.ext.empty()) {
            normalizedReq.extension = parsed.scope.ext;
        }
        if (normalizedReq.mimeType.empty() && !parsed.scope.mime.empty()) {
            normalizedReq.mimeType = parsed.scope.mime;
        }

        if (!normalizedReq.hash.empty()) {
            if (!looksLikeHash(normalizedReq.hash)) {
                co_return Error{ErrorCode::InvalidArgument,
                                "Invalid hash format (expected hex, 8-64 chars)"};
            }
            auto result = co_await searchByHashPrefix(normalizedReq, &metadataTelemetry);
            setExecTime(result, t0);
            if (result) {
                auto r = std::move(result).value();
                maybeEmitJson(req, r);
                co_return Result<SearchResponse>(std::move(r));
            }
            co_return result;
        }

        if (looksLikeHash(normalizedReq.query)) {
            auto result = co_await searchByHashPrefix(normalizedReq, &metadataTelemetry);
            setExecTime(result, t0);
            if (result) {
                auto r = std::move(result).value();
                maybeEmitJson(req, r);
                co_return Result<SearchResponse>(std::move(r));
            }
            co_return result;
        }

        const std::string type = req.type.empty() ? "hybrid" : req.type;

        spdlog::info("SearchService: type='{}' fuzzy={} sim={} pathsOnly={} literal={} limit={} "
                     "filters: ext='{}' mime='{}' path='{}' tags={} allTags={}",
                     type, req.fuzzy, req.similarity, req.pathsOnly, req.literalText, req.limit,
                     req.extension, req.mimeType, req.pathPattern, req.tags.size(),
                     req.matchAllTags);

        Result<SearchResponse> result(Error{ErrorCode::Unknown, "Search path not taken"});

        // Path/filename-first heuristic: if the user likely typed a path-like query,
        // avoid noisy FTS5 attempts and go straight to metadata path/name contains.
        if (looksLikePathQuery(normalizedReq.query)) {
            auto pathResult = co_await pathSearch(normalizedReq, &metadataTelemetry);
            setExecTime(pathResult, t0);
            if (pathResult) {
                auto resp = std::move(pathResult).value();
                resp.type = "path";
                resp.searchStats["mode"] = "path";
                resp.queryInfo = "path/name contains match";
                maybeEmitJson(req, resp);
                co_return Result<SearchResponse>(std::move(resp));
            }
            // Fall through to standard paths on error.
        }

        if (type == "hybrid" || type == "semantic") {
            if (degraded_) {
                spdlog::warn("SearchService: degraded mode active{}",
                             repairDetails_.empty() ? "" : (std::string(": ") + repairDetails_));
                result = metadataSearch(normalizedReq, &metadataTelemetry);
                if (result) {
                    auto resp = std::move(result).value();
                    resp.queryInfo =
                        std::string("degraded mode — using metadata fallback") +
                        (repairDetails_.empty() ? "" : (std::string(" (") + repairDetails_ + ")"));
                    resp.searchStats["mode"] = "degraded";
                    if (!repairDetails_.empty())
                        resp.searchStats["repair_details"] = repairDetails_;
                    if (ctx_.searchRepairProgress > 0)
                        resp.searchStats["repair_progress"] =
                            std::to_string(ctx_.searchRepairProgress);
                    maybeEmitJson(req, resp);
                    result = Result<SearchResponse>(std::move(resp));
                }
            } else if (ctx_.hybridEngine) {
                result = hybridSearch(normalizedReq, parsed.scope, &metadataTelemetry,
                                      normalizedReq.pathPattern);
                if (result && !req.pathPattern.empty()) {
                    auto resp = std::move(result).value();
                    std::vector<SearchItem> filtered;
                    for (const auto& item : resp.results) {
                        if (wildcardMatch(item.path, req.pathPattern)) {
                            filtered.push_back(item);
                        }
                    }
                    resp.results = std::move(filtered);
                    resp.total = resp.results.size();
                    result = Result<SearchResponse>(std::move(resp));
                }
            } else {
                spdlog::warn("Hybrid engine unavailable, using metadata fallback");
                result = metadataSearch(normalizedReq, &metadataTelemetry);
                if (result) {
                    auto resp = std::move(result).value();
                    resp.searchStats["mode"] = "degraded";
                    resp.queryInfo = "hybrid unavailable — metadata fallback";
                    maybeEmitJson(req, resp);
                    result = Result<SearchResponse>(std::move(resp));
                }
            }
        } else {
            result = metadataSearch(normalizedReq, &metadataTelemetry);
        }

        setExecTime(result, t0);

        if (result && !normalizedReq.pathsOnly) {
            auto resp = std::move(result).value();
            // Apply optional enhanced pipeline (Phase A: hotzones only) before snippets.
            if (enhancedCfg_.enable) {
                enhanced_.apply(ctx_, enhancedCfg_, normalizedReq.query, resp.results);
            }
            co_await hydrateSnippetsAsync_worker(normalizedReq, resp, &metadataTelemetry);
            result = Result<SearchResponse>(std::move(resp));
        }

        // --- Plugin Search ---
#ifdef YAMS_ENABLE_DAEMON_FEATURES
        if (ctx_.service_manager) {
            auto abi_host = ctx_.service_manager->getAbiPluginHost();
            if (abi_host) {
                auto loaded_plugins = abi_host->listLoaded();
                for (const auto& plugin_desc : loaded_plugins) {
                    auto iface =
                        abi_host->getInterface(plugin_desc.name, YAMS_IFACE_SEARCH_PROVIDER_V1_ID,
                                               YAMS_IFACE_SEARCH_PROVIDER_V1_VERSION);
                    if (iface) {
                        auto* search_provider =
                            static_cast<yams_search_provider_v1*>(iface.value());
                        if (search_provider && search_provider->search) {
                            // For now, we don't have a good way to do this asynchronously from the
                            // plugin. We will call it synchronously and merge the results.
                        }
                    }
                }
            }
        }
#endif

        if (result) {
            auto resp = std::move(result).value();
            if (req.pathsOnly && resp.paths.empty()) {
                spdlog::info("[SearchService] invoking paths fallback for empty pathsOnly result");
                co_await ensurePathsFallbackAsync(normalizedReq, resp, &metadataTelemetry);
            }
            auto totalElapsed = duration_cast<milliseconds>(steady_clock::now() - t0).count();
            resp.executionTimeMs = static_cast<int64_t>(totalElapsed);
            resp.searchStats["latency_ms"] = std::to_string(totalElapsed);
            resp.searchStats["metadata_operations"] =
                std::to_string(metadataTelemetry.operations.load(std::memory_order_relaxed));
            resp.searchStats["metadata_retries"] =
                std::to_string(metadataTelemetry.retries.load(std::memory_order_relaxed));
            resp.searchStats["metadata_transient_failures"] =
                std::to_string(metadataTelemetry.transientFailures.load(std::memory_order_relaxed));
            result = Result<SearchResponse>(std::move(resp));
        }

        if (result) {
            auto r = std::move(result).value();
            maybeEmitJson(req, r);
            co_return Result<SearchResponse>(std::move(r));
        }
        co_return result;
    }

    boost::asio::awaitable<Result<void>> lightIndexForHash_impl(const std::string& hash,
                                                                std::size_t maxBytes) {
        try {
            if (!ctx_.metadataRepo) {
                co_return Error{ErrorCode::NotInitialized, "metadata repository not available"};
            }
            auto di = co_await retryMetadataOp(
                [&]() { return ctx_.metadataRepo->getDocumentByHash(hash); });
            if (!di) {
                co_return di.error();
            }
            if (!di.value().has_value()) {
                co_return Error{ErrorCode::NotFound, "document not found by hash"};
            }
            auto info = *di.value();

            // If already extracted, nothing to do
            if (info.contentExtracted &&
                info.extractionStatus == metadata::ExtractionStatus::Success) {
                co_return Result<void>();
            }

            // Only attempt lightweight extraction for text-like types or small files
            const std::string mime = info.mimeType;
            const std::string ext = info.fileExtension;
            const bool looksTextMime = (!mime.empty() && mime.rfind("text/", 0) == 0) ||
                                       mime == "application/json" || mime == "application/xml";
            const bool looksTextExt =
                (!ext.empty() &&
                 (ext == ".txt" || ext == ".md" || ext == ".json" || ext == ".csv" ||
                  ext == ".log" || ext == ".xml" || ext == ".yaml" || ext == ".yml" ||
                  ext == ".toml" || ext == ".html" || ext == ".htm"));

            if (!looksTextMime && !looksTextExt) {
                // Skip non-text types here; daemon/background can handle richer extraction if
                // needed.
                co_return Result<void>();
            }

            if (!ctx_.store) {
                co_return Error{ErrorCode::NotInitialized, "content store not available"};
            }

            // Guard: avoid loading very large files in memory for the light path
            if (info.fileSize > 0 && static_cast<std::size_t>(info.fileSize) > maxBytes) {
                spdlog::debug("LightIndex: skipping large file {} (size={} > cap={})",
                              info.fileName, info.fileSize, maxBytes);
                co_return Result<void>();
            }

            auto bytesRes = ctx_.store->retrieveBytes(hash);
            if (!bytesRes) {
                co_return bytesRes.error();
            }
            auto& buf = bytesRes.value();
            if (buf.empty()) {
                co_return Result<void>();
            }
            if (buf.size() > maxBytes) {
                spdlog::debug("LightIndex: trimming content {} from {} to {} bytes", info.fileName,
                              buf.size(), maxBytes);
            }
            const std::size_t n = std::min<std::size_t>(buf.size(), maxBytes);
            std::string text;
            text.reserve(n);
            for (std::size_t i = 0; i < n; ++i) {
                text.push_back(static_cast<char>(buf[i]));
            }

            // Very light HTML handling: strip tags naively when extension suggests HTML
            if (ext == ".html" || ext == ".htm" || mime == "text/html") {
                std::string out;
                out.reserve(text.size());
                bool intag = false;
                for (char c : text) {
                    if (c == '<') {
                        intag = true;
                        continue;
                    }
                    if (c == '>') {
                        intag = false;
                        continue;
                    }
                    if (!intag)
                        out.push_back(c);
                }
                text.swap(out);
            }

            // Index into FTS5 and fuzzy index
            (void)co_await retryMetadataOp([&]() {
                return ctx_.metadataRepo->indexDocumentContent(info.id, info.fileName, text,
                                                               mime.empty() ? "text/plain" : mime);
            });
            (void)co_await retryMetadataOp(
                [&]() { return ctx_.metadataRepo->updateFuzzyIndex(info.id); });

            // Mark extraction success for this light path
            auto d =
                co_await retryMetadataOp([&]() { return ctx_.metadataRepo->getDocument(info.id); });
            if (d && d.value().has_value()) {
                auto updated = *d.value();
                updated.contentExtracted = true;
                updated.extractionStatus = metadata::ExtractionStatus::Success;
                (void)co_await retryMetadataOp(
                    [&]() { return ctx_.metadataRepo->updateDocument(updated); });
            }

            co_return Result<void>();
        } catch (const std::exception& e) {
            co_return Error{ErrorCode::InternalError, e.what()};
        }
    }

    Result<void> lightIndexForHash(const std::string& hash,
                                   std::size_t maxBytes = 2 * 1024 * 1024) override {
        if (!ctx_.workerExecutor) {
            return Error{ErrorCode::NotInitialized, "Worker executor not available"};
        }

        // Since this is a fire-and-forget, we can spawn and detach.
        boost::asio::co_spawn(
            ctx_.workerExecutor,
            [this, hash, maxBytes]() -> boost::asio::awaitable<void> {
                auto result = co_await lightIndexForHash_impl(hash, maxBytes);
                if (!result) {
                    spdlog::warn("lightIndexForHash failed: {}", result.error().message);
                }
            },
            boost::asio::detached);

        return Result<void>();
    }

private:
    AppContext ctx_;
    bool degraded_{false};
    std::string repairDetails_{};
    EnhancedConfig enhancedCfg_{}; // off by default
    EnhancedSearchExecutor enhanced_{};
    std::shared_ptr<yams::search::HotzoneManager> hotzones_{};

    boost::asio::awaitable<void> hydrateSnippetsAsync(const SearchRequest& req,
                                                      SearchResponse& resp,
                                                      MetadataTelemetry* telemetry = nullptr) {
        if (!ctx_.metadataRepo || !ctx_.workerExecutor) {
            co_return;
        }

        std::vector<size_t> todo;
        todo.reserve(resp.results.size());
        for (size_t i = 0; i < resp.results.size(); ++i) {
            const auto& it = resp.results[i];
            if (it.snippet.empty() && (!it.hash.empty() || !it.path.empty())) {
                todo.push_back(i);
            }
        }

        if (todo.empty()) {
            co_return;
        }

        for (size_t i = 0; i < todo.size(); ++i) {
            size_t idx = todo[i];
            auto& out = resp.results[idx];
            std::string hash = out.hash;
            int64_t docId = -1;

            if (!hash.empty()) {
                auto d = co_await retryMetadataOp(
                    [&]() { return ctx_.metadataRepo->getDocumentByHash(hash); }, 4,
                    std::chrono::milliseconds(25), telemetry);
                if (d && d.value().has_value())
                    docId = d.value()->id;
            } else if (!out.path.empty()) {
                auto v = co_await retryMetadataOp(
                    [&]() {
                        return metadata::queryDocumentsByPattern(*ctx_.metadataRepo, out.path);
                    },
                    4, std::chrono::milliseconds(25), telemetry);
                if (v && !v.value().empty())
                    docId = v.value().front().id;
            }

            if (docId >= 0) {
                auto contentResult =
                    co_await retryMetadataOp([&]() { return ctx_.metadataRepo->getContent(docId); },
                                             4, std::chrono::milliseconds(25), telemetry);
                if (contentResult && contentResult.value().has_value()) {
                    const auto& content = contentResult.value().value();
                    if (!content.contentText.empty()) {
                        out.snippet = utils::createSnippet(content.contentText, 200, true);
                    }
                }
            }
        }
    }

    boost::asio::awaitable<void>
    hydrateSnippetsAsync_worker(const SearchRequest& req, SearchResponse& resp,
                                MetadataTelemetry* telemetry = nullptr) {
        if (!ctx_.metadataRepo || !ctx_.workerExecutor)
            co_return;

        std::vector<size_t> todo;
        for (size_t i = 0; i < resp.results.size(); ++i) {
            if (resp.results[i].snippet.empty() &&
                (!resp.results[i].hash.empty() || !resp.results[i].path.empty())) {
                todo.push_back(i);
            }
        }
        if (todo.empty())
            co_return;

        auto outstanding = std::make_shared<std::atomic<size_t>>(todo.size());
        auto timer =
            std::make_shared<boost::asio::steady_timer>(co_await boost::asio::this_coro::executor);

        for (size_t i = 0; i < todo.size(); ++i) {
            boost::asio::co_spawn(
                ctx_.workerExecutor,
                [&, idx = todo[i], outstanding, timer,
                 telemetry]() -> boost::asio::awaitable<void> {
                    auto& out = resp.results[idx];
                    int64_t docId = -1;
                    if (!out.hash.empty()) {
                        if (auto d = co_await retryMetadataOp(
                                [&]() { return ctx_.metadataRepo->getDocumentByHash(out.hash); }, 4,
                                std::chrono::milliseconds(25), telemetry);
                            d && d.value())
                            docId = d.value()->id;
                    } else if (!out.path.empty()) {
                        if (auto v = co_await retryMetadataOp(
                                [&]() {
                                    return metadata::queryDocumentsByPattern(*ctx_.metadataRepo,
                                                                             out.path);
                                },
                                4, std::chrono::milliseconds(25), telemetry);
                            v && !v.value().empty())
                            docId = v.value().front().id;
                    }
                    if (docId >= 0) {
                        if (auto contentResult = co_await retryMetadataOp(
                                [&]() { return ctx_.metadataRepo->getContent(docId); }, 4,
                                std::chrono::milliseconds(25), telemetry);
                            contentResult && contentResult.value()) {
                            if (!contentResult.value()->contentText.empty()) {
                                out.snippet = utils::createSnippet(
                                    contentResult.value()->contentText, 200, true);
                            }
                        }
                    }

                    if (--(*outstanding) == 0) {
                        timer->cancel();
                    }
                    co_return;
                },
                boost::asio::detached);
        }

        std::chrono::milliseconds snippetBudget{req.snippetHydrationTimeoutMs};
        if (snippetBudget.count() > 0) {
            timer->expires_after(snippetBudget);
        } else {
            timer->expires_at(std::chrono::steady_clock::time_point::max());
        }

        boost::system::error_code waitEc;
        co_await timer->async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, waitEc));
        if (waitEc && waitEc != boost::asio::error::operation_aborted) {
            throw boost::system::system_error(waitEc);
        }
        if (!waitEc && snippetBudget.count() > 0) {
            resp.searchStats["snippet_timeout_hit"] = "true";
            resp.searchStats["snippet_budget_ms"] = std::to_string(snippetBudget.count());
        }
    }

    boost::asio::awaitable<void> ensurePathsFallbackAsync(const SearchRequest& req,
                                                          SearchResponse& resp,
                                                          MetadataTelemetry* telemetry = nullptr) {
        if (!ctx_.metadataRepo)
            co_return;
        auto allDocsResult = co_await retryMetadataOp(
            [&]() { return metadata::queryDocumentsByPattern(*ctx_.metadataRepo, "%"); }, 4,
            std::chrono::milliseconds(25), telemetry);
        if (!allDocsResult) {
            spdlog::warn(
                "[SearchService] ensurePathsFallback failed to enumerate paths: code={} message={}",
                static_cast<int>(allDocsResult.error().code), allDocsResult.error().message);
            co_return;
        }
        const auto& docs = allDocsResult.value();
        spdlog::info("[SearchService] ensurePathsFallback scanning {} docs for query '{}'",
                     docs.size(), req.query);

        std::vector<std::string> foundPaths;
        std::vector<std::string> fallbackPaths;
        const std::size_t limit = req.limit == 0 ? std::numeric_limits<std::size_t>::max()
                                                 : static_cast<std::size_t>(req.limit);
        fallbackPaths.reserve(std::min<std::size_t>(docs.size(), limit));

        for (const auto& doc : docs) {
            const bool matches = req.query.empty() ||
                                 doc.filePath.find(req.query) != std::string::npos ||
                                 doc.fileName.find(req.query) != std::string::npos;
            std::string path = !doc.filePath.empty() ? doc.filePath : doc.fileName;
            if (fallbackPaths.size() < limit) {
                fallbackPaths.push_back(path);
            }
            if (matches && foundPaths.size() < limit) {
                foundPaths.push_back(std::move(path));
            }
            if (foundPaths.size() >= limit)
                break;
        }

        if (!foundPaths.empty()) {
            spdlog::info("[SearchService] ensurePathsFallback matched {} paths", foundPaths.size());
            resp.paths = std::move(foundPaths);
        } else {
            spdlog::info(
                "[SearchService] ensurePathsFallback using fallback paths count={} limit={}",
                fallbackPaths.size(), req.limit);
            resp.paths = std::move(fallbackPaths);
        }
    }

    boost::asio::awaitable<Result<SearchResponse>>
    pathSearch(const SearchRequest& req, MetadataTelemetry* telemetry = nullptr) {
        static auto globToSqlLike = [](const std::string& glob) {
            std::string like;
            like.reserve(glob.size());
            for (char c : glob) {
                if (c == '*') {
                    like += '%';
                } else if (c == '?') {
                    like += '_';
                } else {
                    like += c;
                }
            }
            return like;
        };

        if (!ctx_.metadataRepo) {
            co_return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }
        SearchResponse resp;
        resp.type = "path";
        resp.usedHybrid = false;
        std::vector<metadata::DocumentInfo> docs;

        const bool wildcard = hasWildcard(req.query);

        std::string likePattern;
        if (wildcard) {
            likePattern = globToSqlLike(req.query);
        } else {
            likePattern = "%" + req.query + "%";
        }

        auto r = co_await retryMetadataOp(
            [&]() { return metadata::queryDocumentsByPattern(*ctx_.metadataRepo, likePattern); }, 4,
            std::chrono::milliseconds(25), telemetry);

        if (r) {
            docs = std::move(r.value());
        } else {
            co_return r.error();
        }

        // Apply additional filters and shape results
        auto push_path = [&](const metadata::DocumentInfo& d) -> boost::asio::awaitable<void> {
            if (!req.extension.empty()) {
                if (d.fileExtension != req.extension && d.fileExtension != ("." + req.extension))
                    co_return;
            }
            if (!req.mimeType.empty() && d.mimeType != req.mimeType)
                co_return;
            if (!(co_await metadataHasTags(ctx_.metadataRepo.get(), d.id, req.tags,
                                           req.matchAllTags, telemetry)))
                co_return;
            if (req.pathsOnly) {
                resp.paths.push_back(!d.filePath.empty() ? d.filePath : d.fileName);
            } else {
                SearchItem it;
                it.id = d.id;
                it.hash = req.showHash ? d.sha256Hash : "";
                it.title = d.fileName;
                it.path = d.filePath;
                it.score = 1.0; // neutral score for path matches
                resp.results.push_back(std::move(it));
            }
        };
        for (const auto& d : docs) {
            co_await push_path(d);
            if (req.limit != 0) {
                if (req.pathsOnly && resp.paths.size() >= req.limit)
                    break;
                if (!req.pathsOnly && resp.results.size() >= req.limit)
                    break;
            }
        }
        if (req.pathsOnly)
            resp.total = resp.paths.size();
        else
            resp.total = resp.results.size();
        co_return resp;
    }

    template <typename T> void setExecTime(Result<T>& r, std::chrono::steady_clock::time_point t0) {
        using namespace std::chrono;
        (void)t0; // Suppress unused parameter warning - only specialized version uses it
        (void)r;  // Suppress unused parameter warning
        // Generic template doesn't use parameters, only specialized version does
    }

    // Overload specifically to set SearchResponse.executionTimeMs
    void setExecTime(Result<SearchResponse>& r, std::chrono::steady_clock::time_point t0) {
        using namespace std::chrono;
        if (r) {
            auto dur = duration_cast<milliseconds>(steady_clock::now() - t0).count();
            // Extract value, modify, and reconstruct Result
            auto resp = std::move(r).value();
            resp.executionTimeMs = static_cast<int64_t>(dur);
            r = Result<SearchResponse>(std::move(resp));
        }
    }

    boost::asio::awaitable<Result<SearchResponse>>
    searchByHashPrefix(const SearchRequest& req, MetadataTelemetry* telemetry = nullptr) {
        const std::string& rawPrefix = !req.hash.empty() ? req.hash : req.query;
        std::string prefix = rawPrefix;
        std::transform(prefix.begin(), prefix.end(), prefix.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        SearchResponse resp;
        resp.type = "hash";
        resp.usedHybrid = false;

        const std::size_t fetchLimit = std::max<std::size_t>(req.limit * 4, 32);
        auto docsResult = co_await retryMetadataOp(
            [&]() { return ctx_.metadataRepo->findDocumentsByHashPrefix(prefix, fetchLimit); }, 4,
            std::chrono::milliseconds(25), telemetry);
        if (!docsResult) {
            co_return Error{ErrorCode::InternalError,
                            "Failed to enumerate documents for hash search: " +
                                docsResult.error().message};
        }

        const auto& docs = docsResult.value();
        for (const auto& doc : docs) {
            // Optional path and tag filters for CLI parity
            bool pathOk = true;
            if (!req.pathPattern.empty()) {
                if (hasWildcard(req.pathPattern)) {
                    std::string pattern = req.pathPattern;
                    if (!pattern.empty() && pattern.front() != '*' && pattern.front() != '/' &&
                        pattern.find(":/") == std::string::npos) {
                        pattern = "*" + pattern;
                    }
                    pathOk = wildcardMatch(doc.filePath, pattern);
                } else {
                    pathOk = doc.filePath.find(req.pathPattern) != std::string::npos;
                }
            }

            bool metaFiltersOk = true;
            if (!req.extension.empty()) {
                if (doc.fileExtension != req.extension &&
                    doc.fileExtension != ("." + req.extension)) {
                    metaFiltersOk = false;
                }
            }
            if (!req.mimeType.empty() && doc.mimeType != req.mimeType) {
                metaFiltersOk = false;
            }
            if (!req.fileType.empty()) {
                auto classified = utils::classifyFileType(doc.mimeType, doc.fileExtension);
                if (classified != req.fileType) {
                    metaFiltersOk = false;
                }
            }

            if (!pathOk || !metaFiltersOk ||
                !(co_await metadataHasTags(ctx_.metadataRepo.get(), doc.id, req.tags,
                                           req.matchAllTags, telemetry))) {
                continue;
            }

            if (req.pathsOnly) {
                resp.paths.push_back(doc.filePath);
            } else {
                SearchItem it;
                it.id = doc.id;
                it.hash = req.showHash ? doc.sha256Hash : "";
                it.title = doc.fileName;
                it.path = doc.filePath;
                it.score = 1.0;
                it.snippet = "";
                it.mimeType = doc.mimeType;
                it.fileType = utils::classifyFileType(doc.mimeType, doc.fileExtension);
                it.size = static_cast<std::uint64_t>(doc.fileSize);
                it.created = doc.createdTime.time_since_epoch().count();
                it.modified = doc.modifiedTime.time_since_epoch().count();
                it.indexed = doc.indexedTime.time_since_epoch().count();
                if (req.showHash) {
                    it.metadata["hash"] = doc.sha256Hash;
                }
                it.metadata["indexed"] = std::to_string(doc.indexedTime.time_since_epoch().count());
                it.metadata["path"] = doc.filePath;
                resp.results.push_back(std::move(it));
            }

            if ((req.pathsOnly ? resp.paths.size() : resp.results.size()) >= req.limit)
                break;
        }

        resp.total = req.pathsOnly ? resp.paths.size() : resp.results.size();
        resp.wasHashSearch = true;
        resp.detectedHashQuery = rawPrefix;
        co_return resp;
    }

    Result<SearchResponse> hybridSearch(const SearchRequest& req,
                                        const yams::search::ExtractScope& scope,
                                        MetadataTelemetry* telemetry,
                                        const std::string& pathPattern) {
        // Expect ctx_.hybridEngine->search(query, limit) returning Result<vector<...>>
        // Shape inferred from existing MCP code: each result has:
        //  - id
        //  - metadata map with "title" and "path"
        //  - hybrid_score, vector_score, keyword_score, kg_entity_score, structural_score
        //  - content snippet (optional)
        yams::vector::SearchFilter filter;
        if (!scope.name.empty()) {
            filter.metadata_filters["name"] = scope.name;
        }
        if (!pathPattern.empty()) {
            filter.metadata_filters["path"] = pathPattern;
        }
        if (!scope.ext.empty()) {
            std::string ext = scope.ext;
            std::transform(ext.begin(), ext.end(), ext.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (!ext.empty() && ext.front() == '.')
                ext.erase(ext.begin());
            filter.metadata_filters["extension"] = ext;
        }
        if (!scope.mime.empty()) {
            filter.metadata_filters["mime_type"] = scope.mime;
        }
        yams::search::SearchStageBudgets stageBudgets{};
        bool vectorTimedOut = false;
        bool keywordTimedOut = false;
        stageBudgets.vector_timed_out = &vectorTimedOut;
        stageBudgets.keyword_timed_out = &keywordTimedOut;

        bool budgetsActive = false;
        const auto engineConfig = ctx_.hybridEngine->getConfig();
        auto applyBudget = [&](int requestMs, std::chrono::milliseconds configValue,
                               std::optional<std::chrono::milliseconds>& target) {
            if (requestMs > 0) {
                target = std::chrono::milliseconds(requestMs);
                budgetsActive = true;
            } else if (configValue.count() > 0) {
                target = configValue;
                budgetsActive = true;
            }
        };

        applyBudget(req.vectorStageTimeoutMs, engineConfig.vector_timeout_ms,
                    stageBudgets.vector_timeout);
        applyBudget(req.keywordStageTimeoutMs, engineConfig.keyword_timeout_ms,
                    stageBudgets.keyword_timeout);

        auto hres = ctx_.hybridEngine->search(req.query, req.limit, filter,
                                              budgetsActive ? &stageBudgets : nullptr);
        if (!hres) {
            return Error{ErrorCode::InternalError, "Hybrid search failed: " + hres.error().message};
        }

        const auto& vec = hres.value();

        SearchResponse resp;
        resp.type = "hybrid";
        resp.usedHybrid = true;

        if (budgetsActive) {
            if (stageBudgets.vector_timeout.has_value()) {
                resp.searchStats["vector_budget_ms"] =
                    std::to_string(stageBudgets.vector_timeout->count());
            }
            if (stageBudgets.keyword_timeout.has_value()) {
                resp.searchStats["keyword_budget_ms"] =
                    std::to_string(stageBudgets.keyword_timeout->count());
            }
        }
        if (vectorTimedOut) {
            resp.searchStats["vector_timeout_hit"] = "true";
        }
        if (keywordTimedOut) {
            resp.searchStats["keyword_timeout_hit"] = "true";
        }

        if (req.pathsOnly) {
            for (const auto& r : vec) {
                auto itPath = r.metadata.find("path");
                if (itPath != r.metadata.end()) {
                    resp.paths.push_back(itPath->second);
                }
            }
            resp.total = resp.paths.size();
            return resp;
        }

        {
            const size_t n = vec.size();
            const size_t workers = recommendedWorkers(n);
            std::atomic<size_t> next{0};
            std::vector<std::optional<SearchItem>> slots(n);
            auto worker = [&]() {
                while (true) {
                    const size_t i = next.fetch_add(1);
                    if (i >= n)
                        break;
                    const auto& r = vec[i];
                    SearchItem it;
                    it.id = static_cast<int64_t>(i + 1);
                    if (auto itTitle = r.metadata.find("title"); itTitle != r.metadata.end())
                        it.title = itTitle->second;
                    if (auto itPath = r.metadata.find("path"); itPath != r.metadata.end())
                        it.path = itPath->second;
                    it.score = static_cast<double>(r.hybrid_score);
                    if (!r.content.empty())
                        it.snippet = r.content;
                    if (req.verbose) {
                        it.vectorScore = r.vector_score;
                        it.keywordScore = r.keyword_score;
                        it.kgEntityScore = r.kg_entity_score;
                        it.structuralScore = r.structural_score;
                    }
                    slots[i] = std::move(it);
                }
            };
            std::vector<std::thread> ths;
            ths.reserve(workers);
            for (size_t t = 0; t < workers; ++t)
                ths.emplace_back(worker);
            for (auto& th : ths)
                th.join();
            resp.results.reserve(n);
            for (size_t i = 0; i < n; ++i)
                if (slots[i].has_value())
                    resp.results.push_back(std::move(*slots[i]));
        }

        resp.total = resp.results.size();
        return resp;
    }

    Result<SearchResponse> metadataSearch(const SearchRequest& req,
                                          MetadataTelemetry* telemetry = nullptr) {
        // Prepare query - escape regex if literalText is requested
        std::string processedQuery = req.query;
        if (req.literalText) {
            // Escape regex special characters
            processedQuery = escapeRegex(req.query);
        }

        // Get docIds for tags if provided
        std::optional<std::vector<int64_t>> docIds;
        if (!req.tags.empty()) {
            auto docsResult = ctx_.metadataRepo->findDocumentsByTags(req.tags, req.matchAllTags);
            if (docsResult) {
                std::vector<int64_t> ids;
                const auto& docsVec = docsResult.value();
                ids.reserve(docsVec.size());
                for (const auto& doc : docsVec) {
                    ids.push_back(doc.id);
                }
                docIds = std::move(ids);
            }
        }

        auto convertResults = [&](const metadata::SearchResults& metaResults) {
            std::vector<SearchItem> serviceResults;
            serviceResults.reserve(metaResults.results.size());
            for (const auto& item : metaResults.results) {
                SearchItem it;
                it.id = item.document.id;
                it.hash = req.showHash ? item.document.sha256Hash : "";
                it.title = item.document.fileName;
                it.path = item.document.filePath;
                it.score = item.score;
                it.snippet = item.snippet;
                serviceResults.push_back(std::move(it));
            }
            return serviceResults;
        };

        // Fuzzy or full-text via metadata repository
        if (req.fuzzy) {
            auto r = ctx_.metadataRepo->fuzzySearch(processedQuery, req.similarity,
                                                    static_cast<int>(req.limit), docIds);
            if (!r) {
                return Error{ErrorCode::InternalError, "Fuzzy search failed: " + r.error().message};
            }

            const auto& res = r.value();

            SearchResponse resp;
            resp.total = res.totalCount;
            resp.type = "fuzzy";
            resp.executionTimeMs = res.executionTimeMs;
            resp.usedHybrid = false;

            if (req.pathsOnly) {
                for (const auto& item : res.results) {
                    const auto& d = item.document;
                    resp.paths.push_back(!d.filePath.empty() ? d.filePath : d.fileName);
                }
                resp.total = resp.paths.size();
                return resp;
            }

            resp.results = convertResults(res);
            normalizeScores(resp.results, resp.type);
            return resp;
        } else {
            auto r =
                ctx_.metadataRepo->search(processedQuery, static_cast<int>(req.limit), 0, docIds);
            if (!r) {
                return Error{ErrorCode::InternalError,
                             "Full-text search failed: " + r.error().message};
            }

            const auto& res = r.value();
            if (!req.fuzzy && res.totalCount == 0) {
                SearchRequest req2 = req;
                req2.fuzzy = true;
                return metadataSearch(req2, telemetry);
            }
            SearchResponse resp;
            resp.total = res.totalCount;
            resp.type = "full-text";
            resp.executionTimeMs = res.executionTimeMs;
            resp.usedHybrid = false;

            if (req.pathsOnly) {
                for (const auto& item : res.results) {
                    const auto& d = item.document;
                    resp.paths.push_back(!d.filePath.empty() ? d.filePath : d.fileName);
                }
                resp.total = resp.paths.size();
                return resp;
            }

            resp.results = convertResults(res);
            normalizeScores(resp.results, resp.type);
            return resp;
        }
    }
};

std::shared_ptr<ISearchService> makeSearchService(const AppContext& ctx) {
    return std::make_shared<SearchServiceImpl>(ctx);
}

} // namespace yams::app::services
