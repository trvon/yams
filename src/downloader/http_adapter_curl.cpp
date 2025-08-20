/*
 * http_adapter_curl.cpp
 *
 * Notes
 * - Provides minimal probe() and fetchRange() implementations using libcurl easy API.
 * - Honors timeout, TLS verify/CA, proxy, headers, redirects, and Range.
 * - Emits basic progress callbacks and supports cooperative cancellation.
 * - Designed as a stub/MVP: no parallel ranges here; higher-level manager will orchestrate.
 *
 * Build
 * - Linked via yams::curl (alias of CURL::libcurl).
 * - Depends on spdlog for logging.
 */

#include <yams/downloader/downloader.hpp>

#include <spdlog/spdlog.h>
#include <curl/curl.h>

#include <algorithm>
#include <cctype>
#include <string_view>

namespace yams::downloader {

// Local helper: lowercase copy
static std::string to_lower(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (unsigned char c : s)
        out.push_back(static_cast<char>(std::tolower(c)));
    return out;
}

// Local helper: trim whitespace
static std::string trim(std::string_view s) {
    size_t b = 0;
    size_t e = s.size();
    while (b < e && std::isspace(static_cast<unsigned char>(s[b])))
        ++b;
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1])))
        --e;
    return std::string{s.substr(b, e - b)};
}

// Map CURLcode to Error
static Error makeCurlError(CURLcode code, std::string_view where) {
    Error err;
    err.message = std::string(where) + ": " + curl_easy_strerror(code);
    switch (code) {
        case CURLE_OK:
            err.code = ErrorCode::None;
            break;
        case CURLE_OPERATION_TIMEDOUT:
            err.code = ErrorCode::Timeout;
            break;
        case CURLE_SSL_CONNECT_ERROR:
        case CURLE_PEER_FAILED_VERIFICATION:
        /* CURLE_SSL_CACERT is an alias of CURLE_PEER_FAILED_VERIFICATION in newer libcurl */
        /* case CURLE_SSL_CACERT: intentionally omitted to avoid duplicate case */
        case CURLE_SSL_CACERT_BADFILE:
        case CURLE_SSL_CERTPROBLEM:
        case CURLE_SSL_CIPHER:
        case CURLE_SSL_ISSUER_ERROR:
            err.code = ErrorCode::TlsVerificationFailed;
            break;
        case CURLE_COULDNT_RESOLVE_HOST:
        case CURLE_COULDNT_CONNECT:
        case CURLE_RECV_ERROR:
        case CURLE_SEND_ERROR:
        case CURLE_GOT_NOTHING:
            err.code = ErrorCode::NetworkError;
            break;
        default:
            err.code = ErrorCode::Unknown;
            break;
    }
    return err;
}

// Case-insensitive starts_with
static bool istarts_with(std::string_view s, std::string_view prefix) {
    if (s.size() < prefix.size())
        return false;
    for (size_t i = 0; i < prefix.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(s[i])) !=
            std::tolower(static_cast<unsigned char>(prefix[i]))) {
            return false;
        }
    }
    return true;
}

// Header parser context
struct HeaderParseContext {
    bool acceptRangesBytes{false};
    std::optional<std::uint64_t> contentLength{};
    std::optional<std::string> etag;
    std::optional<std::string> lastModified;
};

// CURL header callback
static size_t header_cb(char* buffer, size_t size, size_t nitems, void* userdata) {
    const size_t total = size * nitems;
    if (total == 0 || userdata == nullptr)
        return 0;

    auto* ctx = static_cast<HeaderParseContext*>(userdata);
    std::string_view line(buffer, total);

    // Strip CRLF
    if (!line.empty() && (line.back() == '\r' || line.back() == '\n')) {
        while (!line.empty() && (line.back() == '\r' || line.back() == '\n')) {
            line.remove_suffix(1);
        }
    }

    // We expect "Key: Value"
    auto colon = line.find(':');
    if (colon == std::string_view::npos)
        return total;

    auto key = to_lower(trim(line.substr(0, colon)));
    auto val = trim(line.substr(colon + 1));

    if (key == "accept-ranges") {
        if (to_lower(val) == "bytes") {
            ctx->acceptRangesBytes = true;
        }
    } else if (key == "content-length") {
        // Parse uint64 content length
        std::uint64_t tmp{0};
        auto sv = trim(val);
        auto sv_view = std::string_view(sv);
        const char* first = sv_view.data();
        const char* last = sv_view.data() + sv_view.size();
        auto res = std::from_chars(first, last, tmp);
        if (res.ec == std::errc()) {
            ctx->contentLength = tmp;
        }
    } else if (key == "etag") {
        // Strip surrounding quotes if present
        auto v = trim(val);
        if (v.size() >= 2 &&
            ((v.front() == '"' && v.back() == '"') || (v.front() == '\'' && v.back() == '\''))) {
            v = v.substr(1, v.size() - 2);
        }
        ctx->etag = std::move(v);
    } else if (key == "last-modified") {
        ctx->lastModified = trim(val);
    }

    return total;
}

// Write sink context for fetchRange
struct WriteContext {
    std::function<Expected<void>(std::span<const std::byte>)> sink;
    ProgressCallback onProgress;
    ShouldCancel shouldCancel;
    std::uint64_t downloaded{0};
    std::optional<std::uint64_t> total{};
    bool cancelRequested{false};
};

// CURL write callback
static size_t write_cb(char* ptr, size_t size, size_t nmemb, void* userdata) {
    const size_t total = size * nmemb;
    if (userdata == nullptr)
        return 0;

    auto* ctx = static_cast<WriteContext*>(userdata);
    if (total == 0)
        return 0;

    if (ctx->shouldCancel && ctx->shouldCancel()) {
        ctx->cancelRequested = true;
        return 0; // signal error to curl => CURLE_WRITE_ERROR
    }

    std::span<const std::byte> bytes{reinterpret_cast<const std::byte*>(ptr), total};
    auto r = ctx->sink ? ctx->sink(bytes)
                       : Expected<void>{Error{ErrorCode::IoError, "No sink provided"}};
    if (!r.ok()) {
        // Abort transfer
        return 0;
    }

    ctx->downloaded += static_cast<std::uint64_t>(total);
    if (ctx->onProgress) {
        ProgressEvent ev;
        ev.url = ""; // filled by caller if desired
        ev.downloadedBytes = ctx->downloaded;
        ev.totalBytes = ctx->total;
        if (ctx->total && *ctx->total > 0) {
            ev.percentage =
                static_cast<float>((static_cast<long double>(ctx->downloaded) * 100.0L) /
                                   static_cast<long double>(*ctx->total));
        }
        ev.stage = ProgressStage::Downloading;
        ctx->onProgress(ev);
    }

    return total;
}

// Helper to build curl_slist from headers
static curl_slist* build_header_list(const std::vector<Header>& headers) {
    curl_slist* list = nullptr;
    for (const auto& h : headers) {
        std::string line = h.name;
        line.append(": ");
        line.append(h.value);
        list = curl_slist_append(list, line.c_str());
    }
    return list;
}

// Common CURL easy handle configuration
static void configure_common(CURL* curl, std::chrono::milliseconds timeout, const TlsConfig& tls,
                             const std::optional<std::string>& proxy, bool followRedirects) {
    // Timeouts
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, static_cast<long>(timeout.count()));
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS,
                     static_cast<long>(std::min<long>(timeout.count(), 30000)));

    // Redirects
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, followRedirects ? 1L : 0L);
    curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 5L);

    // TLS
    curl_easy_setopt(curl, CURLOPT_USE_SSL, CURLUSESSL_ALL);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, tls.insecure ? 0L : 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, tls.insecure ? 0L : 2L);
    if (!tls.caPath.empty()) {
        curl_easy_setopt(curl, CURLOPT_CAINFO, tls.caPath.c_str());
    }

    // Proxy
    if (proxy && !proxy->empty()) {
        curl_easy_setopt(curl, CURLOPT_PROXY, proxy->c_str());
    }

    // Robustness
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 30L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 15L);
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
}

class CurlHttpAdapter final : public IHttpAdapter {
public:
    CurlHttpAdapter() = default;
    ~CurlHttpAdapter() override = default;

    Expected<void> probe(std::string_view url, const std::vector<Header>& headers,
                         /*out*/ bool& resumeSupported,
                         /*out*/ std::optional<std::uint64_t>& contentLength,
                         /*out*/ std::optional<std::string>& etag,
                         /*out*/ std::optional<std::string>& lastModified, const TlsConfig& tls,
                         const std::optional<std::string>& proxy,
                         std::chrono::milliseconds timeout) override {
        resumeSupported = false;
        contentLength.reset();

        CURL* curl = curl_easy_init();
        if (!curl) {
            return Error{ErrorCode::Unknown, "curl_easy_init failed"};
        }

        auto* list = build_header_list(headers);
        HeaderParseContext hctx{};

        curl_easy_setopt(curl, CURLOPT_URL, std::string(url).c_str());
        curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 0L);
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, header_cb);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, &hctx);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, list);

        configure_common(curl, timeout, tls, proxy, /*followRedirects=*/true);

        CURLcode rc = curl_easy_perform(curl);
        if (rc != CURLE_OK) {
            // Some servers reject HEAD; try GET range 0-0 as fallback
            spdlog::debug("HEAD probe failed ({}), attempting GET Range 0-0",
                          curl_easy_strerror(rc));
            curl_easy_setopt(curl, CURLOPT_NOBODY, 0L);
            curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);

            struct curl_slist* range = curl_slist_append(nullptr, "Range: bytes=0-0");
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, range);
            rc = curl_easy_perform(curl);
            if (rc == CURLE_OK) {
                long http_status = 0;
                curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_status);
                // If 206, Range works; if 200, server ignored Range.
                if (http_status == 206) {
                    hctx.acceptRangesBytes = true;
                }
            }
            if (range)
                curl_slist_free_all(range);
            if (list)
                curl_slist_free_all(list);
            curl_easy_cleanup(curl);

            if (rc != CURLE_OK) {
                return makeCurlError(rc, "probe(GET range)");
            }
        } else {
            if (list)
                curl_slist_free_all(list);
            curl_easy_cleanup(curl);
        }

        resumeSupported = hctx.acceptRangesBytes;
        contentLength = hctx.contentLength;
        etag = hctx.etag;
        lastModified = hctx.lastModified;
        if (hctx.etag) {
            spdlog::debug("HTTP probe captured ETag: {}", *hctx.etag);
        }
        if (hctx.lastModified) {
            spdlog::debug("HTTP probe captured Last-Modified: {}", *hctx.lastModified);
        }
        etag = hctx.etag;
        lastModified = hctx.lastModified;
        return Expected<void>{}; // success (no error)
    }

    Expected<void> fetchRange(std::string_view url, const std::vector<Header>& headers,
                              std::uint64_t offset, std::uint64_t size, const TlsConfig& tls,
                              const std::optional<std::string>& proxy,
                              std::chrono::milliseconds timeout,
                              const std::function<Expected<void>(std::span<const std::byte>)>& sink,
                              const ShouldCancel& shouldCancel,
                              const ProgressCallback& onProgress) override {
        CURL* curl = curl_easy_init();
        if (!curl) {
            return Error{ErrorCode::Unknown, "curl_easy_init failed"};
        }

        // Build headers including Range
        curl_slist* list = build_header_list(headers);
        std::string rangeHeader;
        if (size == 0) {
            // Open-ended range from offset
            rangeHeader = "Range: bytes=" + std::to_string(offset) + "-";
        } else {
            const auto last = offset + size - 1;
            rangeHeader = "Range: bytes=" + std::to_string(offset) + "-" + std::to_string(last);
        }
        list = curl_slist_append(list, rangeHeader.c_str());

        // Set URL
        curl_easy_setopt(curl, CURLOPT_URL, std::string(url).c_str());

        // Method: GET
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);

        // Headers
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, list);

        // Progress context
        WriteContext wctx;
        wctx.sink = sink;
        wctx.onProgress = onProgress;
        wctx.shouldCancel = shouldCancel;

        // Set write callback
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &wctx);

        // Header capture for content length (to estimate total if possible)
        HeaderParseContext hctx{};
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, header_cb);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, &hctx);

        // Common config
        configure_common(curl, timeout, tls, proxy, /*followRedirects=*/true);

        // Initial progress (connecting)
        if (onProgress) {
            ProgressEvent ev;
            ev.url = std::string(url);
            ev.downloadedBytes = 0;
            ev.totalBytes = std::nullopt;
            ev.stage = ProgressStage::Connecting;
            onProgress(ev);
        }

        // Perform
        CURLcode rc = curl_easy_perform(curl);

        long http_status = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_status);

        if (list)
            curl_slist_free_all(list);
        curl_easy_cleanup(curl);

        if (wctx.cancelRequested) {
            return Error{ErrorCode::PolicyViolation, "Transfer cancelled by user"};
        }
        if (rc != CURLE_OK) {
            return makeCurlError(rc, "fetchRange(GET)");
        }
        if (http_status >= 400) {
            return Error{ErrorCode::ServerError, "HTTP error " + std::to_string(http_status)};
        }

        // Log captured validators (if any) and emit final progress
        if (hctx.etag) {
            spdlog::debug("HTTP fetch captured ETag: {}", *hctx.etag);
        }
        if (hctx.lastModified) {
            spdlog::debug("HTTP fetch captured Last-Modified: {}", *hctx.lastModified);
        }
        if (onProgress) {
            ProgressEvent ev;
            ev.url = std::string(url);
            ev.downloadedBytes = wctx.downloaded;
            ev.totalBytes = hctx.contentLength; // may be std::nullopt
            if (ev.totalBytes && *ev.totalBytes > 0) {
                ev.percentage =
                    static_cast<float>((static_cast<long double>(wctx.downloaded) * 100.0L) /
                                       static_cast<long double>(*ev.totalBytes));
            }
            ev.stage = ProgressStage::Finalizing;
            onProgress(ev);
        }

        return Expected<void>{}; // success
    }
};

/// Factory (optional): provide a way for higher layers to create a CURL adapter.
/// The higher level can also construct CurlHttpAdapter directly if header is visible.
std::unique_ptr<IHttpAdapter> makeCurlHttpAdapter() {
    return std::make_unique<CurlHttpAdapter>();
}

} // namespace yams::downloader
