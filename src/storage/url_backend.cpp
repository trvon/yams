#include <yams/storage/storage_backend.h>
#include <yams/storage/storage_engine.h>

#include <spdlog/spdlog.h>
#include <curl/curl.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <list>
#include <mutex>
#include <optional>
#include <random>
#include <regex>
#include <sstream>
#include <thread>
#include <unordered_map>

namespace yams::storage {

namespace {
constexpr long HTTP_NOT_FOUND = 404;
constexpr long HTTP_CLIENT_ERROR_LO = 400;
constexpr long HTTP_SUCCESS_LO = 200;
constexpr long HTTP_SUCCESS_HI = 299; // inclusive upper bound
} // namespace

// LRU cache for remote reads
class LRUCache {
public:
    struct CacheEntry {
        std::vector<std::byte> data;
        std::chrono::steady_clock::time_point timestamp;
        size_t size{0};
    };

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    explicit LRUCache(size_t maxSize, size_t ttlSeconds) : maxSize_(maxSize), ttl_(ttlSeconds) {}

    auto get(const std::string& key) -> std::optional<std::vector<std::byte>> {
        std::lock_guard<std::mutex> lock(mutex_);

        auto iter = cache_.find(key);
        if (iter == cache_.end()) {
            return std::nullopt;
        }

        // Check TTL
        auto now = std::chrono::steady_clock::now();
        auto age = std::chrono::duration_cast<std::chrono::seconds>(now - iter->second.timestamp);
        if (age.count() > static_cast<long>(ttl_)) {
            currentSize_ -= iter->second.size;
            cache_.erase(iter);
            return std::nullopt;
        }

        // Move to front (most recently used)
        accessOrder_.remove(key);
        accessOrder_.push_front(key);

        return iter->second.data;
    }

    void put(const std::string& key, const std::vector<std::byte>& data) {
        std::lock_guard<std::mutex> lock(mutex_);

        // Remove existing entry if present
        auto iter = cache_.find(key);
        if (iter != cache_.end()) {
            currentSize_ -= iter->second.size;
            accessOrder_.remove(key);
        }

        // Evict LRU entries if needed
        while (currentSize_ + data.size() > maxSize_ && !accessOrder_.empty()) {
            const auto& lruKey = accessOrder_.back();
            auto lruIt = cache_.find(lruKey);
            if (lruIt != cache_.end()) {
                currentSize_ -= lruIt->second.size;
                cache_.erase(lruIt);
            }
            accessOrder_.pop_back();
        }

        // Add new entry
        cache_[key] = CacheEntry{
            .data = data, .timestamp = std::chrono::steady_clock::now(), .size = data.size()};
        currentSize_ += data.size();
        accessOrder_.push_front(key);
    }

    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        cache_.clear();
        accessOrder_.clear();
        currentSize_ = 0;
    }

private:
    std::unordered_map<std::string, CacheEntry> cache_;
    std::list<std::string> accessOrder_;
    size_t maxSize_;
    size_t ttl_;
    size_t currentSize_{0};
    mutable std::mutex mutex_;
};

// Write callback for libcurl
static auto writeCallback(void* contents, size_t size, size_t nmemb, void* userp) -> size_t {
    auto* out = static_cast<std::vector<uint8_t>*>(userp);
    size_t totalSize = size * nmemb;
    auto* bytes = static_cast<uint8_t*>(contents);
    std::span<const uint8_t> src(bytes, totalSize);
    out->insert(out->end(), src.begin(), src.end());
    return totalSize;
}

// Read callback for libcurl uploads
struct ReadData {
    const std::byte* data;
    size_t size;
    size_t offset;
};

static auto readCallback(void* buffer, size_t size, size_t nmemb, void* userp) -> size_t {
    auto* readData = static_cast<ReadData*>(userp);
    size_t bufferSize = size * nmemb;
    std::span<const std::byte> src(readData->data, readData->size);
    auto remainingSpan = src.subspan(readData->offset);
    size_t toRead = std::min(bufferSize, remainingSpan.size());

    if (toRead > 0) {
        std::memcpy(buffer, remainingSpan.data(), toRead);
        readData->offset += toRead;
    }

    return toRead;
}

class URLBackend::Impl {
public:
    std::atomic<bool> healthy{true};
    std::atomic<uint64_t> errorCount{0};
    std::string lastError;

    // Retry helpers
    static auto isRetryable(const Error& err) -> bool {
        return err.code == ErrorCode::NetworkError;
    }

    void backoff(int attempt) const {
        int delay = static_cast<int>(config.baseRetryMs) * (1 << attempt);
        if (config.jitterMs > 0) {
            static thread_local std::mt19937 rng{std::random_device{}()};
            std::uniform_int_distribution<int> dist(0, static_cast<int>(config.jitterMs));
            delay += dist(rng);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    }

    struct HeadResult {
        long statusCode{0};
        std::unordered_map<std::string, std::string> headers;
    };

    static auto headerCallback(char* buffer, size_t size, size_t nitems, void* userdata) -> size_t {
        auto line = std::string(buffer, size * nitems);
        auto pos = line.find(':');
        if (pos != std::string::npos) {
            auto name = line.substr(0, pos);
            auto value = line.substr(pos + 1);
            while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
                value.erase(0, 1);
            }
            while (!value.empty() && (value.back() == '\r' || value.back() == '\n')) {
                value.pop_back();
            }
            auto* hdrs = static_cast<HeadResult*>(userdata);
            hdrs->headers[name] = value;
        }
        return size * nitems;
    }

    auto performHEADOnce(const std::string& url) -> Result<HeadResult> {
        CURL* curl = curl_easy_init();
        if (curl == nullptr) {
            return {Error{ErrorCode::Unknown, "CURL init failed"}};
        }
        HeadResult result;
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, headerCallback);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, &result);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, config.requestTimeout);
        configureAuth(curl);
        CURLcode res = curl_easy_perform(curl);
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &result.statusCode);
        curl_easy_cleanup(curl);
        if (res != CURLE_OK) {
            healthy.store(false);
            ++errorCount;
            lastError = curl_easy_strerror(res);
            return {Error{ErrorCode::NetworkError, lastError}};
        }
        healthy.store(true);
        return result;
    }

    // existing members unchanged...
    BackendConfig config;
    std::string scheme;
    std::string host;
    std::string basePath;
    std::unique_ptr<LRUCache> cache;

    auto parseURL(const std::string& url) -> Result<void> {
        // Parse URL: scheme://[user:pass@]host[:port]/path
        std::regex urlRegex(R"(^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?$)");
        std::smatch match;

        if (!std::regex_match(url, match, urlRegex)) {
            return {Error{ErrorCode::InvalidArgument, "Invalid URL format"}};
        }

        constexpr size_t SCHEME_IDX = 2;
        constexpr size_t AUTHORITY_IDX = 4;
        constexpr size_t PATH_IDX = 5;
        scheme = match[SCHEME_IDX].str();
        std::string authority = match[AUTHORITY_IDX].str();
        basePath = match[PATH_IDX].str();

        // Parse authority for host and credentials
        size_t atPos = authority.find('@');
        if (atPos != std::string::npos) {
            std::string userInfo = authority.substr(0, atPos);
            host = authority.substr(atPos + 1);

            size_t colonPos = userInfo.find(':');
            if (colonPos != std::string::npos) {
                config.credentials["username"] = userInfo.substr(0, colonPos);
                config.credentials["password"] = userInfo.substr(colonPos + 1);
            }
        } else {
            host = authority;
        }

        // Validate scheme
        if (scheme != "s3" && scheme != "http" && scheme != "https" && scheme != "ftp") {
            return {Error{ErrorCode::InvalidArgument, "Unsupported URL scheme: " + scheme}};
        }

        return {};
    }

    auto buildURL(std::string_view key) const -> std::string {
        std::string url = scheme + "://" + host + basePath;
        if (!url.empty() && url.back() != '/') {
            url += '/';
        }
        url += std::string(key);
        return url;
    }

    auto configureAuth(CURL* curl) -> Result<void> {
        if (scheme == "s3") {
            // For S3, check AWS credentials
            static std::mutex envMutex;
            const char* accessKey = nullptr;
            const char* secretKey = nullptr;
            {
                std::lock_guard<std::mutex> lock(envMutex);
                // NOLINTNEXTLINE(concurrency-mt-unsafe): guarded by envMutex
                accessKey = std::getenv("AWS_ACCESS_KEY_ID");
                // NOLINTNEXTLINE(concurrency-mt-unsafe): guarded by envMutex
                secretKey = std::getenv("AWS_SECRET_ACCESS_KEY");
            }

            if ((accessKey != nullptr) && (secretKey != nullptr)) {
                // TODO: Implement AWS Signature V4
                // For now, we'll use basic auth as a placeholder
                // In production, this needs proper AWS signing
                spdlog::warn("S3 authentication not fully implemented - using placeholder");
            }
        } else if (!config.credentials.empty()) {
            // Use credentials from URL or config
            auto userIt = config.credentials.find("username");
            auto passIt = config.credentials.find("password");

            if (userIt != config.credentials.end() && passIt != config.credentials.end()) {
                curl_easy_setopt(curl, CURLOPT_USERNAME, userIt->second.c_str());
                curl_easy_setopt(curl, CURLOPT_PASSWORD, passIt->second.c_str());
            }
        }

        // Check for .netrc file
        curl_easy_setopt(curl, CURLOPT_NETRC, CURL_NETRC_OPTIONAL);

        return {};
    }

    auto performGETOnce(const std::string& url) -> Result<std::vector<std::byte>> {
        CURL* curl = curl_easy_init();
        if (curl == nullptr) {
            return {Error{ErrorCode::Unknown, "Failed to initialize CURL"}};
        }

        std::vector<uint8_t> buffer;
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buffer);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, config.requestTimeout);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

        configureAuth(curl);

        CURLcode res = curl_easy_perform(curl);
        long httpCode = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK) {
            return {Error{ErrorCode::NetworkError, curl_easy_strerror(res)}};
        }

        if (httpCode == HTTP_NOT_FOUND) {
            return {Error{ErrorCode::ChunkNotFound}};
        }

        if (httpCode >= HTTP_CLIENT_ERROR_LO) {
            return {Error{ErrorCode::NetworkError, "HTTP error: " + std::to_string(httpCode)}};
        }

        // Convert to std::byte vector
        std::vector<std::byte> result(buffer.size());
        std::ranges::transform(buffer, result.begin(),
                               [](uint8_t byteVal) { return std::byte{byteVal}; });

        return result;
    }

    auto performPUTOnce(const std::string& url, std::span<const std::byte> data) -> Result<void> {
        CURL* curl = curl_easy_init();
        if (curl == nullptr) {
            return {Error{ErrorCode::Unknown, "Failed to initialize CURL"}};
        }

        ReadData readData{.data = data.data(), .size = data.size(), .offset = 0};

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
        curl_easy_setopt(curl, CURLOPT_READFUNCTION, readCallback);
        curl_easy_setopt(curl, CURLOPT_READDATA, &readData);
        curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(data.size()));
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, config.requestTimeout);

        configureAuth(curl);

        CURLcode res = curl_easy_perform(curl);
        long httpCode = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK) {
            return {Error{ErrorCode::NetworkError, curl_easy_strerror(res)}};
        }

        if (httpCode >= HTTP_CLIENT_ERROR_LO) {
            return {Error{ErrorCode::NetworkError, "HTTP error: " + std::to_string(httpCode)}};
        }

        return {};
    }

    auto performDELETEOnce(const std::string& url) -> Result<void> {
        CURL* curl = curl_easy_init();
        if (curl == nullptr) {
            return {Error{ErrorCode::Unknown, "Failed to initialize CURL"}};
        }

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, config.requestTimeout);

        configureAuth(curl);

        CURLcode res = curl_easy_perform(curl);
        long httpCode = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK) {
            return {Error{ErrorCode::NetworkError, curl_easy_strerror(res)}};
        }

        if (httpCode >= HTTP_CLIENT_ERROR_LO && httpCode != HTTP_NOT_FOUND) {
            return {Error{ErrorCode::NetworkError, "HTTP error: " + std::to_string(httpCode)}};
        }

        return {};
    }

    // ---- retry wrappers ----
    auto performGET(const std::string& url) -> Result<std::vector<std::byte>> {
        for (int attempt = 0;; ++attempt) {
            auto result = performGETOnce(url);
            if (result) {
                return result;
            }
            if (attempt >= config.maxRetries || !isRetryable(result.error())) {
                return result;
            }
            backoff(attempt);
        }
    }

    auto performPUT(const std::string& url, std::span<const std::byte> data) -> Result<void> {
        for (int attempt = 0;; ++attempt) {
            auto res = performPUTOnce(url, data);
            if (res) {
                return res;
            }
            if (attempt >= config.maxRetries || !isRetryable(res.error())) {
                return res;
            }
            backoff(attempt);
        }
    }

    auto performDELETE(const std::string& url) -> Result<void> {
        for (int attempt = 0;; ++attempt) {
            auto res = performDELETEOnce(url);
            if (res) {
                return res;
            }
            if (attempt >= config.maxRetries || !isRetryable(res.error())) {
                return res;
            }
            backoff(attempt);
        }
    }

    auto performHEAD(const std::string& url) -> Result<HeadResult> {
        for (int attempt = 0;; ++attempt) {
            auto res = performHEADOnce(url);
            if (res) {
                return res;
            }
            if (attempt >= config.maxRetries || !isRetryable(res.error())) {
                return res;
            }
            backoff(attempt);
        }
    }
};

URLBackend::URLBackend() : pImpl(std::make_unique<Impl>()) {}

URLBackend::~URLBackend() = default;

// duplicate constants removed; see definitions at top of file

auto URLBackend::initialize(const BackendConfig& config) -> Result<void> {
    pImpl->config = config;

    // Parse URL
    if (auto result = pImpl->parseURL(config.url); !result) {
        return result;
    }

    // Initialize cache
    pImpl->cache = std::make_unique<LRUCache>(config.cacheSize, config.cacheTTL);

    // Initialize libcurl globally (thread-safe)
    static std::once_flag curlInitFlag;
    std::call_once(curlInitFlag, []() { curl_global_init(CURL_GLOBAL_ALL); });

    spdlog::info("Initialized URL backend: scheme={}, host={}, path={}", pImpl->scheme, pImpl->host,
                 pImpl->basePath);

    return {};
}

auto URLBackend::store(std::string_view key, std::span<const std::byte> data) -> Result<void> {
    std::string url = pImpl->buildURL(key);

    auto result = pImpl->performPUT(url, data);
    if (result) {
        // Update cache on successful write
        pImpl->cache->put(std::string(key), std::vector<std::byte>(data.begin(), data.end()));
    }

    return result;
}

auto URLBackend::retrieve(std::string_view key) const -> Result<std::vector<std::byte>> {
    std::string keyStr(key);

    // Check cache first
    if (auto cached = pImpl->cache->get(keyStr)) {
        spdlog::debug("Cache hit for key: {}", key);
        return cached.value();
    }

    // Fetch from remote
    std::string url = pImpl->buildURL(key);
    auto result = pImpl->performGET(url);

    if (result) {
        // Update cache
        pImpl->cache->put(keyStr, result.value());
    }

    return result;
}

auto URLBackend::exists(std::string_view key) const -> Result<bool> {
    std::string url = pImpl->buildURL(key);
    auto headResult = pImpl->performHEAD(url);
    if (!headResult) {
        // Convert network errors into false (not found) where appropriate
        if (headResult.error().code == ErrorCode::ChunkNotFound) {
            return false;
        }
        return {headResult.error()};
    }
    // 2xx indicates success; 404 not found; others propagate error
    const auto& headRes = headResult.value();
    long status = headRes.statusCode;
    if (status == HTTP_NOT_FOUND) {
        return false;
    }
    if (status >= HTTP_SUCCESS_LO && status <= HTTP_SUCCESS_HI) {
        return true;
    }
    return {Error{ErrorCode::NetworkError, "HEAD unexpected status: " + std::to_string(status)}};
}

auto URLBackend::remove(std::string_view key) -> Result<void> {
    std::string url = pImpl->buildURL(key);
    return pImpl->performDELETE(url);
}

auto URLBackend::list(std::string_view prefix) const -> Result<std::vector<std::string>> {
    (void)prefix; // not yet used
    // This would need protocol-specific implementation
    // For S3: use ListObjects API
    // For HTTP: might need directory listing support
    // For FTP: use LIST command

    spdlog::warn("List operation not fully implemented for URL backend");
    return std::vector<std::string>{};
}

auto URLBackend::getStats() const -> Result<::yams::StorageStats> {
    // Return basic stats - could be enhanced with remote metrics
    ::yams::StorageStats stats;
    return stats;
}

auto URLBackend::storeAsync(std::string_view key, std::span<const std::byte> data)
    -> std::future<Result<void>> {
    return std::async(
        std::launch::async,
        [this, key = std::string(key), data = std::vector<std::byte>(data.begin(), data.end())]() {
            return store(key, data);
        });
}

auto URLBackend::retrieveAsync(std::string_view key) const
    -> std::future<Result<std::vector<std::byte>>> {
    return std::async(std::launch::async,
                      [this, key = std::string(key)]() { return retrieve(key); });
}

auto URLBackend::getType() const -> std::string {
    return pImpl->scheme;
}

auto URLBackend::flush() -> Result<void> {
    pImpl->cache->clear();
    return {};
}

} // namespace yams::storage
