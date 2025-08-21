#include <yams/storage/storage_backend.h>
#include <yams/storage/storage_engine.h>
#include <yams/core/error.h>

#include <curl/curl.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <mutex>
#include <regex>
#include <sstream>
#include <thread>
#include <unordered_map>

namespace yams::storage {

// LRU cache for remote reads
class LRUCache {
public:
    struct CacheEntry {
        std::vector<std::byte> data;
        std::chrono::steady_clock::time_point timestamp;
        size_t size;
    };
    
    explicit LRUCache(size_t maxSize, size_t ttlSeconds)
        : maxSize_(maxSize), ttl_(ttlSeconds), currentSize_(0) {}
    
    std::optional<std::vector<std::byte>> get(const std::string& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = cache_.find(key);
        if (it == cache_.end()) {
            return std::nullopt;
        }
        
        // Check TTL
        auto now = std::chrono::steady_clock::now();
        auto age = std::chrono::duration_cast<std::chrono::seconds>(now - it->second.timestamp);
        if (age.count() > static_cast<long>(ttl_)) {
            currentSize_ -= it->second.size;
            cache_.erase(it);
            return std::nullopt;
        }
        
        // Move to front (most recently used)
        accessOrder_.remove(key);
        accessOrder_.push_front(key);
        
        return it->second.data;
    }
    
    void put(const std::string& key, const std::vector<std::byte>& data) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // Remove existing entry if present
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            currentSize_ -= it->second.size;
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
        cache_[key] = {data, std::chrono::steady_clock::now(), data.size()};
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
    size_t currentSize_;
    mutable std::mutex mutex_;
};

// Write callback for libcurl
static size_t writeCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    auto* buffer = static_cast<std::vector<uint8_t>*>(userp);
    size_t totalSize = size * nmemb;
    buffer->insert(buffer->end(), 
                   static_cast<uint8_t*>(contents),
                   static_cast<uint8_t*>(contents) + totalSize);
    return totalSize;
}

// Read callback for libcurl uploads
struct ReadData {
    const std::byte* data;
    size_t size;
    size_t offset;
};

static size_t readCallback(void* buffer, size_t size, size_t nmemb, void* userp) {
    auto* readData = static_cast<ReadData*>(userp);
    size_t bufferSize = size * nmemb;
    size_t remaining = readData->size - readData->offset;
    size_t toRead = std::min(bufferSize, remaining);
    
    if (toRead > 0) {
        std::memcpy(buffer, readData->data + readData->offset, toRead);
        readData->offset += toRead;
    }
    
    return toRead;
}

class URLBackend::Impl {
public:
    BackendConfig config;
    std::string scheme;
    std::string host;
    std::string basePath;
    std::unique_ptr<LRUCache> cache;
    
    Result<void> parseURL(const std::string& url) {
        // Parse URL: scheme://[user:pass@]host[:port]/path
        std::regex urlRegex(R"(^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?$)");
        std::smatch match;
        
        if (!std::regex_match(url, match, urlRegex)) {
            return Result<void>(ErrorCode::InvalidArgument, "Invalid URL format");
        }
        
        scheme = match[2].str();
        std::string authority = match[4].str();
        basePath = match[5].str();
        
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
            return Result<void>(ErrorCode::InvalidArgument, 
                               "Unsupported URL scheme: " + scheme);
        }
        
        return {};
    }
    
    std::string buildURL(std::string_view key) const {
        std::string url = scheme + "://" + host + basePath;
        if (!url.empty() && url.back() != '/') {
            url += '/';
        }
        url += std::string(key);
        return url;
    }
    
    Result<void> configureAuth(CURL* curl) {
        if (scheme == "s3") {
            // For S3, check AWS credentials
            const char* accessKey = std::getenv("AWS_ACCESS_KEY_ID");
            const char* secretKey = std::getenv("AWS_SECRET_ACCESS_KEY");
            
            if (accessKey && secretKey) {
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
    
    Result<std::vector<std::byte>> performGET(const std::string& url) {
        CURL* curl = curl_easy_init();
        if (!curl) {
            return Result<std::vector<std::byte>>(ErrorCode::Unknown, "Failed to initialize CURL");
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
            return Result<std::vector<std::byte>>(ErrorCode::NetworkError, 
                                                  curl_easy_strerror(res));
        }
        
        if (httpCode == 404) {
            return Result<std::vector<std::byte>>(ErrorCode::ChunkNotFound);
        }
        
        if (httpCode >= 400) {
            return Result<std::vector<std::byte>>(ErrorCode::NetworkError,
                                                  "HTTP error: " + std::to_string(httpCode));
        }
        
        // Convert to std::byte vector
        std::vector<std::byte> result(buffer.size());
        std::transform(buffer.begin(), buffer.end(), result.begin(),
                      [](uint8_t b) { return std::byte{b}; });
        
        return result;
    }
    
    Result<void> performPUT(const std::string& url, std::span<const std::byte> data) {
        CURL* curl = curl_easy_init();
        if (!curl) {
            return Result<void>(ErrorCode::Unknown, "Failed to initialize CURL");
        }
        
        ReadData readData{data.data(), data.size(), 0};
        
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
            return Result<void>(ErrorCode::NetworkError, curl_easy_strerror(res));
        }
        
        if (httpCode >= 400) {
            return Result<void>(ErrorCode::NetworkError,
                               "HTTP error: " + std::to_string(httpCode));
        }
        
        return {};
    }
    
    Result<void> performDELETE(const std::string& url) {
        CURL* curl = curl_easy_init();
        if (!curl) {
            return Result<void>(ErrorCode::Unknown, "Failed to initialize CURL");
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
            return Result<void>(ErrorCode::NetworkError, curl_easy_strerror(res));
        }
        
        if (httpCode >= 400 && httpCode != 404) {
            return Result<void>(ErrorCode::NetworkError,
                               "HTTP error: " + std::to_string(httpCode));
        }
        
        return {};
    }
};

URLBackend::URLBackend() : pImpl(std::make_unique<Impl>()) {}

URLBackend::~URLBackend() = default;

Result<void> URLBackend::initialize(const BackendConfig& config) {
    pImpl->config = config;
    
    // Parse URL
    if (auto result = pImpl->parseURL(config.url); !result) {
        return result;
    }
    
    // Initialize cache
    pImpl->cache = std::make_unique<LRUCache>(config.cacheSize, config.cacheTTL);
    
    // Initialize libcurl globally (thread-safe)
    static std::once_flag curlInitFlag;
    std::call_once(curlInitFlag, []() {
        curl_global_init(CURL_GLOBAL_ALL);
    });
    
    spdlog::info("Initialized URL backend: scheme={}, host={}, path={}", 
                 pImpl->scheme, pImpl->host, pImpl->basePath);
    
    return {};
}

Result<void> URLBackend::store(std::string_view key, std::span<const std::byte> data) {
    std::string url = pImpl->buildURL(key);
    
    auto result = pImpl->performPUT(url, data);
    if (result) {
        // Update cache on successful write
        pImpl->cache->put(std::string(key), 
                         std::vector<std::byte>(data.begin(), data.end()));
    }
    
    return result;
}

Result<std::vector<std::byte>> URLBackend::retrieve(std::string_view key) const {
    std::string keyStr(key);
    
    // Check cache first
    if (auto cached = pImpl->cache->get(keyStr)) {
        spdlog::debug("Cache hit for key: {}", key);
        return *cached;
    }
    
    // Fetch from remote
    std::string url = pImpl->buildURL(key);
    auto result = pImpl->performGET(url);
    
    if (result) {
        // Update cache
        pImpl->cache->put(keyStr, *result);
    }
    
    return result;
}

Result<bool> URLBackend::exists(std::string_view key) const {
    // For remote backends, we'll do a HEAD request
    // For now, try to retrieve and check if it succeeds
    auto result = retrieve(key);
    return Result<bool>(result.has_value());
}

Result<void> URLBackend::remove(std::string_view key) {
    std::string url = pImpl->buildURL(key);
    return pImpl->performDELETE(url);
}

Result<std::vector<std::string>> URLBackend::list(std::string_view prefix) const {
    // This would need protocol-specific implementation
    // For S3: use ListObjects API
    // For HTTP: might need directory listing support
    // For FTP: use LIST command
    
    spdlog::warn("List operation not fully implemented for URL backend");
    return std::vector<std::string>{};
}

Result<StorageStats> URLBackend::getStats() const {
    // Return basic stats - could be enhanced with remote metrics
    StorageStats stats;
    return stats;
}

std::future<Result<void>> URLBackend::storeAsync(std::string_view key,
                                                 std::span<const std::byte> data) {
    return std::async(std::launch::async, [this, key = std::string(key), 
                                           data = std::vector<std::byte>(data.begin(), data.end())]() {
        return store(key, data);
    });
}

std::future<Result<std::vector<std::byte>>> URLBackend::retrieveAsync(std::string_view key) const {
    return std::async(std::launch::async, [this, key = std::string(key)]() {
        return retrieve(key);
    });
}

std::string URLBackend::getType() const {
    return pImpl->scheme;
}

Result<void> URLBackend::flush() {
    pImpl->cache->clear();
    return {};
}

} // namespace yams::storage