// S3 object storage plugin

#include <yams/crypto/hasher.h>
#include <yams/plugins/object_storage.h>
#include <yams/storage/s3_signer.h>
#include <yams/storage/storage_backend.h>
#include <yams/storage/storage_backend_extended.h>

extern "C" {
#include <yams/plugins/abi.h>
#include <yams/plugins/object_storage_v1.h>
}

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <regex>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace yams::storage {

namespace {

static size_t writeCb(void* contents, size_t size, size_t nmemb, void* userp) {
    auto* out = static_cast<std::vector<uint8_t>*>(userp);
    size_t total = size * nmemb;
    auto* bytes = static_cast<uint8_t*>(contents);
    out->insert(out->end(), bytes, bytes + total);
    return total;
}

struct ReadData {
    const std::byte* data;
    size_t size;
    size_t offset;
};

static size_t readCb(void* buffer, size_t size, size_t nmemb, void* userp) {
    auto* rd = static_cast<ReadData*>(userp);
    size_t cap = size * nmemb;
    size_t remain = rd->size - rd->offset;
    size_t toCopy = std::min(cap, remain);
    if (toCopy > 0) {
        std::memcpy(buffer, rd->data + rd->offset, toCopy);
        rd->offset += toCopy;
    }
    return toCopy;
}

// RFC 3986 percent-encode; when preserveSlash=true, '/' is not encoded
static std::string percentEncodeRfc3986(const std::string& s, bool preserveSlash) {
    static const char* unreserved =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~";
    std::string out;
    out.reserve(s.size() * 3);
    for (unsigned char c : s) {
        if (std::strchr(unreserved, c) || (preserveSlash && c == '/')) {
            out.push_back((char)c);
        } else {
            char buf[4];
            std::snprintf(buf, sizeof(buf), "%%%02X", c);
            out.append(buf);
        }
    }
    return out;
}

struct S3Url {
    std::string bucket;
    std::string prefix; // may be empty or end with '/'
};

static Result<S3Url> parseS3Url(const std::string& url) {
    // Expect s3://bucket/prefix
    if (url.rfind("s3://", 0) != 0) {
        return Error{ErrorCode::InvalidArgument, "URL must start with s3://"};
    }
    std::string rest = url.substr(5);
    auto slash = rest.find('/');
    S3Url out;
    if (slash == std::string::npos) {
        out.bucket = rest;
        out.prefix.clear();
    } else {
        out.bucket = rest.substr(0, slash);
        out.prefix = rest.substr(slash + 1);
        if (!out.prefix.empty() && out.prefix.back() != '/')
            out.prefix.push_back('/');
    }
    if (out.bucket.empty()) {
        return Error{ErrorCode::InvalidArgument, "Missing bucket in s3 URL"};
    }
    return out;
}

class S3Backend : public IStorageBackendExtended {
public:
    S3Backend() = default;
    ~S3Backend() override = default;

    Result<void> initialize(const BackendConfig& cfg) override {
        config_ = cfg;
        if (cfg.url.empty()) {
            return Error{ErrorCode::InvalidArgument, "S3 backend requires URL"};
        }

        auto parsedUrl = parseS3Url(cfg.url);
        if (!parsedUrl)
            return parsedUrl.error();
        s3_ = parsedUrl.value();

        // Resolve endpoint
        auto it = config_.credentials.find("endpoint");
        if (it != config_.credentials.end()) {
            endpointHost_ = it->second;
        } else {
            endpointHost_ = "s3.amazonaws.com";
        }

        if (endpointHost_.rfind("http://", 0) == 0 || endpointHost_.rfind("https://", 0) == 0) {
            // Strip scheme to keep only host[:port]
            auto pos = endpointHost_.find("://");
            auto hostRest = endpointHost_.substr(pos + 3);
            auto slash = hostRest.find('/');
            endpointHost_ = (slash == std::string::npos) ? hostRest : hostRest.substr(0, slash);
        }

        // Initialize curl global once
        static std::once_flag once;
        std::call_once(once, []() { curl_global_init(CURL_GLOBAL_ALL); });
        return {};
    }

    Result<void> store(std::string_view key, std::span<const std::byte> data) override {
        std::string url = buildObjectUrl(key);
        CURL* curl = curl_easy_init();
        if (!curl)
            return Error{ErrorCode::Unknown, "curl init failed"};

        ReadData rd{data.data(), data.size(), 0};
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
        curl_easy_setopt(curl, CURLOPT_READFUNCTION, readCb);
        curl_easy_setopt(curl, CURLOPT_READDATA, &rd);
        curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(data.size()));
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)config_.requestTimeout);

        std::vector<std::pair<std::string, std::string>> extra;
        if (!config_.storageClass.empty()) {
            extra.emplace_back("x-amz-storage-class", config_.storageClass);
        }
        if (!config_.sseKmsKeyId.empty()) {
            extra.emplace_back("x-amz-server-side-encryption", "aws:kms");
            extra.emplace_back("x-amz-server-side-encryption-aws-kms-key-id", config_.sseKmsKeyId);
        }
        if (config_.checksumAlgorithm == "sha256") {
            auto hex = calculateChecksum(data, "sha256");
            if (hex)
                extra.emplace_back("x-amz-checksum-sha256", hex.value());
        }
        auto hdrsRes = S3Signer::signRequest(curl, config_, "PUT", url, data, extra);
        if (!hdrsRes) {
            curl_easy_cleanup(curl);
            return hdrsRes.error();
        }
        struct curl_slist* headers = hdrsRes.value();
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        CURLcode res = curl_easy_perform(curl);
        long code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK)
            return Error{ErrorCode::NetworkError, curl_easy_strerror(res)};
        if (code >= 400)
            return Error{ErrorCode::NetworkError, "HTTP " + std::to_string(code)};
        return {};
    }

    Result<std::vector<std::byte>> retrieve(std::string_view key) const override {
        std::string url = buildObjectUrl(key);
        CURL* curl = curl_easy_init();
        if (!curl)
            return Error{ErrorCode::Unknown, "curl init failed"};

        std::vector<uint8_t> buf;
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCb);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)config_.requestTimeout);

        auto hdrsRes = S3Signer::signRequest(curl, config_, "GET", url, {});
        if (!hdrsRes) {
            curl_easy_cleanup(curl);
            return hdrsRes.error();
        }
        struct curl_slist* headers = hdrsRes.value();
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        CURLcode res = curl_easy_perform(curl);
        long code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK)
            return Error{ErrorCode::NetworkError, curl_easy_strerror(res)};
        if (code == 404)
            return Error{ErrorCode::ChunkNotFound};
        if (code >= 400)
            return Error{ErrorCode::NetworkError, "HTTP " + std::to_string(code)};

        std::vector<std::byte> out(buf.size());
        std::transform(buf.begin(), buf.end(), out.begin(), [](uint8_t b) { return std::byte{b}; });
        return out;
    }

    Result<bool> exists(std::string_view key) const override {
        std::string url = buildObjectUrl(key);
        CURL* curl = curl_easy_init();
        if (!curl)
            return Error{ErrorCode::Unknown, "curl init failed"};
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)config_.requestTimeout);
        auto hdrsRes = S3Signer::signRequest(curl, config_, "HEAD", url, {});
        if (!hdrsRes) {
            curl_easy_cleanup(curl);
            return hdrsRes.error();
        }
        struct curl_slist* headers = hdrsRes.value();
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        CURLcode res = curl_easy_perform(curl);
        long code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        if (res != CURLE_OK)
            return Error{ErrorCode::NetworkError, curl_easy_strerror(res)};
        if (code == 404)
            return false;
        if (code >= 200 && code <= 299)
            return true;
        return Error{ErrorCode::NetworkError, "HTTP " + std::to_string(code)};
    }

    Result<void> remove(std::string_view key) override {
        std::string url = buildObjectUrl(key);
        CURL* curl = curl_easy_init();
        if (!curl)
            return Error{ErrorCode::Unknown, "curl init failed"};
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)config_.requestTimeout);
        auto hdrsRes = S3Signer::signRequest(curl, config_, "DELETE", url, {});
        if (!hdrsRes) {
            curl_easy_cleanup(curl);
            return hdrsRes.error();
        }
        struct curl_slist* headers = hdrsRes.value();
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        CURLcode res = curl_easy_perform(curl);
        long code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        if (res != CURLE_OK)
            return Error{ErrorCode::NetworkError, curl_easy_strerror(res)};
        if (code >= 400 && code != 404)
            return Error{ErrorCode::NetworkError, "HTTP " + std::to_string(code)};
        return {};
    }

    Result<std::vector<std::string>> list(std::string_view prefix) const override {
        // Perform S3 ListObjectsV2 with effective prefix (base + parameter)
        // Minimal implementation: gather up to 1000 keys, single or multiple pages (max 10 pages)
        std::vector<std::string> results;

        auto percentEncode = [](const std::string& s) {
            static const char* unreserved =
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~";
            std::string out;
            for (unsigned char c : s) {
                if (std::strchr(unreserved, c)) {
                    out.push_back((char)c);
                } else {
                    char buf[4];
                    std::snprintf(buf, sizeof(buf), "%%%02X", c);
                    out.append(buf);
                }
            }
            return out;
        };

        auto buildBucketBase = [this]() {
            std::string scheme = "https://";
            std::string host = endpointHost_;
            std::string path;
            if (config_.usePathStyle) {
                path = "/" + s3_.bucket + "/"; // path-style list on /bucket/
            } else {
                host = s3_.bucket + "." + host;
                path = "/"; // root of bucket
            }
            return std::make_pair(scheme + host + path, host);
        };

        std::string continuation;
        std::string effectivePrefix = s3_.prefix + std::string("") + std::string(prefix);
        int pages = 0;
        do {
            auto [baseUrl, hostForSign] = buildBucketBase();

            // Build canonical query sorted
            std::vector<std::pair<std::string, std::string>> params;
            params.emplace_back("list-type", "2");
            if (!effectivePrefix.empty())
                params.emplace_back("prefix", effectivePrefix);
            params.emplace_back("max-keys", "1000");
            if (!continuation.empty())
                params.emplace_back("continuation-token", continuation);
            std::sort(params.begin(), params.end());
            std::string query;
            for (size_t i = 0; i < params.size(); ++i) {
                if (i)
                    query.push_back('&');
                query += percentEncode(params[i].first);
                query.push_back('=');
                query += percentEncode(params[i].second);
            }
            std::string url = baseUrl + (query.empty() ? std::string("") : ("?" + query));

            CURL* curl = curl_easy_init();
            if (!curl)
                return Error{ErrorCode::Unknown, "curl init failed"};
            std::vector<uint8_t> buf;
            curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCb);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);
            curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)config_.requestTimeout);

            auto hdrsRes = S3Signer::signRequest(curl, config_, "GET", url, {});
            if (!hdrsRes) {
                curl_easy_cleanup(curl);
                return hdrsRes.error();
            }
            struct curl_slist* headers = hdrsRes.value();
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

            CURLcode res = curl_easy_perform(curl);
            long code = 0;
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
            curl_slist_free_all(headers);
            curl_easy_cleanup(curl);
            if (res != CURLE_OK)
                return Error{ErrorCode::NetworkError, curl_easy_strerror(res)};
            if (code >= 400)
                return Error{ErrorCode::NetworkError, "HTTP " + std::to_string(code)};

            // Parse XML for <Key> and NextContinuationToken
            std::string xml(buf.begin(), buf.end());
            std::regex keyRe(R"(<Key>([^<]+)</Key>)");
            for (std::sregex_iterator it(xml.begin(), xml.end(), keyRe), end; it != end; ++it) {
                std::string k = (*it)[1].str();
                // Strip base prefix if present and return relative keys
                if (!s3_.prefix.empty() && k.rfind(s3_.prefix, 0) == 0) {
                    k = k.substr(s3_.prefix.size());
                }
                results.emplace_back(std::move(k));
            }
            std::smatch m;
            std::regex contRe(R"(<NextContinuationToken>([^<]+)</NextContinuationToken>)");
            if (std::regex_search(xml, m, contRe)) {
                continuation = m[1].str();
            } else {
                continuation.clear();
            }
        } while (!continuation.empty() && ++pages < 10 && results.size() < 5000);

        return results;
    }

    Result<::yams::StorageStats> getStats() const override {
        ::yams::StorageStats s; // defaults
        return s;
    }

    std::future<Result<void>> storeAsync(std::string_view key,
                                         std::span<const std::byte> data) override {
        return std::async(
            std::launch::async,
            [this, k = std::string(key), d = std::vector<std::byte>(data.begin(), data.end())]() {
                return store(k, d);
            });
    }

    std::future<Result<std::vector<std::byte>>> retrieveAsync(std::string_view key) const override {
        return std::async(std::launch::async,
                          [this, k = std::string(key)]() { return retrieve(k); });
    }

    std::string getType() const override { return "s3"; }
    bool isRemote() const override { return true; }
    Result<void> flush() override { return {}; }

    // IStorageBackendExtended (multipart)
    Result<std::string>
    initiateMultipartUpload(std::string_view key,
                            const std::unordered_map<std::string, std::string>& metadata) override;

    Result<std::string> uploadPart(std::string_view key, const std::string& uploadId,
                                   int partNumber, std::span<const std::byte> data,
                                   const std::string& checksum) override;

    Result<void>
    completeMultipartUpload(std::string_view key, const std::string& uploadId,
                            const std::vector<std::pair<int, std::string>>& parts) override;

    Result<void> abortMultipartUpload(std::string_view key, const std::string& uploadId) override;

    Result<std::string> calculateChecksum(std::span<const std::byte> data,
                                          const std::string& algorithm) override;

    Result<bool> verifyChecksum(std::string_view key, const std::string& expectedChecksum,
                                const std::string& algorithm) override;

private:
    BackendConfig config_;
    S3Url s3_;
    std::string endpointHost_;

    std::string buildObjectUrl(std::string_view key) const {
        std::string scheme = "https://";
        std::string host = endpointHost_;
        std::string path;
        // Encode prefix and key safely (preserve path separators but encode reserved chars)
        std::string encodedPrefix = percentEncodeRfc3986(s3_.prefix, true);
        std::string encodedKey = percentEncodeRfc3986(std::string(key), true);
        if (config_.usePathStyle) {
            path = "/" + s3_.bucket + "/" + encodedPrefix + encodedKey;
        } else {
            host = s3_.bucket + "." + host;
            path = "/" + encodedPrefix + encodedKey;
        }
        return scheme + host + path;
    }
};

// Out-of-class definitions for multipart and checksum methods

Result<std::string> S3Backend::initiateMultipartUpload(
    std::string_view key, const std::unordered_map<std::string, std::string>& /*metadata*/) {
    // Minimal: ignore metadata; perform POST ?uploads
    std::string url = buildObjectUrl(key) + "?uploads=";
    CURL* curl = curl_easy_init();
    if (!curl)
        return Error{ErrorCode::Unknown, "curl init failed"};
    std::vector<uint8_t> buf;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)config_.requestTimeout);
    std::vector<std::pair<std::string, std::string>> extra;
    if (!config_.storageClass.empty())
        extra.emplace_back("x-amz-storage-class", config_.storageClass);
    if (!config_.sseKmsKeyId.empty()) {
        extra.emplace_back("x-amz-server-side-encryption", "aws:kms");
        extra.emplace_back("x-amz-server-side-encryption-aws-kms-key-id", config_.sseKmsKeyId);
    }
    auto hdrsRes = S3Signer::signRequest(curl, config_, "POST", url, {}, extra);
    if (!hdrsRes) {
        curl_easy_cleanup(curl);
        return hdrsRes.error();
    }
    struct curl_slist* headers = hdrsRes.value();
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    CURLcode res = curl_easy_perform(curl);
    long code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    if (res != CURLE_OK)
        return Error{ErrorCode::NetworkError, curl_easy_strerror(res)};
    if (code >= 400)
        return Error{ErrorCode::NetworkError, "HTTP " + std::to_string(code)};
    std::string xml(buf.begin(), buf.end());
    std::smatch m;
    std::regex re(R"(<UploadId>([^<]+)</UploadId>)");
    if (std::regex_search(xml, m, re)) {
        return m[1].str();
    }
    return Error{ErrorCode::Unknown, "UploadId not found"};
}

Result<std::string> S3Backend::uploadPart(std::string_view key, const std::string& uploadId,
                                          int partNumber, std::span<const std::byte> data,
                                          const std::string& /*checksum*/) {
    // PUT ?partNumber=&uploadId=
    std::string url =
        buildObjectUrl(key) + "?partNumber=" + std::to_string(partNumber) + "&uploadId=" + uploadId;
    CURL* curl = curl_easy_init();
    if (!curl)
        return Error{ErrorCode::Unknown, "curl init failed"};
    ReadData rd{data.data(), data.size(), 0};
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(curl, CURLOPT_READFUNCTION, readCb);
    curl_easy_setopt(curl, CURLOPT_READDATA, &rd);
    curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(data.size()));
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)config_.requestTimeout);
    std::string etag;
    auto headerCb = +[](char* buffer, size_t size, size_t nitems, void* userdata) -> size_t {
        size_t total = size * nitems;
        auto* p = static_cast<std::string*>(userdata);
        std::string_view hv(buffer, total);
        if (hv.rfind("ETag:", 0) == 0 || hv.rfind("Etag:", 0) == 0) {
            auto pos = hv.find(':');
            if (pos != std::string_view::npos) {
                std::string val(hv.substr(pos + 1));
                // trim
                auto start = val.find_first_not_of(" \t\r\n");
                auto end = val.find_last_not_of(" \t\r\n");
                if (start != std::string::npos && end != std::string::npos) {
                    val = val.substr(start, end - start + 1);
                }
                *p = val;
            }
        }
        return total;
    };
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, headerCb);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &etag);

    auto hdrsRes = S3Signer::signRequest(curl, config_, "PUT", url, data);
    if (!hdrsRes) {
        curl_easy_cleanup(curl);
        return hdrsRes.error();
    }
    struct curl_slist* headers = hdrsRes.value();
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    CURLcode res = curl_easy_perform(curl);
    long code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    if (res != CURLE_OK)
        return Error{ErrorCode::NetworkError, curl_easy_strerror(res)};
    if (code >= 400)
        return Error{ErrorCode::NetworkError, "HTTP " + std::to_string(code)};
    if (etag.empty())
        return Error{ErrorCode::Unknown, "Missing ETag for uploaded part"};
    return etag;
}

Result<void>
S3Backend::completeMultipartUpload(std::string_view key, const std::string& uploadId,
                                   const std::vector<std::pair<int, std::string>>& parts) {
    // POST ?uploadId= with XML body
    std::ostringstream xml;
    xml << "<CompleteMultipartUpload>";
    for (const auto& p : parts) {
        xml << "<Part><PartNumber>" << p.first << "</PartNumber><ETag>" << p.second
            << "</ETag></Part>";
    }
    xml << "</CompleteMultipartUpload>";
    std::string body = xml.str();

    std::string url = buildObjectUrl(key) + "?uploadId=" + uploadId;
    CURL* curl = curl_easy_init();
    if (!curl)
        return Error{ErrorCode::Unknown, "curl init failed"};
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)body.size());
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)config_.requestTimeout);

    // Note: signRequest over body payload; include Content-Type for signing
    auto payload =
        std::span<const std::byte>(reinterpret_cast<const std::byte*>(body.data()), body.size());
    std::vector<std::pair<std::string, std::string>> extra{{"content-type", "application/xml"}};
    auto hdrsRes = S3Signer::signRequest(curl, config_, "POST", url, payload, extra);
    if (!hdrsRes) {
        curl_easy_cleanup(curl);
        return hdrsRes.error();
    }
    struct curl_slist* headers = hdrsRes.value();
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    CURLcode res = curl_easy_perform(curl);
    long code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    if (res != CURLE_OK)
        return Error{ErrorCode::NetworkError, curl_easy_strerror(res)};
    if (code >= 400)
        return Error{ErrorCode::NetworkError, "HTTP " + std::to_string(code)};
    return {};
}

Result<void> S3Backend::abortMultipartUpload(std::string_view key, const std::string& uploadId) {
    std::string url = buildObjectUrl(key) + "?uploadId=" + uploadId;
    CURL* curl = curl_easy_init();
    if (!curl)
        return Error{ErrorCode::Unknown, "curl init failed"};
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)config_.requestTimeout);
    auto hdrsRes = S3Signer::signRequest(curl, config_, "DELETE", url, {});
    if (!hdrsRes) {
        curl_easy_cleanup(curl);
        return hdrsRes.error();
    }
    struct curl_slist* headers = hdrsRes.value();
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    CURLcode res = curl_easy_perform(curl);
    long code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    if (res != CURLE_OK)
        return Error{ErrorCode::NetworkError, curl_easy_strerror(res)};
    if (code >= 400)
        return Error{ErrorCode::NetworkError, "HTTP " + std::to_string(code)};
    return {};
}

Result<std::string> S3Backend::calculateChecksum(std::span<const std::byte> data,
                                                 const std::string& algorithm) {
    if (algorithm == "sha256") {
        auto hasher = crypto::createSHA256Hasher();
        hasher->init();
        hasher->update(data);
        return hasher->finalize();
    }
    return Error{ErrorCode::NotImplemented, "Unsupported checksum algorithm"};
}

Result<bool> S3Backend::verifyChecksum(std::string_view key, const std::string& expectedChecksum,
                                       const std::string& algorithm) {
    if (algorithm != "sha256")
        return Error{ErrorCode::NotImplemented, "verifyChecksum only supports sha256"};
    auto dataRes = retrieve(key);
    if (!dataRes)
        return dataRes.error();
    auto hex = calculateChecksum(
        std::span<const std::byte>(dataRes.value().data(), dataRes.value().size()), "sha256");
    if (!hex)
        return hex.error();
    return hex.value() == expectedChecksum;
}

} // namespace

} // namespace yams::storage

using namespace yams::storage;

static const char* kManifestJson = R"JSON({
  "name": "object_storage_s3",
  "version": "0.1.0",
  "interfaces": [
    {"id": "object_storage_v1", "version": 1}
  ]
})JSON";

static yams_object_storage_v1 g_storage_v1;

static int s3_create(const char* config_json, void** out_backend) {
    if (!out_backend)
        return YAMS_PLUGIN_ERR_INVALID;
    auto* backend = new S3Backend();
    BackendConfig cfg;
    if (config_json) {
        try {
            auto j = nlohmann::json::parse(config_json);
            if (j.contains("url"))
                cfg.url = j["url"].get<std::string>();
            if (j.contains("region"))
                cfg.region = j["region"].get<std::string>();
            if (j.contains("use_path_style"))
                cfg.usePathStyle = j["use_path_style"].get<bool>();
            if (j.contains("request_timeout"))
                cfg.requestTimeout = j["request_timeout"].get<int>();
            if (j.contains("storage_class"))
                cfg.storageClass = j["storage_class"].get<std::string>();
            if (j.contains("sse_kms_key_id"))
                cfg.sseKmsKeyId = j["sse_kms_key_id"].get<std::string>();
            if (j.contains("checksum_algorithm"))
                cfg.checksumAlgorithm = j["checksum_algorithm"].get<std::string>();
            if (j.contains("credentials") && j["credentials"].is_object()) {
                for (auto& [k, v] : j["credentials"].items()) {
                    if (v.is_string())
                        cfg.credentials[k] = v.get<std::string>();
                }
            }
        } catch (...) {
        }
    }
    auto r = backend->initialize(cfg);
    if (!r) {
        delete backend;
        return YAMS_PLUGIN_ERR_INIT_FAILED;
    }
    *out_backend = backend;
    return YAMS_PLUGIN_OK;
}

static void s3_destroy(void* backend) {
    delete static_cast<S3Backend*>(backend);
}

static int s3_put(void* backend, const char* key, const void* buf, size_t len, const char*) {
    if (!backend || !key || !buf)
        return YAMS_PLUGIN_ERR_INVALID;
    auto* s3 = static_cast<S3Backend*>(backend);
    auto r = s3->store(key, std::span<const std::byte>(static_cast<const std::byte*>(buf), len));
    return r ? YAMS_PLUGIN_OK : YAMS_PLUGIN_ERR_INVALID;
}

static int s3_get(void* backend, const char* key, void** out_buf, size_t* out_len, const char*) {
    if (!backend || !key || !out_buf || !out_len)
        return YAMS_PLUGIN_ERR_INVALID;
    auto* s3 = static_cast<S3Backend*>(backend);
    auto r = s3->retrieve(key);
    if (!r)
        return YAMS_PLUGIN_ERR_NOT_FOUND;
    auto& data = r.value();
    void* buf = std::malloc(data.size());
    if (!buf)
        return YAMS_PLUGIN_ERR_INVALID;
    std::memcpy(buf, data.data(), data.size());
    *out_buf = buf;
    *out_len = data.size();
    return YAMS_PLUGIN_OK;
}

static int s3_head(void* backend, const char* key, char** out_metadata_json, const char*) {
    if (!backend || !key || !out_metadata_json)
        return YAMS_PLUGIN_ERR_INVALID;
    auto* s3 = static_cast<S3Backend*>(backend);
    auto r = s3->exists(key);
    if (!r || !r.value())
        return YAMS_PLUGIN_ERR_NOT_FOUND;
    *out_metadata_json = strdup("{}");
    return YAMS_PLUGIN_OK;
}

static int s3_del(void* backend, const char* key, const char*) {
    if (!backend || !key)
        return YAMS_PLUGIN_ERR_INVALID;
    auto* s3 = static_cast<S3Backend*>(backend);
    auto r = s3->remove(key);
    return r ? YAMS_PLUGIN_OK : YAMS_PLUGIN_ERR_INVALID;
}

static int s3_list(void* backend, const char* prefix, char** out_list_json, const char*) {
    if (!backend || !out_list_json)
        return YAMS_PLUGIN_ERR_INVALID;
    auto* s3 = static_cast<S3Backend*>(backend);
    auto r = s3->list(prefix ? prefix : "");
    if (!r)
        return YAMS_PLUGIN_ERR_INVALID;
    nlohmann::json arr = nlohmann::json::array();
    for (const auto& k : r.value())
        arr.push_back(k);
    std::string json_str = arr.dump();
    *out_list_json = strdup(json_str.c_str());
    return YAMS_PLUGIN_OK;
}

extern "C" {

YAMS_PLUGIN_API int yams_plugin_get_abi_version(void) {
    return YAMS_PLUGIN_ABI_VERSION;
}

YAMS_PLUGIN_API const char* yams_plugin_get_name(void) {
    return "object_storage_s3";
}

YAMS_PLUGIN_API const char* yams_plugin_get_version(void) {
    return "0.1.0";
}

YAMS_PLUGIN_API const char* yams_plugin_get_manifest_json(void) {
    return kManifestJson;
}

YAMS_PLUGIN_API int yams_plugin_init(const char*, const void*) {
    g_storage_v1.size = sizeof(yams_object_storage_v1);
    g_storage_v1.version = 1;
    g_storage_v1.create = s3_create;
    g_storage_v1.destroy = s3_destroy;
    g_storage_v1.put = s3_put;
    g_storage_v1.get = s3_get;
    g_storage_v1.head = s3_head;
    g_storage_v1.del = s3_del;
    g_storage_v1.list = s3_list;
    return YAMS_PLUGIN_OK;
}

YAMS_PLUGIN_API void yams_plugin_shutdown(void) {}

YAMS_PLUGIN_API int yams_plugin_get_interface(const char* id, uint32_t version, void** out_iface) {
    if (!id || !out_iface)
        return YAMS_PLUGIN_ERR_INVALID;
    *out_iface = nullptr;
    if (version == 1 && std::strcmp(id, "object_storage_v1") == 0) {
        *out_iface = &g_storage_v1;
        return YAMS_PLUGIN_OK;
    }
    return YAMS_PLUGIN_ERR_NOT_FOUND;
}

YAMS_PLUGIN_API int yams_plugin_get_health_json(char** out_json) {
    if (!out_json)
        return YAMS_PLUGIN_ERR_INVALID;
    *out_json = strdup("{\"status\":\"ok\"}");
    return YAMS_PLUGIN_OK;
}

}

extern "C" yams::storage::IStorageBackend* yams_plugin_create_object_storage() {
    return new S3Backend();
}

extern "C" void yams_plugin_destroy_object_storage(yams::storage::IStorageBackend* backend) {
    delete backend;
}
