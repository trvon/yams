#include <yams/storage/storage_runtime_resolver.h>

#include <yams/storage/storage_backend.h>
#include <yams/storage/storage_backend_engine_adapter.h>

#include <curl/curl.h>

#if defined(__APPLE__)
#include <TargetConditionals.h>
#include <CoreFoundation/CoreFoundation.h>
#include <Security/Security.h>
#endif

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <map>
#include <mutex>
#include <regex>

namespace yams::storage {

namespace {

std::string trim(std::string value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) {
        return {};
    }
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

std::string toLower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value;
}

std::string expandTilde(std::string value) {
    if (value.empty() || value.front() != '~') {
        return value;
    }
    const char* home = std::getenv("HOME");
    if (!home || !*home) {
        return value;
    }
    return std::string(home) + value.substr(1);
}

std::string normalizeS3Endpoint(std::string endpoint) {
    endpoint = trim(std::move(endpoint));
    if (endpoint.empty()) {
        return endpoint;
    }

    if (endpoint.rfind("https://", 0) == 0) {
        endpoint.erase(0, 8);
    } else if (endpoint.rfind("http://", 0) == 0) {
        endpoint.erase(0, 7);
    }

    while (!endpoint.empty() && endpoint.back() == '/') {
        endpoint.pop_back();
    }

    auto slashPos = endpoint.find('/');
    if (slashPos != std::string::npos) {
        endpoint = endpoint.substr(0, slashPos);
    }
    return endpoint;
}

bool endsWith(std::string_view value, std::string_view suffix) {
    if (suffix.size() > value.size()) {
        return false;
    }
    return value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
}

bool isHexAccountId(std::string_view value) {
    if (value.size() != 32) {
        return false;
    }
    return std::all_of(value.begin(), value.end(),
                       [](unsigned char c) { return std::isxdigit(c) != 0; });
}

std::optional<std::string> parseS3BucketFromUrl(std::string_view url) {
    std::string value = trim(std::string(url));
    if (value.rfind("s3://", 0) != 0) {
        return std::nullopt;
    }
    std::string rest = value.substr(5);
    if (rest.empty()) {
        return std::nullopt;
    }
    const auto slash = rest.find('/');
    std::string bucket = (slash == std::string::npos) ? rest : rest.substr(0, slash);
    bucket = trim(std::move(bucket));
    if (bucket.empty()) {
        return std::nullopt;
    }
    return bucket;
}

std::string stripBearerPrefix(std::string value) {
    value = trim(std::move(value));
    if (value.size() >= 7) {
        std::string prefix = toLower(value.substr(0, 7));
        if (prefix == "bearer ") {
            return trim(value.substr(7));
        }
    }
    return value;
}

size_t writeResponse(char* ptr, size_t size, size_t nmemb, void* userdata) {
    const size_t bytes = size * nmemb;
    if (!userdata || !ptr || bytes == 0) {
        return 0;
    }
    auto* out = static_cast<std::string*>(userdata);
    out->append(ptr, bytes);
    return bytes;
}

struct CloudflareApiResponse {
    long statusCode{0};
    std::string body;
};

std::optional<std::string> extractJsonStringField(const std::string& body, const std::string& field,
                                                  bool requireResultObject = false) {
    std::string pattern;
    if (requireResultObject) {
        pattern =
            "\\\"result\\\"\\s*:\\s*\\{[\\s\\S]*?\\\"" + field + "\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"";
    } else {
        pattern = "\\\"" + field + "\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"";
    }
    std::regex re(pattern);
    std::smatch m;
    if (!std::regex_search(body, m, re) || m.size() < 2) {
        return std::nullopt;
    }
    return m[1].str();
}

std::string extractCloudflareErrorMessage(const std::string& body) {
    auto msg = extractJsonStringField(body, "message", false);
    if (!msg || msg->empty()) {
        return "unknown error";
    }
    return *msg;
}

bool cloudflareCallSucceeded(const std::string& body) {
    static const std::regex kSuccessTrue("\\\"success\\\"\\s*:\\s*true");
    return std::regex_search(body, kSuccessTrue);
}

Result<CloudflareApiResponse> callCloudflareApi(const std::string& method, const std::string& url,
                                                const std::string& bearerToken,
                                                const std::string* payload = nullptr) {
    static std::once_flag curlInitOnce;
    std::call_once(curlInitOnce, []() { curl_global_init(CURL_GLOBAL_ALL); });

    CURL* curl = curl_easy_init();
    if (!curl) {
        return Error{ErrorCode::InternalError, "Failed to initialize curl"};
    }

    std::string responseBody;
    std::string requestBody;
    struct curl_slist* headers = nullptr;
    auto cleanup = [&]() {
        if (headers) {
            curl_slist_free_all(headers);
            headers = nullptr;
        }
        curl_easy_cleanup(curl);
    };

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 20L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeResponse);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseBody);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    std::string authHeader = "Authorization: Bearer " + bearerToken;
    headers = curl_slist_append(headers, authHeader.c_str());
    headers = curl_slist_append(headers, "Accept: application/json");

    if (method == "POST") {
        headers = curl_slist_append(headers, "Content-Type: application/json");
        requestBody = payload ? *payload : "{}";
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, requestBody.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, requestBody.size());
    } else if (method == "GET") {
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    } else {
        cleanup();
        return Error{ErrorCode::InvalidArgument,
                     "Unsupported HTTP method for Cloudflare API call: " + method};
    }

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    const CURLcode performCode = curl_easy_perform(curl);
    long httpStatus = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpStatus);

    if (performCode != CURLE_OK) {
        std::string err =
            std::string("Cloudflare API request failed: ") + curl_easy_strerror(performCode);
        cleanup();
        return Error{ErrorCode::NetworkError, err};
    }

    if (httpStatus >= 400) {
        std::string message =
            "Cloudflare API request failed (HTTP " + std::to_string(httpStatus) + ")";
        message += ": " + extractCloudflareErrorMessage(responseBody);
        cleanup();
        return Error{ErrorCode::PermissionDenied, message};
    }

    if (!cloudflareCallSucceeded(responseBody)) {
        std::string message = "Cloudflare API returned success=false: ";
        message += extractCloudflareErrorMessage(responseBody);
        cleanup();
        return Error{ErrorCode::PermissionDenied, message};
    }

    CloudflareApiResponse response;
    response.statusCode = httpStatus;
    response.body = std::move(responseBody);
    cleanup();
    return response;
}

Result<int> parsePositiveInt(const std::string& raw, const std::string& keyName) {
    try {
        const int value = std::stoi(raw);
        if (value <= 0) {
            return Error{ErrorCode::InvalidArgument, keyName + " must be > 0"};
        }
        return value;
    } catch (...) {
        return Error{ErrorCode::InvalidArgument,
                     "Invalid integer for " + keyName + ": '" + raw + "'"};
    }
}

struct R2TemporaryCredentials {
    std::string accessKeyId;
    std::string secretAccessKey;
    std::string sessionToken;
};

Result<std::string> resolveCloudflareTokenId(const std::string& accountId,
                                             const std::string& apiToken) {
    auto verify = callCloudflareApi(
        "GET", "https://api.cloudflare.com/client/v4/accounts/" + accountId + "/tokens/verify",
        apiToken);
    if (!verify) {
        return verify.error();
    }
    auto id = extractJsonStringField(verify.value().body, "id", true);
    if (!id || id->empty()) {
        return Error{ErrorCode::InvalidData, "Cloudflare token verify response missing result.id"};
    }
    return *id;
}

Result<R2TemporaryCredentials>
requestR2TemporaryCredentials(const std::string& accountId, const std::string& bucket,
                              const std::string& apiToken, const std::string& parentAccessKeyId,
                              const std::string& permission, int ttlSeconds) {
    std::string payload = "{"
                          "\"bucket\":\"" +
                          bucket + "\"," + "\"parentAccessKeyId\":\"" + parentAccessKeyId + "\"," +
                          "\"permission\":\"" + permission + "\"," +
                          "\"ttlSeconds\":" + std::to_string(ttlSeconds) + "}";

    auto response = callCloudflareApi("POST",
                                      "https://api.cloudflare.com/client/v4/accounts/" + accountId +
                                          "/r2/temp-access-credentials",
                                      apiToken, &payload);
    if (!response) {
        return response.error();
    }

    auto accessKeyId = extractJsonStringField(response.value().body, "accessKeyId", true);
    auto secretAccessKey = extractJsonStringField(response.value().body, "secretAccessKey", true);
    auto sessionToken = extractJsonStringField(response.value().body, "sessionToken", true);
    if (!accessKeyId || !secretAccessKey || !sessionToken) {
        return Error{ErrorCode::InvalidData,
                     "Cloudflare temp-credentials response missing expected credential fields"};
    }

    R2TemporaryCredentials creds;
    creds.accessKeyId = *accessKeyId;
    creds.secretAccessKey = *secretAccessKey;
    creds.sessionToken = *sessionToken;
    return creds;
}

bool parseBool(std::string raw) {
    raw = toLower(trim(std::move(raw)));
    return raw == "1" || raw == "true" || raw == "yes" || raw == "on";
}

std::map<std::string, std::string> parseSimpleToml(const std::filesystem::path& path) {
    std::map<std::string, std::string> config;
    std::ifstream file(path);
    if (!file) {
        return config;
    }

    std::string line;
    std::string currentSection;
    while (std::getline(file, line)) {
        line = trim(std::move(line));
        if (line.empty() || line[0] == '#') {
            continue;
        }

        if (line.front() == '[' && line.back() == ']') {
            currentSection = trim(line.substr(1, line.size() - 2));
            if (!currentSection.empty()) {
                currentSection.push_back('.');
            }
            continue;
        }

        auto eq = line.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        std::string key = trim(line.substr(0, eq));
        std::string value = trim(line.substr(eq + 1));

        bool inQuote = false;
        for (size_t i = 0; i < value.size(); ++i) {
            if (value[i] == '"' || value[i] == '\'') {
                inQuote = !inQuote;
            } else if (value[i] == '#' && !inQuote) {
                value = trim(value.substr(0, i));
                break;
            }
        }
        if (value.size() >= 2) {
            const char first = value.front();
            const char last = value.back();
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                value = value.substr(1, value.size() - 2);
            }
        }

        config[currentSection + key] = value;
    }

    return config;
}

std::string getOrDefault(const std::map<std::string, std::string>& cfg, const std::string& key,
                         const std::string& fallback = {}) {
    auto it = cfg.find(key);
    if (it == cfg.end()) {
        return fallback;
    }
    return trim(it->second);
}

} // namespace

Result<std::string> loadCloudflareApiTokenFromKeychain(std::string_view accountIdView) {
#if defined(__APPLE__) && TARGET_OS_OSX
    const std::string accountId = trim(std::string(accountIdView));
    if (accountId.empty()) {
        return Error{ErrorCode::InvalidArgument,
                     "Account id is required for keychain token lookup"};
    }

    constexpr const char* kService = "ai.yams.r2.api_token";
    UInt32 passwordLen = 0;
    void* passwordData = nullptr;
    OSStatus status = SecKeychainFindGenericPassword(
        nullptr, static_cast<UInt32>(std::char_traits<char>::length(kService)), kService,
        static_cast<UInt32>(accountId.size()), accountId.c_str(), &passwordLen, &passwordData,
        nullptr);
    if (status == errSecItemNotFound) {
        return Error{ErrorCode::NotFound, "No keychain token found for account id " + accountId};
    }
    if (status != errSecSuccess) {
        return Error{ErrorCode::PermissionDenied,
                     "Keychain lookup failed (status " + std::to_string(status) + ")"};
    }

    std::string token;
    if (passwordData && passwordLen > 0) {
        token.assign(static_cast<const char*>(passwordData), passwordLen);
    }
    if (passwordData) {
        SecKeychainItemFreeContent(nullptr, passwordData);
    }

    token = trim(std::move(token));
    if (token.empty()) {
        return Error{ErrorCode::InvalidData,
                     "Keychain token entry is empty for account id " + accountId};
    }
    return token;
#else
    (void)accountIdView;
    return Error{ErrorCode::NotSupported, "Keychain token lookup is only supported on macOS"};
#endif
}

Result<void> storeCloudflareApiTokenInKeychain(std::string_view accountIdView,
                                               std::string_view apiTokenView) {
#if defined(__APPLE__) && TARGET_OS_OSX
    const std::string accountId = trim(std::string(accountIdView));
    const std::string apiToken = trim(std::string(apiTokenView));
    if (accountId.empty()) {
        return Error{ErrorCode::InvalidArgument,
                     "Account id is required for keychain token storage"};
    }
    if (apiToken.empty()) {
        return Error{ErrorCode::InvalidArgument,
                     "API token is required for keychain token storage"};
    }

    constexpr const char* kService = "ai.yams.r2.api_token";
    OSStatus status = SecKeychainAddGenericPassword(
        nullptr, static_cast<UInt32>(std::char_traits<char>::length(kService)), kService,
        static_cast<UInt32>(accountId.size()), accountId.c_str(),
        static_cast<UInt32>(apiToken.size()), apiToken.data(), nullptr);
    if (status == errSecDuplicateItem) {
        SecKeychainItemRef itemRef = nullptr;
        status = SecKeychainFindGenericPassword(
            nullptr, static_cast<UInt32>(std::char_traits<char>::length(kService)), kService,
            static_cast<UInt32>(accountId.size()), accountId.c_str(), nullptr, nullptr, &itemRef);
        if (status != errSecSuccess || itemRef == nullptr) {
            return Error{ErrorCode::PermissionDenied,
                         "Keychain update lookup failed (status " + std::to_string(status) + ")"};
        }
        status = SecKeychainItemModifyAttributesAndData(
            itemRef, nullptr, static_cast<UInt32>(apiToken.size()), apiToken.data());
        CFRelease(itemRef);
    }

    if (status != errSecSuccess) {
        return Error{ErrorCode::PermissionDenied,
                     "Keychain token storage failed (status " + std::to_string(status) + ")"};
    }
    return {};
#else
    (void)accountIdView;
    (void)apiTokenView;
    return Error{ErrorCode::NotSupported, "Keychain token storage is only supported on macOS"};
#endif
}

bool looksLikeCloudflareApiBearerToken(std::string_view value) {
    std::string raw = trim(std::string(value));
    if (raw.empty()) {
        return false;
    }

    std::string lowered = toLower(raw);
    if (lowered.rfind("bearer ", 0) == 0) {
        return true;
    }

    std::string token = stripBearerPrefix(raw);
    if (token.size() < 30) {
        return false;
    }

    const bool hasTokenSeparator =
        token.find('-') != std::string::npos || token.find('.') != std::string::npos;
    if (!hasTokenSeparator) {
        return false;
    }

    return std::all_of(token.begin(), token.end(), [](unsigned char c) {
        return std::isalnum(c) != 0 || c == '-' || c == '_' || c == '.';
    });
}

std::string extractCloudflareR2AccountId(std::string_view endpointHost) {
    std::string host = toLower(normalizeS3Endpoint(std::string(endpointHost)));
    if (host.empty()) {
        return {};
    }

    constexpr std::string_view kDefaultSuffix = ".r2.cloudflarestorage.com";
    constexpr std::string_view kEuSuffix = ".eu.r2.cloudflarestorage.com";
    constexpr std::string_view kFedrampSuffix = ".fedramp.r2.cloudflarestorage.com";

    std::string accountLabel;
    if (endsWith(host, kDefaultSuffix)) {
        accountLabel = host.substr(0, host.size() - kDefaultSuffix.size());
    } else if (endsWith(host, kEuSuffix)) {
        accountLabel = host.substr(0, host.size() - kEuSuffix.size());
    } else if (endsWith(host, kFedrampSuffix)) {
        accountLabel = host.substr(0, host.size() - kFedrampSuffix.size());
    } else {
        return {};
    }

    if (accountLabel.find('.') != std::string::npos || !isHexAccountId(accountLabel)) {
        return {};
    }
    return accountLabel;
}

bool isCloudflareR2EndpointHost(std::string_view endpointHost) {
    return !extractCloudflareR2AccountId(endpointHost).empty();
}

std::optional<RemoteFallbackPolicy> parseRemoteFallbackPolicy(std::string value) {
    value = toLower(trim(std::move(value)));
    if (value.empty() || value == "strict") {
        return RemoteFallbackPolicy::Strict;
    }
    if (value == "fallback_local_if_configured") {
        return RemoteFallbackPolicy::FallbackLocalIfConfigured;
    }
    return std::nullopt;
}

const char* toString(RemoteFallbackPolicy policy) {
    switch (policy) {
        case RemoteFallbackPolicy::Strict:
            return "strict";
        case RemoteFallbackPolicy::FallbackLocalIfConfigured:
            return "fallback_local_if_configured";
    }
    return "strict";
}

Result<StorageBootstrapDecision>
resolveStorageBootstrapDecision(const std::filesystem::path& configPath,
                                const std::filesystem::path& requestedDataDir) {
    StorageBootstrapDecision decision;
    decision.requestedDataDir = requestedDataDir;
    decision.activeDataDir = requestedDataDir;

    const auto cfg = parseSimpleToml(configPath);
    std::string configuredEngine = toLower(getOrDefault(cfg, "storage.engine", "local"));
    if (configuredEngine.empty()) {
        configuredEngine = "local";
    }
    decision.configuredEngine = configuredEngine;

    const auto fallbackPolicyRaw = getOrDefault(cfg, "storage.s3.fallback_policy", "strict");
    auto fallbackPolicy = parseRemoteFallbackPolicy(fallbackPolicyRaw);
    if (!fallbackPolicy) {
        return Error{ErrorCode::InvalidArgument,
                     "Invalid storage.s3.fallback_policy: '" + fallbackPolicyRaw + "'"};
    }
    decision.fallbackPolicy = *fallbackPolicy;

    std::string fallbackRaw = getOrDefault(cfg, "storage.s3.fallback_local_data_dir", "");
    fallbackRaw = expandTilde(std::move(fallbackRaw));
    if (!fallbackRaw.empty()) {
        decision.fallbackLocalDataDir = std::filesystem::path(fallbackRaw);
    }

    if (configuredEngine == "local" || configuredEngine == "filesystem") {
        decision.activeEngine = "local";
        return decision;
    }

    if (configuredEngine != "s3") {
        return Error{ErrorCode::InvalidArgument,
                     "Unsupported storage.engine value: '" + configuredEngine + "'"};
    }

    BackendConfig backendConfig;
    backendConfig.type = "s3";
    backendConfig.url = getOrDefault(cfg, "storage.s3.url", "");
    backendConfig.region = getOrDefault(cfg, "storage.s3.region", "us-east-1");
    backendConfig.usePathStyle = parseBool(getOrDefault(cfg, "storage.s3.use_path_style", "false"));
    const auto endpointHost = normalizeS3Endpoint(getOrDefault(cfg, "storage.s3.endpoint", ""));
    backendConfig.credentials["endpoint"] = endpointHost;

    auto failWithOrFallback = [&](const std::string& reason) -> Result<StorageBootstrapDecision> {
        if (decision.fallbackPolicy == RemoteFallbackPolicy::FallbackLocalIfConfigured &&
            !decision.fallbackLocalDataDir.empty()) {
            decision.activeEngine = "local";
            decision.activeDataDir = decision.fallbackLocalDataDir;
            decision.fallbackTriggered = true;
            decision.fallbackReason = reason;
            return decision;
        }

        if (decision.fallbackPolicy == RemoteFallbackPolicy::FallbackLocalIfConfigured &&
            decision.fallbackLocalDataDir.empty()) {
            return Error{
                ErrorCode::InvalidState,
                reason + " (fallback requested but storage.s3.fallback_local_data_dir is empty)"};
        }

        return Error{ErrorCode::InvalidState, reason};
    };

    if (backendConfig.url.empty()) {
        return failWithOrFallback("storage.engine is 's3' but storage.s3.url is not configured");
    }

    const auto r2AuthModeRaw = getOrDefault(cfg, "storage.s3.r2.auth_mode",
                                            getOrDefault(cfg, "storage.s3.r2_auth_mode", "direct"));
    std::string r2AuthMode = toLower(trim(r2AuthModeRaw));
    if (r2AuthMode.empty()) {
        r2AuthMode = "direct";
    }
    if (r2AuthMode != "direct" && r2AuthMode != "temp_credentials") {
        return Error{ErrorCode::InvalidArgument, "Invalid storage.s3.r2.auth_mode: '" +
                                                     r2AuthModeRaw +
                                                     "' (expected 'direct' or 'temp_credentials')"};
    }

    const bool isR2Endpoint = isCloudflareR2EndpointHost(endpointHost);

    const auto accessKey = getOrDefault(cfg, "storage.s3.access_key", "");
    const auto secretKey = getOrDefault(cfg, "storage.s3.secret_key", "");
    const auto sessionToken = getOrDefault(cfg, "storage.s3.session_token", "");

    if (r2AuthMode == "temp_credentials") {
        if (!isR2Endpoint) {
            return failWithOrFallback(
                "storage.s3.r2.auth_mode is 'temp_credentials' but storage.s3.endpoint is not a "
                "Cloudflare R2 endpoint");
        }

        const auto bucket = parseS3BucketFromUrl(backendConfig.url);
        if (!bucket) {
            return failWithOrFallback(
                "storage.s3.r2.auth_mode is 'temp_credentials' but storage.s3.url is missing a "
                "valid bucket (expected s3://<bucket>/<optional-prefix>)");
        }

        std::string accountId = getOrDefault(cfg, "storage.s3.r2.account_id",
                                             getOrDefault(cfg, "storage.s3.r2_account_id", ""));
        accountId = trim(std::move(accountId));
        const std::string endpointAccountId = extractCloudflareR2AccountId(endpointHost);
        if (!accountId.empty() && !endpointAccountId.empty() && accountId != endpointAccountId) {
            return failWithOrFallback(
                "storage.s3.r2.account_id does not match account id in storage.s3.endpoint "
                "(" +
                accountId + " vs " + endpointAccountId + ")");
        }
        if (accountId.empty()) {
            accountId = endpointAccountId;
        }
        if (!isHexAccountId(accountId)) {
            return failWithOrFallback(
                "storage.s3.r2.auth_mode is 'temp_credentials' but account_id is missing or "
                "invalid (set storage.s3.r2.account_id or use a canonical R2 endpoint)");
        }

        std::string apiToken = getOrDefault(cfg, "storage.s3.r2.api_token",
                                            getOrDefault(cfg, "storage.s3.r2_api_token", ""));
        if (apiToken.empty()) {
            if (const char* envToken = std::getenv("YAMS_R2_API_TOKEN"); envToken && *envToken) {
                apiToken = envToken;
            }
        }
        if (apiToken.empty()) {
            if (const char* cfToken = std::getenv("CLOUDFLARE_API_TOKEN"); cfToken && *cfToken) {
                apiToken = cfToken;
            }
        }
        if (apiToken.empty()) {
            auto keychainToken = loadCloudflareApiTokenFromKeychain(accountId);
            if (keychainToken) {
                apiToken = keychainToken.value();
            } else if (keychainToken.error().code != ErrorCode::NotFound &&
                       keychainToken.error().code != ErrorCode::NotSupported) {
                return failWithOrFallback("Failed to read Cloudflare API token from keychain: " +
                                          keychainToken.error().message);
            }
        }
        apiToken = trim(std::move(apiToken));
        if (apiToken.empty()) {
            return failWithOrFallback(
                "storage.s3.r2.auth_mode is 'temp_credentials' but no API token is configured "
                "(set storage.s3.r2.api_token, YAMS_R2_API_TOKEN/CLOUDFLARE_API_TOKEN, or "
                "store a keychain token for this account)");
        }

        std::string parentAccessKeyId =
            getOrDefault(cfg, "storage.s3.r2.parent_access_key_id",
                         getOrDefault(cfg, "storage.s3.r2_parent_access_key_id", ""));
        parentAccessKeyId = trim(std::move(parentAccessKeyId));
        if (parentAccessKeyId.empty()) {
            auto parentId = resolveCloudflareTokenId(accountId, apiToken);
            if (!parentId) {
                return failWithOrFallback(
                    "Failed to resolve Cloudflare token id for R2 temp credentials: " +
                    parentId.error().message);
            }
            parentAccessKeyId = parentId.value();
        }

        std::string permission = toLower(
            trim(getOrDefault(cfg, "storage.s3.r2.permission",
                              getOrDefault(cfg, "storage.s3.r2_permission", "object-read-write"))));
        if (permission.empty()) {
            permission = "object-read-write";
        }
        if (permission != "object-read-write" && permission != "object-read-only") {
            return Error{ErrorCode::InvalidArgument,
                         "Invalid storage.s3.r2.permission: '" + permission +
                             "' (expected 'object-read-write' or 'object-read-only')"};
        }

        std::string ttlRaw = getOrDefault(cfg, "storage.s3.r2.ttl_seconds",
                                          getOrDefault(cfg, "storage.s3.r2_ttl_seconds", "3600"));
        ttlRaw = trim(std::move(ttlRaw));
        if (ttlRaw.empty()) {
            ttlRaw = "3600";
        }
        auto ttlSeconds = parsePositiveInt(ttlRaw, "storage.s3.r2.ttl_seconds");
        if (!ttlSeconds) {
            return ttlSeconds.error();
        }

        auto tempCreds = requestR2TemporaryCredentials(
            accountId, bucket.value(), apiToken, parentAccessKeyId, permission, ttlSeconds.value());
        if (!tempCreds) {
            return failWithOrFallback("Failed to acquire Cloudflare R2 temporary credentials: " +
                                      tempCreds.error().message);
        }

        backendConfig.credentials["access_key"] = tempCreds.value().accessKeyId;
        backendConfig.credentials["secret_key"] = tempCreds.value().secretAccessKey;
        backendConfig.credentials["session_token"] = tempCreds.value().sessionToken;
        if (toLower(backendConfig.region) == "us-east-1") {
            backendConfig.region = "auto";
        }
    } else {
        if (isR2Endpoint && looksLikeCloudflareApiBearerToken(accessKey)) {
            return failWithOrFallback(
                "storage.s3.access_key looks like a Cloudflare API bearer token. Use "
                "storage.s3.r2.auth_mode='temp_credentials' with storage.s3.r2.api_token "
                "instead of placing bearer tokens in storage.s3.access_key");
        }
        if (!accessKey.empty()) {
            backendConfig.credentials["access_key"] = accessKey;
        }
        if (!secretKey.empty()) {
            backendConfig.credentials["secret_key"] = secretKey;
        }
        if (!sessionToken.empty()) {
            backendConfig.credentials["session_token"] = sessionToken;
        }
    }

    auto backend = StorageBackendFactory::create(backendConfig);
    if (!backend) {
        return failWithOrFallback("Failed to initialize configured S3 backend");
    }

    auto engine = createStorageEngineFromBackend(std::move(backend));
    if (!engine) {
        return failWithOrFallback(
            "Failed to create storage engine adapter for configured S3 backend");
    }

    decision.activeEngine = "s3";
    decision.storageEngineOverride = std::move(engine);
    return decision;
}

} // namespace yams::storage
