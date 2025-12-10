/**
 * @file plugin_repo_client.cpp
 * @brief Implementation of the plugin repository client
 */

#include <yams/plugins/plugin_repo_client.hpp>

#include <cstdlib>
#include <iomanip>
#include <sstream>

#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

namespace yams::plugins {

namespace {

// cURL write callback
size_t writeCallback(char* ptr, size_t size, size_t nmemb, std::string* data) {
    data->append(ptr, size * nmemb);
    return size * nmemb;
}

class CurlHandle {
public:
    CurlHandle() : curl_(curl_easy_init()) {}
    ~CurlHandle() {
        if (curl_)
            curl_easy_cleanup(curl_);
    }
    CurlHandle(const CurlHandle&) = delete;
    CurlHandle& operator=(const CurlHandle&) = delete;

    CURL* get() const { return curl_; }
    explicit operator bool() const { return curl_ != nullptr; }

private:
    CURL* curl_;
};

} // namespace

class PluginRepoClientImpl : public IPluginRepoClient {
public:
    explicit PluginRepoClientImpl(PluginRepoConfig config)
        : config_(std::move(config)) {
        // Allow environment variable override
        if (const char* envUrl = std::getenv("YAMS_PLUGIN_REPO_URL")) {
            config_.repoUrl = envUrl;
        }
        // Remove trailing slash
        while (!config_.repoUrl.empty() && config_.repoUrl.back() == '/') {
            config_.repoUrl.pop_back();
        }
    }

    Result<std::vector<RemotePluginSummary>> list(const std::string& filter) override {
        auto url = config_.repoUrl + "/api/v1/plugins";
        if (!filter.empty()) {
            url += "?q=" + urlEncode(filter);
        }

        auto resp = httpGet(url);
        if (!resp) {
            return resp.error();
        }

        try {
            auto json = nlohmann::json::parse(resp.value());
            std::vector<RemotePluginSummary> results;

            auto plugins = json.value("plugins", nlohmann::json::array());
            for (const auto& p : plugins) {
                RemotePluginSummary summary;
                summary.name = p.value("name", "");
                summary.latestVersion = p.value("version", "");
                summary.description = p.value("description", "");
                summary.downloads = p.value("downloads", 0ULL);

                if (p.contains("interfaces") && p["interfaces"].is_array()) {
                    for (const auto& iface : p["interfaces"]) {
                        if (iface.is_string()) {
                            summary.interfaces.push_back(iface.get<std::string>());
                        }
                    }
                }
                results.push_back(std::move(summary));
            }
            return results;
        } catch (const std::exception& e) {
            return Error{ErrorCode::InvalidData, std::string("Failed to parse plugin list: ") + e.what()};
        }
    }

    Result<RemotePluginInfo> get(
        const std::string& name,
        const std::optional<std::string>& version) override {
        std::string url;
        if (version) {
            url = config_.repoUrl + "/api/v1/plugins/" + urlEncode(name) + "/" + urlEncode(*version);
        } else {
            url = config_.repoUrl + "/api/v1/plugins/" + urlEncode(name) + "/latest";
        }

        auto resp = httpGet(url);
        if (!resp) {
            return resp.error();
        }

        try {
            auto json = nlohmann::json::parse(resp.value());
            return parsePluginInfo(json, name);
        } catch (const std::exception& e) {
            return Error{ErrorCode::InvalidData, std::string("Failed to parse plugin info: ") + e.what()};
        }
    }

    Result<std::vector<std::string>> versions(const std::string& name) override {
        auto url = config_.repoUrl + "/api/v1/plugins/" + urlEncode(name) + "/versions";

        auto resp = httpGet(url);
        if (!resp) {
            return resp.error();
        }

        try {
            auto json = nlohmann::json::parse(resp.value());
            std::vector<std::string> results;

            auto versions = json.value("versions", nlohmann::json::array());
            for (const auto& v : versions) {
                if (v.is_string()) {
                    results.push_back(v.get<std::string>());
                }
            }
            return results;
        } catch (const std::exception& e) {
            return Error{ErrorCode::InvalidData, std::string("Failed to parse versions: ") + e.what()};
        }
    }

    Result<bool> exists(const std::string& name) override {
        auto url = config_.repoUrl + "/api/v1/plugins/" + urlEncode(name);
        auto resp = httpGet(url);
        if (!resp) {
            if (resp.error().code == ErrorCode::NotFound) {
                return false;
            }
            return resp.error();
        }
        return true;
    }

private:
    Result<std::string> httpGet(const std::string& url) {
        CurlHandle curl;
        if (!curl) {
            return Error{ErrorCode::NetworkError, "Failed to initialize cURL"};
        }

        std::string responseData;
        long httpCode = 0;

        curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &responseData);
        curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT_MS, config_.timeout.count());
        curl_easy_setopt(curl.get(), CURLOPT_USERAGENT, config_.userAgent.c_str());
        curl_easy_setopt(curl.get(), CURLOPT_FOLLOWLOCATION, 1L);

        if (!config_.verifyTls) {
            curl_easy_setopt(curl.get(), CURLOPT_SSL_VERIFYPEER, 0L);
            curl_easy_setopt(curl.get(), CURLOPT_SSL_VERIFYHOST, 0L);
        }

        if (config_.proxy) {
            curl_easy_setopt(curl.get(), CURLOPT_PROXY, config_.proxy->c_str());
        }

        // Accept JSON
        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Accept: application/json");
        curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers);

        CURLcode res = curl_easy_perform(curl.get());
        curl_slist_free_all(headers);

        if (res != CURLE_OK) {
            return Error{ErrorCode::NetworkError,
                         std::string("HTTP request failed: ") + curl_easy_strerror(res)};
        }

        curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &httpCode);

        if (httpCode == 404) {
            return Error{ErrorCode::NotFound, "Plugin not found"};
        }
        if (httpCode >= 400) {
            std::ostringstream msg;
            msg << "HTTP error " << httpCode;
            // Try to extract error message from response
            try {
                auto json = nlohmann::json::parse(responseData);
                if (json.contains("error")) {
                    msg << ": " << json["error"].get<std::string>();
                }
            } catch (...) {}
            return Error{ErrorCode::NetworkError, msg.str()};
        }

        return responseData;
    }

    RemotePluginInfo parsePluginInfo(const nlohmann::json& json, const std::string& fallbackName) {
        RemotePluginInfo info;
        info.name = json.value("name", fallbackName);
        info.version = json.value("version", "");
        info.description = json.value("description", "");
        info.author = json.value("author", "");
        info.license = json.value("license", "");
        info.minYamsVersion = json.value("min_yams_version", "");
        info.downloadUrl = json.value("download_url", "");
        info.checksum = json.value("checksum", "");
        info.sizeBytes = json.value("size", 0ULL);
        info.platform = json.value("platform", "");
        info.arch = json.value("arch", "");
        info.abiVersion = json.value("abi_version", 1);
        info.publishedAt = json.value("published_at", "");
        info.updatedAt = json.value("updated_at", "");
        info.downloads = json.value("downloads", 0ULL);

        // If download_url is not provided, construct it
        if (info.downloadUrl.empty() && !info.name.empty() && !info.version.empty()) {
            info.downloadUrl = config_.repoUrl + "/plugins/" + info.name + "/" +
                               info.version + "/" + info.name + "-" + info.version + ".tar.gz";
        }

        if (json.contains("interfaces") && json["interfaces"].is_array()) {
            for (const auto& iface : json["interfaces"]) {
                if (iface.is_string()) {
                    info.interfaces.push_back(iface.get<std::string>());
                }
            }
        }

        return info;
    }

    static std::string urlEncode(const std::string& value) {
        std::ostringstream escaped;
        escaped.fill('0');
        escaped << std::hex;

        for (char c : value) {
            if (std::isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_' ||
                c == '.' || c == '~') {
                escaped << c;
            } else {
                escaped << std::uppercase;
                escaped << '%' << std::setw(2) << int(static_cast<unsigned char>(c));
                escaped << std::nouppercase;
            }
        }
        return escaped.str();
    }

    PluginRepoConfig config_;
};

std::unique_ptr<IPluginRepoClient> makePluginRepoClient(const PluginRepoConfig& config) {
    return std::make_unique<PluginRepoClientImpl>(config);
}

} // namespace yams::plugins
