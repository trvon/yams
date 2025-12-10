// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: Apache-2.0

#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/daemon.h>
#include <yams/vector/sqlite_vec_backend.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <system_error>
#include <vector>

namespace yams::daemon {

bool ConfigResolver::envTruthy(const char* value) {
    if (!value || !*value) {
        return false;
    }
    std::string v(value);
    std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c) { return std::tolower(c); });
    return !(v == "0" || v == "false" || v == "off" || v == "no");
}

std::filesystem::path ConfigResolver::resolveDefaultConfigPath() {
    if (const char* explicitPath = std::getenv("YAMS_CONFIG_PATH")) {
        std::filesystem::path p{explicitPath};
        if (std::filesystem::exists(p))
            return p;
    }
#ifdef _WIN32
    // Windows: prefer roaming APPDATA for config (matches get_config_dir())
    if (const char* appdata = std::getenv("APPDATA")) {
        std::filesystem::path p = std::filesystem::path(appdata) / "yams" / "config.toml";
        if (std::filesystem::exists(p))
            return p;
    }
#endif
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
#ifdef _WIN32
    // Windows: check LOCALAPPDATA
    if (const char* localAppData = std::getenv("LOCALAPPDATA")) {
        std::filesystem::path p = std::filesystem::path(localAppData) / "yams" / "config.toml";
        if (std::filesystem::exists(p))
            return p;
    }
#endif
    return {};
}

std::map<std::string, std::string>
ConfigResolver::parseSimpleTomlFlat(const std::filesystem::path& path) {
    std::map<std::string, std::string> config;
    std::ifstream file(path);
    if (!file)
        return config;

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
            config[currentSection + "." + key] = value;
        } else {
            config[key] = value;
        }
    }
    return config;
}

std::optional<size_t> ConfigResolver::readDbEmbeddingDim(const std::filesystem::path& dbPath) {
    try {
        namespace fs = std::filesystem;
        if (dbPath.empty() || !fs::exists(dbPath))
            return std::nullopt;
        yams::vector::SqliteVecBackend backend;
        auto r = backend.initialize(dbPath.string());
        if (!r)
            return std::nullopt;
        auto dimOpt = backend.getStoredEmbeddingDimension();
        backend.close();
        if (dimOpt && *dimOpt > 0)
            return dimOpt;
    } catch (const std::exception& e) {
        spdlog::debug("Failed to read embedding dimension from {}: {}", dbPath.string(), e.what());
    } catch (...) {
        spdlog::debug("Failed to read embedding dimension from {}: unknown error", dbPath.string());
    }
    return std::nullopt;
}

std::optional<size_t> ConfigResolver::readVectorSentinelDim(const std::filesystem::path& dataDir) {
    try {
        namespace fs = std::filesystem;
        auto p = dataDir / "vectors_sentinel.json";
        if (!fs::exists(p))
            return std::nullopt;
        std::ifstream in(p);
        if (!in)
            return std::nullopt;
        nlohmann::json j;
        in >> j;
        if (j.contains("embedding_dim"))
            return j["embedding_dim"].get<size_t>();
    } catch (const std::exception& e) {
        spdlog::debug("Failed to read vector sentinel: {}", e.what());
    } catch (...) {
        spdlog::debug("Failed to read vector sentinel: unknown error");
    }
    return std::nullopt;
}

void ConfigResolver::writeVectorSentinel(const std::filesystem::path& dataDir, size_t dim,
                                         const std::string& /*tableName*/, int schemaVersion) {
    try {
        namespace fs = std::filesystem;
        fs::create_directories(dataDir);
        nlohmann::json j;
        j["embedding_dim"] = dim;
        j["schema_version"] = schemaVersion;
        j["written_at"] = std::time(nullptr);
        std::ofstream out(dataDir / "vectors_sentinel.json");
        if (out)
            out << j.dump(2);
    } catch (const std::exception& e) {
        spdlog::debug("Failed to write vector sentinel: {}", e.what());
    } catch (...) {
        spdlog::debug("Failed to write vector sentinel: unknown error");
    }
}

bool ConfigResolver::detectEmbeddingPreloadFlag(const DaemonConfig& config) {
    bool flag = false;

    // Config file precedence
    std::filesystem::path cfgPath = config.configFilePath;
    if (cfgPath.empty())
        cfgPath = resolveDefaultConfigPath();
    if (!cfgPath.empty()) {
        try {
            auto kv = parseSimpleTomlFlat(cfgPath);
            auto it = kv.find("embeddings.preload_on_startup");
            if (it != kv.end()) {
                std::string lower = it->second;
                std::transform(lower.begin(), lower.end(), lower.begin(),
                               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                flag = (lower == "1" || lower == "true" || lower == "yes" || lower == "on");
            }
        } catch (const std::exception& e) {
            spdlog::debug("[Warmup] failed to read config for preload flag: {}", e.what());
        }
    }

    // Environment override wins
    if (const char* env = std::getenv("YAMS_EMBED_PRELOAD_ON_STARTUP")) {
        flag = envTruthy(env);
    }

    return flag;
}

std::string ConfigResolver::resolvePreferredModel(const DaemonConfig& config,
                                                  const std::filesystem::path& resolvedDataDir) {
    std::string preferred;

    if (const char* envp = std::getenv("YAMS_PREFERRED_MODEL")) {
        preferred = envp;
        if (!preferred.empty()) {
            spdlog::debug("Preferred model from environment: {}", preferred);
            return preferred;
        }
    }

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath =
            !config.configFilePath.empty() ? config.configFilePath : resolveDefaultConfigPath();
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            // Flat parse for explicit keys
            auto kv = parseSimpleTomlFlat(cfgPath);
            auto it = kv.find("embeddings.preferred_model");
            if (it != kv.end() && !it->second.empty()) {
                preferred = it->second;
                spdlog::debug("Preferred model from config: {}", preferred);
                return preferred;
            }

            // daemon.models.preload_models -> take the first known value
            auto preload = kv.find("daemon.models.preload_models");
            if (preload != kv.end()) {
                const auto& v = preload->second;
                if (v.find("all-MiniLM-L6-v2") != std::string::npos) {
                    preferred = "all-MiniLM-L6-v2";
                } else if (v.find("all-mpnet-base-v2") != std::string::npos) {
                    preferred = "all-mpnet-base-v2";
                }
                if (!preferred.empty()) {
                    spdlog::debug("Preferred model from config preload list: {}", preferred);
                    return preferred;
                }
            }

            // Fallback: scan lines to catch cases the flat parser misses
            std::ifstream in(cfgPath);
            std::string line;
            auto trim = [](std::string& t) {
                if (t.empty())
                    return;
                t.erase(0, t.find_first_not_of(" \t"));
                auto p = t.find_last_not_of(" \t");
                if (p != std::string::npos)
                    t.erase(p + 1);
            };

            while (std::getline(in, line)) {
                std::string l = line;
                trim(l);
                if (l.empty() || l[0] == '#')
                    continue;

                if (l.find("embeddings.preferred_model") != std::string::npos) {
                    auto eq = l.find('=');
                    if (eq != std::string::npos) {
                        std::string v = l.substr(eq + 1);
                        trim(v);
                        if (!v.empty() && v.front() == '"' && v.back() == '"')
                            v = v.substr(1, v.size() - 2);
                        preferred = v;
                    }
                    if (!preferred.empty()) {
                        spdlog::debug("Preferred model from config: {}", preferred);
                        return preferred;
                    }
                }
                // daemon.models.preload_models -> take the first
                if (l.find("daemon.models.preload_models") != std::string::npos) {
                    auto eq = l.find('=');
                    if (eq != std::string::npos) {
                        std::string v = l.substr(eq + 1);
                        trim(v);
                        if (v.find("all-MiniLM-L6-v2") != std::string::npos) {
                            preferred = "all-MiniLM-L6-v2";
                        } else if (v.find("all-mpnet-base-v2") != std::string::npos) {
                            preferred = "all-mpnet-base-v2";
                        }
                    }
                    if (!preferred.empty()) {
                        spdlog::debug("Preferred model from config preload list: {}", preferred);
                        return preferred;
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for preferred model: {}", e.what());
    }

    try {
        if (!resolvedDataDir.empty()) {
            namespace fs = std::filesystem;
            fs::path models = resolvedDataDir / "models";
            std::error_code ec;
            if (fs::exists(models, ec) && fs::is_directory(models, ec)) {
                [[maybe_unused]] size_t dbDim = 0;
                try {
                    // Prefer sentinel dim when available
                    if (auto s = readVectorSentinelDim(resolvedDataDir))
                        dbDim = *s;
                } catch (...) {
                }
                std::vector<std::string> preferences;
                (void)dbDim;

                for (const auto& pref : preferences) {
                    fs::path modelPath = models / pref;
                    if (fs::exists(modelPath / "model.onnx", ec)) {
                        spdlog::debug("Auto-detected preferred model: {}", pref);
                        return pref;
                    }
                }

                // If no preferred model found, use the first available
                for (const auto& e : fs::directory_iterator(models, ec)) {
                    if (e.is_directory() && fs::exists(e.path() / "model.onnx", ec)) {
                        preferred = e.path().filename().string();
                        spdlog::debug("Using first available model: {}", preferred);
                        return preferred;
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error auto-detecting models: {}", e.what());
    }

    return preferred;
}

bool ConfigResolver::isSymbolExtractionEnabled(const DaemonConfig& config) {
    bool enableSymbols = true;

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath =
            !config.configFilePath.empty() ? config.configFilePath : resolveDefaultConfigPath();
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            auto flat = parseSimpleTomlFlat(cfgPath);
            auto it = flat.find("plugins.symbol_extraction.enable");
            if (it != flat.end()) {
                std::string v = it->second;
                std::transform(v.begin(), v.end(), v.begin(),
                               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                enableSymbols = !(v == "0" || v == "false" || v == "off" || v == "no");
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("[ConfigResolver] Failed to read symbol extraction flag: {}", e.what());
    } catch (...) {
        spdlog::debug("[ConfigResolver] Failed to read symbol extraction flag: unknown error");
    }

    return enableSymbols;
}

int ConfigResolver::readTimeoutMs(const char* envName, int defaultMs, int minMs) {
    try {
        if (const char* v = std::getenv(envName)) {
            int val = std::stoi(v);
            return std::max(minMs, val);
        }
    } catch (...) {
    }
    return defaultMs;
}

size_t ConfigResolver::readVectorMaxElements() {
    constexpr size_t kDefaultMaxElements = 100000;
    constexpr size_t kMinMaxElements = 1000;
    constexpr size_t kMaxMaxElements = 10000000; // 10M reasonable upper bound

    // 1. Environment variable takes precedence
    if (const char* env = std::getenv("YAMS_VECTOR_MAX_ELEMENTS")) {
        try {
            size_t val = std::stoull(env);
            if (val >= kMinMaxElements && val <= kMaxMaxElements) {
                spdlog::info("[ConfigResolver] Using YAMS_VECTOR_MAX_ELEMENTS={}", val);
                return val;
            }
            spdlog::warn(
                "[ConfigResolver] YAMS_VECTOR_MAX_ELEMENTS={} out of range [{}, {}], using default",
                val, kMinMaxElements, kMaxMaxElements);
        } catch (...) {
            spdlog::warn("[ConfigResolver] Invalid YAMS_VECTOR_MAX_ELEMENTS value, using default");
        }
    }

    // 2. Config file
    auto cfgPath = resolveDefaultConfigPath();
    if (!cfgPath.empty()) {
        try {
            auto kv = parseSimpleTomlFlat(cfgPath);
            auto it = kv.find("vector_database.max_elements");
            if (it != kv.end() && !it->second.empty()) {
                size_t val = std::stoull(it->second);
                if (val >= kMinMaxElements && val <= kMaxMaxElements) {
                    spdlog::info(
                        "[ConfigResolver] Using vector_database.max_elements={} from config", val);
                    return val;
                }
                spdlog::warn(
                    "[ConfigResolver] vector_database.max_elements={} out of range, using default",
                    val);
            }
        } catch (...) {
            // Ignore parse errors
        }
    }

    // 3. Default
    return kDefaultMaxElements;
}

} // namespace yams::daemon
