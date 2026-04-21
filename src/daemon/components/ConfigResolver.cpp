// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/common/fs_utils.h>
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

namespace {

std::optional<std::size_t> parseSize(const std::string& raw) {
    try {
        if (raw.empty()) {
            return std::nullopt;
        }
        return static_cast<std::size_t>(std::stoull(raw));
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<bool> parseBool01(const std::string& raw) {
    try {
        if (raw.empty()) {
            return std::nullopt;
        }
        return std::stoll(raw) != 0;
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<double> parseDouble(const std::string& raw) {
    try {
        if (raw.empty()) {
            return std::nullopt;
        }
        return std::stod(raw);
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<yams::vector::ChunkingStrategy> parseChunkingStrategy(const std::string& raw) {
    if (raw.empty()) {
        return std::nullopt;
    }
    std::string s = raw;
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

    if (s == "fixed" || s == "fixed_size") {
        return yams::vector::ChunkingStrategy::FIXED_SIZE;
    }
    if (s == "sentence" || s == "sentence_based") {
        return yams::vector::ChunkingStrategy::SENTENCE_BASED;
    }
    if (s == "paragraph" || s == "paragraph_based") {
        return yams::vector::ChunkingStrategy::PARAGRAPH_BASED;
    }
    if (s == "recursive" || s == "recursive_split") {
        return yams::vector::ChunkingStrategy::RECURSIVE;
    }
    if (s == "sliding" || s == "sliding_window") {
        return yams::vector::ChunkingStrategy::SLIDING_WINDOW;
    }
    if (s == "markdown" || s == "markdown_aware") {
        return yams::vector::ChunkingStrategy::MARKDOWN_AWARE;
    }
    return std::nullopt;
}

} // namespace

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
    // YAMS_CONFIG is the canonical env var used by test fixtures and
    // config_helpers.cpp; accept it here so resolver-based policies (topology
    // engine, routing, integration, ...) respect the harness-supplied TOML.
    if (const char* fixtureConfig = std::getenv("YAMS_CONFIG")) {
        std::filesystem::path p{fixtureConfig};
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
        yams::common::ensureDirectories(dataDir);
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

ConfigResolver::EmbeddingSelectionPolicy ConfigResolver::resolveEmbeddingSelectionPolicy() {
    EmbeddingSelectionPolicy policy{};

    auto parseStrategy = [](const std::string& raw) {
        std::string value = raw;
        std::transform(value.begin(), value.end(), value.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (value == "intro_headings" || value == "intro+headings" ||
            value == "intro_headings_only") {
            return EmbeddingSelectionPolicy::Strategy::IntroHeadings;
        }
        return EmbeddingSelectionPolicy::Strategy::Ranked;
    };

    auto parseMode = [](const std::string& raw) {
        std::string value = raw;
        std::transform(value.begin(), value.end(), value.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (value == "full") {
            return EmbeddingSelectionPolicy::Mode::Full;
        }
        if (value == "adaptive") {
            return EmbeddingSelectionPolicy::Mode::Adaptive;
        }
        return EmbeddingSelectionPolicy::Mode::Budgeted;
    };

    auto parseSize = [](const std::string& raw, std::size_t fallback) {
        try {
            return static_cast<std::size_t>(std::stoull(raw));
        } catch (...) {
            return fallback;
        }
    };

    auto parseDouble = [](const std::string& raw, double fallback) {
        try {
            return std::stod(raw);
        } catch (...) {
            return fallback;
        }
    };

    try {
        auto cfgPath = resolveDefaultConfigPath();
        if (!cfgPath.empty()) {
            auto kv = parseSimpleTomlFlat(cfgPath);
            if (auto it = kv.find("embeddings.selection.strategy"); it != kv.end()) {
                policy.strategy = parseStrategy(it->second);
            }
            if (auto it = kv.find("embeddings.selection.mode"); it != kv.end()) {
                policy.mode = parseMode(it->second);
            }
            if (auto it = kv.find("embeddings.selection.max_chunks_per_doc"); it != kv.end()) {
                policy.maxChunksPerDoc = parseSize(it->second, policy.maxChunksPerDoc);
            }
            if (auto it = kv.find("embeddings.selection.max_chars_per_doc"); it != kv.end()) {
                policy.maxCharsPerDoc = parseSize(it->second, policy.maxCharsPerDoc);
            }
            if (auto it = kv.find("embeddings.selection.heading_boost"); it != kv.end()) {
                policy.headingBoost = parseDouble(it->second, policy.headingBoost);
            }
            if (auto it = kv.find("embeddings.selection.intro_boost"); it != kv.end()) {
                policy.introBoost = parseDouble(it->second, policy.introBoost);
            }
        }
    } catch (...) {
    }

    // Env overrides (config component owns this precedence)
    if (const char* v = std::getenv("YAMS_EMBED_SELECTION_STRATEGY")) {
        policy.strategy = parseStrategy(v);
    }
    if (const char* v = std::getenv("YAMS_EMBED_SELECTION_MODE")) {
        policy.mode = parseMode(v);
    }
    if (const char* v = std::getenv("YAMS_EMBED_MAX_CHUNKS_PER_DOC")) {
        policy.maxChunksPerDoc = parseSize(v, policy.maxChunksPerDoc);
    }
    if (const char* v = std::getenv("YAMS_EMBED_MAX_CHARS_PER_DOC")) {
        policy.maxCharsPerDoc = parseSize(v, policy.maxCharsPerDoc);
    }
    if (const char* v = std::getenv("YAMS_EMBED_SELECTION_HEADING_BOOST")) {
        policy.headingBoost = parseDouble(v, policy.headingBoost);
    }
    if (const char* v = std::getenv("YAMS_EMBED_SELECTION_INTRO_BOOST")) {
        policy.introBoost = parseDouble(v, policy.introBoost);
    }

    return policy;
}

ConfigResolver::EmbeddingChunkingPolicy ConfigResolver::resolveEmbeddingChunkingPolicy() {
    EmbeddingChunkingPolicy policy{};

    // Embedding pipeline defaults differ from generic chunker defaults.
    policy.strategy = yams::vector::ChunkingStrategy::PARAGRAPH_BASED;
    policy.config.preserve_sentences = false;
    policy.config.use_token_count = false;
    policy.config.strategy = policy.strategy;

    auto applyFromKv = [&](const std::map<std::string, std::string>& kv) {
        if (auto it = kv.find("embeddings.chunking.strategy"); it != kv.end()) {
            if (auto s = parseChunkingStrategy(it->second); s) {
                policy.strategy = *s;
                policy.config.strategy = *s;
                policy.overridden = true;
            }
        }
        if (auto it = kv.find("embeddings.chunking.preserve_sentences"); it != kv.end()) {
            if (auto v = parseBool01(it->second); v) {
                policy.config.preserve_sentences = *v;
                policy.overridden = true;
            }
        }
        if (auto it = kv.find("embeddings.chunking.use_tokens"); it != kv.end()) {
            if (auto v = parseBool01(it->second); v) {
                policy.config.use_token_count = *v;
                policy.overridden = true;
            }
        }
        if (auto it = kv.find("embeddings.chunking.target"); it != kv.end()) {
            if (auto v = parseSize(it->second); v && *v > 0) {
                policy.config.target_chunk_size = *v;
                policy.overridden = true;
            }
        }
        if (auto it = kv.find("embeddings.chunking.max"); it != kv.end()) {
            if (auto v = parseSize(it->second); v && *v > 0) {
                policy.config.max_chunk_size = *v;
                policy.overridden = true;
            }
        }
        if (auto it = kv.find("embeddings.chunking.min"); it != kv.end()) {
            if (auto v = parseSize(it->second); v && *v > 0) {
                policy.config.min_chunk_size = *v;
                policy.overridden = true;
            }
        }
        if (auto it = kv.find("embeddings.chunking.overlap"); it != kv.end()) {
            if (auto v = parseSize(it->second); v) {
                policy.config.overlap_size = *v;
                if (*v == 0) {
                    policy.config.overlap_percentage = 0.0;
                }
                policy.overridden = true;
            }
        }
        if (auto it = kv.find("embeddings.chunking.overlap_pct"); it != kv.end()) {
            if (auto v = parseDouble(it->second); v) {
                double pct = *v;
                if (pct < 0.0) {
                    pct = 0.0;
                }
                if (pct > 1.0) {
                    pct = 1.0;
                }
                policy.config.overlap_percentage = pct;
                if (pct == 0.0) {
                    policy.config.overlap_size = 0;
                }
                policy.overridden = true;
            }
        }
    };

    // Config file (best-effort).
    try {
        auto cfgPath = resolveDefaultConfigPath();
        if (!cfgPath.empty()) {
            applyFromKv(parseSimpleTomlFlat(cfgPath));
        }
    } catch (...) {
    }

    // Env overrides (backwards compatible with existing embedding pipeline vars).
    if (const char* v = std::getenv("YAMS_EMBED_CHUNK_STRATEGY")) {
        if (auto s = parseChunkingStrategy(v); s) {
            policy.strategy = *s;
            policy.config.strategy = *s;
            policy.overridden = true;
        }
    }
    if (const char* v = std::getenv("YAMS_EMBED_CHUNK_PRESERVE_SENTENCES")) {
        if (auto b = parseBool01(v); b) {
            policy.config.preserve_sentences = *b;
            policy.overridden = true;
        }
    }
    if (const char* v = std::getenv("YAMS_EMBED_CHUNK_USE_TOKENS")) {
        if (auto b = parseBool01(v); b) {
            policy.config.use_token_count = *b;
            policy.overridden = true;
        }
    }
    if (const char* v = std::getenv("YAMS_EMBED_CHUNK_TARGET")) {
        if (auto s = parseSize(v); s && *s > 0) {
            policy.config.target_chunk_size = *s;
            policy.overridden = true;
        }
    }
    if (const char* v = std::getenv("YAMS_EMBED_CHUNK_MAX")) {
        if (auto s = parseSize(v); s && *s > 0) {
            policy.config.max_chunk_size = *s;
            policy.overridden = true;
        }
    }
    if (const char* v = std::getenv("YAMS_EMBED_CHUNK_MIN")) {
        if (auto s = parseSize(v); s && *s > 0) {
            policy.config.min_chunk_size = *s;
            policy.overridden = true;
        }
    }
    if (const char* v = std::getenv("YAMS_EMBED_CHUNK_OVERLAP")) {
        if (auto s = parseSize(v); s) {
            policy.config.overlap_size = *s;
            if (*s == 0) {
                policy.config.overlap_percentage = 0.0;
            }
            policy.overridden = true;
        }
    }
    if (const char* v = std::getenv("YAMS_EMBED_CHUNK_OVERLAP_PCT")) {
        if (auto d = parseDouble(v); d) {
            double pct = *d;
            if (pct < 0.0) {
                pct = 0.0;
            }
            if (pct > 1.0) {
                pct = 1.0;
            }
            policy.config.overlap_percentage = pct;
            if (pct == 0.0) {
                policy.config.overlap_size = 0;
            }
            policy.overridden = true;
        }
    }

    // Sanity: ensure min <= target <= max.
    if (policy.config.min_chunk_size > policy.config.max_chunk_size) {
        policy.config.max_chunk_size = policy.config.min_chunk_size;
    }
    if (policy.config.target_chunk_size < policy.config.min_chunk_size) {
        policy.config.target_chunk_size = policy.config.min_chunk_size;
    }
    if (policy.config.target_chunk_size > policy.config.max_chunk_size) {
        policy.config.target_chunk_size = policy.config.max_chunk_size;
    }

    return policy;
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
                if (v.find("simeon-default") != std::string::npos) {
                    preferred = "simeon-default";
                } else if (v.find("embeddinggemma-300m") != std::string::npos) {
                    preferred = "embeddinggemma-300m";
                } else if (v.find("all-MiniLM-L6-v2") != std::string::npos) {
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
                        if (v.find("simeon-default") != std::string::npos) {
                            preferred = "simeon-default";
                        } else if (v.find("embeddinggemma-300m") != std::string::npos) {
                            preferred = "embeddinggemma-300m";
                        } else if (v.find("all-MiniLM-L6-v2") != std::string::npos) {
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

    // If simeon is the selected backend and nothing else named a preferred
    // model above, return the simeon-default sentinel instead of scanning
    // the ONNX models directory (which would pick an incompatible model).
    if (resolveEmbeddingBackend("auto") == "simeon") {
        spdlog::debug("Preferred model defaulted to simeon-default (backend=simeon)");
        return "simeon-default";
    }

    try {
        if (!resolvedDataDir.empty()) {
            namespace fs = std::filesystem;
            fs::path models = resolvedDataDir / "models";
            std::error_code ec;
            if (fs::exists(models, ec) && fs::is_directory(models, ec)) {
                // Use the first available model with model.onnx
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

std::string ConfigResolver::resolveEmbeddingBackend(const std::string& defaultValue) {
    auto normalize = [](std::string s) {
        for (auto& c : s)
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (s == "hybrid" || s == "local" || s == "local_onnx" || s == "onnx") {
            return std::string("daemon");
        }
        return s;
    };

    if (const char* envp = std::getenv("YAMS_EMBED_BACKEND")) {
        std::string v(envp);
        if (!v.empty()) {
            return normalize(std::move(v));
        }
    }

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath = resolveDefaultConfigPath();
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            auto kv = parseSimpleTomlFlat(cfgPath);
            auto it = kv.find("embeddings.backend");
            if (it != kv.end() && !it->second.empty()) {
                return normalize(it->second);
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for embedding backend: {}", e.what());
    }

    return normalize(defaultValue);
}

ConfigResolver::TopologyRoutingPolicy ConfigResolver::resolveTopologyRoutingPolicy() {
    TopologyRoutingPolicy policy;

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath = resolveDefaultConfigPath();
        if (cfgPath.empty() || !fs::exists(cfgPath)) {
            return policy;
        }

        auto kv = parseSimpleTomlFlat(cfgPath);

        auto parseBool = [](std::string v) -> std::optional<bool> {
            for (auto& c : v)
                c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            if (v == "true" || v == "1" || v == "yes" || v == "on")
                return true;
            if (v == "false" || v == "0" || v == "no" || v == "off")
                return false;
            return std::nullopt;
        };

        if (auto it = kv.find("search.topology.enable_weak_query_routing"); it != kv.end()) {
            policy.enableWeakQueryRouting = parseBool(it->second);
        }
        auto parseSize = [](const std::string& s) -> std::optional<std::size_t> {
            try {
                return static_cast<std::size_t>(std::stoul(s));
            } catch (const std::exception&) {
                return std::nullopt;
            }
        };
        auto parseFloat = [](const std::string& s) -> std::optional<float> {
            try {
                return std::stof(s);
            } catch (const std::exception&) {
                return std::nullopt;
            }
        };
        if (auto it = kv.find("search.topology.max_clusters"); it != kv.end()) {
            policy.maxClusters = parseSize(it->second);
        }
        if (auto it = kv.find("search.topology.max_docs"); it != kv.end()) {
            policy.maxDocs = parseSize(it->second);
        }
        if (auto it = kv.find("search.topology.medoid_boost"); it != kv.end()) {
            policy.medoidBoost = parseFloat(it->second);
        }
        if (auto it = kv.find("search.topology.bridge_boost"); it != kv.end()) {
            policy.bridgeBoost = parseFloat(it->second);
        }
        if (auto it = kv.find("search.topology.routed_base_multiplier"); it != kv.end()) {
            policy.routedBaseMultiplier = parseFloat(it->second);
        }
        if (auto it = kv.find("search.topology.routing_variant"); it != kv.end()) {
            if (!it->second.empty())
                policy.routingVariant = it->second;
        }
        if (auto it = kv.find("search.topology.integration"); it != kv.end()) {
            if (!it->second.empty())
                policy.integration = it->second;
        }
        if (auto it = kv.find("search.topology.recall_expand_per_cluster"); it != kv.end()) {
            policy.recallExpandPerCluster = parseSize(it->second);
        }
        if (auto it = kv.find("search.topology.rrf_k"); it != kv.end()) {
            policy.rrfK = parseFloat(it->second);
        }
        if (auto it = kv.find("search.topology.route_scoring"); it != kv.end()) {
            if (!it->second.empty())
                policy.routeScoring = it->second;
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for topology routing policy: {}", e.what());
    }

    return policy;
}

ConfigResolver::TopologyEnginePolicy ConfigResolver::resolveTopologyEnginePolicy() {
    TopologyEnginePolicy policy;

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath = resolveDefaultConfigPath();
        if (cfgPath.empty() || !fs::exists(cfgPath)) {
            return policy;
        }

        auto kv = parseSimpleTomlFlat(cfgPath);

        if (auto it = kv.find("topology.engine"); it != kv.end()) {
            if (!it->second.empty()) {
                policy.engine = it->second;
            }
        }
        if (auto it = kv.find("topology.hdbscan_min_points"); it != kv.end()) {
            try {
                policy.hdbscanMinPoints = static_cast<std::size_t>(std::stoul(it->second));
            } catch (const std::exception&) {
            }
        }
        if (auto it = kv.find("topology.hdbscan_min_cluster_size"); it != kv.end()) {
            try {
                policy.hdbscanMinClusterSize = static_cast<std::size_t>(std::stoul(it->second));
            } catch (const std::exception&) {
            }
        }
        if (auto it = kv.find("topology.feature_smoothing_hops"); it != kv.end()) {
            try {
                policy.featureSmoothingHops = static_cast<std::size_t>(std::stoul(it->second));
            } catch (const std::exception&) {
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for topology engine policy: {}", e.what());
    }

    return policy;
}

namespace {

std::optional<std::string> readEnvString(const char* name) {
    const char* raw = std::getenv(name);
    if (!raw || !*raw)
        return std::nullopt;
    return std::string(raw);
}

std::optional<std::uint32_t> readEnvU32(const char* name) {
    const char* raw = std::getenv(name);
    if (!raw || !*raw)
        return std::nullopt;
    try {
        return static_cast<std::uint32_t>(std::stoul(raw));
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

std::optional<float> readEnvFloat(const char* name) {
    const char* raw = std::getenv(name);
    if (!raw || !*raw)
        return std::nullopt;
    try {
        return std::stof(raw);
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

std::optional<std::uint32_t> parseTomlU32(const std::string& s) {
    try {
        return static_cast<std::uint32_t>(std::stoul(s));
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

std::optional<std::size_t> parseTomlSize(const std::string& s) {
    try {
        return static_cast<std::size_t>(std::stoul(s));
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

std::optional<float> parseTomlFloat(const std::string& s) {
    try {
        return std::stof(s);
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

std::optional<bool> parseTomlBool(const std::string& s) {
    std::string v;
    v.reserve(s.size());
    for (char c : s)
        v.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    if (v == "true" || v == "1" || v == "yes" || v == "on")
        return true;
    if (v == "false" || v == "0" || v == "no" || v == "off")
        return false;
    return std::nullopt;
}

} // namespace

ConfigResolver::SimeonEncoderPolicy ConfigResolver::resolveSimeonEncoderPolicy() {
    SimeonEncoderPolicy policy;

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath = resolveDefaultConfigPath();
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            auto kv = parseSimpleTomlFlat(cfgPath);
            if (auto it = kv.find("embeddings.simeon.ngram_mode");
                it != kv.end() && !it->second.empty())
                policy.ngramMode = it->second;
            if (auto it = kv.find("embeddings.simeon.ngram_min"); it != kv.end())
                policy.ngramMin = parseTomlU32(it->second);
            if (auto it = kv.find("embeddings.simeon.ngram_max"); it != kv.end())
                policy.ngramMax = parseTomlU32(it->second);
            if (auto it = kv.find("embeddings.simeon.sketch_dim"); it != kv.end())
                policy.sketchDim = parseTomlU32(it->second);
            if (auto it = kv.find("embeddings.simeon.output_dim"); it != kv.end())
                policy.outputDim = parseTomlU32(it->second);
            if (auto it = kv.find("embeddings.simeon.projection");
                it != kv.end() && !it->second.empty())
                policy.projection = it->second;
            if (auto it = kv.find("embeddings.simeon.l2_normalize"); it != kv.end())
                policy.l2Normalize = parseTomlBool(it->second);
            if (auto it = kv.find("embeddings.simeon.pq_bytes"); it != kv.end())
                policy.pqBytes = parseTomlU32(it->second);
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for simeon encoder policy: {}", e.what());
    }

    if (auto v = readEnvString("YAMS_SIMEON_NGRAM_MODE"))
        policy.ngramMode = std::move(v);
    if (auto v = readEnvU32("YAMS_SIMEON_NGRAM_MIN"))
        policy.ngramMin = v;
    if (auto v = readEnvU32("YAMS_SIMEON_NGRAM_MAX"))
        policy.ngramMax = v;
    if (auto v = readEnvU32("YAMS_SIMEON_SKETCH_DIM"))
        policy.sketchDim = v;
    if (auto v = readEnvU32("YAMS_SIMEON_OUTPUT_DIM"))
        policy.outputDim = v;
    if (auto v = readEnvString("YAMS_SIMEON_PROJECTION"))
        policy.projection = std::move(v);
    if (auto v = readEnvU32("YAMS_SIMEON_PQ_BYTES"))
        policy.pqBytes = v;

    return policy;
}

ConfigResolver::SimeonBm25Policy ConfigResolver::resolveSimeonBm25Policy() {
    SimeonBm25Policy policy;

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath = resolveDefaultConfigPath();
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            auto kv = parseSimpleTomlFlat(cfgPath);
            if (auto it = kv.find("embeddings.simeon.bm25.enabled"); it != kv.end())
                policy.enabled = parseTomlBool(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.variant");
                it != kv.end() && !it->second.empty())
                policy.variant = it->second;
            if (auto it = kv.find("embeddings.simeon.bm25.subword_gamma"); it != kv.end())
                policy.subwordGamma = parseTomlFloat(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.max_corpus_docs"); it != kv.end())
                policy.maxCorpusDocs = parseTomlSize(it->second);
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for simeon bm25 policy: {}", e.what());
    }

    if (const char* raw = std::getenv("YAMS_SIMEON_BM25_ENABLED")) {
        if (auto b = parseTomlBool(std::string(raw)))
            policy.enabled = b;
    }
    if (auto v = readEnvString("YAMS_SIMEON_BM25_VARIANT"))
        policy.variant = std::move(v);
    if (auto v = readEnvFloat("YAMS_SIMEON_BM25_SUBWORD_GAMMA"))
        policy.subwordGamma = v;
    if (const char* raw = std::getenv("YAMS_SIMEON_BM25_MAX_CORPUS_DOCS")) {
        try {
            policy.maxCorpusDocs = static_cast<std::size_t>(std::stoul(raw));
        } catch (const std::exception&) {
        }
    }

    return policy;
}

ConfigResolver::RerankerBackendPolicy ConfigResolver::resolveRerankerBackendPolicy() {
    return resolveRerankerBackendPolicy(DaemonConfig{});
}

ConfigResolver::RerankerBackendPolicy
ConfigResolver::resolveRerankerBackendPolicy(const DaemonConfig& config) {
    RerankerBackendPolicy policy;

    auto normalize = [](std::string value) {
        std::transform(value.begin(), value.end(), value.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return value;
    };

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath =
            !config.configFilePath.empty() ? config.configFilePath : resolveDefaultConfigPath();
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            auto kv = parseSimpleTomlFlat(cfgPath);
            if (auto it = kv.find("search.reranker_backend");
                it != kv.end() && !it->second.empty()) {
                policy.backend = normalize(it->second);
                return policy;
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for reranker backend: {}", e.what());
    }

    return policy;
}

ConfigResolver::PostIngestCaps ConfigResolver::resolvePostIngestCaps() {
    PostIngestCaps caps;

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath = resolveDefaultConfigPath();
        if (cfgPath.empty() || !fs::exists(cfgPath)) {
            return caps;
        }

        auto kv = parseSimpleTomlFlat(cfgPath);

        auto parseBounded = [](const std::string& s, std::uint32_t lo,
                               std::uint32_t hi) -> std::optional<std::uint32_t> {
            try {
                auto raw = static_cast<std::uint32_t>(std::stoul(s));
                if (raw < lo || raw > hi)
                    return std::nullopt;
                return raw;
            } catch (const std::exception&) {
                return std::nullopt;
            }
        };

        if (auto it = kv.find("tuning.post_ingest.total_concurrent"); it != kv.end()) {
            caps.totalConcurrent = parseBounded(it->second, 1, 256);
        }
        if (auto it = kv.find("tuning.post_ingest.embed_concurrent"); it != kv.end()) {
            caps.embedConcurrent = parseBounded(it->second, 1, 32);
        }
        if (auto it = kv.find("tuning.post_ingest.extraction_concurrent"); it != kv.end()) {
            caps.extractionConcurrent = parseBounded(it->second, 1, 64);
        }
        if (auto it = kv.find("tuning.post_ingest.kg_concurrent"); it != kv.end()) {
            caps.kgConcurrent = parseBounded(it->second, 1, 64);
        }
        if (auto it = kv.find("tuning.post_ingest.symbol_concurrent"); it != kv.end()) {
            caps.symbolConcurrent = parseBounded(it->second, 1, 32);
        }
        if (auto it = kv.find("tuning.post_ingest.entity_concurrent"); it != kv.end()) {
            caps.entityConcurrent = parseBounded(it->second, 1, 16);
        }
        if (auto it = kv.find("tuning.post_ingest.title_concurrent"); it != kv.end()) {
            caps.titleConcurrent = parseBounded(it->second, 1, 16);
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for post-ingest caps: {}", e.what());
    }

    return caps;
}

std::string ConfigResolver::resolveRerankerModel(const DaemonConfig& config) {
    std::string preferred;

    if (const char* envp = std::getenv("YAMS_RERANKER_MODEL")) {
        preferred = envp;
        if (!preferred.empty()) {
            spdlog::debug("Reranker model from environment: {}", preferred);
            return preferred;
        }
    }

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath =
            !config.configFilePath.empty() ? config.configFilePath : resolveDefaultConfigPath();
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            auto kv = parseSimpleTomlFlat(cfgPath);
            auto it = kv.find("search.reranker_model");
            if (it != kv.end() && !it->second.empty()) {
                preferred = it->second;
                spdlog::debug("Reranker model from config: {}", preferred);
                return preferred;
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for reranker model: {}", e.what());
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
