// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/common/fs_utils.h>
#include <yams/config/config_helpers.h>
#include <yams/config/detail/config_parse_utils.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/daemon.h>
#include <yams/vector/sqlite_vec_backend.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <mutex>
#include <system_error>
#include <vector>

namespace yams::daemon {

namespace {

using yams::config::detail::parseDouble;
using yams::config::detail::parseTomlBool;
using yams::config::detail::parseUnsignedIntegral;
using yams::config::detail::trimView;

template <typename T> std::optional<T> parseSignedIntegral(std::string_view raw) {
    raw = trimView(raw);
    if (raw.empty()) {
        return std::nullopt;
    }
    T value{};
    const char* begin = raw.data();
    const char* end = begin + raw.size();
    auto [ptr, ec] = std::from_chars(begin, end, value);
    if (ec != std::errc{} || ptr != end) {
        return std::nullopt;
    }
    return value;
}

std::optional<std::size_t> parseSize(std::string_view raw) {
    return parseUnsignedIntegral<std::size_t>(raw);
}

std::optional<bool> parseBool01(std::string_view raw) {
    auto parsed = parseSignedIntegral<long long>(raw);
    if (!parsed) {
        return std::nullopt;
    }
    return *parsed != 0;
}

std::optional<bool> parseBoolValue(std::string raw) {
    raw.erase(std::remove_if(raw.begin(), raw.end(),
                             [](unsigned char c) { return std::isspace(c) != 0; }),
              raw.end());
    std::transform(raw.begin(), raw.end(), raw.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (raw == "true" || raw == "yes" || raw == "on" || raw == "1") {
        return true;
    }
    if (raw == "false" || raw == "no" || raw == "off" || raw == "0") {
        return false;
    }
    return std::nullopt;
}

std::string embeddingDispatchEnvSignature() {
    static constexpr const char* kEnvNames[] = {
        "YAMS_EMBED_SELECTION_STRATEGY",
        "YAMS_EMBED_SELECTION_MODE",
        "YAMS_EMBED_MAX_CHUNKS_PER_DOC",
        "YAMS_EMBED_MAX_CHARS_PER_DOC",
        "YAMS_EMBED_SELECTION_HEADING_BOOST",
        "YAMS_EMBED_SELECTION_INTRO_BOOST",
        "YAMS_EMBED_CHUNK_STRATEGY",
        "YAMS_EMBED_CHUNK_PRESERVE_SENTENCES",
        "YAMS_EMBED_CHUNK_USE_TOKENS",
        "YAMS_EMBED_CHUNK_TARGET",
        "YAMS_EMBED_CHUNK_MAX",
        "YAMS_EMBED_CHUNK_MIN",
        "YAMS_EMBED_CHUNK_OVERLAP",
        "YAMS_EMBED_CHUNK_OVERLAP_PCT",
    };

    std::string signature;
    for (const auto* name : kEnvNames) {
        signature += name;
        signature += '=';
        signature += yams::config::getenv_copy(name);
        signature += '\n';
    }
    return signature;
}

std::string embeddingDispatchConfigSignature() {
    const auto path = ConfigResolver::resolveDefaultConfigPath();
    if (path.empty()) {
        return {};
    }

    std::string signature = path.string();
    signature += '|';
    try {
        const auto mtime = std::filesystem::last_write_time(path);
        signature += std::to_string(static_cast<long long>(mtime.time_since_epoch().count()));
    } catch (...) {
        signature += "mtime-error";
    }
    return signature;
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
            auto kv = yams::config::parse_simple_toml(cfgPath);
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

    // Environment override wins only when it is a valid boolean.
    if (auto value = yams::config::read_env_bool("YAMS_EMBED_PRELOAD_ON_STARTUP").value) {
        flag = *value;
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
        return parseUnsignedIntegral<std::size_t>(raw).value_or(fallback);
    };

    auto parseDouble = [](const std::string& raw, double fallback) {
        return yams::config::detail::parseDouble(raw).value_or(fallback);
    };

    try {
        auto cfgPath = resolveDefaultConfigPath();
        if (!cfgPath.empty()) {
            auto kv = yams::config::parse_simple_toml(cfgPath);
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
            if (auto it = kv.find("embeddings.selection.update_semantic_graph_during_ingest");
                it != kv.end()) {
                if (auto v = parseBoolValue(it->second); v.has_value()) {
                    policy.updateSemanticGraphDuringIngest = *v;
                }
            }
        }
    } catch (const std::exception& error) {
        spdlog::debug("Error reading embedding selection policy: {}", error.what());
    } catch (...) {
        spdlog::debug("Error reading embedding selection policy: unknown error");
    }

    // Env overrides (config component owns this precedence).
    if (auto value = yams::config::getenv_nonempty("YAMS_EMBED_SELECTION_STRATEGY")) {
        policy.strategy = parseStrategy(*value);
    }
    if (auto value = yams::config::getenv_nonempty("YAMS_EMBED_SELECTION_MODE")) {
        policy.mode = parseMode(*value);
    }
    policy.maxChunksPerDoc = yams::config::read_env_size("YAMS_EMBED_MAX_CHUNKS_PER_DOC")
                                 .valueOr(policy.maxChunksPerDoc);
    policy.maxCharsPerDoc =
        yams::config::read_env_size("YAMS_EMBED_MAX_CHARS_PER_DOC").valueOr(policy.maxCharsPerDoc);
    policy.headingBoost = yams::config::read_env_double("YAMS_EMBED_SELECTION_HEADING_BOOST")
                              .valueOr(policy.headingBoost);
    policy.introBoost = yams::config::read_env_double("YAMS_EMBED_SELECTION_INTRO_BOOST")
                            .valueOr(policy.introBoost);

    return policy;
}

ConfigResolver::EmbeddingChunkingPolicy ConfigResolver::resolveEmbeddingChunkingPolicy() {
    EmbeddingChunkingPolicy policy{};

    // Embedding pipeline defaults differ from generic chunker defaults.
    policy.strategy = yams::vector::ChunkingStrategy::PARAGRAPH_BASED;
    policy.config.preserve_sentences = false;
    policy.config.use_token_count = false;
    policy.config.strategy = yams::vector::ChunkingStrategy::PARAGRAPH_BASED;

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
            applyFromKv(yams::config::parse_simple_toml(cfgPath));
        }
    } catch (const std::exception& error) {
        spdlog::debug("Error reading embedding chunking policy: {}", error.what());
    } catch (...) {
        spdlog::debug("Error reading embedding chunking policy: unknown error");
    }

    // Env overrides (backwards compatible with existing embedding pipeline vars).
    if (auto value = yams::config::getenv_nonempty("YAMS_EMBED_CHUNK_STRATEGY")) {
        if (auto strategy = parseChunkingStrategy(*value)) {
            policy.strategy = *strategy;
            policy.config.strategy = *strategy;
            policy.overridden = true;
        }
    }
    if (auto value = yams::config::read_env_bool("YAMS_EMBED_CHUNK_PRESERVE_SENTENCES").value) {
        policy.config.preserve_sentences = *value;
        policy.overridden = true;
    }
    if (auto value = yams::config::read_env_bool("YAMS_EMBED_CHUNK_USE_TOKENS").value) {
        policy.config.use_token_count = *value;
        policy.overridden = true;
    }
    if (auto value = yams::config::read_env_size("YAMS_EMBED_CHUNK_TARGET").value;
        value && *value > 0) {
        policy.config.target_chunk_size = *value;
        policy.overridden = true;
    }
    if (auto value = yams::config::read_env_size("YAMS_EMBED_CHUNK_MAX").value;
        value && *value > 0) {
        policy.config.max_chunk_size = *value;
        policy.overridden = true;
    }
    if (auto value = yams::config::read_env_size("YAMS_EMBED_CHUNK_MIN").value;
        value && *value > 0) {
        policy.config.min_chunk_size = *value;
        policy.overridden = true;
    }
    if (auto value = yams::config::read_env_size("YAMS_EMBED_CHUNK_OVERLAP").value) {
        policy.config.overlap_size = *value;
        if (*value == 0) {
            policy.config.overlap_percentage = 0.0;
        }
        policy.overridden = true;
    }
    if (auto value = yams::config::read_env_float("YAMS_EMBED_CHUNK_OVERLAP_PCT").value) {
        const double percentage = std::clamp(static_cast<double>(*value), 0.0, 1.0);
        policy.config.overlap_percentage = percentage;
        if (percentage == 0.0) {
            policy.config.overlap_size = 0;
        }
        policy.overridden = true;
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

ConfigResolver::EmbeddingDispatchPolicy ConfigResolver::resolveEmbeddingDispatchPolicy() {
    struct CacheState {
        std::mutex mutex;
        std::optional<EmbeddingDispatchPolicy> value;
        std::string envSignature;
        std::string configSignature;
        std::chrono::steady_clock::time_point nextCheck;
    };

    static CacheState cache;
    static constexpr auto kCacheTtl = std::chrono::seconds(1);

    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(cache.mutex);
    if (cache.value && now < cache.nextCheck) {
        return *cache.value;
    }

    const auto envSignature = embeddingDispatchEnvSignature();
    const auto configSignature = embeddingDispatchConfigSignature();
    if (cache.value && envSignature == cache.envSignature &&
        configSignature == cache.configSignature) {
        cache.nextCheck = now + kCacheTtl;
        return *cache.value;
    }

    cache.value = EmbeddingDispatchPolicy{resolveEmbeddingSelectionPolicy(),
                                          resolveEmbeddingChunkingPolicy()};
    cache.envSignature = envSignature;
    cache.configSignature = configSignature;
    cache.nextCheck = now + kCacheTtl;
    return *cache.value;
}

std::string ConfigResolver::resolvePreferredModel(const DaemonConfig& config,
                                                  const std::filesystem::path& resolvedDataDir) {
    std::string preferred;

    // Returns true if the model name is acceptable as the preferred model for
    // the currently-resolved embedding backend.  simeon sentinels are only
    // valid when the backend is actually simeon (in-process, model-free).
    auto acceptAsPreferred = [&](const std::string& model) -> bool {
        if (model != "simeon-default" && model != "simeon")
            return true;
        if (resolveEmbeddingBackend("auto") == "simeon")
            return true;
        spdlog::debug("Model '{}' is a simeon sentinel but backend is not simeon; "
                      "skipping",
                      model);
        return false;
    };

    if (auto configured = yams::config::getenv_nonempty("YAMS_PREFERRED_MODEL")) {
        preferred = *configured;
        spdlog::debug("Preferred model from environment: {}", preferred);
        if (acceptAsPreferred(preferred))
            return preferred;
        preferred.clear();
    }

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath =
            !config.configFilePath.empty() ? config.configFilePath : resolveDefaultConfigPath();
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            // Flat parse for explicit keys
            auto kv = yams::config::parse_simple_toml(cfgPath);
            auto it = kv.find("embeddings.preferred_model");
            if (it != kv.end() && !it->second.empty()) {
                preferred = it->second;
                spdlog::debug("Preferred model from config: {}", preferred);
                if (acceptAsPreferred(preferred))
                    return preferred;
                preferred.clear();
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
                    if (acceptAsPreferred(preferred))
                        return preferred;
                    preferred.clear();
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
                        if (acceptAsPreferred(preferred))
                            return preferred;
                        preferred.clear();
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
                        if (acceptAsPreferred(preferred))
                            return preferred;
                        preferred.clear();
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

ResolvedEmbeddingConfig
ConfigResolver::resolveEmbeddingConfig(const DaemonConfig& config,
                                       const std::filesystem::path& resolvedDataDir) {
    ResolvedEmbeddingConfig result;
    result.backend = resolveEmbeddingBackend("auto");
    result.isTrainingFree = (result.backend == "simeon");
    result.preferredModel = resolvePreferredModel(config, resolvedDataDir);

    auto isSimeonSentinel = [](const std::string& s) {
        return s == "simeon-default" || s == "simeon";
    };

    if (result.isTrainingFree && !result.preferredModel.empty() &&
        !isSimeonSentinel(result.preferredModel)) {
        spdlog::warn("[ConfigResolver] backend '{}' is model-free but preferred_model '{}' "
                     "is an ONNX model name; using 'simeon-default' instead",
                     result.backend, result.preferredModel);
        result.warnings.push_back(fmt::format(
            "backend '{}' is model-free but preferred_model '{}' is an ONNX model name; "
            "using 'simeon-default' instead",
            result.backend, result.preferredModel));
        result.preferredModel = "simeon-default";
    }

    if (!result.isTrainingFree && isSimeonSentinel(result.preferredModel) &&
        result.backend != "simeon") {
        spdlog::warn("[ConfigResolver] preferred_model '{}' is a simeon sentinel but "
                     "backend is '{}'; model may not be loadable",
                     result.preferredModel, result.backend);
        result.warnings.push_back(
            fmt::format("preferred_model '{}' is a simeon sentinel but backend is '{}'; "
                        "model may not be loadable",
                        result.preferredModel, result.backend));
    }

    if (result.backend == "onnxruntime" && !result.preferredModel.empty() &&
        !isSimeonSentinel(result.preferredModel) && !resolvedDataDir.empty()) {
        namespace fs = std::filesystem;
        fs::path modelPath = resolvedDataDir / "models" / result.preferredModel / "model.onnx";
        std::error_code ec;
        if (!fs::exists(modelPath, ec)) {
            spdlog::warn("[ConfigResolver] backend 'onnxruntime' selected but model file "
                         "not found: {} (download with: yams model --download {})",
                         modelPath.string(), result.preferredModel);
            result.warnings.push_back(
                fmt::format("backend 'onnxruntime' selected but model file not found: {} "
                            "(download with: yams model --download {})",
                            modelPath.string(), result.preferredModel));
        }
    }

    return result;
}

ConfigResolver::TopologyRoutingPolicy ConfigResolver::resolveTopologyRoutingPolicy() {
    TopologyRoutingPolicy policy;

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath = resolveDefaultConfigPath();
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            auto kv = yams::config::parse_simple_toml(cfgPath);
            auto parseFloat = [](const std::string& s) -> std::optional<float> {
                try {
                    return std::stof(s);
                } catch (const std::exception&) {
                    return std::nullopt;
                }
            };
            if (auto it = kv.find("search.topology.mode"); it != kv.end()) {
                policy.mode = std::string(trimView(it->second));
            }
            if (auto it = kv.find("search.topology.vector_policy"); it != kv.end()) {
                policy.vectorPolicy = std::string(trimView(it->second));
            }
            if (auto it = kv.find("search.topology.enable_weak_query_routing"); it != kv.end()) {
                policy.enableWeakQueryRouting = parseBoolValue(it->second);
            }
            if (auto it = kv.find("search.topology.min_clusters"); it != kv.end()) {
                policy.minClusters = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.max_clusters"); it != kv.end()) {
                policy.maxClusters = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.max_seed_documents"); it != kv.end()) {
                policy.maxSeedDocuments = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.representative_limit"); it != kv.end()) {
                policy.representativeLimit = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.ann_candidate_limit"); it != kv.end()) {
                policy.annCandidateLimit = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.adaptive_probe_score_gap"); it != kv.end()) {
                policy.adaptiveProbeScoreGap = parseFloat(it->second);
            }
            if (auto it = kv.find("search.topology.narrow_min_boundary_margin"); it != kv.end()) {
                policy.narrowMinBoundaryMargin = parseFloat(it->second);
            }
            if (auto it = kv.find("search.topology.max_docs"); it != kv.end()) {
                policy.maxDocs = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.expansion_output_limit"); it != kv.end()) {
                policy.expansionOutputLimit = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.medoid_boost"); it != kv.end()) {
                policy.medoidBoost = parseFloat(it->second);
            }
            if (auto it = kv.find("search.topology.evidence_weight"); it != kv.end()) {
                policy.evidenceWeight = parseFloat(it->second);
            }
            if (auto it = kv.find("search.topology.route_scoring"); it != kv.end()) {
                policy.routeScoring = std::string(trimView(it->second));
            }
            if (auto it = kv.find("search.topology.sparse_dense_alpha"); it != kv.end()) {
                policy.sparseDenseAlpha = parseFloat(it->second);
            }
            if (auto it = kv.find("search.topology.min_route_score"); it != kv.end()) {
                policy.minRouteScore = parseFloat(it->second);
            }
            if (auto it = kv.find("search.topology.route_calibration_fingerprint");
                it != kv.end()) {
                policy.routeCalibrationFingerprint = std::string(trimView(it->second));
            }
            if (auto it = kv.find("search.topology.route_calibration_queries"); it != kv.end()) {
                policy.routeCalibrationQueries = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.route_calibration_protected_candidates");
                it != kv.end()) {
                policy.routeCalibrationProtectedCandidates = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.route_calibration_missed_protected_candidates");
                it != kv.end()) {
                policy.routeCalibrationMissedProtectedCandidates = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.route_min_calibration_queries");
                it != kv.end()) {
                policy.routeMinCalibrationQueries = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.route_max_misses_per_thousand");
                it != kv.end()) {
                policy.routeMaxMissesPerThousand = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.route_calibration_min_boundary_margin");
                it != kv.end()) {
                policy.routeCalibrationMinBoundaryMargin = parseFloat(it->second);
            }
            if (auto it = kv.find("search.topology.route_calibration_min_seed_hits");
                it != kv.end()) {
                policy.routeCalibrationMinSeedHits = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.route_work_max_rows_visited"); it != kv.end()) {
                policy.routeWorkMaxRowsVisited = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.route_work_max_exact_distance_evaluations");
                it != kv.end()) {
                policy.routeWorkMaxExactDistanceEvaluations = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.route_work_max_ann_candidates");
                it != kv.end()) {
                policy.routeWorkMaxAnnCandidates = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.expansion_source"); it != kv.end()) {
                policy.expansionSource = std::string(trimView(it->second));
            }
            if (auto it = kv.find("search.topology.graph_neighbor_min_score"); it != kv.end()) {
                policy.graphNeighborMinScore = parseFloat(it->second);
            }
            if (auto it = kv.find("search.topology.graph_neighbor_reciprocal_only");
                it != kv.end()) {
                policy.graphNeighborReciprocalOnly = parseBoolValue(it->second);
            }
            if (auto it = kv.find("search.topology.graph_weighted_seed_ranking"); it != kv.end()) {
                policy.graphWeightedSeedRanking = parseBoolValue(it->second);
            }
            if (auto it = kv.find("search.topology.graph_vector_seed_probe"); it != kv.end()) {
                policy.graphVectorSeedProbe = parseSize(it->second);
            }
            if (auto it = kv.find("search.topology.rescue_selector"); it != kv.end()) {
                policy.rescueSelector = std::string(trimView(it->second));
            }
            if (auto it = kv.find("search.topology.rrf_k"); it != kv.end()) {
                policy.rrfK = parseFloat(it->second);
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for topology routing policy: {}", e.what());
    }

    if (auto value =
            yams::config::read_env_bool("YAMS_SEARCH_ENABLE_TOPOLOGY_WEAK_ROUTING").value) {
        policy.enableWeakQueryRouting = value;
    }
    if (auto value = yams::config::read_env_size("YAMS_SEARCH_TOPOLOGY_MAX_CLUSTERS").value) {
        policy.maxClusters = value;
    }
    if (auto value = yams::config::read_env_size("YAMS_SEARCH_TOPOLOGY_MAX_DOCS").value) {
        policy.maxDocs = value;
    }
    if (auto value = yams::config::read_env_float("YAMS_SEARCH_TOPOLOGY_MEDOID_BOOST").value) {
        policy.medoidBoost = value;
    }
    if (auto value = yams::config::getenv_nonempty("YAMS_SEARCH_TOPOLOGY_ROUTE_SCORING")) {
        policy.routeScoring = std::string(trimView(*value));
    }
    if (auto value =
            yams::config::read_env_float("YAMS_SEARCH_TOPOLOGY_SPARSE_DENSE_ALPHA").value) {
        policy.sparseDenseAlpha = value;
    }
    if (auto value = yams::config::read_env_float("YAMS_SEARCH_TOPOLOGY_MIN_ROUTE_SCORE").value) {
        policy.minRouteScore = value;
    }
    if (auto value = yams::config::getenv_nonempty("YAMS_SEARCH_TOPOLOGY_EXPANSION_SOURCE")) {
        policy.expansionSource = std::string(trimView(*value));
    }
    if (auto value =
            yams::config::read_env_float("YAMS_SEARCH_TOPOLOGY_GRAPH_NEIGHBOR_MIN_SCORE").value) {
        policy.graphNeighborMinScore = value;
    }
    if (auto value =
            yams::config::read_env_bool("YAMS_SEARCH_TOPOLOGY_GRAPH_NEIGHBOR_RECIPROCAL_ONLY")
                .value) {
        policy.graphNeighborReciprocalOnly = value;
    }
    if (auto value =
            yams::config::read_env_size("YAMS_SEARCH_TOPOLOGY_GRAPH_VECTOR_SEED_PROBE").value) {
        policy.graphVectorSeedProbe = value;
    }

    return policy;
}

ConfigResolver::TopologyEnginePolicy ConfigResolver::resolveTopologyEnginePolicy() {
    TopologyEnginePolicy policy;

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath = resolveDefaultConfigPath();
        std::map<std::string, std::string> kv;
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            kv = yams::config::parse_simple_toml(cfgPath);
        }

        if (auto it = kv.find("topology.engine"); it != kv.end()) {
            if (!it->second.empty()) {
                policy.engine = it->second;
            }
        }
        if (auto it = kv.find("topology.routing_representatives"); it != kv.end()) {
            policy.routingRepresentativeCount = parseSize(it->second);
        }
        if (auto it = kv.find("topology.boundary_spill"); it != kv.end()) {
            policy.boundarySpillEnabled = parseBoolValue(it->second);
        }
        if (auto it = kv.find("topology.boundary_spill_limit"); it != kv.end()) {
            policy.boundarySpillLimit = parseSize(it->second);
        }
        if (auto it = kv.find("topology.boundary_spill_distance_ratio"); it != kv.end()) {
            policy.boundarySpillDistanceRatio = parseDouble(it->second);
        }
        if (auto it = kv.find("topology.boundary_spill_residual_penalty"); it != kv.end()) {
            policy.boundarySpillResidualPenalty = parseDouble(it->second);
        }
        if (auto it = kv.find("topology.features.entity_fusion"); it != kv.end()) {
            policy.featureEntityFusion = parseBoolValue(it->second);
        }
        if (auto it = kv.find("topology.features.entity_signature_k"); it != kv.end()) {
            policy.featureEntitySignatureK = parseSize(it->second);
        }
        if (auto it = kv.find("topology.features.entity_fusion_alpha"); it != kv.end()) {
            if (auto value = parseDouble(it->second); value.has_value()) {
                policy.featureEntityFusionAlpha = static_cast<float>(*value);
            }
        }
        if (auto it = kv.find("topology.features.entity_min_confidence"); it != kv.end()) {
            if (auto value = parseDouble(it->second); value.has_value()) {
                policy.featureEntityMinConfidence = static_cast<float>(*value);
            }
        }
        if (auto it = kv.find("topology.features.matryoshka_coarse_view"); it != kv.end()) {
            policy.featureMatryoshkaCoarseView = parseBoolValue(it->second);
        }
        if (auto it = kv.find("topology.features.matryoshka_target_dim"); it != kv.end()) {
            policy.featureMatryoshkaTargetDim = parseSize(it->second);
        }
        if (auto it = kv.find("topology.features.minhash_sketch"); it != kv.end()) {
            policy.featureMinHashSketch = parseBoolValue(it->second);
        }
        if (auto it = kv.find("topology.features.minhash_sketch_dim"); it != kv.end()) {
            policy.featureMinHashSketchDim = parseSize(it->second);
        }
        if (auto it = kv.find("topology.features.minhash_alpha"); it != kv.end()) {
            if (auto value = parseDouble(it->second); value.has_value()) {
                policy.featureMinHashAlpha = static_cast<float>(*value);
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for topology engine policy: {}", e.what());
    }

    const auto applyBoolEnv = [](const char* name, std::optional<bool>& target) {
        if (const auto value = parseBoolValue(yams::config::getenv_copy(name)); value.has_value()) {
            target = value;
        }
    };
    const auto applySizeEnv = [](const char* name, std::optional<std::size_t>& target) {
        if (const auto value = parseSize(yams::config::getenv_copy(name)); value.has_value()) {
            target = value;
        }
    };
    const auto applyFloatEnv = [](const char* name, std::optional<float>& target) {
        if (const auto value = parseDouble(yams::config::getenv_copy(name)); value.has_value()) {
            target = static_cast<float>(*value);
        }
    };
    applyBoolEnv("YAMS_TOPOLOGY_FEATURE_ENTITY_FUSION", policy.featureEntityFusion);
    applySizeEnv("YAMS_TOPOLOGY_FEATURE_ENTITY_K", policy.featureEntitySignatureK);
    applyFloatEnv("YAMS_TOPOLOGY_FEATURE_ENTITY_ALPHA", policy.featureEntityFusionAlpha);
    applyFloatEnv("YAMS_TOPOLOGY_FEATURE_ENTITY_MIN_CONF", policy.featureEntityMinConfidence);
    applyBoolEnv("YAMS_TOPOLOGY_FEATURE_MATRYOSHKA", policy.featureMatryoshkaCoarseView);
    applySizeEnv("YAMS_TOPOLOGY_FEATURE_MATRYOSHKA_DIM", policy.featureMatryoshkaTargetDim);
    applyBoolEnv("YAMS_TOPOLOGY_FEATURE_MINHASH", policy.featureMinHashSketch);
    applySizeEnv("YAMS_TOPOLOGY_FEATURE_MINHASH_DIM", policy.featureMinHashSketchDim);
    applyFloatEnv("YAMS_TOPOLOGY_FEATURE_MINHASH_ALPHA", policy.featureMinHashAlpha);

    return policy;
}

ConfigResolver::TopologyTunerPolicy ConfigResolver::resolveTopologyTunerPolicy() {
    TopologyTunerPolicy policy;

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath = resolveDefaultConfigPath();
        if (cfgPath.empty() || !fs::exists(cfgPath)) {
            return policy;
        }

        auto kv = yams::config::parse_simple_toml(cfgPath);

        if (auto it = kv.find("topology.tuner.enabled"); it != kv.end()) {
            policy.enabled = ConfigResolver::envTruthy(it->second.c_str());
        }
        if (auto it = kv.find("topology.tuner.cooldown_minutes"); it != kv.end())
            policy.cooldownMinutes = parseUnsignedIntegral<std::uint32_t>(it->second);
        if (auto it = kv.find("topology.tuner.doc_count_delta"); it != kv.end())
            policy.docCountDelta = parseSize(it->second);
        if (auto it = kv.find("topology.tuner.reward.alpha_singleton"); it != kv.end()) {
            policy.rewardAlphaSingleton = parseDouble(it->second);
        }
        if (auto it = kv.find("topology.tuner.reward.beta_giant_cluster"); it != kv.end()) {
            policy.rewardBetaGiantCluster = parseDouble(it->second);
        }
        if (auto it = kv.find("topology.tuner.reward.gamma_gini_deviation"); it != kv.end()) {
            policy.rewardGammaGiniDeviation = parseDouble(it->second);
        }
        if (auto it = kv.find("topology.tuner.reward.delta_intra_edge"); it != kv.end()) {
            policy.rewardDeltaIntraEdge = parseDouble(it->second);
        }
        if (auto it = kv.find("topology.tuner.reward_mode"); it != kv.end()) {
            if (!it->second.empty()) {
                policy.rewardMode = it->second;
            }
        }
        if (auto it = kv.find("topology.tuner.persistence_sample_size"); it != kv.end())
            policy.persistenceSampleSize = parseSize(it->second);
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for topology tuner policy: {}", e.what());
    }

    return policy;
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
            auto kv = yams::config::parse_simple_toml(cfgPath);
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

ConfigResolver::InstrumentationPolicy
ConfigResolver::resolveInstrumentationPolicy(const DaemonConfig& config) {
    InstrumentationPolicy policy;

    auto normalize = [](std::string value) {
        std::transform(value.begin(), value.end(), value.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return value;
    };
    auto readBool = [](const std::map<std::string, std::string>& kv,
                       const char* key) -> std::optional<bool> {
        auto it = kv.find(key);
        if (it == kv.end()) {
            return std::nullopt;
        }
        return parseTomlBool(it->second);
    };
    auto readU64 = [](const std::map<std::string, std::string>& kv,
                      const char* key) -> std::optional<std::uint64_t> {
        auto it = kv.find(key);
        if (it == kv.end()) {
            return std::nullopt;
        }
        try {
            return static_cast<std::uint64_t>(std::stoull(it->second));
        } catch (const std::exception&) {
            return std::nullopt;
        }
    };

    std::optional<bool> suppressAutoRepair;
    std::optional<bool> suppressSimeonLexicalBuild;
    std::optional<bool> suppressVectorIndexBuild;

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath =
            !config.configFilePath.empty() ? config.configFilePath : resolveDefaultConfigPath();
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            auto kv = yams::config::parse_simple_toml(cfgPath);
            if (auto it = kv.find("daemon.instrumentation.profile");
                it != kv.end() && !it->second.empty()) {
                policy.profile = normalize(it->second);
            }
            suppressAutoRepair = readBool(kv, "daemon.instrumentation.suppress_auto_repair");
            suppressSimeonLexicalBuild =
                readBool(kv, "daemon.instrumentation.suppress_simeon_lexical_build");
            suppressVectorIndexBuild =
                readBool(kv, "daemon.instrumentation.suppress_vector_index_build");
            if (auto warnMb = readU64(kv, "daemon.instrumentation.msl_stack_log_warn_mb")) {
                policy.mslStackLogWarnBytes = *warnMb * 1024ULL * 1024ULL;
            }
            if (auto warnBytes = readU64(kv, "daemon.instrumentation.msl_stack_log_warn_bytes")) {
                policy.mslStackLogWarnBytes = *warnBytes;
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for instrumentation policy: {}", e.what());
    }

    const bool mslActive =
        yams::config::read_env_bool("MallocStackLogging").valueOr(false) ||
        yams::config::read_env_bool("MallocStackLoggingNoCompact").valueOr(false);
    if (policy.profile.empty()) {
        policy.profile = "auto";
    }
    policy.profile = normalize(policy.profile);
    if (policy.profile == "msl") {
        policy.profile = "memory";
    }

    policy.memoryProfileActive = policy.profile == "memory" ||
                                 policy.profile == "memory_instrumentation" ||
                                 (policy.profile == "auto" && mslActive);

    const bool defaultSuppress = policy.memoryProfileActive;
    policy.suppressAutoRepair = suppressAutoRepair.value_or(defaultSuppress);
    policy.suppressSimeonLexicalBuild = suppressSimeonLexicalBuild.value_or(defaultSuppress);
    policy.suppressVectorIndexBuild = suppressVectorIndexBuild.value_or(defaultSuppress);

    return policy;
}

TuningConfig ConfigResolver::applyRuntimeTuning(const ConfigSections& sections,
                                                TuningConfig tuningConfig) {
    const auto findSection = [&](std::string_view name) -> const ConfigSection* {
        auto it = sections.find(std::string(name));
        return it == sections.end() ? nullptr : &it->second;
    };
    const auto findValue = [](const ConfigSection* section,
                              std::string_view key) -> const std::string* {
        if (!section) {
            return nullptr;
        }
        auto it = section->find(std::string(key));
        return it == section->end() ? nullptr : &it->second;
    };
    const auto parseUnsigned = [&](const ConfigSection* section, std::string_view sectionName,
                                   std::string_view key) -> std::optional<std::uint64_t> {
        const auto* raw = findValue(section, key);
        if (!raw) {
            return std::nullopt;
        }
        auto value = parseUnsignedIntegral<std::uint64_t>(*raw);
        if (!value) {
            spdlog::warn("Config: failed to parse {}.{} as unsigned integer", sectionName, key);
        }
        return value;
    };
    const auto parseUint32 = [&](const ConfigSection* section, std::string_view sectionName,
                                 std::string_view key) -> std::optional<std::uint32_t> {
        const auto* raw = findValue(section, key);
        if (!raw) {
            return std::nullopt;
        }
        auto value = parseUnsignedIntegral<std::uint32_t>(*raw);
        if (!value) {
            spdlog::warn("Config: failed to parse {}.{} as uint32", sectionName, key);
        }
        return value;
    };
    const auto parseSize = [&](const ConfigSection* section, std::string_view sectionName,
                               std::string_view key) -> std::optional<std::size_t> {
        const auto* raw = findValue(section, key);
        if (!raw) {
            return std::nullopt;
        }
        auto value = parseUnsignedIntegral<std::size_t>(*raw);
        if (!value) {
            spdlog::warn("Config: failed to parse {}.{} as size", sectionName, key);
        }
        return value;
    };
    const auto parseSigned = [&](const ConfigSection* section, std::string_view sectionName,
                                 std::string_view key) -> std::optional<int> {
        const auto* raw = findValue(section, key);
        if (!raw) {
            return std::nullopt;
        }
        auto value = parseSignedIntegral<int>(*raw);
        if (!value) {
            spdlog::warn("Config: failed to parse {}.{} as integer", sectionName, key);
        }
        return value;
    };
    const auto parseFloating = [&](const ConfigSection* section, std::string_view sectionName,
                                   std::string_view key) -> std::optional<double> {
        const auto* raw = findValue(section, key);
        if (!raw) {
            return std::nullopt;
        }
        auto value = parseDouble(*raw);
        if (!value) {
            spdlog::warn("Config: failed to parse {}.{} as floating-point", sectionName, key);
        }
        return value;
    };
    const auto parseBoolean = [&](const ConfigSection* section, std::string_view sectionName,
                                  std::string_view key) -> std::optional<bool> {
        const auto* raw = findValue(section, key);
        if (!raw) {
            return std::nullopt;
        }
        auto value = parseBoolValue(*raw);
        if (!value) {
            spdlog::warn("Config: failed to parse {}.{} as boolean", sectionName, key);
        }
        return value;
    };

    const auto* tuning = findSection("tuning");
    const auto applyUint32 = [&](std::string_view key, auto setter) {
        if (auto value = parseUint32(tuning, "tuning", key)) {
            setter(*value);
        }
    };
    applyUint32("backpressure_read_pause_ms", &TuneAdvisor::setBackpressureReadPauseMs);
    applyUint32("worker_poll_ms", &TuneAdvisor::setWorkerPollMs);
    if (auto value = parseFloating(tuning, "tuning", "idle_cpu_pct")) {
        TuneAdvisor::setIdleCpuThresholdPercent(*value);
    }
    if (auto value = parseUnsigned(tuning, "tuning", "idle_mux_low_bytes")) {
        TuneAdvisor::setIdleMuxLowBytes(*value);
    }
    applyUint32("idle_shrink_hold_ms", &TuneAdvisor::setIdleShrinkHoldMs);
    applyUint32("pool_cooldown_ms", &TuneAdvisor::setPoolCooldownMs);
    if (auto value = parseSigned(tuning, "tuning", "pool_scale_step")) {
        TuneAdvisor::setPoolScaleStep(*value);
    }
    applyUint32("pool_ipc_min", &TuneAdvisor::setPoolMinSizeIpc);
    applyUint32("pool_ipc_max", &TuneAdvisor::setPoolMaxSizeIpc);
    applyUint32("pool_io_min", &TuneAdvisor::setPoolMinSizeIpcIo);
    applyUint32("pool_io_max", &TuneAdvisor::setPoolMaxSizeIpcIo);
    applyUint32("io_conn_per_thread", &TuneAdvisor::setIoConnPerThread);
    applyUint32("post_ingest_threads", &TuneAdvisor::setPostIngestThreads);
    applyUint32("post_ingest_queue_max", &TuneAdvisor::setPostIngestQueueMax);
    applyUint32("list_inflight_limit", &TuneAdvisor::setListInflightLimit);
    applyUint32("list_admission_wait_ms", &TuneAdvisor::setListAdmissionWaitMs);
    applyUint32("grep_inflight_limit", &TuneAdvisor::setGrepInflightLimit);
    applyUint32("grep_admission_wait_ms", &TuneAdvisor::setGrepAdmissionWaitMs);

    if (auto value = parseBoolean(tuning, "tuning", "use_internal_bus_for_repair")) {
        TuneAdvisor::setUseInternalBusForRepair(*value);
    }
    if (auto value = parseBoolean(tuning, "tuning", "use_internal_bus_for_post_ingest")) {
        TuneAdvisor::setUseInternalBusForPostIngest(*value);
    }
    if (const auto* rawProfile = findValue(tuning, "profile")) {
        std::string profile = *rawProfile;
        std::transform(profile.begin(), profile.end(), profile.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (profile == "efficient" || profile == "conservative") {
            TuneAdvisor::setTuningProfile(TuneAdvisor::Profile::Efficient);
        } else if (profile == "aggressive") {
            TuneAdvisor::setTuningProfile(TuneAdvisor::Profile::Aggressive);
        } else if (profile == "balanced") {
            TuneAdvisor::setTuningProfile(TuneAdvisor::Profile::Balanced);
        } else {
            spdlog::warn("Config: unknown tuning.profile '{}'", profile);
        }
    }

    const auto updateSize = [&](std::string_view key, std::size_t& field) {
        if (auto value = parseSize(tuning, "tuning", key)) {
            field = *value;
        }
    };
    const auto updateUint32 = [&](std::string_view key, std::uint32_t& field) {
        if (auto value = parseUint32(tuning, "tuning", key)) {
            field = *value;
        }
    };
    updateUint32("target_cpu_percent", tuningConfig.targetCpuPercent);
    updateSize("post_ingest_capacity", tuningConfig.postIngestCapacity);
    updateSize("post_ingest_threads_min", tuningConfig.postIngestThreadsMin);
    updateSize("post_ingest_threads_max", tuningConfig.postIngestThreadsMax);
    updateSize("admit_warn_threshold", tuningConfig.admitWarnThreshold);
    updateSize("admit_stop_threshold", tuningConfig.admitStopThreshold);
    updateUint32("control_interval_ms", tuningConfig.controlIntervalMs);
    updateUint32("hold_ms", tuningConfig.holdMs);

    const auto* ingest = findSection("tuning.ingest");
    if (auto value = parseUnsigned(ingest, "tuning.ingest", "store_batch_size")) {
        constexpr std::uint64_t kMaximumStoreBatchSize = 256;
        if (*value >= 1 && *value <= kMaximumStoreBatchSize) {
            tuningConfig.ingestStoreBatchSize = static_cast<std::uint32_t>(*value);
        } else {
            spdlog::warn("Config: tuning.ingest.store_batch_size outside range 1..{}",
                         kMaximumStoreBatchSize);
        }
    }

    const auto* postIngest = findSection("tuning.post_ingest");
    if (auto value = parseUnsigned(postIngest, "tuning.post_ingest", "coalesce_ms")) {
        constexpr std::uint64_t kMaxCoalesceMs = 20;
        if (*value <= kMaxCoalesceMs) {
            tuningConfig.postIngestCoalesceMs = static_cast<std::uint32_t>(*value);
        } else {
            spdlog::warn("Config: tuning.post_ingest.coalesce_ms outside range 0..{}",
                         kMaxCoalesceMs);
        }
    }
    const auto applyPostIngestCap = [&](std::string_view key, const char* envName,
                                        std::uint32_t minimum, std::uint32_t maximum, auto setter) {
        auto value = parseUnsigned(postIngest, "tuning.post_ingest", key);
        if (!value || yams::config::getenv_optional(envName).has_value()) {
            return;
        }
        if (*value < minimum || *value > maximum) {
            spdlog::warn("Config: tuning.post_ingest.{} outside range {}..{}", key, minimum,
                         maximum);
            return;
        }
        setter(static_cast<std::uint32_t>(*value));
    };
    applyPostIngestCap("total_concurrent", "YAMS_POST_INGEST_TOTAL_CONCURRENT", 1, 256,
                       &TuneAdvisor::setPostIngestTotalConcurrent);
    applyPostIngestCap("embed_concurrent", "YAMS_POST_EMBED_CONCURRENT", 1, 32,
                       &TuneAdvisor::setPostEmbedConcurrent);
    applyPostIngestCap("extraction_concurrent", "YAMS_POST_EXTRACTION_CONCURRENT", 1, 64,
                       &TuneAdvisor::setPostExtractionConcurrent);
    applyPostIngestCap("kg_concurrent", "YAMS_POST_KG_CONCURRENT", 1, 64,
                       &TuneAdvisor::setPostKgConcurrent);
    applyPostIngestCap("symbol_concurrent", "YAMS_POST_SYMBOL_CONCURRENT", 1, 32,
                       &TuneAdvisor::setPostSymbolConcurrent);
    applyPostIngestCap("entity_concurrent", "YAMS_POST_ENTITY_CONCURRENT", 1, 16,
                       &TuneAdvisor::setPostEntityConcurrent);
    applyPostIngestCap("title_concurrent", "YAMS_POST_TITLE_CONCURRENT", 1, 16,
                       &TuneAdvisor::setPostTitleConcurrent);
    applyPostIngestCap("batch_size", "YAMS_POST_INGEST_BATCH_SIZE", 1, 256,
                       &TuneAdvisor::setPostIngestBatchSize);

    const auto* gradient = findSection("gradient_limiter");
    if (auto value = parseBoolean(gradient, "gradient_limiter", "enable")) {
        TuneAdvisor::setEnableGradientLimiters(*value);
    }
    if (auto value = parseFloating(gradient, "gradient_limiter", "smoothing_alpha")) {
        TuneAdvisor::setGradientSmoothingAlpha(*value);
    }
    if (auto value = parseFloating(gradient, "gradient_limiter", "long_window_alpha")) {
        TuneAdvisor::setGradientLongAlpha(*value);
    }
    if (auto value = parseUint32(gradient, "gradient_limiter", "warmup_samples")) {
        TuneAdvisor::setGradientWarmupSamples(*value);
    }
    if (auto value = parseFloating(gradient, "gradient_limiter", "tolerance")) {
        TuneAdvisor::setGradientTolerance(*value);
    }
    if (auto value = parseFloating(gradient, "gradient_limiter", "initial_limit")) {
        TuneAdvisor::setGradientInitialLimit(*value);
    }
    if (auto value = parseFloating(gradient, "gradient_limiter", "min_limit")) {
        TuneAdvisor::setGradientMinLimit(*value);
    }
    if (auto value = parseFloating(gradient, "gradient_limiter", "max_limit")) {
        TuneAdvisor::setGradientMaxLimit(*value);
    }

    return tuningConfig;
}

ConfigResolver::PostIngestCaps ConfigResolver::resolvePostIngestCaps() {
    PostIngestCaps caps;

    try {
        const auto configPath = resolveDefaultConfigPath();
        if (configPath.empty()) {
            return caps;
        }
        const auto values = yams::config::parse_simple_toml(configPath);
        const auto parseBounded = [](std::string_view raw, std::uint32_t minimum,
                                     std::uint32_t maximum) -> std::optional<std::uint32_t> {
            const auto parsed = parseUnsignedIntegral<std::uint32_t>(raw);
            if (!parsed || *parsed < minimum || *parsed > maximum) {
                return std::nullopt;
            }
            return parsed;
        };

        if (const auto it = values.find("tuning.post_ingest.total_concurrent");
            it != values.end()) {
            caps.totalConcurrent = parseBounded(it->second, 1, 256);
        }
        if (const auto it = values.find("tuning.post_ingest.embed_concurrent");
            it != values.end()) {
            caps.embedConcurrent = parseBounded(it->second, 1, 32);
        }
        if (const auto it = values.find("tuning.post_ingest.extraction_concurrent");
            it != values.end()) {
            caps.extractionConcurrent = parseBounded(it->second, 1, 64);
        }
        if (const auto it = values.find("tuning.post_ingest.kg_concurrent"); it != values.end()) {
            caps.kgConcurrent = parseBounded(it->second, 1, 64);
        }
        if (const auto it = values.find("tuning.post_ingest.symbol_concurrent");
            it != values.end()) {
            caps.symbolConcurrent = parseBounded(it->second, 1, 32);
        }
        if (const auto it = values.find("tuning.post_ingest.entity_concurrent");
            it != values.end()) {
            caps.entityConcurrent = parseBounded(it->second, 1, 16);
        }
        if (const auto it = values.find("tuning.post_ingest.title_concurrent");
            it != values.end()) {
            caps.titleConcurrent = parseBounded(it->second, 1, 16);
        }
        if (const auto it = values.find("tuning.post_ingest.batch_size"); it != values.end()) {
            caps.batchSize = parseBounded(it->second, 1, 256);
        }
    } catch (const std::exception& error) {
        spdlog::debug("Error reading config for post-ingest caps: {}", error.what());
    }

    return caps;
}

ConfigResolver::WriteCoordinatorTuning ConfigResolver::resolveWriteCoordinatorTuning() {
    WriteCoordinatorTuning tuning;

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath = resolveDefaultConfigPath();
        if (cfgPath.empty() || !fs::exists(cfgPath)) {
            return tuning;
        }

        auto kv = yams::config::parse_simple_toml(cfgPath);

        if (auto it = kv.find("tuning.write_coordinator.kg_dedup_enabled"); it != kv.end()) {
            tuning.kgDedupEnabled = parseBoolValue(it->second);
        }
        if (auto it = kv.find("tuning.write_coordinator.kg_dedup_max_edges"); it != kv.end()) {
            try {
                auto raw = static_cast<std::uint32_t>(std::stoul(it->second));
                if (raw >= 1000 && raw <= 1000000) {
                    tuning.kgDedupMaxEdges = raw;
                }
            } catch (const std::exception& error) {
                spdlog::debug("Invalid tuning.write_coordinator.kg_dedup_max_edges: {}",
                              error.what());
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for write-coordinator tuning: {}", e.what());
    }

    return tuning;
}

std::string ConfigResolver::resolveRerankerModel(const DaemonConfig& config) {
    if (const auto configured = yams::config::getenv_nonempty("YAMS_RERANKER_MODEL")) {
        return *configured;
    }

    try {
        const auto configPath =
            !config.configFilePath.empty() ? config.configFilePath : resolveDefaultConfigPath();
        const auto values = yams::config::parse_simple_toml(configPath);
        if (const auto it = values.find("search.reranker_model");
            it != values.end() && !it->second.empty()) {
            return it->second;
        }
    } catch (const std::exception& error) {
        spdlog::debug("Error reading config for reranker model: {}", error.what());
    }
    return {};
}

bool ConfigResolver::isSymbolExtractionEnabled(const DaemonConfig& config) {
    try {
        const auto configPath =
            !config.configFilePath.empty() ? config.configFilePath : resolveDefaultConfigPath();
        const auto values = yams::config::parse_simple_toml(configPath);
        if (const auto it = values.find("plugins.symbol_extraction.enable"); it != values.end()) {
            return parseTomlBool(it->second).value_or(true);
        }
    } catch (const std::exception& error) {
        spdlog::debug("[ConfigResolver] Failed to read symbol extraction flag: {}", error.what());
    } catch (...) {
        spdlog::debug("[ConfigResolver] Failed to read symbol extraction flag: unknown error");
    }
    return true;
}

int ConfigResolver::readTimeoutMs(const char* envName, int defaultMs, int minMs) {
    if (auto value = yams::config::read_env_int(envName).value) {
        return std::max(minMs, *value);
    }
    return defaultMs;
}

size_t ConfigResolver::readVectorMaxElements() {
    constexpr size_t kDefaultMaxElements = 100000;
    constexpr size_t kMinMaxElements = 1000;
    constexpr size_t kMaxMaxElements = 10000000; // 10M reasonable upper bound

    // 1. Environment variable takes precedence
    if (auto value = yams::config::read_env_size("YAMS_VECTOR_MAX_ELEMENTS").value) {
        if (*value >= kMinMaxElements && *value <= kMaxMaxElements) {
            spdlog::info("[ConfigResolver] Using YAMS_VECTOR_MAX_ELEMENTS={}", *value);
            return *value;
        }
        spdlog::warn(
            "[ConfigResolver] YAMS_VECTOR_MAX_ELEMENTS={} out of range [{}, {}], using default",
            *value, kMinMaxElements, kMaxMaxElements);
    }

    // 2. Config file
    auto cfgPath = resolveDefaultConfigPath();
    if (!cfgPath.empty()) {
        try {
            auto kv = yams::config::parse_simple_toml(cfgPath);
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
        } catch (const std::exception& error) {
            spdlog::debug("Invalid vector_database.max_elements config: {}", error.what());
        } catch (...) {
            spdlog::debug("Invalid vector_database.max_elements config: unknown error");
        }
    }

    // 3. Default
    return kDefaultMaxElements;
}

} // namespace yams::daemon
