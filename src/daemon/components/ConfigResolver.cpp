// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/common/fs_utils.h>
#include <yams/config/config_helpers.h>
#include <yams/config/detail/config_parse_utils.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/daemon.h>
#include <yams/vector/dim_resolver.h>
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
    return resolveEmbeddingConfig(config, {}).preloadOnStartup;
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
    return resolveEmbeddingConfig(config, resolvedDataDir).preferredModel;
}

ResolvedEmbeddingConfig
ConfigResolver::resolveEmbeddingConfig(const DaemonConfig& config,
                                       const std::filesystem::path& resolvedDataDir) {
    namespace fs = std::filesystem;

    ResolvedEmbeddingConfig result;
    result.effectiveConfigPath =
        !config.configFilePath.empty() ? config.configFilePath : resolveDefaultConfigPath();

    std::map<std::string, std::string> values;
    if (!result.effectiveConfigPath.empty()) {
        try {
            values = yams::config::parse_simple_toml(result.effectiveConfigPath);
        } catch (const std::exception& error) {
            result.warnings.push_back(fmt::format("unable to read embedding config '{}': {}",
                                                  result.effectiveConfigPath.string(),
                                                  error.what()));
        }
    }

    const auto configured = [&](std::string_view key) -> std::optional<std::string> {
        const auto it = values.find(std::string(key));
        if (it == values.end() || it->second.empty()) {
            return std::nullopt;
        }
        return it->second;
    };
    const auto normalizeBackend = [](std::string backend) {
        std::transform(backend.begin(), backend.end(), backend.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (backend == "onnx" || backend == "onnxruntime" || backend == "onnx-runtime" ||
            backend == "ort" || backend == "local_onnx") {
            return std::string{"onnxruntime"};
        }
        if (backend == "hybrid" || backend == "local") {
            return std::string{"daemon"};
        }
        return backend;
    };
    const auto noteConflict = [&](std::string_view field, std::string_view authoritativeKey,
                                  std::string_view authoritativeValue,
                                  std::string_view compatibilityKey,
                                  std::string_view compatibilityValue) {
        if (authoritativeValue == compatibilityValue) {
            return;
        }
        result.warnings.push_back(fmt::format(
            "embedding {} conflict: {}='{}' overrides compatibility key {}='{}'", field,
            authoritativeKey, authoritativeValue, compatibilityKey, compatibilityValue));
    };

    const auto canonicalBackend = configured("embeddings.backend");
    const auto runtimeBackend = configured("embeddings.runtime.backend");
    if (canonicalBackend && runtimeBackend) {
        noteConflict("backend", "embeddings.backend", normalizeBackend(*canonicalBackend),
                     "embeddings.runtime.backend", normalizeBackend(*runtimeBackend));
    }
    if (canonicalBackend) {
        result.backend = normalizeBackend(*canonicalBackend);
        result.provenance["backend"] = "config:embeddings.backend";
    } else if (runtimeBackend) {
        result.backend = normalizeBackend(*runtimeBackend);
        result.provenance["backend"] = "config:embeddings.runtime.backend";
    } else {
        result.backend = "auto";
        result.provenance["backend"] = "default:auto";
    }
    if (auto environmentBackend = yams::config::getenv_nonempty("YAMS_EMBED_BACKEND")) {
        const auto normalized = normalizeBackend(*environmentBackend);
        if (normalized != result.backend) {
            noteConflict("backend", "YAMS_EMBED_BACKEND", normalized, result.provenance["backend"],
                         result.backend);
        }
        result.backend = normalized;
        result.provenance["backend"] = "environment:YAMS_EMBED_BACKEND";
    }
    result.isTrainingFree = result.backend == "simeon";

    const auto canonicalModel = configured("embeddings.preferred_model");
    const auto runtimeModel = configured("embeddings.runtime.preferred_model");
    if (canonicalModel && runtimeModel) {
        noteConflict("preferred model", "embeddings.preferred_model", *canonicalModel,
                     "embeddings.runtime.preferred_model", *runtimeModel);
    }
    if (canonicalModel) {
        result.preferredModel = *canonicalModel;
        result.provenance["preferred_model"] = "config:embeddings.preferred_model";
    } else if (runtimeModel) {
        result.preferredModel = *runtimeModel;
        result.provenance["preferred_model"] = "config:embeddings.runtime.preferred_model";
    } else if (const auto preloadModels = configured("daemon.models.preload_models")) {
        static constexpr std::string_view kKnownModels[] = {"simeon-default",
                                                            "embeddinggemma-300m",
                                                            "all-MiniLM-L6-v2",
                                                            "all-mpnet-base-v2",
                                                            "jina-embeddings-v2-small-en",
                                                            "nomic-embed-text-v1",
                                                            "bge-small-en-v1.5",
                                                            "bge-base-en-v1.5"};
        for (const auto model : kKnownModels) {
            if (preloadModels->find(model) != std::string::npos) {
                result.preferredModel = model;
                result.provenance["preferred_model"] = "config:daemon.models.preload_models";
                break;
            }
        }
    }
    if (auto environmentModel = yams::config::getenv_nonempty("YAMS_PREFERRED_MODEL")) {
        if (!result.preferredModel.empty() && *environmentModel != result.preferredModel) {
            noteConflict("preferred model", "YAMS_PREFERRED_MODEL", *environmentModel,
                         result.provenance["preferred_model"], result.preferredModel);
        }
        result.preferredModel = *environmentModel;
        result.provenance["preferred_model"] = "environment:YAMS_PREFERRED_MODEL";
    }

    if (result.preferredModel.empty() && result.isTrainingFree) {
        result.preferredModel = "simeon-default";
        result.provenance["preferred_model"] = "default:simeon-default";
    }
    const auto isSimeonSentinel = [](const std::string& model) {
        return model == "simeon-default" || model == "simeon";
    };
    if (result.isTrainingFree && !result.preferredModel.empty() &&
        !isSimeonSentinel(result.preferredModel)) {
        const auto configuredModel = result.preferredModel;
        const auto configuredProvenance = result.provenance["preferred_model"];
        const auto warning = fmt::format(
            "backend '{}' is model-free but preferred_model '{}' is an ONNX model name; "
            "using 'simeon-default' instead",
            result.backend, configuredModel);
        spdlog::warn("[ConfigResolver] {}", warning);
        result.warnings.push_back(warning);
        result.preferredModel = "simeon-default";
        result.provenance["preferred_model"] =
            "normalization:simeon-backend(" + configuredProvenance + ")";
    } else if (!result.isTrainingFree && isSimeonSentinel(result.preferredModel)) {
        const auto rejectedModel = result.preferredModel;
        const auto warning = fmt::format(
            "preferred_model '{}' is a simeon sentinel but backend is '{}'; searching installed "
            "models instead",
            rejectedModel, result.backend);
        spdlog::warn("[ConfigResolver] {}", warning);
        result.warnings.push_back(warning);
        result.preferredModel.clear();
        result.provenance["preferred_model"] =
            "rejected:simeon-sentinel(" + result.provenance["preferred_model"] + ")";
    }

    if (result.preferredModel.empty() && !resolvedDataDir.empty()) {
        std::vector<std::string> availableModels;
        std::error_code error;
        const auto modelsDir = resolvedDataDir / "models";
        if (fs::is_directory(modelsDir, error)) {
            for (const auto& entry : fs::directory_iterator(modelsDir, error)) {
                if (entry.is_directory(error) && fs::exists(entry.path() / "model.onnx", error)) {
                    availableModels.push_back(entry.path().filename().string());
                }
            }
        }
        if (!availableModels.empty()) {
            std::sort(availableModels.begin(), availableModels.end());
            result.preferredModel = availableModels.front();
            result.provenance["preferred_model"] = "data_directory:models";
        }
    }

    if (const auto preload = configured("embeddings.preload_on_startup")) {
        if (const auto parsed = parseBoolValue(*preload)) {
            result.preloadOnStartup = *parsed;
            result.provenance["preload"] = "config:embeddings.preload_on_startup";
        } else {
            result.warnings.push_back(fmt::format(
                "invalid embeddings.preload_on_startup value '{}'; using false", *preload));
        }
    } else {
        result.provenance["preload"] = "default:false";
    }
    if (const auto environmentPreload =
            yams::config::read_env_bool("YAMS_EMBED_PRELOAD_ON_STARTUP").value) {
        result.preloadOnStartup = *environmentPreload;
        result.provenance["preload"] = "environment:YAMS_EMBED_PRELOAD_ON_STARTUP";
    }

    const auto applyRuntimeSize = [&](std::string_view key, std::optional<std::size_t>& target,
                                      std::string_view provenanceKey) {
        if (const auto raw = configured(key)) {
            if (const auto parsed = parseSize(*raw)) {
                target = parsed;
                result.provenance[std::string(provenanceKey)] = "config:" + std::string(key);
            } else {
                result.warnings.push_back(
                    fmt::format("invalid {} value '{}'; ignoring", key, *raw));
            }
        }
    };
    applyRuntimeSize("embeddings.runtime.batch_size", result.runtime.batchSize, "batch_size");
    applyRuntimeSize("embeddings.runtime.batch_target", result.runtime.batchTarget, "batch_target");
    if (const auto raw = configured("embeddings.runtime.repair_lock_timeout_ms")) {
        if (const auto parsed = parseUnsignedIntegral<std::uint64_t>(*raw)) {
            result.runtime.repairLockTimeoutMs = parsed;
            result.provenance["repair_lock_timeout_ms"] =
                "config:embeddings.runtime.repair_lock_timeout_ms";
        } else {
            result.warnings.push_back(fmt::format(
                "invalid embeddings.runtime.repair_lock_timeout_ms value '{}'; ignoring", *raw));
        }
    }
    if (const auto value = yams::config::read_env_size("YAMS_EMBED_BATCH").value) {
        result.runtime.batchSize = value;
        result.provenance["batch_size"] = "environment:YAMS_EMBED_BATCH";
    }
    if (const auto value = yams::config::read_env_size("YAMS_EMBED_BATCH_TARGET").value) {
        result.runtime.batchTarget = value;
        result.provenance["batch_target"] = "environment:YAMS_EMBED_BATCH_TARGET";
    }
    if (const auto value = yams::config::read_env_size("YAMS_REPAIR_LOCK_TIMEOUT_MS").value) {
        result.runtime.repairLockTimeoutMs = static_cast<std::uint64_t>(*value);
        result.provenance["repair_lock_timeout_ms"] = "environment:YAMS_REPAIR_LOCK_TIMEOUT_MS";
    }

    if (!resolvedDataDir.empty()) {
        if (const auto databaseDimension = readDbEmbeddingDim(resolvedDataDir / "vectors.db")) {
            result.dimension = databaseDimension;
            result.dimensionSource = EmbeddingDimensionSource::ExistingDatabase;
            result.provenance["dimension"] = "database:vectors.db";
        } else if (const auto sentinelDimension = readVectorSentinelDim(resolvedDataDir)) {
            result.dimension = sentinelDimension;
            result.dimensionSource = EmbeddingDimensionSource::Sentinel;
            result.provenance["dimension"] = "sentinel:vectors_sentinel.json";
        }
    }
    if (!result.dimension) {
        static constexpr std::string_view kDimensionKeys[] = {
            "embeddings.embedding_dim", "vector_database.embedding_dim", "vector_index.dimension"};
        std::optional<std::size_t> selectedDimension;
        std::string selectedKey;
        for (const auto key : kDimensionKeys) {
            const auto raw = configured(key);
            if (!raw) {
                continue;
            }
            const auto parsed = parseSize(*raw);
            if (!parsed || *parsed == 0) {
                result.warnings.push_back(
                    fmt::format("invalid {} value '{}'; ignoring", key, *raw));
                continue;
            }
            if (!selectedDimension) {
                selectedDimension = parsed;
                selectedKey = key;
            } else if (*selectedDimension != *parsed) {
                noteConflict("dimension", selectedKey, std::to_string(*selectedDimension), key,
                             std::to_string(*parsed));
            }
        }
        if (!selectedDimension && result.isTrainingFree) {
            if (const auto raw = configured("embeddings.simeon.output_dim")) {
                if (const auto parsed = parseSize(*raw); parsed && *parsed > 0) {
                    selectedDimension = parsed;
                    selectedKey = "embeddings.simeon.output_dim";
                }
            }
        }
        if (selectedDimension) {
            result.dimension = selectedDimension;
            result.dimensionSource = EmbeddingDimensionSource::Config;
            result.provenance["dimension"] = "config:" + selectedKey;
        }
    }
    if (!result.dimension) {
        if (const auto environmentDimension = yams::config::read_env_size("YAMS_EMBED_DIM").value;
            environmentDimension && *environmentDimension > 0) {
            result.dimension = environmentDimension;
            result.dimensionSource = EmbeddingDimensionSource::Environment;
            result.provenance["dimension"] = "environment:YAMS_EMBED_DIM";
        }
    }
    if (!result.dimension && !resolvedDataDir.empty() && !result.preferredModel.empty()) {
        const auto modelDirectory = resolvedDataDir / "models" / result.preferredModel;
        if (const auto modelConfigDimension =
                yams::vector::dimres::dim_from_model_config(modelDirectory)) {
            result.dimension = modelConfigDimension;
            result.dimensionSource = EmbeddingDimensionSource::ModelConfig;
            result.provenance["dimension"] = "model_config:" + result.preferredModel;
        } else if (const auto modelNameDimension =
                       yams::vector::dimres::dim_from_model_name(result.preferredModel)) {
            result.dimension = modelNameDimension;
            result.dimensionSource = EmbeddingDimensionSource::ModelName;
            result.provenance["dimension"] = "model_name:" + result.preferredModel;
        }
    }
    if (!result.dimension) {
        result.provenance["dimension"] = "unresolved";
    }

    if (result.backend == "onnxruntime" && !result.preferredModel.empty() &&
        !isSimeonSentinel(result.preferredModel) && !resolvedDataDir.empty()) {
        const auto modelPath = resolvedDataDir / "models" / result.preferredModel / "model.onnx";
        std::error_code error;
        if (!fs::exists(modelPath, error)) {
            const auto warning = fmt::format(
                "backend 'onnxruntime' selected but model file not found: {} (download with: yams "
                "model --download {})",
                modelPath.string(), result.preferredModel);
            spdlog::warn("[ConfigResolver] {}", warning);
            result.warnings.push_back(warning);
        }
    }

    result.runtime.backend = result.backend;
    if (!result.preferredModel.empty()) {
        result.runtime.preferredModel = result.preferredModel;
    }
    result.policyIdentity =
        fmt::format("{}:{}:{}", result.backend,
                    result.preferredModel.empty() ? "unresolved" : result.preferredModel,
                    result.dimension ? std::to_string(*result.dimension) : "unresolved");
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
    [[maybe_unused]] TuneAdvisor::ConfiguredOverrideUpdate configuredOverrideUpdate;

    // An empty provenance map identifies a fresh lifecycle snapshot. Clear only the overrides
    // owned by this resolver so a prior in-process daemon cannot leak typed tuning into the next
    // one. Reload passes the current snapshot (including provenance) and therefore remains
    // incremental.
    if (!tuningConfig.tuneAdvisorOverridesResolved) {
        TuneAdvisor::resetConfiguredOverrides();
        tuningConfig.tuneAdvisorOverridesResolved = true;
    }

    const auto findSection = [&](std::string_view name) -> const ConfigSection* {
        auto it = sections.find(std::string(name));
        return it == sections.end() ? nullptr : &it->second;
    };
    const auto noteSource = [&](std::string_view sectionName, std::string_view key) {
        const std::string qualified = std::string(sectionName) + "." + std::string(key);
        tuningConfig.provenance.insert_or_assign(qualified, "config:" + qualified);
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
            noteSource("tuning", key);
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
            noteSource("tuning", key);
        }
    };
    const auto updateUint32 = [&](std::string_view key, std::uint32_t& field) {
        if (auto value = parseUint32(tuning, "tuning", key)) {
            field = *value;
            noteSource("tuning", key);
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

    const auto* ipc = findSection("tuning.ipc");
    if (auto value = parseUint32(ipc, "tuning.ipc", "timeout_ms")) {
        if (*value >= 500 && *value <= 600000) {
            TuneAdvisor::setIpcTimeoutMs(*value);
            noteSource("tuning.ipc", "timeout_ms");
        } else {
            spdlog::warn("Config: tuning.ipc.timeout_ms outside range 500..600000");
        }
    }
    if (auto value = parseUint32(ipc, "tuning.ipc", "stream_chunk_timeout_ms")) {
        if (*value >= 1000 && *value <= 600000) {
            TuneAdvisor::setStreamChunkTimeoutMs(*value);
            noteSource("tuning.ipc", "stream_chunk_timeout_ms");
        } else {
            spdlog::warn("Config: tuning.ipc.stream_chunk_timeout_ms outside range 1000..600000");
        }
    }

    const auto* resource = findSection("tuning.resource");
    if (auto value = parseBoolean(resource, "tuning.resource", "enabled")) {
        TuneAdvisor::setEnableResourceGovernor(*value);
        noteSource("tuning.resource", "enabled");
    }
    if (auto value = parseBoolean(resource, "tuning.resource", "admission_control")) {
        TuneAdvisor::setEnableAdmissionControl(*value);
        noteSource("tuning.resource", "admission_control");
    }
    if (auto value = parseUint32(resource, "tuning.resource", "warning_scale_percent")) {
        if (*value >= 10 && *value <= 100) {
            TuneAdvisor::setGovernorWarningScalePercent(*value);
            noteSource("tuning.resource", "warning_scale_percent");
        } else {
            spdlog::warn("Config: tuning.resource.warning_scale_percent outside range 10..100");
        }
    }
    if (auto value = parseUnsigned(resource, "tuning.resource", "memory_budget_bytes")) {
        constexpr std::uint64_t kMinimumMemoryBudget = 64ULL * 1024ULL * 1024ULL;
        if (*value >= kMinimumMemoryBudget) {
            TuneAdvisor::setMemoryBudgetBytes(*value);
            noteSource("tuning.resource", "memory_budget_bytes");
        } else {
            spdlog::warn("Config: tuning.resource.memory_budget_bytes below 64 MiB");
        }
    }
    const auto applyResourceThreshold = [&](std::string_view key, auto setter) {
        if (auto value = parseFloating(resource, "tuning.resource", key)) {
            if (*value >= 0.5 && *value <= 0.99) {
                setter(*value);
                noteSource("tuning.resource", key);
            } else {
                spdlog::warn("Config: tuning.resource.{} outside range 0.5..0.99", key);
            }
        }
    };
    applyResourceThreshold("memory_warning_threshold", &TuneAdvisor::setMemoryWarningThreshold);
    applyResourceThreshold("memory_critical_threshold", &TuneAdvisor::setMemoryCriticalThreshold);
    applyResourceThreshold("memory_emergency_threshold", &TuneAdvisor::setMemoryEmergencyThreshold);
    if (auto value = parseUint32(resource, "tuning.resource", "memory_hysteresis_ms")) {
        if (*value >= 10 && *value <= 10000) {
            TuneAdvisor::setMemoryHysteresisMs(*value);
            noteSource("tuning.resource", "memory_hysteresis_ms");
        } else {
            spdlog::warn("Config: tuning.resource.memory_hysteresis_ms outside range 10..10000");
        }
    }
    if (auto value = parseUint32(resource, "tuning.resource", "cpu_hysteresis_ms")) {
        if (*value >= 10 && *value <= 10000) {
            TuneAdvisor::setCpuLevelHysteresisMs(*value);
            noteSource("tuning.resource", "cpu_hysteresis_ms");
        } else {
            spdlog::warn("Config: tuning.resource.cpu_hysteresis_ms outside range 10..10000");
        }
    }

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
            noteSource("tuning.post_ingest", "coalesce_ms");
        } else {
            spdlog::warn("Config: tuning.post_ingest.coalesce_ms outside range 0..{}",
                         kMaxCoalesceMs);
        }
    }
    if (auto value = parseUint32(postIngest, "tuning.post_ingest", "rpc_queue_max")) {
        if (*value >= 10 && *value <= 1'000'000) {
            TuneAdvisor::setPostIngestRpcQueueMax(*value);
            noteSource("tuning.post_ingest", "rpc_queue_max");
        } else {
            spdlog::warn("Config: tuning.post_ingest.rpc_queue_max outside range 10..1000000");
        }
    }
    if (auto value = parseUint32(postIngest, "tuning.post_ingest", "rpc_max_per_batch")) {
        if (*value >= 1 && *value <= 1024) {
            TuneAdvisor::setPostIngestRpcMaxPerBatch(*value);
            noteSource("tuning.post_ingest", "rpc_max_per_batch");
        } else {
            spdlog::warn("Config: tuning.post_ingest.rpc_max_per_batch outside range 1..1024");
        }
    }
    const auto applyPostIngestCap = [&](std::string_view key, std::uint32_t minimum,
                                        std::uint32_t maximum, auto setter) {
        auto value = parseUnsigned(postIngest, "tuning.post_ingest", key);
        if (!value) {
            return;
        }
        if (*value < minimum || *value > maximum) {
            spdlog::warn("Config: tuning.post_ingest.{} outside range {}..{}", key, minimum,
                         maximum);
            return;
        }
        setter(static_cast<std::uint32_t>(*value));
        noteSource("tuning.post_ingest", key);
    };
    applyPostIngestCap("total_concurrent", 1, 256, &TuneAdvisor::setPostIngestTotalConcurrent);
    applyPostIngestCap("embed_concurrent", 1, 32, &TuneAdvisor::setPostEmbedConcurrent);
    applyPostIngestCap("extraction_concurrent", 1, 64, &TuneAdvisor::setPostExtractionConcurrent);
    applyPostIngestCap("kg_concurrent", 1, 64, &TuneAdvisor::setPostKgConcurrent);
    applyPostIngestCap("symbol_concurrent", 1, 32, &TuneAdvisor::setPostSymbolConcurrent);
    applyPostIngestCap("entity_concurrent", 1, 16, &TuneAdvisor::setPostEntityConcurrent);
    applyPostIngestCap("title_concurrent", 1, 16, &TuneAdvisor::setPostTitleConcurrent);
    applyPostIngestCap("batch_size", 1, 256, &TuneAdvisor::setPostIngestBatchSize);

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

void ConfigResolver::applySearchMaintenance(const ConfigSections& sections, DaemonConfig& config) {
    const auto section = sections.find("search");
    if (section == sections.end()) {
        return;
    }
    const auto value = section->second.find("automatic_rebuilds");
    if (value == section->second.end()) {
        return;
    }
    const auto parsed = parseBoolValue(value->second);
    if (!parsed.has_value()) {
        spdlog::warn("Config: failed to parse search.automatic_rebuilds as boolean");
        return;
    }
    config.searchMaintenance.automaticRebuildsEnabled = parsed;
    config.searchMaintenance.automaticRebuildsSource = "config:search.automatic_rebuilds";
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
