// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/config/config_helpers.h>
#include <yams/config/detail/config_parse_utils.h>
#include <yams/daemon/components/ConfigResolver.h>

#include <spdlog/spdlog.h>

#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <string_view>

namespace yams::daemon {
namespace {

using yams::config::detail::parseDouble;
using yams::config::detail::parseTomlBool;
using yams::config::detail::parseUnsignedIntegral;

std::optional<std::size_t> parseSize(std::string_view raw) {
    return parseUnsignedIntegral<std::size_t>(raw);
}

std::optional<std::uint32_t> parseTomlU32(const std::string& s) {
    return parseUnsignedIntegral<std::uint32_t>(s);
}

std::optional<std::size_t> parseTomlSize(const std::string& s) {
    return parseSize(s);
}

std::optional<float> parseTomlFloat(const std::string& s) {
    auto parsed = parseDouble(s);
    if (!parsed) {
        return std::nullopt;
    }
    return static_cast<float>(*parsed);
}

} // namespace

bool ConfigResolver::envTruthy(const char* value) {
    if (value == nullptr) {
        return false;
    }
    return parseTomlBool(value).value_or(false);
}

bool ConfigResolver::resolvePluginDirStrict(bool configuredStrict) {
    return yams::config::read_env_bool("YAMS_PLUGIN_DIR_STRICT").valueOr(configuredStrict);
}

std::filesystem::path ConfigResolver::resolveDefaultConfigPath() {
    const auto path = yams::config::get_config_path();
    return std::filesystem::exists(path) ? path : std::filesystem::path{};
}

std::map<std::string, std::string>
ConfigResolver::parseSimpleTomlFlat(const std::filesystem::path& path) {
    return yams::config::parse_simple_toml(path);
}

std::string ConfigResolver::resolveEmbeddingBackend(const std::string& defaultValue) {
    auto normalize = [](std::string s) {
        for (auto& c : s)
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (s == "onnx" || s == "onnxruntime" || s == "onnx-runtime" || s == "ort" ||
            s == "local_onnx") {
            return std::string("onnxruntime");
        }
        if (s == "hybrid" || s == "local") {
            return std::string("daemon");
        }
        return s;
    };

    if (auto envp = yams::config::getenv_copy("YAMS_EMBED_BACKEND"); !envp.empty()) {
        return normalize(std::move(envp));
    }

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath = resolveDefaultConfigPath();
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            auto kv = yams::config::parse_simple_toml(cfgPath);
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

ConfigResolver::SimeonEncoderPolicy ConfigResolver::resolveSimeonEncoderPolicy() {
    SimeonEncoderPolicy policy;

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath = resolveDefaultConfigPath();
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            auto kv = yams::config::parse_simple_toml(cfgPath);
            if (auto it = kv.find("embeddings.simeon.encoder_profile");
                it != kv.end() && !it->second.empty())
                policy.encoderProfile = it->second;
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

    if (auto v = yams::config::getenv_nonempty("YAMS_SIMEON_NGRAM_MODE"))
        policy.ngramMode = std::move(v);
    if (auto v = yams::config::read_env_u32("YAMS_SIMEON_NGRAM_MIN").value)
        policy.ngramMin = v;
    if (auto v = yams::config::read_env_u32("YAMS_SIMEON_NGRAM_MAX").value)
        policy.ngramMax = v;
    if (auto v = yams::config::read_env_u32("YAMS_SIMEON_SKETCH_DIM").value)
        policy.sketchDim = v;
    if (auto v = yams::config::read_env_u32("YAMS_SIMEON_OUTPUT_DIM").value)
        policy.outputDim = v;
    if (auto v = yams::config::getenv_nonempty("YAMS_SIMEON_PROJECTION"))
        policy.projection = std::move(v);
    if (auto v = yams::config::read_env_u32("YAMS_SIMEON_PQ_BYTES").value)
        policy.pqBytes = v;

    return policy;
}

ConfigResolver::EmbeddingRuntimePolicy ConfigResolver::resolveEmbeddingRuntimePolicy() {
    EmbeddingRuntimePolicy policy;

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath = resolveDefaultConfigPath();
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            auto kv = yams::config::parse_simple_toml(cfgPath);
            if (auto it = kv.find("embeddings.runtime.backend");
                it != kv.end() && !it->second.empty())
                policy.backend = it->second;
            if (auto it = kv.find("embeddings.runtime.preferred_model");
                it != kv.end() && !it->second.empty())
                policy.preferredModel = it->second;
            if (auto it = kv.find("embeddings.runtime.batch_size"); it != kv.end())
                if (auto v = parseTomlU32(it->second))
                    policy.batchSize = static_cast<std::size_t>(*v);
            if (auto it = kv.find("embeddings.runtime.batch_target"); it != kv.end())
                if (auto v = parseTomlU32(it->second))
                    policy.batchTarget = static_cast<std::size_t>(*v);
            if (auto it = kv.find("embeddings.runtime.repair_lock_timeout_ms"); it != kv.end())
                if (auto val = parseTomlU32(it->second)) {
                    policy.repairLockTimeoutMs = static_cast<std::uint64_t>(*val);
                }
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for embedding runtime: {}", e.what());
    }

    // Env overrides (preserve existing test/CI overlays)
    if (auto v = yams::config::getenv_nonempty("YAMS_EMBED_BACKEND"))
        policy.backend = std::move(v);
    if (auto v = yams::config::getenv_nonempty("YAMS_PREFERRED_MODEL"))
        policy.preferredModel = std::move(v);
    if (auto v = yams::config::getenv_nonempty("YAMS_EMBED_BATCH")) {
        try {
            policy.batchSize = static_cast<std::size_t>(std::stoull(*v));
        } catch (...) {
            spdlog::debug("config: failed to parse YAMS_EMBED_BATCH size_t");
        }
    }
    if (auto v = yams::config::getenv_nonempty("YAMS_EMBED_BATCH_TARGET")) {
        try {
            policy.batchTarget = static_cast<std::size_t>(std::stoull(*v));
        } catch (...) {
            spdlog::debug("config: failed to parse YAMS_EMBED_BATCH_TARGET size_t");
        }
    }
    if (auto v = yams::config::getenv_nonempty("YAMS_REPAIR_LOCK_TIMEOUT_MS")) {
        try {
            policy.repairLockTimeoutMs = static_cast<std::uint64_t>(std::stoull(*v));
        } catch (...) {
            spdlog::debug("config: failed to parse YAMS_REPAIR_LOCK_TIMEOUT_MS uint64");
        }
    }

    return policy;
}

ConfigResolver::SimeonBm25Policy ConfigResolver::resolveSimeonBm25Policy() {
    SimeonBm25Policy policy;

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath = resolveDefaultConfigPath();
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            auto kv = yams::config::parse_simple_toml(cfgPath);
            if (auto it = kv.find("embeddings.simeon.bm25.enabled"); it != kv.end())
                policy.enabled = parseTomlBool(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.variant");
                it != kv.end() && !it->second.empty())
                policy.variant = it->second;
            if (auto it = kv.find("embeddings.simeon.bm25.subword_gamma"); it != kv.end())
                policy.subwordGamma = parseTomlFloat(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.max_corpus_docs"); it != kv.end())
                policy.maxCorpusDocs = parseTomlSize(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.max_corpus_bytes"); it != kv.end())
                policy.maxCorpusBytes = parseTomlSize(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.build_doc_chunk_bytes"); it != kv.end())
                policy.buildDocChunkBytes = parseTomlSize(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.build_doc_max_chunks"); it != kv.end())
                policy.buildDocMaxChunks = parseTomlSize(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.fragment_geometry.enabled");
                it != kv.end())
                policy.fragmentGeometryEnabled = parseTomlBool(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.fragment_geometry.encoder_profile");
                it != kv.end() && !it->second.empty())
                policy.fragmentGeometryEncoderProfile = it->second;
            if (auto it = kv.find("embeddings.simeon.bm25.fragment_geometry.max_docs");
                it != kv.end())
                policy.fragmentGeometryMaxDocs = parseTomlSize(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.fragment_geometry.max_corpus_bytes");
                it != kv.end())
                policy.fragmentGeometryMaxCorpusBytes = parseTomlSize(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.fragment_geometry.pmi_sample_docs");
                it != kv.end())
                policy.fragmentGeometryPmiSampleDocs = parseTomlSize(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.fragment_geometry.pmi_sample_bytes");
                it != kv.end())
                policy.fragmentGeometryPmiSampleBytes = parseTomlSize(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.router.enabled"); it != kv.end())
                policy.routerEnabled = parseTomlBool(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.router.preset");
                it != kv.end() && !it->second.empty())
                policy.routerPreset = it->second;
        }
    } catch (const std::exception& e) {
        spdlog::debug("Error reading config for simeon bm25 policy: {}", e.what());
    }

    if (auto value = yams::config::read_env_bool("YAMS_SIMEON_BM25_ENABLED").value)
        policy.enabled = value;
    if (auto v = yams::config::getenv_nonempty("YAMS_SIMEON_BM25_VARIANT"))
        policy.variant = std::move(v);
    if (auto v = yams::config::read_env_float("YAMS_SIMEON_BM25_SUBWORD_GAMMA").value)
        policy.subwordGamma = v;
    if (auto v = yams::config::getenv_nonempty("YAMS_SIMEON_BM25_MAX_CORPUS_DOCS"))
        policy.maxCorpusDocs = parseSize(*v);
    if (auto v = yams::config::getenv_nonempty("YAMS_SIMEON_BM25_MAX_CORPUS_BYTES"))
        policy.maxCorpusBytes = parseSize(*v);
    if (auto v = yams::config::getenv_nonempty("YAMS_SIMEON_BM25_BUILD_DOC_CHUNK_BYTES"))
        policy.buildDocChunkBytes = parseSize(*v);
    if (auto v = yams::config::getenv_nonempty("YAMS_SIMEON_BM25_BUILD_DOC_MAX_CHUNKS"))
        policy.buildDocMaxChunks = parseSize(*v);
    if (auto value =
            yams::config::read_env_bool("YAMS_SIMEON_BM25_FRAGMENT_GEOMETRY_ENABLED").value)
        policy.fragmentGeometryEnabled = value;
    if (auto v = yams::config::getenv_nonempty("YAMS_SIMEON_BM25_FRAGMENT_GEOMETRY_MAX_DOCS"))
        policy.fragmentGeometryMaxDocs = parseSize(*v);
    if (auto v =
            yams::config::getenv_nonempty("YAMS_SIMEON_BM25_FRAGMENT_GEOMETRY_MAX_CORPUS_BYTES"))
        policy.fragmentGeometryMaxCorpusBytes = parseSize(*v);
    if (auto v =
            yams::config::getenv_nonempty("YAMS_SIMEON_BM25_FRAGMENT_GEOMETRY_PMI_SAMPLE_DOCS"))
        policy.fragmentGeometryPmiSampleDocs = parseSize(*v);
    if (auto v =
            yams::config::getenv_nonempty("YAMS_SIMEON_BM25_FRAGMENT_GEOMETRY_PMI_SAMPLE_BYTES"))
        policy.fragmentGeometryPmiSampleBytes = parseSize(*v);
    if (auto value = yams::config::read_env_bool("YAMS_SIMEON_BM25_ROUTER_ENABLED").value)
        policy.routerEnabled = value;
    if (auto v = yams::config::getenv_nonempty("YAMS_SIMEON_BM25_ROUTER_PRESET"))
        policy.routerPreset = std::move(v);

    if (auto value = yams::config::read_env_bool("YAMS_SIMEON_STRATEGY_ROUTER_ENABLED").value)
        policy.strategyRouterEnabled = value;

    return policy;
}

} // namespace yams::daemon
