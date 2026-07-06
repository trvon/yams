// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/config/config_helpers.h>
#include <yams/daemon/components/ConfigResolver.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <string_view>

namespace yams::daemon {
namespace {

std::string_view trimView(std::string_view raw) {
    while (!raw.empty() && std::isspace(static_cast<unsigned char>(raw.front())) != 0) {
        raw.remove_prefix(1);
    }
    while (!raw.empty() && std::isspace(static_cast<unsigned char>(raw.back())) != 0) {
        raw.remove_suffix(1);
    }
    return raw;
}

template <typename T> std::optional<T> parseUnsignedIntegral(std::string_view raw) {
    raw = trimView(raw);
    if (raw.empty() || raw.front() == '-') {
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

std::optional<double> parseDouble(const std::string& raw) {
    auto view = trimView(raw);
    if (view.empty()) {
        return std::nullopt;
    }
    std::string value{view};
    char* end = nullptr;
    errno = 0;
    double parsed = std::strtod(value.c_str(), &end);
    if (errno != 0 || end == nullptr || end == value.c_str() || *end != '\0') {
        return std::nullopt;
    }
    return parsed;
}

std::string getenvValue(const char* name) {
    const char* raw = std::getenv(name); // NOLINT(concurrency-mt-unsafe)
    return raw != nullptr ? std::string(raw) : std::string{};
}

std::optional<std::string> readEnvString(const char* name) {
    auto raw = getenvValue(name);
    if (raw.empty())
        return std::nullopt;
    return raw;
}

std::optional<std::uint32_t> readEnvU32(const char* name) {
    auto raw = getenvValue(name);
    if (raw.empty())
        return std::nullopt;
    return parseUnsignedIntegral<std::uint32_t>(raw);
}

std::optional<float> readEnvFloat(const char* name) {
    auto raw = getenvValue(name);
    if (raw.empty())
        return std::nullopt;
    auto parsed = parseDouble(raw);
    if (!parsed) {
        return std::nullopt;
    }
    return static_cast<float>(*parsed);
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

std::optional<bool> parseTomlBool(std::string_view s) {
    std::string v(s);
    std::transform(v.begin(), v.end(), v.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (v == "true" || v == "1" || v == "yes" || v == "on")
        return true;
    if (v == "false" || v == "0" || v == "no" || v == "off")
        return false;
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
    if (auto explicitPath = getenvValue("YAMS_CONFIG_PATH"); !explicitPath.empty()) {
        std::filesystem::path p{explicitPath};
        if (std::filesystem::exists(p))
            return p;
    }
    // YAMS_CONFIG is the canonical env var used by test fixtures and
    // config_helpers.cpp; accept it here so resolver-based policies respect
    // the harness-supplied TOML.
    if (auto fixtureConfig = getenvValue("YAMS_CONFIG"); !fixtureConfig.empty()) {
        std::filesystem::path p{fixtureConfig};
        if (std::filesystem::exists(p))
            return p;
    }
#ifdef _WIN32
    // Windows: prefer roaming APPDATA for config (matches get_config_dir())
    if (auto appdata = getenvValue("APPDATA"); !appdata.empty()) {
        std::filesystem::path p = std::filesystem::path(appdata) / "yams" / "config.toml";
        if (std::filesystem::exists(p))
            return p;
    }
#endif
    if (auto xdg = getenvValue("XDG_CONFIG_HOME"); !xdg.empty()) {
        std::filesystem::path p = std::filesystem::path(xdg) / "yams" / "config.toml";
        if (std::filesystem::exists(p))
            return p;
    }
    if (auto home = getenvValue("HOME"); !home.empty()) {
        std::filesystem::path p = std::filesystem::path(home) / ".config" / "yams" / "config.toml";
        if (std::filesystem::exists(p))
            return p;
    }
#ifdef _WIN32
    // Windows: check LOCALAPPDATA
    if (auto localAppData = getenvValue("LOCALAPPDATA"); !localAppData.empty()) {
        std::filesystem::path p = std::filesystem::path(localAppData) / "yams" / "config.toml";
        if (std::filesystem::exists(p))
            return p;
    }
#endif
    return {};
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

    if (auto envp = getenvValue("YAMS_EMBED_BACKEND"); !envp.empty()) {
        return normalize(std::move(envp));
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

ConfigResolver::EmbeddingRuntimePolicy ConfigResolver::resolveEmbeddingRuntimePolicy() {
    EmbeddingRuntimePolicy policy;

    try {
        namespace fs = std::filesystem;
        fs::path cfgPath = resolveDefaultConfigPath();
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            auto kv = parseSimpleTomlFlat(cfgPath);
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
    if (auto v = readEnvString("YAMS_EMBED_BACKEND"))
        policy.backend = std::move(v);
    if (auto v = readEnvString("YAMS_PREFERRED_MODEL"))
        policy.preferredModel = std::move(v);
    if (auto v = readEnvString("YAMS_EMBED_BATCH")) {
        try {
            policy.batchSize = static_cast<std::size_t>(std::stoull(*v));
        } catch (...) {
            spdlog::debug("config: failed to parse YAMS_EMBED_BATCH size_t");
        }
    }
    if (auto v = readEnvString("YAMS_EMBED_BATCH_TARGET")) {
        try {
            policy.batchTarget = static_cast<std::size_t>(std::stoull(*v));
        } catch (...) {
            spdlog::debug("config: failed to parse YAMS_EMBED_BATCH_TARGET size_t");
        }
    }
    if (auto v = readEnvString("YAMS_REPAIR_LOCK_TIMEOUT_MS")) {
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
            if (auto it = kv.find("embeddings.simeon.bm25.max_corpus_bytes"); it != kv.end())
                policy.maxCorpusBytes = parseTomlSize(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.build_doc_chunk_bytes"); it != kv.end())
                policy.buildDocChunkBytes = parseTomlSize(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.build_doc_max_chunks"); it != kv.end())
                policy.buildDocMaxChunks = parseTomlSize(it->second);
            if (auto it = kv.find("embeddings.simeon.bm25.fragment_geometry.enabled");
                it != kv.end())
                policy.fragmentGeometryEnabled = parseTomlBool(it->second);
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

    if (auto raw = getenvValue("YAMS_SIMEON_BM25_ENABLED"); !raw.empty()) {
        if (auto b = parseTomlBool(raw))
            policy.enabled = b;
    }
    if (auto v = readEnvString("YAMS_SIMEON_BM25_VARIANT"))
        policy.variant = std::move(v);
    if (auto v = readEnvFloat("YAMS_SIMEON_BM25_SUBWORD_GAMMA"))
        policy.subwordGamma = v;
    if (auto v = readEnvString("YAMS_SIMEON_BM25_MAX_CORPUS_DOCS"))
        policy.maxCorpusDocs = parseSize(*v);
    if (auto v = readEnvString("YAMS_SIMEON_BM25_MAX_CORPUS_BYTES"))
        policy.maxCorpusBytes = parseSize(*v);
    if (auto v = readEnvString("YAMS_SIMEON_BM25_BUILD_DOC_CHUNK_BYTES"))
        policy.buildDocChunkBytes = parseSize(*v);
    if (auto v = readEnvString("YAMS_SIMEON_BM25_BUILD_DOC_MAX_CHUNKS"))
        policy.buildDocMaxChunks = parseSize(*v);
    if (auto raw = getenvValue("YAMS_SIMEON_BM25_FRAGMENT_GEOMETRY_ENABLED"); !raw.empty()) {
        if (auto b = parseTomlBool(raw))
            policy.fragmentGeometryEnabled = b;
    }
    if (auto v = readEnvString("YAMS_SIMEON_BM25_FRAGMENT_GEOMETRY_MAX_DOCS"))
        policy.fragmentGeometryMaxDocs = parseSize(*v);
    if (auto v = readEnvString("YAMS_SIMEON_BM25_FRAGMENT_GEOMETRY_MAX_CORPUS_BYTES"))
        policy.fragmentGeometryMaxCorpusBytes = parseSize(*v);
    if (auto v = readEnvString("YAMS_SIMEON_BM25_FRAGMENT_GEOMETRY_PMI_SAMPLE_DOCS"))
        policy.fragmentGeometryPmiSampleDocs = parseSize(*v);
    if (auto v = readEnvString("YAMS_SIMEON_BM25_FRAGMENT_GEOMETRY_PMI_SAMPLE_BYTES"))
        policy.fragmentGeometryPmiSampleBytes = parseSize(*v);
    if (auto raw = getenvValue("YAMS_SIMEON_BM25_ROUTER_ENABLED"); !raw.empty()) {
        if (auto b = parseTomlBool(raw))
            policy.routerEnabled = b;
    }
    if (auto v = readEnvString("YAMS_SIMEON_BM25_ROUTER_PRESET"))
        policy.routerPreset = std::move(v);

    if (auto raw = getenvValue("YAMS_SIMEON_STRATEGY_ROUTER_ENABLED"); !raw.empty()) {
        if (auto b = parseTomlBool(raw))
            policy.strategyRouterEnabled = b;
    }

    return policy;
}

} // namespace yams::daemon
