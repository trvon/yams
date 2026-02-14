#pragma once

#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/resource/gpu_info.h>

#include <onnxruntime_cxx_api.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::onnx_util {

namespace detail {

// Read an environment variable, returning fallback if unset or empty.
inline std::string envOr(const char* name, const std::string& fallback) {
    const char* v = std::getenv(name);
    return (v && v[0]) ? std::string(v) : fallback;
}

inline bool envBool(const char* name, bool fallback) {
    const char* v = std::getenv(name);
    if (!v || !v[0])
        return fallback;
    std::string s(v);
    std::transform(s.begin(), s.end(), s.begin(), ::tolower);
    if (s == "0" || s == "false" || s == "no" || s == "off")
        return false;
    if (s == "1" || s == "true" || s == "yes" || s == "on")
        return true;
    return fallback;
}

inline int envInt(const char* name, int fallback) {
    const char* v = std::getenv(name);
    if (!v || !v[0])
        return fallback;
    try {
        return std::stoi(v);
    } catch (...) {
        return fallback;
    }
}

inline size_t envSizeT(const char* name, size_t fallback) {
    const char* v = std::getenv(name);
    if (!v || !v[0])
        return fallback;
    try {
        return static_cast<size_t>(std::stoull(v));
    } catch (...) {
        return fallback;
    }
}

inline std::optional<size_t> envSizeTOpt(const char* name) {
    const char* v = std::getenv(name);
    if (!v || !v[0])
        return std::nullopt;
    try {
        return static_cast<size_t>(std::stoull(v));
    } catch (...) {
        return std::nullopt;
    }
}

inline double profileGpuMemFraction() {
    // Map TuneAdvisor profile to a conservative VRAM fraction.
    // Efficient  -> ~55% (leave headroom for desktop + other workloads)
    // Balanced   -> ~70%
    // Aggressive -> ~85%
    const double s = yams::daemon::TuneAdvisor::profileScale();
    return std::clamp(0.55 + 0.30 * s, 0.10, 0.95);
}

inline std::string arenaExtendStrategyName(int v) {
    // ORT expects the string names for MIGraphX arena strategy.
    // See ORT's migraphx_execution_provider_info.cc mapping.
    switch (v) {
        case 1:
            return "kSameAsRequested";
        case 0:
        default:
            return "kNextPowerOfTwo";
    }
}

inline bool directoryHasMxrFiles(const std::filesystem::path& dir) {
    std::error_code ec;
    if (dir.empty() || !std::filesystem::exists(dir, ec) || !std::filesystem::is_directory(dir, ec))
        return false;
    for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
        if (ec)
            return false;
        if (!entry.is_regular_file(ec) || ec)
            continue;
        if (entry.path().extension() == ".mxr")
            return true;
    }
    return false;
}

inline void setEnvIfAbsent(const char* name, const std::string& value) {
    if (value.empty())
        return;
    const char* existing = std::getenv(name);
    if (existing && existing[0])
        return;
#ifdef _WIN32
    (void)_putenv_s(name, value.c_str());
#else
    (void)setenv(name, value.c_str(), 0);
#endif
}

} // namespace detail

// Try to attach the best available GPU execution provider at runtime.
// Queries Ort::GetAvailableProviders() so this works with any ONNX Runtime
// build (Homebrew, pip, Conan, etc.) without compile-time defines.
//
// modelCacheDir: optional directory for CoreML model cache (eliminates
// recompilation overhead on subsequent loads).
inline std::string appendGpuProvider(Ort::SessionOptions& opts,
                                     const std::string& modelCacheDir = "") {
    constexpr const char kProviderMIGraphX[] = "MIGraphX";
    constexpr const char kProviderMIGraphXAvailable[] = "MIGraphXExecutionProvider";
    constexpr const char kMIGraphXModelCacheDirKey[] = "migraphx_model_cache_dir";
    constexpr const char kOrtMIGraphXModelCachePathEnv[] = "ORT_MIGRAPHX_MODEL_CACHE_PATH";

    if (detail::envBool("YAMS_ONNX_FORCE_CPU", false) ||
        detail::envBool("YAMS_ONNX_DISABLE_GPU", false)) {
        static std::atomic<bool> logged{false};
        if (!logged.exchange(true)) {
            spdlog::info("[ONNX] GPU execution providers disabled via env; using CPU");
        }
        (void)opts;
        (void)modelCacheDir;
        return "cpu";
    }

    std::vector<std::string> providers;
    try {
        providers = Ort::GetAvailableProviders();
    } catch (...) {
        spdlog::debug("[ONNX] Could not query available providers");
        return "cpu";
    }

    auto has = [&](const char* name) {
        return std::find(providers.begin(), providers.end(), name) != providers.end();
    };

    // -----------------------------------------------------------------
    // CoreML (macOS) — modern string-map API with optimized defaults
    // -----------------------------------------------------------------
    if (has("CoreMLExecutionProvider")) {
        try {
            std::unordered_map<std::string, std::string> coreml_opts;

            // CPUAndNeuralEngine avoids GPU partitioning overhead for
            // transformer models with dynamic-shape rotary embeddings.
            coreml_opts["MLComputeUnits"] =
                detail::envOr("YAMS_COREML_COMPUTE_UNITS", "CPUAndNeuralEngine");

            // MLProgram has better op coverage than NeuralNetwork, resulting
            // in fewer partitions and less data-transfer overhead.
            coreml_opts["ModelFormat"] = detail::envOr("YAMS_COREML_MODEL_FORMAT", "MLProgram");

            // Model cache eliminates ~2s CoreML recompilation per session.
            std::string cacheDir = detail::envOr("YAMS_COREML_CACHE_DIR", modelCacheDir);
            if (!cacheDir.empty()) {
                coreml_opts["ModelCacheDirectory"] = cacheDir;
            }

            opts.AppendExecutionProvider("CoreML", coreml_opts);
            static std::atomic<bool> logged_coreml{false};
            if (!logged_coreml.exchange(true)) {
                spdlog::info("[ONNX] CoreML execution provider enabled "
                             "(compute={}, format={}, cache={})",
                             coreml_opts["MLComputeUnits"], coreml_opts["ModelFormat"],
                             cacheDir.empty() ? "(none)" : cacheDir);
            } else {
                spdlog::debug("[ONNX] CoreML execution provider attached (pooled session)");
            }
            return "coreml";
        } catch (const std::exception& e) {
            spdlog::warn("[ONNX] CoreML provider failed: {}", e.what());
        }
    }

    // -----------------------------------------------------------------
    // MIGraphX (AMD ROCm) — modern string-map API
    // -----------------------------------------------------------------
    if (has(kProviderMIGraphXAvailable)) {
        try {
            // IMPORTANT: In ORT 1.23.x the legacy OrtMIGraphXProviderOptions constructor
            // does not propagate model cache dir / compiled caching settings.
            // Use the provider-options map API instead.
            std::unordered_map<std::string, std::string> migraphx_opts;

            const int deviceId = detail::envInt("YAMS_MIGRAPHX_DEVICE_ID", 0);
            const bool fp16 = detail::envBool("YAMS_MIGRAPHX_FP16", false);
            const bool fp8 = detail::envBool("YAMS_MIGRAPHX_FP8", false);
            const bool int8 = detail::envBool("YAMS_MIGRAPHX_INT8", false);
            const bool exhaustiveTune = detail::envBool("YAMS_MIGRAPHX_EXHAUSTIVE_TUNE", false);
            const int arenaExtend = detail::envInt("YAMS_MIGRAPHX_ARENA_EXTEND_STRATEGY", 0);

            migraphx_opts["device_id"] = std::to_string(deviceId);
            migraphx_opts["migraphx_fp16_enable"] = fp16 ? "1" : "0";
            migraphx_opts["migraphx_fp8_enable"] = fp8 ? "1" : "0";
            migraphx_opts["migraphx_int8_enable"] = int8 ? "1" : "0";
            migraphx_opts["migraphx_exhaustive_tune"] = exhaustiveTune ? "1" : "0";
            migraphx_opts["migraphx_arena_extend_strategy"] =
                detail::arenaExtendStrategyName(arenaExtend);

            // Profile-aware VRAM budgeting. If YAMS_MIGRAPHX_MEM_LIMIT is set, it always wins.
            // Otherwise, set a conservative fraction of total VRAM (when detectable) so we
            // don't default to effectively "use 100% GPU".
            {
                if (auto overrideLimit = detail::envSizeTOpt("YAMS_MIGRAPHX_MEM_LIMIT")) {
                    migraphx_opts["migraphx_mem_limit"] = std::to_string(*overrideLimit);
                } else {
                    const auto& gpu = yams::daemon::resource::detectGpu();
                    if (gpu.detected && gpu.vramBytes > 0 &&
                        gpu.vramBytes <=
                            static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
                        const double frac = detail::profileGpuMemFraction();
                        uint64_t budget = static_cast<uint64_t>(static_cast<double>(gpu.vramBytes) *
                                                                std::clamp(frac, 0.0, 1.0));
                        constexpr uint64_t kMinBudgetBytes = 256ull * 1024ull * 1024ull;
                        budget = std::clamp(budget, kMinBudgetBytes, gpu.vramBytes);
                        migraphx_opts["migraphx_mem_limit"] =
                            std::to_string(static_cast<size_t>(budget));
                        spdlog::debug("[ONNX] MIGraphX mem budget: {:.0f}% of VRAM ({} bytes / {} "
                                      "bytes) [profile_scale={:.2f}]",
                                      frac * 100.0, budget, gpu.vramBytes,
                                      yams::daemon::TuneAdvisor::profileScale());
                    }
                }
            }

            // Optional INT8 calibration settings
            const bool useNativeCalTable =
                detail::envBool("YAMS_MIGRAPHX_USE_NATIVE_CAL_TABLE", false);
            if (useNativeCalTable) {
                migraphx_opts["migraphx_int8_use_native_calibration_table"] = "1";
            }
            if (const char* table = std::getenv("YAMS_MIGRAPHX_INT8_CAL_TABLE")) {
                if (table[0]) {
                    migraphx_opts["migraphx_int8_calibration_table_name"] = table;
                }
            }

            // Compiled model caching (dramatically reduces repeated compile time when sessions
            // are recreated, e.g., after scale-down / eviction).
            const bool saveCompiled = detail::envBool("YAMS_MIGRAPHX_SAVE_COMPILED", true);
            const bool loadCompiled = detail::envBool("YAMS_MIGRAPHX_LOAD_COMPILED", true);
            std::string compiledPath = detail::envOr("YAMS_MIGRAPHX_COMPILED_PATH", "");
            // NOTE: ORT's MIGraphX EP writes a hashed `*.mxr` filename under the provided path.
            // In practice this behaves like a cache directory, not a single fixed file.
            std::filesystem::path cacheDir;
            if (!compiledPath.empty()) {
                std::filesystem::path p(compiledPath);
                cacheDir = (p.extension() == ".mxr") ? p.parent_path() : p;
            } else if (!modelCacheDir.empty()) {
                // modelCacheDir is typically the model directory (e.g.,
                // ~/.local/share/yams/models/<name>).
                cacheDir = std::filesystem::path(modelCacheDir);
            }

            // ORT MIGraphX caching is enabled by setting migraphx_model_cache_dir.
            // ORT does not currently support independent save/load toggles, so we treat
            // either knob disabling as "disable caching".
            if (!cacheDir.empty() && saveCompiled && loadCompiled) {
                std::error_code ec;
                std::filesystem::create_directories(cacheDir, ec);

                const std::string cacheDirStr = cacheDir.string();
                const bool hit = detail::directoryHasMxrFiles(cacheDir);

                // Defensive: some ORT ROCm builds appear to still consult the env override
                // even when provider options are set. Setting this (without overriding user
                // configuration) avoids attempts to write to an empty cache dir (""/hash.mxr).
                detail::setEnvIfAbsent(kOrtMIGraphXModelCachePathEnv, cacheDirStr);
                if (!std::getenv(kOrtMIGraphXModelCachePathEnv) && !cacheDirStr.empty()) {
                    // Unreachable due to setEnvIfAbsent, but keep logic simple.
                    // (If setenv failed, getenv would still be null.)
                    spdlog::warn(
                        "[ONNX] Failed to set ORT_MIGRAPHX_MODEL_CACHE_PATH; caching may fail");
                } else {
                    spdlog::debug("[ONNX] ORT_MIGRAPHX_MODEL_CACHE_PATH={}", cacheDirStr);
                }
                if (hit) {
                    spdlog::debug("[ONNX] MIGraphX compiled cache hit (dir={})", cacheDirStr);
                } else {
                    spdlog::debug("[ONNX] MIGraphX compiled cache miss (dir={})", cacheDirStr);
                }

                migraphx_opts[kMIGraphXModelCacheDirKey] = cacheDirStr;
                if (!hit) {
                    spdlog::debug("[ONNX] MIGraphX will save compiled artifact under: {}",
                                  cacheDirStr);
                }
                spdlog::debug("[ONNX] MIGraphX cache config: migraphx_model_cache_dir='{}' "
                              "ORT_MIGRAPHX_MODEL_CACHE_PATH='{}'",
                              migraphx_opts[kMIGraphXModelCacheDirKey],
                              (std::getenv(kOrtMIGraphXModelCachePathEnv)
                                   ? std::getenv(kOrtMIGraphXModelCachePathEnv)
                                   : "(unset)"));
            }

            opts.AppendExecutionProvider(kProviderMIGraphX, migraphx_opts);
            static std::atomic<bool> logged_migraphx{false};
            if (!logged_migraphx.exchange(true)) {
                spdlog::info("[ONNX] MIGraphX execution provider enabled (AMD GPU via ROCm) "
                             "(device_id={}, fp16={}, fp8={}, int8={}, exhaustive_tune={})",
                             deviceId, fp16, fp8, int8, exhaustiveTune);
            } else {
                spdlog::debug("[ONNX] MIGraphX execution provider attached (pooled session)");
            }
            return "migraphx";
        } catch (const std::exception& e) {
            spdlog::warn("[ONNX] MIGraphX provider failed: {}", e.what());
        }
    }

    // -----------------------------------------------------------------
    // CUDA (NVIDIA) — modern string-map API
    // -----------------------------------------------------------------
    if (has("CUDAExecutionProvider")) {
        try {
            std::unordered_map<std::string, std::string> cuda_opts;
            cuda_opts["device_id"] = "0";
            cuda_opts["arena_extend_strategy"] = "kNextPowerOfTwo";
            cuda_opts["cudnn_conv_algo_search"] = "EXHAUSTIVE";
            cuda_opts["do_copy_in_default_stream"] = "1";
            cuda_opts["enable_cuda_graph"] = detail::envOr("YAMS_CUDA_GRAPH", "0");
            cuda_opts["use_tf32"] = "1";

            // Optional memory limit override
            std::string memLimit = detail::envOr("YAMS_CUDA_MEM_LIMIT", "");
            if (!memLimit.empty()) {
                cuda_opts["gpu_mem_limit"] = memLimit;
            }

            opts.AppendExecutionProvider("CUDA", cuda_opts);
            static std::atomic<bool> logged_cuda{false};
            if (!logged_cuda.exchange(true)) {
                spdlog::info("[ONNX] CUDA execution provider enabled (device_id=0)");
            } else {
                spdlog::debug("[ONNX] CUDA execution provider attached (pooled session)");
            }
            return "cuda";
        } catch (const std::exception& e) {
            spdlog::warn("[ONNX] CUDA provider failed: {}", e.what());
        }
    }

    // -----------------------------------------------------------------
    // DirectML (Windows) — modern string-map API with explicit device_id
    // -----------------------------------------------------------------
    if (has("DmlExecutionProvider")) {
        try {
            std::unordered_map<std::string, std::string> dml_opts;
            dml_opts["device_id"] = detail::envOr("YAMS_DML_DEVICE_ID", "0");

            opts.AppendExecutionProvider("DML", dml_opts);
            static std::atomic<bool> logged_dml{false};
            if (!logged_dml.exchange(true)) {
                spdlog::info("[ONNX] DirectML execution provider enabled");
            } else {
                spdlog::debug("[ONNX] DirectML execution provider attached (pooled session)");
            }
            return "directml";
        } catch (const std::exception& e) {
            spdlog::warn("[ONNX] DirectML provider failed: {}", e.what());
        }
    }

    spdlog::warn("[ONNX] No GPU execution provider available, using CPU");
    return "cpu";
}

} // namespace yams::onnx_util
