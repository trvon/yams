#pragma once

#include <onnxruntime_cxx_api.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
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

} // namespace detail

// Try to attach the best available GPU execution provider at runtime.
// Queries Ort::GetAvailableProviders() so this works with any ONNX Runtime
// build (Homebrew, pip, Conan, etc.) without compile-time defines.
//
// modelCacheDir: optional directory for CoreML model cache (eliminates
// recompilation overhead on subsequent loads).
inline std::string appendGpuProvider(Ort::SessionOptions& opts,
                                     const std::string& modelCacheDir = "") {
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
    if (has("MIGraphXExecutionProvider")) {
        try {
            std::unordered_map<std::string, std::string> migraphx_opts;
            migraphx_opts["device_id"] = "0";
            migraphx_opts["migraphx_fp16_enable"] = detail::envOr("YAMS_MIGRAPHX_FP16", "0");
            migraphx_opts["migraphx_exhaustive_tune"] =
                detail::envOr("YAMS_MIGRAPHX_EXHAUSTIVE_TUNE", "0");

            opts.AppendExecutionProvider("MIGraphX", migraphx_opts);
            static std::atomic<bool> logged_migraphx{false};
            if (!logged_migraphx.exchange(true)) {
                spdlog::info("[ONNX] MIGraphX execution provider enabled (AMD GPU via ROCm)");
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
