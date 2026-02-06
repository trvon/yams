#pragma once

#include <onnxruntime_cxx_api.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::onnx_util {

// Try to attach the best available GPU execution provider at runtime.
// Queries Ort::GetAvailableProviders() so this works with any ONNX Runtime
// build (Homebrew, pip, Conan, etc.) without compile-time defines.
inline void appendGpuProvider(Ort::SessionOptions& opts) {
    std::vector<std::string> providers;
    try {
        providers = Ort::GetAvailableProviders();
    } catch (...) {
        spdlog::debug("[ONNX] Could not query available providers");
        return;
    }

    auto has = [&](const char* name) {
        return std::find(providers.begin(), providers.end(), name) != providers.end();
    };

    if (has("CoreMLExecutionProvider")) {
        try {
            std::unordered_map<std::string, std::string> coreml_opts;
            coreml_opts["MLComputeUnits"] = "All";
            opts.AppendExecutionProvider("CoreML", coreml_opts);
            static std::atomic<bool> logged_coreml{false};
            if (!logged_coreml.exchange(true)) {
                spdlog::info("[ONNX] CoreML execution provider enabled (Neural Engine + GPU)");
            } else {
                spdlog::debug("[ONNX] CoreML execution provider attached (pooled session)");
            }
            return;
        } catch (const std::exception& e) {
            spdlog::warn("[ONNX] CoreML provider failed: {}", e.what());
        }
    }
    if (has("MIGraphXExecutionProvider")) {
        try {
            OrtMIGraphXProviderOptions migraphx_opts{};
            migraphx_opts.device_id = 0;
            opts.AppendExecutionProvider_MIGraphX(migraphx_opts);
            static std::atomic<bool> logged_migraphx{false};
            if (!logged_migraphx.exchange(true)) {
                spdlog::info("[ONNX] MIGraphX execution provider enabled (AMD GPU via ROCm)");
            } else {
                spdlog::debug("[ONNX] MIGraphX execution provider attached (pooled session)");
            }
            return;
        } catch (const Ort::Exception& e) {
            spdlog::warn("[ONNX] MIGraphX provider failed: {}", e.what());
        }
    }
    if (has("CUDAExecutionProvider")) {
        try {
            OrtCUDAProviderOptions cuda_opts{};
            cuda_opts.device_id = 0;
            cuda_opts.arena_extend_strategy = 0;
            cuda_opts.gpu_mem_limit = SIZE_MAX;
            cuda_opts.cudnn_conv_algo_search = OrtCudnnConvAlgoSearchExhaustive;
            cuda_opts.do_copy_in_default_stream = 1;
            opts.AppendExecutionProvider_CUDA(cuda_opts);
            static std::atomic<bool> logged_cuda{false};
            if (!logged_cuda.exchange(true)) {
                spdlog::info("[ONNX] CUDA execution provider enabled (device_id=0)");
            } else {
                spdlog::debug("[ONNX] CUDA execution provider attached (pooled session)");
            }
            return;
        } catch (const Ort::Exception& e) {
            spdlog::warn("[ONNX] CUDA provider failed: {}", e.what());
        }
    }
    if (has("DmlExecutionProvider")) {
        try {
            opts.AppendExecutionProvider("DML");
            static std::atomic<bool> logged_dml{false};
            if (!logged_dml.exchange(true)) {
                spdlog::info("[ONNX] DirectML execution provider enabled");
            } else {
                spdlog::debug("[ONNX] DirectML execution provider attached (pooled session)");
            }
            return;
        } catch (const Ort::Exception& e) {
            spdlog::warn("[ONNX] DirectML provider failed: {}", e.what());
        }
    }

    spdlog::debug("[ONNX] No GPU execution provider available, using CPU");
}

} // namespace yams::onnx_util
