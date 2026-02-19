// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <string>
#include <vector>

#include <onnxruntime_cxx_api.h>

#if defined(__APPLE__)

#include "plugins/onnx/onnx_gpu_provider.h"

TEST_CASE("CoreMLExecutionProvider is available in linked ONNX Runtime",
          "[daemon][gpu][onnx][coreml][catch2]") {
    auto providers = Ort::GetAvailableProviders();

    // Log all available providers for diagnostics
    INFO("Available ONNX Runtime providers:");
    for (const auto& p : providers) {
        INFO("  - " << p);
    }

    bool hasCoreML =
        std::find(providers.begin(), providers.end(), "CoreMLExecutionProvider") != providers.end();
    CHECK(hasCoreML);
}

TEST_CASE("appendGpuProvider attaches CoreML on macOS", "[daemon][gpu][onnx][coreml][catch2]") {
    Ort::SessionOptions opts;
    // Test with default (no cache dir)
    REQUIRE_NOTHROW((void)yams::onnx_util::appendGpuProvider(opts));
}

TEST_CASE("appendGpuProvider accepts optional cache directory",
          "[daemon][gpu][onnx][coreml][catch2]") {
    Ort::SessionOptions opts;
    REQUIRE_NOTHROW((void)yams::onnx_util::appendGpuProvider(opts, "/tmp"));
}

#if defined(__aarch64__) || defined(__arm64__)

#include <yams/daemon/resource/gpu_info.h>

TEST_CASE("GPU detection and ONNX provider are consistent on Apple Silicon",
          "[daemon][gpu][onnx][coreml][catch2]") {
    const auto& gpu = yams::daemon::resource::detectGpu();
    CHECK(gpu.detected);
    CHECK(gpu.provider == "coreml");
    CHECK(gpu.unifiedMemory);

    auto providers = Ort::GetAvailableProviders();
    bool hasCoreML =
        std::find(providers.begin(), providers.end(), "CoreMLExecutionProvider") != providers.end();
    CHECK(hasCoreML);
}

#endif // __aarch64__ || __arm64__

#endif // __APPLE__
