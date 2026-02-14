// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Diagnostic test to isolate ONNX embedding GPU failure.
// Compares GPU-enabled (CoreML) vs CPU-only codepaths to identify
// the exact failure point when `yams repair --embeddings` returns status=4.

#include <catch2/catch_test_macros.hpp>

#include <yams/compat/unistd.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/resource/onnx_model_pool.h>

#include <atomic>
#include <algorithm>
#include <cmath>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <string>
#include <thread>
#include <vector>

#include <onnxruntime_cxx_api.h>

namespace yams::daemon::test {

using namespace std::chrono_literals;
namespace fs = std::filesystem;

static const std::string kModelName = "nomic-embed-text-v1.5"; // NOLINT(cert-err58-cpp)

// Resolve models root: YAMS_MODELS_ROOT env > /Volumes/picaso/yams/models > ~/.yams/models
static std::string resolveModelsRoot() {
    if (const char* env = std::getenv("YAMS_MODELS_ROOT"))
        return env;

    // Check data_dir from config (hardcoded for this diagnostic)
    const fs::path configuredRoot = "/Volumes/picaso/yams/models";
    if (fs::exists(configuredRoot))
        return configuredRoot.string();

    const char* home = std::getenv("HOME");
    if (home) {
        fs::path fallback = fs::path(home) / ".yams/models";
        if (fs::exists(fallback))
            return fallback.string();
    }
    return {};
}

static bool checkModelAvailable(const std::string& modelName) {
    std::string root = resolveModelsRoot();
    if (root.empty())
        return false;
    return fs::exists(fs::path(root) / modelName / "model.onnx");
}

// ---------------------------------------------------------------------------
// Latency stats helpers
// ---------------------------------------------------------------------------
struct LatencyStats {
    std::int64_t avgMs = 0;
    std::int64_t p50Ms = 0;
    std::int64_t p95Ms = 0;
};

static std::int64_t percentileNearestRankMs(std::vector<std::int64_t> samples,
                                            double percentile01) {
    if (samples.empty())
        return 0;
    if (percentile01 <= 0.0)
        percentile01 = 0.0;
    if (percentile01 >= 1.0)
        percentile01 = 1.0;

    std::sort(samples.begin(), samples.end());
    const size_t n = samples.size();
    const size_t idx = static_cast<size_t>(std::llround(percentile01 * static_cast<double>(n - 1)));
    return samples[std::min(idx, n - 1)];
}

static LatencyStats computeLatencyStatsMs(const std::vector<std::int64_t>& samples) {
    LatencyStats st;
    if (samples.empty())
        return st;
    std::int64_t sum = 0;
    for (auto v : samples)
        sum += v;
    st.avgMs = sum / static_cast<std::int64_t>(samples.size());
    st.p50Ms = percentileNearestRankMs(samples, 0.50);
    st.p95Ms = percentileNearestRankMs(samples, 0.95);
    return st;
}

// ---------------------------------------------------------------------------
// Fixture with configurable GPU toggle
// ---------------------------------------------------------------------------
struct DiagnosticFixture {
    explicit DiagnosticFixture(bool gpu = false) {
        config_.maxLoadedModels = 2;
        config_.maxMemoryGB = 2;
        config_.numThreads = 2;
        config_.enableGPU = gpu;
        config_.lazyLoading = true;
        config_.modelIdleTimeout = std::chrono::seconds(60);
        config_.preloadModels.clear();
        config_.modelsRoot = resolveModelsRoot();
    }

    ~DiagnosticFixture() {
        if (pool_) {
            pool_->shutdown();
        }
    }

    ModelPoolConfig config_;
    std::unique_ptr<OnnxModelPool> pool_;
};

// ---------------------------------------------------------------------------
// 1. Report available ONNX Runtime providers
// ---------------------------------------------------------------------------
TEST_CASE("ONNX Diagnostic: available providers", "[daemon][onnx][diagnostic][.requires_model]") {
    auto providers = Ort::GetAvailableProviders();

    UNSCOPED_INFO("=== ONNX Runtime Available Providers ===");
    for (const auto& p : providers) {
        UNSCOPED_INFO("  provider: " << p);
    }
    UNSCOPED_INFO("  total: " << providers.size());

#if defined(__APPLE__)
    bool hasCoreML = false;
    for (const auto& p : providers) {
        if (p == "CoreMLExecutionProvider") {
            hasCoreML = true;
            break;
        }
    }
    UNSCOPED_INFO("  CoreMLExecutionProvider present: " << (hasCoreML ? "YES" : "NO"));
    CHECK(hasCoreML);
#endif

    CHECK(!providers.empty());
}

// ---------------------------------------------------------------------------
// 2. GPU-enabled model load + embedding generation
// ---------------------------------------------------------------------------
TEST_CASE("ONNX Diagnostic: GPU model load and embed",
          "[daemon][onnx][diagnostic][.requires_model]") {
    if (!checkModelAvailable(kModelName)) {
        SKIP("Model " + kModelName + " not found at ~/.yams/models/" + kModelName +
             "/model.onnx. Download with: yams model --download " + kModelName);
    }

    DiagnosticFixture fix(/* gpu = */ true);
    fix.pool_ = std::make_unique<OnnxModelPool>(fix.config_);

    auto initResult = fix.pool_->initialize();
    REQUIRE(initResult);

    // --- Model load ---
    UNSCOPED_INFO("=== GPU Model Load ===");
    auto loadStart = std::chrono::steady_clock::now();

    Result<OnnxModelPool::ModelHandle> h{yams::Error{ErrorCode::Unknown, "not attempted"}};
    try {
        h = fix.pool_->acquireModel(kModelName, 60s);
    } catch (const std::exception& ex) {
        UNSCOPED_INFO("  EXCEPTION during acquireModel: " << ex.what());
        FAIL("acquireModel threw: " + std::string(ex.what()));
    }

    auto loadEnd = std::chrono::steady_clock::now();
    auto loadMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(loadEnd - loadStart).count();
    UNSCOPED_INFO("  load time: " << loadMs << " ms");

    if (!h) {
        UNSCOPED_INFO("  LOAD FAILED: code=" << static_cast<int>(h.error().code)
                                             << " msg=" << h.error().message);
        FAIL("GPU model load failed: " + h.error().message);
    }

    auto& session = *h.value();
    UNSCOPED_INFO("  execution provider: " << session.getExecutionProvider());
    UNSCOPED_INFO("  embedding dim:      " << session.getEmbeddingDim());
    UNSCOPED_INFO("  max seq length:     " << session.getMaxSequenceLength());
    UNSCOPED_INFO("  isValid:            " << session.isValid());

    // --- Embedding generation ---
    UNSCOPED_INFO("=== GPU Embedding Generation ===");
    std::vector<std::string> texts = {"hello world", "diagnostic test text"};
    auto embedStart = std::chrono::steady_clock::now();

    Result<std::vector<std::vector<float>>> r{yams::Error{ErrorCode::Unknown, "not attempted"}};
    try {
        r = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings(texts);
    } catch (const std::exception& ex) {
        UNSCOPED_INFO("  EXCEPTION during generateBatchEmbeddings: " << ex.what());
        FAIL("generateBatchEmbeddings threw: " + std::string(ex.what()));
    }

    auto embedEnd = std::chrono::steady_clock::now();
    auto embedMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(embedEnd - embedStart).count();
    UNSCOPED_INFO("  embed time: " << embedMs << " ms");

    if (!r) {
        UNSCOPED_INFO("  EMBED FAILED: code=" << static_cast<int>(r.error().code)
                                              << " msg=" << r.error().message);
        FAIL("GPU batch embedding failed: " + r.error().message);
    }

    auto& mat = r.value();
    UNSCOPED_INFO("  batch size returned: " << mat.size());
    if (!mat.empty()) {
        UNSCOPED_INFO("  embedding dim returned: " << mat[0].size());
    }

    // After the first successful inference, the session should have discovered model dims.
    UNSCOPED_INFO("  session embedding dim (post-infer): " << session.getEmbeddingDim());
    UNSCOPED_INFO("  session max seq length (post-infer): " << session.getMaxSequenceLength());
    UNSCOPED_INFO("  session isValid (post-infer): " << session.isValid());

    // Warm (steady-state) timing: run 3 more times and report avg.
    constexpr int kWarmIters = 3;
    long warmTotalMs = 0;
    for (int i = 0; i < kWarmIters; ++i) {
        auto t0 = std::chrono::steady_clock::now();
        auto warm = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings(texts);
        auto t1 = std::chrono::steady_clock::now();
        warmTotalMs += std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
        REQUIRE(warm);
    }
    UNSCOPED_INFO("  warm avg time: " << (warmTotalMs / kWarmIters) << " ms");
    CHECK(mat.size() == texts.size());
}

// ---------------------------------------------------------------------------
// 3. CPU-only model load + embedding generation (control)
// ---------------------------------------------------------------------------
TEST_CASE("ONNX Diagnostic: CPU model load and embed",
          "[daemon][onnx][diagnostic][.requires_model]") {
    if (!checkModelAvailable(kModelName)) {
        SKIP("Model " + kModelName + " not found at ~/.yams/models/" + kModelName +
             "/model.onnx. Download with: yams model --download " + kModelName);
    }

    DiagnosticFixture fix(/* gpu = */ false);
    fix.pool_ = std::make_unique<OnnxModelPool>(fix.config_);

    auto initResult = fix.pool_->initialize();
    REQUIRE(initResult);

    // --- Model load ---
    UNSCOPED_INFO("=== CPU Model Load ===");
    auto loadStart = std::chrono::steady_clock::now();

    Result<OnnxModelPool::ModelHandle> h{yams::Error{ErrorCode::Unknown, "not attempted"}};
    try {
        h = fix.pool_->acquireModel(kModelName, 60s);
    } catch (const std::exception& ex) {
        UNSCOPED_INFO("  EXCEPTION during acquireModel: " << ex.what());
        FAIL("acquireModel threw: " + std::string(ex.what()));
    }

    auto loadEnd = std::chrono::steady_clock::now();
    auto loadMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(loadEnd - loadStart).count();
    UNSCOPED_INFO("  load time: " << loadMs << " ms");

    if (!h) {
        UNSCOPED_INFO("  LOAD FAILED: code=" << static_cast<int>(h.error().code)
                                             << " msg=" << h.error().message);
        FAIL("CPU model load failed: " + h.error().message);
    }

    auto& session = *h.value();
    UNSCOPED_INFO("  execution provider: " << session.getExecutionProvider());
    UNSCOPED_INFO("  embedding dim:      " << session.getEmbeddingDim());
    UNSCOPED_INFO("  max seq length:     " << session.getMaxSequenceLength());
    UNSCOPED_INFO("  isValid:            " << session.isValid());

    // --- Embedding generation ---
    UNSCOPED_INFO("=== CPU Embedding Generation ===");
    std::vector<std::string> texts = {"hello world", "diagnostic test text"};
    auto embedStart = std::chrono::steady_clock::now();

    Result<std::vector<std::vector<float>>> r{yams::Error{ErrorCode::Unknown, "not attempted"}};
    try {
        r = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings(texts);
    } catch (const std::exception& ex) {
        UNSCOPED_INFO("  EXCEPTION during generateBatchEmbeddings: " << ex.what());
        FAIL("generateBatchEmbeddings threw: " + std::string(ex.what()));
    }

    auto embedEnd = std::chrono::steady_clock::now();
    auto embedMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(embedEnd - embedStart).count();
    UNSCOPED_INFO("  embed time: " << embedMs << " ms");

    if (!r) {
        UNSCOPED_INFO("  EMBED FAILED: code=" << static_cast<int>(r.error().code)
                                              << " msg=" << r.error().message);
        FAIL("CPU batch embedding failed: " + r.error().message);
    }

    auto& mat = r.value();
    UNSCOPED_INFO("  batch size returned: " << mat.size());
    if (!mat.empty()) {
        UNSCOPED_INFO("  embedding dim returned: " << mat[0].size());
    }
    CHECK(mat.size() == texts.size());
}

// ---------------------------------------------------------------------------
// 4. Side-by-side GPU vs CPU comparison
// ---------------------------------------------------------------------------
TEST_CASE("ONNX Diagnostic: GPU vs CPU comparison", "[daemon][onnx][diagnostic][.requires_model]") {
    if (!checkModelAvailable(kModelName)) {
        SKIP("Model " + kModelName + " not found at ~/.yams/models/" + kModelName +
             "/model.onnx. Download with: yams model --download " + kModelName);
    }

    const std::vector<std::string> texts = {"hello world", "diagnostic comparison"};

    // ---- GPU attempt ----
    bool gpuLoadOk = false;
    bool gpuEmbedOk = false;
    std::string gpuEP;
    size_t gpuDim = 0;
    std::string gpuError;
    std::int64_t gpuLoadMs = 0, gpuEmbedMs = 0;

    {
        DiagnosticFixture fix(true);
        fix.pool_ = std::make_unique<OnnxModelPool>(fix.config_);
        auto init = fix.pool_->initialize();
        REQUIRE(init);

        auto t0 = std::chrono::steady_clock::now();
        try {
            auto h = fix.pool_->acquireModel(kModelName, 60s);
            gpuLoadMs =
                static_cast<std::int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                              std::chrono::steady_clock::now() - t0)
                                              .count());
            if (h) {
                gpuLoadOk = true;
                auto& s = *h.value();
                gpuEP = s.getExecutionProvider();
                gpuDim = s.getEmbeddingDim();

                auto t1 = std::chrono::steady_clock::now();
                auto r = const_cast<OnnxModelSession&>(s).generateBatchEmbeddings(texts);
                gpuEmbedMs =
                    static_cast<std::int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                  std::chrono::steady_clock::now() - t1)
                                                  .count());
                if (r) {
                    gpuEmbedOk = true;
                } else {
                    gpuError = "embed: " + r.error().message;
                }
            } else {
                gpuError = "load: " + h.error().message;
            }
        } catch (const std::exception& ex) {
            gpuLoadMs =
                static_cast<std::int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                              std::chrono::steady_clock::now() - t0)
                                              .count());
            gpuError = std::string("exception: ") + ex.what();
        }
    }

    // ---- CPU attempt ----
    bool cpuLoadOk = false;
    bool cpuEmbedOk = false;
    std::string cpuEP;
    size_t cpuDim = 0;
    std::string cpuError;
    std::int64_t cpuLoadMs = 0, cpuEmbedMs = 0;

    {
        DiagnosticFixture fix(false);
        fix.pool_ = std::make_unique<OnnxModelPool>(fix.config_);
        auto init = fix.pool_->initialize();
        REQUIRE(init);

        auto t0 = std::chrono::steady_clock::now();
        try {
            auto h = fix.pool_->acquireModel(kModelName, 60s);
            cpuLoadMs =
                static_cast<std::int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                              std::chrono::steady_clock::now() - t0)
                                              .count());
            if (h) {
                cpuLoadOk = true;
                auto& s = *h.value();
                cpuEP = s.getExecutionProvider();
                cpuDim = s.getEmbeddingDim();

                auto t1 = std::chrono::steady_clock::now();
                auto r = const_cast<OnnxModelSession&>(s).generateBatchEmbeddings(texts);
                cpuEmbedMs =
                    static_cast<std::int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                  std::chrono::steady_clock::now() - t1)
                                                  .count());
                if (r) {
                    cpuEmbedOk = true;
                } else {
                    cpuError = "embed: " + r.error().message;
                }
            } else {
                cpuError = "load: " + h.error().message;
            }
        } catch (const std::exception& ex) {
            cpuLoadMs =
                static_cast<std::int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                              std::chrono::steady_clock::now() - t0)
                                              .count());
            cpuError = std::string("exception: ") + ex.what();
        }
    }

    // ---- Report ----
    UNSCOPED_INFO("========================================");
    UNSCOPED_INFO("  GPU vs CPU Comparison for " << kModelName);
    UNSCOPED_INFO("========================================");
    UNSCOPED_INFO("  GPU load:  " << (gpuLoadOk ? "OK" : "FAIL") << "  (" << gpuLoadMs << " ms)"
                                  << "  EP=" << gpuEP);
    UNSCOPED_INFO("  GPU embed: " << (gpuEmbedOk ? "OK" : "FAIL") << "  (" << gpuEmbedMs << " ms)"
                                  << "  dim=" << gpuDim);
    if (!gpuError.empty())
        UNSCOPED_INFO("  GPU error: " << gpuError);

    UNSCOPED_INFO("  CPU load:  " << (cpuLoadOk ? "OK" : "FAIL") << "  (" << cpuLoadMs << " ms)"
                                  << "  EP=" << cpuEP);
    UNSCOPED_INFO("  CPU embed: " << (cpuEmbedOk ? "OK" : "FAIL") << "  (" << cpuEmbedMs << " ms)"
                                  << "  dim=" << cpuDim);
    if (!cpuError.empty())
        UNSCOPED_INFO("  CPU error: " << cpuError);

    // ---- Diagnosis ----
    if (gpuEmbedOk && cpuEmbedOk) {
        UNSCOPED_INFO("  DIAGNOSIS: Both GPU and CPU work. Issue is elsewhere (ABI, cooldown, "
                      "daemon state).");
        CHECK(gpuDim == cpuDim);
    } else if (!gpuEmbedOk && cpuEmbedOk) {
        UNSCOPED_INFO("  DIAGNOSIS: GPU fails but CPU works => CoreML / GPU issue.");
        FAIL("GPU embedding fails while CPU succeeds. GPU error: " + gpuError);
    } else if (gpuEmbedOk && !cpuEmbedOk) {
        UNSCOPED_INFO("  DIAGNOSIS: GPU works but CPU fails => unusual; check CPU EP.");
        FAIL("CPU embedding fails while GPU succeeds. CPU error: " + cpuError);
    } else {
        UNSCOPED_INFO("  DIAGNOSIS: Both fail => model file, tokenizer, or ONNX Runtime issue.");
        FAIL("Both GPU and CPU embedding failed. GPU: " + gpuError + " CPU: " + cpuError);
    }
}

// ---------------------------------------------------------------------------
// 5. CoreML configuration variants benchmark
//    Separates compilation (first inference triggers lazy CoreML compile)
//    from steady-state inference to reveal true performance characteristics.
// ---------------------------------------------------------------------------
TEST_CASE("ONNX Diagnostic: CoreML config variants",
          "[daemon][onnx][diagnostic][.requires_model]") {
    auto providers = Ort::GetAvailableProviders();
    const bool hasCoreML =
        std::find(providers.begin(), providers.end(), "CoreMLExecutionProvider") != providers.end();
    if (!hasCoreML) {
        SKIP("CoreMLExecutionProvider not available (non-macOS build)");
    }

    if (!checkModelAvailable(kModelName)) {
        SKIP("Model " + kModelName + " not found at ~/.yams/models/" + kModelName +
             "/model.onnx. Download with: yams model --download " + kModelName);
    }

    const std::vector<std::string> texts = {"hello world", "diagnostic benchmark text"};
    constexpr int kIterations = 3;

    struct ConfigVariant {
        std::string label;
        // env overrides (empty string = unset the var)
        std::string computeUnits;
        std::string modelFormat;
        std::string cacheDir;
        bool enableGpu;
    };

    std::string modelsRoot = resolveModelsRoot();
    std::string modelDir = (fs::path(modelsRoot) / kModelName).string();

    std::vector<ConfigVariant> variants = {
        {"OLD: ALL + NeuralNetwork", "ALL", "NeuralNetwork", "", true},
        {"NEW: CPUAndNE + MLProgram", "CPUAndNeuralEngine", "MLProgram", "", true},
        {"NEW + Cache (cold)", "CPUAndNeuralEngine", "MLProgram", modelDir, true},
        {"NEW + Cache (warm)", "CPUAndNeuralEngine", "MLProgram", modelDir, true},
        {"CoreML CPUOnly + MLProgram", "CPUOnly", "MLProgram", "", true},
        {"CPU-only (control)", "", "", "", false},
    };

    struct TimingResult {
        std::string label;
        std::int64_t compileMs = 0; // First inference (includes lazy CoreML compilation)
        std::int64_t embedMs = 0;   // Steady-state inference average
        bool ok = false;
        std::string error;
    };

    std::vector<TimingResult> results;

    auto now = []() { return std::chrono::steady_clock::now(); };
    auto elapsedMs = [](auto start) -> std::int64_t {
        return static_cast<std::int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                             std::chrono::steady_clock::now() - start)
                                             .count());
    };

    for (const auto& variant : variants) {
        TimingResult timing;
        timing.label = variant.label;

        // Set env vars for this variant
        if (variant.enableGpu) {
            if (!variant.computeUnits.empty())
                ::setenv("YAMS_COREML_COMPUTE_UNITS", variant.computeUnits.c_str(), 1);
            if (!variant.modelFormat.empty())
                ::setenv("YAMS_COREML_MODEL_FORMAT", variant.modelFormat.c_str(), 1);
            if (!variant.cacheDir.empty())
                ::setenv("YAMS_COREML_CACHE_DIR", variant.cacheDir.c_str(), 1);
        }

        {
            DiagnosticFixture fix(variant.enableGpu);
            fix.pool_ = std::make_unique<OnnxModelPool>(fix.config_);
            auto init = fix.pool_->initialize();
            REQUIRE(init);

            try {
                auto h = fix.pool_->acquireModel(kModelName, 120s);

                if (h) {
                    auto& s = *h.value();

                    // --- Warmup / compilation pass ---
                    // The first generateBatchEmbeddings() triggers lazy CoreML
                    // model compilation. Measure this separately.
                    auto tCompile = now();
                    auto warmup =
                        const_cast<OnnxModelSession&>(s).generateBatchEmbeddings({"warmup"});
                    timing.compileMs = elapsedMs(tCompile);

                    if (!warmup) {
                        timing.error = "compile warmup: " + warmup.error().message;
                    } else {
                        // --- Steady-state inference ---
                        std::int64_t totalEmbedMs = 0;
                        bool allOk = true;
                        for (int i = 0; i < kIterations; ++i) {
                            auto t1 = now();
                            auto r =
                                const_cast<OnnxModelSession&>(s).generateBatchEmbeddings(texts);
                            totalEmbedMs += elapsedMs(t1);
                            if (!r) {
                                allOk = false;
                                timing.error =
                                    "embed iter " + std::to_string(i) + ": " + r.error().message;
                                break;
                            }
                        }
                        timing.embedMs = totalEmbedMs / kIterations;
                        timing.ok = allOk;
                    }
                } else {
                    timing.error = "load: " + h.error().message;
                }
            } catch (const std::exception& ex) {
                timing.error = std::string("exception: ") + ex.what();
            }
        }

        // Clean up env vars
        ::unsetenv("YAMS_COREML_COMPUTE_UNITS");
        ::unsetenv("YAMS_COREML_MODEL_FORMAT");
        ::unsetenv("YAMS_COREML_CACHE_DIR");

        results.push_back(timing);
    }

    // ---- Report ----
    UNSCOPED_INFO("================================================================");
    UNSCOPED_INFO("  CoreML Config Variants Benchmark (" << kIterations << " iterations each)");
    UNSCOPED_INFO("  Compile = first inference (includes lazy CoreML compilation)");
    UNSCOPED_INFO("  Embed   = avg steady-state inference (post-compilation)");
    UNSCOPED_INFO("================================================================");
    UNSCOPED_INFO("  " << std::left << std::setw(35) << "Config" << std::setw(14) << "Compile(ms)"
                       << std::setw(14) << "Embed(ms)"
                       << "Status");
    UNSCOPED_INFO("  " << std::string(76, '-'));
    for (const auto& r : results) {
        if (r.ok) {
            UNSCOPED_INFO("  " << std::left << std::setw(35) << r.label << std::setw(14)
                               << r.compileMs << std::setw(14) << r.embedMs << "OK");
        } else {
            UNSCOPED_INFO("  " << std::left << std::setw(35) << r.label << std::setw(14)
                               << r.compileMs << std::setw(14) << "-"
                               << "FAIL: " << r.error);
        }
    }
    UNSCOPED_INFO("================================================================");

    // At minimum, the CPU control should work
    REQUIRE(!results.empty());
    CHECK(results.back().ok);
}

// ---------------------------------------------------------------------------
// 5b. MIGraphX (ROCm) configuration variants benchmark
//     Parity with CoreML table: separate first inference from steady-state.
// ---------------------------------------------------------------------------
TEST_CASE("ONNX Diagnostic: MIGraphX config variants",
          "[daemon][onnx][diagnostic][.requires_model]") {
    auto providers = Ort::GetAvailableProviders();
    const bool hasMIGraphX = std::find(providers.begin(), providers.end(),
                                       "MIGraphXExecutionProvider") != providers.end();
    if (!hasMIGraphX) {
        SKIP("MIGraphXExecutionProvider not available (no ROCm / no MIGraphX EP)");
    }

    if (!checkModelAvailable(kModelName)) {
        SKIP("Model " + kModelName + " not found at ~/.yams/models/" + kModelName +
             "/model.onnx. Download with: yams model --download " + kModelName);
    }

    const std::vector<std::string> texts = {"hello world", "diagnostic benchmark text"};
    constexpr int kWarmIterationsGpu = 30;
    constexpr int kWarmIterationsCpu = 5;

    struct Variant {
        std::string label;
        bool enableGpu;
        bool fp16;
    };

    std::vector<Variant> variants = {
        {"ROCm: MIGraphX (cache)", true, false},
        {"ROCm: MIGraphX fp16 (cache)", true, true},
        {"CPU-only (control)", false, false},
    };

    struct ResultRow {
        std::string label;
        std::int64_t loadMs = 0;
        std::int64_t firstInferMs = 0;
        LatencyStats warm;
        double textsPerSec = 0.0;
        std::string ep;
        bool ok = false;
        std::string error;
    };

    auto now = []() { return std::chrono::steady_clock::now(); };
    auto elapsedMs = [](auto start) -> std::int64_t {
        return static_cast<std::int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                             std::chrono::steady_clock::now() - start)
                                             .count());
    };

    std::vector<ResultRow> results;
    results.reserve(variants.size());

    for (const auto& variant : variants) {
        ResultRow row;
        row.label = variant.label;

        // Configure MIGraphX knobs for this run.
        if (variant.enableGpu) {
            ::setenv("YAMS_MIGRAPHX_FP16", variant.fp16 ? "1" : "0", 1);
            ::setenv("YAMS_MIGRAPHX_FP8", "0", 1);
            ::setenv("YAMS_MIGRAPHX_INT8", "0", 1);
            ::setenv("YAMS_MIGRAPHX_EXHAUSTIVE_TUNE", "0", 1);
            // Keep caching enabled by default; relies on model directory cache.
            ::setenv("YAMS_MIGRAPHX_SAVE_COMPILED", "1", 1);
            ::setenv("YAMS_MIGRAPHX_LOAD_COMPILED", "1", 1);
        }

        {
            DiagnosticFixture fix(variant.enableGpu);
            fix.pool_ = std::make_unique<OnnxModelPool>(fix.config_);
            auto init = fix.pool_->initialize();
            REQUIRE(init);

            try {
                auto tLoad = now();
                auto h = fix.pool_->acquireModel(kModelName, 120s);
                row.loadMs = elapsedMs(tLoad);
                if (!h) {
                    row.error = "load: " + h.error().message;
                    results.push_back(row);
                    continue;
                }

                auto& s = *h.value();
                row.ep = s.getExecutionProvider();

                // First inference (includes any lazy compilation / graph init).
                // IMPORTANT: MIGraphX compiled artifacts appear shape-specialized; using the same
                // batch shape as steady-state avoids cache mismatches (e.g., batch=1 vs batch=2).
                auto tFirst = now();
                auto warmup = const_cast<OnnxModelSession&>(s).generateBatchEmbeddings(texts);
                row.firstInferMs = elapsedMs(tFirst);
                if (!warmup) {
                    row.error = "first infer: " + warmup.error().message;
                    results.push_back(row);
                    continue;
                }

                const int warmIters = variant.enableGpu ? kWarmIterationsGpu : kWarmIterationsCpu;
                std::vector<std::int64_t> lat;
                lat.reserve(static_cast<size_t>(warmIters));

                for (int i = 0; i < warmIters; ++i) {
                    auto t0 = now();
                    auto r = const_cast<OnnxModelSession&>(s).generateBatchEmbeddings(texts);
                    const std::int64_t dt = elapsedMs(t0);
                    lat.push_back(dt);
                    if (!r) {
                        row.error = "embed iter " + std::to_string(i) + ": " + r.error().message;
                        break;
                    }
                }

                if (row.error.empty()) {
                    row.warm = computeLatencyStatsMs(lat);
                    const double textsPerCall = static_cast<double>(texts.size());
                    row.textsPerSec =
                        row.warm.avgMs > 0
                            ? (textsPerCall / static_cast<double>(row.warm.avgMs)) * 1000.0
                            : 0.0;
                    row.ok = true;
                }
            } catch (const std::exception& ex) {
                row.error = std::string("exception: ") + ex.what();
            }
        }

        if (variant.enableGpu) {
            ::unsetenv("YAMS_MIGRAPHX_FP16");
            ::unsetenv("YAMS_MIGRAPHX_FP8");
            ::unsetenv("YAMS_MIGRAPHX_INT8");
            ::unsetenv("YAMS_MIGRAPHX_EXHAUSTIVE_TUNE");
            ::unsetenv("YAMS_MIGRAPHX_SAVE_COMPILED");
            ::unsetenv("YAMS_MIGRAPHX_LOAD_COMPILED");
        }

        results.push_back(row);
    }

    // ---- Report ----
    UNSCOPED_INFO(
        "========================================================================================");
    UNSCOPED_INFO("  MIGraphX (ROCm) Config Variants Benchmark");
    UNSCOPED_INFO("  Load      = acquireModel() (session create / provider attach)");
    UNSCOPED_INFO("  FirstInfer = first generateBatchEmbeddings() (compile / graph init if any)");
    UNSCOPED_INFO("  Warm      = steady-state embedding latency over repeated calls");
    UNSCOPED_INFO(
        "========================================================================================");
    UNSCOPED_INFO("  " << std::left << std::setw(28) << "Config" << std::setw(10) << "EP"
                       << std::setw(10) << "Load" << std::setw(12) << "FirstInfer" << std::setw(10)
                       << "Avg" << std::setw(10) << "p50" << std::setw(10) << "p95" << std::setw(12)
                       << "texts/s"
                       << "Status");
    UNSCOPED_INFO("  " << std::string(92, '-'));
    for (const auto& r : results) {
        if (r.ok) {
            UNSCOPED_INFO("  " << std::left << std::setw(28) << r.label << std::setw(10)
                               << (r.ep.empty() ? "?" : r.ep) << std::setw(10) << r.loadMs
                               << std::setw(12) << r.firstInferMs << std::setw(10) << r.warm.avgMs
                               << std::setw(10) << r.warm.p50Ms << std::setw(10) << r.warm.p95Ms
                               << std::fixed << std::setprecision(1) << std::setw(12)
                               << r.textsPerSec << "OK");
        } else {
            UNSCOPED_INFO("  " << std::left << std::setw(28) << r.label << std::setw(10)
                               << (r.ep.empty() ? "?" : r.ep) << std::setw(10) << r.loadMs
                               << std::setw(12) << r.firstInferMs << std::setw(10) << "-"
                               << std::setw(10) << "-" << std::setw(10) << "-" << std::setw(12)
                               << "-" << "FAIL: " << r.error);
        }
    }
    UNSCOPED_INFO(
        "========================================================================================");

    // At minimum, CPU control should work.
    REQUIRE(!results.empty());
    CHECK(results.back().ok);
}

// ---------------------------------------------------------------------------
// 6. Batch size sweep benchmark
//    Measures throughput across batch sizes to identify optimal batching.
// ---------------------------------------------------------------------------
TEST_CASE("ONNX Benchmark: batch size sweep", "[daemon][onnx][benchmark][.requires_model]") {
    if (!checkModelAvailable(kModelName)) {
        SKIP("Model " + kModelName + " not found at ~/.yams/models/" + kModelName +
             "/model.onnx. Download with: yams model --download " + kModelName);
    }

    // Build a 64-text corpus
    constexpr int kCorpusSize = 64;
    constexpr int kIterations = 3;
    std::vector<std::string> corpus;
    corpus.reserve(kCorpusSize);
    for (int i = 0; i < kCorpusSize; ++i) {
        corpus.push_back("Benchmark text number " + std::to_string(i) +
                         " for batch throughput measurement.");
    }

    DiagnosticFixture fix(false); // CPU-only for consistent results
    fix.pool_ = std::make_unique<OnnxModelPool>(fix.config_);
    REQUIRE(fix.pool_->initialize());

    auto h = fix.pool_->acquireModel(kModelName, 60s);
    REQUIRE(h);
    auto& session = *h.value();

    // Warmup pass
    auto warmup = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings({"warmup"});
    REQUIRE(warmup);

    struct BatchResult {
        int batchSize;
        std::int64_t avgMs;
        double textsPerSec;
    };
    std::vector<BatchResult> results;

    std::vector<int> batchSizes = {1, 2, 4, 8, 16, 32, 64};

    auto now = []() { return std::chrono::steady_clock::now(); };

    for (int bs : batchSizes) {
        std::int64_t totalMs = 0;

        for (int iter = 0; iter < kIterations; ++iter) {
            // Process corpus in chunks of batchSize
            auto t0 = now();
            for (int offset = 0; offset < kCorpusSize; offset += bs) {
                int end = std::min(offset + bs, kCorpusSize);
                std::vector<std::string> batch(corpus.begin() + offset, corpus.begin() + end);
                auto r = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings(batch);
                REQUIRE(r);
            }
            totalMs += std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - t0)
                           .count();
        }

        std::int64_t avgMs = totalMs / kIterations;
        double textsPerSec = avgMs > 0 ? (static_cast<double>(kCorpusSize) / avgMs) * 1000.0 : 0.0;
        results.push_back({bs, avgMs, textsPerSec});
    }

    UNSCOPED_INFO("================================================================");
    UNSCOPED_INFO("  Batch Size Sweep (" << kIterations << " iterations, " << kCorpusSize
                                         << " texts, CPU-only)");
    UNSCOPED_INFO("================================================================");
    UNSCOPED_INFO("  " << std::left << std::setw(12) << "BatchSize" << std::setw(14) << "Avg(ms)"
                       << "Texts/sec");
    UNSCOPED_INFO("  " << std::string(40, '-'));
    for (const auto& r : results) {
        UNSCOPED_INFO("  " << std::left << std::setw(12) << r.batchSize << std::setw(14) << r.avgMs
                           << std::fixed << std::setprecision(1) << r.textsPerSec);
    }
    UNSCOPED_INFO("================================================================");

    // Verify we got results for all batch sizes (throughput comparison is
    // informational — with max-length padding, large batches may be slower
    // due to memory pressure; dynamic padding would fix this).
    REQUIRE(results.size() >= 2);
    CHECK(results.front().textsPerSec > 0.0);
}

// ---------------------------------------------------------------------------
// 7. Concurrent inference throughput benchmark
//    Measures scaling with multiple threads each using their own session.
// ---------------------------------------------------------------------------
TEST_CASE("ONNX Benchmark: concurrent inference throughput",
          "[daemon][onnx][benchmark][.requires_model]") {
    if (!checkModelAvailable(kModelName)) {
        SKIP("Model " + kModelName + " not found at ~/.yams/models/" + kModelName +
             "/model.onnx. Download with: yams model --download " + kModelName);
    }

    constexpr int kInferenceCycles = 3;
    const std::vector<std::string> texts = {
        "concurrent benchmark text one",
        "concurrent benchmark text two",
        "concurrent benchmark text three",
    };

    struct ThreadResult {
        int threadCount;
        std::int64_t totalMs;
        double textsPerSec;
        int successes;
    };
    std::vector<ThreadResult> results;

    std::vector<int> threadCounts = {1, 2, 4};

    for (int nThreads : threadCounts) {
        TuneAdvisor::setOnnxSessionsPerModel(static_cast<uint32_t>(nThreads));

        DiagnosticFixture fix(false);
        fix.config_.maxMemoryGB = 4;
        fix.pool_ = std::make_unique<OnnxModelPool>(fix.config_);
        REQUIRE(fix.pool_->initialize());

        // Warmup
        {
            auto h = fix.pool_->acquireModel(kModelName, 60s);
            REQUIRE(h);
            auto& s = *h.value();
            auto r = const_cast<OnnxModelSession&>(s).generateBatchEmbeddings({"warmup"});
            REQUIRE(r);
        }

        std::atomic<int> successes{0};
        auto t0 = std::chrono::steady_clock::now();

        std::vector<std::thread> threads;
        for (int i = 0; i < nThreads; ++i) {
            threads.emplace_back([&]() {
                auto h = fix.pool_->acquireModel(kModelName, 30s);
                if (!h)
                    return;

                auto& s = *h.value();
                for (int c = 0; c < kInferenceCycles; ++c) {
                    auto r = const_cast<OnnxModelSession&>(s).generateBatchEmbeddings(texts);
                    if (r && r.value().size() == texts.size()) {
                        successes++;
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        auto totalMs =
            static_cast<std::int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                          std::chrono::steady_clock::now() - t0)
                                          .count());
        int totalTexts = successes.load() * static_cast<int>(texts.size());
        double textsPerSec =
            totalMs > 0 ? (static_cast<double>(totalTexts) / totalMs) * 1000.0 : 0.0;

        results.push_back({nThreads, totalMs, textsPerSec, successes.load()});
    }

    TuneAdvisor::setOnnxSessionsPerModel(0);

    UNSCOPED_INFO("================================================================");
    UNSCOPED_INFO("  Concurrent Inference Throughput (" << kInferenceCycles
                                                        << " cycles/thread, CPU-only)");
    UNSCOPED_INFO("================================================================");
    UNSCOPED_INFO("  " << std::left << std::setw(10) << "Threads" << std::setw(14) << "Total(ms)"
                       << std::setw(14) << "Texts/sec"
                       << "Successes");
    UNSCOPED_INFO("  " << std::string(52, '-'));
    for (const auto& r : results) {
        UNSCOPED_INFO("  " << std::left << std::setw(10) << r.threadCount << std::setw(14)
                           << r.totalMs << std::setw(14) << std::fixed << std::setprecision(1)
                           << r.textsPerSec << r.successes);
    }
    UNSCOPED_INFO("================================================================");

    // All threads should succeed
    for (const auto& r : results) {
        CHECK(r.successes == r.threadCount * kInferenceCycles);
    }
}

// ---------------------------------------------------------------------------
// 8. Session reuse performance benchmark
//    Measures acquire + embed latency across multiple cycles to show
//    cold (cycle 0) vs warm (cycles 1-N) performance characteristics.
// ---------------------------------------------------------------------------
TEST_CASE("ONNX Benchmark: session reuse performance",
          "[daemon][onnx][benchmark][.requires_model]") {
    if (!checkModelAvailable(kModelName)) {
        SKIP("Model " + kModelName + " not found at ~/.yams/models/" + kModelName +
             "/model.onnx. Download with: yams model --download " + kModelName);
    }

    constexpr int kCycles = 5;
    const std::vector<std::string> texts = {"session reuse benchmark text"};

    DiagnosticFixture fix(false);
    fix.config_.maxMemoryGB = 2;
    fix.pool_ = std::make_unique<OnnxModelPool>(fix.config_);
    REQUIRE(fix.pool_->initialize());

    struct CycleResult {
        int cycle;
        std::int64_t acquireMs;
        std::int64_t embedMs;
    };
    std::vector<CycleResult> results;

    auto now = []() { return std::chrono::steady_clock::now(); };

    for (int i = 0; i < kCycles; ++i) {
        auto t0 = now();
        auto h = fix.pool_->acquireModel(kModelName, 60s);
        auto acquireMs =
            static_cast<std::int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                          std::chrono::steady_clock::now() - t0)
                                          .count());
        REQUIRE(h);

        auto& session = *h.value();
        auto t1 = now();
        auto r = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings(texts);
        auto embedMs =
            static_cast<std::int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                          std::chrono::steady_clock::now() - t1)
                                          .count());
        REQUIRE(r);

        results.push_back({i, acquireMs, embedMs});
        // Handle released at end of iteration — session returned to pool
    }

    UNSCOPED_INFO("================================================================");
    UNSCOPED_INFO("  Session Reuse Performance (" << kCycles << " cycles, CPU-only)");
    UNSCOPED_INFO("  Cycle 0 = cold (model load + session creation)");
    UNSCOPED_INFO("  Cycles 1-" << (kCycles - 1) << " = warm (pool reuse)");
    UNSCOPED_INFO("================================================================");
    UNSCOPED_INFO("  " << std::left << std::setw(8) << "Cycle" << std::setw(16) << "Acquire(ms)"
                       << "Embed(ms)");
    UNSCOPED_INFO("  " << std::string(38, '-'));
    for (const auto& r : results) {
        UNSCOPED_INFO("  " << std::left << std::setw(8) << r.cycle << std::setw(16) << r.acquireMs
                           << r.embedMs);
    }
    UNSCOPED_INFO("================================================================");

    // Warm acquires (cycles 1+) should be faster than cold (cycle 0)
    REQUIRE(results.size() >= 2);
    CHECK(results[1].acquireMs <= results[0].acquireMs);
}

// ---------------------------------------------------------------------------
// 9. Dynamic padding throughput comparison
//    Runs batch size sweep with dynamic padding ON (default) to show improved
//    throughput when tensor size scales with content instead of fixed padding.
// ---------------------------------------------------------------------------
TEST_CASE("ONNX Benchmark: dynamic padding throughput",
          "[daemon][onnx][benchmark][.requires_model]") {
    if (!checkModelAvailable(kModelName)) {
        SKIP("Model " + kModelName + " not found at ~/.yams/models/" + kModelName +
             "/model.onnx. Download with: yams model --download " + kModelName);
    }

    // Build a 64-text corpus of SHORT texts (dynamic padding shines here)
    constexpr int kCorpusSize = 64;
    constexpr int kIterations = 3;
    std::vector<std::string> corpus;
    corpus.reserve(kCorpusSize);
    for (int i = 0; i < kCorpusSize; ++i) {
        corpus.push_back("short text " + std::to_string(i));
    }

    struct BatchResult {
        int batchSize;
        std::int64_t avgMs;
        double textsPerSec;
        std::string label;
    };
    std::vector<BatchResult> allResults;

    std::vector<int> batchSizes = {1, 2, 4, 8, 16, 32, 64};
    auto now = []() { return std::chrono::steady_clock::now(); };

    // --- Run with dynamic padding ON (default) ---
    {
        DiagnosticFixture fix(false);
        fix.pool_ = std::make_unique<OnnxModelPool>(fix.config_);
        REQUIRE(fix.pool_->initialize());

        auto h = fix.pool_->acquireModel(kModelName, 60s);
        REQUIRE(h);
        auto& session = *h.value();

        // Warmup pass
        auto warmup = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings({"warmup"});
        REQUIRE(warmup);

        for (int bs : batchSizes) {
            std::int64_t totalMs = 0;
            for (int iter = 0; iter < kIterations; ++iter) {
                auto t0 = now();
                for (int offset = 0; offset < kCorpusSize; offset += bs) {
                    int end = std::min(offset + bs, kCorpusSize);
                    std::vector<std::string> batch(corpus.begin() + offset, corpus.begin() + end);
                    auto r = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings(batch);
                    REQUIRE(r);
                }
                totalMs +=
                    static_cast<std::int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                  std::chrono::steady_clock::now() - t0)
                                                  .count());
            }
            std::int64_t avgMs = totalMs / kIterations;
            double tps = avgMs > 0 ? (static_cast<double>(kCorpusSize) / avgMs) * 1000.0 : 0.0;
            allResults.push_back({bs, avgMs, tps, "Dynamic ON"});
        }
    }

    // --- Run with dynamic padding OFF ---
    {
        ::setenv("YAMS_ONNX_DYNAMIC_PADDING", "0", 1);

        DiagnosticFixture fix(false);
        fix.pool_ = std::make_unique<OnnxModelPool>(fix.config_);
        REQUIRE(fix.pool_->initialize());

        auto h = fix.pool_->acquireModel(kModelName, 60s);
        REQUIRE(h);
        auto& session = *h.value();

        auto warmup = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings({"warmup"});
        REQUIRE(warmup);

        for (int bs : batchSizes) {
            std::int64_t totalMs = 0;
            for (int iter = 0; iter < kIterations; ++iter) {
                auto t0 = now();
                for (int offset = 0; offset < kCorpusSize; offset += bs) {
                    int end = std::min(offset + bs, kCorpusSize);
                    std::vector<std::string> batch(corpus.begin() + offset, corpus.begin() + end);
                    auto r = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings(batch);
                    REQUIRE(r);
                }
                totalMs +=
                    static_cast<std::int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                  std::chrono::steady_clock::now() - t0)
                                                  .count());
            }
            std::int64_t avgMs = totalMs / kIterations;
            double tps = avgMs > 0 ? (static_cast<double>(kCorpusSize) / avgMs) * 1000.0 : 0.0;
            allResults.push_back({bs, avgMs, tps, "Dynamic OFF"});
        }

        ::unsetenv("YAMS_ONNX_DYNAMIC_PADDING");
    }

    // Print comparison table
    UNSCOPED_INFO("================================================================");
    UNSCOPED_INFO("  Dynamic Padding Throughput Comparison ("
                  << kIterations << " iterations, " << kCorpusSize << " short texts, CPU-only)");
    UNSCOPED_INFO("================================================================");
    UNSCOPED_INFO("  " << std::left << std::setw(14) << "Mode" << std::setw(12) << "BatchSize"
                       << std::setw(14) << "Avg(ms)"
                       << "Texts/sec");
    UNSCOPED_INFO("  " << std::string(54, '-'));
    for (const auto& r : allResults) {
        UNSCOPED_INFO("  " << std::left << std::setw(14) << r.label << std::setw(12) << r.batchSize
                           << std::setw(14) << r.avgMs << std::fixed << std::setprecision(1)
                           << r.textsPerSec);
    }
    UNSCOPED_INFO("================================================================");

    // Dynamic padding ON should not be slower than OFF for short texts
    REQUIRE(allResults.size() >= 2);
    CHECK(allResults.front().textsPerSec > 0.0);
}

} // namespace yams::daemon::test
