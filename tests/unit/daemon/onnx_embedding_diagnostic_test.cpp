// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Diagnostic test to isolate ONNX embedding GPU failure.
// Compares GPU-enabled (CoreML) vs CPU-only codepaths to identify
// the exact failure point when `yams repair --embeddings` returns status=4.

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/resource/onnx_model_pool.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <string>
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
    long gpuLoadMs = 0, gpuEmbedMs = 0;

    {
        DiagnosticFixture fix(true);
        fix.pool_ = std::make_unique<OnnxModelPool>(fix.config_);
        auto init = fix.pool_->initialize();
        REQUIRE(init);

        auto t0 = std::chrono::steady_clock::now();
        try {
            auto h = fix.pool_->acquireModel(kModelName, 60s);
            gpuLoadMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
            if (h) {
                gpuLoadOk = true;
                auto& s = *h.value();
                gpuEP = s.getExecutionProvider();
                gpuDim = s.getEmbeddingDim();

                auto t1 = std::chrono::steady_clock::now();
                auto r = const_cast<OnnxModelSession&>(s).generateBatchEmbeddings(texts);
                gpuEmbedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - t1)
                                 .count();
                if (r) {
                    gpuEmbedOk = true;
                } else {
                    gpuError = "embed: " + r.error().message;
                }
            } else {
                gpuError = "load: " + h.error().message;
            }
        } catch (const std::exception& ex) {
            gpuLoadMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
            gpuError = std::string("exception: ") + ex.what();
        }
    }

    // ---- CPU attempt ----
    bool cpuLoadOk = false;
    bool cpuEmbedOk = false;
    std::string cpuEP;
    size_t cpuDim = 0;
    std::string cpuError;
    long cpuLoadMs = 0, cpuEmbedMs = 0;

    {
        DiagnosticFixture fix(false);
        fix.pool_ = std::make_unique<OnnxModelPool>(fix.config_);
        auto init = fix.pool_->initialize();
        REQUIRE(init);

        auto t0 = std::chrono::steady_clock::now();
        try {
            auto h = fix.pool_->acquireModel(kModelName, 60s);
            cpuLoadMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
            if (h) {
                cpuLoadOk = true;
                auto& s = *h.value();
                cpuEP = s.getExecutionProvider();
                cpuDim = s.getEmbeddingDim();

                auto t1 = std::chrono::steady_clock::now();
                auto r = const_cast<OnnxModelSession&>(s).generateBatchEmbeddings(texts);
                cpuEmbedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - t1)
                                 .count();
                if (r) {
                    cpuEmbedOk = true;
                } else {
                    cpuError = "embed: " + r.error().message;
                }
            } else {
                cpuError = "load: " + h.error().message;
            }
        } catch (const std::exception& ex) {
            cpuLoadMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
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
        long compileMs = 0; // First inference (includes lazy CoreML compilation)
        long embedMs = 0;   // Steady-state inference average
        bool ok = false;
        std::string error;
    };

    std::vector<TimingResult> results;

    auto now = []() { return std::chrono::steady_clock::now(); };
    auto elapsedMs = [](auto start) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - start)
            .count();
    };

    for (const auto& variant : variants) {
        TimingResult timing;
        timing.label = variant.label;

        // Set env vars for this variant
        if (variant.enableGpu) {
            if (!variant.computeUnits.empty())
                setenv("YAMS_COREML_COMPUTE_UNITS", variant.computeUnits.c_str(), 1);
            if (!variant.modelFormat.empty())
                setenv("YAMS_COREML_MODEL_FORMAT", variant.modelFormat.c_str(), 1);
            if (!variant.cacheDir.empty())
                setenv("YAMS_COREML_CACHE_DIR", variant.cacheDir.c_str(), 1);
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
                        long totalEmbedMs = 0;
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
        unsetenv("YAMS_COREML_COMPUTE_UNITS");
        unsetenv("YAMS_COREML_MODEL_FORMAT");
        unsetenv("YAMS_COREML_CACHE_DIR");

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

} // namespace yams::daemon::test
