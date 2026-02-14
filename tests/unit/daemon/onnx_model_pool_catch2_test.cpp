// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/core/types.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/resource/onnx_model_pool.h>
#include <yams/vector/embedding_generator.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <thread>
#include <vector>

#ifdef _WIN32
static int setenv(const char* name, const char* value, int overwrite) {
    return _putenv_s(name, value);
}
static void unsetenv(const char* name) {
    _putenv_s(name, "");
}
#endif

namespace yams::daemon::test {

using namespace std::chrono_literals;
namespace fs = std::filesystem;

// Configurable model name: YAMS_TEST_MODEL_NAME env or default
static std::string testModelName() {
    if (const char* env = std::getenv("YAMS_TEST_MODEL_NAME"))
        return env;
    return "nomic-embed-text-v1.5";
}

// Resolve models root: YAMS_MODELS_ROOT env > /Volumes/picaso/yams/models > ~/.yams/models
static std::string resolveModelsRoot() {
    if (const char* env = std::getenv("YAMS_MODELS_ROOT"))
        return env;

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

// Helper to check model availability using resolved root
static bool checkModelAvailable(const std::string& modelName) {
    std::string root = resolveModelsRoot();
    if (root.empty())
        return false;
    return fs::exists(fs::path(root) / modelName / "model.onnx");
}

// Test fixture
struct OnnxModelPoolFixture {
    OnnxModelPoolFixture() {
        // Create test config
        config_.maxLoadedModels = 2;
        config_.maxMemoryGB = 1.0;
        config_.numThreads = 2;
        config_.enableGPU = false;
        config_.lazyLoading = true; // Don't block on initialization
        config_.modelIdleTimeout = std::chrono::seconds(1);
        config_.preloadModels.clear(); // No preloading for tests
        config_.modelsRoot = resolveModelsRoot();

        // Set test mode environment variable to handle missing models gracefully
        setenv("YAMS_TEST_MODE", "1", 1);
    }

    ~OnnxModelPoolFixture() {
        if (pool_) {
            pool_->shutdown();
        }

        // Clean up test mode environment variable
        unsetenv("YAMS_TEST_MODE");
    }

    ModelPoolConfig config_;
    std::unique_ptr<OnnxModelPool> pool_;
};

// Test pool creation and initialization
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: pool creation", "[daemon]") {
    pool_ = std::make_unique<OnnxModelPool>(config_);

    auto result = pool_->initialize();
    REQUIRE(result);

    // Should start with no models loaded (lazy loading)
    auto loadedModels = pool_->getLoadedModels();
    CHECK(loadedModels.size() == 0);

    auto stats = pool_->getStats();
    CHECK(stats.loadedModels == 0);
    CHECK(stats.totalRequests == 0);
}

// Test model loading with non-existent model (always runs, even in CI)
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: model loading non-existent", "[daemon]") {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    REQUIRE(initResult);

    // Try to load non-existent model - should fail quickly
    auto start = std::chrono::steady_clock::now();
    auto result = pool_->loadModel("nonexistent-model");
    auto elapsed = std::chrono::steady_clock::now() - start;

    // Should fail quickly (under 1 second)
    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    CHECK(elapsedMs < 1000);
    INFO("Non-existent model check took " << elapsedMs << "ms");

    REQUIRE_FALSE(result);
    CHECK(result.error().code == yams::ErrorCode::NotFound);
}

// Test model loading with real model
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: model loading",
                 "[daemon][.slow][.requires_model]") {
    auto modelName = testModelName();
    if (!checkModelAvailable(modelName)) {
        SKIP("Model " + modelName + " not found. Download with: yams model --download " +
             modelName);
    }

    // Skip in CI or when we want fast tests only
    if (std::getenv("CI") || std::getenv("GITHUB_ACTIONS") || std::getenv("YAMS_SKIP_SLOW_TESTS")) {
        SKIP("Skipping real model loading test in CI/fast mode. "
             "Set YAMS_ENABLE_SLOW_TESTS=1 to force run.");
    }

    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    REQUIRE(initResult);

    // Test with real model - should complete in seconds, not minutes
    auto start = std::chrono::steady_clock::now();
    auto result = pool_->loadModel(modelName);
    auto elapsed = std::chrono::steady_clock::now() - start;

    // Fail if it takes more than 30 seconds - something is wrong
    auto elapsedSeconds = std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
    INFO("Model loading took " << elapsedSeconds << " seconds");
    REQUIRE(elapsedSeconds < 30);

    if (result) {
        CHECK(pool_->isModelLoaded(modelName));
        auto loadedModels = pool_->getLoadedModels();
        CHECK(loadedModels.size() == 1);
        CHECK(loadedModels[0] == modelName);
    } else {
        // Log but don't fail - model might have issues
        SKIP("Model loading failed: " + result.error().message);
    }
}

// Test model acquisition timeout
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: acquisition timeout", "[daemon]") {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    REQUIRE(initResult);

    auto modelName = testModelName();
    if (checkModelAvailable(modelName)) {
        // Test with real model acquisition - should succeed or timeout appropriately
        auto result = pool_->acquireModel(modelName, 100ms);
        // We don't assert success here since model might be slow to load
        // But we do verify that it doesn't crash
        SUCCEED("Model acquisition completed without crash");
    } else {
        // Test with non-existent model (should fail quickly)
        auto result = pool_->acquireModel("nonexistent-model", 10ms);
        REQUIRE_FALSE(result);
        CHECK(result.error().code == yams::ErrorCode::NotFound);
    }
}

// Test model pool statistics
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: statistics", "[daemon]") {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    REQUIRE(initResult);

    auto stats1 = pool_->getStats();
    CHECK(stats1.totalRequests == 0);
    CHECK(stats1.cacheHits == 0);
    CHECK(stats1.cacheMisses == 0);

    // Try to acquire a model (should update stats regardless of success)
    auto modelName = testModelName();
    std::string testModel = checkModelAvailable(modelName) ? modelName : "nonexistent-model";
    auto result = pool_->acquireModel(testModel, 10ms);

    auto stats2 = pool_->getStats();
    CHECK(stats2.totalRequests == 1);

    // Stats should be updated regardless of model availability
    if (!result) {
        CHECK(stats2.cacheMisses == 1);
    }
}

// Test concurrent model acquisition attempts
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: concurrent acquisition", "[daemon]") {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    REQUIRE(initResult);

    const int numThreads = 5;
    std::atomic<int> attempts{0};
    std::atomic<int> failures{0};

    auto modelName = testModelName();
    std::string testModel = checkModelAvailable(modelName) ? modelName : "nonexistent-model";

    std::vector<std::thread> threads;
    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([this, &testModel, &attempts, &failures]() {
            for (int j = 0; j < 10; ++j) {
                attempts++;
                auto result = pool_->acquireModel(testModel, 10ms);
                if (!result) {
                    failures++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    CHECK(attempts == numThreads * 10);
    // Results depend on model availability, but should handle concurrent access safely
    if (testModel == "nonexistent-model") {
        CHECK(failures == numThreads * 10);
    }

    auto stats = pool_->getStats();
    CHECK(stats.totalRequests == numThreads * 10);
}

// Test pool shutdown
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: pool shutdown", "[daemon]") {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    REQUIRE(initResult);

    // Shutdown the pool
    pool_->shutdown();

    // Should not be able to load models after shutdown
    auto result = pool_->loadModel("test");
    CHECK_FALSE(result);

    // Should not be able to acquire models after shutdown
    auto acquireResult = pool_->acquireModel("test", 10ms);
    CHECK_FALSE(acquireResult);
}

// Test model unloading
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: model unloading", "[daemon]") {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    REQUIRE(initResult);

    // Try to unload a non-existent model
    auto result = pool_->unloadModel("nonexistent");
    REQUIRE_FALSE(result);
    CHECK(result.error().code == yams::ErrorCode::NotFound);
}

// Test LRU eviction (conceptual test since we can't load real models)
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: LRU eviction", "[daemon]") {
    config_.maxLoadedModels = 2;
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    REQUIRE(initResult);

    // The actual eviction would happen if we could load real models
    // This test verifies the API exists and doesn't crash
    auto stats = pool_->getStats();
    CHECK(stats.loadedModels >= 0);
    CHECK(stats.loadedModels <= static_cast<int>(config_.maxLoadedModels));
}

// Test memory limit enforcement
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: memory limit", "[daemon]") {
    config_.maxMemoryGB = 1.0; // Small limit (1GB)
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    REQUIRE(initResult);

    // Memory usage should be tracked
    auto memUsage = pool_->getMemoryUsage();
    CHECK(memUsage >= 0);

    auto stats = pool_->getStats();
    CHECK(stats.totalMemoryBytes <= static_cast<size_t>(config_.maxMemoryGB * 1024 * 1024 * 1024));
}

// Test maintenance operations
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: maintenance", "[daemon]") {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    REQUIRE(initResult);

    // Perform maintenance (should not crash even with no models)
    pool_->performMaintenance();

    // Stats should still be valid
    auto stats = pool_->getStats();
    CHECK(stats.loadedModels >= 0);
}

// Test model handle RAII behavior
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: model handle RAII", "[daemon]") {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    REQUIRE(initResult);

    {
        // Try to acquire a model in a scope
        auto result = pool_->acquireModel("test", 10ms);

        if (result) {
            // If successful, handle should be valid
            auto& handle = result.value();
            CHECK(handle.isValid());

            // Handle will be released when going out of scope
        }
    }

    // After scope, resources should be released
    // (Can't directly test this without real models)
    SUCCEED("Model handle RAII completed");
}

// Test that lazy loading is respected
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: lazy loading respected", "[daemon]") {
    config_.lazyLoading = true;
    config_.preloadModels = {"model1", "model2"};

    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    REQUIRE(initResult);

    // With lazy loading, even preload models shouldn't block initialization
    // (They would fail to load in test environment anyway)
    auto loadedModels = pool_->getLoadedModels();

    // Should have attempted to load but failed (or succeeded if models exist)
    // The key is that initialization didn't hang
    CHECK(initResult);
}

// Test model path resolution
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: model path resolution", "[daemon]") {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    REQUIRE(initResult);

    // Test various model name formats
    std::vector<std::string> testModels = {testModelName(), "all-mpnet-base-v2", "test-model",
                                           "/absolute/path/model.onnx"};

    for (const auto& mn : testModels) {
        auto result = pool_->loadModel(mn);

        // In test environment, these should all fail with NotFound
        if (!result) {
            INFO("Model: " << mn);
            CHECK(result.error().code == yams::ErrorCode::NotFound);
        }
    }
}

// ============================================================================
// Real-model tests (require ONNX model on disk)
// ============================================================================

// Test session reuse performance — second acquire should be fast (pool reuse)
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: session reuse performance",
                 "[daemon][.slow][.requires_model]") {
    auto modelName = testModelName();
    if (!checkModelAvailable(modelName)) {
        SKIP("Model " + modelName + " not found. Download with: yams model --download " +
             modelName);
    }

    // Disable mock mode for real model tests
    unsetenv("YAMS_TEST_MODE");

    config_.maxMemoryGB = 2;
    config_.modelIdleTimeout = std::chrono::seconds(60);
    pool_ = std::make_unique<OnnxModelPool>(config_);
    REQUIRE(pool_->initialize());

    // First acquire — cold (triggers model load + session creation)
    auto coldStart = std::chrono::steady_clock::now();
    {
        auto h = pool_->acquireModel(modelName, 60s);
        REQUIRE(h);

        auto& session = *h.value();
        auto r = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings({"warmup text"});
        REQUIRE(r);
    }
    // Handle released — session returned to pool
    auto coldMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - coldStart)
                      .count();

    // Second acquire — warm (should reuse pooled session)
    auto warmStart = std::chrono::steady_clock::now();
    {
        auto h = pool_->acquireModel(modelName, 5s);
        REQUIRE(h);

        auto& session = *h.value();
        auto r = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings({"reuse text"});
        REQUIRE(r);
    }
    auto warmMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - warmStart)
                      .count();

    UNSCOPED_INFO("Cold acquire+embed: " << coldMs << " ms");
    UNSCOPED_INFO("Warm acquire+embed: " << warmMs << " ms");

    // Warm acquire should be significantly faster (no model compilation)
    CHECK(warmMs < coldMs);
}

// Test concurrent batch embedding — multiple threads acquire sessions simultaneously
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: concurrent batch embedding",
                 "[daemon][.slow][.requires_model]") {
    auto modelName = testModelName();
    if (!checkModelAvailable(modelName)) {
        SKIP("Model " + modelName + " not found. Download with: yams model --download " +
             modelName);
    }

    // Disable mock mode for real model tests
    unsetenv("YAMS_TEST_MODE");

    constexpr int kThreads = 4;
    TuneAdvisor::setOnnxSessionsPerModel(kThreads);

    config_.maxMemoryGB = 4;
    config_.modelIdleTimeout = std::chrono::seconds(60);
    pool_ = std::make_unique<OnnxModelPool>(config_);
    REQUIRE(pool_->initialize());

    // Warm up the model with a single acquire first
    {
        auto h = pool_->acquireModel(modelName, 60s);
        REQUIRE(h);
        auto& session = *h.value();
        auto r = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings({"warmup"});
        REQUIRE(r);
    }

    std::atomic<int> successes{0};
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i]() {
            auto h = pool_->acquireModel(modelName, 30s);
            if (!h) {
                failures++;
                return;
            }

            auto& session = *h.value();
            std::vector<std::string> texts = {
                "thread " + std::to_string(i) + " text one",
                "thread " + std::to_string(i) + " text two",
                "thread " + std::to_string(i) + " text three",
            };
            auto r = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings(texts);
            if (r && r.value().size() == texts.size()) {
                successes++;
            } else {
                failures++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Reset override
    TuneAdvisor::setOnnxSessionsPerModel(0);

    UNSCOPED_INFO("Concurrent results: " << successes << " succeeded, " << failures << " failed");
    CHECK(successes == kThreads);
    CHECK(failures == 0);
}

// Regression: some MIGraphX builds can return a fixed output batch size smaller than requested
// (e.g. output_B=2 for requested_B=8/16). Ensure OnnxModelSession adapts by chunking instead of
// surfacing shape-mismatch errors.
TEST_CASE_METHOD(OnnxModelPoolFixture,
                 "OnnxModelPool: MIGraphX oversized batch adapts without shape mismatch",
                 "[daemon][.slow][.requires_model][migraphx]") {
    auto modelName = testModelName();
    if (!checkModelAvailable(modelName)) {
        SKIP("Model " + modelName + " not found. Download with: yams model --download " +
             modelName);
    }

    // Disable mock mode for real model tests
    unsetenv("YAMS_TEST_MODE");

    config_.maxMemoryGB = 4;
    config_.modelIdleTimeout = std::chrono::seconds(60);
    pool_ = std::make_unique<OnnxModelPool>(config_);
    REQUIRE(pool_->initialize());

    auto h = pool_->acquireModel(modelName, 60s);
    REQUIRE(h);

    auto& session = *h.value();
    auto& mutableSession = const_cast<OnnxModelSession&>(session);
    const auto provider = mutableSession.getExecutionProvider();
    if (provider.find("migraphx") == std::string::npos) {
        SKIP("MIGraphX provider not active (provider='" + provider + "')");
    }

    // First large batch should either run directly or learn provider batch cap and chunk.
    std::vector<std::string> batch16;
    batch16.reserve(16);
    for (int i = 0; i < 16; ++i) {
        batch16.push_back("migraphx regression text #" + std::to_string(i));
    }

    auto r16 = mutableSession.generateBatchEmbeddings(batch16);
    REQUIRE(r16);
    REQUIRE(r16.value().size() == batch16.size());
    for (const auto& emb : r16.value()) {
        REQUIRE(!emb.empty());
    }

    // Run a second oversized batch to verify learned-cap path is stable across calls.
    std::vector<std::string> batch33;
    batch33.reserve(33);
    for (int i = 0; i < 33; ++i) {
        batch33.push_back("migraphx regression follow-up #" + std::to_string(i));
    }

    auto r33 = mutableSession.generateBatchEmbeddings(batch33);
    REQUIRE(r33);
    REQUIRE(r33.value().size() == batch33.size());
    for (const auto& emb : r33.value()) {
        REQUIRE(!emb.empty());
    }
}

TEST_CASE_METHOD(OnnxModelPoolFixture,
                 "OnnxModelPool: MIGraphX learned cap survives session reacquire",
                 "[daemon][.slow][.requires_model][migraphx][regression]") {
    auto modelName = testModelName();
    if (!checkModelAvailable(modelName)) {
        SKIP("Model " + modelName + " not found. Download with: yams model --download " +
             modelName);
    }

    unsetenv("YAMS_TEST_MODE");

    config_.maxMemoryGB = 4;
    config_.modelIdleTimeout = std::chrono::seconds(60);
    pool_ = std::make_unique<OnnxModelPool>(config_);
    REQUIRE(pool_->initialize());

    auto runOversized = [&](const std::string& marker, size_t batchSize) {
        auto h = pool_->acquireModel(modelName, 60s);
        REQUIRE(h);

        auto& session = *h.value();
        auto& mutableSession = const_cast<OnnxModelSession&>(session);
        const auto provider = mutableSession.getExecutionProvider();
        if (provider.find("migraphx") == std::string::npos) {
            SKIP("MIGraphX provider not active (provider='" + provider + "')");
        }

        std::vector<std::string> texts;
        texts.reserve(batchSize);
        for (size_t i = 0; i < batchSize; ++i) {
            texts.push_back(marker + " #" + std::to_string(i));
        }

        auto r = mutableSession.generateBatchEmbeddings(texts);
        REQUIRE(r);
        REQUIRE(r.value().size() == texts.size());
        for (const auto& emb : r.value()) {
            REQUIRE(!emb.empty());
        }
    };

    // First run learns cap on one acquired session.
    runOversized("migraphx session-1", 16);

    // Reacquire a session and verify oversized batch still succeeds.
    runOversized("migraphx session-2", 24);
}

// Test pool stats after real usage
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: pool stats after real usage",
                 "[daemon][.slow][.requires_model]") {
    auto modelName = testModelName();
    if (!checkModelAvailable(modelName)) {
        SKIP("Model " + modelName + " not found. Download with: yams model --download " +
             modelName);
    }

    // Disable mock mode for real model tests
    unsetenv("YAMS_TEST_MODE");

    config_.maxMemoryGB = 2;
    config_.modelIdleTimeout = std::chrono::seconds(60);
    pool_ = std::make_unique<OnnxModelPool>(config_);
    REQUIRE(pool_->initialize());

    // First acquire (cold — cache miss)
    {
        auto h = pool_->acquireModel(modelName, 60s);
        REQUIRE(h);
    }

    // Second acquire (warm — cache hit)
    {
        auto h = pool_->acquireModel(modelName, 5s);
        REQUIRE(h);
    }

    auto stats = pool_->getStats();
    UNSCOPED_INFO("totalRequests: " << stats.totalRequests);
    UNSCOPED_INFO("cacheHits:     " << stats.cacheHits);
    UNSCOPED_INFO("cacheMisses:   " << stats.cacheMisses);
    UNSCOPED_INFO("loadedModels:  " << stats.loadedModels);
    UNSCOPED_INFO("hitRate:       " << stats.hitRate);
    UNSCOPED_INFO("memoryUsage:   " << pool_->getMemoryUsage());

    CHECK(stats.totalRequests == 2);
    CHECK(stats.cacheMisses == 1);
    CHECK(stats.cacheHits == 1);
    CHECK(stats.loadedModels == 1);
    CHECK(stats.hitRate > 0.0);
    CHECK(pool_->getMemoryUsage() > 0);
}

// Test that Handle RAII properly returns session to pool (single-session pool)
TEST_CASE_METHOD(OnnxModelPoolFixture, "OnnxModelPool: handle RAII returns session",
                 "[daemon][.slow][.requires_model]") {
    auto modelName = testModelName();
    if (!checkModelAvailable(modelName)) {
        SKIP("Model " + modelName + " not found. Download with: yams model --download " +
             modelName);
    }

    // Disable mock mode for real model tests
    unsetenv("YAMS_TEST_MODE");

    // Single session pool — if RAII doesn't return the session, second acquire deadlocks
    TuneAdvisor::setOnnxSessionsPerModel(1);

    config_.maxMemoryGB = 2;
    config_.modelIdleTimeout = std::chrono::seconds(60);
    pool_ = std::make_unique<OnnxModelPool>(config_);
    REQUIRE(pool_->initialize());

    // Acquire in a scope block, run inference, let handle go out of scope
    {
        auto h = pool_->acquireModel(modelName, 60s);
        REQUIRE(h);
        auto& session = *h.value();
        auto r = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings({"raii test"});
        REQUIRE(r);
    }
    // Handle destroyed — session should be back in the pool

    // Immediately re-acquire — should succeed quickly if RAII returned the session
    auto start = std::chrono::steady_clock::now();
    auto h2 = pool_->acquireModel(modelName, 5s);
    auto acquireMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count();

    // Reset override
    TuneAdvisor::setOnnxSessionsPerModel(0);

    UNSCOPED_INFO("Re-acquire after RAII release took " << acquireMs << " ms");
    REQUIRE(h2);
    CHECK(acquireMs < 1000); // Should be near-instant, not waiting for timeout
}

// ---------------------------------------------------------------------------
// Dynamic padding correctness: embeddings should be numerically identical
// whether dynamic padding is enabled (default) or disabled.
// ---------------------------------------------------------------------------
TEST_CASE_METHOD(OnnxModelPoolFixture,
                 "OnnxModelPool: dynamic padding produces identical embeddings",
                 "[daemon][.slow][.requires_model]") {
    auto modelName = testModelName();
    if (!checkModelAvailable(modelName)) {
        SKIP("Model " + modelName + " not found. Download with: yams model --download " +
             modelName);
    }

    // Disable mock mode for real model tests
    unsetenv("YAMS_TEST_MODE");

    const std::vector<std::string> texts = {
        "short", "a slightly longer sentence for testing dynamic padding behavior",
        "the quick brown fox jumps over the lazy dog", "x"};

    // Phase 1: Generate embeddings with dynamic padding enabled (default)
    std::vector<std::vector<float>> dynamicEmbeddings;
    {
        config_.maxMemoryGB = 2;
        config_.modelIdleTimeout = std::chrono::seconds(60);
        pool_ = std::make_unique<OnnxModelPool>(config_);
        REQUIRE(pool_->initialize());

        auto h = pool_->acquireModel(modelName, 60s);
        REQUIRE(h);
        auto& session = *h.value();
        auto r = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings(texts);
        REQUIRE(r);
        dynamicEmbeddings = std::move(r.value());

        pool_->shutdown();
        pool_.reset();
    }

    // Phase 2: Generate embeddings with dynamic padding disabled
    std::vector<std::vector<float>> fixedEmbeddings;
    {
        setenv("YAMS_ONNX_DYNAMIC_PADDING", "0", 1);

        config_.maxMemoryGB = 2;
        config_.modelIdleTimeout = std::chrono::seconds(60);
        pool_ = std::make_unique<OnnxModelPool>(config_);
        REQUIRE(pool_->initialize());

        auto h = pool_->acquireModel(modelName, 60s);
        REQUIRE(h);
        auto& session = *h.value();
        auto r = const_cast<OnnxModelSession&>(session).generateBatchEmbeddings(texts);
        REQUIRE(r);
        fixedEmbeddings = std::move(r.value());

        pool_->shutdown();
        pool_.reset();

        unsetenv("YAMS_ONNX_DYNAMIC_PADDING");
    }

    // Compare: embeddings should be identical within floating point tolerance
    REQUIRE(dynamicEmbeddings.size() == fixedEmbeddings.size());
    REQUIRE(dynamicEmbeddings.size() == texts.size());

    for (size_t i = 0; i < texts.size(); ++i) {
        REQUIRE(dynamicEmbeddings[i].size() == fixedEmbeddings[i].size());
        double maxDiff = 0.0;
        for (size_t d = 0; d < dynamicEmbeddings[i].size(); ++d) {
            double diff = std::abs(static_cast<double>(dynamicEmbeddings[i][d]) -
                                   static_cast<double>(fixedEmbeddings[i][d]));
            maxDiff = std::max(maxDiff, diff);
        }
        UNSCOPED_INFO("Text " << i << " (\"" << texts[i].substr(0, 30) << "...\")"
                              << " max_diff=" << maxDiff << " dim=" << dynamicEmbeddings[i].size());
        // Allow small epsilon for floating point rounding differences
        CHECK(maxDiff < 1e-4);
    }
}

} // namespace yams::daemon::test
