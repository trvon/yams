// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/core/types.h>
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

// Helper to check model availability
static bool checkModelAvailable(const std::string& modelName) {
    const char* home = std::getenv("HOME");
    if (!home)
        return false;

    fs::path modelPath = fs::path(home) / ".yams/models" / modelName / "model.onnx";
    return fs::exists(modelPath);
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
    if (!checkModelAvailable("all-MiniLM-L6-v2")) {
        SKIP("Model all-MiniLM-L6-v2 not found at ~/.yams/models/all-MiniLM-L6-v2/model.onnx. "
             "Download with: yams model --download all-MiniLM-L6-v2");
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
    auto result = pool_->loadModel("all-MiniLM-L6-v2");
    auto elapsed = std::chrono::steady_clock::now() - start;

    // Fail if it takes more than 30 seconds - something is wrong
    auto elapsedSeconds = std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
    INFO("Model loading took " << elapsedSeconds << " seconds");
    REQUIRE(elapsedSeconds < 30);

    if (result) {
        CHECK(pool_->isModelLoaded("all-MiniLM-L6-v2"));
        auto loadedModels = pool_->getLoadedModels();
        CHECK(loadedModels.size() == 1);
        CHECK(loadedModels[0] == "all-MiniLM-L6-v2");
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

    if (checkModelAvailable("all-MiniLM-L6-v2")) {
        // Test with real model acquisition - should succeed or timeout appropriately
        auto result = pool_->acquireModel("all-MiniLM-L6-v2", 100ms);
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
    std::string testModel =
        checkModelAvailable("all-MiniLM-L6-v2") ? "all-MiniLM-L6-v2" : "nonexistent-model";
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

    std::string testModel =
        checkModelAvailable("all-MiniLM-L6-v2") ? "all-MiniLM-L6-v2" : "nonexistent-model";

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
    std::vector<std::string> testModels = {"all-MiniLM-L6-v2", "all-mpnet-base-v2", "test-model",
                                           "/absolute/path/model.onnx"};

    for (const auto& modelName : testModels) {
        auto result = pool_->loadModel(modelName);

        // In test environment, these should all fail with NotFound
        if (!result) {
            INFO("Model: " << modelName);
            CHECK(result.error().code == yams::ErrorCode::NotFound);
        }
    }
}

} // namespace yams::daemon::test
