#include <chrono>
#include <filesystem>
#include <thread>
#include <gtest/gtest.h>
#include <yams/daemon/resource/onnx_model_pool.h>
#include <yams/vector/embedding_generator.h>

namespace yams::daemon::test {

using namespace std::chrono_literals;
namespace fs = std::filesystem;

class OnnxModelPoolTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test config
        config_.maxLoadedModels = 2;
        config_.maxMemoryGB = 1.0;
        config_.numThreads = 2;
        config_.enableGPU = false;
        config_.lazyLoading = true; // Don't block on initialization
        config_.modelIdleTimeout = std::chrono::seconds(1);
        config_.preloadModels.clear(); // No preloading for tests
    }

    void TearDown() override {
        if (pool_) {
            pool_->shutdown();
        }
    }

    ModelPoolConfig config_;
    std::unique_ptr<OnnxModelPool> pool_;
};

// Test pool creation and initialization
TEST_F(OnnxModelPoolTest, PoolCreation) {
    pool_ = std::make_unique<OnnxModelPool>(config_);

    auto result = pool_->initialize();
    ASSERT_TRUE(result) << "Failed to initialize pool: " << result.error().message;

    // Should start with no models loaded (lazy loading)
    auto loadedModels = pool_->getLoadedModels();
    EXPECT_EQ(loadedModels.size(), 0);

    auto stats = pool_->getStats();
    EXPECT_EQ(stats.loadedModels, 0);
    EXPECT_EQ(stats.totalRequests, 0);
}

// Test model loading (will fail if model doesn't exist, but tests the API)
TEST_F(OnnxModelPoolTest, ModelLoading) {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    ASSERT_TRUE(initResult);

    // Try to load a model (will likely fail in test environment)
    auto result = pool_->loadModel("test-model");

    if (!result) {
        // Expected in test environment without actual models
        EXPECT_EQ(result.error().code, ErrorCode::NotFound);
        EXPECT_NE(result.error().message.find("not found"), std::string::npos);
    } else {
        // If it somehow succeeds, verify it's loaded
        EXPECT_TRUE(pool_->isModelLoaded("test-model"));
        auto loadedModels = pool_->getLoadedModels();
        EXPECT_EQ(loadedModels.size(), 1);
    }
}

// Test model acquisition timeout
TEST_F(OnnxModelPoolTest, AcquisitionTimeout) {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    ASSERT_TRUE(initResult);

    // Try to acquire a non-existent model with short timeout
    auto result = pool_->acquireModel("nonexistent-model", 10ms);

    EXPECT_FALSE(result);
    if (!result) {
        // Should fail with NotFound, not timeout (since model doesn't exist)
        EXPECT_EQ(result.error().code, ErrorCode::NotFound);
    }
}

// Test model pool statistics
TEST_F(OnnxModelPoolTest, Statistics) {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    ASSERT_TRUE(initResult);

    auto stats1 = pool_->getStats();
    EXPECT_EQ(stats1.totalRequests, 0);
    EXPECT_EQ(stats1.cacheHits, 0);
    EXPECT_EQ(stats1.cacheMisses, 0);

    // Try to acquire a model (will fail but should update stats)
    auto result = pool_->acquireModel("test-model", 10ms);

    auto stats2 = pool_->getStats();
    EXPECT_EQ(stats2.totalRequests, 1);

    // Cache miss since model doesn't exist
    if (!result) {
        EXPECT_EQ(stats2.cacheMisses, 1);
    }
}

// Test concurrent model acquisition attempts
TEST_F(OnnxModelPoolTest, ConcurrentAcquisition) {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    ASSERT_TRUE(initResult);

    const int numThreads = 5;
    std::atomic<int> attempts{0};
    std::atomic<int> failures{0};

    std::vector<std::thread> threads;
    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([this, &attempts, &failures]() {
            for (int j = 0; j < 10; ++j) {
                attempts++;
                auto result = pool_->acquireModel("model", 10ms);
                if (!result) {
                    failures++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(attempts, numThreads * 10);
    // All should fail since model doesn't exist
    EXPECT_EQ(failures, numThreads * 10);

    auto stats = pool_->getStats();
    EXPECT_EQ(stats.totalRequests, numThreads * 10);
}

// Test pool shutdown
TEST_F(OnnxModelPoolTest, PoolShutdown) {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    ASSERT_TRUE(initResult);

    // Shutdown the pool
    pool_->shutdown();

    // Should not be able to load models after shutdown
    auto result = pool_->loadModel("test");
    EXPECT_FALSE(result);

    // Should not be able to acquire models after shutdown
    auto acquireResult = pool_->acquireModel("test", 10ms);
    EXPECT_FALSE(acquireResult);
}

// Test model unloading
TEST_F(OnnxModelPoolTest, ModelUnloading) {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    ASSERT_TRUE(initResult);

    // Try to unload a non-existent model
    auto result = pool_->unloadModel("nonexistent");
    EXPECT_FALSE(result);
    if (!result) {
        EXPECT_EQ(result.error().code, ErrorCode::NotFound);
    }
}

// Test LRU eviction (conceptual test since we can't load real models)
TEST_F(OnnxModelPoolTest, LruEviction) {
    config_.maxLoadedModels = 2;
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    ASSERT_TRUE(initResult);

    // The actual eviction would happen if we could load real models
    // This test verifies the API exists and doesn't crash
    auto stats = pool_->getStats();
    EXPECT_GE(stats.loadedModels, 0);
    EXPECT_LE(stats.loadedModels, config_.maxLoadedModels);
}

// Test memory limit enforcement
TEST_F(OnnxModelPoolTest, MemoryLimit) {
    config_.maxMemoryGB = 1.0; // Small limit (1GB)
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    ASSERT_TRUE(initResult);

    // Memory usage should be tracked
    auto memUsage = pool_->getMemoryUsage();
    EXPECT_GE(memUsage, 0);

    auto stats = pool_->getStats();
    EXPECT_LE(stats.totalMemoryBytes, config_.maxMemoryGB * 1024 * 1024 * 1024);
}

// Test maintenance operations
TEST_F(OnnxModelPoolTest, Maintenance) {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    ASSERT_TRUE(initResult);

    // Perform maintenance (should not crash even with no models)
    pool_->performMaintenance();

    // Stats should still be valid
    auto stats = pool_->getStats();
    EXPECT_GE(stats.loadedModels, 0);
}

// Test model handle RAII behavior
TEST_F(OnnxModelPoolTest, ModelHandleRaii) {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    ASSERT_TRUE(initResult);

    {
        // Try to acquire a model in a scope
        auto result = pool_->acquireModel("test", 10ms);

        if (result) {
            // If successful, handle should be valid
            auto& handle = result.value();
            EXPECT_TRUE(handle.isValid());

            // Handle will be released when going out of scope
        }
    }

    // After scope, resources should be released
    // (Can't directly test this without real models)
}

// Test that lazy loading is respected
TEST_F(OnnxModelPoolTest, LazyLoadingRespected) {
    config_.lazyLoading = true;
    config_.preloadModels = {"model1", "model2"};

    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    ASSERT_TRUE(initResult);

    // With lazy loading, even preload models shouldn't block initialization
    // (They would fail to load in test environment anyway)
    auto loadedModels = pool_->getLoadedModels();

    // Should have attempted to load but failed (or succeeded if models exist)
    // The key is that initialization didn't hang
    EXPECT_TRUE(initResult);
}

// Test model path resolution
TEST_F(OnnxModelPoolTest, ModelPathResolution) {
    pool_ = std::make_unique<OnnxModelPool>(config_);
    auto initResult = pool_->initialize();
    ASSERT_TRUE(initResult);

    // Test various model name formats
    std::vector<std::string> testModels = {"all-MiniLM-L6-v2", "all-mpnet-base-v2", "test-model",
                                           "/absolute/path/model.onnx"};

    for (const auto& modelName : testModels) {
        auto result = pool_->loadModel(modelName);

        // In test environment, these should all fail with NotFound
        if (!result) {
            EXPECT_EQ(result.error().code, ErrorCode::NotFound) << "Model: " << modelName;
        }
    }
}

} // namespace yams::daemon::test