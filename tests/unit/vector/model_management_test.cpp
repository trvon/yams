#include <chrono>
#include <fstream>
#include <thread>
#include <gtest/gtest.h>
#include <yams/core/format.h>
#include <yams/core/types.h>
#include <yams/vector/model_cache.h>
#include <yams/vector/model_loader.h>
#include <yams/vector/model_registry.h>

using namespace yams::vector;

class ModelManagementTest : public ::testing::Test {
protected:
    void SetUp() override {
        registry_ = std::make_unique<ModelRegistry>();

        ModelCacheConfig cache_config;
        cache_config.max_memory_bytes = 1024 * 1024 * 1024; // 1GB
        cache_config.max_models = 5;
        cache_config.enable_warmup = true;
        cache_ = std::make_unique<ModelCache>(cache_config);

        loader_ = std::make_shared<ModelLoader>();
        cache_->initialize(loader_);
    }

    ModelInfo createTestModelInfo(const std::string& id, const std::string& name,
                                  size_t dimension) {
        ModelInfo info;
        info.model_id = id;
        info.name = name;
        info.version = "1.0.0";
        info.path = std::format("/tmp/models/{}.onnx", id);
        info.format = "ONNX";
        info.embedding_dimension = dimension;
        info.max_sequence_length = 512;
        info.model_size_bytes = 100 * 1024 * 1024; // 100MB
        info.avg_inference_time_ms = 10.0;
        info.throughput_per_sec = 100.0;
        info.description = "Test model";
        info.is_available = true;
        return info;
    }

    std::unique_ptr<ModelRegistry> registry_;
    std::unique_ptr<ModelCache> cache_;
    std::shared_ptr<ModelLoader> loader_;
};

// Test model registration
TEST_F(ModelManagementTest, RegisterModel) {
    auto info = createTestModelInfo("test_model_1", "TestModel", 384);

    auto result = registry_->registerModel(info);
    EXPECT_TRUE(result.has_value());

    EXPECT_TRUE(registry_->hasModel("test_model_1"));
}

// Test duplicate registration
TEST_F(ModelManagementTest, RegisterDuplicateModel) {
    auto info = createTestModelInfo("test_model_1", "TestModel", 384);

    auto result1 = registry_->registerModel(info);
    EXPECT_TRUE(result1.has_value());

    auto result2 = registry_->registerModel(info);
    EXPECT_FALSE(result2.has_value());
    EXPECT_EQ(result2.error().code, yams::ErrorCode::InvalidArgument);
}

// Test model retrieval
TEST_F(ModelManagementTest, GetModel) {
    auto info = createTestModelInfo("test_model_1", "TestModel", 384);
    registry_->registerModel(info);

    auto result = registry_->getModel("test_model_1");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value().model_id, "test_model_1");
    EXPECT_EQ(result.value().embedding_dimension, 384);
}

// Test get models by dimension
TEST_F(ModelManagementTest, GetModelsByDimension) {
    registry_->registerModel(createTestModelInfo("model_384_1", "Model1", 384));
    registry_->registerModel(createTestModelInfo("model_384_2", "Model2", 384));
    registry_->registerModel(createTestModelInfo("model_768_1", "Model3", 768));

    auto result = registry_->getModelsByDimension(384);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value().size(), 2);

    auto result2 = registry_->getModelsByDimension(768);
    EXPECT_TRUE(result2.has_value());
    EXPECT_EQ(result2.value().size(), 1);
}

// Test model selection
TEST_F(ModelManagementTest, SelectBestModel) {
    auto model1 = createTestModelInfo("model_1", "Model1", 384);
    model1.throughput_per_sec = 50.0;

    auto model2 = createTestModelInfo("model_2", "Model2", 384);
    model2.throughput_per_sec = 100.0;

    auto model3 = createTestModelInfo("model_3", "Model3", 384);
    model3.throughput_per_sec = 75.0;

    registry_->registerModel(model1);
    registry_->registerModel(model2);
    registry_->registerModel(model3);

    std::map<std::string, std::string> requirements;
    auto result = registry_->selectBestModel(384, requirements);

    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "model_2"); // Highest throughput
}

// Test model metrics update
TEST_F(ModelManagementTest, UpdateMetrics) {
    auto info = createTestModelInfo("test_model_1", "TestModel", 384);
    registry_->registerModel(info);

    // Update metrics multiple times
    for (int i = 0; i < 10; ++i) {
        auto result = registry_->updateMetrics("test_model_1", 15.0 + i, true);
        EXPECT_TRUE(result.has_value());
    }

    auto result = registry_->updateMetrics("test_model_1", 0.0, false); // Failed inference
    EXPECT_TRUE(result.has_value());
}

// Test registry statistics
TEST_F(ModelManagementTest, RegistryStatistics) {
    registry_->registerModel(createTestModelInfo("model_1", "Model1", 384));
    registry_->registerModel(createTestModelInfo("model_2", "Model2", 384));
    registry_->registerModel(createTestModelInfo("model_3", "Model3", 768));

    auto stats = registry_->getStats();

    EXPECT_EQ(stats.total_models, 3);
    EXPECT_EQ(stats.available_models, 3);
    EXPECT_EQ(stats.models_by_dimension[384], 2);
    EXPECT_EQ(stats.models_by_dimension[768], 1);
    EXPECT_GT(stats.total_model_size_bytes, 0);
}

// Test model compatibility
TEST_F(ModelManagementTest, ModelCompatibility) {
    auto model1 = createTestModelInfo("model_1", "BERT", 384);
    auto model2 = createTestModelInfo("model_2", "BERT", 384);
    auto model3 = createTestModelInfo("model_3", "GPT", 768);

    EXPECT_TRUE(ModelCompatibilityChecker::areCompatible(model1, model2));
    EXPECT_FALSE(ModelCompatibilityChecker::areCompatible(model1, model3));
}

// Test cache initialization
TEST_F(ModelManagementTest, CacheInitialization) {
    EXPECT_EQ(cache_->getModelCount(), 0);
    EXPECT_GT(cache_->getAvailableMemory(), 0);
}

// Test cache statistics
TEST_F(ModelManagementTest, CacheStatistics) {
    auto stats = cache_->getStats();

    EXPECT_EQ(stats.total_models_loaded, 0);
    EXPECT_EQ(stats.cache_hits, 0);
    EXPECT_EQ(stats.cache_misses, 0);
    EXPECT_EQ(stats.evictions, 0);
    EXPECT_GT(stats.max_memory_bytes, 0);
}

// Test loader format detection
TEST_F(ModelManagementTest, LoaderFormatDetection) {
    auto formats = loader_->getSupportedFormats();
    EXPECT_GT(formats.size(), 0);

    EXPECT_TRUE(loader_->supportsFormat("ONNX"));
    EXPECT_TRUE(loader_->supportsFormat("onnx"));
}

// Test loader statistics
TEST_F(ModelManagementTest, LoaderStatistics) {
    auto stats = loader_->getStats();

    EXPECT_EQ(stats.total_loads, 0);
    EXPECT_EQ(stats.successful_loads, 0);
    EXPECT_EQ(stats.failed_loads, 0);
}

// Test model discovery
TEST_F(ModelManagementTest, ModelDiscovery) {
    // Create a temporary directory for testing
    std::string test_dir = "/tmp/test_models";
    std::filesystem::create_directories(test_dir);

    // Create mock model files
    std::ofstream file1(test_dir + "/model1.onnx");
    file1 << "mock model data";
    file1.close();

    auto result = registry_->discoverModels(test_dir);
    EXPECT_TRUE(result.has_value());

    // Clean up
    std::filesystem::remove_all(test_dir);
}

// Test concurrent model access
TEST_F(ModelManagementTest, ConcurrentAccess) {
    // Register multiple models
    for (int i = 0; i < 10; ++i) {
        auto info =
            createTestModelInfo(yams::format("model_{}", i), yams::format("Model{}", i), 384);
        registry_->registerModel(info);
    }

    // Concurrent reads
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([this, &success_count]() {
            for (int i = 0; i < 10; ++i) {
                auto result = registry_->getModel(yams::format("model_{}", i));
                if (result.has_value()) {
                    success_count++;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(success_count, 40); // 4 threads * 10 models
}

// Test memory management
TEST_F(ModelManagementTest, MemoryManagement) {
    CacheMemoryManager mem_manager(1024 * 1024); // 1MB

    EXPECT_TRUE(mem_manager.canAllocate(512 * 1024)); // 512KB

    auto result = mem_manager.allocate("model1", 512 * 1024);
    EXPECT_TRUE(result.has_value());

    EXPECT_EQ(mem_manager.getUsedMemory(), 512 * 1024);
    EXPECT_EQ(mem_manager.getAvailableMemory(), 512 * 1024);

    EXPECT_FALSE(mem_manager.canAllocate(600 * 1024)); // Too large

    mem_manager.deallocate("model1");
    EXPECT_EQ(mem_manager.getUsedMemory(), 0);
}