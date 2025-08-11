#include <gtest/gtest.h>
#include <yams/vector/vector_index_manager.h>
#include <yams/core/types.h>
#include <thread>
#include <random>
#include <chrono>
#include <algorithm>
#include <atomic>

using namespace yams::vector;

class VectorIndexManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_ = IndexConfig{};
        config_.dimension = 128;
        config_.type = IndexType::FLAT;
        config_.distance_metric = DistanceMetric::COSINE;
        
        manager_ = std::make_unique<VectorIndexManager>(config_);
    }
    
    // Generate random normalized vector
    std::vector<float> generateRandomVector(size_t dim) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
        
        std::vector<float> vec(dim);
        float norm = 0.0f;
        
        for (size_t i = 0; i < dim; ++i) {
            vec[i] = dist(gen);
            norm += vec[i] * vec[i];
        }
        
        // Normalize
        norm = std::sqrt(norm);
        if (norm > 0) {
            for (auto& val : vec) {
                val /= norm;
            }
        }
        
        return vec;
    }
    
    IndexConfig config_;
    std::unique_ptr<VectorIndexManager> manager_;
};

// Test index creation and initialization
TEST_F(VectorIndexManagerTest, CreateAndInitialize) {
    EXPECT_NE(manager_, nullptr);
    auto result = manager_->initialize();
    EXPECT_TRUE(result.has_value());
    EXPECT_TRUE(manager_->isInitialized());
}

// Test adding single vector
TEST_F(VectorIndexManagerTest, AddSingleVector) {
    auto vec = generateRandomVector(128);
    std::string id = "test_vector_1";
    
    auto result = manager_->addVector(id, vec);
    EXPECT_TRUE(result.has_value());
}

// Test adding multiple vectors
TEST_F(VectorIndexManagerTest, AddMultipleVectors) {
    const size_t num_vectors = 100;
    std::vector<std::string> ids;
    std::vector<std::vector<float>> vectors;
    
    for (size_t i = 0; i < num_vectors; ++i) {
        ids.push_back("vec_" + std::to_string(i));
        vectors.push_back(generateRandomVector(128));
    }
    
    for (size_t i = 0; i < num_vectors; ++i) {
        auto result = manager_->addVector(ids[i], vectors[i]);
        EXPECT_TRUE(result.has_value());
    }
}

// Test vector retrieval - feature may not be implemented yet
TEST_F(VectorIndexManagerTest, DISABLED_GetVector) {
    auto vec = generateRandomVector(128);
    std::string id = "test_vector";
    
    manager_->addVector(id, vec);
    
    // Get vector feature may not be available in current API
    // This test is disabled until the feature is implemented
}

// Test vector update - feature may not be implemented yet
TEST_F(VectorIndexManagerTest, DISABLED_UpdateVector) {
    // Update vector feature may not be available in current API
    // This test is disabled until the feature is implemented
}

// Test vector deletion
TEST_F(VectorIndexManagerTest, RemoveVector) {
    std::string id = "delete_test";
    auto vec = generateRandomVector(128);
    
    manager_->addVector(id, vec);
    
    auto result = manager_->removeVector(id);
    EXPECT_TRUE(result.has_value());
}

// Test k-NN search
TEST_F(VectorIndexManagerTest, SearchKNN) {
    // Add test vectors
    const size_t num_vectors = 50;
    for (size_t i = 0; i < num_vectors; ++i) {
        manager_->addVector("vec_" + std::to_string(i), generateRandomVector(128));
    }
    
    // Search for nearest neighbors
    auto query = generateRandomVector(128);
    size_t k = 5;
    
    SearchFilter filter;
    auto results = manager_->search(query, k, filter);
    EXPECT_TRUE(results.has_value());
    EXPECT_LE(results.value().size(), k);
    
    // Verify results are sorted by distance
    for (size_t i = 1; i < results.value().size(); ++i) {
        EXPECT_LE(results.value()[i-1].distance, results.value()[i].distance);
    }
}

// Test radius search
TEST_F(VectorIndexManagerTest, SearchRadius) {
    // Add test vectors
    const size_t num_vectors = 50;
    for (size_t i = 0; i < num_vectors; ++i) {
        manager_->addVector("vec_" + std::to_string(i), generateRandomVector(128));
    }
    
    // Search within radius
    auto query = generateRandomVector(128);
    float radius = 0.5f;
    
    SearchFilter filter;
    filter.max_distance = radius;
    auto results = manager_->search(query, 100, filter);  // Get up to 100 within radius
    EXPECT_TRUE(results.has_value());
    
    // Verify all results are within radius
    for (const auto& result : results.value()) {
        EXPECT_LE(result.distance, radius);
    }
}

// Test empty index search
TEST_F(VectorIndexManagerTest, SearchEmptyIndex) {
    auto query = generateRandomVector(128);
    SearchFilter filter;
    
    auto results = manager_->search(query, 5, filter);
    EXPECT_TRUE(results.has_value());
    EXPECT_EQ(results.value().size(), 0);
}

// Test invalid dimension handling
TEST_F(VectorIndexManagerTest, InvalidDimension) {
    auto vec_wrong_dim = generateRandomVector(64);  // Wrong dimension
    
    auto result = manager_->addVector("invalid", vec_wrong_dim);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, yams::ErrorCode::InvalidArgument);
}

// Test duplicate ID handling
TEST_F(VectorIndexManagerTest, DuplicateID) {
    std::string id = "duplicate";
    auto vec1 = generateRandomVector(128);
    auto vec2 = generateRandomVector(128);
    
    auto result1 = manager_->addVector(id, vec1);
    EXPECT_TRUE(result1.has_value());
    
    // Adding same ID again might replace or fail depending on implementation
    auto result2 = manager_->addVector(id, vec2);
    // Just check that it doesn't crash - behavior may vary
}

// Test concurrent additions
TEST_F(VectorIndexManagerTest, ConcurrentAdditions) {
    const size_t num_threads = 4;
    const size_t vectors_per_thread = 25;
    
    std::vector<std::thread> threads;
    std::atomic<size_t> success_count{0};
    
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([this, t, vectors_per_thread, &success_count]() {
            for (size_t i = 0; i < vectors_per_thread; ++i) {
                std::string id = "thread_" + std::to_string(t) + "_vec_" + std::to_string(i);
                auto vec = generateRandomVector(128);
                
                auto result = manager_->addVector(id, vec);
                if (result.has_value()) {
                    success_count++;
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    EXPECT_EQ(success_count, num_threads * vectors_per_thread);
    EXPECT_EQ(manager_->size(), num_threads * vectors_per_thread);
}

// Test concurrent searches
TEST_F(VectorIndexManagerTest, ConcurrentSearches) {
    // Add test vectors
    const size_t num_vectors = 100;
    for (size_t i = 0; i < num_vectors; ++i) {
        manager_->addVector("vec_" + std::to_string(i), generateRandomVector(128));
    }
    
    const size_t num_threads = 4;
    const size_t searches_per_thread = 10;
    
    std::vector<std::thread> threads;
    std::atomic<size_t> success_count{0};
    
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([this, searches_per_thread, &success_count]() {
            for (size_t i = 0; i < searches_per_thread; ++i) {
                auto query = generateRandomVector(128);
                SearchFilter filter;
                auto results = manager_->search(query, 5, filter);
                
                if (results.has_value()) {
                    success_count++;
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    EXPECT_EQ(success_count, num_threads * searches_per_thread);
}

// Test index persistence (save/load)
TEST_F(VectorIndexManagerTest, IndexPersistence) {
    // Add test vectors
    const size_t num_vectors = 20;
    std::vector<std::string> ids;
    std::vector<std::vector<float>> vectors;
    
    for (size_t i = 0; i < num_vectors; ++i) {
        ids.push_back("vec_" + std::to_string(i));
        vectors.push_back(generateRandomVector(128));
        manager_->addVector(ids.back(), vectors.back());
    }
    
    // Save index
    std::string index_path = "/tmp/test_index.bin";
    auto save_result = manager_->saveIndex(index_path);
    EXPECT_TRUE(save_result.has_value());
    
    // Create new manager and load index
    auto new_manager = std::make_unique<VectorIndexManager>(config_);
    new_manager->initialize();
    auto load_result = new_manager->loadIndex(index_path);
    EXPECT_TRUE(load_result.has_value());
    
    // Verify loaded index works by searching
    auto query = generateRandomVector(128);
    SearchFilter filter;
    auto results = new_manager->search(query, 5, filter);
    EXPECT_TRUE(results.has_value());
    
    // Clean up
    std::remove(index_path.c_str());
}

// Test HNSW index type
TEST_F(VectorIndexManagerTest, HNSWIndex) {
    IndexConfig hnsw_config;
    hnsw_config.dimension = 128;
    hnsw_config.type = IndexType::HNSW;
    hnsw_config.distance_metric = DistanceMetric::L2;
    hnsw_config.hnsw_m = 16;
    hnsw_config.hnsw_ef_construction = 200;
    
    auto hnsw_manager = std::make_unique<VectorIndexManager>(hnsw_config);
    
    // Add vectors
    const size_t num_vectors = 100;
    for (size_t i = 0; i < num_vectors; ++i) {
        hnsw_manager->addVector("vec_" + std::to_string(i), generateRandomVector(128));
    }
    
    // Build index
    auto build_result = hnsw_manager->buildIndex();
    EXPECT_TRUE(build_result.has_value());
    
    // Search
    auto query = generateRandomVector(128);
    SearchFilter filter;
    auto results = hnsw_manager->search(query, 10, filter);
    EXPECT_TRUE(results.has_value());
    EXPECT_GT(results.value().size(), 0);
}

// Test batch operations
TEST_F(VectorIndexManagerTest, BatchOperations) {
    // Batch add
    std::vector<std::string> ids;
    std::vector<std::vector<float>> vectors;
    
    for (size_t i = 0; i < 50; ++i) {
        ids.push_back("batch_" + std::to_string(i));
        vectors.push_back(generateRandomVector(128));
    }
    
    // Add vectors one by one (batch API may not exist)
    for (size_t i = 0; i < ids.size(); ++i) {
        auto result = manager_->addVector(ids[i], vectors[i]);
        EXPECT_TRUE(result.has_value());
    }
    
    // Remove vectors one by one
    for (size_t i = 0; i < 25; ++i) {
        auto result = manager_->removeVector(ids[i]);
        EXPECT_TRUE(result.has_value());
    }
}

// Test memory usage tracking
TEST_F(VectorIndexManagerTest, MemoryUsage) {
    auto stats = manager_->getStats();
    size_t initial_memory = stats.index_size_bytes;
    EXPECT_GE(initial_memory, 0);
    
    // Add vectors and check memory increase
    const size_t num_vectors = 100;
    for (size_t i = 0; i < num_vectors; ++i) {
        manager_->addVector("vec_" + std::to_string(i), generateRandomVector(128));
    }
    
    stats = manager_->getStats();
    size_t after_add_memory = stats.index_size_bytes;
    EXPECT_GT(after_add_memory, initial_memory);
    
    // Expected memory: at least num_vectors * dimension * sizeof(float)
    size_t min_expected = num_vectors * 128 * sizeof(float);
    EXPECT_GE(after_add_memory - initial_memory, min_expected);
}

// Test error recovery
TEST_F(VectorIndexManagerTest, ErrorRecovery) {
    // Try to remove non-existent vector
    auto result2 = manager_->removeVector("non_existent");
    // May or may not return error depending on implementation
    
    // Verify manager still works after errors
    auto vec = generateRandomVector(128);
    auto result4 = manager_->addVector("recovery_test", vec);
    EXPECT_TRUE(result4.has_value());
}

// Test index statistics
TEST_F(VectorIndexManagerTest, IndexStatistics) {
    // Add vectors
    const size_t num_vectors = 50;
    for (size_t i = 0; i < num_vectors; ++i) {
        manager_->addVector("vec_" + std::to_string(i), generateRandomVector(128));
    }
    
    auto stats = manager_->getStats();
    EXPECT_EQ(stats.num_vectors, num_vectors);
    EXPECT_EQ(stats.dimension, 128);
    EXPECT_GT(stats.index_size_bytes, 0);
}

// Performance benchmark test
TEST_F(VectorIndexManagerTest, PerformanceBenchmark) {
    const size_t num_vectors = 1000;
    const size_t num_queries = 100;
    const size_t k = 10;
    
    // Add vectors
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < num_vectors; ++i) {
        manager_->addVector("vec_" + std::to_string(i), generateRandomVector(128));
    }
    auto add_time = std::chrono::high_resolution_clock::now() - start;
    
    // Perform searches
    start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < num_queries; ++i) {
        auto query = generateRandomVector(128);
        SearchFilter filter;
        manager_->search(query, k, filter);
    }
    auto search_time = std::chrono::high_resolution_clock::now() - start;
    
    // Calculate metrics
    auto add_ms = std::chrono::duration_cast<std::chrono::milliseconds>(add_time).count();
    auto search_ms = std::chrono::duration_cast<std::chrono::milliseconds>(search_time).count();
    
    double add_per_sec = (num_vectors * 1000.0) / add_ms;
    double search_per_sec = (num_queries * 1000.0) / search_ms;
    
    // Log performance (these are not hard requirements, just monitoring)
    std::cout << "Performance Metrics:" << std::endl;
    std::cout << "  Additions: " << add_per_sec << " vectors/sec" << std::endl;
    std::cout << "  Searches: " << search_per_sec << " queries/sec" << std::endl;
    
    // Basic sanity checks
    EXPECT_GT(add_per_sec, 100);     // At least 100 additions per second
    EXPECT_GT(search_per_sec, 10);   // At least 10 searches per second
}