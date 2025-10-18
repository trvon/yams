// PBI-057: Unit tests for sqlite-vec C++ wrapper integration
// Tests the new C++20/23 wrapper functionality and template-based API

#include <cmath>
#include <string>
#include <vector>
#include <gtest/gtest.h>
#include <yams/vector/sqlite_vec_wrapper.hpp>

namespace yams::vector::test {

class SqliteVecWrapperTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Check if we should skip vector tests
        if (const char* skip_env = std::getenv("YAMS_SQLITE_VEC_SKIP_INIT")) {
            std::string v(skip_env);
            if (v == "1" || v == "true") {
                GTEST_SKIP() << "Skipping sqlite-vec wrapper test (YAMS_SQLITE_VEC_SKIP_INIT=1)";
            }
        }
        if (const char* disable_env = std::getenv("YAMS_DISABLE_VECTORS")) {
            std::string v(disable_env);
            if (v == "1" || v == "true") {
                GTEST_SKIP() << "Skipping sqlite-vec wrapper test (YAMS_DISABLE_VECTORS=1)";
            }
        }
    }
};

// Test basic wrapper initialization
TEST_F(SqliteVecWrapperTest, WrapperInitialization) {
    SqliteVecWrapper wrapper;
    EXPECT_TRUE(wrapper.initialize()) << "Failed to initialize sqlite-vec wrapper";
}

// Test distance metric types
TEST_F(SqliteVecWrapperTest, DistanceMetricTypes) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    // Verify all distance metrics are available
    std::vector<std::string> metrics = {"l2", "cosine", "l1", "hamming"};

    for (const auto& metric : metrics) {
        EXPECT_TRUE(wrapper.supportsMetric(metric))
            << "Distance metric '" << metric << "' should be supported";
    }
}

// Test template-based vector operations with different types
TEST_F(SqliteVecWrapperTest, TemplateVectorOperationsFloat) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    std::vector<float> vec1 = {1.0f, 2.0f, 3.0f, 4.0f};
    std::vector<float> vec2 = {4.0f, 3.0f, 2.0f, 1.0f};

    auto distance = wrapper.computeDistance<float>(vec1, vec2, DistanceMetric::L2);
    ASSERT_TRUE(distance.has_value());
    EXPECT_GT(*distance, 0.0f) << "L2 distance should be positive";
}

TEST_F(SqliteVecWrapperTest, TemplateVectorOperationsDouble) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    std::vector<double> vec1 = {1.0, 2.0, 3.0, 4.0};
    std::vector<double> vec2 = {1.0, 2.0, 3.0, 4.0};

    auto distance = wrapper.computeDistance<double>(vec1, vec2, DistanceMetric::L2);
    ASSERT_TRUE(distance.has_value());
    EXPECT_NEAR(*distance, 0.0, 1e-6) << "L2 distance between identical vectors should be 0";
}

// Test normalized vector operations
TEST_F(SqliteVecWrapperTest, NormalizedVectorOperations) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    std::vector<float> vec = {3.0f, 4.0f, 0.0f, 0.0f};

    auto normalized = wrapper.normalizeVector(vec);
    ASSERT_TRUE(normalized.has_value());

    // Check magnitude is 1.0
    float magnitude = 0.0f;
    for (float val : *normalized) {
        magnitude += val * val;
    }
    magnitude = std::sqrt(magnitude);

    EXPECT_NEAR(magnitude, 1.0f, 1e-6) << "Normalized vector should have unit magnitude";
}

// Test cosine similarity
TEST_F(SqliteVecWrapperTest, CosineSimilarity) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    // Identical vectors should have cosine distance ~0 (similarity ~1)
    std::vector<float> vec1 = {1.0f, 0.0f, 0.0f, 0.0f};
    std::vector<float> vec2 = {1.0f, 0.0f, 0.0f, 0.0f};

    auto distance = wrapper.computeDistance<float>(vec1, vec2, DistanceMetric::Cosine);
    ASSERT_TRUE(distance.has_value());
    EXPECT_NEAR(*distance, 0.0f, 1e-5) << "Cosine distance between identical vectors should be ~0";

    // Orthogonal vectors
    std::vector<float> vec3 = {1.0f, 0.0f, 0.0f, 0.0f};
    std::vector<float> vec4 = {0.0f, 1.0f, 0.0f, 0.0f};

    distance = wrapper.computeDistance<float>(vec3, vec4, DistanceMetric::Cosine);
    ASSERT_TRUE(distance.has_value());
    EXPECT_GT(*distance, 0.9f) << "Cosine distance between orthogonal vectors should be high";
}

// Test L1 (Manhattan) distance
TEST_F(SqliteVecWrapperTest, L1Distance) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    std::vector<float> vec1 = {1.0f, 2.0f, 3.0f, 4.0f};
    std::vector<float> vec2 = {4.0f, 3.0f, 2.0f, 1.0f};

    auto distance = wrapper.computeDistance<float>(vec1, vec2, DistanceMetric::L1);
    ASSERT_TRUE(distance.has_value());

    // L1 distance = |1-4| + |2-3| + |3-2| + |4-1| = 3 + 1 + 1 + 3 = 8
    EXPECT_NEAR(*distance, 8.0f, 1e-5) << "L1 distance should be 8.0";
}

// Test Hamming distance
TEST_F(SqliteVecWrapperTest, HammingDistance) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    // For Hamming, typically used with binary vectors
    std::vector<int8_t> vec1 = {1, 0, 1, 0, 1, 0, 1, 0};
    std::vector<int8_t> vec2 = {1, 1, 1, 1, 0, 0, 0, 0};

    auto distance = wrapper.computeDistance<int8_t>(vec1, vec2, DistanceMetric::Hamming);
    ASSERT_TRUE(distance.has_value());

    // Hamming distance = number of differing positions = 4
    EXPECT_NEAR(*distance, 4.0f, 1e-5) << "Hamming distance should be 4.0";
}

// Test batch vector operations
TEST_F(SqliteVecWrapperTest, BatchVectorOperations) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    std::vector<std::vector<float>> batch = {{1.0f, 0.0f, 0.0f, 0.0f},
                                             {0.0f, 1.0f, 0.0f, 0.0f},
                                             {0.0f, 0.0f, 1.0f, 0.0f},
                                             {0.0f, 0.0f, 0.0f, 1.0f}};

    std::vector<float> query = {1.0f, 0.0f, 0.0f, 0.0f};

    auto distances = wrapper.batchComputeDistances(batch, query, DistanceMetric::L2);
    ASSERT_TRUE(distances.has_value());
    ASSERT_EQ(distances->size(), 4);

    // First vector is identical, should have distance ~0
    EXPECT_NEAR(distances->at(0), 0.0f, 1e-5);

    // Others are orthogonal, should have distance ~1.414 (sqrt(2))
    for (size_t i = 1; i < 4; ++i) {
        EXPECT_NEAR(distances->at(i), std::sqrt(2.0f), 1e-4);
    }
}

// Test error handling - dimension mismatch
TEST_F(SqliteVecWrapperTest, DimensionMismatchError) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    std::vector<float> vec1 = {1.0f, 2.0f, 3.0f};
    std::vector<float> vec2 = {1.0f, 2.0f}; // Different dimension

    auto distance = wrapper.computeDistance<float>(vec1, vec2, DistanceMetric::L2);
    EXPECT_FALSE(distance.has_value()) << "Should fail with dimension mismatch";
}

// Test error handling - empty vectors
TEST_F(SqliteVecWrapperTest, EmptyVectorError) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    std::vector<float> empty_vec;
    std::vector<float> normal_vec = {1.0f, 2.0f, 3.0f};

    auto distance = wrapper.computeDistance<float>(empty_vec, normal_vec, DistanceMetric::L2);
    EXPECT_FALSE(distance.has_value()) << "Should fail with empty vector";
}

// Test SIMD optimizations (if enabled)
TEST_F(SqliteVecWrapperTest, SIMDOptimizations) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    if (!wrapper.hasSIMDSupport()) {
        GTEST_SKIP() << "SIMD not available on this platform";
    }

    // Large vectors to benefit from SIMD
    const size_t dim = 512;
    std::vector<float> vec1(dim, 1.0f);
    std::vector<float> vec2(dim, 1.0f);

    auto distance = wrapper.computeDistance<float>(vec1, vec2, DistanceMetric::L2);
    ASSERT_TRUE(distance.has_value());
    EXPECT_NEAR(*distance, 0.0f, 1e-4) << "L2 distance should be 0 with SIMD";
}

// Test metadata handling
TEST_F(SqliteVecWrapperTest, MetadataHandling) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    VectorMetadata meta;
    meta.id = "test_vector_001";
    meta.dimension = 4;
    meta.metric = DistanceMetric::Cosine;
    meta.timestamp = std::chrono::system_clock::now();
    meta.tags = {"test", "unit_test", "wrapper"};

    std::vector<float> vec = {1.0f, 0.0f, 0.0f, 0.0f};

    EXPECT_TRUE(wrapper.addVectorWithMetadata(vec, meta));

    auto retrieved_meta = wrapper.getVectorMetadata("test_vector_001");
    ASSERT_TRUE(retrieved_meta.has_value());
    EXPECT_EQ(retrieved_meta->id, "test_vector_001");
    EXPECT_EQ(retrieved_meta->dimension, 4);
    EXPECT_EQ(retrieved_meta->tags.size(), 3);
}

// Test vector serialization/deserialization
TEST_F(SqliteVecWrapperTest, VectorSerialization) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    std::vector<float> original = {1.5f, 2.5f, 3.5f, 4.5f};

    auto serialized = wrapper.serializeVector(original);
    ASSERT_TRUE(serialized.has_value());
    EXPECT_GT(serialized->size(), 0);

    auto deserialized = wrapper.deserializeVector<float>(*serialized);
    ASSERT_TRUE(deserialized.has_value());
    ASSERT_EQ(deserialized->size(), original.size());

    for (size_t i = 0; i < original.size(); ++i) {
        EXPECT_FLOAT_EQ(deserialized->at(i), original[i]);
    }
}

// Test quantization support
TEST_F(SqliteVecWrapperTest, VectorQuantization) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    if (!wrapper.supportsQuantization()) {
        GTEST_SKIP() << "Quantization not supported";
    }

    std::vector<float> vec = {1.5f, 2.5f, 3.5f, 4.5f};

    auto quantized = wrapper.quantizeVector(vec, QuantizationType::Int8);
    ASSERT_TRUE(quantized.has_value());

    auto dequantized = wrapper.dequantizeVector<float>(*quantized, QuantizationType::Int8);
    ASSERT_TRUE(dequantized.has_value());

    // Quantization introduces small errors
    for (size_t i = 0; i < vec.size(); ++i) {
        EXPECT_NEAR(dequantized->at(i), vec[i], 0.1f);
    }
}

// Test thread safety with concurrent operations
TEST_F(SqliteVecWrapperTest, ThreadSafety) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    const int num_threads = 4;
    const int ops_per_thread = 100;

    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < ops_per_thread; ++i) {
                std::vector<float> vec = {static_cast<float>(t), static_cast<float>(i), 0.0f, 0.0f};
                std::vector<float> query = {0.0f, 0.0f, 0.0f, 0.0f};

                auto dist = wrapper.computeDistance<float>(vec, query, DistanceMetric::L2);
                if (dist.has_value()) {
                    success_count++;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(success_count.load(), num_threads * ops_per_thread)
        << "All concurrent operations should succeed";
}

// Test memory efficiency with large vectors
TEST_F(SqliteVecWrapperTest, LargeVectorHandling) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    // Test with large dimension (e.g., 1536 like OpenAI embeddings)
    const size_t large_dim = 1536;
    std::vector<float> large_vec(large_dim, 0.5f);
    std::vector<float> large_query(large_dim, 0.5f);

    auto distance = wrapper.computeDistance<float>(large_vec, large_query, DistanceMetric::L2);
    ASSERT_TRUE(distance.has_value());
    EXPECT_NEAR(*distance, 0.0f, 1e-3);
}

// Test index statistics
TEST_F(SqliteVecWrapperTest, IndexStatistics) {
    SqliteVecWrapper wrapper;
    ASSERT_TRUE(wrapper.initialize());

    auto stats = wrapper.getIndexStatistics();
    ASSERT_TRUE(stats.has_value());

    EXPECT_GE(stats->total_vectors, 0);
    EXPECT_GE(stats->memory_usage_bytes, 0);
}

// Test cleanup and resource management
TEST_F(SqliteVecWrapperTest, ResourceCleanup) {
    {
        SqliteVecWrapper wrapper;
        ASSERT_TRUE(wrapper.initialize());

        std::vector<float> vec = {1.0f, 2.0f, 3.0f, 4.0f};
        ASSERT_TRUE(wrapper.addVector("cleanup_test", vec));
    }
    // Wrapper destroyed, should clean up resources

    // Create new wrapper to verify state is clean
    SqliteVecWrapper new_wrapper;
    EXPECT_TRUE(new_wrapper.initialize());
}

} // namespace yams::vector::test
