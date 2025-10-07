// PBI-040: Minimal vector smoke test (conditional on sqlite-vec availability)
// Tests basic vector DB initialization, insert, and search without heavy load.

#include <cstdlib>
#include <filesystem>
#include <gtest/gtest.h>
#include <yams/vector/vector_database.h>

namespace yams::vector {

class VectorSmokeTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Check if sqlite-vec is available by attempting to load it
        // If YAMS_SQLITE_VEC_SKIP_INIT=1 or YAMS_DISABLE_VECTORS=1, skip these tests
        if (const char* skip_env = std::getenv("YAMS_SQLITE_VEC_SKIP_INIT")) {
            std::string v(skip_env);
            if (v == "1" || v == "true") {
                GTEST_SKIP() << "Skipping vector smoke test (YAMS_SQLITE_VEC_SKIP_INIT=1)";
            }
        }
        if (const char* disable_env = std::getenv("YAMS_DISABLE_VECTORS")) {
            std::string v(disable_env);
            if (v == "1" || v == "true") {
                GTEST_SKIP() << "Skipping vector smoke test (YAMS_DISABLE_VECTORS=1)";
            }
        }
    }

    void TearDown() override {
        // Cleanup is automatic with in-memory DB
    }
};

TEST_F(VectorSmokeTest, InitializeInMemory) {
    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 128;
    config.create_if_missing = true;
    config.use_in_memory = true;

    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize()) << "Failed to initialize in-memory vector DB";
}

TEST_F(VectorSmokeTest, InsertAndSearchBasic) {
    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4; // Small dimension for speed
    config.create_if_missing = true;
    config.use_in_memory = true;

    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    // Insert a simple vector
    std::vector<float> embedding = {1.0f, 0.0f, 0.0f, 0.0f};
    ASSERT_TRUE(db.addVector("test_hash_001", embedding));

    // Search for it
    auto results = db.search(embedding, 1);
    ASSERT_TRUE(results.has_value());
    ASSERT_EQ(results->size(), 1);
    EXPECT_EQ(results->at(0).hash, "test_hash_001");
}

TEST_F(VectorSmokeTest, GetVectorCount) {
    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = 4;
    config.create_if_missing = true;
    config.use_in_memory = true;

    VectorDatabase db(config);
    ASSERT_TRUE(db.initialize());

    auto count = db.getVectorCount();
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(*count, 0);

    // Add a vector
    std::vector<float> embedding = {1.0f, 0.0f, 0.0f, 0.0f};
    ASSERT_TRUE(db.addVector("test_hash_002", embedding));

    count = db.getVectorCount();
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(*count, 1);
}

} // namespace yams::vector
