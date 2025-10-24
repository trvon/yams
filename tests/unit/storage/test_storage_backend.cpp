#include <algorithm>
#include <filesystem>
#include <random>
#include <thread>
#include <gtest/gtest.h>
#include <yams/core/types.h>
#include <yams/storage/storage_backend.h>

using namespace yams::storage;

class StorageBackendTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temp directory for tests
        testDir_ = std::filesystem::temp_directory_path() / "yams_storage_test";
        std::filesystem::create_directories(testDir_);
    }

    void TearDown() override {
        // Clean up test directory
        std::filesystem::remove_all(testDir_);
    }

    std::vector<std::byte> generateTestData(size_t size) {
        std::vector<std::byte> data(size);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 255);

        for (auto& byte : data) {
            byte = static_cast<std::byte>(dis(gen));
        }

        return data;
    }

    std::filesystem::path testDir_;
};

// Filesystem Backend Tests
TEST_F(StorageBackendTest, FilesystemBackendInitialize) {
    BackendConfig config;
    config.type = "filesystem";
    config.localPath = testDir_ / "storage";

    auto backend = StorageBackendFactory::create(config);
    ASSERT_NE(backend, nullptr);
    EXPECT_EQ(backend->getType(), "filesystem");
    EXPECT_FALSE(backend->isRemote());
}

TEST_F(StorageBackendTest, FilesystemBackendStoreRetrieve) {
    BackendConfig config;
    config.type = "filesystem";
    config.localPath = testDir_ / "storage";

    auto backend = StorageBackendFactory::create(config);
    ASSERT_NE(backend, nullptr);

    // Test data
    std::string key = "test_object_123";
    auto data = generateTestData(1024);

    // Store data
    auto storeResult = backend->store(key, data);
    ASSERT_TRUE(storeResult);

    // Retrieve data
    auto retrieveResult = backend->retrieve(key);
    ASSERT_TRUE(retrieveResult);
    EXPECT_EQ(retrieveResult.value(), data);
}

TEST_F(StorageBackendTest, FilesystemBackendExists) {
    BackendConfig config;
    config.type = "filesystem";
    config.localPath = testDir_ / "storage";

    auto backend = StorageBackendFactory::create(config);
    ASSERT_NE(backend, nullptr);

    std::string key = "test_exists";
    auto data = generateTestData(256);

    // Check non-existent key
    auto existsResult = backend->exists(key);
    ASSERT_TRUE(existsResult);
    EXPECT_FALSE(existsResult.value());

    // Store and check again
    backend->store(key, data);
    existsResult = backend->exists(key);
    ASSERT_TRUE(existsResult);
    EXPECT_TRUE(existsResult.value());
}

TEST_F(StorageBackendTest, FilesystemBackendRemove) {
    BackendConfig config;
    config.type = "filesystem";
    config.localPath = testDir_ / "storage";

    auto backend = StorageBackendFactory::create(config);
    ASSERT_NE(backend, nullptr);

    std::string key = "test_remove";
    auto data = generateTestData(512);

    // Store data
    backend->store(key, data);
    ASSERT_TRUE(backend->exists(key).value());

    // Remove data
    auto removeResult = backend->remove(key);
    ASSERT_TRUE(removeResult);

    // Verify removed
    auto existsResult = backend->exists(key);
    ASSERT_TRUE(existsResult);
    EXPECT_FALSE(existsResult.value());
}

TEST_F(StorageBackendTest, FilesystemBackendList) {
    BackendConfig config;
    config.type = "filesystem";
    config.localPath = testDir_ / "storage";

    auto backend = StorageBackendFactory::create(config);
    ASSERT_NE(backend, nullptr);

    // Store multiple objects
    std::vector<std::string> keys = {"prefix/object1", "prefix/object2", "prefix/sub/object3",
                                     "other/object4"};

    auto data = generateTestData(128);
    for (const auto& key : keys) {
        backend->store(key, data);
    }

    // List all
    auto listResult = backend->list();
    ASSERT_TRUE(listResult);
    EXPECT_GE(listResult.value().size(), 4u);

    // List with prefix
    listResult = backend->list("prefix");
    ASSERT_TRUE(listResult);
    auto& results = listResult.value();
    EXPECT_EQ(std::count_if(results.begin(), results.end(),
                            [](const auto& s) { return s.starts_with("prefix"); }),
              3u);
}

TEST_F(StorageBackendTest, FilesystemBackendAsyncOperations) {
    BackendConfig config;
    config.type = "filesystem";
    config.localPath = testDir_ / "storage";

    auto backend = StorageBackendFactory::create(config);
    ASSERT_NE(backend, nullptr);

    std::string key = "async_test";
    auto data = generateTestData(2048);

    // Async store
    auto storeFuture = backend->storeAsync(key, data);
    auto storeResult = storeFuture.get();
    ASSERT_TRUE(storeResult);

    // Async retrieve
    auto retrieveFuture = backend->retrieveAsync(key);
    auto retrieveResult = retrieveFuture.get();
    ASSERT_TRUE(retrieveResult);
    EXPECT_EQ(retrieveResult.value(), data);
}

TEST_F(StorageBackendTest, FilesystemBackendLargeFile) {
    BackendConfig config;
    config.type = "filesystem";
    config.localPath = testDir_ / "storage";

    auto backend = StorageBackendFactory::create(config);
    ASSERT_NE(backend, nullptr);

    // Test with 10MB file
    std::string key = "large_file";
    auto data = generateTestData(10 * 1024 * 1024);

    auto storeResult = backend->store(key, data);
    ASSERT_TRUE(storeResult);

    auto retrieveResult = backend->retrieve(key);
    ASSERT_TRUE(retrieveResult);
    EXPECT_EQ(retrieveResult.value(), data);
}

TEST_F(StorageBackendTest, FilesystemBackendConcurrency) {
    BackendConfig config;
    config.type = "filesystem";
    config.localPath = testDir_ / "storage";

    auto backend = StorageBackendFactory::create(config);
    ASSERT_NE(backend, nullptr);

    const int numThreads = 10;
    const int numOperations = 100;
    std::vector<std::thread> threads;

    // Concurrent writes
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < numOperations; ++i) {
                std::string key = "thread_" + std::to_string(t) + "_" + std::to_string(i);
                auto data = generateTestData(256);
                backend->store(key, data);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all objects exist
    for (int t = 0; t < numThreads; ++t) {
        for (int i = 0; i < numOperations; ++i) {
            std::string key = "thread_" + std::to_string(t) + "_" + std::to_string(i);
            auto existsResult = backend->exists(key);
            ASSERT_TRUE(existsResult);
            EXPECT_TRUE(existsResult.value());
        }
    }
}

// URL Backend Factory Tests
TEST_F(StorageBackendTest, ParseURLLocalPath) {
    auto config = StorageBackendFactory::parseURL("/path/to/storage");
    EXPECT_EQ(config.type, "filesystem");
    EXPECT_EQ(config.localPath, "/path/to/storage");
}

TEST_F(StorageBackendTest, ParseURLFileScheme) {
    auto config = StorageBackendFactory::parseURL("file:///path/to/storage");
    EXPECT_EQ(config.type, "filesystem");
    EXPECT_EQ(config.localPath, "/path/to/storage");
}

TEST_F(StorageBackendTest, ParseURLS3Scheme) {
    auto config = StorageBackendFactory::parseURL("s3://bucket/prefix/path");
    EXPECT_EQ(config.type, "s3");
    EXPECT_EQ(config.url, "s3://bucket/prefix/path");
}

TEST_F(StorageBackendTest, ParseURLHTTPScheme) {
    auto config = StorageBackendFactory::parseURL("http://example.com/storage");
    EXPECT_EQ(config.type, "http");
    EXPECT_EQ(config.url, "http://example.com/storage");
}

TEST_F(StorageBackendTest, ParseURLWithQueryParams) {
    auto config = StorageBackendFactory::parseURL(
        "s3://bucket/path?cache_size=1048576&cache_ttl=7200&timeout=60");
    EXPECT_EQ(config.type, "s3");
    EXPECT_EQ(config.cacheSize, 1048576u);
    EXPECT_EQ(config.cacheTTL, 7200u);
    EXPECT_EQ(config.requestTimeout, 60u);
}

// Storage Stats Test
TEST_F(StorageBackendTest, FilesystemBackendStats) {
    BackendConfig config;
    config.type = "filesystem";
    config.localPath = testDir_ / "storage";

    auto backend = StorageBackendFactory::create(config);
    ASSERT_NE(backend, nullptr);

    // Store some objects
    for (int i = 0; i < 5; ++i) {
        std::string key = "stats_test_" + std::to_string(i);
        auto data = generateTestData(1024 * (i + 1));
        backend->store(key, data);
    }

    // Get stats
    auto statsResult = backend->getStats();
    ASSERT_TRUE(statsResult);

    auto& stats = statsResult.value();
    EXPECT_EQ(stats.totalObjects, 5u);
    EXPECT_EQ(stats.totalBytes, 1024u + 2048u + 3072u + 4096u + 5120u);
}

// Edge Cases
TEST_F(StorageBackendTest, FilesystemBackendEmptyData) {
    BackendConfig config;
    config.type = "filesystem";
    config.localPath = testDir_ / "storage";

    auto backend = StorageBackendFactory::create(config);
    ASSERT_NE(backend, nullptr);

    std::string key = "empty_data";
    std::vector<std::byte> emptyData;

    auto storeResult = backend->store(key, emptyData);
    ASSERT_TRUE(storeResult);

    auto retrieveResult = backend->retrieve(key);
    ASSERT_TRUE(retrieveResult);
    EXPECT_TRUE(retrieveResult.value().empty());
}

TEST_F(StorageBackendTest, FilesystemBackendSpecialCharacters) {
    BackendConfig config;
    config.type = "filesystem";
    config.localPath = testDir_ / "storage";

    auto backend = StorageBackendFactory::create(config);
    ASSERT_NE(backend, nullptr);

    // Test keys with special characters (but valid for filesystem)
    std::vector<std::string> keys = {"test-with-dash", "test_with_underscore", "test.with.dots",
                                     "test123numbers"};

    auto data = generateTestData(256);
    for (const auto& key : keys) {
        auto storeResult = backend->store(key, data);
        ASSERT_TRUE(storeResult) << "Failed to store key: " << key;

        auto retrieveResult = backend->retrieve(key);
        ASSERT_TRUE(retrieveResult) << "Failed to retrieve key: " << key;
        EXPECT_EQ(retrieveResult.value(), data);
    }
}

TEST_F(StorageBackendTest, FilesystemBackendNonExistentKey) {
    BackendConfig config;
    config.type = "filesystem";
    config.localPath = testDir_ / "storage";

    auto backend = StorageBackendFactory::create(config);
    ASSERT_NE(backend, nullptr);

    auto retrieveResult = backend->retrieve("non_existent_key");
    ASSERT_FALSE(retrieveResult);
    EXPECT_EQ(retrieveResult.error().code, yams::ErrorCode::ChunkNotFound);
}

TEST_F(StorageBackendTest, FilesystemBackendFlush) {
    BackendConfig config;
    config.type = "filesystem";
    config.localPath = testDir_ / "storage";

    auto backend = StorageBackendFactory::create(config);
    ASSERT_NE(backend, nullptr);

    // Flush should always succeed for filesystem backend
    auto flushResult = backend->flush();
    ASSERT_TRUE(flushResult);
}
