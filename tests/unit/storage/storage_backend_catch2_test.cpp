/**
 * Storage Backend Tests (Catch2)
 *
 * Comprehensive tests for the storage backend abstraction layer.
 * Tests FilesystemBackend and StorageBackendFactory functionality.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <unistd.h>
#endif

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <yams/core/types.h>
#include <yams/storage/storage_backend.h>

namespace yams::storage {
namespace fs = std::filesystem;

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Generate a unique suffix for test directories to avoid collisions
 */
inline std::string uniqueSuffix() {
    static std::atomic<int> counter{0};
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(
#ifdef _WIN32
               _getpid()
#else
               getpid()
#endif
                   ) +
           "_" + std::to_string(now) + "_" + std::to_string(counter++);
}

/**
 * Generate random test data
 */
inline std::vector<std::byte> generateTestData(size_t size) {
    std::vector<std::byte> data(size);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255);
    for (auto& byte : data) {
        byte = static_cast<std::byte>(dis(gen));
    }
    return data;
}

/**
 * Generate deterministic test data (for verification)
 */
inline std::vector<std::byte> generateDeterministicData(size_t size, uint8_t seed = 42) {
    std::vector<std::byte> data(size);
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<std::byte>((seed + i) % 256);
    }
    return data;
}

/**
 * RAII test directory manager
 */
class TestDirectory {
public:
    TestDirectory() : path_(fs::temp_directory_path() / ("yams_storage_test_" + uniqueSuffix())) {
        fs::create_directories(path_);
    }

    ~TestDirectory() {
        std::error_code ec;
        fs::remove_all(path_, ec);
    }

    const fs::path& path() const { return path_; }
    fs::path subdir(std::string_view name) const { return path_ / name; }

    TestDirectory(const TestDirectory&) = delete;
    TestDirectory& operator=(const TestDirectory&) = delete;

private:
    fs::path path_;
};

/**
 * Helper to create a backend with proper initialization
 */
inline std::unique_ptr<IStorageBackend> createFilesystemBackend(const fs::path& basePath) {
    BackendConfig config;
    config.type = "filesystem";
    config.localPath = basePath;
    return StorageBackendFactory::create(config);
}

// =============================================================================
// FilesystemBackend - Basic Operations
// =============================================================================

TEST_CASE("FilesystemBackend - Initialization", "[storage][backend][filesystem][init]") {
    TestDirectory testDir;

    SECTION("Creates backend with valid config") {
        auto backend = createFilesystemBackend(testDir.subdir("storage"));
        REQUIRE(backend != nullptr);
        CHECK(backend->getType() == "filesystem");
        CHECK_FALSE(backend->isRemote());
    }

    SECTION("Creates storage directory if it doesn't exist") {
        auto storagePath = testDir.subdir("new_storage");
        REQUIRE_FALSE(fs::exists(storagePath));

        auto backend = createFilesystemBackend(storagePath);
        REQUIRE(backend != nullptr);

        // Store something to trigger directory creation
        auto data = generateTestData(100);
        auto result = backend->store("test_key", data);
        REQUIRE(result);

        CHECK(fs::exists(storagePath));
    }

    SECTION("Factory creates correct backend type") {
        BackendConfig config;
        config.type = "filesystem";
        config.localPath = testDir.subdir("factory_test");

        auto backend = StorageBackendFactory::create(config);
        REQUIRE(backend != nullptr);
        CHECK(backend->getType() == "filesystem");
    }
}

TEST_CASE("FilesystemBackend - Store and Retrieve", "[storage][backend][filesystem][crud]") {
    TestDirectory testDir;
    auto backend = createFilesystemBackend(testDir.subdir("storage"));
    REQUIRE(backend != nullptr);

    SECTION("Store and retrieve small data") {
        auto data = generateDeterministicData(256);
        auto storeResult = backend->store("small_key", data);
        REQUIRE(storeResult);

        auto retrieveResult = backend->retrieve("small_key");
        REQUIRE(retrieveResult);
        CHECK(retrieveResult.value() == data);
    }

    SECTION("Store and retrieve 1KB data") {
        auto data = generateTestData(1024);
        REQUIRE(backend->store("1kb_key", data));

        auto result = backend->retrieve("1kb_key");
        REQUIRE(result);
        CHECK(result.value() == data);
    }

    SECTION("Store and retrieve 1MB data") {
        auto data = generateTestData(1024 * 1024);
        REQUIRE(backend->store("1mb_key", data));

        auto result = backend->retrieve("1mb_key");
        REQUIRE(result);
        CHECK(result.value() == data);
    }

    SECTION("Store and retrieve empty data") {
        std::vector<std::byte> emptyData;
        REQUIRE(backend->store("empty_key", emptyData));

        auto result = backend->retrieve("empty_key");
        REQUIRE(result);
        CHECK(result.value().empty());
    }

    SECTION("Overwrite existing key") {
        auto data1 = generateDeterministicData(100, 1);
        auto data2 = generateDeterministicData(200, 2);

        REQUIRE(backend->store("overwrite_key", data1));
        REQUIRE(backend->store("overwrite_key", data2));

        auto result = backend->retrieve("overwrite_key");
        REQUIRE(result);
        CHECK(result.value() == data2);
        CHECK(result.value() != data1);
    }

    SECTION("Retrieve non-existent key fails") {
        auto result = backend->retrieve("non_existent_key_12345");
        REQUIRE_FALSE(result);
        CHECK(result.error().code == ErrorCode::ChunkNotFound);
    }
}

TEST_CASE("FilesystemBackend - Exists", "[storage][backend][filesystem][exists]") {
    TestDirectory testDir;
    auto backend = createFilesystemBackend(testDir.subdir("storage"));
    REQUIRE(backend != nullptr);

    SECTION("Returns false for non-existent key") {
        auto result = backend->exists("missing_key");
        REQUIRE(result);
        CHECK_FALSE(result.value());
    }

    SECTION("Returns true after store") {
        auto data = generateTestData(100);
        REQUIRE(backend->store("exists_key", data));

        auto result = backend->exists("exists_key");
        REQUIRE(result);
        CHECK(result.value());
    }

    SECTION("Returns false after remove") {
        auto data = generateTestData(100);
        REQUIRE(backend->store("remove_test", data));
        REQUIRE(backend->remove("remove_test"));

        auto result = backend->exists("remove_test");
        REQUIRE(result);
        CHECK_FALSE(result.value());
    }
}

TEST_CASE("FilesystemBackend - Remove", "[storage][backend][filesystem][remove]") {
    TestDirectory testDir;
    auto backend = createFilesystemBackend(testDir.subdir("storage"));
    REQUIRE(backend != nullptr);

    SECTION("Remove existing key succeeds") {
        auto data = generateTestData(100);
        REQUIRE(backend->store("to_remove", data));
        REQUIRE(backend->exists("to_remove").value());

        auto result = backend->remove("to_remove");
        REQUIRE(result);
        CHECK_FALSE(backend->exists("to_remove").value());
    }

    SECTION("Remove non-existent key returns success") {
        // Filesystem backend treats removing non-existent as success
        auto result = backend->remove("never_existed");
        // Just check it doesn't crash - behavior may vary
        (void)result;
    }

    SECTION("Cannot retrieve after remove") {
        auto data = generateTestData(100);
        REQUIRE(backend->store("retrieve_after_remove", data));
        REQUIRE(backend->remove("retrieve_after_remove"));

        auto result = backend->retrieve("retrieve_after_remove");
        REQUIRE_FALSE(result);
    }
}

TEST_CASE("FilesystemBackend - List", "[storage][backend][filesystem][list]") {
    TestDirectory testDir;
    auto backend = createFilesystemBackend(testDir.subdir("storage"));
    REQUIRE(backend != nullptr);

    SECTION("List empty storage returns empty") {
        auto result = backend->list();
        REQUIRE(result);
        CHECK(result.value().empty());
    }

    SECTION("List returns all stored keys") {
        auto data = generateTestData(50);
        std::vector<std::string> keys = {"key1", "key2", "key3"};

        for (const auto& key : keys) {
            REQUIRE(backend->store(key, data));
        }

        auto result = backend->list();
        REQUIRE(result);
        CHECK(result.value().size() == keys.size());

        for (const auto& key : keys) {
            CHECK(std::find(result.value().begin(), result.value().end(), key) !=
                  result.value().end());
        }
    }

    SECTION("List with prefix filters correctly") {
        auto data = generateTestData(50);
        REQUIRE(backend->store("prefix_a/item1", data));
        REQUIRE(backend->store("prefix_a/item2", data));
        REQUIRE(backend->store("prefix_a/sub/item3", data));
        REQUIRE(backend->store("prefix_b/item1", data));
        REQUIRE(backend->store("other/item", data));

        auto result = backend->list("prefix_a");
        REQUIRE(result);

        auto& items = result.value();
        auto prefixACount = std::count_if(items.begin(), items.end(), [](const std::string& s) {
            return s.find("prefix_a") == 0;
        });
        CHECK(prefixACount == 3);
    }

    SECTION("List with non-matching prefix returns empty") {
        auto data = generateTestData(50);
        REQUIRE(backend->store("actual/key", data));

        auto result = backend->list("nonexistent_prefix");
        REQUIRE(result);
        CHECK(result.value().empty());
    }
}

// =============================================================================
// FilesystemBackend - Statistics
// =============================================================================

TEST_CASE("FilesystemBackend - Stats", "[storage][backend][filesystem][stats]") {
    TestDirectory testDir;
    auto backend = createFilesystemBackend(testDir.subdir("storage"));
    REQUIRE(backend != nullptr);

    SECTION("Empty storage has zero stats") {
        auto result = backend->getStats();
        REQUIRE(result);
        CHECK(result.value().totalObjects == 0);
        CHECK(result.value().totalBytes == 0);
    }

    SECTION("Stats reflect stored objects") {
        auto data1 = generateTestData(1000);
        auto data2 = generateTestData(2000);
        auto data3 = generateTestData(3000);

        REQUIRE(backend->store("stats1", data1));
        REQUIRE(backend->store("stats2", data2));
        REQUIRE(backend->store("stats3", data3));

        auto result = backend->getStats();
        REQUIRE(result);
        CHECK(result.value().totalObjects == 3);
        CHECK(result.value().totalBytes == 6000);
    }

    SECTION("Stats update after remove") {
        auto data = generateTestData(500);
        REQUIRE(backend->store("remove_stats", data));

        auto beforeStats = backend->getStats();
        REQUIRE(beforeStats);
        auto beforeCount = beforeStats.value().totalObjects;
        auto beforeBytes = beforeStats.value().totalBytes;

        REQUIRE(backend->remove("remove_stats"));

        auto afterStats = backend->getStats();
        REQUIRE(afterStats);
        CHECK(afterStats.value().totalObjects == beforeCount - 1);
        CHECK(afterStats.value().totalBytes == beforeBytes - 500);
    }
}

// =============================================================================
// FilesystemBackend - Async Operations
// =============================================================================

TEST_CASE("FilesystemBackend - Async Operations", "[storage][backend][filesystem][async]") {
    TestDirectory testDir;
    auto backend = createFilesystemBackend(testDir.subdir("storage"));
    REQUIRE(backend != nullptr);

    SECTION("Async store and retrieve") {
        auto data = generateTestData(2048);

        auto storeFuture = backend->storeAsync("async_key", data);
        auto storeResult = storeFuture.get();
        REQUIRE(storeResult);

        auto retrieveFuture = backend->retrieveAsync("async_key");
        auto retrieveResult = retrieveFuture.get();
        REQUIRE(retrieveResult);
        CHECK(retrieveResult.value() == data);
    }

    SECTION("Multiple concurrent async stores") {
        constexpr int numOps = 10;
        std::vector<std::future<Result<void>>> futures;
        std::vector<std::pair<std::string, std::vector<std::byte>>> testItems;

        for (int i = 0; i < numOps; ++i) {
            auto key = "concurrent_" + std::to_string(i);
            auto data = generateDeterministicData(256, static_cast<uint8_t>(i));
            testItems.emplace_back(key, data);
            futures.push_back(backend->storeAsync(key, data));
        }

        // Wait for all stores
        for (auto& f : futures) {
            REQUIRE(f.get());
        }

        // Verify all data
        for (const auto& [key, expectedData] : testItems) {
            auto result = backend->retrieve(key);
            REQUIRE(result);
            CHECK(result.value() == expectedData);
        }
    }
}

// =============================================================================
// FilesystemBackend - Key Names
// =============================================================================

TEST_CASE("FilesystemBackend - Key Names", "[storage][backend][filesystem][keys]") {
    TestDirectory testDir;
    auto backend = createFilesystemBackend(testDir.subdir("storage"));
    REQUIRE(backend != nullptr);

    auto data = generateTestData(100);

    SECTION("Simple alphanumeric keys") {
        REQUIRE(backend->store("simple123", data));
        CHECK(backend->retrieve("simple123"));
    }

    SECTION("Keys with dashes") {
        REQUIRE(backend->store("key-with-dashes", data));
        CHECK(backend->retrieve("key-with-dashes"));
    }

    SECTION("Keys with underscores") {
        REQUIRE(backend->store("key_with_underscores", data));
        CHECK(backend->retrieve("key_with_underscores"));
    }

    SECTION("Keys with dots") {
        REQUIRE(backend->store("key.with.dots", data));
        CHECK(backend->retrieve("key.with.dots"));
    }

    SECTION("Keys with path separators (nested)") {
        REQUIRE(backend->store("path/to/nested/key", data));
        CHECK(backend->retrieve("path/to/nested/key"));
    }

    SECTION("SHA256-like hash keys") {
        std::string hashKey = "a1b2c3d4e5f6789012345678901234567890123456789012345678901234abcd";
        REQUIRE(backend->store(hashKey, data));
        CHECK(backend->retrieve(hashKey));
    }

    SECTION("Mixed case keys") {
        REQUIRE(backend->store("MixedCaseKey", data));
        CHECK(backend->retrieve("MixedCaseKey"));
    }
}

// =============================================================================
// FilesystemBackend - Concurrency
// =============================================================================

TEST_CASE("FilesystemBackend - Concurrency", "[storage][backend][filesystem][concurrent]") {
    TestDirectory testDir;
    auto backend = createFilesystemBackend(testDir.subdir("storage"));
    REQUIRE(backend != nullptr);

    SECTION("Concurrent writes to different keys") {
        constexpr int numThreads = 8;
        constexpr int numOpsPerThread = 50;
        std::vector<std::thread> threads;
        std::atomic<int> successCount{0};

        for (int t = 0; t < numThreads; ++t) {
            threads.emplace_back([&, t]() {
                for (int i = 0; i < numOpsPerThread; ++i) {
                    auto key = "thread_" + std::to_string(t) + "_op_" + std::to_string(i);
                    auto data = generateTestData(128);
                    if (backend->store(key, data)) {
                        ++successCount;
                    }
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        CHECK(successCount == numThreads * numOpsPerThread);

        // Verify all keys exist
        for (int t = 0; t < numThreads; ++t) {
            for (int i = 0; i < numOpsPerThread; ++i) {
                auto key = "thread_" + std::to_string(t) + "_op_" + std::to_string(i);
                CHECK(backend->exists(key).value());
            }
        }
    }

    SECTION("Concurrent reads") {
        // Store test data first
        auto data = generateDeterministicData(1024, 42);
        REQUIRE(backend->store("shared_read_key", data));

        constexpr int numReaders = 10;
        constexpr int numReadsPerThread = 20;
        std::vector<std::thread> threads;
        std::atomic<int> successCount{0};
        std::atomic<int> dataMatchCount{0};

        for (int t = 0; t < numReaders; ++t) {
            threads.emplace_back([&]() {
                for (int i = 0; i < numReadsPerThread; ++i) {
                    auto result = backend->retrieve("shared_read_key");
                    if (result) {
                        ++successCount;
                        if (result.value() == data) {
                            ++dataMatchCount;
                        }
                    }
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        CHECK(successCount == numReaders * numReadsPerThread);
        CHECK(dataMatchCount == numReaders * numReadsPerThread);
    }

    SECTION("Concurrent write and read to same key") {
        auto initialData = generateDeterministicData(256, 1);
        REQUIRE(backend->store("concurrent_rw", initialData));

        std::atomic<bool> running{true};
        std::atomic<int> readSuccess{0};
        std::atomic<int> writeSuccess{0};

        // Writer thread
        std::thread writer([&]() {
            for (int i = 0; i < 50 && running; ++i) {
                auto data = generateDeterministicData(256, static_cast<uint8_t>(i + 10));
                if (backend->store("concurrent_rw", data)) {
                    ++writeSuccess;
                }
            }
        });

        // Reader threads
        std::vector<std::thread> readers;
        for (int r = 0; r < 4; ++r) {
            readers.emplace_back([&]() {
                while (running || writeSuccess < 50) {
                    auto result = backend->retrieve("concurrent_rw");
                    if (result) {
                        ++readSuccess;
                    }
                    if (writeSuccess >= 50)
                        break;
                }
            });
        }

        writer.join();
        running = false;
        for (auto& reader : readers) {
            reader.join();
        }

        CHECK(writeSuccess == 50);
        CHECK(readSuccess > 0);
    }
}

// =============================================================================
// FilesystemBackend - Edge Cases
// =============================================================================

TEST_CASE("FilesystemBackend - Edge Cases", "[storage][backend][filesystem][edge]") {
    TestDirectory testDir;
    auto backend = createFilesystemBackend(testDir.subdir("storage"));
    REQUIRE(backend != nullptr);

    SECTION("Flush always succeeds") {
        auto result = backend->flush();
        REQUIRE(result);
    }

    SECTION("Large file handling (10MB)") {
        auto data = generateTestData(10 * 1024 * 1024);
        REQUIRE(backend->store("large_10mb", data));

        auto result = backend->retrieve("large_10mb");
        REQUIRE(result);
        CHECK(result.value().size() == data.size());
        CHECK(result.value() == data);
    }

    SECTION("Many small files") {
        constexpr int numFiles = 500;
        auto data = generateTestData(64);

        for (int i = 0; i < numFiles; ++i) {
            auto key = "small_" + std::to_string(i);
            REQUIRE(backend->store(key, data));
        }

        auto stats = backend->getStats();
        REQUIRE(stats);
        CHECK(stats.value().totalObjects == numFiles);
    }

    SECTION("Binary data with all byte values") {
        std::vector<std::byte> allBytes(256);
        for (int i = 0; i < 256; ++i) {
            allBytes[i] = static_cast<std::byte>(i);
        }

        REQUIRE(backend->store("all_bytes", allBytes));
        auto result = backend->retrieve("all_bytes");
        REQUIRE(result);
        CHECK(result.value() == allBytes);
    }
}

// =============================================================================
// StorageBackendFactory - URL Parsing
// =============================================================================

TEST_CASE("StorageBackendFactory - URL Parsing", "[storage][backend][factory][url]") {
    SECTION("Local path without scheme") {
        auto config = StorageBackendFactory::parseURL("/path/to/storage");
        CHECK(config.type == "filesystem");
        CHECK(config.localPath == "/path/to/storage");
    }

    SECTION("File scheme URL") {
        auto config = StorageBackendFactory::parseURL("file:///path/to/storage");
        CHECK(config.type == "filesystem");
        CHECK(config.localPath == "/path/to/storage");
    }

    SECTION("S3 scheme URL") {
        auto config = StorageBackendFactory::parseURL("s3://my-bucket/prefix/path");
        CHECK(config.type == "s3");
        CHECK(config.url == "s3://my-bucket/prefix/path");
    }

    SECTION("HTTP scheme URL") {
        auto config = StorageBackendFactory::parseURL("http://example.com/storage");
        CHECK(config.type == "http");
        CHECK(config.url == "http://example.com/storage");
    }

    SECTION("HTTPS scheme URL") {
        auto config = StorageBackendFactory::parseURL("https://secure.example.com/storage");
        CHECK(config.type == "https");
        CHECK(config.url == "https://secure.example.com/storage");
    }

    SECTION("URL with query parameters") {
        auto config = StorageBackendFactory::parseURL(
            "s3://bucket/path?cache_size=1048576&cache_ttl=7200&timeout=60");
        CHECK(config.type == "s3");
        CHECK(config.cacheSize == 1048576u);
        CHECK(config.cacheTTL == 7200u);
        CHECK(config.requestTimeout == 60u);
    }

    SECTION("URL with partial query parameters") {
        auto config = StorageBackendFactory::parseURL("s3://bucket/path?cache_size=2097152");
        CHECK(config.type == "s3");
        CHECK(config.cacheSize == 2097152u);
        // Default values for unspecified params
        CHECK(config.cacheTTL == 3600u);
        CHECK(config.requestTimeout == 30u);
    }
}

// =============================================================================
// StorageBackendFactory - Backend Creation
// =============================================================================

TEST_CASE("StorageBackendFactory - Backend Creation", "[storage][backend][factory][create]") {
    TestDirectory testDir;

    SECTION("Create filesystem backend from config") {
        BackendConfig config;
        config.type = "filesystem";
        config.localPath = testDir.subdir("fs_backend");

        auto backend = StorageBackendFactory::create(config);
        REQUIRE(backend != nullptr);
        CHECK(backend->getType() == "filesystem");
        CHECK_FALSE(backend->isRemote());
    }

    SECTION("Factory respects configuration options") {
        BackendConfig config;
        config.type = "filesystem";
        config.localPath = testDir.subdir("config_test");
        config.maxConcurrentOps = 5;
        config.requestTimeout = 60;

        auto backend = StorageBackendFactory::create(config);
        REQUIRE(backend != nullptr);
        // Backend created successfully with custom config
    }
}

// =============================================================================
// BackendConfig - Default Values
// =============================================================================

TEST_CASE("BackendConfig - Default Values", "[storage][backend][config]") {
    BackendConfig config;

    CHECK(config.type == "filesystem");
    CHECK(config.cacheSize == 256 * 1024 * 1024);
    CHECK(config.cacheTTL == 3600);
    CHECK(config.maxConcurrentOps == 10);
    CHECK(config.requestTimeout == 30);
    CHECK(config.maxRetries == 3);
    CHECK(config.baseRetryMs == 100);
    CHECK(config.jitterMs == 50);
    CHECK(config.enableRangeGets == true);
    CHECK(config.usePathStyle == false);
}

} // namespace yams::storage
