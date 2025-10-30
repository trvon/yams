/**
 * @file crypto_test.cpp
 * @brief Comprehensive cryptographic hash tests for SHA-256 implementation
 *
 * Migrated from GTest to Catch2 as part of PBI-068.
 * Replaces sha256_hasher_test.cpp (183 LOC, 11 tests).
 *
 * Test Coverage:
 * - Basic hashing: empty input, known test vectors
 * - Streaming updates: chunked vs single update
 * - File operations: sync and async hashing
 * - Progress callbacks
 * - Error handling: non-existent files
 * - Performance: large file throughput (release builds only)
 */

#include <catch2/catch_test_macros.hpp>
#include <yams/core/span.h>
#include <yams/crypto/hasher.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace yams;
using namespace yams::crypto;

// ============================================================================
// Test Helpers
// ============================================================================

namespace {

/**
 * @brief Create a temporary directory for test files
 */
std::filesystem::path makeTempDir(std::string_view prefix = "yams_crypto_test_") {
    const auto base = std::filesystem::temp_directory_path();
    std::uniform_int_distribution<int> dist(0, 9999);
    thread_local std::mt19937_64 rng{std::random_device{}()};
    for (int attempt = 0; attempt < 512; ++attempt) {
        const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        auto candidate =
            base / (std::string(prefix) + std::to_string(stamp) + "_" + std::to_string(dist(rng)));
        std::error_code ec;
        if (std::filesystem::create_directories(candidate, ec)) {
            return candidate;
        }
    }
    return base;
}

/**
 * @brief Generate random bytes for testing
 */
std::vector<std::byte> generateRandomBytes(std::size_t size) {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    std::vector<std::byte> data(size);
    std::uniform_int_distribution<int> dist(0, 255);
    std::generate(data.begin(), data.end(), [&] { return std::byte(dist(rng)); });
    return data;
}

/**
 * @brief Create a test file with specific content
 */
std::filesystem::path createTestFile(const std::filesystem::path& dir, std::string_view name,
                                     const std::vector<std::byte>& content) {
    auto path = dir / name;
    std::filesystem::create_directories(path.parent_path());
    std::ofstream stream(path, std::ios::binary);
    stream.write(reinterpret_cast<const char*>(content.data()),
                 static_cast<std::streamsize>(content.size()));
    stream.close();
    return path;
}

/**
 * @brief Test vectors for known SHA-256 hashes
 */
struct TestVectors {
    static constexpr std::string_view EMPTY_SHA256 =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    static constexpr std::string_view ABC_SHA256 =
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    static constexpr std::string_view HELLO_WORLD_SHA256 =
        "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e";

    static std::vector<std::byte> getABCData() {
        return {std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
    }

    static std::vector<std::byte> getHelloWorldData() {
        const char* str = "Hello World";
        return std::vector<std::byte>(reinterpret_cast<const std::byte*>(str),
                                      reinterpret_cast<const std::byte*>(str + 11));
    }
};

/**
 * @brief RAII fixture for test directory management
 */
class CryptoTestFixture {
public:
    CryptoTestFixture() : testDir(makeTempDir()), hasher(std::make_unique<SHA256Hasher>()) {}

    ~CryptoTestFixture() {
        std::error_code ec;
        std::filesystem::remove_all(testDir, ec);
    }

    std::filesystem::path testDir;
    std::unique_ptr<SHA256Hasher> hasher;
};

} // namespace

// ============================================================================
// SHA-256 Hasher Tests
// ============================================================================

TEST_CASE("SHA256Hasher - Basic hashing", "[crypto][sha256]") {
    CryptoTestFixture fixture;

    SECTION("Empty input produces known hash") {
        fixture.hasher->init();
        auto hash = fixture.hasher->finalize();
        REQUIRE(hash == TestVectors::EMPTY_SHA256);
    }

    SECTION("Known test vector: 'abc'") {
        auto data = TestVectors::getABCData();
        fixture.hasher->init();
        fixture.hasher->update(std::span<const std::byte>(data.data(), data.size()));
        auto hash = fixture.hasher->finalize();
        REQUIRE(hash == TestVectors::ABC_SHA256);
    }

    SECTION("Known test vector: 'Hello World'") {
        auto data = TestVectors::getHelloWorldData();
        fixture.hasher->init();
        fixture.hasher->update(std::span<const std::byte>(data.data(), data.size()));
        auto hash = fixture.hasher->finalize();
        REQUIRE(hash == TestVectors::HELLO_WORLD_SHA256);
    }

    SECTION("Static hash method") {
        auto data = TestVectors::getABCData();
        auto hash = SHA256Hasher::hash(std::span<const std::byte>(data.data(), data.size()));
        REQUIRE(hash == TestVectors::ABC_SHA256);
    }
}

TEST_CASE("SHA256Hasher - Large input handling", "[crypto][sha256]") {
    CryptoTestFixture fixture;

    SECTION("1MB of random data") {
        auto data = generateRandomBytes(1024 * 1024);

        fixture.hasher->init();
        fixture.hasher->update(std::span<const std::byte>(data.data(), data.size()));
        auto hash1 = fixture.hasher->finalize();

        // Hash again to verify consistency
        fixture.hasher->init();
        fixture.hasher->update(std::span<const std::byte>(data.data(), data.size()));
        auto hash2 = fixture.hasher->finalize();

        REQUIRE(hash1 == hash2);
        REQUIRE(hash1.size() == 64u); // SHA-256 hex string length
    }
}

TEST_CASE("SHA256Hasher - Streaming updates", "[crypto][sha256]") {
    CryptoTestFixture fixture;
    auto data = generateRandomBytes(1000);

    SECTION("Chunked hashing matches single-pass hashing") {
        // Hash data in chunks
        fixture.hasher->init();
        fixture.hasher->update(std::span<const std::byte>(data.data(), 100));
        fixture.hasher->update(std::span<const std::byte>(data.data() + 100, 400));
        fixture.hasher->update(std::span<const std::byte>(data.data() + 500, 500));
        auto hash1 = fixture.hasher->finalize();

        // Hash all at once
        fixture.hasher->init();
        fixture.hasher->update(std::span<const std::byte>(data.data(), data.size()));
        auto hash2 = fixture.hasher->finalize();

        REQUIRE(hash1 == hash2);
    }
}

TEST_CASE("SHA256Hasher - File operations", "[crypto][sha256][file]") {
    CryptoTestFixture fixture;

    SECTION("Synchronous file hashing") {
        auto content = generateRandomBytes(10000);
        auto path = createTestFile(fixture.testDir, "test.bin", content);

        // Hash file
        auto fileHash = fixture.hasher->hashFile(path);

        // Hash content directly
        fixture.hasher->init();
        fixture.hasher->update(std::span{content});
        auto contentHash = fixture.hasher->finalize();

        REQUIRE(fileHash == contentHash);
    }

    SECTION("Generic hash method with file") {
        std::vector<std::byte> vec = TestVectors::getABCData();
        auto path = createTestFile(fixture.testDir, "generic_test.bin", vec);
        auto hash = fixture.hasher->hashFile(path);
        REQUIRE(hash == TestVectors::ABC_SHA256);
    }

    SECTION("Generic hash method with vector") {
        std::vector<std::byte> vec = TestVectors::getABCData();
        auto hash = fixture.hasher->hash(vec);
        REQUIRE(hash == TestVectors::ABC_SHA256);
    }
}

TEST_CASE("SHA256Hasher - Async file hashing", "[crypto][sha256][async]") {
    CryptoTestFixture fixture;

    SECTION("Async hashing matches sync hashing") {
        auto content = generateRandomBytes(50000);
        auto path = createTestFile(fixture.testDir, "async_test.bin", content);

        // Hash asynchronously
        auto future = fixture.hasher->hashFileAsync(path);
        auto result = future.get();

        REQUIRE(result.has_value());

        // Verify against sync hash
        auto syncHash = fixture.hasher->hashFile(path);
        REQUIRE(result.value() == syncHash);
    }
}

TEST_CASE("SHA256Hasher - Progress callback", "[crypto][sha256][callback]") {
    CryptoTestFixture fixture;

    SECTION("Progress is reported during hashing") {
        auto content = generateRandomBytes(1024 * 1024); // 1MB
        auto path = createTestFile(fixture.testDir, "progress_test.bin", content);

        // Track progress
        std::vector<std::pair<uint64_t, uint64_t>> progressUpdates;
        fixture.hasher->setProgressCallback([&](uint64_t current, uint64_t total) {
            progressUpdates.emplace_back(current, total);
        });

        fixture.hasher->hashFile(path);

        // Verify progress was reported
        REQUIRE_FALSE(progressUpdates.empty());

        // Verify final progress equals file size
        auto [finalCurrent, finalTotal] = progressUpdates.back();
        REQUIRE(finalCurrent == content.size());
        REQUIRE(finalTotal == content.size());
    }
}

TEST_CASE("SHA256Hasher - Error handling", "[crypto][sha256][error]") {
    CryptoTestFixture fixture;

    SECTION("Non-existent file returns error") {
        auto future = fixture.hasher->hashFileAsync("/non/existent/file");
        auto result = future.get();

        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().code == ErrorCode::FileNotFound);
    }
}

#ifdef NDEBUG
TEST_CASE("SHA256Hasher - Performance", "[crypto][sha256][performance][!benchmark]") {
    CryptoTestFixture fixture;

    SECTION("100MB file hashing throughput") {
        constexpr size_t fileSize = 100 * 1024 * 1024;
        auto content = generateRandomBytes(fileSize);
        auto path = createTestFile(fixture.testDir, "perf_test.bin", content);

        auto start = std::chrono::high_resolution_clock::now();
        auto hash = fixture.hasher->hashFile(path);
        auto end = std::chrono::high_resolution_clock::now();

        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        auto throughput = (fileSize / 1024.0 / 1024.0) / (duration.count() / 1000.0);

        INFO("Hashed " << (fileSize / 1024 / 1024) << "MB in " << duration.count() << "ms ("
                       << throughput << "MB/s)");

        // Expect at least 100MB/s
        REQUIRE(throughput > 100.0);
    }
}
#endif
