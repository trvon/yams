// Catch2 tests for ManifestManager
// Migrated from GTest: manifest_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <yams/chunking/chunker.h>
#include <yams/crypto/hasher.h>
#include <yams/manifest/manifest_manager.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>
#include <thread>

using namespace yams;
using namespace yams::manifest;
using namespace yams::chunking;

namespace {

std::filesystem::path make_temp_dir(std::string_view prefix = "yams_manifest_test_") {
    namespace fs = std::filesystem;
    const auto base = fs::temp_directory_path();
    std::uniform_int_distribution<int> dist(0, 9999);
    thread_local std::mt19937_64 rng{std::random_device{}()};
    for (int attempt = 0; attempt < 512; ++attempt) {
        const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        auto candidate =
            base / (std::string(prefix) + std::to_string(stamp) + "_" + std::to_string(dist(rng)));
        std::error_code ec;
        if (fs::create_directories(candidate, ec)) {
            return candidate;
        }
    }
    return base;
}

std::vector<std::byte> generateRandomBytes(std::size_t size) {
    static thread_local std::mt19937_64 engine{std::random_device{}()};
    std::vector<std::byte> data(size);
    std::uniform_int_distribution<int> dist(0, 255);
    std::generate(data.begin(), data.end(), [&] { return std::byte(dist(engine)); });
    return data;
}

struct ManifestTestFixture {
    ManifestTestFixture() {
        testDir = make_temp_dir();

        ManifestManager::Config config{.enableCompression = false, // Disable for predictable tests
                                       .enableChecksums = true,
                                       .enableCaching = true,
                                       .cacheSize = 100};

        manager = std::make_unique<ManifestManager>(std::move(config));
    }

    ~ManifestTestFixture() {
        manager.reset();
        std::error_code ec;
        std::filesystem::remove_all(testDir, ec);
    }

    // Helper to create test chunks
    std::vector<Chunk> createTestChunks(size_t count, size_t chunkSize = 1024) {
        std::vector<Chunk> chunks;
        chunks.reserve(count);

        uint64_t offset = 0;
        auto hasher = crypto::createSHA256Hasher();

        for (size_t i = 0; i < count; ++i) {
            auto data = generateRandomBytes(chunkSize);
            auto hash = hasher->hash(data);

            chunks.emplace_back(
                Chunk{.data = data, .hash = hash, .offset = offset, .size = data.size()});

            offset += data.size();
        }

        return chunks;
    }

    // Helper to create test file info
    FileInfo createTestFileInfo(const std::vector<Chunk>& chunks) {
        uint64_t totalSize = 0;
        std::vector<std::byte> allData;

        for (const auto& chunk : chunks) {
            totalSize += chunk.size;
            allData.insert(allData.end(), chunk.data.begin(), chunk.data.end());
        }

        auto hasher = crypto::createSHA256Hasher();
        auto fileHash = hasher->hash(allData);

        return FileInfo{.hash = fileHash,
                        .size = totalSize,
                        .mimeType = "text/plain",
                        .createdAt = std::chrono::system_clock::now(),
                        .originalName = "test.txt"};
    }

    std::filesystem::path testDir;
    std::unique_ptr<ManifestManager> manager;
};

} // namespace

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager create simple manifest",
                 "[manifest][catch2]") {
    auto chunks = createTestChunks(5, 512);
    auto fileInfo = createTestFileInfo(chunks);

    auto result = manager->createManifest(fileInfo, chunks);
    REQUIRE(result.has_value());

    const auto& manifest = result.value();
    CHECK(manifest.version == Manifest::CURRENT_VERSION);
    CHECK(manifest.fileHash == fileInfo.hash);
    CHECK(manifest.fileSize == fileInfo.size);
    CHECK(manifest.originalName == fileInfo.originalName);
    CHECK(manifest.mimeType == fileInfo.mimeType);
    CHECK(manifest.chunks.size() == chunks.size());

    // Verify chunk mapping
    for (size_t i = 0; i < chunks.size(); ++i) {
        CHECK(manifest.chunks[i].hash == chunks[i].hash);
        CHECK(manifest.chunks[i].offset == chunks[i].offset);
        CHECK(manifest.chunks[i].size == chunks[i].size);
    }

    CHECK(manifest.isValid());
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager create empty manifest fails",
                 "[manifest][catch2]") {
    std::vector<Chunk> emptyChunks;
    FileInfo fileInfo{.hash = "empty",
                      .size = 0,
                      .mimeType = "text/plain",
                      .createdAt = std::chrono::system_clock::now(),
                      .originalName = "empty.txt"};

    auto result = manager->createManifest(fileInfo, emptyChunks);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error() == ErrorCode::InvalidArgument);
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager create large manifest",
                 "[manifest][catch2]") {
    // Test with many chunks
    auto chunks = createTestChunks(1000, 64);
    auto fileInfo = createTestFileInfo(chunks);

    auto result = manager->createManifest(fileInfo, chunks);
    REQUIRE(result.has_value());

    const auto& manifest = result.value();
    CHECK(manifest.chunks.size() == 1000u);
    CHECK(manifest.isValid());

    // Verify total size calculation
    CHECK(manifest.calculateTotalSize() == manifest.fileSize);
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager serialize deserialize roundtrip",
                 "[manifest][catch2]") {
    auto chunks = createTestChunks(10, 256);
    auto fileInfo = createTestFileInfo(chunks);

    // Create manifest
    auto createResult = manager->createManifest(fileInfo, chunks);
    REQUIRE(createResult.has_value());
    const auto& originalManifest = createResult.value();

    // Serialize
    auto serializeResult = manager->serialize(originalManifest);
    REQUIRE(serializeResult.has_value());
    const auto& serializedData = serializeResult.value();

    // Verify serialized size is reasonable
    CHECK(serializedData.size() > 100u); // Should have substantial content
    CHECK(serializedData.size() <
          originalManifest.fileSize); // Should be less than original file size

    // Deserialize
    auto deserializeResult = manager->deserialize(serializedData);
    REQUIRE(deserializeResult.has_value());
    const auto& deserializedManifest = deserializeResult.value();

    // Verify roundtrip accuracy
    CHECK(deserializedManifest.version == originalManifest.version);
    CHECK(deserializedManifest.fileHash == originalManifest.fileHash);
    CHECK(deserializedManifest.fileSize == originalManifest.fileSize);
    CHECK(deserializedManifest.originalName == originalManifest.originalName);
    CHECK(deserializedManifest.mimeType == originalManifest.mimeType);
    CHECK(deserializedManifest.chunks.size() == originalManifest.chunks.size());

    // Verify all chunks match
    for (size_t i = 0; i < originalManifest.chunks.size(); ++i) {
        CHECK(deserializedManifest.chunks[i].hash == originalManifest.chunks[i].hash);
        CHECK(deserializedManifest.chunks[i].offset == originalManifest.chunks[i].offset);
        CHECK(deserializedManifest.chunks[i].size == originalManifest.chunks[i].size);
    }
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager deserialize corrupted data fails",
                 "[manifest][catch2]") {
    std::vector<std::byte> corruptedData{std::byte{0xFF}, std::byte{0xFE}, std::byte{0xFD}};

    auto result = manager->deserialize(corruptedData);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error() == ErrorCode::CorruptedData);
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager validate manifest", "[manifest][catch2]") {
    auto chunks = createTestChunks(5, 512);
    auto fileInfo = createTestFileInfo(chunks);

    auto createResult = manager->createManifest(fileInfo, chunks);
    REQUIRE(createResult.has_value());
    auto manifest = createResult.value();

    // Valid manifest
    auto validationResult = manager->validateManifest(manifest);
    REQUIRE(validationResult.has_value());
    CHECK(validationResult.value());

    // Test invalid manifest - wrong file size
    manifest.fileSize = 12345;
    validationResult = manager->validateManifest(manifest);
    REQUIRE(validationResult.has_value());
    CHECK_FALSE(validationResult.value());
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager validate manifest with gaps",
                 "[manifest][catch2]") {
    auto chunks = createTestChunks(3, 512);
    auto fileInfo = createTestFileInfo(chunks);

    auto createResult = manager->createManifest(fileInfo, chunks);
    REQUIRE(createResult.has_value());
    auto manifest = createResult.value();

    // Create gap in chunk offsets
    manifest.chunks[1].offset = manifest.chunks[0].offset + manifest.chunks[0].size + 100;

    auto validationResult = manager->validateManifest(manifest);
    REQUIRE(validationResult.has_value());
    CHECK_FALSE(validationResult.value());
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager file reconstruction", "[manifest][catch2]") {
    auto chunks = createTestChunks(5, 1024);
    auto fileInfo = createTestFileInfo(chunks);

    // Create manifest
    auto createResult = manager->createManifest(fileInfo, chunks);
    REQUIRE(createResult.has_value());
    const auto& manifest = createResult.value();

    // Mock chunk provider
    struct MockChunkProvider : public IChunkProvider {
        const std::vector<Chunk>& chunks;

        explicit MockChunkProvider(const std::vector<Chunk>& c) : chunks(c) {}

        Result<std::vector<std::byte>> getChunk(const std::string& hash) const override {
            for (const auto& chunk : chunks) {
                if (chunk.hash == hash) {
                    return Result<std::vector<std::byte>>(chunk.data);
                }
            }
            return Result<std::vector<std::byte>>(ErrorCode::ChunkNotFound);
        }

        bool hasChunk(const std::string& hash) const {
            return std::ranges::any_of(chunks,
                                       [&hash](const auto& chunk) { return chunk.hash == hash; });
        }
    };

    MockChunkProvider provider(chunks);
    auto outputPath = testDir / "reconstructed.txt";

    // Reconstruct file
    auto result = manager->reconstructFile(manifest, outputPath, provider);
    REQUIRE(result.has_value());

    // Verify reconstructed file
    REQUIRE(std::filesystem::exists(outputPath));
    CHECK(std::filesystem::file_size(outputPath) == manifest.fileSize);

    // Verify file content matches original
    std::ifstream file(outputPath, std::ios::binary);
    REQUIRE(file.is_open());

    for (const auto& chunk : chunks) {
        std::vector<char> buffer(chunk.size);
        file.read(buffer.data(), static_cast<std::streamsize>(chunk.size));
        REQUIRE(file.good());

        // Compare chunk data
        auto chunkBytes = reinterpret_cast<const std::byte*>(buffer.data());
        CHECK(std::equal(chunk.data.begin(), chunk.data.end(), chunkBytes));
    }
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager file reconstruction missing chunk",
                 "[manifest][catch2]") {
    auto chunks = createTestChunks(3, 512);
    auto fileInfo = createTestFileInfo(chunks);

    auto createResult = manager->createManifest(fileInfo, chunks);
    REQUIRE(createResult.has_value());
    const auto& manifest = createResult.value();

    // Mock provider missing the second chunk
    struct IncompleteChunkProvider : public IChunkProvider {
        const std::vector<Chunk>& chunks;

        explicit IncompleteChunkProvider(const std::vector<Chunk>& c) : chunks(c) {}

        Result<std::vector<std::byte>> getChunk(const std::string& hash) const override {
            // Skip the second chunk
            for (size_t i = 0; i < chunks.size(); ++i) {
                if (i == 1)
                    continue; // Skip second chunk
                if (chunks[i].hash == hash) {
                    return Result<std::vector<std::byte>>(chunks[i].data);
                }
            }
            return Result<std::vector<std::byte>>(ErrorCode::ChunkNotFound);
        }

        bool hasChunk(const std::string& hash) const {
            for (size_t i = 0; i < chunks.size(); ++i) {
                if (i == 1)
                    continue;
                if (chunks[i].hash == hash)
                    return true;
            }
            return false;
        }
    };

    IncompleteChunkProvider provider(chunks);
    auto outputPath = testDir / "failed_reconstruction.txt";

    auto result = manager->reconstructFile(manifest, outputPath, provider);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error() == ErrorCode::ChunkNotFound);
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager async file reconstruction",
                 "[manifest][catch2]") {
    auto chunks = createTestChunks(3, 256);
    auto fileInfo = createTestFileInfo(chunks);

    auto createResult = manager->createManifest(fileInfo, chunks);
    REQUIRE(createResult.has_value());
    const auto& manifest = createResult.value();

    // Mock chunk provider with delays
    struct SlowChunkProvider : public IChunkProvider {
        const std::vector<Chunk>& chunks;

        explicit SlowChunkProvider(const std::vector<Chunk>& c) : chunks(c) {}

        Result<std::vector<std::byte>> getChunk(const std::string& hash) const override {
            // Simulate network delay
            std::this_thread::sleep_for(std::chrono::milliseconds(10));

            for (const auto& chunk : chunks) {
                if (chunk.hash == hash) {
                    return Result<std::vector<std::byte>>(chunk.data);
                }
            }
            return Result<std::vector<std::byte>>(ErrorCode::ChunkNotFound);
        }

        bool hasChunk(const std::string& hash) const {
            return std::ranges::any_of(chunks,
                                       [&hash](const auto& chunk) { return chunk.hash == hash; });
        }
    };

    SlowChunkProvider provider(chunks);
    auto outputPath = testDir / "async_reconstructed.txt";

    // Start async reconstruction
    auto future = manager->reconstructFileAsync(manifest, outputPath, provider);

    // Should complete within reasonable time
    auto status = future.wait_for(std::chrono::seconds(5));
    REQUIRE(status == std::future_status::ready);

    auto result = future.get();
    REQUIRE(result.has_value());

    // Verify file was created
    REQUIRE(std::filesystem::exists(outputPath));
    CHECK(std::filesystem::file_size(outputPath) == manifest.fileSize);
}

TEST_CASE("ManifestManager mime type detection", "[manifest][catch2]") {
    // Test various file extensions
    CHECK(ManifestManager::detectMimeType("test.txt") == "text/plain");
    CHECK(ManifestManager::detectMimeType("config.json") == "application/json");
    CHECK(ManifestManager::detectMimeType("image.jpg") == "image/jpeg");
    CHECK(ManifestManager::detectMimeType("image.JPEG") == "image/jpeg"); // Case insensitive
    CHECK(ManifestManager::detectMimeType("document.pdf") == "application/pdf");
    CHECK(ManifestManager::detectMimeType("unknown.xyz") == "application/octet-stream");
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager statistics", "[manifest][catch2]") {
    auto chunks = createTestChunks(5, 512);
    auto fileInfo = createTestFileInfo(chunks);

    // Create and serialize multiple manifests
    for (int i = 0; i < 3; ++i) {
        auto createResult = manager->createManifest(fileInfo, chunks);
        REQUIRE(createResult.has_value());

        auto serializeResult = manager->serialize(createResult.value());
        REQUIRE(serializeResult.has_value());

        auto deserializeResult = manager->deserialize(serializeResult.value());
        REQUIRE(deserializeResult.has_value());
    }

    auto stats = manager->getStats();
    CHECK(stats.totalManifests >= 3u); // At least 3 manifests
    CHECK(stats.avgSerializationTime.count() > 0);
    CHECK(stats.avgDeserializationTime.count() > 0);
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager concurrent operations",
                 "[manifest][catch2]") {
    const int numThreads = 10;
    const int operationsPerThread = 5;

    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};

    auto chunks = createTestChunks(3, 256);
    auto fileInfo = createTestFileInfo(chunks);

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([this, &chunks, &fileInfo, &successCount, operationsPerThread]() {
            for (int j = 0; j < operationsPerThread; ++j) {
                // Create manifest
                auto createResult = manager->createManifest(fileInfo, chunks);
                if (!createResult.has_value())
                    continue;

                // Serialize
                auto serializeResult = manager->serialize(createResult.value());
                if (!serializeResult.has_value())
                    continue;

                // Deserialize
                auto deserializeResult = manager->deserialize(serializeResult.value());
                if (!deserializeResult.has_value())
                    continue;

                // Validate
                auto validateResult = manager->validateManifest(deserializeResult.value());
                if (!validateResult.has_value() || !validateResult.value())
                    continue;

                successCount++;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    CHECK(successCount.load() == numThreads * operationsPerThread);
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager performance benchmark",
                 "[manifest][catch2][!benchmark]") {
    // Test serialization performance
    auto chunks = createTestChunks(100, 1024); // 100KB file
    auto fileInfo = createTestFileInfo(chunks);

    auto createResult = manager->createManifest(fileInfo, chunks);
    REQUIRE(createResult.has_value());
    const auto& manifest = createResult.value();

    // Benchmark serialization
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < 10; ++i) {
        auto result = manager->serialize(manifest);
        REQUIRE(result.has_value());
    }
    auto end = std::chrono::steady_clock::now();

    auto avgSerializationTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 10;

    // Should serialize 100KB file manifest in < 50ms
    CHECK(avgSerializationTime < 50);

    // Verify manifest size is < 10% of file size
    auto serializeResult = manager->serialize(manifest);
    REQUIRE(serializeResult.has_value());

    double manifestRatio = static_cast<double>(serializeResult.value().size()) / manifest.fileSize;
    CHECK(manifestRatio < 0.1); // < 10%
}
