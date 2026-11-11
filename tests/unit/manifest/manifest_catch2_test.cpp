// Catch2 tests for ManifestManager
// Migrated from GTest: manifest_test.cpp

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <yams/chunking/chunker.h>
#include <yams/crypto/hasher.h>
#include <yams/manifest/manifest_manager.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <random>
#include <span>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#include "../../common/test_helpers_catch2.h"

using namespace yams;
using namespace yams::manifest;
using namespace yams::chunking;

namespace {

std::vector<std::byte> generateRandomBytes(std::size_t size) {
    static thread_local std::mt19937_64 engine{std::random_device{}()};
    std::vector<std::byte> data(size);
    std::uniform_int_distribution<int> dist(0, 255);
    std::generate(data.begin(), data.end(), [&] { return std::byte(dist(engine)); });
    return data;
}

template <typename T> void appendFixturePod(std::vector<std::byte>& output, T value) {
    static_assert(std::is_trivially_copyable_v<T>);
    const auto bytes = std::as_bytes(std::span<const T, 1>{&value, 1});
    output.insert(output.end(), bytes.begin(), bytes.end());
}

void appendFixtureString(std::vector<std::byte>& output, std::string_view value) {
    appendFixturePod(output, static_cast<std::uint32_t>(value.size()));
    const auto bytes = std::as_bytes(std::span{value.data(), value.size()});
    output.insert(output.end(), bytes.begin(), bytes.end());
}

std::vector<std::byte> makeBinaryManifestFixture(bool legacy64BitFlags,
                                                 std::uint64_t firstChunkFlags = 1) {
    const std::string kFileHash(64, 'f');
    const std::string kFirstChunkHash(64, 'a');
    const std::string kSecondChunkHash(64, 'b');
    const std::string kThirdChunkHash(64, 'c');

    std::vector<std::byte> output;
    const std::array magic{std::byte{'Y'}, std::byte{'A'}, std::byte{'M'}, std::byte{'S'}};
    output.insert(output.end(), magic.begin(), magic.end());
    appendFixturePod(output, Manifest::CURRENT_VERSION);
    appendFixtureString(output, kFileHash);
    appendFixturePod(output, std::uint64_t{18});
    appendFixtureString(output, "legacy.bin");
    appendFixtureString(output, "application/octet-stream");
    appendFixturePod(output, std::uint32_t{3});

    const auto appendChunk = [&](std::string_view hash, std::uint64_t offset, std::uint32_t size,
                                 std::uint64_t flags) {
        appendFixtureString(output, hash);
        appendFixturePod(output, offset);
        appendFixturePod(output, size);
        if (legacy64BitFlags) {
            appendFixturePod(output, flags);
        } else {
            appendFixturePod(output, static_cast<std::uint32_t>(flags));
        }
    };
    appendChunk(kFirstChunkHash, 0, 5, firstChunkFlags);
    appendChunk(kSecondChunkHash, 5, 7, 2);
    appendChunk(kThirdChunkHash, 12, 6, 3);
    return output;
}

struct ManifestTestFixture {
    ManifestTestFixture() {
        testDir = yams::test::make_temp_dir("yams_manifest_test_");

        ManifestManager::Config config{.enableCompression = false, // Disable for predictable tests
                                       .enableChecksums = true,
                                       .enableCaching = true,
                                       .cacheSize = 100};

        manager = std::make_unique<ManifestManager>(config);
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
    CHECK((manifest.version == Manifest::CURRENT_VERSION));
    CHECK((manifest.fileHash == fileInfo.hash));
    CHECK((manifest.fileSize == fileInfo.size));
    CHECK((manifest.originalName == fileInfo.originalName));
    CHECK((manifest.mimeType == fileInfo.mimeType));
    CHECK((manifest.chunks.size() == chunks.size()));

    // Verify chunk mapping
    for (size_t i = 0; i < chunks.size(); ++i) {
        CHECK((manifest.chunks[i].hash == chunks[i].hash));
        CHECK((manifest.chunks[i].offset == chunks[i].offset));
        CHECK((manifest.chunks[i].size == chunks[i].size));
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
    CHECK((result.error() == ErrorCode::InvalidArgument));
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager create large manifest",
                 "[manifest][catch2]") {
    // Test with many chunks
    auto chunks = createTestChunks(1000, 64);
    auto fileInfo = createTestFileInfo(chunks);

    auto result = manager->createManifest(fileInfo, chunks);
    REQUIRE(result.has_value());

    const auto& manifest = result.value();
    CHECK((manifest.chunks.size() == 1000u));
    CHECK(manifest.isValid());

    // Verify total size calculation
    CHECK((manifest.calculateTotalSize() == manifest.fileSize));
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
    CHECK((serializedData.size() > 100u)); // Should have substantial content
    CHECK((serializedData.size() <
           originalManifest.fileSize)); // Should be less than original file size

    // Deserialize
    auto deserializeResult = manager->deserialize(serializedData);
    REQUIRE(deserializeResult.has_value());
    const auto& deserializedManifest = deserializeResult.value();

    // Verify roundtrip accuracy
    CHECK((deserializedManifest.version == originalManifest.version));
    CHECK((deserializedManifest.fileHash == originalManifest.fileHash));
    CHECK((deserializedManifest.fileSize == originalManifest.fileSize));
    CHECK((deserializedManifest.originalName == originalManifest.originalName));
    CHECK((deserializedManifest.mimeType == originalManifest.mimeType));
    CHECK((deserializedManifest.chunks.size() == originalManifest.chunks.size()));

    // Verify all chunks match
    for (size_t i = 0; i < originalManifest.chunks.size(); ++i) {
        CHECK((deserializedManifest.chunks[i].hash == originalManifest.chunks[i].hash));
        CHECK((deserializedManifest.chunks[i].offset == originalManifest.chunks[i].offset));
        CHECK((deserializedManifest.chunks[i].size == originalManifest.chunks[i].size));
    }
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager reads canonical 32-bit chunk flags",
                 "[manifest][catch2][compatibility]") {
    const auto bytes = makeBinaryManifestFixture(false);

    auto result = manager->deserialize(bytes);

    REQUIRE(result.has_value());
    REQUIRE((result.value().chunks.size() == 3));
    CHECK((result.value().chunks[0].flags == 1));
    CHECK((result.value().chunks[1].flags == 2));
    CHECK((result.value().chunks[2].flags == 3));
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager reads legacy 64-bit chunk flags",
                 "[manifest][catch2][compatibility]") {
    const auto bytes = makeBinaryManifestFixture(true);

    auto result = manager->deserialize(bytes);

    REQUIRE(result.has_value());
    REQUIRE((result.value().chunks.size() == 3));
    CHECK((result.value().chunks[0].flags == 1));
    CHECK((result.value().chunks[1].flags == 2));
    CHECK((result.value().chunks[2].flags == 3));
    CHECK((result.value().calculateTotalSize() == result.value().fileSize));
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager rejects oversized legacy chunk flags",
                 "[manifest][catch2][compatibility]") {
    const auto bytes = makeBinaryManifestFixture(
        true, static_cast<std::uint64_t>(std::numeric_limits<std::uint32_t>::max()) + 1);

    auto result = manager->deserialize(bytes);

    REQUIRE_FALSE(result.has_value());
    CHECK((result.error() == ErrorCode::CorruptedData));
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager rejects trailing binary manifest bytes",
                 "[manifest][catch2][compatibility]") {
    auto bytes = makeBinaryManifestFixture(false);
    appendFixturePod(bytes, std::uint32_t{0});

    auto result = manager->deserialize(bytes);

    REQUIRE_FALSE(result.has_value());
    CHECK((result.error() == ErrorCode::CorruptedData));
}

TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager deserialize corrupted data fails",
                 "[manifest][catch2]") {
    std::vector<std::byte> corruptedData{std::byte{0xFF}, std::byte{0xFE}, std::byte{0xFD}};

    auto result = manager->deserialize(corruptedData);
    REQUIRE_FALSE(result.has_value());
    CHECK((result.error() == ErrorCode::CorruptedData));
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
    CHECK((std::filesystem::file_size(outputPath) == manifest.fileSize));

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
    CHECK((result.error() == ErrorCode::ChunkNotFound));
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
    REQUIRE((status == std::future_status::ready));

    auto result = future.get();
    REQUIRE(result.has_value());

    // Verify file was created
    REQUIRE(std::filesystem::exists(outputPath));
    CHECK((std::filesystem::file_size(outputPath) == manifest.fileSize));
}

TEST_CASE("ManifestManager mime type detection", "[manifest][catch2]") {
    // Test various file extensions
    CHECK((ManifestManager::detectMimeType("test.txt") == "text/plain"));
    CHECK((ManifestManager::detectMimeType("config.json") == "application/json"));
    CHECK((ManifestManager::detectMimeType("image.jpg") == "image/jpeg"));
    CHECK((ManifestManager::detectMimeType("image.JPEG") == "image/jpeg")); // Case insensitive
    CHECK((ManifestManager::detectMimeType("document.pdf") == "application/pdf"));
    CHECK((ManifestManager::detectMimeType("unknown.xyz") == "application/octet-stream"));
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
    CHECK((stats.totalManifests >= 3u)); // At least 3 manifests
    CHECK((stats.avgSerializationTime.count() > 0));
    CHECK((stats.avgDeserializationTime.count() > 0));
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
        threads.emplace_back([this, &chunks, &fileInfo, &successCount]() {
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

    CHECK((successCount.load() == numThreads * operationsPerThread));
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
    CHECK((avgSerializationTime < 50));

    // Verify manifest size is < 10% of file size
    auto serializeResult = manager->serialize(manifest);
    REQUIRE(serializeResult.has_value());

    double manifestRatio = static_cast<double>(serializeResult.value().size()) / manifest.fileSize;
    CHECK((manifestRatio < 0.1)); // < 10%
}

// An empty file has no chunks, and older stores wrote a zero-chunk manifest for it. isValid()
// required a chunk and a non-zero size, so every such manifest deserialized as ManifestInvalid:
// the empty document could not be read back, and memory-sync backfill skipped it as corrupt.
TEST_CASE_METHOD(ManifestTestFixture, "ManifestManager accepts the manifest of an empty file",
                 "[manifest][catch2]") {
    Manifest empty;
    empty.fileHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    empty.fileSize = 0;
    empty.originalName = "README.md";
    CHECK(empty.isValid());

    auto serialized = manager->serialize(empty);
    REQUIRE(serialized.has_value());
    auto restored = manager->deserialize(serialized.value());
    REQUIRE(restored.has_value());
    CHECK(restored.value().fileSize == 0);
    CHECK(restored.value().chunks.empty());

    // A non-empty size with no chunks, or chunks for an empty size, still describe no file.
    Manifest sizeWithoutChunks = empty;
    sizeWithoutChunks.fileSize = 10;
    CHECK_FALSE(sizeWithoutChunks.isValid());
    Manifest chunksWithoutSize = empty;
    chunksWithoutSize.chunks.push_back(
        manifest::ChunkRef{.hash = std::string(64, 'a'), .offset = 0, .size = 4});
    CHECK_FALSE(chunksWithoutSize.isValid());

    // Chunk sizes that do not add up to the file size describe a different file.
    Manifest mismatched = empty;
    mismatched.fileSize = 10;
    mismatched.chunks.push_back(
        manifest::ChunkRef{.hash = std::string(64, 'a'), .offset = 0, .size = 4});
    CHECK_FALSE(mismatched.isValid());
    mismatched.chunks.push_back(
        manifest::ChunkRef{.hash = std::string(64, 'b'), .offset = 4, .size = 6});
    CHECK(mismatched.isValid());
}

// The cache sits in front of deserialize(). Whatever it holds or evicts, a manifest must come
// back as the one that was serialized, including with a cache too small for the working set.
TEST_CASE_METHOD(ManifestTestFixture,
                 "ManifestManager deserialize is exact at every cache capacity",
                 "[manifest][catch2]") {
    std::vector<Manifest> originals;
    std::vector<std::vector<std::byte>> serialized;
    for (size_t i = 0; i < 8; ++i) {
        auto chunks = createTestChunks(i + 1, 64);
        auto created = manager->createManifest(createTestFileInfo(chunks), chunks);
        REQUIRE(created.has_value());
        auto bytes = manager->serialize(created.value());
        REQUIRE(bytes.has_value());
        originals.push_back(created.value());
        serialized.push_back(std::move(bytes.value()));
    }

    const auto capacity = GENERATE(size_t{0}, size_t{1}, size_t{3}, size_t{100});
    ManifestManager small(ManifestManager::Config{.enableCompression = false,
                                                  .enableChecksums = true,
                                                  .enableCaching = true,
                                                  .cacheSize = capacity});

    // Every stored field must survive the cache, so compare the bytes a restored manifest
    // serializes to. Manifest equality would not do: timestamps are not serialized.
    const auto reserializesTo = [&small](const Manifest& restored,
                                         const std::vector<std::byte>& expected) {
        auto bytes = small.serialize(restored);
        return bytes.has_value() && bytes.value() == expected;
    };

    // Repeats keep some entries hot while the sweep pushes others out.
    const std::vector<size_t> order = {0, 1, 0, 2, 3, 0, 4, 1, 5, 6, 0, 7, 7, 2, 1,
                                       0, 3, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7};

    SECTION("single thread") {
        for (const size_t index : order) {
            auto restored = small.deserialize(serialized[index]);
            REQUIRE(restored.has_value());
            CHECK(restored.value().fileHash == originals[index].fileHash);
            CHECK(reserializesTo(restored.value(), serialized[index]));
        }
    }

    SECTION("concurrent readers") {
        std::atomic<int> wrong{0};
        std::vector<std::thread> readers;
        for (size_t t = 0; t < 4; ++t) {
            readers.emplace_back([&, t] {
                for (int round = 0; round < 20; ++round) {
                    for (const size_t step : order) {
                        const size_t index = (step + t) % serialized.size();
                        auto restored = small.deserialize(serialized[index]);
                        if (!restored.has_value() ||
                            !reserializesTo(restored.value(), serialized[index])) {
                            wrong.fetch_add(1);
                        }
                    }
                }
            });
        }
        for (auto& reader : readers) {
            reader.join();
        }
        CHECK(wrong.load() == 0);
    }
}
