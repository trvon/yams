#include <catch2/catch_test_macros.hpp>

#include <yams/crypto/hasher.h>
#include <yams/integrity/chunk_validator.h>
#include <yams/manifest/manifest_manager.h>

#include <cstring>
#include <string>
#include <vector>

using namespace yams::integrity;

namespace {

std::vector<std::byte> toBytes(const std::string& s) {
    std::vector<std::byte> out(s.size());
    std::memcpy(out.data(), s.data(), s.size());
    return out;
}

} // namespace

TEST_CASE("ChunkValidator single chunk validation", "[integrity][chunk-validator]") {
    ChunkValidationConfig cfg;
    cfg.maxParallelValidations = 4;

    ChunkValidator validator(cfg);

    SECTION("validates chunk with correct hash") {
        auto hasher = yams::crypto::createSHA256Hasher();
        std::string data = "test chunk data";
        auto bytes = toBytes(data);
        std::string hash = hasher->hash(std::span<const std::byte>(bytes.data(), bytes.size()));

        auto result =
            validator.validateChunk(std::span<const std::byte>(bytes.data(), bytes.size()), hash);

        CHECK(result.isValid);
        CHECK(result.errorMessage.empty());
    }

    SECTION("detects chunk with incorrect hash") {
        std::string data = "test chunk data";
        auto bytes = toBytes(data);

        auto result = validator.validateChunk(
            std::span<const std::byte>(bytes.data(), bytes.size()), "bad-hash");

        CHECK_FALSE(result.isValid);
        CHECK_FALSE(result.errorMessage.empty());
    }

    SECTION("validates empty chunk") {
        auto hasher = yams::crypto::createSHA256Hasher();
        std::vector<std::byte> emptyBytes;
        std::string hash = hasher->hash(std::span<const std::byte>(emptyBytes.data(), 0));

        auto result =
            validator.validateChunk(std::span<const std::byte>(emptyBytes.data(), 0), hash);

        CHECK(result.isValid);
    }
}

TEST_CASE("ChunkValidator configuration", "[integrity][chunk-validator]") {
    ChunkValidationConfig cfg;
    cfg.maxParallelValidations = 8;
    cfg.failOnFirstError = true;

    ChunkValidator validator(cfg);

    SECTION("getConfig returns current configuration") {
        const auto& currentCfg = validator.getConfig();
        CHECK(currentCfg.maxParallelValidations == 8);
        CHECK(currentCfg.failOnFirstError);
    }

    SECTION("setConfig updates configuration") {
        ChunkValidationConfig newCfg;
        newCfg.maxParallelValidations = 2;
        newCfg.failOnFirstError = false;

        validator.setConfig(newCfg);

        const auto& currentCfg = validator.getConfig();
        CHECK(currentCfg.maxParallelValidations == 2);
        CHECK_FALSE(currentCfg.failOnFirstError);
    }
}

TEST_CASE("ChunkValidator validates manifest with valid chunks", "[integrity][chunk-validator]") {
    ChunkValidationConfig cfg;
    cfg.maxParallelValidations = 4;
    ChunkValidator validator(cfg);

    auto hasher = yams::crypto::createSHA256Hasher();
    std::string data1 = "chunk one data";
    std::string data2 = "chunk two is longer";
    auto bytes1 = toBytes(data1);
    auto bytes2 = toBytes(data2);

    std::string hash1 = hasher->hash(std::span<const std::byte>(bytes1.data(), bytes1.size()));
    std::string hash2 = hasher->hash(std::span<const std::byte>(bytes2.data(), bytes2.size()));

    yams::manifest::Manifest manifest;
    manifest.fileHash = hasher->hash(std::span<const std::byte>(bytes1.data(), bytes1.size()));
    manifest.fileSize = data1.size() + data2.size();
    manifest.chunks.push_back(
        {.hash = hash1, .offset = 0, .size = static_cast<uint32_t>(data1.size())});
    manifest.chunks.push_back({.hash = hash2,
                               .offset = static_cast<uint64_t>(data1.size()),
                               .size = static_cast<uint32_t>(data2.size())});

    auto chunkProvider = [&](const std::string& hash) -> yams::Result<std::vector<std::byte>> {
        if (hash == hash1)
            return bytes1;
        if (hash == hash2)
            return bytes2;
        return yams::Error{yams::ErrorCode::ChunkNotFound, "no such chunk"};
    };

    auto report = validator.validateManifest(manifest, chunkProvider);
    CHECK(report.overallSuccess);
    CHECK(report.validChunks == 2);
    CHECK(report.invalidChunks == 0);
    CHECK(report.failures.empty());
}

TEST_CASE("ChunkValidator detects invalid chunk in manifest", "[integrity][chunk-validator]") {
    ChunkValidationConfig cfg;
    cfg.failOnFirstError = true;
    ChunkValidator validator(cfg);

    auto hasher = yams::crypto::createSHA256Hasher();
    std::string goodData = "good chunk";
    std::string badData = "bad chunk data";
    auto goodBytes = toBytes(goodData);
    auto badBytes = toBytes(badData);

    std::string goodHash =
        hasher->hash(std::span<const std::byte>(goodBytes.data(), goodBytes.size()));

    yams::manifest::Manifest manifest;
    manifest.fileHash = goodHash;
    manifest.fileSize = goodData.size() + badData.size();
    manifest.chunks.push_back(
        {.hash = goodHash, .offset = 0, .size = static_cast<uint32_t>(goodData.size())});
    manifest.chunks.push_back({.hash = "wrong-hash-for-bad-chunk",
                               .offset = static_cast<uint64_t>(goodData.size()),
                               .size = static_cast<uint32_t>(badData.size())});

    auto chunkProvider = [&](const std::string& hash) -> yams::Result<std::vector<std::byte>> {
        if (hash == goodHash)
            return goodBytes;
        return badBytes;
    };

    auto report = validator.validateManifest(manifest, chunkProvider);
    CHECK_FALSE(report.overallSuccess);
    CHECK(report.invalidChunks > 0);
}
