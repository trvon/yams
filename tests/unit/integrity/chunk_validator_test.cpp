#include <catch2/catch_test_macros.hpp>

#include <yams/crypto/hasher.h>
#include <yams/integrity/chunk_validator.h>

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
        
        auto result = validator.validateChunk(
            std::span<const std::byte>(bytes.data(), bytes.size()), hash);
        
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
        
        auto result = validator.validateChunk(
            std::span<const std::byte>(emptyBytes.data(), 0), hash);
        
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
