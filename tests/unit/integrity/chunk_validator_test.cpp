#include <gtest/gtest.h>

#include <yams/crypto/hasher.h>
#include <yams/integrity/chunk_validator.h>

#include <cstring>
#include <string>
#include <vector>

using namespace yams::integrity;

namespace {

std::vector<std::byte> to_bytes(const std::string& s) {
    std::vector<std::byte> out(s.size());
    std::memcpy(out.data(), s.data(), s.size());
    return out;
}

struct ChunkFixture {
    std::vector<std::vector<std::byte>> backing;
    std::vector<std::pair<std::span<const std::byte>, std::string>> chunks;

    explicit ChunkFixture(std::size_t count) {
        auto hasher = yams::crypto::createSHA256Hasher();
        backing.reserve(count);
        chunks.reserve(count);
        for (std::size_t i = 0; i < count; ++i) {
            std::string data = "chunk_" + std::to_string(i);
            backing.push_back(to_bytes(data));
            auto span = std::span<const std::byte>(backing.back().data(), backing.back().size());
            auto hash = hasher->hash(span);
            chunks.emplace_back(span, hash);
        }
    }
};

} // namespace

TEST(ChunkValidatorParallel, ValidatesMultipleChunks) {
    ChunkValidationConfig cfg;
    cfg.maxParallelValidations = 4;

    ChunkValidator validator(cfg);
    ChunkFixture fixture(32);

    auto results = validator.validateChunks(fixture.chunks);
    ASSERT_EQ(results.size(), fixture.chunks.size());
    for (const auto& res : results) {
        EXPECT_TRUE(res.isValid);
        EXPECT_TRUE(res.errorMessage.empty());
    }
}

TEST(ChunkValidatorParallel, DetectsInvalidChunks) {
    ChunkValidationConfig cfg;
    cfg.maxParallelValidations = 8;

    ChunkValidator validator(cfg);
    ChunkFixture fixture(16);

    auto corrupted = fixture.chunks;
    corrupted[5].second = "bad-hash";

    auto results = validator.validateChunks(corrupted);
    ASSERT_EQ(results.size(), corrupted.size());

    std::size_t invalidCount = 0;
    for (std::size_t i = 0; i < results.size(); ++i) {
        if (i == 5) {
            EXPECT_FALSE(results[i].isValid);
            EXPECT_FALSE(results[i].errorMessage.empty());
            ++invalidCount;
        } else {
            EXPECT_TRUE(results[i].isValid);
            EXPECT_TRUE(results[i].errorMessage.empty());
        }
    }
    EXPECT_EQ(invalidCount, 1u);
}
