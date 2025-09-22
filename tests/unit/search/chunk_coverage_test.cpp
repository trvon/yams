#include <gtest/gtest.h>
#include <yams/search/chunk_coverage.h>

using namespace yams;
using namespace yams::search;

namespace {
vector::SearchResult makeVR(const std::string& id, float sim) {
    vector::SearchResult r;
    r.id = id;
    r.distance = 0.0f;
    r.similarity = sim;
    return r;
}
} // namespace

TEST(ChunkCoverageTest, BaseIdExtraction) {
    EXPECT_EQ(baseIdFromChunkId("docA#0"), "docA");
    EXPECT_EQ(baseIdFromChunkId("docB#12"), "docB");
    EXPECT_EQ(baseIdFromChunkId("nohash"), "nohash");
    EXPECT_EQ(baseIdFromChunkId(""), "");
}

TEST(ChunkCoverageTest, GroupAndAggregate_MAX) {
    std::vector<vector::SearchResult> vr{
        makeVR("docA#0", 0.9f),
        makeVR("docA#1", 0.7f),
        makeVR("docB#0", 0.8f),
    };
    auto resolver = [](std::string_view base) -> std::optional<size_t> {
        if (base == "docA")
            return 4u; // 2/4 => 0.5
        if (base == "docB")
            return 2u; // 1/2 => 0.5
        return std::nullopt;
    };
    auto groups = groupAndAggregate(vr, Pooling::MAX, resolver);
    ASSERT_EQ(groups.size(), 2u);
    // Sorted by pooled_score desc: docA(0.9) then docB(0.8)
    EXPECT_EQ(groups[0].base_id, std::string("docA"));
    EXPECT_FLOAT_EQ(groups[0].pooled_score, 0.9f);
    ASSERT_TRUE(groups[0].coverage().has_value());
    EXPECT_FLOAT_EQ(groups[0].coverage().value(), 0.5f);
    EXPECT_EQ(groups[0].contributing_chunks, 2u);

    EXPECT_EQ(groups[1].base_id, std::string("docB"));
    EXPECT_FLOAT_EQ(groups[1].pooled_score, 0.8f);
    ASSERT_TRUE(groups[1].coverage().has_value());
    EXPECT_FLOAT_EQ(groups[1].coverage().value(), 0.5f);
    EXPECT_EQ(groups[1].contributing_chunks, 1u);
}

TEST(ChunkCoverageTest, GroupAndAggregate_AVG_Deduplicates) {
    std::vector<vector::SearchResult> vr{
        makeVR("docX#0", 0.2f), makeVR("docX#1", 0.4f), makeVR("docX#1", 0.4f) // dup id
    };
    auto groups = groupAndAggregate(vr, Pooling::AVG, {});
    ASSERT_EQ(groups.size(), 1u);
    EXPECT_EQ(groups[0].base_id, std::string("docX"));
    EXPECT_EQ(groups[0].contributing_chunks, 2u); // duplicate ignored
    EXPECT_FALSE(groups[0].coverage().has_value());
    EXPECT_FLOAT_EQ(groups[0].pooled_score, (0.2f + 0.4f) / 2.0f);
}
