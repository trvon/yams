// Catch2 tests for chunk coverage
// Migrated from GTest: chunk_coverage_test.cpp

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

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

TEST_CASE("ChunkCoverage base ID extraction", "[search][chunk][catch2]") {
    CHECK(baseIdFromChunkId("docA#0") == "docA");
    CHECK(baseIdFromChunkId("docB#12") == "docB");
    CHECK(baseIdFromChunkId("nohash") == "nohash");
    CHECK(baseIdFromChunkId("") == "");
}

TEST_CASE("ChunkCoverage group and aggregate MAX pooling", "[search][chunk][catch2]") {
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
    REQUIRE(groups.size() == 2u);

    // Sorted by pooled_score desc: docA(0.9) then docB(0.8)
    CHECK(groups[0].base_id == std::string("docA"));
    CHECK(groups[0].pooled_score == Catch::Approx(0.9f));
    REQUIRE(groups[0].coverage().has_value());
    CHECK(groups[0].coverage().value() == Catch::Approx(0.5f));
    CHECK(groups[0].contributing_chunks == 2u);

    CHECK(groups[1].base_id == std::string("docB"));
    CHECK(groups[1].pooled_score == Catch::Approx(0.8f));
    REQUIRE(groups[1].coverage().has_value());
    CHECK(groups[1].coverage().value() == Catch::Approx(0.5f));
    CHECK(groups[1].contributing_chunks == 1u);
}

TEST_CASE("ChunkCoverage group and aggregate AVG pooling deduplicates", "[search][chunk][catch2]") {
    std::vector<vector::SearchResult> vr{
        makeVR("docX#0", 0.2f), makeVR("docX#1", 0.4f), makeVR("docX#1", 0.4f) // dup id
    };
    auto groups = groupAndAggregate(vr, Pooling::AVG, {});
    REQUIRE(groups.size() == 1u);
    CHECK(groups[0].base_id == std::string("docX"));
    CHECK(groups[0].contributing_chunks == 2u); // duplicate ignored
    CHECK_FALSE(groups[0].coverage().has_value());
    CHECK(groups[0].pooled_score == Catch::Approx((0.2f + 0.4f) / 2.0f));
}
