#include <catch2/catch_test_macros.hpp>

#include "src/search/search_vector_pipeline_internal.h"

using yams::search::SearchEngineConfig;

TEST_CASE("Vector pipeline over-fetches chunks for aggregating unique docs",
          "[search][vector][catch2]") {
    SearchEngineConfig cfg;
    cfg.chunkAggregation = SearchEngineConfig::ChunkAggregation::WEIGHTED_TOP_K_AVG;
    cfg.chunkAggregationTopK = 3;

    CHECK(yams::search::detail::testingVectorRawCandidateLimit(cfg, 10, false) == 30U);
    CHECK(yams::search::detail::testingVectorRawCandidateLimit(cfg, 10, true) == 10U);

    cfg.chunkAggregation = SearchEngineConfig::ChunkAggregation::MAX;
    CHECK(yams::search::detail::testingVectorRawCandidateLimit(cfg, 10, false) == 10U);
}
