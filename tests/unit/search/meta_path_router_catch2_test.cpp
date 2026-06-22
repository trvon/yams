#include <catch2/catch_test_macros.hpp>

#include <yams/search/meta_path_router.h>

using yams::search::computeMetaPathBoosts;
using yams::search::SearchEngineConfig;

TEST_CASE("MetaPathRouter no-ops when disabled or missing inputs", "[search][meta-path]") {
    SearchEngineConfig cfg;
    cfg.enableMetaPathRouting = false;
    cfg.metaPathSeedK = 8;

    auto disabled = computeMetaPathBoosts({0.1f, 0.2f}, nullptr, nullptr, cfg);
    CHECK(disabled.docBoost.empty());
    CHECK((disabled.seedDocCount == 0U));

    cfg.enableMetaPathRouting = true;

    auto emptyEmbedding = computeMetaPathBoosts({}, nullptr, nullptr, cfg);
    CHECK(emptyEmbedding.docBoost.empty());
    CHECK((emptyEmbedding.seedDocCount == 0U));

    cfg.metaPathSeedK = 0;
    auto noSeedsRequested = computeMetaPathBoosts({0.1f}, nullptr, nullptr, cfg);
    CHECK(noSeedsRequested.docBoost.empty());
    CHECK((noSeedsRequested.seedDocCount == 0U));
}
