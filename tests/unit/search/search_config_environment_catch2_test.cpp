#include "src/search/search_config_environment_internal.h"

#include "tests/common/test_helpers_catch2.h"
#include <yams/search/search_environment.hpp>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

using Catch::Approx;
using yams::search::LegacySearchConfigEnvironment;
using yams::search::SearchEngineConfig;
using yams::search::TuningState;

namespace {

LegacySearchConfigEnvironment environmentFrom(std::unordered_map<std::string, std::string> values) {
    return LegacySearchConfigEnvironment{
        [values = std::move(values)](std::string_view name) -> std::optional<std::string> {
            const auto found = values.find(std::string{name});
            if (found == values.end()) {
                return std::nullopt;
            }
            return found->second;
        }};
}

} // namespace

TEST_CASE("legacy search environment is inert unless explicitly enabled",
          "[search][config][environment][catch2]") {
    SearchEngineConfig config;
    const auto originalTextWeight = config.textWeight;
    const auto environment = environmentFrom(
        {{"YAMS_SEARCH_TEXT_WEIGHT", "0.75"}, {"YAMS_TUNING_OVERRIDE", "SCIENTIFIC"}});

    CHECK_FALSE(environment.enabled());
    CHECK_FALSE(environment.tuningStateOverride().has_value());
    const auto pins = environment.applyTo(config);
    CHECK(static_cast<bool>(config.textWeight == originalTextWeight));
    CHECK_FALSE(pins.text);
}

TEST_CASE("legacy search environment snapshot is immutable across ambient mutation",
          "[search][config][environment][snapshot][catch2]") {
    yams::test::ScopedEnvVar enabled{"YAMS_ENABLE_ENV_OVERRIDES", std::string{"1"}};
    yams::test::ScopedEnvVar textWeight{"YAMS_SEARCH_TEXT_WEIGHT", std::string{"0.25"}};

    const auto snapshot = yams::search::snapshotLegacySearchEnvironment();
    textWeight.set("0.75");
    const auto environment = LegacySearchConfigEnvironment{
        [snapshot](std::string_view name) -> std::optional<std::string> {
            const auto value = snapshot.find(std::string{name});
            return value == snapshot.end() ? std::nullopt
                                           : std::optional<std::string>{value->second};
        }};
    SearchEngineConfig config;
    const auto pins = environment.applyTo(config);

    CHECK(static_cast<bool>(config.textWeight == Approx(0.25F)));
    CHECK(pins.text);
}

TEST_CASE("legacy search environment applies typed benchmark overrides",
          "[search][config][environment][catch2]") {
    SearchEngineConfig config;
    config.textMaxResults = 10;
    config.vectorMaxResults = 20;

    const auto environment = environmentFrom({
        {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
        {"YAMS_TUNING_OVERRIDE", "SCIENTIFIC"},
        {"YAMS_SEARCH_TEXT_WEIGHT", "0.35"},
        {"YAMS_SEARCH_SIMILARITY_THRESHOLD", "4.0"},
        {"YAMS_SEARCH_ENABLE_RERANKING", "off"},
        {"YAMS_SEARCH_RERANK_TOPK", "17"},
        {"YAMS_SEARCH_ZOOM_LEVEL", "STREET"},
        {"YAMS_CANDIDATE_MULTIPLIER", "2.0"},
    });

    REQUIRE(environment.enabled());
    REQUIRE(environment.tuningStateOverride().has_value());
    CHECK(static_cast<bool>(*environment.tuningStateOverride() == TuningState::SCIENTIFIC));

    const auto pins = environment.applyTo(config);
    CHECK(static_cast<bool>(config.textWeight == Approx(0.35F)));
    CHECK(static_cast<bool>(config.similarityThreshold == Approx(1.0F)));
    CHECK_FALSE(config.enableReranking);
    CHECK(static_cast<bool>(config.rerankTopK == 17));
    CHECK(static_cast<bool>(config.zoomLevel == SearchEngineConfig::NavigationZoomLevel::Street));
    CHECK(static_cast<bool>(config.textMaxResults == 20));
    CHECK(static_cast<bool>(config.vectorMaxResults == 40));
    CHECK(pins.text);
    CHECK(pins.similarityThreshold);
}

namespace {

// Overlays retired because nothing outside product code set, benched, or documented them
// (tests/scripts/report_config_key_usage.py). Typed SearchEngineConfig fields remain the
// only way to reach these levers.
constexpr const char* kRetiredSearchOverlays[] = {
    "YAMS_SEARCH_BYPASS_CORPUS_WARMING_GATE",
    "YAMS_SEARCH_ENABLE_GRAPH_FUSION_WINDOW_GUARD",
    "YAMS_SEARCH_ENABLE_GRAPH_QUERY_EXPANSION",
    "YAMS_SEARCH_ENABLE_LEXICAL_EXPANSION",
    "YAMS_SEARCH_ENABLE_WEAK_QUERY_FANOUT_BOOST",
    "YAMS_SEARCH_FUSION_EVIDENCE_RESCUE_MIN_SCORE",
    "YAMS_SEARCH_FUSION_EVIDENCE_RESCUE_SLOTS",
    "YAMS_SEARCH_GRAPH_COMMUNITY_WEIGHT",
    "YAMS_SEARCH_GRAPH_ENABLE_PATHS",
    "YAMS_SEARCH_GRAPH_EXPANSION_FTS_PENALTY",
    "YAMS_SEARCH_GRAPH_EXPANSION_MAX_SEEDS",
    "YAMS_SEARCH_GRAPH_EXPANSION_MAX_TERMS",
    "YAMS_SEARCH_GRAPH_EXPANSION_MIN_HITS",
    "YAMS_SEARCH_GRAPH_EXPANSION_QUERY_NEIGHBOR_K",
    "YAMS_SEARCH_GRAPH_EXPANSION_QUERY_NEIGHBOR_MIN_SCORE",
    "YAMS_SEARCH_GRAPH_EXPANSION_VECTOR_PENALTY",
    "YAMS_SEARCH_GRAPH_FALLBACK_TOP_SIGNAL",
    "YAMS_SEARCH_GRAPH_FUSION_GUARD_DEPTH_MULTIPLIER",
    "YAMS_SEARCH_GRAPH_HOP_DECAY",
    "YAMS_SEARCH_GRAPH_MAX_ADDED_IN_FUSION_WINDOW",
    "YAMS_SEARCH_GRAPH_MAX_HOPS",
    "YAMS_SEARCH_GRAPH_MAX_NEIGHBORS",
    "YAMS_SEARCH_GRAPH_MAX_PATHS",
    "YAMS_SEARCH_GRAPH_TEXT_MIN_ADMISSION_SCORE",
    "YAMS_SEARCH_GRAPH_TEXT_WEIGHT",
    "YAMS_SEARCH_GRAPH_USE_QUERY_CONCEPTS",
    "YAMS_SEARCH_GRAPH_VECTOR_REQUIRE_BASELINE_TEXT_ANCHORING",
    "YAMS_SEARCH_GRAPH_VECTOR_REQUIRE_CORROBORATION",
    "YAMS_SEARCH_GRAPH_VECTOR_REQUIRE_TEXT_ANCHORING",
    "YAMS_SEARCH_GRAPH_VECTOR_WEIGHT",
    "YAMS_SEARCH_LEXICAL_EXPANSION_MIN_HITS",
    "YAMS_SEARCH_LEXICAL_EXPANSION_PENALTY",
    "YAMS_SEARCH_RERANK_TOP_K",
    "YAMS_SEARCH_SEMANTIC_RESCUE_MIN_SCORE",
    "YAMS_SEARCH_STRONG_VECTOR_ONLY_TOP_RANK",
    "YAMS_SEARCH_TIERED_MIN_CANDIDATES",
    "YAMS_SEARCH_TIERED_NARROW_VECTOR_SEARCH",
    "YAMS_SEARCH_TOPOLOGY_FINAL_RESCUE_SLOTS",
    "YAMS_SEARCH_TOPOLOGY_FUSION_RESCUE_SLOTS",
    "YAMS_SEARCH_VECTOR_ONLY_NEAR_MISS_PENALTY",
    "YAMS_SEARCH_VECTOR_ONLY_NEAR_MISS_RESERVE",
    "YAMS_SEARCH_VECTOR_ONLY_NEAR_MISS_SLACK",
    "YAMS_SEARCH_WEAK_QUERY_ENTITY_VECTOR_FANOUT_MULTIPLIER",
    "YAMS_SEARCH_WEAK_QUERY_MIN_TEXT_HITS",
    "YAMS_SEARCH_WEAK_QUERY_MIN_TOP_TEXT_SCORE",
};

} // namespace

TEST_CASE("legacy search environment never consults retired overlays",
          "[search][config][environment][catch2]") {
    std::unordered_map<std::string, int> requested;
    const LegacySearchConfigEnvironment environment{
        [&requested](std::string_view name) -> std::optional<std::string> {
            ++requested[std::string{name}];
            if (name == "YAMS_ENABLE_ENV_OVERRIDES") {
                return std::string{"1"};
            }
            return std::nullopt;
        }};
    SearchEngineConfig config;
    (void)environment.applyTo(config);

    for (const char* key : kRetiredSearchOverlays) {
        INFO(key);
        CHECK(requested.count(key) == 0);
    }
    // One name, one read: the duplicated semantic-rescue block is gone.
    CHECK(requested["YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS"] == 1);
    CHECK(requested["YAMS_SEARCH_RERANK_TOPK"] == 1);
}
