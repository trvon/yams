#include "src/search/search_config_environment_internal.h"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <string>
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
    CHECK(config.textWeight == originalTextWeight);
    CHECK_FALSE(pins.text);
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
    CHECK(*environment.tuningStateOverride() == TuningState::SCIENTIFIC);

    const auto pins = environment.applyTo(config);
    CHECK(config.textWeight == Approx(0.35F));
    CHECK(config.similarityThreshold == Approx(1.0F));
    CHECK_FALSE(config.enableReranking);
    CHECK(config.rerankTopK == 17);
    CHECK(config.zoomLevel == SearchEngineConfig::NavigationZoomLevel::Street);
    CHECK(config.textMaxResults == 20);
    CHECK(config.vectorMaxResults == 40);
    CHECK(pins.text);
    CHECK(pins.similarityThreshold);
}
