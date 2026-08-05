#include "src/search/search_config_environment_internal.h"

#include "tests/common/test_helpers_catch2.h"
#include <yams/search/search_environment.hpp>

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
