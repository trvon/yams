#include "../../../src/daemon/components/title_enrichment_policy.h"
#include "../../../src/daemon/resource/extraction_response.h"
#include <catch2/catch_test_macros.hpp>

TEST_CASE("External extraction preserves string metadata", "[daemon][extraction][contract]") {
    const auto response = nlohmann::json{
        {"text", "body"}, {"metadata", {{"title", "Embedded title"}, {"author", "Ada"}}}};
    const auto result = yams::daemon::decodeExtractionResponse(response);
    REQUIRE(result);
    CHECK(result->text == "body");
    REQUIRE(result->metadata.contains("title"));
    CHECK(result->metadata.at("title") == "Embedded title");
    CHECK(result->metadata.at("author") == "Ada");
}

TEST_CASE("External extraction accepts content alias and rejects plugin errors",
          "[daemon][extraction][contract]") {
    const auto result = yams::daemon::decodeExtractionResponse({{"content", "alternate"}});
    REQUIRE(result);
    CHECK(result->text == "alternate");
    CHECK_FALSE(yams::daemon::decodeExtractionResponse({{"text", "body"}, {"error", "failed"}}));
    CHECK_FALSE(yams::daemon::decodeExtractionResponse(nlohmann::json::array()));
    CHECK_FALSE(yams::daemon::decodeExtractionResponse({{"text", 42}}));
}

TEST_CASE("Supplied titles preserve entity enrichment without allowing title replacement",
          "[daemon][extraction][contract]") {
    const auto supplied = yams::daemon::planTitleEnrichment(false, true, true, false);
    CHECK(supplied.dispatch);
    CHECK(supplied.preserveTitle);
    const auto inferred = yams::daemon::planTitleEnrichment(false, false, true, false);
    CHECK(inferred.dispatch);
    CHECK_FALSE(inferred.preserveTitle);
    CHECK_FALSE(yams::daemon::planTitleEnrichment(true, true, true, false).dispatch);
    CHECK_FALSE(yams::daemon::planTitleEnrichment(false, true, false, false).dispatch);
    CHECK_FALSE(yams::daemon::planTitleEnrichment(false, true, true, true).dispatch);
}

TEST_CASE("External metadata ignores non-string values without losing valid fields",
          "[daemon][extraction][contract]") {
    const auto result = yams::daemon::decodeExtractionResponse(
        {{"text", ""}, {"metadata", {{"title", "Title"}, {"count", 2}, {"nested", {{"x", 1}}}}}});
    REQUIRE(result);
    CHECK(result->metadata.size() == 1);
    CHECK(result->metadata.at("title") == "Title");
}
