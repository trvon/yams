// Pipeline stage rendering tests
// Tests for the extracted renderPipelineStages() function.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <yams/cli/pipeline_stage_render.h>

using Catch::Matchers::ContainsSubstring;
using yams::cli::detail::renderPipelineStages;
using yams::cli::detail::StageInfo;

TEST_CASE("Pipeline stages: all idle produces no rows", "[pipeline]") {
    StageInfo stages[] = {
        {"Extraction", 0, 0, 4, 0, 0, 0},       {"Knowledge Graph", 0, 0, 8, 0, 0, 0},
        {"Symbols", 0, 0, 4, 0, 0, 0},          {"Entities", 0, 0, 4, 0, 0, 0},
        {"Title Extraction", 0, 0, 4, 0, 0, 0},
    };
    auto rows = renderPipelineStages(stages, 5);
    REQUIRE(rows.empty());
}

TEST_CASE("Pipeline stages: completed stage shows done with checkmark", "[pipeline]") {
    StageInfo stages[] = {
        {"Knowledge Graph", 0, 0, 8, 694, 694, 0},
    };
    auto rows = renderPipelineStages(stages, 1);
    REQUIRE(rows.size() == 1);
    CHECK_THAT(rows[0].label, ContainsSubstring("Knowledge Graph"));
    CHECK_THAT(rows[0].value, ContainsSubstring("694/694 done"));
    CHECK_THAT(rows[0].value, ContainsSubstring("\xe2\x9c\x93")); // ✓
}

TEST_CASE("Pipeline stages: completed stage with drops shows dropped count", "[pipeline]") {
    StageInfo stages[] = {
        {"Symbols", 0, 0, 4, 100, 100, 3},
    };
    auto rows = renderPipelineStages(stages, 1);
    REQUIRE(rows.size() == 1);
    CHECK_THAT(rows[0].value, ContainsSubstring("100/100 done"));
    CHECK_THAT(rows[0].value, ContainsSubstring("3 dropped"));
}

TEST_CASE("Pipeline stages: active stage shows progress bar and slots", "[pipeline]") {
    StageInfo stages[] = {
        {"Title Extraction", 2, 694, 4, 694, 0, 0},
    };
    auto rows = renderPipelineStages(stages, 1);
    REQUIRE(rows.size() == 1);
    CHECK_THAT(rows[0].value, ContainsSubstring("2/4 slots"));
    CHECK_THAT(rows[0].value, ContainsSubstring("queue: 694"));
    CHECK_THAT(rows[0].value, ContainsSubstring("0/694 done"));
}

TEST_CASE("Pipeline stages: idle stage with no history is hidden", "[pipeline]") {
    StageInfo stages[] = {
        {"Entities", 0, 0, 4, 0, 0, 0},
        {"Knowledge Graph", 0, 0, 8, 10, 10, 0},
    };
    auto rows = renderPipelineStages(stages, 2);
    REQUIRE(rows.size() == 1);
    CHECK_THAT(rows[0].label, ContainsSubstring("Knowledge Graph"));
}

TEST_CASE("Pipeline stages: mixed completed, active, and idle", "[pipeline]") {
    StageInfo stages[] = {
        {"Extraction", 1, 0, 4, 0, 0, 0},            // active, no audit
        {"Knowledge Graph", 0, 0, 8, 500, 500, 0},   // completed
        {"Symbols", 3, 50, 4, 400, 200, 0},          // active with queue
        {"Entities", 0, 0, 4, 0, 0, 0},              // idle
        {"Title Extraction", 2, 100, 4, 300, 50, 0}, // active with queue
    };
    auto rows = renderPipelineStages(stages, 5);
    // Entities is idle → hidden, so expect 4 rows
    REQUIRE(rows.size() == 4);

    // Extraction: active, no audit counters
    CHECK_THAT(rows[0].label, ContainsSubstring("Extraction"));
    CHECK_THAT(rows[0].value, ContainsSubstring("1/4 slots"));

    // KG: completed
    CHECK_THAT(rows[1].label, ContainsSubstring("Knowledge Graph"));
    CHECK_THAT(rows[1].value, ContainsSubstring("500/500 done"));

    // Symbols: active
    CHECK_THAT(rows[2].label, ContainsSubstring("Symbols"));
    CHECK_THAT(rows[2].value, ContainsSubstring("3/4 slots"));
    CHECK_THAT(rows[2].value, ContainsSubstring("queue: 50"));
    CHECK_THAT(rows[2].value, ContainsSubstring("200/400 done"));

    // Title Extraction: active
    CHECK_THAT(rows[3].label, ContainsSubstring("Title Extraction"));
    CHECK_THAT(rows[3].value, ContainsSubstring("2/4 slots"));
}

TEST_CASE("Pipeline stages: partially done but not active shows bar", "[pipeline]") {
    // queued > consumed, but inflight=0 and queueDepth=0 → not completed (consumed != queued)
    StageInfo stages[] = {
        {"Symbols", 0, 0, 4, 100, 50, 0},
    };
    auto rows = renderPipelineStages(stages, 1);
    REQUIRE(rows.size() == 1);
    // Should show progress bar format (not completed checkmark)
    CHECK_THAT(rows[0].value, ContainsSubstring("0/4 slots"));
    CHECK_THAT(rows[0].value, ContainsSubstring("50/100 done"));
    // Should NOT contain checkmark
    CHECK(rows[0].value.find("\xe2\x9c\x93") == std::string::npos);
}
