// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
#include <catch2/catch_get_random_seed.hpp>
#include <catch2/catch_test_macros.hpp>

#include <array>
#include <chrono>
#include <limits>
#include <thread>
#include <utility>
#include <vector>

#include <yams/daemon/components/startup_phase.h>

#include "../../../../tools/fuzzing/startup_fuzz_support.h"
#include "../../../common/test_helpers_catch2.h"

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t*, std::size_t);

namespace {
using yams::daemon::ServiceManagerState;
using yams::fuzz::StartupAction;
constexpr auto action(StartupAction value) {
    return static_cast<std::uint8_t>(value);
}

TEST_CASE("A starvation deadline cannot make incomplete startup maintenance eligible",
          "[daemon][startup-replay][pressure]") {
    yams::test::SpdlogLevelGuard logging;
    spdlog::set_level(spdlog::level::off);
    std::vector<std::uint8_t> trace{
        action(StartupAction::Pressure), action(StartupAction::Advance),
        action(StartupAction::Advance),  action(StartupAction::Advance),
        action(StartupAction::Advance),  action(StartupAction::Deadline),
        action(StartupAction::Probe)};
    auto incomplete = yams::fuzz::replayStartup(trace);
    REQUIRE(incomplete.ok());
    CHECK(incomplete.admissions == 0);
    CHECK(incomplete.forcedAdmissions == 0);

    // Search initialization finishes only AFTER the first five-minute deadline.
    trace.push_back(action(StartupAction::Advance));
    trace.push_back(action(StartupAction::Advance));
    auto justReady = yams::fuzz::replayStartup(trace);
    REQUIRE(justReady.ok());
    CHECK(justReady.admissions == 0); // ineligible time did not age the pressure gate
    trace.push_back(action(StartupAction::Deadline));
    auto eventual = yams::fuzz::replayStartup(trace);
    REQUIRE(eventual.ok());
    CHECK(eventual.admissions == 1);
    CHECK(eventual.forcedAdmissions == 1);
    trace.push_back(action(StartupAction::Cancel));
    trace.push_back(action(StartupAction::Deadline));
    auto cancelled = yams::fuzz::replayStartup(trace);
    REQUIRE(cancelled.ok());
    CHECK(cancelled.admissions == eventual.admissions);
}

TEST_CASE("Startup replay reaches ready with optional vectors enabled or skipped",
          "[daemon][startup-replay][readiness]") {
    yams::test::SpdlogLevelGuard logging;
    spdlog::set_level(spdlog::level::off);
    for (const std::uint8_t flags : {std::uint8_t{0}, std::uint8_t{0x80}}) {
        const std::vector<std::uint8_t> trace(7, action(StartupAction::Advance) | flags);
        const auto replay = yams::fuzz::replayStartup(trace);
        REQUIRE(replay.ok());
        CHECK(replay.admissions > 0);
        CHECK((replay.statesVisited & (1U << static_cast<unsigned>(ServiceManagerState::Ready))) !=
              0);
    }
}

TEST_CASE("Failure and shutdown cannot be revived by later startup advances",
          "[daemon][startup-replay][shutdown]") {
    yams::test::SpdlogLevelGuard logging;
    spdlog::set_level(spdlog::level::off);
    for (std::size_t phase = 0; phase < 6; ++phase) {
        CAPTURE(phase);
        std::vector<std::uint8_t> trace(phase, action(StartupAction::Advance));
        trace.insert(trace.end(),
                     {action(StartupAction::Fail), action(StartupAction::Deadline),
                      action(StartupAction::Advance), action(StartupAction::Shutdown),
                      action(StartupAction::FinishShutdown), action(StartupAction::Advance)});
        auto failed = yams::fuzz::replayStartup(trace);
        REQUIRE(failed.ok());
        CHECK(failed.admissions == 0);
        trace.push_back(action(StartupAction::Reset));
        trace.insert(trace.end(), 7, action(StartupAction::Advance));
        auto restarted = yams::fuzz::replayStartup(trace);
        REQUIRE(restarted.ok());
        CHECK(restarted.admissions > 0);
    }
}

TEST_CASE("Seeded startup schedules and float bit patterns satisfy production contracts",
          "[daemon][startup-replay][seeded]") {
    yams::test::SpdlogLevelGuard logging;
    spdlog::set_level(spdlog::level::off);
    const auto rootSeed = Catch::getSeed();
    std::uint32_t visited = 0;
    for (std::uint32_t round = 0; round < 1024; ++round) {
        CAPTURE(rootSeed, round);
        auto state = rootSeed ^ (round * 2654435761U);
        std::array<std::uint8_t, 256> trace{};
        for (auto& byte : trace) {
            state = state * 1664525U + 1013904223U;
            byte = static_cast<std::uint8_t>(state >> 24);
            // Bias enough valid advancement to exercise Ready as well as abort paths.
            if ((state & 3U) == 0) {
                byte = action(StartupAction::Advance) | (byte & 0x80U);
            }
        }
        const auto replay = yams::fuzz::replayStartup(trace);
        CAPTURE(replay.offset, replay.error);
        REQUIRE(replay.ok());
        REQUIRE(yams::fuzz::semanticScoresBounded(trace));
        visited |= replay.statesVisited;
    }
    for (auto phase : {ServiceManagerState::Ready, ServiceManagerState::Failed,
                       ServiceManagerState::ShuttingDown, ServiceManagerState::Stopped,
                       ServiceManagerState::VectorsReady}) {
        CAPTURE(phase);
        CHECK((visited & (1U << static_cast<unsigned>(phase))) != 0);
    }
}

TEST_CASE("Coverage fuzzer entry reuses the startup replay and score contracts",
          "[daemon][startup-replay][fuzzer-entry]") {
    yams::test::SpdlogLevelGuard logging;
    const std::array<std::uint8_t, 64> trace{};
    CHECK(LLVMFuzzerTestOneInput(trace.data(), trace.size()) == 0);
}

TEST_CASE("Semantic scores and explicit thresholds reject nonfinite values",
          "[daemon][startup-replay][semantic-values]") {
    using namespace yams::daemon::embed;
    const auto nan = std::numeric_limits<float>::quiet_NaN();
    const auto infinity = std::numeric_limits<float>::infinity();
    const std::vector<float> good{1.0f}, opposite{-1.0f}, zero{0.0f}, bad{nan}, huge{infinity};
    CHECK(semanticInverseNorm(zero) == 0.0f);
    CHECK(semanticInverseNorm(bad) == 0.0f);
    CHECK(semanticInverseNorm(huge) == 0.0f);
    CHECK(semanticCosineSimilarity(good, 1.0f, good, 1.0f) == 1.0f);
    CHECK(semanticCosineSimilarity(good, 1.0f, opposite, 1.0f) == -1.0f);
    CHECK(semanticCosineSimilarity(bad, 1.0f, good, 1.0f) == 0.0f);
    CHECK(semanticCosineSimilarity(good, infinity, good, 1.0f) == 0.0f);
    CHECK(semanticCosineSimilarity(good, nan, good, 1.0f) == 0.0f);
    CHECK(semanticCosineSimilarity(good, 0.0f, good, 1.0f) == 0.0f);
    CHECK(semanticCosineSimilarity(good, 1.0f, std::span<const float>{}, 1.0f) == 0.0f);
    CHECK_FALSE(semanticExplicitThreshold(nan).has_value());
    CHECK_FALSE(semanticExplicitThreshold(infinity).has_value());
    CHECK(semanticExplicitThreshold(-infinity) == std::nullopt);
    CHECK(semanticExplicitThreshold(-2.0f) == 0.0f);
    CHECK(semanticExplicitThreshold(2.0f) == 1.0f);
    CHECK(semanticExplicitThreshold(0.75f) == 0.75f);
}
} // namespace

TEST_CASE("Startup phase reporting names each phase and advances elapsed time",
          "[daemon][startup-phase][visibility]") {
    // A 50-minute stall was previously reported as a bare state number at debug level. This pins
    // the exact label per phase (a swap must fail) and that elapsed time actually advances.
    // Marker *placement* in ServiceManager is not unit-tested here: it needs a full init run.
    using yams::daemon::StartupPhase;
    using yams::daemon::startupPhaseName;
    const std::array<std::pair<StartupPhase, std::string_view>, 7> expected{{
        {StartupPhase::Idle, "idle"},
        {StartupPhase::VectorsInitialized, "vectors_initialized"},
        {StartupPhase::EmbeddingReconcile, "embedding_reconcile"},
        {StartupPhase::MemorySyncApply, "memory_sync_apply"},
        {StartupPhase::DirectP2p, "direct_p2p_recovery"},
        {StartupPhase::SearchBuild, "search_build"},
        {StartupPhase::AsyncInitComplete, "async_init_complete"},
    }};
    for (const auto& [phase, name] : expected) {
        CAPTURE(static_cast<int>(phase));
        CHECK(startupPhaseName(phase) == name);
    }
    CHECK(startupPhaseName(static_cast<StartupPhase>(200)) == "unknown");

    yams::daemon::StartupPhaseTracker tracker;
    CHECK(tracker.phase() == StartupPhase::Idle);
    CHECK(tracker.elapsedMs() == 0); // idle never reports elapsed time
    tracker.begin(StartupPhase::MemorySyncApply);
    CHECK(tracker.phase() == StartupPhase::MemorySyncApply);
    // Bounded spin, not a sleep loop: prove elapsed time advances rather than merely being >= 0.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{200};
    while (tracker.elapsedMs() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    CHECK(tracker.elapsedMs() > 0);
    tracker.begin(StartupPhase::SearchBuild);
    CHECK(tracker.phase() == StartupPhase::SearchBuild);
    CHECK(startupPhaseName(tracker.phase()) == "search_build");
}
