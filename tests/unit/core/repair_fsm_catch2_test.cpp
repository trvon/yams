// RepairFsm: one repair cycle moves forward through its stages, never backward, and returns to
// Idle through reset(). A failed cycle parks in Error and may be retried a bounded number of
// times.

#include <catch2/catch_test_macros.hpp>

#include <yams/core/repair_fsm.h>

#include <cstdint>
#include <utility>
#include <vector>

using yams::core::RepairFsm;
using State = yams::core::RepairFsm::State;

namespace {

const std::vector<State> kStageOrder{State::Scan, State::Detect, State::Fix, State::Reindex,
                                     State::Complete};

} // namespace

TEST_CASE("RepairFsm walks the full stage order and resets to Idle", "[unit][core][repair][fsm]") {
    RepairFsm fsm;
    REQUIRE(fsm.state() == State::Idle);

    REQUIRE(fsm.start());
    CHECK(fsm.state() == State::Scan);
    for (std::size_t i = 1; i < kStageOrder.size(); ++i) {
        REQUIRE(fsm.advance(kStageOrder[i]));
        CHECK(fsm.state() == kStageOrder[i]);
    }
    CHECK(fsm.state() == State::Complete);

    REQUIRE(fsm.reset());
    CHECK(fsm.state() == State::Idle);

    // The next cycle starts cleanly.
    REQUIRE(fsm.start());
    CHECK(fsm.state() == State::Scan);
}

TEST_CASE("RepairFsm lets a cycle skip stages that have no work", "[unit][core][repair][fsm]") {
    RepairFsm fsm;
    REQUIRE(fsm.start());
    REQUIRE(fsm.advance(State::Detect));
    REQUIRE(fsm.advance(State::Reindex)); // nothing to fix this cycle
    REQUIRE(fsm.advance(State::Complete));
    CHECK(fsm.state() == State::Complete);
}

TEST_CASE("RepairFsm rejects illegal transitions and leaves the state alone",
          "[unit][core][repair][fsm]") {
    RepairFsm fsm;
    std::vector<State> seen;
    fsm.set_on_state_change([&seen](State s) { seen.push_back(s); });

    SECTION("nothing but start() leaves Idle") {
        CHECK_FALSE(fsm.advance(State::Detect));
        CHECK_FALSE(fsm.advance(State::Complete));
        CHECK_FALSE(fsm.fail());
        CHECK_FALSE(fsm.retry());
        CHECK_FALSE(fsm.reset());
        CHECK(fsm.state() == State::Idle);
        CHECK(seen.empty());
    }

    SECTION("start() is rejected while a cycle is active") {
        REQUIRE(fsm.start());
        CHECK_FALSE(fsm.start());
        CHECK(fsm.state() == State::Scan);
    }

    SECTION("a stage cannot be re-entered or revisited") {
        REQUIRE(fsm.start());
        REQUIRE(fsm.advance(State::Fix));
        seen.clear();
        CHECK_FALSE(fsm.advance(State::Fix));
        CHECK_FALSE(fsm.advance(State::Detect));
        CHECK_FALSE(fsm.advance(State::Scan));
        CHECK(fsm.state() == State::Fix);
        CHECK(seen.empty());
    }

    SECTION("advance() cannot target Idle or Error") {
        REQUIRE(fsm.start());
        CHECK_FALSE(fsm.advance(State::Idle));
        CHECK_FALSE(fsm.advance(State::Error));
        CHECK(fsm.state() == State::Scan);
    }

    SECTION("a completed cycle only resets") {
        REQUIRE(fsm.start());
        REQUIRE(fsm.advance(State::Complete));
        CHECK_FALSE(fsm.advance(State::Complete));
        CHECK_FALSE(fsm.start());
        CHECK_FALSE(fsm.fail());
        CHECK_FALSE(fsm.retry());
        CHECK(fsm.state() == State::Complete);
    }

    SECTION("a failed cycle does not advance") {
        REQUIRE(fsm.start());
        REQUIRE(fsm.fail());
        CHECK_FALSE(fsm.advance(State::Detect));
        CHECK_FALSE(fsm.start());
        CHECK_FALSE(fsm.fail());
        CHECK(fsm.state() == State::Error);
    }
}

TEST_CASE("RepairFsm fails from any active stage and resets to Idle", "[unit][core][repair][fsm]") {
    for (std::size_t i = 0; i + 1 < kStageOrder.size(); ++i) { // every stage before Complete
        RepairFsm fsm;
        REQUIRE(fsm.start());
        if (i > 0) {
            REQUIRE(fsm.advance(kStageOrder[i]));
        }
        REQUIRE(fsm.fail());
        CHECK(fsm.state() == State::Error);
        REQUIRE(fsm.reset());
        CHECK(fsm.state() == State::Idle);
    }
}

TEST_CASE("RepairFsm abandons an active cycle through reset()", "[unit][core][repair][fsm]") {
    RepairFsm fsm;
    REQUIRE(fsm.start());
    REQUIRE(fsm.advance(State::Detect));
    REQUIRE(fsm.reset());
    CHECK(fsm.state() == State::Idle);
}

TEST_CASE("RepairFsm retries a failed cycle up to max_retries with growing backoff",
          "[unit][core][repair][fsm]") {
    RepairFsm::Config cfg;
    cfg.max_retries = 2;
    cfg.repair_backoff_ms = 100;
    RepairFsm fsm(cfg);

    std::vector<std::pair<std::uint32_t, std::uint32_t>> backoffs;
    fsm.set_on_backoff([&backoffs](std::uint32_t attempt, std::uint32_t backoffMs) {
        backoffs.emplace_back(attempt, backoffMs);
    });

    REQUIRE(fsm.start());
    REQUIRE(fsm.advance(State::Fix));
    REQUIRE(fsm.fail());
    CHECK(fsm.retries() == 0);

    REQUIRE(fsm.retry()); // attempt 1
    CHECK(fsm.state() == State::Scan);
    CHECK(fsm.retries() == 1);

    REQUIRE(fsm.fail());
    REQUIRE(fsm.retry()); // attempt 2
    CHECK(fsm.retries() == 2);

    REQUIRE(fsm.fail());
    CHECK_FALSE(fsm.retry()); // budget spent: the cycle stays failed
    CHECK(fsm.state() == State::Error);
    CHECK(fsm.retries() == 2);

    REQUIRE(backoffs.size() == 2);
    CHECK(backoffs[0] == std::pair<std::uint32_t, std::uint32_t>{1, 100});
    CHECK(backoffs[1] == std::pair<std::uint32_t, std::uint32_t>{2, 200});

    // reset() restores the retry budget for the next cycle.
    REQUIRE(fsm.reset());
    CHECK(fsm.retries() == 0);
    REQUIRE(fsm.start());
    REQUIRE(fsm.fail());
    CHECK(fsm.retry());
}

TEST_CASE("RepairFsm with max_retries 0 never retries", "[unit][core][repair][fsm]") {
    RepairFsm::Config cfg;
    cfg.max_retries = 0;
    RepairFsm fsm(cfg);
    REQUIRE(fsm.start());
    REQUIRE(fsm.fail());
    CHECK_FALSE(fsm.retry());
    CHECK(fsm.state() == State::Error);
}

TEST_CASE("RepairFsm backoff saturates instead of overflowing", "[unit][core][repair][fsm]") {
    RepairFsm::Config cfg;
    cfg.max_retries = 64;
    cfg.repair_backoff_ms = 0x40000000U;
    RepairFsm fsm(cfg);
    std::uint32_t last = 0;
    fsm.set_on_backoff([&last](std::uint32_t, std::uint32_t backoffMs) { last = backoffMs; });

    REQUIRE(fsm.start());
    for (int i = 0; i < 40; ++i) {
        REQUIRE(fsm.fail());
        REQUIRE(fsm.retry());
        CHECK(last >= cfg.repair_backoff_ms);
    }
    CHECK(last == 0xFFFFFFFFU);
}

TEST_CASE("RepairFsm reports every accepted transition to the callback, in order",
          "[unit][core][repair][fsm]") {
    RepairFsm fsm;
    std::vector<State> seen;
    fsm.set_on_state_change([&seen](State s) { seen.push_back(s); });

    REQUIRE(fsm.start());
    REQUIRE(fsm.advance(State::Detect));
    CHECK_FALSE(fsm.advance(State::Scan)); // rejected: no callback
    REQUIRE(fsm.advance(State::Fix));
    REQUIRE(fsm.fail());
    REQUIRE(fsm.retry());
    REQUIRE(fsm.advance(State::Complete));
    REQUIRE(fsm.reset());

    const std::vector<State> expected{State::Scan, State::Detect,   State::Fix, State::Error,
                                      State::Scan, State::Complete, State::Idle};
    CHECK(seen == expected);
}
