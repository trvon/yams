#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <thread>
#include <vector>
#include <yams/daemon/components/SearchAdmissionController.h>

using namespace yams::daemon;

TEST_CASE("SearchAdmissionController basic lifecycle", "[daemon][search-admission][catch2]") {
    SearchAdmissionController ctrl;

    CHECK(ctrl.active() == 0);
    CHECK(ctrl.queued() == 0);

    ctrl.onQueued();
    CHECK(ctrl.queued() == 1);

    REQUIRE(ctrl.tryStart(4));
    CHECK(ctrl.active() == 1);
    CHECK(ctrl.queued() == 0);

    ctrl.onFinished();
    CHECK(ctrl.active() == 0);
}

TEST_CASE("SearchAdmissionController enforces concurrency cap",
          "[daemon][search-admission][catch2]") {
    SearchAdmissionController ctrl;
    constexpr std::uint32_t cap = 2;

    ctrl.onQueued();
    REQUIRE(ctrl.tryStart(cap));
    ctrl.onQueued();
    REQUIRE(ctrl.tryStart(cap));

    ctrl.onQueued();
    CHECK_FALSE(ctrl.tryStart(cap));
    CHECK(ctrl.active() == 2);
    CHECK(ctrl.queued() == 1);

    ctrl.onFinished();
    REQUIRE(ctrl.tryStart(cap));
    CHECK(ctrl.active() == 2);
    CHECK(ctrl.queued() == 0);
}

TEST_CASE("SearchAdmissionController cap=0 means unlimited", "[daemon][search-admission][catch2]") {
    SearchAdmissionController ctrl;

    for (int i = 0; i < 100; ++i) {
        ctrl.onQueued();
        REQUIRE(ctrl.tryStart(0));
    }
    CHECK(ctrl.active() == 100);
    CHECK(ctrl.queued() == 0);
}

TEST_CASE("SearchAdmissionController onRejected decrements queued",
          "[daemon][search-admission][catch2]") {
    SearchAdmissionController ctrl;

    ctrl.onQueued();
    ctrl.onQueued();
    ctrl.onRejected();
    CHECK(ctrl.queued() == 1);
    CHECK(ctrl.active() == 0);
}

TEST_CASE("SearchAdmissionController concurrent tryStart respects cap",
          "[daemon][search-admission][catch2]") {
    SearchAdmissionController ctrl;
    constexpr std::uint32_t cap = 8;
    constexpr int totalAttempts = 200;

    for (int i = 0; i < totalAttempts; ++i)
        ctrl.onQueued();

    std::atomic<int> started{0};
    std::vector<std::thread> threads;
    threads.reserve(totalAttempts);

    for (int i = 0; i < totalAttempts; ++i) {
        threads.emplace_back([&] {
            if (ctrl.tryStart(cap))
                started.fetch_add(1, std::memory_order_relaxed);
        });
    }

    for (auto& t : threads)
        t.join();

    CHECK(ctrl.active() <= cap);
    CHECK(started.load() <= cap);
}
