// RepairService drives core::RepairFsm through the stages its background loop really performs.
//
// The service here has an empty context: the loop takes the queued document off the pending
// queue and runs detection, which finds nothing to dispatch. That cycle is Scan -> Detect ->
// Complete -> Idle; the Fix and Reindex stages are entered only when there is work to dispatch.

#include <catch2/catch_test_macros.hpp>

#include <yams/core/repair_fsm.h>
#include <yams/daemon/components/RepairService.h>

#include <chrono>
#include <cstddef>
#include <future>
#include <mutex>
#include <string>
#include <vector>

using namespace yams::daemon;
using State = yams::core::RepairFsm::State;

TEST_CASE("RepairService background loop walks RepairFsm through a batch cycle",
          "[unit][daemon][repair][fsm][catch2]") {
    RepairService::Config cfg;
    cfg.enable = true;
    RepairService service(RepairServiceContext{}, nullptr, [] { return std::size_t{0}; }, cfg);

    std::mutex mutex;
    std::vector<State> seen;
    std::promise<void> cycleDone;
    auto cycleDoneFuture = cycleDone.get_future();
    bool signalled = false;
    service.testing_setRepairStageObserver([&](State s) {
        std::lock_guard<std::mutex> lk(mutex);
        seen.push_back(s);
        if (s == State::Idle && !signalled) {
            signalled = true;
            cycleDone.set_value();
        }
    });

    service.start();
    service.enqueueEmbeddingRepair({std::string(64, 'a')});

    const bool finished =
        cycleDoneFuture.wait_for(std::chrono::seconds(20)) == std::future_status::ready;
    service.stop();
    REQUIRE(finished);

    std::lock_guard<std::mutex> lk(mutex);
    REQUIRE(seen.size() >= 4);
    const std::vector<State> firstCycle(seen.begin(), seen.begin() + 4);
    CHECK(firstCycle ==
          std::vector<State>{State::Scan, State::Detect, State::Complete, State::Idle});
}
