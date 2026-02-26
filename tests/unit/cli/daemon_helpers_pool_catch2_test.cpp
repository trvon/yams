#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <memory>
#include <thread>

#include <yams/cli/daemon_helpers.h>

using namespace std::chrono_literals;

TEST_CASE("DaemonClientPool pruning thread exits promptly on shutdown", "[cli][daemon][pool]") {
    yams::daemon::ClientConfig clientCfg;
    clientCfg.transportMode = yams::daemon::ClientTransportMode::InProcess;

    yams::cli::DaemonClientPool::Config poolCfg;
    poolCfg.min_clients = 1;
    poolCfg.max_clients = 1;
    poolCfg.idle_timeout = std::chrono::seconds(60);
    poolCfg.client_config = clientCfg;

    std::chrono::milliseconds worst{0};
    for (int i = 0; i < 3; ++i) {
        auto pool = std::make_unique<yams::cli::DaemonClientPool>(poolCfg);

        // Let the pruning thread enter its wait state before destruction.
        std::this_thread::sleep_for(80ms);

        const auto start = std::chrono::steady_clock::now();
        pool.reset();
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);
        worst = std::max(worst, elapsed);
    }

    CHECK(worst < 1100ms);
}
