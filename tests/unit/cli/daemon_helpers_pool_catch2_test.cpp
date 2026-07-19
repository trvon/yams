#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <memory>
#include <thread>

#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/sandbox_probe.h>

#include "../../common/test_helpers_catch2.h"

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

TEST_CASE("is_transport_failure classifies 'Daemon not ready yet' as transport",
          "[cli][daemon][transport]") {
    using yams::Error;
    using yams::ErrorCode;

    yams::Error notReady{ErrorCode::InvalidState,
                         "Daemon not ready yet for list (status=initializing)"};
    CHECK(yams::cli::is_transport_failure(notReady));

    yams::Error timeout{ErrorCode::Timeout, "deadline exceeded"};
    CHECK(yams::cli::is_transport_failure(timeout));

    yams::Error otherInvalid{ErrorCode::InvalidState, "unrelated invalid state"};
    CHECK_FALSE(yams::cli::is_transport_failure(otherInvalid));

    yams::Error notFound{ErrorCode::NotFound, "missing key"};
    CHECK_FALSE(yams::cli::is_transport_failure(notFound));
}

TEST_CASE("Explicit pinned socket bypasses the generic sandbox probe", "[cli][daemon][transport]") {
    struct ProbeReset {
        ~ProbeReset() { yams::daemon::set_unix_socket_probe_for_tests(nullptr); }
    } probeReset;

    yams::daemon::set_unix_socket_probe_for_tests([] { return false; });
    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::nullopt);
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar config(
        "YAMS_CONFIG", std::string("/nonexistent/yams-daemon-helper-test-config.toml"));
    yams::test::ScopedEnvVar socketPath("YAMS_DAEMON_SOCKET_PATH",
                                        std::string("/tmp/yams-pinned-test.sock"));
    yams::test::ScopedEnvVar legacySocket("YAMS_DAEMON_SOCKET", std::nullopt);
    yams::test::ScopedEnvVar noAutoStart("YAMS_CLI_DISABLE_DAEMON_AUTOSTART", std::string("1"));

    yams::daemon::ClientConfig configRequest;
    auto plan = yams::cli::prepare_cli_daemon_client_plan(
        configRequest, yams::cli::CliDaemonAccessPolicy::AllowInProcessFallback);

    REQUIRE(plan);
    const auto& resolved = plan.value();
    CHECK(resolved.resolvedMode == yams::daemon::ClientTransportMode::Socket);
    CHECK(resolved.config.transportMode == yams::daemon::ClientTransportMode::Socket);
    CHECK(resolved.config.socketPath == "/tmp/yams-pinned-test.sock");
    CHECK(resolved.requireSocket);
    CHECK_FALSE(resolved.allowInProcessFallback);
    CHECK_FALSE(resolved.config.autoStart);
}
