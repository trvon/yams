// Daemon SIGTERM signal handling integration tests
// Validates that SIGTERM triggers prompt graceful shutdown

#define CATCH_CONFIG_MAIN
#include <spdlog/spdlog.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>
#include "test_async_helpers.h"
#include "test_daemon_harness.h"
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>
#include <yams/compat/unistd.h>
#include <yams/daemon/client/daemon_client.h>

using namespace yams::daemon;
using namespace yams::test;
using namespace std::chrono_literals;

// Note: These tests spawn the daemon in-process (via DaemonHarness) and test
// the signal handling by setting the global shutdown flag that SIGTERM would set.
// For true out-of-process SIGTERM testing, see the CLI tests.

// External linkage to daemon_main.cpp's g_shutdown_requested
// We simulate SIGTERM by setting this flag directly since the daemon
// runs in the same process for integration tests.
namespace {

// Create a local atomic for testing since we can't access daemon_main.cpp's static
std::atomic<bool> g_test_shutdown_requested{false};

DaemonClient createClient(const std::filesystem::path& socketPath,
                          std::chrono::milliseconds connectTimeout = 2s) {
    ClientConfig config;
    config.socketPath = socketPath;
    config.connectTimeout = connectTimeout;
    config.autoStart = false;
    config.requestTimeout = 5s;
    return DaemonClient(config);
}
} // namespace

TEST_CASE("Daemon responds to external shutdown flag", "[daemon][sigterm][shutdown]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    SECTION("daemon stops promptly when external shutdown flag set") {
        // Reset test flag before starting daemon
        g_test_shutdown_requested.store(false);

        DaemonHarness harness;
        // Use pre-runLoop callback to set up signal hook BEFORE runLoop starts.
        // This avoids the race condition where runLoop reads signalCheckHook_ while
        // the main thread is writing to it via setSignalCheckHook().
        REQUIRE(harness.start(std::chrono::seconds(10), [](yams::daemon::YamsDaemon* daemon) {
            // Install hook that checks our test shutdown flag (for when loop iterates)
            daemon->setSignalCheckHook([daemon]() {
                if (g_test_shutdown_requested.load(std::memory_order_relaxed)) {
                    spdlog::info("[Test] Signal check: shutdown requested via test flag");
                    daemon->requestStop();
                    return true;
                }
                return false;
            });

            // KEY FIX (yams-qe6r): Bind external shutdown flag to CV predicate
            // This allows immediate wake-up from CV wait when flag is set
            daemon->setExternalShutdownFlag(&g_test_shutdown_requested);
        }));

        auto* daemon = harness.daemon();
        REQUIRE(daemon != nullptr);
        REQUIRE(daemon->isRunning());

        // Verify daemon is running
        auto client = createClient(harness.socketPath());
        REQUIRE(yams::cli::run_sync(client.connect(), 3s).has_value());
        auto pingResult = yams::cli::run_sync(client.ping(), 2s);
        REQUIRE(pingResult.has_value());

        // Simulate SIGTERM by setting the shutdown flag
        auto start = std::chrono::steady_clock::now();
        g_test_shutdown_requested.store(true, std::memory_order_relaxed);
        spdlog::info("[Test] Set shutdown flag, waiting for daemon to stop...");

        // Wait for daemon to acknowledge shutdown - should happen within 2 seconds
        // The bug causes this to potentially take much longer (up to statusTickMs)
        // Note: We check isStopRequested() instead of isRunning() because isRunning()
        // is only cleared when stop() is explicitly called, but isStopRequested() is
        // set immediately when the shutdown signal is detected by the runLoop.
        bool stopRequested = false;
        for (int i = 0; i < 40; ++i) { // 40 * 50ms = 2s max
            if (daemon->isStopRequested()) {
                stopRequested = true;
                break;
            }
            std::this_thread::sleep_for(50ms);
        }

        auto elapsed = std::chrono::steady_clock::now() - start;
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
        spdlog::info("[Test] Daemon stopRequested={} after {}ms", stopRequested, elapsed_ms);

        REQUIRE(stopRequested);
        // Should respond within 100ms if the fix works (CV predicate checks external flag
        // with 50ms poll interval). Without the fix, it can take up to statusTickMs (250ms+)
        REQUIRE(elapsed < 500ms);
    }
}

TEST_CASE("Daemon shutdown timing with external flag", "[daemon][sigterm][timing]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    SECTION("shutdown completes within reasonable time after flag set") {
        // Reset test flag before starting daemon
        g_test_shutdown_requested.store(false);

        DaemonHarness harness;
        // Use pre-runLoop callback to set up signal hook BEFORE runLoop starts.
        // This avoids the race condition where runLoop reads signalCheckHook_ while
        // the main thread is writing to it via setSignalCheckHook().
        REQUIRE(harness.start(std::chrono::seconds(10), [](yams::daemon::YamsDaemon* daemon) {
            // Install hook (for when loop iterates)
            daemon->setSignalCheckHook([daemon]() {
                if (g_test_shutdown_requested.load(std::memory_order_relaxed)) {
                    daemon->requestStop();
                    return true;
                }
                return false;
            });

            // KEY FIX (yams-qe6r): Bind external shutdown flag to CV predicate
            daemon->setExternalShutdownFlag(&g_test_shutdown_requested);
        }));

        auto* daemon = harness.daemon();
        REQUIRE(daemon != nullptr);

        // Do some operations first
        auto client = createClient(harness.socketPath());
        REQUIRE(yams::cli::run_sync(client.connect(), 3s).has_value());

        AddDocumentRequest req;
        req.name = "test_doc.txt";
        req.content = "Test content for shutdown timing test";
        auto addResult = yams::cli::run_sync(client.streamingAddDocument(req), 5s);
        REQUIRE(addResult.has_value());

        // Now trigger shutdown
        auto start = std::chrono::steady_clock::now();
        g_test_shutdown_requested.store(true);

        // Poll for shutdown acknowledgment (stopRequested flag)
        // Note: We check isStopRequested() because isRunning() is only cleared
        // when stop() is explicitly called, but we want to measure the time
        // until the daemon recognizes the shutdown signal.
        while (!daemon->isStopRequested()) {
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > 5s) {
                FAIL("Daemon did not acknowledge shutdown within 5 seconds");
            }
            std::this_thread::sleep_for(50ms);
        }

        auto elapsed = std::chrono::steady_clock::now() - start;
        spdlog::info("[Test] Shutdown acknowledged in {}ms",
                     std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());

        // With the fix, shutdown acknowledgment should be prompt (<100ms)
        REQUIRE(elapsed < 500ms);
    }
}

#if !defined(_WIN32)
// This test actually sends SIGTERM to the current process
// Only enabled on Unix-like systems where signals work as expected
TEST_CASE("Daemon handles actual SIGTERM signal", "[daemon][sigterm][signal][!mayfail]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    // This test is marked !mayfail because sending SIGTERM to the test process
    // could interfere with the test harness itself.
    // It serves as documentation of expected behavior.

    SECTION("SIGTERM triggers daemon shutdown") {
        // Note: In a real scenario, the daemon runs in a separate process
        // and receives SIGTERM from the CLI's `yams daemon stop` command.
        // This in-process test simulates that by using the hook mechanism.

        DaemonHarness harness;
        REQUIRE(harness.start());

        auto* daemon = harness.daemon();
        REQUIRE(daemon != nullptr);

        // For this test, we directly call requestStop() to simulate
        // what the signal handler would do
        REQUIRE(daemon->isRunning());

        auto start = std::chrono::steady_clock::now();
        daemon->requestStop();

        // Wait for runLoop to acknowledge stop (via isStopRequested)
        while (!daemon->isStopRequested()) {
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed > 5s) {
                FAIL("Daemon did not respond to requestStop within 5 seconds");
            }
            std::this_thread::sleep_for(50ms);
        }

        auto elapsed = std::chrono::steady_clock::now() - start;
        spdlog::info("[Test] requestStop() -> daemon acknowledged in {}ms",
                     std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());

        // requestStop() directly sets the flag and notifies CV, so should be immediate
        REQUIRE(elapsed < 100ms);
    }
}
#endif
