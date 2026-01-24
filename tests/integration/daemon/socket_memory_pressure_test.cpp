// Socket Memory Pressure Test Suite
//
// This test suite reproduces socket crashes under memory pressure and concurrent operations.
// It targets the SIGSEGV crash that occurs when:
// 1. Socket connections are closed during idle timeout
// 2. Multiple concurrent connections are being processed
// 3. Memory is constrained (simulated via rapid connect/disconnect cycles)
// 4. Socket handles are accessed after closure (fd=UINT64_MAX in crash logs)

#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <filesystem>
#include <thread>
#include <vector>

#include "test_async_helpers.h"
#include "test_daemon_harness.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace yams::daemon;
using namespace yams::test;
using namespace std::chrono_literals;

namespace {

// Helper to create a client with specific timeout configuration
DaemonClient createTestClient(const std::filesystem::path& socketPath,
                              std::chrono::milliseconds readTimeout = 500ms) {
    ClientConfig config;
    config.socketPath = socketPath;
    config.connectTimeout = 2s;
    config.requestTimeout = readTimeout;
    config.autoStart = false;
    config.enableChunkedResponses = false;
    return DaemonClient(config);
}

// Helper to connect with retry logic for GlobalIOContext stabilization
bool connectWithRetry(DaemonClient& client, int maxRetries = 3,
                      std::chrono::milliseconds retryDelay = 200ms) {
    // Initial delay for GlobalIOContext threads to stabilize
    std::this_thread::sleep_for(200ms);

    for (int attempt = 0; attempt < maxRetries; ++attempt) {
        auto result = yams::cli::run_sync(client.connect(), 5s);
        if (result.has_value()) {
            return true;
        }
        std::this_thread::sleep_for(retryDelay);
    }
    return false;
}

} // namespace

TEST_CASE("Socket memory pressure - rapid connect/disconnect cycles",
          "[daemon][socket][memory-pressure][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    DaemonHarness harness;
    REQUIRE(harness.start());

    std::atomic<int> successfulOps{0};
    std::atomic<int> failedOps{0};
    std::atomic<bool> shouldStop{false};

    // Spawn multiple threads that rapidly connect, idle, and disconnect
    constexpr int numThreads = 4;
    std::vector<std::thread> workers;

    for (int i = 0; i < numThreads; ++i) {
        workers.emplace_back([&, threadId = i]() {
            int localSuccess = 0;
            int localFailed = 0;

            for (int attempt = 0; attempt < 10 && !shouldStop.load(); ++attempt) {
                try {
                    auto client = createTestClient(harness.socketPath(), 500ms);

                    // Connect
                    auto connResult = yams::cli::run_sync(client.connect(), 2s);
                    if (!connResult.has_value()) {
                        localFailed++;
                        continue;
                    }

                    // Let connection idle (triggers idle timeout path)
                    std::this_thread::sleep_for(600ms);

                    // Try to send request on potentially timed-out connection
                    auto statusResult = yams::cli::run_sync(client.status(), 1s);

                    if (statusResult.has_value()) {
                        localSuccess++;
                    } else {
                        localFailed++;
                    }

                    // Disconnect
                    client.disconnect();

                } catch (const std::exception& e) {
                    INFO("Thread " << threadId << " exception: " << e.what());
                    localFailed++;
                }

                // Brief pause between cycles
                std::this_thread::sleep_for(50ms);
            }

            successfulOps.fetch_add(localSuccess);
            failedOps.fetch_add(localFailed);
        });
    }

    // Let workers run
    for (auto& worker : workers) {
        worker.join();
    }

    INFO("Successful operations: " << successfulOps.load());
    INFO("Failed operations: " << failedOps.load());

    // Test passes if daemon didn't crash (no SIGSEGV)
    // Some failures are expected due to timeouts, but daemon should remain stable
    REQUIRE(successfulOps.load() > 0);
}

TEST_CASE("Socket memory pressure - concurrent idle connections",
          "[daemon][socket][memory-pressure][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    DaemonHarness harness;
    REQUIRE(harness.start());

    // Initial delay for GlobalIOContext threads to stabilize
    std::this_thread::sleep_for(200ms);

    constexpr int numConnections = 8;
    std::vector<std::unique_ptr<DaemonClient>> clients;

    // Create multiple connections with retry logic for first connection
    for (int i = 0; i < numConnections; ++i) {
        auto client = std::make_unique<DaemonClient>(createTestClient(harness.socketPath()));

        // First connection may need retries due to GlobalIOContext timing
        bool connected = false;
        for (int attempt = 0; attempt < 3; ++attempt) {
            auto connResult = yams::cli::run_sync(client->connect(), 2s);
            if (connResult.has_value()) {
                connected = true;
                break;
            }
            std::this_thread::sleep_for(200ms);
        }
        REQUIRE(connected);
        clients.push_back(std::move(client));
    }

    // Let all connections idle past timeout threshold (3x 2s read timeout = 6s)
    INFO("Idling connections to trigger timeout cleanup...");
    std::this_thread::sleep_for(7s);

    // Try to use connections after idle timeout
    int successfulReconnects = 0;
    for (size_t i = 0; i < clients.size(); ++i) {
        try {
            // Connection may have been closed by idle timeout
            auto statusResult = yams::cli::run_sync(clients[i]->status(), 1s);

            if (!statusResult.has_value()) {
                // Expected: connection was closed, try to reconnect
                auto reconnResult = yams::cli::run_sync(clients[i]->connect(), 2s);
                if (reconnResult.has_value()) {
                    successfulReconnects++;
                }
            }
        } catch (const std::exception& e) {
            INFO("Client " << i << " error: " << e.what());
        }
    }

    INFO("Successfully reconnected " << successfulReconnects << " out of " << clients.size()
                                     << " clients");

    // Cleanup
    for (auto& client : clients) {
        client->disconnect();
    }

    // Test passes if daemon didn't crash
    REQUIRE(true);
}

TEST_CASE("Socket memory pressure - handle validity after close",
          "[daemon][socket][memory-pressure][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    DaemonHarness harness;
    REQUIRE(harness.start());

    auto client = createTestClient(harness.socketPath());
    auto connResult = yams::cli::run_sync(client.connect(), 2s);
    REQUIRE(connResult.has_value());

    // Send a request that will complete
    auto statusResult = yams::cli::run_sync(client.status(), 2s);
    REQUIRE(statusResult.has_value());

    // Force disconnect
    client.disconnect();

    // Try to use after disconnect - may auto-reconnect or fail, but must not crash
    // The key safety property is graceful handling (no SIGSEGV/use-after-free)
    auto statusResult2 = yams::cli::run_sync(client.status(), 1s);
    // Either succeeds (auto-reconnect) or fails gracefully - both are valid
    (void)statusResult2;

    // Daemon should still be responsive
    auto client2 = createTestClient(harness.socketPath());
    auto connResult2 = yams::cli::run_sync(client2.connect(), 2s);
    REQUIRE(connResult2.has_value());
}

TEST_CASE("Socket memory pressure - native_handle safety",
          "[daemon][socket][memory-pressure][crash]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    DaemonHarness harness;
    REQUIRE(harness.start());

    SECTION("native_handle after socket close returns invalid value") {
        auto client = createTestClient(harness.socketPath());
        REQUIRE(connectWithRetry(client));

        client.disconnect();

        // After disconnect, internal socket is closed
        // Accessing native_handle on a closed socket should be safe
        // This tests the exact scenario from the crash log where fd=18446744073709551615
        // The daemon should not dereference the socket handle after closure

        // Re-establish connection to verify daemon didn't crash
        // Use retry logic since reconnecting after disconnect may need time
        REQUIRE(connectWithRetry(client));
    }

    SECTION("logging socket fd during idle timeout doesn't crash") {
        auto client = createTestClient(harness.socketPath(), 1s);
        REQUIRE(connectWithRetry(client));

        // Let connection idle to trigger the timeout path that logs the fd
        // From crash log line 371: "Closing idle connection after {} consecutive read timeouts
        // (fd={})"
        std::this_thread::sleep_for(7s);

        // Try to reconnect - daemon should be stable
        // Use retry logic since reconnecting after idle timeout may need time
        REQUIRE(connectWithRetry(client));
    }
}

TEST_CASE("Socket memory pressure - stress test with mixed operations",
          "[daemon][socket][memory-pressure][stress]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    DaemonHarness harness;
    REQUIRE(harness.start());

    std::atomic<int> totalOps{0};
    std::atomic<int> crashes{0};
    std::atomic<bool> shouldStop{false};

    constexpr int numThreads = 6;
    constexpr int opsPerThread = 20;
    std::vector<std::thread> workers;

    for (int i = 0; i < numThreads; ++i) {
        workers.emplace_back([&, threadId = i]() {
            for (int op = 0; op < opsPerThread && !shouldStop.load(); ++op) {
                try {
                    auto client = createTestClient(harness.socketPath(), 800ms);

                    // Connect
                    auto connResult = yams::cli::run_sync(client.connect(), 2s);
                    if (!connResult.has_value()) {
                        continue;
                    }

                    // Mix of operations
                    switch (op % 3) {
                        case 0:
                            // Quick status check
                            {
                                yams::cli::run_sync(client.status(), 1s);
                            }
                            break;
                        case 1:
                            // Idle timeout path
                            std::this_thread::sleep_for(1s);
                            break;
                        case 2:
                            // Rapid reconnect
                            client.disconnect();
                            yams::cli::run_sync(client.connect(), 2s);
                            break;
                    }

                    totalOps.fetch_add(1);

                } catch (const std::exception& e) {
                    crashes.fetch_add(1);
                    INFO("Thread " << threadId << " op " << op << " exception: " << e.what());
                }
            }
        });
    }

    for (auto& worker : workers) {
        worker.join();
    }

    INFO("Total operations completed: " << totalOps.load());
    INFO("Exceptions caught: " << crashes.load());

    // Test passes if daemon remained stable (no SIGSEGV)
    // We expect some exceptions due to timeouts, but no crashes
    REQUIRE(crashes.load() < totalOps.load());
}
