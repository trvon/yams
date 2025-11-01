// Daemon shutdown behavior integration tests
// Validates graceful/force shutdown, timing, idempotency, and in-flight operations

#define CATCH_CONFIG_MAIN
#include <atomic>
#include <thread>
#include "test_async_helpers.h"
#include "test_daemon_harness.h"
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/client/daemon_client.h>

using namespace yams::daemon;
using namespace yams::test;
using namespace std::chrono_literals;

namespace {
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

TEST_CASE("Daemon shutdown timing", "[daemon][shutdown][timing]") {
    DaemonHarness harness;
    REQUIRE(harness.start());

    auto client = createClient(harness.socketPath());
    REQUIRE(yams::cli::run_sync(client.connect(), 3s).has_value());

    SECTION("shutdown request returns immediately") {
        auto start = std::chrono::steady_clock::now();
        auto result = yams::cli::run_sync(client.shutdown(true), 5s);
        auto elapsed = std::chrono::steady_clock::now() - start;

        REQUIRE(result.has_value());
        REQUIRE(elapsed < 500ms);
    }
}

TEST_CASE("Daemon shutdown with in-flight operations", "[daemon][shutdown][operations]") {
    DaemonHarness harness;
    REQUIRE(harness.start());

    auto client = createClient(harness.socketPath());
    REQUIRE(yams::cli::run_sync(client.connect(), 3s).has_value());

    SECTION("shutdown accepts requests to completion") {
        std::atomic<bool> operationComplete{false};
        std::atomic<bool> operationStarted{false};

        std::thread bgThread([&]() {
            AddDocumentRequest req;
            req.name = "test_doc.txt";
            req.content = "Test content";

            operationStarted = true;
            auto result = yams::cli::run_sync(client.streamingAddDocument(req), 10s);
            operationComplete = result.has_value();
        });

        while (!operationStarted) {
            std::this_thread::sleep_for(10ms);
        }
        std::this_thread::sleep_for(50ms);

        auto shutdownResult = yams::cli::run_sync(client.shutdown(true), 5s);
        REQUIRE(shutdownResult.has_value());

        bgThread.join();

        REQUIRE(operationComplete);
    }

    SECTION("multiple operations complete before shutdown") {
        constexpr int numOps = 3;
        std::atomic<int> completedOps{0};
        std::vector<std::thread> threads;

        for (int i = 0; i < numOps; ++i) {
            threads.emplace_back([&, i]() {
                AddDocumentRequest req;
                req.name = "doc_" + std::to_string(i) + ".txt";
                req.content = "Content " + std::to_string(i);

                auto result = yams::cli::run_sync(client.streamingAddDocument(req), 10s);
                if (result.has_value()) {
                    completedOps++;
                }
            });
        }

        std::this_thread::sleep_for(100ms);

        auto shutdownResult = yams::cli::run_sync(client.shutdown(true), 5s);
        REQUIRE(shutdownResult.has_value());

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(completedOps == numOps);
    }
}

TEST_CASE("Daemon shutdown idempotency", "[daemon][shutdown][idempotent]") {
    DaemonHarness harness;
    REQUIRE(harness.start());

    auto client = createClient(harness.socketPath());
    REQUIRE(yams::cli::run_sync(client.connect(), 3s).has_value());

    SECTION("multiple shutdown requests are harmless") {
        auto result1 = yams::cli::run_sync(client.shutdown(true), 3s);
        REQUIRE(result1.has_value());

        std::this_thread::sleep_for(50ms);

        auto result2 = yams::cli::run_sync(client.shutdown(true), 3s);
    }

    SECTION("concurrent shutdown requests don't crash") {
        std::vector<std::thread> threads;
        std::atomic<int> successCount{0};

        for (int i = 0; i < 3; ++i) {
            threads.emplace_back([&]() {
                auto result = yams::cli::run_sync(client.shutdown(true), 3s);
                if (result.has_value()) {
                    successCount++;
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(successCount >= 1);
    }
}

TEST_CASE("Daemon shutdown after operations", "[daemon][shutdown][lifecycle]") {
    DaemonHarness harness;
    REQUIRE(harness.start());

    auto client = createClient(harness.socketPath());
    REQUIRE(yams::cli::run_sync(client.connect(), 3s).has_value());

    SECTION("shutdown after document operations") {
        for (int i = 0; i < 5; ++i) {
            AddDocumentRequest req;
            req.name = "doc_" + std::to_string(i) + ".txt";
            req.content = "Test content " + std::to_string(i);

            auto result = yams::cli::run_sync(client.streamingAddDocument(req), 5s);
            REQUIRE(result.has_value());
        }

        ListRequest listReq;
        listReq.limit = 10;
        auto listResult = yams::cli::run_sync(client.list(listReq), 5s);
        REQUIRE(listResult.has_value());
        REQUIRE(listResult.value().items.size() >= 5);

        auto shutdownResult = yams::cli::run_sync(client.shutdown(true), 5s);
        REQUIRE(shutdownResult.has_value());
    }

    SECTION("shutdown after status and ping operations") {
        auto statusResult = yams::cli::run_sync(client.status(), 5s);
        REQUIRE(statusResult.has_value());

        auto pingResult = yams::cli::run_sync(client.ping(), 5s);
        REQUIRE(pingResult.has_value());

        auto shutdownResult = yams::cli::run_sync(client.shutdown(true), 5s);
        REQUIRE(shutdownResult.has_value());
    }
}

TEST_CASE("Daemon shutdown under load", "[daemon][shutdown][stress]") {
    DaemonHarness harness;
    REQUIRE(harness.start());

    auto client = createClient(harness.socketPath());
    REQUIRE(yams::cli::run_sync(client.connect(), 3s).has_value());

    SECTION("shutdown with multiple concurrent clients") {
        std::vector<std::thread> clientThreads;
        std::atomic<int> successfulOps{0};
        constexpr int numClients = 5;

        for (int i = 0; i < numClients; ++i) {
            clientThreads.emplace_back([&, i]() {
                auto localClient = createClient(harness.socketPath());
                if (!yams::cli::run_sync(localClient.connect(), 3s).has_value()) {
                    return;
                }

                for (int j = 0; j < 3; ++j) {
                    AddDocumentRequest req;
                    req.name = "client_" + std::to_string(i) + "_doc_" + std::to_string(j) + ".txt";
                    req.content = "Content from client " + std::to_string(i);

                    auto result = yams::cli::run_sync(localClient.streamingAddDocument(req), 5s);
                    if (result.has_value()) {
                        successfulOps++;
                    }
                }
            });
        }

        std::this_thread::sleep_for(200ms);

        auto shutdownResult = yams::cli::run_sync(client.shutdown(true), 5s);
        REQUIRE(shutdownResult.has_value());

        for (auto& t : clientThreads) {
            t.join();
        }

        REQUIRE(successfulOps > 0);
    }
}

TEST_CASE("Daemon graceful shutdown behavior", "[daemon][shutdown][graceful]") {
    SECTION("daemon stops after shutdown request") {
        DaemonHarness harness;
        REQUIRE(harness.start());

        auto client = createClient(harness.socketPath());
        REQUIRE(yams::cli::run_sync(client.connect(), 3s).has_value());

        auto shutdownResult = yams::cli::run_sync(client.shutdown(true), 5s);
        REQUIRE(shutdownResult.has_value());

        // Verify daemon actually stops
        auto* daemon = harness.daemon();
        REQUIRE(daemon != nullptr);

        // Wait for daemon to stop (max 5 seconds)
        auto deadline = std::chrono::steady_clock::now() + 5s;
        while (daemon->isRunning() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(100ms);
        }

        REQUIRE_FALSE(daemon->isRunning());
    }

    SECTION("daemon responds to shutdown then stops") {
        DaemonHarness harness;
        REQUIRE(harness.start());

        auto client = createClient(harness.socketPath());
        REQUIRE(yams::cli::run_sync(client.connect(), 3s).has_value());

        REQUIRE(harness.daemon()->isRunning());

        // Send shutdown
        auto shutdownResult = yams::cli::run_sync(client.shutdown(true), 5s);
        REQUIRE(shutdownResult.has_value());

        // Poll for daemon to stop
        bool stopped = false;
        for (int i = 0; i < 50; ++i) {
            if (!harness.daemon()->isRunning()) {
                stopped = true;
                break;
            }
            std::this_thread::sleep_for(100ms);
        }

        REQUIRE(stopped);
    }

    SECTION("socket becomes unavailable after shutdown") {
        DaemonHarness harness;
        REQUIRE(harness.start());

        auto client = createClient(harness.socketPath());
        REQUIRE(yams::cli::run_sync(client.connect(), 3s).has_value());

        // Shutdown
        auto shutdownResult = yams::cli::run_sync(client.shutdown(true), 5s);
        REQUIRE(shutdownResult.has_value());

        // Wait for daemon to stop
        std::this_thread::sleep_for(2s);

        // Verify daemon is not running
        REQUIRE_FALSE(DaemonClient::isDaemonRunning(harness.socketPath()));

        // New connections should fail
        auto newClient = createClient(harness.socketPath(), 1s);
        auto connectResult = yams::cli::run_sync(newClient.connect(), 2s);
        REQUIRE_FALSE(connectResult.has_value());
    }
}
