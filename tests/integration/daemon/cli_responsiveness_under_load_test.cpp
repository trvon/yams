// Integration test verifying CLI commands (status, ping) remain responsive
// even when the daemon is under heavy ingestion load.
// This tests the fix that routes these requests to a dedicated CLI executor pool.

#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "test_async_helpers.h"
#include "test_daemon_harness.h"
#include <yams/daemon/client/daemon_client.h>

using namespace yams::daemon;
using namespace yams::test;
using namespace std::chrono_literals;

namespace {

// Detect if running under ThreadSanitizer to adjust timing expectations
#if defined(__SANITIZE_THREAD__)
constexpr bool kThreadSanitizerEnabled = true;
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
constexpr bool kThreadSanitizerEnabled = true;
#else
constexpr bool kThreadSanitizerEnabled = false;
#endif
#else
constexpr bool kThreadSanitizerEnabled = false;
#endif

// Timeout multiplier for sanitizer builds (TSAN significantly slows down execution)
constexpr int kSanitizerTimeoutMultiplier = kThreadSanitizerEnabled ? 3 : 1;

DaemonClient createClient(const std::filesystem::path& socketPath) {
    ClientConfig config;
    config.socketPath = socketPath;
    config.connectTimeout = 5s;
    config.requestTimeout = 10s;
    config.autoStart = false;
    return DaemonClient(config);
}

// Helper to connect with retry logic
bool connectWithRetry(DaemonClient& client, int maxRetries = 3) {
    std::this_thread::sleep_for(200ms);
    for (int attempt = 0; attempt < maxRetries; ++attempt) {
        auto result = yams::cli::run_sync(client.connect(), 5s);
        if (result.has_value())
            return true;
        std::this_thread::sleep_for(200ms);
    }
    return false;
}

void startHarnessWithRetry(DaemonHarness& harness, int maxRetries = 3,
                           std::chrono::milliseconds retryDelay = 250ms) {
    for (int attempt = 0; attempt < maxRetries; ++attempt) {
        if (harness.start(30s)) {
            return;
        }
        harness.stop();
        std::this_thread::sleep_for(retryDelay);
    }

    SKIP("Skipping CLI responsiveness section due to daemon startup instability");
}

std::string generateLargeContent(size_t sizeBytes) {
    std::string content;
    content.reserve(sizeBytes);
    for (size_t i = 0; i < sizeBytes; ++i) {
        content.push_back(static_cast<char>('a' + (i % 26)));
    }
    return content;
}
} // namespace

TEST_CASE("CLI commands remain responsive under heavy ingestion load",
          "[daemon][cli][responsiveness][load]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    DaemonHarness harness;
    startHarnessWithRetry(harness);

    auto client = createClient(harness.socketPath());
    REQUIRE(connectWithRetry(client));

    // Verify baseline - status should work
    {
        auto statusResult = yams::cli::run_sync(client.status(), 2s);
        REQUIRE(statusResult.has_value());
        INFO("Baseline status check passed");
    }

    SECTION("status responds during concurrent document ingestion") {
        constexpr int numIngestThreads = 4;
        constexpr int docsPerThread = 10;
        std::atomic<bool> stopIngesting{false};
        std::atomic<int> totalIngested{0};
        std::atomic<int> statusSuccesses{0};
        std::atomic<int> statusAttempts{0};

        // Start threads that continuously ingest documents
        std::vector<std::thread> ingestThreads;
        for (int i = 0; i < numIngestThreads; ++i) {
            ingestThreads.emplace_back([&, threadId = i]() {
                auto threadClient = createClient(harness.socketPath());
                if (!connectWithRetry(threadClient))
                    return;

                for (int doc = 0; doc < docsPerThread && !stopIngesting.load(); ++doc) {
                    AddDocumentRequest req;
                    req.name = "load_test_thread" + std::to_string(threadId) + "_doc" +
                               std::to_string(doc) + ".txt";
                    // Use moderately sized content to create actual load
                    req.content = generateLargeContent(10 * 1024); // 10KB per doc

                    auto result = yams::cli::run_sync(threadClient.streamingAddDocument(req), 30s);
                    if (result.has_value()) {
                        totalIngested.fetch_add(1);
                    }
                }
            });
        }

        // While ingestion is happening, repeatedly check status
        // Status should respond within reasonable time even under load
        auto deadline = std::chrono::steady_clock::now() + 10s;
        while (std::chrono::steady_clock::now() < deadline &&
               totalIngested.load() < numIngestThreads * docsPerThread) {
            statusAttempts.fetch_add(1);

            auto start = std::chrono::steady_clock::now();
            auto statusResult = yams::cli::run_sync(client.status(), 3s);
            auto elapsed = std::chrono::steady_clock::now() - start;

            if (statusResult.has_value()) {
                statusSuccesses.fetch_add(1);
                INFO("Status responded in "
                     << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
                     << "ms");
            } else {
                WARN("Status failed: " << statusResult.error().message);
            }

            // Don't spam too fast
            std::this_thread::sleep_for(200ms);
        }

        stopIngesting.store(true);
        for (auto& t : ingestThreads) {
            t.join();
        }

        INFO("Total documents ingested: " << totalIngested.load());
        INFO("Status attempts: " << statusAttempts.load());
        INFO("Status successes: " << statusSuccesses.load());

        // Status should succeed most of the time (allow some failures due to timing)
        float successRate = static_cast<float>(statusSuccesses.load()) / statusAttempts.load();
        REQUIRE(successRate >= 0.8f); // At least 80% success rate
    }

    SECTION("ping responds during heavy ingestion") {
        constexpr int numIngestThreads = 4;
        constexpr int docsPerThread = 5;
        std::atomic<bool> stopIngesting{false};
        std::atomic<int> totalIngested{0};
        std::atomic<int> pingSuccesses{0};
        std::atomic<int> pingAttempts{0};

        // Start ingestion threads
        std::vector<std::thread> ingestThreads;
        for (int i = 0; i < numIngestThreads; ++i) {
            ingestThreads.emplace_back([&, threadId = i]() {
                auto threadClient = createClient(harness.socketPath());
                if (!connectWithRetry(threadClient))
                    return;

                for (int doc = 0; doc < docsPerThread && !stopIngesting.load(); ++doc) {
                    AddDocumentRequest req;
                    req.name = "ping_test_thread" + std::to_string(threadId) + "_doc" +
                               std::to_string(doc) + ".txt";
                    req.content = generateLargeContent(20 * 1024); // 20KB per doc

                    auto result = yams::cli::run_sync(threadClient.streamingAddDocument(req), 30s);
                    if (result.has_value()) {
                        totalIngested.fetch_add(1);
                    }
                }
            });
        }

        // While ingestion is happening, repeatedly ping
        auto deadline = std::chrono::steady_clock::now() + 10s;
        while (std::chrono::steady_clock::now() < deadline &&
               totalIngested.load() < numIngestThreads * docsPerThread) {
            pingAttempts.fetch_add(1);

            auto start = std::chrono::steady_clock::now();
            auto pingResult = yams::cli::run_sync(client.ping(), 2s);
            auto elapsed = std::chrono::steady_clock::now() - start;

            if (pingResult.has_value()) {
                pingSuccesses.fetch_add(1);
                // Ping should be fast even under load (with sanitizer-adjusted timeout)
                REQUIRE(elapsed < std::chrono::seconds(1 * kSanitizerTimeoutMultiplier));
            } else {
                WARN("Ping failed: " << pingResult.error().message);
            }

            std::this_thread::sleep_for(100ms);
        }

        stopIngesting.store(true);
        for (auto& t : ingestThreads) {
            t.join();
        }

        INFO("Total documents ingested: " << totalIngested.load());
        INFO("Ping attempts: " << pingAttempts.load());
        INFO("Ping successes: " << pingSuccesses.load());

        // Ping should have very high success rate
        float successRate = static_cast<float>(pingSuccesses.load()) / pingAttempts.load();
        REQUIRE(successRate >= 0.9f); // At least 90% success rate for ping
    }

    SECTION("shutdown request honored under load") {
        constexpr int numIngestThreads = 2;
        std::atomic<bool> stopIngesting{false};
        std::atomic<int> totalIngested{0};

        // Start ingestion threads
        std::vector<std::thread> ingestThreads;
        for (int i = 0; i < numIngestThreads; ++i) {
            ingestThreads.emplace_back([&, threadId = i]() {
                auto threadClient = createClient(harness.socketPath());
                if (!connectWithRetry(threadClient))
                    return;

                for (int doc = 0; doc < 100 && !stopIngesting.load(); ++doc) {
                    AddDocumentRequest req;
                    req.name = "shutdown_test_thread" + std::to_string(threadId) + "_doc" +
                               std::to_string(doc) + ".txt";
                    req.content = generateLargeContent(5 * 1024);

                    auto result = yams::cli::run_sync(threadClient.streamingAddDocument(req), 30s);
                    if (result.has_value()) {
                        totalIngested.fetch_add(1);
                    }
                }
            });
        }

        // Give ingestion a head start
        std::this_thread::sleep_for(500ms);

        // Now send shutdown while ingestion is happening
        auto start = std::chrono::steady_clock::now();
        auto shutdownResult = yams::cli::run_sync(client.shutdown(true), 5s);
        auto elapsed = std::chrono::steady_clock::now() - start;

        INFO("Shutdown responded in "
             << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() << "ms");

        // Shutdown should be acknowledged quickly (within 3 seconds)
        REQUIRE(elapsed < 3s);

        stopIngesting.store(true);
        for (auto& t : ingestThreads) {
            t.join();
        }

        // Daemon should stop after shutdown
        auto* daemon = harness.daemon();
        auto deadline = std::chrono::steady_clock::now() + 10s;
        while (daemon && daemon->isRunning() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(100ms);
        }

        if (daemon) {
            REQUIRE_FALSE(daemon->isRunning());
        }
    }
}
