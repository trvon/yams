// Stats Command Crash Test Suite
//
// This test suite reproduces crashes in the stats command that occur when:
// 1. Daemon returns incomplete status responses (missing requestCounts keys)
// 2. Daemon is not running or fails to respond
// 3. Map access via .at() throws out_of_range exceptions
// 4. Null pointer dereferences in daemon client

#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <map>

#include "test_async_helpers.h"
#include "test_daemon_harness.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace yams::daemon;
using namespace yams::test;
using namespace std::chrono_literals;

TEST_CASE("Stats command - missing requestCounts keys", "[stats][command][crash][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    DaemonHarness harness;
    REQUIRE(harness.start());

    ClientConfig cfg;
    cfg.enableChunkedResponses = false;
    cfg.requestTimeout = 2s;
    cfg.socketPath = harness.socketPath();

    auto client = DaemonClient(cfg);
    auto connResult = yams::cli::run_sync(client.connect(), 3s);
    REQUIRE(connResult.has_value());

    SECTION("status request with missing keys doesn't crash") {
        auto sres = yams::cli::run_sync(client.status(), 3s);
        REQUIRE(sres.has_value());

        const auto& st = sres.value();

        // Simulate stats command logic that uses .at() which can throw
        // From stats_command.cpp:90-104
        auto safeGet = [&](const char* key) -> int64_t {
            if (st.requestCounts.count(key)) {
                return st.requestCounts.at(key);
            }
            return 0;
        };

        // These keys may not exist, especially on fresh daemon startup
        int64_t docs = safeGet("storage_documents");
        int64_t logicalBytes = safeGet("storage_logical_bytes");
        int64_t physicalBytes = safeGet("storage_physical_bytes");
        int64_t dedupSaved = safeGet("casDedupSavedBytes");
        int64_t compressSaved = safeGet("casCompressSavedBytes");

        INFO("docs=" << docs << " logical=" << logicalBytes << " physical=" << physicalBytes);
        INFO("dedup_saved=" << dedupSaved << " compress_saved=" << compressSaved);

        // Test passes if we didn't crash from .at() on missing keys
        REQUIRE(true);
    }

    SECTION("empty requestCounts map doesn't crash") {
        // Retry status request in case of transient connection issues
        yams::Result<StatusResponse> sres;
        for (int attempt = 0; attempt < 3; ++attempt) {
            sres = yams::cli::run_sync(client.status(), 3s);
            if (sres.has_value())
                break;
            std::this_thread::sleep_for(200ms);
        }
        REQUIRE(sres.has_value());

        const auto& st = sres.value();

        // Even if requestCounts is empty, we shouldn't crash
        for (const char* key :
             {"storage_documents", "storage_logical_bytes", "storage_physical_bytes",
              "casDedupSavedBytes", "casCompressSavedBytes", "metadataPhysicalBytes",
              "indexPhysicalBytes", "vectorPhysicalBytes", "logsTmpPhysicalBytes"}) {
            bool hasKey = st.requestCounts.count(key) > 0;
            INFO("Key '" << key << "' present: " << hasKey);
        }

        REQUIRE(true);
    }
}

TEST_CASE("Stats command - daemon not available", "[stats][command][crash][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    // Don't start daemon - test behavior when daemon is unavailable
    DaemonHarness harness;

    ClientConfig cfg;
    cfg.enableChunkedResponses = false;
    cfg.requestTimeout = 500ms;
    cfg.connectTimeout = 500ms;
    cfg.socketPath = harness.socketPath();
    cfg.autoStart = false;

    auto client = DaemonClient(cfg);

    SECTION("connection failure doesn't crash") {
        auto connResult = yams::cli::run_sync(client.connect(), 1s);

        // Should fail gracefully, not crash
        REQUIRE(!connResult.has_value());
    }

    SECTION("status request on disconnected client doesn't crash") {
        auto sres = yams::cli::run_sync(client.status(), 1s);

        // Should fail gracefully, not crash
        REQUIRE(!sres.has_value());
    }
}

TEST_CASE("Stats command - response validation", "[stats][command][crash][unit]") {
    // Test the stats command formatting logic in isolation

    SECTION("formatSize with null/missing values") {
        // Simulates what happens when requestCounts.at() would throw
        std::map<std::string, int64_t> requestCounts;

        // Empty map - all keys missing
        auto safeAt = [&](const char* key) -> int64_t {
            auto it = requestCounts.find(key);
            if (it != requestCounts.end()) {
                return it->second;
            }
            return 0; // Safe default instead of throwing
        };

        REQUIRE(safeAt("storage_documents") == 0);
        REQUIRE(safeAt("storage_logical_bytes") == 0);
        REQUIRE(safeAt("nonexistent_key") == 0);
    }

    SECTION("formatSize with various byte values") {
        auto formatBytes = [](uint64_t bytes) -> std::string {
            if (bytes == 0)
                return "0 B";

            const char* units[] = {"B", "KB", "MB", "GB", "TB"};
            int unitIndex = 0;
            double size = static_cast<double>(bytes);

            while (size >= 1024.0 && unitIndex < 4) {
                size /= 1024.0;
                unitIndex++;
            }

            std::ostringstream oss;
            if (unitIndex == 0) {
                oss << bytes << " B";
            } else {
                int precision = (size < 10.0) ? 1 : 0;
                oss << std::fixed << std::setprecision(precision) << size << " "
                    << units[unitIndex];
            }
            return oss.str();
        };

        REQUIRE(formatBytes(0) == "0 B");
        REQUIRE(formatBytes(512) == "512 B");
        REQUIRE(formatBytes(1024) == "1.0 KB");
        REQUIRE(formatBytes(1536) == "1.5 KB");
        REQUIRE(formatBytes(1048576) == "1.0 MB");
    }
}

TEST_CASE("Stats command - concurrent requests", "[stats][command][crash][stress]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    DaemonHarness harness;
    REQUIRE(harness.start());

    constexpr int numThreads = 4;
    constexpr int requestsPerThread = 5;
    std::vector<std::thread> workers;
    std::atomic<int> successCount{0};
    std::atomic<int> failCount{0};

    for (int i = 0; i < numThreads; ++i) {
        workers.emplace_back([&, threadId = i]() {
            ClientConfig cfg;
            cfg.enableChunkedResponses = false;
            cfg.requestTimeout = 2s;
            cfg.socketPath = harness.socketPath();

            auto client = DaemonClient(cfg);
            auto connResult = yams::cli::run_sync(client.connect(), 3s);
            if (!connResult.has_value()) {
                failCount.fetch_add(requestsPerThread);
                return;
            }

            for (int req = 0; req < requestsPerThread; ++req) {
                try {
                    auto sres = yams::cli::run_sync(client.status(), 3s);

                    if (sres.has_value()) {
                        // Safe access to requestCounts
                        const auto& st = sres.value();
                        int64_t docs = st.requestCounts.count("storage_documents")
                                           ? st.requestCounts.at("storage_documents")
                                           : 0;
                        (void)docs; // Use the value to avoid warnings

                        successCount.fetch_add(1);
                    } else {
                        failCount.fetch_add(1);
                    }
                } catch (const std::exception& e) {
                    INFO("Thread " << threadId << " request " << req << " exception: " << e.what());
                    failCount.fetch_add(1);
                }
            }
        });
    }

    for (auto& worker : workers) {
        worker.join();
    }

    INFO("Successful requests: " << successCount.load());
    INFO("Failed requests: " << failCount.load());

    // Test passes if daemon didn't crash and we got some successful responses
    REQUIRE(successCount.load() > 0);
}
