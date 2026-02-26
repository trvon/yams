/**
 * @file client_timeout_recovery_test.cpp
 * @brief Integration tests for daemon client timeout and stale connection recovery
 */

#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "test_async_helpers.h"
#include "test_daemon_harness.h"
#include "test_helpers_catch2.h"
#include <yams/cli/cli_sync.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/SocketServer.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using namespace yams::daemon;
using namespace yams::test;

namespace {
DaemonClient createClient(const fs::path& socketPath) {
    ClientConfig config;
    config.socketPath = socketPath;
    config.requestTimeout = 3s;
    config.headerTimeout = 5s;
    config.bodyTimeout = 10s;
    config.autoStart = false;
    return DaemonClient(config);
}

bool connectWithRetry(DaemonClient& client, int maxRetries = 3,
                      std::chrono::milliseconds retryDelay = 200ms) {
    for (int attempt = 0; attempt < maxRetries; ++attempt) {
        auto result = yams::cli::run_sync(client.connect(), 5s);
        if (result.has_value()) {
            return true;
        }
        std::this_thread::sleep_for(retryDelay);
    }
    return false;
}

bool listWithRetry(DaemonClient& client, int maxRetries = 6,
                   std::chrono::milliseconds retryDelay = 200ms) {
    ListRequest req;
    req.limit = 10;
    for (int attempt = 0; attempt < maxRetries; ++attempt) {
        auto result = yams::cli::run_sync(client.list(req), 5s);
        if (result.has_value()) {
            return true;
        }
        std::this_thread::sleep_for(retryDelay);
    }
    return false;
}

bool addDocumentWithRetry(DaemonClient& client, const AddDocumentRequest& request,
                          int maxRetries = 6, std::chrono::milliseconds retryDelay = 250ms,
                          std::string* lastError = nullptr) {
    std::string latestError;
    for (int attempt = 0; attempt < maxRetries; ++attempt) {
        auto result = yams::cli::run_sync(client.streamingAddDocument(request), 20s);
        if (result.has_value()) {
            if (lastError != nullptr) {
                *lastError = latestError;
            }
            return true;
        }

        latestError = result.error().message;
        const bool duplicateWrite = latestError.find("already exists") != std::string::npos ||
                                    latestError.find("duplicate") != std::string::npos ||
                                    latestError.find("UNIQUE constraint") != std::string::npos;
        if (duplicateWrite) {
            if (lastError != nullptr) {
                *lastError = latestError;
            }
            return true;
        }

        std::this_thread::sleep_for(retryDelay);
    }

    if (lastError != nullptr) {
        *lastError = latestError;
    }
    return false;
}

bool containsSubstring(const std::vector<std::string>& values, const std::string& needle) {
    return std::any_of(values.begin(), values.end(),
                       [&](const auto& value) { return value.find(needle) != std::string::npos; });
}

DaemonHarness::Options makeLeanHarnessOptions() {
    DaemonHarness::Options opts;
    opts.enableModelProvider = false;
    opts.useMockModelProvider = false;
    return opts;
}

bool startHarnessWithRetry(DaemonHarness& harness, std::chrono::milliseconds timeout,
                           int maxAttempts = 3) {
    for (int attempt = 0; attempt < maxAttempts; ++attempt) {
        if (harness.start(timeout)) {
            return true;
        }
        harness.stop();
        std::this_thread::sleep_for(300ms);
    }
    return false;
}
} // namespace

TEST_CASE("Client timeout recovery: Immediate EOF detection and retry",
          "[daemon][timeout][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    DaemonHarness harness(makeLeanHarnessOptions());
    REQUIRE(startHarnessWithRetry(harness, 10s));

    auto client = createClient(harness.socketPath());

    SECTION("First request succeeds, second request after idle period works") {
        ListRequest req1;
        req1.limit = 10;

        auto result1 = yams::cli::run_sync(client.list(req1), 2s);
        REQUIRE(result1.has_value());

        // Sleep for 2 seconds to allow connection to become idle
        std::this_thread::sleep_for(2s);

        ListRequest req2;
        req2.limit = 10;

        // Connection may be stale after idle period - allow time for reconnection
        // Retry logic handles stale connection scenarios where pool needs to reconnect
        // Note: Connection pool may need multiple attempts to detect stale connection
        // and establish a new one. Give it more time and retries.
        yams::Result<yams::daemon::ListResponse> result2;
        for (int attempt = 0; attempt < 5; ++attempt) {
            result2 = yams::cli::run_sync(client.list(req2), 5s);
            if (result2.has_value())
                break;
            // Longer delay between retries to allow pool to fully reset
            std::this_thread::sleep_for(500ms);
        }
        REQUIRE(result2.has_value());
    }
}

TEST_CASE("Client timeout recovery: Honors YAMS_IPC_TIMEOUT_MS for idle reap",
          "[daemon][timeout][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    ScopedEnvVar ipcTimeoutEnv{"YAMS_IPC_TIMEOUT_MS", std::string{"1000"}};
    ScopedEnvVar maxIdleEnv{"YAMS_MAX_IDLE_TIMEOUTS", std::string{"1"}};

    DaemonHarness harness(makeLeanHarnessOptions());
    REQUIRE(startHarnessWithRetry(harness, 10s));

    auto idleClient = createClient(harness.socketPath());
    REQUIRE(connectWithRetry(idleClient));

    auto warmIdle = yams::cli::run_sync(idleClient.status(), 2s);
    REQUIRE(warmIdle.has_value());

    auto* socketServer = harness.daemon()->getSocketServer();
    REQUIRE(socketServer != nullptr);

    std::this_thread::sleep_for(2500ms);

    bool observedIdleReap = false;
    auto deadline = std::chrono::steady_clock::now() + 4s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (socketServer->activeConnections() == 0) {
            observedIdleReap = true;
            break;
        }
        std::this_thread::sleep_for(200ms);
    }

    REQUIRE(observedIdleReap);

    yams::Result<yams::daemon::ListResponse> resultAfterIdle;
    ListRequest req;
    req.limit = 10;
    for (int attempt = 0; attempt < 5; ++attempt) {
        resultAfterIdle = yams::cli::run_sync(idleClient.list(req), 5s);
        if (resultAfterIdle.has_value()) {
            break;
        }
        std::this_thread::sleep_for(200ms);
    }

    REQUIRE(resultAfterIdle.has_value());
}

TEST_CASE("Client timeout recovery: Connection lifetime expiry remains crash-free",
          "[daemon][timeout][lifetime][integration][stress]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    ScopedEnvVar lifetimeEnv{"YAMS_CONNECTION_LIFETIME_S", std::string{"1"}};
    ScopedEnvVar ipcTimeoutEnv{"YAMS_IPC_TIMEOUT_MS", std::string{"5000"}};
    ScopedEnvVar maxIdleEnv{"YAMS_MAX_IDLE_TIMEOUTS", std::string{"10"}};

    DaemonHarness harness(makeLeanHarnessOptions());
    REQUIRE(startHarnessWithRetry(harness, 12s));

    auto client = createClient(harness.socketPath());
    REQUIRE(connectWithRetry(client));
    REQUIRE(yams::cli::run_sync(client.status(), 5s).has_value());

    auto* socketServer = harness.daemon()->getSocketServer();
    REQUIRE(socketServer != nullptr);
    const uint64_t baselineConnections = socketServer->totalConnections();

    constexpr int kLifetimeCycles = 8;
    for (int cycle = 0; cycle < kLifetimeCycles; ++cycle) {
        INFO("lifetime cycle " << cycle);

        auto warm = yams::cli::run_sync(client.status(), 5s);
        REQUIRE(warm.has_value());

        std::this_thread::sleep_for(1300ms);

        REQUIRE(harness.daemon() != nullptr);
        REQUIRE(harness.daemon()->isRunning());
        REQUIRE(listWithRetry(client));
        REQUIRE(harness.daemon()->isRunning());
    }

    REQUIRE(socketServer->totalConnections() > baselineConnections);
}

TEST_CASE("Client timeout recovery: Parallel clients survive lifetime expiry",
          "[daemon][timeout][lifetime][integration][stress][parallel]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    ScopedEnvVar lifetimeEnv{"YAMS_CONNECTION_LIFETIME_S", std::string{"1"}};
    ScopedEnvVar ipcTimeoutEnv{"YAMS_IPC_TIMEOUT_MS", std::string{"5000"}};
    ScopedEnvVar maxIdleEnv{"YAMS_MAX_IDLE_TIMEOUTS", std::string{"10"}};

    DaemonHarness harness(makeLeanHarnessOptions());
    REQUIRE(startHarnessWithRetry(harness, 12s));

    auto* socketServer = harness.daemon()->getSocketServer();
    REQUIRE(socketServer != nullptr);
    const uint64_t baselineConnections = socketServer->totalConnections();

    constexpr int kClients = 4;
    std::vector<std::unique_ptr<DaemonClient>> clients;
    clients.reserve(kClients);
    for (int i = 0; i < kClients; ++i) {
        clients.push_back(std::make_unique<DaemonClient>(createClient(harness.socketPath())));
        REQUIRE(connectWithRetry(*clients.back()));
    }

    constexpr int kLifetimeCycles = 5;
    for (int cycle = 0; cycle < kLifetimeCycles; ++cycle) {
        INFO("parallel lifetime cycle " << cycle);

        for (int i = 0; i < kClients; ++i) {
            auto warm = yams::cli::run_sync(clients[i]->status(), 5s);
            REQUIRE(warm.has_value());
            std::this_thread::sleep_for(120ms);
        }

        std::this_thread::sleep_for(1300ms);

        std::vector<uint8_t> recovered(kClients, 0);
        std::vector<std::thread> threads;
        threads.reserve(kClients);
        for (int i = 0; i < kClients; ++i) {
            threads.emplace_back(
                [&, i]() { recovered[i] = listWithRetry(*clients[i], 8, 150ms) ? 1 : 0; });
        }
        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(harness.daemon() != nullptr);
        REQUIRE(harness.daemon()->isRunning());
        for (int i = 0; i < kClients; ++i) {
            REQUIRE(recovered[i] == 1);
        }
    }

    REQUIRE(socketServer->totalConnections() > baselineConnections + kClients);
}

TEST_CASE("Client timeout recovery: Streaming request connection handling",
          "[daemon][timeout][streaming][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    DaemonHarness harness(makeLeanHarnessOptions());
    REQUIRE(startHarnessWithRetry(harness, 10s));

    auto client = createClient(harness.socketPath());

    SECTION("Streaming requests work after idle period") {
        GrepRequest req1;
        req1.pattern = "test";
        req1.pathsOnly = true;

        auto result1 = yams::cli::run_sync(client.grep(req1), 2s);
        REQUIRE(result1.has_value());

        // Sleep for 2 seconds to allow connection to become idle
        std::this_thread::sleep_for(2s);

        GrepRequest req2;
        req2.pattern = "another";
        req2.pathsOnly = false;

        // Streaming requests may need reconnection after idle period
        // Give more time and retries for connection pool to recover
        yams::Result<yams::daemon::GrepResponse> result2;
        for (int attempt = 0; attempt < 5; ++attempt) {
            result2 = yams::cli::run_sync(client.grep(req2), 5s);
            if (result2.has_value())
                break;
            // Longer delay between retries to allow pool to fully reset
            std::this_thread::sleep_for(500ms);
        }
        REQUIRE(result2.has_value());
    }
}

TEST_CASE("Client timeout recovery: Connection pool management",
          "[daemon][timeout][pool][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    DaemonHarness harness(makeLeanHarnessOptions());
    REQUIRE(startHarnessWithRetry(harness, 10s));

    auto client = createClient(harness.socketPath());

    SECTION("Pool handles multiple requests over time") {
        for (int i = 0; i < 3; ++i) {
            ListRequest req;
            req.limit = 5;
            auto result = yams::cli::run_sync(client.list(req), 2s);
            REQUIRE(result.has_value());
            std::this_thread::sleep_for(100ms);
        }

        std::this_thread::sleep_for(1s);

        ListRequest freshReq;
        freshReq.limit = 10;

        auto freshResult = yams::cli::run_sync(client.list(freshReq), 2s);
        REQUIRE(freshResult.has_value());
    }
}

TEST_CASE("Client timeout recovery: Rapid request cycle",
          "[daemon][timeout][stress][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    DaemonHarness harness(makeLeanHarnessOptions());
    REQUIRE(startHarnessWithRetry(harness, 10s));

    auto client = createClient(harness.socketPath());

    SECTION("Handles rapid cycles without issues") {
        constexpr int kCycles = 5;

        for (int cycle = 0; cycle < kCycles; ++cycle) {
            ListRequest req;
            req.limit = 5;
            auto result = yams::cli::run_sync(client.list(req), 2s);
            REQUIRE(result.has_value());
            std::this_thread::sleep_for(200ms);
        }
    }
}

TEST_CASE("Client timeout recovery: Daemon restart handling",
          "[daemon][timeout][reconnect][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    DaemonHarness harness(makeLeanHarnessOptions());
    REQUIRE(startHarnessWithRetry(harness, 12s));

    auto client = createClient(harness.socketPath());

    SECTION("Reconnects after daemon restart") {
        // Allow daemon to fully initialize before first request
        std::this_thread::sleep_for(500ms);

        ListRequest req1;
        req1.limit = 10;
        auto result1 = yams::cli::run_sync(client.list(req1), 5s);
        REQUIRE(result1.has_value());

        std::this_thread::sleep_for(500ms);

        harness.stop();
        std::this_thread::sleep_for(500ms);

        REQUIRE(startHarnessWithRetry(harness, 12s));

        // Allow restarted daemon to fully initialize
        std::this_thread::sleep_for(500ms);

        ListRequest req2;
        req2.limit = 10;
        auto result2 = yams::cli::run_sync(client.list(req2), 5s);
        REQUIRE(result2.has_value());
    }
}

TEST_CASE("Client timeout recovery: Error message quality",
          "[daemon][timeout][errors][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    DaemonHarness harness(makeLeanHarnessOptions());
    REQUIRE(startHarnessWithRetry(harness, 10s));

    auto client = createClient(harness.socketPath());

    SECTION("Provides descriptive errors when daemon unavailable") {
        ListRequest req1;
        req1.limit = 10;
        auto result1 = yams::cli::run_sync(client.list(req1), 2s);
        REQUIRE(result1.has_value());

        std::this_thread::sleep_for(500ms);

        harness.stop();

        ListRequest req2;
        req2.limit = 10;
        auto result2 = yams::cli::run_sync(client.list(req2), 2s);

        REQUIRE_FALSE(result2.has_value());

        const auto& msg = result2.error().message;

        // Error message should provide some context about what went wrong
        // Accept EOF-related messages as valid since they describe the connection state
        bool hasContext =
            msg.find("Connection") != std::string::npos || msg.find("stale") != std::string::npos ||
            msg.find("closed") != std::string::npos || msg.find("daemon") != std::string::npos ||
            msg.find("EOF") != std::string::npos || msg.find("End of file") != std::string::npos ||
            msg.find("socket") != std::string::npos || msg.find("connect") != std::string::npos;

        REQUIRE(hasContext);
    }
}

TEST_CASE("Client timeout recovery: Connection refused when daemon down",
          "[daemon][timeout][connection-refused][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    DaemonHarness harness(makeLeanHarnessOptions());
    REQUIRE(startHarnessWithRetry(harness, 12s));

    auto client = createClient(harness.socketPath());
    REQUIRE(connectWithRetry(client));

    // Allow daemon to fully initialize
    std::this_thread::sleep_for(500ms);

    ListRequest req1;
    req1.limit = 1;
    auto result1 = yams::cli::run_sync(client.list(req1), 5s);
    REQUIRE(result1.has_value());

    std::this_thread::sleep_for(200ms);
    harness.stop();

    ListRequest req2;
    req2.limit = 1;
    auto result2 = yams::cli::run_sync(client.list(req2), 2s);
    REQUIRE_FALSE(result2.has_value());

    const auto& msg = result2.error().message;
    bool hasContext =
        msg.find("Connection") != std::string::npos || msg.find("refused") != std::string::npos ||
        msg.find("daemon") != std::string::npos || msg.find("socket") != std::string::npos ||
        msg.find("EOF") != std::string::npos || msg.find("End of file") != std::string::npos ||
        msg.find("closed") != std::string::npos || msg.find("establish") != std::string::npos;
    REQUIRE(hasContext);
}

TEST_CASE("Client timeout recovery: Shutdown cancels in-flight request",
          "[daemon][timeout][shutdown][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    DaemonHarness harness(makeLeanHarnessOptions());
    REQUIRE(startHarnessWithRetry(harness, 10s));

    ClientConfig config;
    config.socketPath = harness.socketPath();
    config.requestTimeout = 10s;
    config.headerTimeout = 10s;
    config.bodyTimeout = 10s;
    config.autoStart = false;
    config.maxInflight = 1;
    DaemonClient client(config);
    REQUIRE(connectWithRetry(client));

    std::atomic<bool> done{false};
    std::optional<yams::Error> requestError;
    std::thread requestThread([&]() {
        AddDocumentRequest req;
        req.name = "shutdown_inflight.txt";
        req.content = std::string(2 * 1024 * 1024, 'x');
        auto result = yams::cli::run_sync(client.streamingAddDocument(req), 10s);
        if (!result.has_value()) {
            requestError = result.error();
        }
        done.store(true);
    });

    std::this_thread::sleep_for(50ms);
    harness.stop();

    auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!done.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(10ms);
    }
    requestThread.join();

    REQUIRE(done.load());
    if (requestError.has_value()) {
        const auto& msg = requestError->message;
        bool hasContext =
            msg.find("cancel") != std::string::npos ||
            msg.find("Connection") != std::string::npos ||
            msg.find("closed") != std::string::npos || msg.find("timeout") != std::string::npos ||
            msg.find("daemon") != std::string::npos || msg.find("pipe") != std::string::npos ||
            msg.find("shutdown") != std::string::npos;
        REQUIRE(hasContext);
    }
}

TEST_CASE("Client timeout recovery: Tag filtering matrix for list and search",
          "[daemon][timeout][tags][integration]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    DaemonHarness harness(makeLeanHarnessOptions());
    REQUIRE(startHarnessWithRetry(harness, 12s));

    auto client = createClient(harness.socketPath());
    REQUIRE(connectWithRetry(client));

    const std::string redName = "catch2_tag_filter_red.txt";
    const std::string blueName = "catch2_tag_filter_blue.txt";
    const std::string queryToken = "catch2-tag-filter-token";

    AddDocumentRequest addRed;
    addRed.name = redName;
    addRed.content = "red content " + queryToken;
    addRed.tags = {"team-red", "group-a"};
    std::string addRedError;
    const bool addRedOk = addDocumentWithRetry(client, addRed, 8, 300ms, &addRedError);
    INFO("addRed last error: " << addRedError);
    REQUIRE(addRedOk);

    AddDocumentRequest addBlue;
    addBlue.name = blueName;
    addBlue.content = "blue content " + queryToken;
    addBlue.tags = {"team-blue", "group-a"};
    std::string addBlueError;
    const bool addBlueOk = addDocumentWithRetry(client, addBlue, 8, 300ms, &addBlueError);
    INFO("addBlue last error: " << addBlueError);
    REQUIRE(addBlueOk);

    ListRequest listRedOnly;
    listRedOnly.limit = 50;
    listRedOnly.tags = {"team-red"};
    listRedOnly.matchAllTags = true;
    yams::Result<yams::daemon::ListResponse> listRedOnlyResult;
    std::vector<std::string> listedNames;
    bool foundRed = false;
    for (int attempt = 0; attempt < 20; ++attempt) {
        listRedOnlyResult = yams::cli::run_sync(client.list(listRedOnly), 10s);
        if (!listRedOnlyResult.has_value()) {
            std::this_thread::sleep_for(250ms);
            continue;
        }

        listedNames.clear();
        for (const auto& item : listRedOnlyResult.value().items) {
            listedNames.push_back(item.name);
            listedNames.push_back(item.fileName);
            listedNames.push_back(item.path);
        }

        if (containsSubstring(listedNames, redName)) {
            foundRed = true;
            break;
        }
        std::this_thread::sleep_for(250ms);
    }

    REQUIRE(listRedOnlyResult.has_value());
    REQUIRE(foundRed);
    REQUIRE(containsSubstring(listedNames, redName));
    REQUIRE_FALSE(containsSubstring(listedNames, blueName));

    ListRequest listImpossible;
    listImpossible.limit = 50;
    listImpossible.tags = {"team-red", "team-blue"};
    listImpossible.matchAllTags = true;
    auto listImpossibleResult = yams::cli::run_sync(client.list(listImpossible), 10s);
    REQUIRE(listImpossibleResult.has_value());
    REQUIRE(listImpossibleResult.value().items.empty());

    if (std::getenv("TSAN_OPTIONS") != nullptr) {
        SUCCEED("Skipping strict search tag assertions under TSAN (eventual indexing instability)");
        return;
    }

    SearchRequest searchRedOnly;
    searchRedOnly.query = queryToken;
    searchRedOnly.searchType = "keyword";
    searchRedOnly.pathsOnly = true;
    searchRedOnly.limit = 50;
    searchRedOnly.tags = {"team-red"};
    searchRedOnly.matchAllTags = true;

    yams::Result<SearchResponse> searchRedResult;
    std::vector<std::string> searchRedPaths;
    bool sawExpectedTagFilteredResult = false;
    for (int attempt = 0; attempt < 30; ++attempt) {
        searchRedResult = yams::cli::run_sync(client.search(searchRedOnly), 10s);
        if (!searchRedResult.has_value()) {
            std::this_thread::sleep_for(300ms);
            continue;
        }

        searchRedPaths.clear();
        for (const auto& item : searchRedResult.value().results) {
            searchRedPaths.push_back(item.path);
            auto it = item.metadata.find("path");
            if (it != item.metadata.end()) {
                searchRedPaths.push_back(it->second);
            }
        }

        const bool hasRed = containsSubstring(searchRedPaths, redName);
        const bool hasBlue = containsSubstring(searchRedPaths, blueName);
        if (hasRed && !hasBlue) {
            sawExpectedTagFilteredResult = true;
            break;
        }

        std::this_thread::sleep_for(300ms);
    }
    REQUIRE(searchRedResult.has_value());
    REQUIRE(sawExpectedTagFilteredResult);
    REQUIRE(containsSubstring(searchRedPaths, redName));
    REQUIRE_FALSE(containsSubstring(searchRedPaths, blueName));

    SearchRequest searchImpossible;
    searchImpossible.query = queryToken;
    searchImpossible.searchType = "keyword";
    searchImpossible.pathsOnly = true;
    searchImpossible.limit = 50;
    searchImpossible.tags = {"team-red", "team-blue"};
    searchImpossible.matchAllTags = true;

    auto searchImpossibleResult = yams::cli::run_sync(client.search(searchImpossible), 10s);
    REQUIRE(searchImpossibleResult.has_value());
    REQUIRE(searchImpossibleResult.value().results.empty());
}
