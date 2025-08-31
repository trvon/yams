#include <spdlog/spdlog.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <functional>
#include <thread>
#include <vector>
#ifndef _WIN32
#include <unistd.h>
#endif

#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/mcp/mcp_server.h>

using namespace std::chrono_literals;

namespace yams::daemon::test {

class PooledRequestIT : public ::testing::Test {
protected:
    void SetUp() override {
        // Try to ensure the daemon is running before tests execute.
        // Only start if not already running to avoid conflicting daemons.
        if (!DaemonClient::isDaemonRunning()) {
            ClientConfig cfg{};
            // Use a dedicated, writable test data directory to avoid readiness issues
            // and interference with user/system state.
            namespace fs = std::filesystem;
            fs::path dataRoot;
            try {
                auto tmp = fs::temp_directory_path();
                dataRoot = tmp / (std::string("yams-pooled-it-") + std::to_string(::getpid()));
                (void)fs::create_directories(dataRoot);
            } catch (...) {
                // Best-effort fallback to build directory if tmp is not available
                dataRoot = fs::path("build") / "yams-debug" / "testdata" /
                           (std::string("yams-pooled-it-") + std::to_string(::getpid()));
                (void)fs::create_directories(dataRoot);
            }
            cfg.dataDir = dataRoot;

            // Bump timeouts globally for this test run to avoid premature read timeouts
            DaemonClient::setTimeoutEnvVars(std::chrono::milliseconds{120000},  // header 120s
                                            std::chrono::milliseconds{300000}); // body 5m

            auto started = DaemonClient::startDaemon(cfg);
            if (!started) {
                spdlog::warn("PooledRequestIT: could not auto-start daemon: {}",
                             started.error().message);
            }
        }

        // Wait for daemon readiness to avoid connection races.
        ASSERT_TRUE(waitForDaemonReady(45s)) << "Daemon failed to become ready in time";
    }

    // Poll until the daemon responds to a status or ping call, up to timeout.
    bool waitForDaemonReady(std::chrono::milliseconds timeout) {
        const auto start = std::chrono::steady_clock::now();
        const auto deadline = start + timeout;
        std::chrono::milliseconds sleep{100};
        while (std::chrono::steady_clock::now() < deadline) {
            if (!DaemonClient::isDaemonRunning()) {
                std::this_thread::sleep_for(sleep);
                sleep =
                    std::min<std::chrono::milliseconds>(sleep * 2, std::chrono::milliseconds{500});
                continue;
            }
            DaemonClient client{};
            // Prefer status for service readiness
            if (auto st = client.status(); st) {
                const auto& s = st.value();
                auto has = [&](const char* key) -> bool {
                    auto it = s.readinessStates.find(key);
                    return it != s.readinessStates.end() && it->second;
                };
                const bool coreReady = has("ipc_server") && has("content_store") && has("database");
                const bool queryReady = has("metadata_repo") || has("search_engine");
                if (coreReady && queryReady) {
                    return true;
                }
            } else {
                // Fallback to ping as a readiness probe when status isn't available yet
                if (auto pg = client.ping(); pg) {
                    return true;
                }
            }
            std::this_thread::sleep_for(sleep);
            sleep = std::min<std::chrono::milliseconds>(sleep * 2, std::chrono::milliseconds{500});
        }
        return false;
    }

    void TearDown() {
        // Ensure daemon is stopped between tests to avoid cross-test interference
        yams::daemon::DaemonClient c{};
        (void)c.shutdown(true);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
};

TEST_F(PooledRequestIT, Search_Succeeds_With_Render_Or_Fallback) {
    // Small pool to exercise gating
    yams::cli::DaemonClientPool::Config pool_cfg;
    pool_cfg.max_clients = 2;
    // Increase timeouts to accommodate heavy startup/streaming under load
    pool_cfg.client_config.headerTimeout = std::chrono::milliseconds{90000};  // 90s
    pool_cfg.client_config.bodyTimeout = std::chrono::milliseconds{300000};   // 5m inactivity
    pool_cfg.client_config.requestTimeout = std::chrono::milliseconds{60000}; // 60s send window

    yams::cli::PooledRequestManager<SearchRequest, SearchResponse> mgr(pool_cfg);

    SearchRequest req{};
    req.query = "test";
    req.limit = 5;

    bool rendered = false;
    bool fellback = false;

    auto render = [&](const SearchResponse& resp) -> yams::Result<void> {
        rendered = true;
        spdlog::debug("Search response: results={}, total={}", resp.results.size(),
                      resp.totalCount);
        return yams::Result<void>();
    };

    auto fallback = [&]() -> yams::Result<void> {
        fellback = true;
        spdlog::warn("Search fallback executed: daemon unavailable");
        return yams::Result<void>();
    };

    auto r = mgr.execute(req, fallback, render);
    EXPECT_TRUE(r) << "execute() should return ok";
    EXPECT_TRUE(rendered) << "Expected daemon render path; fallback indicates a failure";
    EXPECT_FALSE(fellback) << "Fallback should not be used when daemon is ready";
}

TEST_F(PooledRequestIT, Grep_Succeeds_With_Render_Or_Fallback) {
    yams::cli::DaemonClientPool::Config pool_cfg;
    pool_cfg.max_clients = 2;
    pool_cfg.client_config.headerTimeout = std::chrono::milliseconds{120000}; // 120s
    pool_cfg.client_config.bodyTimeout = std::chrono::milliseconds{300000};   // 5m inactivity
    pool_cfg.client_config.requestTimeout = std::chrono::milliseconds{60000}; // 60s send window

    yams::cli::PooledRequestManager<GrepRequest, GrepResponse> mgr(pool_cfg);

    GrepRequest req{};
    req.pattern = "YAMS";
    // Robust: exercise recursive streaming across source tree without artificial caps
    req.paths = {"include", "src"};
    req.caseInsensitive = true; // Non-strict search
    req.recursive = true;       // Prefer streaming path
    req.maxMatches = 0;         // No per-file cap

    bool rendered = false;
    bool fellback = false;

    auto render = [&](const GrepResponse& resp) -> yams::Result<void> {
        rendered = true;
        spdlog::debug("Grep response: filesSearched={}, totalMatches={}", resp.filesSearched,
                      resp.totalMatches);
        return yams::Result<void>();
    };

    auto fallback = [&]() -> yams::Result<void> {
        fellback = true;
        spdlog::warn("Grep fallback executed: daemon unavailable");
        return yams::Result<void>();
    };

    auto r = mgr.execute(req, fallback, render);
    EXPECT_TRUE(r);
    EXPECT_TRUE(rendered) << "Expected daemon render path; fallback indicates a failure";
    EXPECT_FALSE(fellback) << "Fallback should not be used when daemon is ready";
}

TEST_F(PooledRequestIT, Pool_Unavailable_Still_Proceeds_With_DaemonClient) {
    // Ensure pool has capacity; we require daemon path (no fallback)
    yams::cli::DaemonClientPool::Config pool_cfg;
    pool_cfg.max_clients = 1;
    pool_cfg.acquire_timeout = std::chrono::milliseconds{500};
    pool_cfg.client_config.headerTimeout = std::chrono::milliseconds{90000};  // 90s
    pool_cfg.client_config.bodyTimeout = std::chrono::milliseconds{240000};   // 4m inactivity
    pool_cfg.client_config.requestTimeout = std::chrono::milliseconds{60000}; // 60s send window

    yams::cli::PooledRequestManager<SearchRequest, SearchResponse> mgr(pool_cfg);

    SearchRequest req{};
    req.query = "test";
    req.limit = 1;

    bool rendered = false;
    bool fellback = false;

    auto render = [&](const SearchResponse&) -> yams::Result<void> {
        rendered = true;
        return yams::Result<void>();
    };

    auto fallback = [&]() -> yams::Result<void> {
        fellback = true;
        return yams::Result<void>();
    };

    auto r = mgr.execute(req, fallback, render);
    EXPECT_TRUE(r);
    EXPECT_TRUE(rendered) << "Expected daemon render path";
    EXPECT_FALSE(fellback) << "Fallback should not occur when daemon is ready";
}

TEST_F(PooledRequestIT, Concurrent_Execute_Basic) {
    // Modest gating to allow some contention
    yams::cli::DaemonClientPool::Config pool_cfg;
    pool_cfg.max_clients = 2;
    pool_cfg.client_config.headerTimeout = std::chrono::milliseconds{60000};  // 60s
    pool_cfg.client_config.bodyTimeout = std::chrono::milliseconds{180000};   // 3m inactivity
    pool_cfg.client_config.requestTimeout = std::chrono::milliseconds{45000}; // 45s send window

    yams::cli::PooledRequestManager<SearchRequest, SearchResponse> mgr(pool_cfg);

    std::atomic<int> renders{0};
    std::atomic<int> fallbacks{0};

    constexpr int kThreads = 4;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&]() {
            SearchRequest req{};
            req.query = "concurrent";
            req.limit = 3;

            auto render = [&](const SearchResponse&) -> yams::Result<void> {
                renders.fetch_add(1);
                return yams::Result<void>();
            };

            auto fallback = [&]() -> yams::Result<void> {
                fallbacks.fetch_add(1);
                return yams::Result<void>();
            };

            auto r = mgr.execute(req, fallback, render);
            ASSERT_TRUE(r);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(renders.load(), kThreads) << "All requests should render via daemon";
    EXPECT_EQ(fallbacks.load(), 0) << "No fallbacks expected when daemon is ready";
}

// --- MCP pooled request coverage ---
// Exercise MCPServer's pooled request path for search via its testing interface.
// This asserts that either we get a successful render from the daemon or a
// well-formed fallback error when the daemon is unavailable.
class FakeTransport : public yams::mcp::ITransport {
public:
    void send(const nlohmann::json& /*message*/) override {}
    yams::mcp::MessageResult receive() override {
        return yams::Error{yams::ErrorCode::InvalidArgument,
                           "FakeTransport has no incoming messages"};
    }
    bool isConnected() const override { return true; }
    void close() override {}
    yams::mcp::TransportState getState() const override {
        return yams::mcp::TransportState::Connected;
    }
};

TEST_F(PooledRequestIT, MCP_Pooled_Search_Execute_Render_Or_Fallback) {
    // Construct MCP server with a fake transport; don't start the server loop.
    std::atomic<bool> shutdown{false};
    auto transport = std::make_unique<FakeTransport>();
    yams::mcp::MCPServer server(std::move(transport), &shutdown);

    // Build a minimal MCP search request
    yams::mcp::MCPSearchRequest req;
    req.query = "pooled";
    req.limit = 3;

    // Call the testing handler which uses PooledRequestManager under the hood
    auto result = server.testHandleSearchDocuments(req);
    ASSERT_TRUE(result) << "MCP pooled search should succeed against running daemon";
    const auto& resp = result.value();
    EXPECT_GE(resp.total, 0u);
}

} // namespace yams::daemon::test