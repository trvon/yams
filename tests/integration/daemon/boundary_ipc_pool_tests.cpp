// Boundary micro-tests for IPC under connection pooling
// - client ↔ pool: acquisition, status
// - pool ↔ daemon: connection reuse, streaming header-first, persistence

#include <spdlog/spdlog.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <thread>
#ifndef _WIN32
#include <unistd.h>
#endif

#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/daemon_client.h>

namespace yams {
namespace daemon {
namespace test {

class BoundaryIpcPoolIT : public ::testing::Test {
protected:
    void SetUp() override {
        // Ensure daemon is running and ready. Start it if needed.
        if (!DaemonClient::isDaemonRunning()) {
            ClientConfig cfg{};
            // Use a temp data dir when starting to avoid interference
            namespace fs = std::filesystem;
            fs::path dataRoot;
            try {
                auto tmp = fs::temp_directory_path();
                dataRoot = tmp / (std::string("yams-boundary-it-") + std::to_string(::getpid()));
                (void)fs::create_directories(dataRoot);
            } catch (...) {
                dataRoot = fs::path("build") / "yams-debug" / "testdata" /
                           (std::string("yams-boundary-it-") + std::to_string(::getpid()));
                (void)fs::create_directories(dataRoot);
            }
            cfg.dataDir = dataRoot;
            (void)DaemonClient::startDaemon(cfg);
        }

        ASSERT_TRUE(waitForDaemonReady(std::chrono::milliseconds(20000)))
            << "Daemon not ready for boundary tests";
    }

    static bool waitForDaemonReady(std::chrono::milliseconds timeout) {
        const auto start = std::chrono::steady_clock::now();
        const auto deadline = start + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (!DaemonClient::isDaemonRunning()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            DaemonClient client{};
            auto st = client.status();
            if (st) {
                const auto& s = st.value();
                auto has = [&](const char* key) -> bool {
                    auto it = s.readinessStates.find(key);
                    return it != s.readinessStates.end() && it->second;
                };
                const bool coreReady = has("ipc_server") && has("content_store") && has("database");
                const bool queryReady = has("metadata_repo") || has("search_engine");
                if (coreReady && queryReady)
                    return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }

    void TearDown() {
        // Ensure any running daemon is shut down to avoid cross-test interference
        yams::daemon::DaemonClient c{};
        (void)c.shutdown(true);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
};

// client ↔ pool: acquire and call status via leased client
TEST_F(BoundaryIpcPoolIT, ClientPool_AcquireAndStatus) {
    yams::cli::DaemonClientPool::Config pool_cfg;
    pool_cfg.max_clients = 1;
    pool_cfg.client_config.headerTimeout = std::chrono::milliseconds(60000);
    pool_cfg.client_config.bodyTimeout = std::chrono::milliseconds(180000);
    pool_cfg.client_config.requestTimeout = std::chrono::milliseconds(15000);
    yams::cli::DaemonClientPool pool(pool_cfg);

    auto lease_res = pool.acquire();
    ASSERT_TRUE(lease_res) << lease_res.error().message;
    auto lease = std::move(lease_res).value();
    ASSERT_TRUE(lease.valid());

    auto st = lease->status();
    ASSERT_TRUE(st) << st.error().message;
    EXPECT_FALSE(st.value().overallStatus.empty());
}

// pool ↔ daemon: reuse one leased connection for two streaming calls back-to-back
TEST_F(BoundaryIpcPoolIT, PoolDaemon_ReuseConnection_StreamingSearchThenList) {
    yams::cli::DaemonClientPool::Config pool_cfg;
    pool_cfg.max_clients = 1;
    pool_cfg.client_config.singleUseConnections = false; // keep socket open across calls
    pool_cfg.client_config.enableChunkedResponses = true;
    pool_cfg.client_config.headerTimeout = std::chrono::milliseconds(90000);
    pool_cfg.client_config.bodyTimeout = std::chrono::milliseconds(300000);
    pool_cfg.client_config.requestTimeout = std::chrono::milliseconds(30000);
    yams::cli::DaemonClientPool pool(pool_cfg);

    auto lease_res = pool.acquire();
    ASSERT_TRUE(lease_res) << lease_res.error().message;
    auto lease = std::move(lease_res).value();
    ASSERT_TRUE(lease.valid());

    // Streaming search
    SearchRequest sreq{};
    sreq.query = "boundary";
    sreq.limit = 3;
    auto sres = lease->streamingSearch(sreq);
    ASSERT_TRUE(sres) << sres.error().message;

    // Streaming list (immediately after, same lease/socket)
    ListRequest lreq{};
    lreq.limit = 10;
    auto lres = lease->streamingList(lreq);
    ASSERT_TRUE(lres) << lres.error().message;
}

// daemon ↔ pool: after a streaming call, ensure the same leased client can still perform ping
TEST_F(BoundaryIpcPoolIT, DaemonPool_PersistAfterStream_Ping) {
    yams::cli::DaemonClientPool::Config pool_cfg;
    pool_cfg.max_clients = 1;
    pool_cfg.client_config.singleUseConnections = false;
    pool_cfg.client_config.enableChunkedResponses = true;
    pool_cfg.client_config.headerTimeout = std::chrono::milliseconds(60000);
    pool_cfg.client_config.bodyTimeout = std::chrono::milliseconds(180000);
    pool_cfg.client_config.requestTimeout = std::chrono::milliseconds(20000);
    yams::cli::DaemonClientPool pool(pool_cfg);

    auto lease_res = pool.acquire();
    ASSERT_TRUE(lease_res) << lease_res.error().message;
    auto lease = std::move(lease_res).value();
    ASSERT_TRUE(lease.valid());

    // Do a small streaming grep to exercise chunk flow
    GrepRequest greq{};
    greq.pattern = "YAMS";
    greq.paths = {"include"};
    greq.recursive = true;
    auto gres = lease->streamingGrep(greq);
    ASSERT_TRUE(gres) << gres.error().message;

    // Now ping on same connection
    auto ping = lease->ping();
    ASSERT_TRUE(ping) << ping.error().message;
}

} // namespace test
} // namespace daemon
} // namespace yams
