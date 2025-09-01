// Clean, API-aligned test for adding then querying via the daemon client.

#include <spdlog/spdlog.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <string>
#include <thread>

#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/cli/asio_client_pool.hpp>

using namespace std::chrono_literals;

namespace yams::daemon::test {

class PoolAddThenQueryIT : public ::testing::Test {
protected:
    void SetUp() override {
        ClientConfig cfg{};
        // Attempt to start the daemon. It's okay if it's already running.
        auto started = DaemonClient::startDaemon(cfg);
        if (!started) {
            spdlog::warn("PoolAddThenQueryIT: could not auto-start daemon: {}",
                         started.error().message);
        }
        // Wait until daemon reports ready (metadata repo + content store initialized)
    DaemonClient probe(cfg);
    yams::cli::AsioClientPool pool{};
        const auto t_deadline = std::chrono::steady_clock::now() + 30s;
        bool ready = false;
        while (std::chrono::steady_clock::now() < t_deadline) {
            auto st = pool.status();
            if (st) {
                const auto& s = st.value();
                bool meta_ready = false;
                bool store_ready = false;
                if (auto it = s.readinessStates.find("metadata_repo");
                    it != s.readinessStates.end())
                    meta_ready = it->second;
                if (auto it = s.readinessStates.find("content_store");
                    it != s.readinessStates.end())
                    store_ready = it->second;
                if (s.ready || s.overallStatus == "ready" || (meta_ready && store_ready)) {
                    ready = true;
                    break;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
        ASSERT_TRUE(ready) << "Daemon not ready (metadata_repo/content_store) before test start";
        // Give the daemon a brief moment to settle sockets/listeners fully
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    void TearDown() {
        // Best-effort: ensure daemon is shut down between tests to avoid cross-test interference
        yams::daemon::DaemonClient c{};
        (void)c.shutdown(true);
        // Give the daemon a moment to stop and clean up socket/pid
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
};

TEST_F(PoolAddThenQueryIT, AddFilesThenSearchGrepList) {
    // Use single-use connections to exercise connection lifecycle under sequences.
    ClientConfig client_cfg;
    client_cfg.singleUseConnections = false;  // keep a persistent connection for sequence
    client_cfg.enableChunkedResponses = true; // align with server's chunked responses
    client_cfg.enableCircuitBreaker = false;  // let retries proceed even after transient errors
    client_cfg.requestTimeout = std::chrono::milliseconds(15000);
    client_cfg.headerTimeout = std::chrono::milliseconds(30000);
    client_cfg.bodyTimeout = std::chrono::milliseconds(60000);
    client_cfg.maxRetries = 5;
    DaemonClient client(client_cfg);
    yams::cli::AsioClientPool pool{};

    // 1) Ping
    auto ping_res = pool.ping();
    ASSERT_TRUE(ping_res) << "Daemon did not respond to ping: " << ping_res.error().message;

    // 2) Add three small in-memory documents
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count();
    const std::string base_name = std::string("pool-add-then-query-") + std::to_string(now_ms);

    auto make_add_req = [&](const std::string& suffix, const std::string& text) {
        yams::daemon::AddDocumentRequest req;
        req.name = base_name + "-" + suffix;
        req.content = text;
        req.tags = {"it-test"};
        return req;
    };

    auto a1_req = make_add_req("alpha", "alpha content for integration test\nneedle-one\n");
    auto a2_req = make_add_req("beta", "beta content for integration test\nneedle-two\n");
    auto a3_req = make_add_req("gamma", "gamma content for integration test\nneedle-three\n");

    auto add1_res = pool.call<yams::daemon::AddDocumentRequest, yams::daemon::AddDocumentResponse>(a1_req);
    ASSERT_TRUE(add1_res) << "Failed to add document 1: " << add1_res.error().message;
    auto add2_res = pool.call<yams::daemon::AddDocumentRequest, yams::daemon::AddDocumentResponse>(a2_req);
    ASSERT_TRUE(add2_res) << "Failed to add document 2: " << add2_res.error().message;
    auto add3_res = pool.call<yams::daemon::AddDocumentRequest, yams::daemon::AddDocumentResponse>(a3_req);
    ASSERT_TRUE(add3_res) << "Failed to add document 3: " << add3_res.error().message;

    // 3) List (retry: fetch a page and count entries that include our base_name)
    yams::daemon::ListRequest lreq{};
    lreq.limit = 1000;    // fetch a larger page to avoid pagination misses
    lreq.recentCount = 0; // show all (disable recent-only trimming when possible)
    lreq.recent = false;  // don't force recent
    yams::Result<yams::daemon::ListResponse> lres(yams::ErrorCode::Unknown);
    size_t match_count = 0;
    {
        const auto t_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        do {
            lres = pool.call<ListRequest, ListResponse>(lreq);
            match_count = 0;
            if (lres) {
                for (const auto& it : lres.value().items) {
                    if (it.name.find(base_name) != std::string::npos ||
                        it.path.find(base_name) != std::string::npos ||
                        it.fileName.find(base_name) != std::string::npos) {
                        match_count++;
                    }
                }
            }
            if (lres && match_count >= 3)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(150));
        } while (std::chrono::steady_clock::now() < t_deadline);
    }
    ASSERT_TRUE(lres) << "List request failed: " << lres.error().message;
    ASSERT_GE(match_count, 3u) << "Expected at least 3 listed items containing base_name";

    // 4) Grep (retry to allow indexing)
    yams::daemon::GrepRequest greq{};
    greq.pattern = "integration test";
    yams::Result<yams::daemon::GrepResponse> gres(yams::ErrorCode::Unknown);
    {
        const auto t_deadline = std::chrono::steady_clock::now() + 30s;
        do {
            gres = pool.call<yams::daemon::GrepRequest, yams::daemon::GrepResponse>(greq);
            if (gres && gres.value().totalMatches >= 3)
                break;
            std::this_thread::sleep_for(150ms);
        } while (std::chrono::steady_clock::now() < t_deadline);
    }
    ASSERT_TRUE(gres) << "Grep request failed: " << gres.error().message;
    EXPECT_GE(gres.value().totalMatches, 3u);

    // 5) Search (retry; now that list shows items, search should also surface content soon)
    yams::daemon::SearchRequest sreq{};
    sreq.query = "needle-two"; // content token within beta document
    sreq.literalText = true;   // exact substring
    yams::Result<yams::daemon::SearchResponse> sres(yams::ErrorCode::Unknown);
    {
        const auto t_deadline = std::chrono::steady_clock::now() + 30s;
        do {
            sres = pool.call<yams::daemon::SearchRequest, yams::daemon::SearchResponse>(sreq);
            if (sres && !sres.value().results.empty())
                break;
            std::this_thread::sleep_for(200ms);
        } while (std::chrono::steady_clock::now() < t_deadline);
    }
    ASSERT_TRUE(sres) << "Search request failed: " << sres.error().message;
    EXPECT_GE(sres.value().results.size(), 1u) << "No results for content token after add.";
}

} // namespace yams::daemon::test