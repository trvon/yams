#include <spdlog/spdlog.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace std::chrono_literals;

namespace yams::daemon::test {

// Goal: add a few documents, then exercise search/grep/list via pooled requests.
// The assertions are tolerant of daemon availability and indexing latency.
TEST(PoolAddThenQueryIT, AddFilesThenSearchGrepList) {
    // Ensure daemon is likely up and pool connections are warmed
    (void)DaemonClient::startDaemon({});
    // Wait for the daemon to be ready by repeatedly trying to ping it.
    bool daemon_ready = false;
    for (int i = 0; i < 10; ++i) {
        yams::cli::PooledRequestManager<PingRequest, PongResponse> ping_mgr(pool_cfg);
        PingRequest ping_req{};
        std::atomic<bool> pong_received = false;
        auto render_ping = [&](const PongResponse&) -> yams::Result<void> {
            pong_received = true;
            return yams::Result<void>();
        };
        auto fallback_ignore = []() -> yams::Result<void> { return yams::Result<void>(); };
        if (ping_mgr.execute(ping_req, fallback_ignore, render_ping) && pong_received) {
            daemon_ready = true;
            break;
        }
        std::this_thread::sleep_for(500ms);
    }
    if (!daemon_ready) {
        GTEST_SKIP() << "Daemon did not become ready in time, skipping test.";
    }

    // 2. Prepare and add three small in-memory documents
    yams::cli::PooledRequestManager<AddDocumentRequest, AddDocumentResponse> add_mgr(pool_cfg);
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count();
    const std::string base = "pool-add-then-query-" + std::to_string(now_ms);

    auto make_add = [&](const std::string& suffix, const std::string& text) {
        AddDocumentRequest req{};
        req.name = base + "-" + suffix + ".txt";
        req.content = text;
        req.tags = {"it", "pooled", "add"};
        return req;
    };

    auto a1 = make_add("alpha", "alpha content for pooled request test\nneedle-one\n");
    auto a2 = make_add("beta", "beta content for pooled request test\nneedle-two\n");
    auto a3 = make_add("gamma", "gamma content for pooled request test\nneedle-three\n");

    auto render_add = [](const AddDocumentResponse&) -> yams::Result<void> {
        return yams::Result<void>();
    };

    ASSERT_TRUE(add_mgr.execute(a1, fallback_fail, render_add));
    ASSERT_TRUE(add_mgr.execute(a2, fallback_fail, render_add));
    ASSERT_TRUE(add_mgr.execute(a3, fallback_fail, render_add));

    // 3. Poll with retries until indexed content is found.
    const int max_attempts = 25; // 25 * 200ms = 5s total wait

    // Search
    yams::cli::PooledRequestManager<SearchRequest, SearchResponse> search_mgr(pool_cfg);
    bool search_ok = false;
    for (int attempts = 0; attempts < max_attempts; ++attempts) {
        SearchRequest sreq{};
        sreq.query = "pooled request test";
        sreq.limit = 10;
        sreq.timeout = 5s;
        std::atomic<bool> found_results = false;
        auto render_search = [&](const SearchResponse& resp) -> yams::Result<void> {
            if (resp.results.size() >= 3) {
                found_results = true;
            }
            return yams::Result<void>();
        };
        if (search_mgr.execute(sreq, fallback_fail, render_search) && found_results) {
            search_ok = true;
            break;
        }
        std::this_thread::sleep_for(200ms);
    }
    EXPECT_TRUE(search_ok) << "Search did not find expected results after adding documents.";

    // Grep
    yams::cli::PooledRequestManager<GrepRequest, GrepResponse> grep_mgr(pool_cfg);
    bool grep_ok = false;
    for (int attempts = 0; attempts < max_attempts; ++attempts) {
        GrepRequest greq{};
        greq.pattern = "needle-two";
        greq.literalText = true;
        std::atomic<bool> grep_found = false;
        auto render_grep = [&](const GrepResponse& resp) -> yams::Result<void> {
            if (!resp.matches.empty()) {
                grep_found = true;
            }
            return yams::Result<void>();
        };
        if (grep_mgr.execute(greq, fallback_fail, render_grep) && grep_found) {
            grep_ok = true;
            break;
        }
        std::this_thread::sleep_for(200ms);
    }
    EXPECT_TRUE(grep_ok) << "Grep did not find expected results after adding documents.";

    // List
    yams::cli::PooledRequestManager<ListRequest, ListResponse> list_mgr(pool_cfg);
    bool list_ok = false;
    for (int attempts = 0; attempts < max_attempts; ++attempts) {
        ListRequest lreq{};
        lreq.recent = true;
        lreq.recentCount = 10;
        std::atomic<size_t> found_count = 0;
        auto render_list = [&](const ListResponse& resp) -> yams::Result<void> {
            size_t count = 0;
            for (const auto& item : resp.items) {
                if (item.name.find(base) != std::string::npos) {
                    count++;
                }
            }
            found_count = count;
            return yams::Result<void>();
        };
        if (list_mgr.execute(lreq, fallback_fail, render_list) && found_count >= 3) {
            list_ok = true;
            break;
        }
        std::this_thread::sleep_for(200ms);
    }
    EXPECT_TRUE(list_ok) << "List did not find the recently added documents.";
}

} // namespace yams::daemon::test