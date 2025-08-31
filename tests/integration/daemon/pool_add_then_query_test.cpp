// Integration coverage for add -> list/grep/search using pooled client with
// fallback-first semantics (no live daemon required).

#include <spdlog/spdlog.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <string>
#include <thread>

#include <yams/cli/daemon_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

using namespace std::chrono_literals;

namespace yams::daemon::test {

class PoolAddThenQueryIT : public ::testing::Test {};

TEST_F(PoolAddThenQueryIT, AddFilesThenSearchGrepList_FallbackFirst) {
    // Force deterministic fallback by pointing to a non-existent socket and disabling autoStart.
    yams::cli::DaemonClientPool::Config pool_cfg;
    pool_cfg.max_clients = 2;
    pool_cfg.client_config.autoStart = false;
    pool_cfg.client_config.socketPath = "/tmp/yams-nonexistent-it.sock";
    pool_cfg.client_config.requestTimeout = std::chrono::milliseconds(2000);

    // Build unique base name for synthetic docs
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count();
    const std::string base_name = std::string("pool-add-then-query-") + std::to_string(now_ms);

    auto make_add_req = [&](const std::string& suffix, const std::string& text) {
        AddDocumentRequest req;
        req.name = base_name + "-" + suffix;
        req.content = text;
        req.tags = {"it-test"};
        return req;
    };

    auto a1_req = make_add_req("alpha", "alpha content for integration test\nneedle-one\n");
    auto a2_req = make_add_req("beta", "beta content for integration test\nneedle-two\n");
    auto a3_req = make_add_req("gamma", "gamma content for integration test\nneedle-three\n");

    // Manager that will execute requests and rely on fallback when daemon is unavailable.
    yams::cli::PooledRequestManager<AddDocumentRequest, AddDocumentResponse> add_mgr(pool_cfg);
    yams::cli::PooledRequestManager<ListRequest, ListResponse> list_mgr(pool_cfg);
    yams::cli::PooledRequestManager<GrepRequest, GrepResponse> grep_mgr(pool_cfg);
    yams::cli::PooledRequestManager<SearchRequest, SearchResponse> search_mgr(pool_cfg);

    auto expect_fallback_only = [](auto&& manager, auto&& request) {
        bool rendered = false;
        bool fellback = false;
        auto render = [&](const auto&) -> yams::Result<void> {
            rendered = true;
            return yams::Result<void>();
        };
        auto fallback = [&]() -> yams::Result<void> {
            fellback = true;
            return yams::Result<void>();
        };
        auto r = manager.execute(request, fallback, render);
        EXPECT_TRUE(r);
        EXPECT_FALSE(rendered) << "Render should not occur without a daemon";
        EXPECT_TRUE(fellback) << "Fallback should be executed when daemon is unavailable";
    };

    // 1) Add three docs (fallback expected)
    expect_fallback_only(add_mgr, a1_req);
    expect_fallback_only(add_mgr, a2_req);
    expect_fallback_only(add_mgr, a3_req);

    // 2) List (fallback expected)
    ListRequest lreq{};
    lreq.limit = 100;
    expect_fallback_only(list_mgr, lreq);

    // 3) Grep (fallback expected)
    GrepRequest greq{};
    greq.pattern = "integration test";
    expect_fallback_only(grep_mgr, greq);

    // 4) Search (fallback expected)
    SearchRequest sreq{};
    sreq.query = "needle-two";
    sreq.literalText = true;
    expect_fallback_only(search_mgr, sreq);
}

} // namespace yams::daemon::test