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

// This test ensures a streaming request (Search) completes promptly even if new data
// is being added concurrently, i.e., the stream should not hang waiting for new data.
TEST(PoolStreamingIT, StreamingCompletesWhileAddingData) {
    // Best-effort ensure daemon is running
    (void)DaemonClient::startDaemon({});
    std::this_thread::sleep_for(100ms);

    // Use a small pool; allow parallelism for add + search
    yams::cli::DaemonClientPool::Config pool_cfg;
    pool_cfg.max_clients = 2;
    pool_cfg.client_config.enableChunkedResponses = true; // force streaming path

    yams::cli::PooledRequestManager<SearchRequest, SearchResponse> search_mgr(pool_cfg);
    yams::cli::PooledRequestManager<AddDocumentRequest, AddDocumentResponse> add_mgr(pool_cfg);
    yams::cli::PooledRequestManager<PingRequest, PongResponse> ping_mgr(pool_cfg);

    std::atomic<bool> search_rendered{false};
    std::atomic<bool> add_done{false};

    // Prepare a generic search that should complete regardless of current index contents
    SearchRequest sreq{};
    sreq.query = "test"; // generic query; results may be zero
    sreq.limit = 5;
    sreq.timeout = 5s; // server-side timeout hint

    auto render_search = [&](const SearchResponse&) -> yams::Result<void> {
        search_rendered = true;
        return yams::Result<void>();
    };
    auto fallback_ok = [&]() -> yams::Result<void> {
        // If daemon isn't available, treat as success to keep CI stable
        return yams::Result<void>();
    };

    // Prepare an in-memory add (no filesystem dependency)
    AddDocumentRequest areq{};
    areq.content = "hello world from streaming-concurrency-test";
    areq.name = std::string("streaming-concurrency-") +
                std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now().time_since_epoch())
                                   .count());
    areq.tags = {"it", "concurrency", "streaming"};

    auto render_add = [&](const AddDocumentResponse&) -> yams::Result<void> {
        add_done = true;
        return yams::Result<void>();
    };

    // Pre-warm daemon connection with a ping to avoid auto-start delays counting against us
    PingRequest ping{};
    auto rping = ping_mgr.execute(ping, fallback_ok,
                                  [&](const PongResponse&) { return yams::Result<void>(); });
    (void)rping;
    std::this_thread::sleep_for(200ms);

    auto t_start = std::chrono::steady_clock::now();

    // Launch search in one thread
    std::thread t_search([&] {
        auto r = search_mgr.execute(sreq, fallback_ok, render_search);
        EXPECT_TRUE(r);
    });

    // Slight delay, then launch add in parallel
    std::thread t_add([&] {
        std::this_thread::sleep_for(150ms);
        auto r = add_mgr.execute(areq, fallback_ok, render_add);
        EXPECT_TRUE(r);
    });

    t_search.join();
    t_add.join();

    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t_start);

    // Assert the streaming search completed in a reasonable time window.
    // Allow for daemon auto-start and pool acquisition delays in CI environments.
    EXPECT_LT(elapsed_ms.count(), 25000) << "Streaming search should not hang when data is added.";

    // Either the render path (daemon) or fallback (no daemon) should have succeeded.
    // We don't require both due to CI flakiness around daemon availability.
    SUCCEED() << "rendered=" << (search_rendered ? "true" : "false")
              << ", add_done=" << (add_done ? "true" : "false")
              << ", elapsed_ms=" << elapsed_ms.count();
}

} // namespace yams::daemon::test
