// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/ipc/request_handler.h>

#include <boost/asio.hpp>

#include <atomic>
#include <chrono>

using namespace yams::daemon;

TEST_CASE("RequestHandler: writer_drain clears queued state on write error",
          "[daemon][ipc][mux][writer_drain]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    boost::asio::io_context io;

    boost::asio::local::stream_protocol::socket client_sock(io);
    boost::asio::local::stream_protocol::socket server_sock(io);
    boost::asio::local::connect_pair(client_sock, server_sock);

    RequestHandler::Config cfg;
    cfg.enable_multiplexing = true;
    cfg.enable_streaming = true;
    cfg.total_queued_bytes_cap = 1024 * 1024;
    cfg.per_request_queue_cap = 16;
    cfg.writer_budget_bytes_per_turn = 1; // ensure drain pops only one frame per turn

    auto handler = std::make_shared<RequestHandler>(/*processor=*/nullptr, cfg);

    constexpr uint64_t rid = 42;
    REQUIRE(handler->testing_enqueue_frame_sync(rid, std::vector<uint8_t>(256, 0xAA), false));
    REQUIRE(handler->testing_enqueue_frame_sync(rid, std::vector<uint8_t>(256, 0xBB), true));

    // Sanity: queued bytes present.
    {
        auto snap = handler->testing_mux_snapshot();
        REQUIRE(snap.queues == 1);
        REQUIRE(snap.total_queued_bytes > 0);
        REQUIRE(snap.active > 0);
    }

    // Close peer to force async_write error during drain.
    client_sock.close();

    std::atomic_bool done{false};
    boost::asio::co_spawn(
        io,
        [handler, s = std::move(server_sock), &done]() mutable -> boost::asio::awaitable<void> {
            co_await handler->testing_writer_drain(s);
            done.store(true, std::memory_order_release);
        },
        boost::asio::detached);

    const auto start = std::chrono::steady_clock::now();
    while (!done.load(std::memory_order_acquire) &&
           (std::chrono::steady_clock::now() - start) < std::chrono::seconds(2)) {
        io.run_for(std::chrono::milliseconds(50));
    }
    REQUIRE(done.load(std::memory_order_acquire));

    // On write error, queued frames/bytes must be dropped so the connection can't get stuck in a
    // permanently "exhausted" state.
    {
        auto snap = handler->testing_mux_snapshot();
        REQUIRE(snap.queues == 0);
        REQUIRE(snap.active == 0);
        REQUIRE(snap.total_queued_bytes == 0);
        REQUIRE(snap.writer_running == false);
    }

    // Subsequent enqueues should re-activate rr_active_ normally.
    REQUIRE(handler->testing_enqueue_frame_sync(rid, std::vector<uint8_t>(32, 0xCC), true));
    {
        auto snap = handler->testing_mux_snapshot();
        REQUIRE(snap.active > 0);
        REQUIRE(snap.queues == 1);
    }
}
