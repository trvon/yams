// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/ipc/request_handler.h>

#include <boost/asio.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>

#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <thread>

using namespace yams::daemon;
using namespace boost::asio;

namespace {

class MockRequestProcessor : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request& request) override {
        co_return SuccessResponse{"OK"};
    }

    boost::asio::awaitable<std::optional<Response>>
    process_streaming(const Request& request) override {
        // Simulate long running operation
        boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
        timer.expires_after(std::chrono::milliseconds(100));
        co_await timer.async_wait(boost::asio::use_awaitable);
        co_return SuccessResponse{"Streamed"};
    }

    bool supports_streaming(const Request& request) const override { return true; }
};

} // anonymous namespace

TEST_CASE("RequestHandlerLifetime: detached coroutine survives handler destruction",
          "[daemon][.streaming_disabled]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif
    // Streaming path is currently disabled in request_handler.cpp (force_unary=true)
    // to avoid head-of-line blocking. This test requires the streaming path to spawn
    // detached coroutines. Skip until streaming is re-enabled.
    SKIP("Streaming path disabled (force_unary=true in request_handler.cpp)");

    boost::asio::io_context io;

    // Create a socket pair
    boost::asio::local::stream_protocol::socket client_sock(io);
    boost::asio::local::stream_protocol::socket server_sock(io);
    boost::asio::local::connect_pair(client_sock, server_sock);

    auto server_sock_ptr =
        std::make_shared<boost::asio::local::stream_protocol::socket>(std::move(server_sock));

    // Create handler
    RequestHandler::Config config;
    config.enable_multiplexing = true;
    config.enable_streaming = true;
    auto processor = std::make_shared<MockRequestProcessor>();
    auto handler = std::make_shared<RequestHandler>(processor, config);

    yams::compat::stop_source stop_source;

    // Spawn the handler connection loop
    std::promise<void> handler_finished;
    auto handler_future = handler_finished.get_future();

    boost::asio::co_spawn(
        io,
        [&, handler, server_sock_ptr,
         token = stop_source.get_token()]() mutable -> boost::asio::awaitable<void> {
            co_await handler->handle_connection(server_sock_ptr, token, 1);
            handler_finished.set_value();
        },
        boost::asio::detached);

    // Send a request that triggers streaming (and thus detached coroutine)
    // We need to construct a valid message frame
    Request req = SearchRequest{}; // SearchRequest supports streaming
    Message msg;
    msg.version = 1;
    msg.requestId = 123;
    msg.expectsStreamingResponse = true;
    msg.payload = req;

    MessageFramer framer(1024 * 1024);
    std::vector<uint8_t> frame;
    auto res = framer.frame_message_into(msg, frame);
    REQUIRE(res);

    boost::asio::write(client_sock, boost::asio::buffer(frame));

    // Run IO for a bit to let the request be received and detached coroutine spawned
    io.run_for(std::chrono::milliseconds(10));

    // Now close the server socket to force handle_connection to exit
    server_sock_ptr->close();

    // Run IO until handle_connection finishes
    io.run_for(std::chrono::milliseconds(10));

    // Verify handler_connection finished
    REQUIRE(handler_future.wait_for(std::chrono::seconds(0)) == std::future_status::ready);

    // Drop the main handler shared_ptr
    std::weak_ptr<RequestHandler> weak_handler = handler;
    handler.reset();

    // At this point, if the detached coroutine captured shared_from_this,
    // weak_handler should still be lockable (or at least the object alive).
    // But wait, the detached coroutine might have finished if the socket close caused write error.
    // However, the MockRequestProcessor waits 100ms.
    // We only ran for 20ms total.
    // So the detached coroutine should still be waiting on the timer.

    // If the fix is working, the handler should still be alive because the detached coroutine holds
    // a shared_ptr.
    REQUIRE_FALSE(weak_handler.expired());
    INFO("RequestHandler should be kept alive by detached coroutine");

    // Run IO to let the detached coroutine finish (timer expires)
    io.run_for(std::chrono::milliseconds(200));

    // Now the detached coroutine should be done, and handler destroyed
    REQUIRE(weak_handler.expired());
    INFO("RequestHandler should be destroyed after detached coroutine finishes");
}
