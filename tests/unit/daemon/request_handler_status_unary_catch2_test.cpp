// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/request_handler.h>

#include <boost/asio.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <thread>
#include <vector>

using namespace yams::daemon;

namespace {

class CountingProcessor final : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request&) override {
        unary_calls_++;
        StatusResponse s{};
        s.running = true;
        s.ready = false;
        s.version = "test";
        s.overallStatus = "initializing";
        co_return Response{std::in_place_type<StatusResponse>, s};
    }

    boost::asio::awaitable<std::optional<Response>> process_streaming(const Request&) override {
        streaming_calls_++;
        co_return std::optional<Response>{std::nullopt};
    }

    bool supports_streaming(const Request&) const override { return true; }

    int unary_calls() const { return unary_calls_.load(); }
    int streaming_calls() const { return streaming_calls_.load(); }

private:
    std::atomic<int> unary_calls_{0};
    std::atomic<int> streaming_calls_{0};
};

} // namespace

TEST_CASE("RequestHandler: StatusRequest forces unary even when client expects streaming",
          "[daemon][ipc][status][unary]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    boost::asio::io_context io;

    boost::asio::local::stream_protocol::socket client_sock(io);
    boost::asio::local::stream_protocol::socket server_sock(io);
    boost::asio::local::connect_pair(client_sock, server_sock);

    RequestHandler::Config cfg;
    cfg.enable_multiplexing = false;
    cfg.enable_streaming = true;

    auto processor = std::make_shared<CountingProcessor>();
    auto handler = std::make_shared<RequestHandler>(processor, cfg);

    // Run the handler on the server socket.
    yams::compat::stop_source stop_source;
    auto server_sock_ptr =
        std::make_shared<boost::asio::local::stream_protocol::socket>(std::move(server_sock));
    boost::asio::co_spawn(
        io,
        [handler, server_sock_ptr,
         token = stop_source.get_token()]() -> boost::asio::awaitable<void> {
            co_await handler->handle_connection(server_sock_ptr, token, 1);
            co_return;
        },
        boost::asio::detached);

    // Drive the async server while the client uses sync I/O.
    auto work = boost::asio::make_work_guard(io);
    std::thread io_thread([&io]() { io.run(); });

    // Send StatusRequest that asks for streaming.
    Message msg;
    msg.version = 1;
    msg.requestId = 7;
    msg.expectsStreamingResponse = true;
    StatusRequest sreq;
    sreq.detailed = true;
    msg.payload = Request{std::in_place_type<StatusRequest>, sreq};

    MessageFramer framer(1024 * 1024);
    std::vector<uint8_t> frame;
    auto fr = framer.frame_message_into(msg, frame);
    REQUIRE(fr);
    boost::asio::write(client_sock, boost::asio::buffer(frame));

    // Read one response frame and verify it's not chunked.
    std::array<uint8_t, MessageFramer::HEADER_SIZE> hdr_buf{};
    boost::asio::read(client_sock, boost::asio::buffer(hdr_buf));
    auto hdr_res = framer.parse_header(std::span<const uint8_t>(hdr_buf.data(), hdr_buf.size()));
    REQUIRE(hdr_res);
    const auto hdr = hdr_res.value();
    REQUIRE(hdr.is_chunked() == false);

    std::vector<uint8_t> payload(hdr.payload_size);
    boost::asio::read(client_sock, boost::asio::buffer(payload));

    // Confirm handler used the unary processor path.
    REQUIRE(processor->unary_calls() == 1);
    REQUIRE(processor->streaming_calls() == 0);

    // Stop the connection loop.
    stop_source.request_stop();
    {
        boost::system::error_code ec;
        client_sock.close(ec);
    }
    work.reset();
    io_thread.join();
}
