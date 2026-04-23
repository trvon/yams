// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/request_handler.h>

#include <boost/asio.hpp>

#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <future>
#include <memory>
#include <thread>
#include <vector>

using namespace yams::daemon;
using namespace std::chrono_literals;

namespace {

class CountingStatusProcessor final : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request& request) override {
        REQUIRE(std::holds_alternative<StatusRequest>(request));
        calls_.fetch_add(1, std::memory_order_relaxed);
        StatusResponse response{};
        response.running = true;
        response.ready = true;
        response.version = "test";
        response.overallStatus = "ready";
        co_return Response{std::in_place_type<StatusResponse>, std::move(response)};
    }

    int calls() const { return calls_.load(std::memory_order_relaxed); }

private:
    std::atomic<int> calls_{0};
};

class LargeSuccessProcessor final : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request&) override {
        co_return Response{std::in_place_type<SuccessResponse>,
                           SuccessResponse{std::string(8ULL * 1024ULL * 1024ULL, 'x')}};
    }
};

uint32_t test_crc32(const std::vector<uint8_t>& data) {
    uint32_t crc = 0xFFFFFFFF;
    for (auto byte : data) {
        crc ^= byte;
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ ((crc & 1) ? 0xEDB88320u : 0u);
        }
    }
    return ~crc;
}

std::vector<uint8_t> make_raw_frame(const std::vector<uint8_t>& payload, uint32_t flags = 0) {
    MessageFramer::FrameHeader header;
    header.payload_size = static_cast<uint32_t>(payload.size());
    header.checksum = test_crc32(payload);
    header.flags = flags;
    header.to_network();

    std::vector<uint8_t> frame(sizeof(header) + payload.size());
    std::memcpy(frame.data(), &header, sizeof(header));
    if (!payload.empty()) {
        std::memcpy(frame.data() + sizeof(header), payload.data(), payload.size());
    }
    return frame;
}

Message read_message(boost::asio::local::stream_protocol::socket& socket, MessageFramer& framer) {
    std::array<uint8_t, MessageFramer::HEADER_SIZE> header_buf{};
    boost::asio::read(socket, boost::asio::buffer(header_buf));

    auto header_result =
        framer.parse_header(std::span<const uint8_t>(header_buf.data(), header_buf.size()));
    REQUIRE(header_result);

    std::vector<uint8_t> frame(header_buf.begin(), header_buf.end());
    const auto payload_size = header_result.value().payload_size;
    if (payload_size > 0) {
        std::vector<uint8_t> payload(payload_size);
        boost::asio::read(socket, boost::asio::buffer(payload));
        frame.insert(frame.end(), payload.begin(), payload.end());
    }

    auto message_result = framer.parse_frame(frame);
    REQUIRE(message_result);
    return std::move(message_result.value());
}

void write_in_chunks(boost::asio::local::stream_protocol::socket& socket,
                     const std::vector<uint8_t>& frame,
                     std::initializer_list<std::size_t> chunkSizes) {
    std::size_t offset = 0;
    for (const auto chunkSize : chunkSizes) {
        if (offset >= frame.size()) {
            break;
        }
        const auto remaining = frame.size() - offset;
        const auto toWrite = std::min(chunkSize, remaining);
        boost::asio::write(socket, boost::asio::buffer(frame.data() + offset, toWrite));
        offset += toWrite;
    }
    if (offset < frame.size()) {
        boost::asio::write(socket,
                           boost::asio::buffer(frame.data() + offset, frame.size() - offset));
    }
}

} // namespace

TEST_CASE("RequestHandlerIoEdge: malformed frame returns shaped parse error",
          "[daemon][ipc][io-edge][malformed-frame]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    boost::asio::io_context io;

    boost::asio::local::stream_protocol::socket client_sock(io);
    boost::asio::local::stream_protocol::socket server_sock(io);
    boost::asio::local::connect_pair(client_sock, server_sock);

    RequestHandler::Config cfg;
    cfg.enable_multiplexing = false;
    cfg.enable_streaming = false;

    auto handler = std::make_shared<RequestHandler>(std::shared_ptr<RequestProcessor>{}, cfg);

    yams::compat::stop_source stop_source;
    auto server_sock_ptr =
        std::make_shared<boost::asio::local::stream_protocol::socket>(std::move(server_sock));
    std::promise<void> handler_finished;
    auto handler_future = handler_finished.get_future();

    auto work = boost::asio::make_work_guard(io);
    boost::asio::co_spawn(
        io,
        [handler, server_sock_ptr, token = stop_source.get_token(),
         finished = std::move(handler_finished)]() mutable -> boost::asio::awaitable<void> {
            co_await handler->handle_connection(server_sock_ptr, token, 1);
            finished.set_value();
        },
        boost::asio::detached);

    std::thread io_thread([&io]() { io.run(); });

    // Valid transport framing with a payload that protobuf cannot parse as an Envelope.
    const auto malformed_frame = make_raw_frame(std::vector<uint8_t>{0x80});
    boost::asio::write(client_sock, boost::asio::buffer(malformed_frame));

    MessageFramer framer;
    auto error_message = read_message(client_sock, framer);
    REQUIRE(error_message.requestId == 0);

    auto* payload = std::get_if<Response>(&error_message.payload);
    REQUIRE(payload != nullptr);
    auto* error = std::get_if<ErrorResponse>(payload);
    REQUIRE(error != nullptr);
    REQUIRE(error->code == yams::ErrorCode::SerializationError);
    CHECK(error->message.find("Failed to parse protobuf Envelope") != std::string::npos);

    REQUIRE(handler_future.wait_for(2s) == std::future_status::ready);

    [[maybe_unused]] const bool stop_requested = stop_source.request_stop();
    work.reset();
    io_thread.join();
}

TEST_CASE("RequestHandlerIoEdge: write errors ECONNRESET EPIPE handled",
          "[daemon][ipc][io-edge][peer-close]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

#ifndef _WIN32
    const auto previousSigpipeHandler = std::signal(SIGPIPE, SIG_IGN);
#endif

    boost::asio::io_context io;

    boost::asio::local::stream_protocol::socket client_sock(io);
    boost::asio::local::stream_protocol::socket server_sock(io);
    boost::asio::local::connect_pair(client_sock, server_sock);

    RequestHandler::Config cfg;
    cfg.enable_multiplexing = false;
    cfg.enable_streaming = false;

    auto processor = std::make_shared<LargeSuccessProcessor>();
    auto handler = std::make_shared<RequestHandler>(processor, cfg);

    yams::compat::stop_source stop_source;
    auto server_sock_ptr =
        std::make_shared<boost::asio::local::stream_protocol::socket>(std::move(server_sock));
    std::promise<void> handler_finished;
    auto handler_future = handler_finished.get_future();

    auto work = boost::asio::make_work_guard(io);
    boost::asio::co_spawn(
        io,
        [handler, server_sock_ptr, token = stop_source.get_token(),
         finished = std::move(handler_finished)]() mutable -> boost::asio::awaitable<void> {
            co_await handler->handle_connection(server_sock_ptr, token, 1);
            finished.set_value();
        },
        boost::asio::detached);

    std::thread io_thread([&io]() { io.run(); });

    MessageFramer framer;
    Message msg;
    msg.version = 1;
    msg.requestId = 99;
    msg.payload = Request{std::in_place_type<PingRequest>, PingRequest{}};
    std::vector<uint8_t> frame;
    REQUIRE(framer.frame_message_into(msg, frame));
    boost::asio::write(client_sock, boost::asio::buffer(frame));

    boost::system::error_code ec;
    client_sock.shutdown(boost::asio::local::stream_protocol::socket::shutdown_both, ec);
    client_sock.close(ec);

    REQUIRE(handler_future.wait_for(3s) == std::future_status::ready);

    [[maybe_unused]] const bool stop_requested = stop_source.request_stop();
    work.reset();
    io_thread.join();

#ifndef _WIN32
    std::signal(SIGPIPE, previousSigpipeHandler);
#endif
}

TEST_CASE("RequestHandlerIoEdge: fragmented requests preserve persistent session reuse",
          "[daemon][ipc][io-edge][fragmented][reuse]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    boost::asio::io_context io;

    boost::asio::local::stream_protocol::socket client_sock(io);
    boost::asio::local::stream_protocol::socket server_sock(io);
    boost::asio::local::connect_pair(client_sock, server_sock);

    RequestHandler::Config cfg;
    cfg.enable_multiplexing = false;
    cfg.enable_streaming = false;

    auto processor = std::make_shared<CountingStatusProcessor>();
    auto handler = std::make_shared<RequestHandler>(processor, cfg);

    yams::compat::stop_source stop_source;
    auto server_sock_ptr =
        std::make_shared<boost::asio::local::stream_protocol::socket>(std::move(server_sock));
    auto work = boost::asio::make_work_guard(io);
    boost::asio::co_spawn(
        io,
        [handler, server_sock_ptr,
         token = stop_source.get_token()]() -> boost::asio::awaitable<void> {
            co_await handler->handle_connection(server_sock_ptr, token, 1);
            co_return;
        },
        boost::asio::detached);

    std::thread io_thread([&io]() { io.run(); });

    MessageFramer framer;
    auto buildStatusFrame = [&](uint64_t requestId) {
        Message msg;
        msg.version = 1;
        msg.requestId = requestId;
        msg.payload = Request{std::in_place_type<StatusRequest>, StatusRequest{.detailed = false}};
        std::vector<uint8_t> frame;
        REQUIRE(framer.frame_message_into(msg, frame));
        return frame;
    };

    const auto frame1 = buildStatusFrame(41);
    write_in_chunks(client_sock, frame1, {1, 2, 5, 3});
    auto response1 = read_message(client_sock, framer);
    REQUIRE(response1.requestId == 41);
    auto* response_payload1 = std::get_if<Response>(&response1.payload);
    REQUIRE(response_payload1 != nullptr);
    auto* status1 = std::get_if<StatusResponse>(response_payload1);
    REQUIRE(status1 != nullptr);

    const auto frame2 = buildStatusFrame(42);
    write_in_chunks(client_sock, frame2, {7, 1, 1, 64});
    auto response2 = read_message(client_sock, framer);
    REQUIRE(response2.requestId == 42);
    auto* response_payload2 = std::get_if<Response>(&response2.payload);
    REQUIRE(response_payload2 != nullptr);
    auto* status2 = std::get_if<StatusResponse>(response_payload2);
    REQUIRE(status2 != nullptr);

    REQUIRE(processor->calls() == 2);

    [[maybe_unused]] const bool stop_requested = stop_source.request_stop();
    {
        boost::system::error_code close_ec;
        client_sock.close(close_ec);
    }
    work.reset();
    io_thread.join();
}
