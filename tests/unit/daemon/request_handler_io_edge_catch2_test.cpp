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

    (void)stop_source.request_stop();
    work.reset();
    io_thread.join();
}

TEST_CASE("RequestHandlerIoEdge: write errors ECONNRESET EPIPE handled",
          "[daemon][.io_edge_pending]") {
    SKIP("Async test harness pending for ECONNRESET/EPIPE error injection");
}
