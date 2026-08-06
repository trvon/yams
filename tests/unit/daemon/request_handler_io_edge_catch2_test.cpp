// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/request_handler.h>

#include <boost/asio.hpp>

#include <array>
#include <chrono>
#include <csignal>
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

#ifndef _WIN32
class ScopedSigpipeIgnore {
public:
    ScopedSigpipeIgnore() : previous_(std::signal(SIGPIPE, SIG_IGN)) {}
    ~ScopedSigpipeIgnore() {
        if (previous_ != SIG_ERR) {
            std::signal(SIGPIPE, previous_);
        }
    }

    ScopedSigpipeIgnore(const ScopedSigpipeIgnore&) = delete;
    ScopedSigpipeIgnore& operator=(const ScopedSigpipeIgnore&) = delete;

private:
    using Handler = void (*)(int);
    Handler previous_;
};
#endif

class BlockingSearchProcessor final : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request& request) override {
        const auto* searchRequest = std::get_if<SearchRequest>(&request);
        REQUIRE(searchRequest != nullptr);
        REQUIRE(searchRequest->cancellationSignal);
        entered_.store(true, std::memory_order_release);

        boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
        while (!released_.load(std::memory_order_acquire)) {
            if (searchRequest->cancellationSignal->load(std::memory_order_acquire)) {
                cancellationObserved_.store(true, std::memory_order_release);
                break;
            }
            timer.expires_after(5ms);
            co_await timer.async_wait(boost::asio::use_awaitable);
        }

        SearchResponse response{};
        co_return Response{std::in_place_type<SearchResponse>, std::move(response)};
    }

    [[nodiscard]] bool entered() const { return entered_.load(std::memory_order_acquire); }
    [[nodiscard]] bool cancellationObserved() const {
        return cancellationObserved_.load(std::memory_order_acquire);
    }
    void release() { released_.store(true, std::memory_order_release); }

private:
    std::atomic<bool> entered_{false};
    std::atomic<bool> released_{false};
    std::atomic<bool> cancellationObserved_{false};
};

bool waitFor(std::chrono::milliseconds timeout, const std::function<bool()>& predicate) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(5ms);
    }
    return predicate();
}

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
    constexpr std::size_t kHeaderSize = sizeof(MessageFramer::FrameHeader);
    MessageFramer::FrameHeader header;
    header.payload_size = static_cast<uint32_t>(payload.size());
    header.checksum = test_crc32(payload);
    header.flags = flags;
    header.to_network();

    std::vector<uint8_t> frame(kHeaderSize + payload.size());
    std::memcpy(frame.data(), &header, sizeof(header));
    if (!payload.empty()) {
        std::memcpy(frame.data() + kHeaderSize, payload.data(), payload.size());
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

TEST_CASE("RequestHandlerIoEdge: disconnect cancels an in-flight search",
          "[daemon][ipc][io-edge][cancel][disconnect]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    boost::asio::io_context io;

    boost::asio::local::stream_protocol::socket client_sock(io);
    boost::asio::local::stream_protocol::socket server_sock(io);
    boost::asio::local::connect_pair(client_sock, server_sock);

    RequestHandler::Config cfg;
    cfg.enable_multiplexing = true;
    cfg.enable_streaming = false;

    auto processor = std::make_shared<BlockingSearchProcessor>();
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

    constexpr uint64_t kRequestId = 31337;
    MessageFramer framer;
    Message message;
    message.version = 1;
    message.requestId = kRequestId;
    message.payload = Request{std::in_place_type<SearchRequest>,
                              SearchRequest{.query = "disconnect cancellation"}};
    std::vector<uint8_t> frame;
    REQUIRE(framer.frame_message_into(message, frame));
    boost::asio::write(client_sock, boost::asio::buffer(frame));

    REQUIRE(waitFor(2s, [&] { return processor->entered(); }));

    boost::system::error_code ec;
    client_sock.close(ec);
    REQUIRE(handler_future.wait_for(2s) == std::future_status::ready);

    CHECK(waitFor(2s, [&] { return processor->cancellationObserved(); }));

    processor->release();

    [[maybe_unused]] const bool stop_requested = stop_source.request_stop();
    work.reset();
    io_thread.join();
}

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
    ScopedSigpipeIgnore sigpipeGuard;
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

    boost::system::error_code ec;
    client_sock.shutdown(boost::asio::local::stream_protocol::socket::shutdown_both, ec);
    client_sock.close(ec);

    Request request{std::in_place_type<StatusRequest>, StatusRequest{.detailed = false}};
    auto resultFuture = boost::asio::co_spawn(io, handler->handle_request(server_sock, request, 99),
                                              boost::asio::use_future);
    io.run();
    auto result = resultFuture.get();

    REQUIRE(processor->calls() == 1);
    REQUIRE_FALSE(result);
    CHECK(result.error().code == yams::ErrorCode::NetworkError);
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
