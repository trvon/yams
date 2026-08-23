// SPDX-License-Identifier: GPL-3.0-or-later

// pi-lens-ignore: fatal error
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

class ListOneShotProcessor final : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request&) override { co_return makeResponse(); }

    boost::asio::awaitable<std::optional<Response>> process_streaming(const Request&) override {
        co_return makeResponse();
    }

    bool supports_streaming(const Request&) const override { return true; }

private:
    static Response makeResponse() {
        ListResponse response;
        response.totalCount = 2;
        response.items = {
            ListEntry{.hash = "hash-a", .path = "/docs/a.txt", .fileName = "a.txt"},
            ListEntry{.hash = "hash-b", .path = "/docs/b.txt", .fileName = "b.txt"},
        };
        return Response{std::in_place_type<ListResponse>, std::move(response)};
    }
};

class SearchOneShotProcessor final : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request&) override { co_return makeResponse(); }

    boost::asio::awaitable<std::optional<Response>> process_streaming(const Request&) override {
        co_return makeResponse();
    }

    bool supports_streaming(const Request&) const override { return true; }

private:
    static Response makeResponse() {
        SearchResponse response;
        response.totalCount = 2;
        response.results = {
            SearchResult{.id = "hash-a", .path = "/docs/a.txt", .score = 1.0},
            SearchResult{.id = "hash-b", .path = "/docs/b.txt", .score = 0.5},
        };
        return Response{std::in_place_type<SearchResponse>, std::move(response)};
    }
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

TEST_CASE("RequestHandler: one-shot list header does not repeat final entries",
          "[daemon][ipc][list][streaming][regression]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    boost::asio::io_context io;
    boost::asio::local::stream_protocol::socket clientSock(io);
    boost::asio::local::stream_protocol::socket serverSock(io);
    boost::asio::local::connect_pair(clientSock, serverSock);

    RequestHandler::Config config;
    config.enable_multiplexing = false;
    config.enable_streaming = true;

    auto handler =
        std::make_shared<RequestHandler>(std::make_shared<ListOneShotProcessor>(), config);
    yams::compat::stop_source stopSource;
    auto serverSockPtr =
        std::make_shared<boost::asio::local::stream_protocol::socket>(std::move(serverSock));
    boost::asio::co_spawn(
        io,
        [handler, serverSockPtr, token = stopSource.get_token()]() -> boost::asio::awaitable<void> {
            co_await handler->handle_connection(serverSockPtr, token, 1);
            co_return;
        },
        boost::asio::detached);

    auto work = boost::asio::make_work_guard(io);
    std::thread ioThread([&io]() { io.run(); });

    Message request;
    request.version = 1;
    request.requestId = 8;
    request.expectsStreamingResponse = true;
    request.payload = Request{std::in_place_type<ListRequest>, ListRequest{}};

    MessageFramer framer(1024 * 1024);
    std::vector<uint8_t> frame;
    REQUIRE(framer.frame_message_into(request, frame));
    boost::asio::write(clientSock, boost::asio::buffer(frame));

    auto readResponse = [&]() {
        std::array<uint8_t, MessageFramer::HEADER_SIZE> headerBytes{};
        boost::asio::read(clientSock, boost::asio::buffer(headerBytes));
        auto parsedHeader =
            framer.parse_header(std::span<const uint8_t>(headerBytes.data(), headerBytes.size()));
        REQUIRE(parsedHeader);
        std::vector<uint8_t> framed(headerBytes.begin(), headerBytes.end());
        const auto payloadOffset = framed.size();
        framed.resize(payloadOffset + parsedHeader.value().payload_size);
        boost::asio::read(clientSock, boost::asio::buffer(framed.data() + payloadOffset,
                                                          parsedHeader.value().payload_size));
        auto parsed = framer.parse_frame(framed);
        REQUIRE(parsed);
        return std::pair{parsedHeader.value(), std::move(parsed.value())};
    };

    auto [headerFrame, headerMessage] = readResponse();
    REQUIRE(headerFrame.is_header_only());
    const auto* headerResponse =
        std::get_if<ListResponse>(&std::get<Response>(headerMessage.payload));
    REQUIRE(headerResponse != nullptr);
    CHECK(headerResponse->items.empty());
    CHECK(headerResponse->totalCount == 2);

    auto [finalFrame, finalMessage] = readResponse();
    REQUIRE(finalFrame.is_last_chunk());
    const auto* finalResponse =
        std::get_if<ListResponse>(&std::get<Response>(finalMessage.payload));
    REQUIRE(finalResponse != nullptr);
    REQUIRE(finalResponse->items.size() == 2);
    CHECK(finalResponse->items[0].hash == "hash-a");
    CHECK(finalResponse->items[1].hash == "hash-b");

    stopSource.request_stop();
    {
        boost::system::error_code error;
        clientSock.close(error);
    }
    work.reset();
    ioThread.join();
}

TEST_CASE("RequestHandler: one-shot search header does not repeat final results",
          "[daemon][ipc][search][streaming][regression]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    boost::asio::io_context io;
    boost::asio::local::stream_protocol::socket clientSock(io);
    boost::asio::local::stream_protocol::socket serverSock(io);
    boost::asio::local::connect_pair(clientSock, serverSock);

    RequestHandler::Config config;
    config.enable_multiplexing = false;
    config.enable_streaming = true;

    auto handler =
        std::make_shared<RequestHandler>(std::make_shared<SearchOneShotProcessor>(), config);
    yams::compat::stop_source stopSource;
    auto serverSockPtr =
        std::make_shared<boost::asio::local::stream_protocol::socket>(std::move(serverSock));
    boost::asio::co_spawn(
        io,
        [handler, serverSockPtr, token = stopSource.get_token()]() -> boost::asio::awaitable<void> {
            co_await handler->handle_connection(serverSockPtr, token, 1);
            co_return;
        },
        boost::asio::detached);

    auto work = boost::asio::make_work_guard(io);
    std::thread ioThread([&io]() { io.run(); });

    Message request;
    request.version = 1;
    request.requestId = 9;
    request.expectsStreamingResponse = true;
    request.payload = Request{std::in_place_type<SearchRequest>, SearchRequest{}};

    MessageFramer framer(1024 * 1024);
    std::vector<uint8_t> frame;
    REQUIRE(framer.frame_message_into(request, frame));
    boost::asio::write(clientSock, boost::asio::buffer(frame));

    auto readResponse = [&]() {
        std::array<uint8_t, MessageFramer::HEADER_SIZE> headerBytes{};
        boost::asio::read(clientSock, boost::asio::buffer(headerBytes));
        auto parsedHeader =
            framer.parse_header(std::span<const uint8_t>(headerBytes.data(), headerBytes.size()));
        REQUIRE(parsedHeader);
        std::vector<uint8_t> framed(headerBytes.begin(), headerBytes.end());
        const auto payloadOffset = framed.size();
        framed.resize(payloadOffset + parsedHeader.value().payload_size);
        boost::asio::read(clientSock, boost::asio::buffer(framed.data() + payloadOffset,
                                                          parsedHeader.value().payload_size));
        auto parsed = framer.parse_frame(framed);
        REQUIRE(parsed);
        return std::pair{parsedHeader.value(), std::move(parsed.value())};
    };

    auto [headerFrame, headerMessage] = readResponse();
    REQUIRE(headerFrame.is_header_only());
    const auto* headerResponse =
        std::get_if<SearchResponse>(&std::get<Response>(headerMessage.payload));
    REQUIRE(headerResponse != nullptr);
    CHECK(headerResponse->results.empty());
    CHECK(headerResponse->totalCount == 2);

    auto [finalFrame, finalMessage] = readResponse();
    REQUIRE(finalFrame.is_last_chunk());
    const auto* finalResponse =
        std::get_if<SearchResponse>(&std::get<Response>(finalMessage.payload));
    REQUIRE(finalResponse != nullptr);
    REQUIRE(finalResponse->results.size() == 2);
    CHECK(finalResponse->results[0].id == "hash-a");
    CHECK(finalResponse->results[1].id == "hash-b");

    stopSource.request_stop();
    {
        boost::system::error_code error;
        clientSock.close(error);
    }
    work.reset();
    ioThread.join();
}
