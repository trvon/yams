// AsioTransportAdapter retry behavior tests

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <array>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/write.hpp>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <future>
#include <thread>

#include <yams/core/types.h>
#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

#ifndef _WIN32
#include <sys/un.h>
#endif

using namespace yams::daemon;
using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {

fs::path makeTempRuntimeDir(const std::string& name) {
    auto base = fs::temp_directory_path();
#ifndef _WIN32
    constexpr std::size_t maxUnixPath = sizeof(sockaddr_un::sun_path) - 1;
    auto candidate = base / "yams-transport-tests" / name / "ipc.sock";
    if (candidate.native().size() >= maxUnixPath) {
        base = fs::path("/tmp");
    }
#endif
    auto dir = base / "yams-transport-tests" / name;
    std::error_code ec;
    fs::create_directories(dir, ec);
    return dir;
}

std::string randomSuffix() {
    return std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

std::vector<uint8_t> readFrame(boost::asio::local::stream_protocol::socket& sock) {
    MessageFramer::FrameHeader netHeader{};
    boost::system::error_code ec;
    boost::asio::read(sock, boost::asio::buffer(&netHeader, sizeof(netHeader)), ec);
    if (ec) {
        throw std::runtime_error("read header failed: " + ec.message());
    }

    MessageFramer::FrameHeader header = netHeader;
    header.from_network();

    std::vector<uint8_t> frame(sizeof(netHeader) + header.payload_size);
    std::memcpy(frame.data(), &netHeader, sizeof(netHeader));

    if (header.payload_size > 0) {
        boost::asio::read(sock,
                          boost::asio::buffer(frame.data() + sizeof(netHeader),
                                              header.payload_size),
                          ec);
        if (ec) {
            throw std::runtime_error("read payload failed: " + ec.message());
        }
    }
    return frame;
}

void writeStatusResponse(boost::asio::local::stream_protocol::socket& sock, uint64_t requestId) {
    MessageFramer framer;
    StatusResponse status{};
    status.running = true;
    status.ready = true;
    status.version = "test";

    Message responseMsg;
    responseMsg.version = PROTOCOL_VERSION;
    responseMsg.requestId = requestId;
    responseMsg.timestamp = std::chrono::steady_clock::now();
    responseMsg.payload = status;

    auto framed = framer.frame_message(responseMsg);
    if (!framed) {
        throw std::runtime_error("failed to frame response");
    }

    boost::system::error_code ec;
    boost::asio::write(sock, boost::asio::buffer(framed.value()), ec);
    if (ec) {
        throw std::runtime_error("write response failed: " + ec.message());
    }
}

} // namespace

TEST_CASE("AsioTransportAdapter retries unary request after EOF", "[daemon][transport][unit][ipc]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    auto runtimeDir = makeTempRuntimeDir("unary-retry-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    boost::asio::io_context server_io;
    boost::asio::local::stream_protocol::acceptor acceptor(
        server_io, boost::asio::local::stream_protocol::endpoint(socketPath.string()));

    std::thread server_thread([&] {
        try {
            // First connection: accept, read header, close without response.
            boost::asio::local::stream_protocol::socket sock1(server_io);
            acceptor.accept(sock1);
            std::array<uint8_t, sizeof(MessageFramer::FrameHeader)> header_buf{};
            boost::asio::read(sock1, boost::asio::buffer(header_buf));
            sock1.close();

            // Second connection: accept and respond.
            boost::asio::local::stream_protocol::socket sock2(server_io);
            acceptor.accept(sock2);
            auto frame = readFrame(sock2);
            MessageFramer framer;
            auto parsed = framer.parse_frame(frame);
            if (!parsed) {
                throw std::runtime_error("failed to parse request frame");
            }
            auto reqId = parsed.value().requestId;
            writeStatusResponse(sock2, reqId);
        } catch (const std::exception&) {
        }
    });

    TransportOptions opts;
    opts.socketPath = socketPath;
    opts.poolEnabled = true;
    opts.requestTimeout = 5s;
    opts.headerTimeout = 2s;
    opts.bodyTimeout = 2s;

    AsioTransportAdapter adapter(opts);
    const Request req{StatusRequest{false}};

    auto fut = boost::asio::co_spawn(GlobalIOContext::global_executor(), adapter.send_request(req),
                                     boost::asio::use_future);
    REQUIRE(fut.wait_for(5s) == std::future_status::ready);
    auto result = fut.get();
    REQUIRE(result.has_value());
    REQUIRE(std::holds_alternative<StatusResponse>(result.value().payload));

    server_thread.join();
    fs::remove(socketPath, ec);
    AsioConnectionPool::shutdown_all(100ms);
}

TEST_CASE("AsioTransportAdapter retries streaming request after transient close",
          "[daemon][transport][unit][ipc]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    auto runtimeDir = makeTempRuntimeDir("streaming-retry-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    boost::asio::io_context server_io;
    boost::asio::local::stream_protocol::acceptor acceptor(
        server_io, boost::asio::local::stream_protocol::endpoint(socketPath.string()));

    std::thread server_thread([&] {
        try {
            // First connection: accept and close immediately.
            boost::asio::local::stream_protocol::socket sock1(server_io);
            acceptor.accept(sock1);
            sock1.close();

            // Second connection: accept and respond.
            boost::asio::local::stream_protocol::socket sock2(server_io);
            acceptor.accept(sock2);
            auto frame = readFrame(sock2);
            MessageFramer framer;
            auto parsed = framer.parse_frame(frame);
            if (!parsed) {
                throw std::runtime_error("failed to parse request frame");
            }
            auto reqId = parsed.value().requestId;
            writeStatusResponse(sock2, reqId);
        } catch (const std::exception&) {
        }
    });

    TransportOptions opts;
    opts.socketPath = socketPath;
    opts.poolEnabled = true;
    opts.requestTimeout = 5s;
    opts.headerTimeout = 2s;
    opts.bodyTimeout = 2s;

    AsioTransportAdapter adapter(opts);
    const Request req{StatusRequest{false}};

    std::promise<void> header_seen;
    auto header_fut = header_seen.get_future();
    std::promise<void> complete_seen;
    auto complete_fut = complete_seen.get_future();
    std::promise<Error> error_seen;
    std::atomic<bool> header_set{false};
    std::atomic<bool> complete_set{false};
    std::atomic<bool> error_set{false};

    auto onHeader = [&](const Response& r) {
        if (std::holds_alternative<StatusResponse>(r)) {
            if (!header_set.exchange(true)) {
                header_seen.set_value();
            }
        }
    };
    auto onChunk = [&](const Response&, bool) { return true; };
    auto onError = [&](const Error& e) {
        if (!error_set.exchange(true)) {
            error_seen.set_value(e);
        }
    };
    auto onComplete = [&] {
        if (!complete_set.exchange(true)) {
            complete_seen.set_value();
        }
    };

    auto fut = boost::asio::co_spawn(
        GlobalIOContext::global_executor(),
        adapter.send_request_streaming(req, onHeader, onChunk, onError, onComplete),
        boost::asio::use_future);

    REQUIRE(fut.wait_for(5s) == std::future_status::ready);
    auto result = fut.get();
    REQUIRE(result.has_value());
    REQUIRE(header_fut.wait_for(1s) == std::future_status::ready);
    REQUIRE(complete_fut.wait_for(1s) == std::future_status::ready);

    server_thread.join();
    fs::remove(socketPath, ec);
    AsioConnectionPool::shutdown_all(100ms);
}
