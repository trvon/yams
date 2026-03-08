// DaemonClient list/status mapping regression tests

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include <yams/cli/cli_sync.h>
#include <yams/core/types.h>
#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

#ifndef _WIN32
#include <sys/un.h>
#endif

using namespace yams;
using namespace yams::daemon;
using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {

fs::path makeTempRuntimeDir(const std::string& name) {
    auto base = fs::temp_directory_path();
#ifndef _WIN32
    constexpr std::size_t maxUnixPath = sizeof(sockaddr_un::sun_path) - 1;
    auto candidate = base / "yams-client-tests" / name / "ipc.sock";
    if (candidate.native().size() >= maxUnixPath) {
        base = fs::path("/tmp");
    }
#endif
    auto dir = base / "yams-client-tests" / name;
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
        boost::asio::read(
            sock, boost::asio::buffer(frame.data() + sizeof(netHeader), header.payload_size), ec);
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
    status.ready = false;
    status.overallStatus = "initializing";
    status.lifecycleState = "initializing";
    status.retryAfterMs = 200;

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

TEST_CASE("DaemonClient list returns friendly retry error on StatusResponse",
          "[daemon][client][list][regression]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    auto runtimeDir = makeTempRuntimeDir("list-status-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    boost::asio::io_context serverIo;
    boost::asio::local::stream_protocol::acceptor acceptor(
        serverIo, boost::asio::local::stream_protocol::endpoint(socketPath.string()));

    std::thread serverThread([&] {
        try {
            boost::asio::local::stream_protocol::socket sock(serverIo);
            acceptor.accept(sock);

            auto frame = readFrame(sock);
            MessageFramer framer;
            auto parsed = framer.parse_frame(frame);
            if (!parsed) {
                throw std::runtime_error("failed to parse request frame");
            }

            const auto& reqMsg = parsed.value();
            REQUIRE(std::holds_alternative<Request>(reqMsg.payload));
            const auto& reqVariant = std::get<Request>(reqMsg.payload);
            REQUIRE(std::holds_alternative<ListRequest>(reqVariant));

            writeStatusResponse(sock, reqMsg.requestId);
            sock.close();
        } catch (...) {
        }
    });

    ClientConfig cfg;
    cfg.socketPath = socketPath;
    cfg.requestTimeout = 3s;
    cfg.headerTimeout = 3s;
    cfg.bodyTimeout = 3s;
    cfg.autoStart = false;

    DaemonClient client(cfg);
    ListRequest req;
    req.limit = 10;

    auto result = yams::cli::run_sync(client.list(req), 5s);

    if (serverThread.joinable()) {
        serverThread.join();
    }
    fs::remove(socketPath, ec);
    AsioConnectionPool::shutdown_all(100ms);

    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().code == ErrorCode::InvalidState);
    CHECK(result.error().message.find("Daemon not ready yet") != std::string::npos);
    CHECK(result.error().message.find("initializing") != std::string::npos);
}

TEST_CASE("DaemonClient list rewrites repeated EOF to retry guidance",
          "[daemon][client][list][regression]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    auto runtimeDir = makeTempRuntimeDir("list-eof-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    boost::asio::io_context serverIo;
    boost::asio::local::stream_protocol::acceptor acceptor(
        serverIo, boost::asio::local::stream_protocol::endpoint(socketPath.string()));

    std::thread serverThread([&] {
        try {
            for (int i = 0; i < 2; ++i) {
                boost::asio::local::stream_protocol::socket sock(serverIo);
                acceptor.accept(sock);
                (void)readFrame(sock);
                sock.close();
            }
        } catch (...) {
        }
    });

    ClientConfig cfg;
    cfg.socketPath = socketPath;
    cfg.requestTimeout = 3s;
    cfg.headerTimeout = 3s;
    cfg.bodyTimeout = 3s;
    cfg.autoStart = false;

    DaemonClient client(cfg);
    ListRequest req;
    req.limit = 10;

    auto result = yams::cli::run_sync(client.list(req), 5s);

    if (serverThread.joinable()) {
        serverThread.join();
    }
    fs::remove(socketPath, ec);
    AsioConnectionPool::shutdown_all(100ms);

    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().code == ErrorCode::InvalidState);
    CHECK(result.error().message.find("try again shortly") != std::string::npos);
    CHECK(result.error().message.find("[ipc:eof]") == std::string::npos);
}
