// DaemonClient list/status mapping regression tests

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <mutex>
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

void writeListResponse(boost::asio::local::stream_protocol::socket& sock, uint64_t requestId) {
    MessageFramer framer;

    ListResponse response{};
    response.totalCount = 0;
    response.queryInfo = "list";

    Message responseMsg;
    responseMsg.version = PROTOCOL_VERSION;
    responseMsg.requestId = requestId;
    responseMsg.timestamp = std::chrono::steady_clock::now();
    responseMsg.payload = response;

    auto framed = framer.frame_message(responseMsg);
    if (!framed) {
        throw std::runtime_error("failed to frame list response");
    }

    boost::system::error_code ec;
    boost::asio::write(sock, boost::asio::buffer(framed.value()), ec);
    if (ec) {
        throw std::runtime_error("write list response failed: " + ec.message());
    }
}

void writeAddResponse(boost::asio::local::stream_protocol::socket& sock, uint64_t requestId) {
    MessageFramer framer;

    AddDocumentResponse response{};
    response.hash = "test-hash";
    response.path = "/tmp/test.txt";
    response.message = "queued";

    Message responseMsg;
    responseMsg.version = PROTOCOL_VERSION;
    responseMsg.requestId = requestId;
    responseMsg.timestamp = std::chrono::steady_clock::now();
    responseMsg.payload = response;

    auto framed = framer.frame_message(responseMsg);
    if (!framed) {
        throw std::runtime_error("failed to frame add response");
    }

    boost::system::error_code ec;
    boost::asio::write(sock, boost::asio::buffer(framed.value()), ec);
    if (ec) {
        throw std::runtime_error("write add response failed: " + ec.message());
    }
}

} // namespace

TEST_CASE("DaemonClient add accepts a successful transport retry without replaying it again",
          "[daemon][client][add][regression]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    auto runtimeDir = makeTempRuntimeDir("add-retry-success-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    boost::asio::io_context serverIo;
    boost::asio::local::stream_protocol::acceptor acceptor(
        serverIo, boost::asio::local::stream_protocol::endpoint(socketPath.string()));
    std::atomic<int> requestCount{0};

    std::thread serverThread([&] {
        try {
            boost::asio::local::stream_protocol::socket first(serverIo);
            acceptor.accept(first);
            (void)readFrame(first);
            requestCount.fetch_add(1, std::memory_order_relaxed);
            first.close();

            boost::asio::local::stream_protocol::socket retry(serverIo);
            acceptor.accept(retry);
            auto frame = readFrame(retry);
            requestCount.fetch_add(1, std::memory_order_relaxed);
            MessageFramer framer;
            auto parsed = framer.parse_frame(frame);
            if (!parsed) {
                throw std::runtime_error("failed to parse retried add frame");
            }
            writeAddResponse(retry, parsed.value().requestId);

            // A stale error in the request handler used to trigger an outer AddDocument retry
            // after the transport retry had already succeeded. Give that replay time to arrive.
            std::this_thread::sleep_for(250ms);
            boost::system::error_code availableEc;
            if (retry.available(availableEc) > 0 && !availableEc) {
                auto replay = readFrame(retry);
                requestCount.fetch_add(1, std::memory_order_relaxed);
                auto replayParsed = framer.parse_frame(replay);
                if (replayParsed) {
                    writeAddResponse(retry, replayParsed.value().requestId);
                }
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
    AddDocumentRequest req;
    req.path = "/tmp/test.txt";
    auto result = yams::cli::run_sync(client.streamingAddDocument(req), 5s);

    if (serverThread.joinable()) {
        serverThread.join();
    }
    fs::remove(socketPath, ec);
    AsioConnectionPool::shutdown_all(100ms);

    REQUIRE(result.has_value());
    CHECK(result.value().hash == "test-hash");
    CHECK(requestCount.load(std::memory_order_relaxed) == 2);
}

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

TEST_CASE("DaemonClient status uses a fresh connection for each call",
          "[daemon][client][status][regression]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    auto runtimeDir = makeTempRuntimeDir("status-fresh-connection-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    boost::asio::io_context serverIo;
    boost::asio::local::stream_protocol::acceptor acceptor(
        serverIo, boost::asio::local::stream_protocol::endpoint(socketPath.string()));

    std::mutex acceptedMutex;
    std::condition_variable acceptedCv;
    std::size_t acceptedCount = 0;
    auto recordAccepted = [&] {
        {
            std::lock_guard<std::mutex> lock(acceptedMutex);
            ++acceptedCount;
        }
        acceptedCv.notify_all();
    };
    auto waitAccepted = [&](std::size_t expected) {
        std::unique_lock<std::mutex> lock(acceptedMutex);
        return acceptedCv.wait_for(lock, 1s, [&] { return acceptedCount >= expected; });
    };
    std::atomic<bool> stop{false};

    std::thread firstServerThread([&] {
        try {
            boost::asio::local::stream_protocol::socket sock(serverIo);
            acceptor.accept(sock);
            recordAccepted();

            while (!stop.load(std::memory_order_acquire)) {
                auto frame = readFrame(sock);
                MessageFramer framer;
                auto parsed = framer.parse_frame(frame);
                if (!parsed) {
                    throw std::runtime_error("failed to parse request frame");
                }

                const auto& reqMsg = parsed.value();
                REQUIRE(std::holds_alternative<Request>(reqMsg.payload));
                const auto& reqVariant = std::get<Request>(reqMsg.payload);
                REQUIRE(std::holds_alternative<StatusRequest>(reqVariant));

                writeStatusResponse(sock, reqMsg.requestId);
            }
        } catch (...) {
        }
    });

    std::thread secondServerThread([&] {
        try {
            boost::asio::local::stream_protocol::socket sock(serverIo);
            acceptor.accept(sock);
            recordAccepted();

            auto frame = readFrame(sock);
            MessageFramer framer;
            auto parsed = framer.parse_frame(frame);
            if (!parsed) {
                throw std::runtime_error("failed to parse request frame");
            }

            const auto& reqMsg = parsed.value();
            REQUIRE(std::holds_alternative<Request>(reqMsg.payload));
            const auto& reqVariant = std::get<Request>(reqMsg.payload);
            REQUIRE(std::holds_alternative<StatusRequest>(reqVariant));

            writeStatusResponse(sock, reqMsg.requestId);
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

    auto first = yams::cli::run_sync(client.status(), 5s);
    REQUIRE(waitAccepted(1));
    REQUIRE(first.has_value());

    auto second = yams::cli::run_sync(client.status(), 5s);
    REQUIRE(waitAccepted(2));
    REQUIRE(second.has_value());

    stop.store(true, std::memory_order_release);
    AsioConnectionPool::shutdown_all(100ms);
    if (firstServerThread.joinable()) {
        firstServerThread.join();
    }
    if (secondServerThread.joinable()) {
        secondServerThread.join();
    }
    boost::system::error_code closeEc;
    acceptor.close(closeEc);
    fs::remove(socketPath, ec);
}

TEST_CASE("DaemonClient routes status to proxy and list to main socket",
          "[daemon][client][proxy][routing]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    auto runtimeDir = makeTempRuntimeDir("proxy-routing-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    auto proxyPath = runtimeDir / "ipc.proxy.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);
    fs::remove(proxyPath, ec);

    boost::asio::io_context mainIo;
    boost::asio::local::stream_protocol::acceptor mainAcceptor(
        mainIo, boost::asio::local::stream_protocol::endpoint(socketPath.string()));
    boost::asio::io_context proxyIo;
    boost::asio::local::stream_protocol::acceptor proxyAcceptor(
        proxyIo, boost::asio::local::stream_protocol::endpoint(proxyPath.string()));

    std::promise<void> proxyAccepted;
    std::promise<void> mainAccepted;

    std::thread proxyThread([&] {
        try {
            boost::asio::local::stream_protocol::socket sock(proxyIo);
            proxyAcceptor.accept(sock);
            proxyAccepted.set_value();

            auto frame = readFrame(sock);
            MessageFramer framer;
            auto parsed = framer.parse_frame(frame);
            if (!parsed) {
                throw std::runtime_error("failed to parse proxy request frame");
            }

            const auto& reqMsg = parsed.value();
            REQUIRE(std::holds_alternative<Request>(reqMsg.payload));
            const auto& reqVariant = std::get<Request>(reqMsg.payload);
            REQUIRE(std::holds_alternative<StatusRequest>(reqVariant));

            writeStatusResponse(sock, reqMsg.requestId);
        } catch (...) {
        }
    });

    std::thread mainThread([&] {
        try {
            boost::asio::local::stream_protocol::socket sock(mainIo);
            mainAcceptor.accept(sock);
            mainAccepted.set_value();

            auto frame = readFrame(sock);
            MessageFramer framer;
            auto parsed = framer.parse_frame(frame);
            if (!parsed) {
                throw std::runtime_error("failed to parse main request frame");
            }

            const auto& reqMsg = parsed.value();
            REQUIRE(std::holds_alternative<Request>(reqMsg.payload));
            const auto& reqVariant = std::get<Request>(reqMsg.payload);
            REQUIRE(std::holds_alternative<ListRequest>(reqVariant));

            writeListResponse(sock, reqMsg.requestId);
        } catch (...) {
        }
    });

    ClientConfig cfg;
    cfg.socketPath = socketPath;
    cfg.proxySocketPath = proxyPath;
    cfg.requestTimeout = 3s;
    cfg.headerTimeout = 3s;
    cfg.bodyTimeout = 3s;
    cfg.autoStart = false;

    DaemonClient client(cfg);

    auto statusResult = yams::cli::run_sync(client.status(), 5s);
    REQUIRE(proxyAccepted.get_future().wait_for(1s) == std::future_status::ready);
    REQUIRE(statusResult.has_value());

    ListRequest listReq;
    listReq.limit = 1;
    auto listResult = yams::cli::run_sync(client.list(listReq), 5s);
    REQUIRE(mainAccepted.get_future().wait_for(1s) == std::future_status::ready);
    REQUIRE(listResult.has_value());

    if (proxyThread.joinable()) {
        proxyThread.join();
    }
    if (mainThread.joinable()) {
        mainThread.join();
    }
    boost::system::error_code closeEc;
    mainAcceptor.close(closeEc);
    proxyAcceptor.close(closeEc);
    fs::remove(socketPath, ec);
    fs::remove(proxyPath, ec);
    AsioConnectionPool::shutdown_all(100ms);
}
