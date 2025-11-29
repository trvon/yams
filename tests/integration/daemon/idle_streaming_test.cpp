#include <chrono>
#include <filesystem>
#include <thread>
#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/write.hpp>
#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/SocketServer.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

// Windows daemon tests are currently unstable - AF_UNIX socket shutdown crashes
#ifdef _WIN32
#define SKIP_DAEMON_TEST_ON_WINDOWS()                                                              \
    SKIP("Daemon tests unstable on Windows - see windows-daemon-ipc-plan.md")
#else
#define SKIP_DAEMON_TEST_ON_WINDOWS() ((void)0)
#endif

using namespace yams::daemon;
using namespace std::chrono_literals;

struct IdleStreamingFixture {
    IdleStreamingFixture() {
        socketPath_ = std::filesystem::temp_directory_path() / "yams_test_idle_streaming.sock";
        std::filesystem::remove(socketPath_);

        coordinator_ = std::make_unique<WorkCoordinator>();
        coordinator_->start(4);

        state_ = std::make_unique<StateComponent>();

        dispatcher_ = std::make_unique<RequestDispatcher>(nullptr, nullptr, nullptr);

        SocketServer::Config config;
        config.socketPath = socketPath_;
        // Use shorter timeout for faster test: 3 timeouts = 3 x 500ms = 1.5s idle
        config.connectionTimeout = std::chrono::milliseconds(500);
        config.maxConnections = 10;

        server_ = std::make_unique<SocketServer>(config, coordinator_.get(), dispatcher_.get(),
                                                 state_.get());
        auto result = server_->start();
        REQUIRE(result.has_value());

        std::this_thread::sleep_for(100ms);
    }

    ~IdleStreamingFixture() {
        if (server_) {
            server_->stop();
        }
        if (coordinator_) {
            coordinator_->stop();
            coordinator_->join();
        }
        std::filesystem::remove(socketPath_);
    }

    std::shared_ptr<boost::asio::io_context> createClientIo() {
        return std::make_shared<boost::asio::io_context>();
    }

    boost::asio::local::stream_protocol::socket createClientSocket(boost::asio::io_context& io) {
        boost::asio::local::stream_protocol::socket socket(io);
        socket.connect(boost::asio::local::stream_protocol::endpoint(socketPath_.string()));
        return socket;
    }

    std::vector<uint8_t> createPingRequest(uint64_t requestId, bool streaming = false) {
        Message msg;
        msg.version = PROTOCOL_VERSION;
        msg.requestId = requestId;
        msg.expectsStreamingResponse = streaming;
        msg.timestamp = std::chrono::steady_clock::now();
        PingRequest req; // Ping is simplest - doesn't need daemon/services
        msg.payload = Request{req};

        MessageFramer framer;
        auto result = framer.frame_message(msg);
        return result.has_value() ? result.value() : std::vector<uint8_t>{};
    }

    std::filesystem::path socketPath_;
    std::unique_ptr<WorkCoordinator> coordinator_;
    std::unique_ptr<StateComponent> state_;
    std::unique_ptr<RequestDispatcher> dispatcher_;
    std::unique_ptr<SocketServer> server_;
};

TEST_CASE_METHOD(IdleStreamingFixture, "Persistent connection - streaming after idle",
                 "[daemon][idle][streaming][persistent]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    // Test persistent connection that goes idle then sends streaming request
    auto clientIo = createClientIo();
    auto socket = createClientSocket(*clientIo);

    // Send initial request to establish connection
    {
        auto encoded = createPingRequest(1, false); // unary first
        REQUIRE(!encoded.empty());
        boost::asio::write(socket, boost::asio::buffer(encoded));

        std::array<uint8_t, 4096> buf;
        boost::system::error_code ec;
        auto bytes = socket.read_some(boost::asio::buffer(buf), ec);
        REQUIRE(!ec);
        REQUIRE(bytes > 0);
    }

    // Connection stays open but idle for 2s (exceeds 3 x 500ms timeouts)
    std::this_thread::sleep_for(2s);

    // Send streaming request on same connection after idle
    {
        auto encoded = createPingRequest(2, true); // streaming=true
        REQUIRE(!encoded.empty());
        boost::asio::write(socket, boost::asio::buffer(encoded));

        std::array<uint8_t, 4096> buf;
        boost::system::error_code ec;
        socket.non_blocking(true);

        auto start = std::chrono::steady_clock::now();
        bool received = false;
        while (!received && std::chrono::steady_clock::now() - start < 5s) {
            auto bytes = socket.read_some(boost::asio::buffer(buf), ec);
            if (!ec && bytes > 0) {
                received = true;
                break;
            }
            if (ec == boost::asio::error::would_block) {
                std::this_thread::sleep_for(50ms);
                continue;
            }
            if (ec) {
                break;
            }
        }

        REQUIRE(received); // This should pass after the multiplexing fix
    }
}

TEST_CASE_METHOD(IdleStreamingFixture, "New connection - streaming after server idle",
                 "[daemon][idle][streaming][new-conn]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    // Test 1: Verify server works immediately after start
    {
        auto clientIo = createClientIo();
        auto socket = createClientSocket(*clientIo);
        auto encoded = createPingRequest(1, true); // streaming=true
        REQUIRE(!encoded.empty());

        boost::asio::write(socket, boost::asio::buffer(encoded));

        std::array<uint8_t, 4096> buf;
        boost::system::error_code ec;
        auto bytes = socket.read_some(boost::asio::buffer(buf), ec);

        REQUIRE(!ec);
        REQUIRE(bytes > 0);
        // Socket closes here, connection ends
    }

    // Test 2: Let server idle past 3 read timeouts (3 x 500ms = 1.5s)
    // Server stays running but has no active connections
    // Sleep for 2s to ensure we exceed the 3-timeout threshold
    std::this_thread::sleep_for(2s);

    // Test 3: New connection after server idle should work with streaming request
    {
        auto clientIo = createClientIo();
        auto socket = createClientSocket(*clientIo);
        auto encoded = createPingRequest(2, true); // streaming=true
        REQUIRE(!encoded.empty());

        boost::asio::write(socket, boost::asio::buffer(encoded));

        std::array<uint8_t, 4096> buf;
        boost::system::error_code ec;
        socket.non_blocking(true);

        auto start = std::chrono::steady_clock::now();
        bool received = false;
        while (!received && std::chrono::steady_clock::now() - start < 5s) {
            auto bytes = socket.read_some(boost::asio::buffer(buf), ec);
            if (!ec && bytes > 0) {
                received = true;
                break;
            }
            if (ec == boost::asio::error::would_block) {
                std::this_thread::sleep_for(50ms);
                continue;
            }
            if (ec) {
                break;
            }
        }

        REQUIRE(received); // This should pass after the multiplexing fix
    }
}

TEST_CASE_METHOD(IdleStreamingFixture, "Persistent connection - unary after idle",
                 "[daemon][idle][unary][persistent]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    // Test persistent connection that goes idle then sends unary request
    auto clientIo = createClientIo();
    auto socket = createClientSocket(*clientIo);

    // Send initial request to establish connection
    {
        auto encoded = createPingRequest(1, false); // unary first
        REQUIRE(!encoded.empty());
        boost::asio::write(socket, boost::asio::buffer(encoded));

        std::array<uint8_t, 4096> buf;
        boost::system::error_code ec;
        auto bytes = socket.read_some(boost::asio::buffer(buf), ec);
        REQUIRE(!ec);
        REQUIRE(bytes > 0);
    }

    // Connection stays open but idle for 2s (exceeds 3 x 500ms timeouts)
    std::this_thread::sleep_for(2s);

    // Send another unary request on same connection after idle
    {
        auto encoded = createPingRequest(2, false); // unary
        REQUIRE(!encoded.empty());
        boost::asio::write(socket, boost::asio::buffer(encoded));

        std::array<uint8_t, 4096> buf;
        boost::system::error_code ec;
        auto bytes = socket.read_some(boost::asio::buffer(buf), ec);

        REQUIRE(!ec);
        REQUIRE(bytes > 0); // Unary should always work
    }
}

TEST_CASE_METHOD(IdleStreamingFixture, "New connection - unary after server idle",
                 "[daemon][idle][unary][new-conn]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();
    // Wait for server to idle (3 x 500ms = 1.5s)
    std::this_thread::sleep_for(2s);

    auto clientIo = createClientIo();
    auto socket = createClientSocket(*clientIo);
    auto encoded = createPingRequest(1, false); // streaming=false
    REQUIRE(!encoded.empty());

    boost::asio::write(socket, boost::asio::buffer(encoded));

    std::array<uint8_t, 4096> buf;
    boost::system::error_code ec;
    auto bytes = socket.read_some(boost::asio::buffer(buf), ec);

    REQUIRE(!ec);
    REQUIRE(bytes > 0); // Unary should always work
}
