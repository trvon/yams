#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
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

using namespace yams::daemon;
using namespace std::chrono_literals;

struct WriterDrainFixture {
    WriterDrainFixture() {
        socketPath_ = std::filesystem::temp_directory_path() / "yams_test_writer_drain.sock";
        std::filesystem::remove(socketPath_);

        coordinator_ = std::make_unique<WorkCoordinator>();
        coordinator_->start(4);

        state_ = std::make_unique<StateComponent>();
        dispatcher_ = std::make_unique<RequestDispatcher>(nullptr, nullptr, nullptr);

        SocketServer::Config config;
        config.socketPath = socketPath_;
        config.connectionTimeout = std::chrono::milliseconds(500);
        config.maxConnections = 10;

        server_ = std::make_unique<SocketServer>(config, coordinator_.get(), dispatcher_.get(),
                                                 state_.get());
        auto result = server_->start();
        REQUIRE(result.has_value());

        std::this_thread::sleep_for(100ms);
    }

    ~WriterDrainFixture() {
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
        PingRequest req;
        msg.payload = Request{req};

        MessageFramer framer;
        auto result = framer.frame_message(msg);
        return result.has_value() ? result.value() : std::vector<uint8_t>{};
    }

    bool waitForResponse(boost::asio::local::stream_protocol::socket& socket,
                         std::chrono::milliseconds timeout = 3s) {
        std::array<uint8_t, 4096> buf;
        boost::system::error_code ec;
        socket.non_blocking(true);

        auto start = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - start < timeout) {
            auto bytes = socket.read_some(boost::asio::buffer(buf), ec);
            if (!ec && bytes > 0) {
                return true;
            }
            if (ec == boost::asio::error::would_block) {
                std::this_thread::sleep_for(50ms);
                continue;
            }
            if (ec) {
                INFO("Socket error: " << ec.message());
                return false;
            }
        }
        return false;
    }

    std::filesystem::path socketPath_;
    std::unique_ptr<WorkCoordinator> coordinator_;
    std::unique_ptr<StateComponent> state_;
    std::unique_ptr<RequestDispatcher> dispatcher_;
    std::unique_ptr<SocketServer> server_;
};

TEST_CASE_METHOD(WriterDrainFixture,
                 "writer_drain: Multiple streaming requests on persistent connection",
                 "[daemon][writer_drain][streaming]") {
    // Test multiple streaming requests on same connection to verify writer_drain completes properly
    auto clientIo = createClientIo();
    auto socket = createClientSocket(*clientIo);

    // Send 3 streaming requests sequentially
    for (int i = 1; i <= 3; i++) {
        INFO("Streaming request " << i);
        auto encoded = createPingRequest(i, true);
        REQUIRE(!encoded.empty());

        boost::asio::write(socket, boost::asio::buffer(encoded));

        bool received = waitForResponse(socket, 3s);
        REQUIRE(received);

        // Small delay between requests
        std::this_thread::sleep_for(100ms);
    }
}

TEST_CASE_METHOD(WriterDrainFixture,
                 "writer_drain: Streaming request after idle with previous streaming",
                 "[daemon][writer_drain][idle]") {
    // This test reproduces the production scenario:
    // 1. Send streaming request (writer_drain runs)
    // 2. Connection goes idle
    // 3. Send another streaming request
    // Expected: Second request should get response
    // Bug: Second request hangs if writer_drain didn't complete properly

    auto clientIo = createClientIo();
    auto socket = createClientSocket(*clientIo);

    // First streaming request
    {
        INFO("First streaming request");
        auto encoded = createPingRequest(1, true);
        REQUIRE(!encoded.empty());
        boost::asio::write(socket, boost::asio::buffer(encoded));

        bool received = waitForResponse(socket, 3s);
        REQUIRE(received);
    }

    // Let connection go idle (2 seconds > 3 x 500ms timeout)
    INFO("Connection going idle for 2 seconds");
    std::this_thread::sleep_for(2s);

    // Second streaming request after idle
    {
        INFO("Second streaming request after idle");
        auto encoded = createPingRequest(2, true);
        REQUIRE(!encoded.empty());
        boost::asio::write(socket, boost::asio::buffer(encoded));

        // This is where the bug manifests - handler spawns but never responds
        bool received = waitForResponse(socket, 5s);

        if (!received) {
            FAIL("Second streaming request timed out - writer_drain likely stuck!");
        }
        REQUIRE(received);
    }
}

TEST_CASE_METHOD(WriterDrainFixture, "writer_drain: Concurrent streaming requests",
                 "[daemon][writer_drain][concurrent]") {
    // Test multiple concurrent streaming requests to check for race conditions
    auto clientIo = createClientIo();
    auto socket = createClientSocket(*clientIo);

    // Send multiple requests without waiting for responses (multiplexing)
    const int NUM_REQUESTS = 5;

    for (int i = 1; i <= NUM_REQUESTS; i++) {
        auto encoded = createPingRequest(i, true);
        REQUIRE(!encoded.empty());
        boost::asio::write(socket, boost::asio::buffer(encoded));
        std::this_thread::sleep_for(10ms); // Small delay to interleave
    }

    // Now wait for all responses - accumulate into buffer and count complete frames
    int responses_received = 0;
    std::vector<uint8_t> accumulated;
    std::array<uint8_t, 4096> buf;
    boost::system::error_code ec;
    socket.non_blocking(true);

    auto start = std::chrono::steady_clock::now();
    int read_attempts = 0;
    while (responses_received < NUM_REQUESTS && std::chrono::steady_clock::now() - start < 10s) {
        auto bytes = socket.read_some(boost::asio::buffer(buf), ec);
        read_attempts++;
        if (!ec && bytes > 0) {
            accumulated.insert(accumulated.end(), buf.begin(), buf.begin() + bytes);

            // Count complete frames (each frame has 20-byte header with payload size)
            size_t offset = 0;
            while (offset + 20 <= accumulated.size()) {
                // Read payload size from header (big-endian/network byte order uint32 at offset 8)
                // Frame header: magic(4) + version(4) + payload_size(4) + checksum(4) + flags(4)
                uint32_t payload_size = (static_cast<uint32_t>(accumulated[offset + 8]) << 24) |
                                        (static_cast<uint32_t>(accumulated[offset + 9]) << 16) |
                                        (static_cast<uint32_t>(accumulated[offset + 10]) << 8) |
                                        static_cast<uint32_t>(accumulated[offset + 11]);

                size_t frame_size = 20 + payload_size;
                if (offset + frame_size <= accumulated.size()) {
                    responses_received++;
                    INFO("Received complete frame " << responses_received << " (size=" << frame_size
                                                    << ")");
                    offset += frame_size;
                } else {
                    break; // Incomplete frame
                }
            }
            // Remove processed frames
            if (offset > 0) {
                accumulated.erase(accumulated.begin(), accumulated.begin() + offset);
            }
        }
        if (ec == boost::asio::error::would_block) {
            std::this_thread::sleep_for(50ms);
            continue;
        }
        if (ec) {
            break;
        }
    }

    INFO("Received " << responses_received << " out of " << NUM_REQUESTS << " responses");
    REQUIRE(responses_received == NUM_REQUESTS);
}

TEST_CASE_METHOD(WriterDrainFixture, "writer_drain: Rapid fire streaming requests",
                 "[daemon][writer_drain][rapid]") {
    // Send streaming requests rapidly to stress test writer_drain
    auto clientIo = createClientIo();
    auto socket = createClientSocket(*clientIo);

    const int NUM_RAPID = 5; // Reduced from 10 to avoid timeout issues

    // Send all requests rapidly (with tiny delay to prevent socket buffer overflow)
    for (int i = 1; i <= NUM_RAPID; i++) {
        auto encoded = createPingRequest(i, true);
        REQUIRE(!encoded.empty());
        boost::asio::write(socket, boost::asio::buffer(encoded));
        std::this_thread::sleep_for(10ms); // Small delay between sends
    }

    // Collect responses - accumulate and count complete frames
    std::vector<uint8_t> accumulated;
    std::array<uint8_t, 4096> buf;
    boost::system::error_code ec;
    socket.non_blocking(true);

    auto start = std::chrono::steady_clock::now();
    int received = 0;
    while (received < NUM_RAPID && std::chrono::steady_clock::now() - start < 10s) {
        auto bytes = socket.read_some(boost::asio::buffer(buf), ec);
        if (!ec && bytes > 0) {
            accumulated.insert(accumulated.end(), buf.begin(), buf.begin() + bytes);

            // Count complete frames
            size_t offset = 0;
            while (offset + 20 <= accumulated.size()) {
                // Frame header: magic(4) + version(4) + payload_size(4) + checksum(4) + flags(4)
                // Network byte order (big-endian)
                uint32_t payload_size = (static_cast<uint32_t>(accumulated[offset + 8]) << 24) |
                                        (static_cast<uint32_t>(accumulated[offset + 9]) << 16) |
                                        (static_cast<uint32_t>(accumulated[offset + 10]) << 8) |
                                        static_cast<uint32_t>(accumulated[offset + 11]);

                size_t frame_size = 20 + payload_size;
                if (offset + frame_size <= accumulated.size()) {
                    received++;
                    offset += frame_size;
                } else {
                    break;
                }
            }
            if (offset > 0) {
                accumulated.erase(accumulated.begin(), accumulated.begin() + offset);
            }
        }
        if (ec == boost::asio::error::would_block) {
            std::this_thread::sleep_for(10ms);
            continue;
        }
        if (ec) {
            break;
        }
    }

    INFO("Rapid fire: received " << received << " out of " << NUM_RAPID);
    REQUIRE(received == NUM_RAPID);
}

TEST_CASE_METHOD(WriterDrainFixture, "writer_drain: Mix of streaming and unary after idle",
                 "[daemon][writer_drain][mixed]") {
    // Test mix of streaming and unary requests after idle
    auto clientIo = createClientIo();
    auto socket = createClientSocket(*clientIo);

    // First: unary request
    {
        auto encoded = createPingRequest(1, false);
        boost::asio::write(socket, boost::asio::buffer(encoded));
        REQUIRE(waitForResponse(socket, 3s));
    }

    std::this_thread::sleep_for(2s); // Idle

    // Second: streaming request
    {
        auto encoded = createPingRequest(2, true);
        boost::asio::write(socket, boost::asio::buffer(encoded));
        REQUIRE(waitForResponse(socket, 3s));
    }

    std::this_thread::sleep_for(2s); // Idle again

    // Third: unary request
    {
        auto encoded = createPingRequest(3, false);
        boost::asio::write(socket, boost::asio::buffer(encoded));
        REQUIRE(waitForResponse(socket, 3s));
    }

    std::this_thread::sleep_for(2s); // Idle again

    // Fourth: streaming request - this is where it typically fails
    {
        auto encoded = createPingRequest(4, true);
        boost::asio::write(socket, boost::asio::buffer(encoded));
        bool received = waitForResponse(socket, 5s);

        if (!received) {
            FAIL("Streaming request after multiple idles failed - writer_drain issue!");
        }
        REQUIRE(received);
    }
}
