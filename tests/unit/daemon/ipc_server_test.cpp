#include <atomic>
#include <thread>
#include <unistd.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <yams/daemon/ipc/async_ipc_server.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/message_serializer.h>

namespace yams::daemon::test {

using namespace std::chrono_literals;
namespace fs = std::filesystem;

class IpcServerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing socket
        socketPath_ = fs::temp_directory_path() / "test_ipc_server.sock";
        std::error_code ec;
        fs::remove(socketPath_, ec);

        // Create server config
        serverConfig_.socket_path = socketPath_;
        serverConfig_.worker_threads = 2;
        serverConfig_.max_connections = 10;
        serverConfig_.connection_timeout = 1s;
    }

    void TearDown() override {
        // Stop server if running
        if (server_) {
            server_->stop();
        }

        // Clean up socket
        std::error_code ec;
        fs::remove(socketPath_, ec);
    }

    // Helper to create a client socket
    int createClientSocket() {
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0)
            return -1;

        struct sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, socketPath_.c_str(), sizeof(addr.sun_path) - 1);

        if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
            close(fd);
            return -1;
        }

        return fd;
    }

    // Helper to send a framed message
    Result<void> sendFramedMessage(int fd, const Message& msg) {
        MessageFramer framer;
        auto framedResult = framer.frame_message(msg);
        if (!framedResult)
            return framedResult.error();

        auto& data = framedResult.value();
        size_t totalSent = 0;
        while (totalSent < data.size()) {
            ssize_t sent = send(fd, data.data() + totalSent, data.size() - totalSent, MSG_NOSIGNAL);
            if (sent < 0) {
                return Error{ErrorCode::NetworkError, "Send failed"};
            }
            totalSent += sent;
        }

        return Result<void>();
    }

    // Helper to receive a framed message
    Result<Message> receiveFramedMessage(int fd) {
        MessageFramer framer;

        // Read header first
        std::vector<uint8_t> headerData(sizeof(MessageFramer::FrameHeader));
        size_t totalReceived = 0;

        while (totalReceived < headerData.size()) {
            ssize_t received =
                recv(fd, headerData.data() + totalReceived, headerData.size() - totalReceived, 0);
            if (received <= 0) {
                return Error{ErrorCode::NetworkError, "Receive failed"};
            }
            totalReceived += received;
        }

        // Parse header to get payload size
        MessageFramer::FrameHeader networkHeader;
        std::memcpy(&networkHeader, headerData.data(), sizeof(MessageFramer::FrameHeader));

        MessageFramer::FrameHeader header;
        header.magic = ntohl(networkHeader.magic);
        header.version = ntohl(networkHeader.version);
        header.payload_size = ntohl(networkHeader.payload_size);
        header.checksum = ntohl(networkHeader.checksum);

        // Read payload
        std::vector<uint8_t> payload(header.payload_size);
        totalReceived = 0;

        while (totalReceived < payload.size()) {
            ssize_t received =
                recv(fd, payload.data() + totalReceived, payload.size() - totalReceived, 0);
            if (received <= 0) {
                return Error{ErrorCode::NetworkError, "Receive payload failed"};
            }
            totalReceived += received;
        }

        // Combine and parse
        std::vector<uint8_t> fullFrame;
        fullFrame.insert(fullFrame.end(), headerData.begin(), headerData.end());
        fullFrame.insert(fullFrame.end(), payload.begin(), payload.end());

        return framer.parse_frame(fullFrame);
    }

    fs::path socketPath_;
    AsyncIpcServer::Config serverConfig_;
    std::unique_ptr<AsyncIpcServer> server_;
};

// Test server creation and startup
TEST_F(IpcServerTest, ServerStartStop) {
    server_ = std::make_unique<AsyncIpcServer>(serverConfig_);

    // Set a simple handler
    server_->set_handler([](const Request& req) -> Response {
        return std::visit(
            [](auto&& r) -> Response {
                using T = std::decay_t<decltype(r)>;
                if constexpr (std::is_same_v<T, PingRequest>) {
                    return PongResponse{std::chrono::steady_clock::now(),
                                        std::chrono::milliseconds{0}};
                }
                return ErrorResponse{ErrorCode::NotImplemented, "Not implemented"};
            },
            req);
    });

    // Start server
    auto result = server_->start();
    ASSERT_TRUE(result) << "Failed to start server: " << result.error().message;

    // Socket should exist
    EXPECT_TRUE(fs::exists(socketPath_));

    // Stop server
    server_->stop();

    // Socket should be removed
    std::this_thread::sleep_for(10ms);
    EXPECT_FALSE(fs::exists(socketPath_));
}

// Test basic request/response
TEST_F(IpcServerTest, BasicRequestResponse) {
    server_ = std::make_unique<AsyncIpcServer>(serverConfig_);

    // Set handler that responds to different requests
    server_->set_handler([](const Request& req) -> Response {
        return std::visit(
            [](auto&& r) -> Response {
                using T = std::decay_t<decltype(r)>;

                if constexpr (std::is_same_v<T, PingRequest>) {
                    return PongResponse{std::chrono::steady_clock::now(),
                                        std::chrono::milliseconds{0}};
                } else if constexpr (std::is_same_v<T, StatusRequest>) {
                    StatusResponse sr;
                    sr.running = true;
                    sr.ready = true;
                    sr.uptimeSeconds = 5;
                    sr.requestsProcessed = 2;
                    sr.activeConnections = 0;
                    sr.memoryUsageMb = 0.0;
                    sr.cpuUsagePercent = 0.0;
                    return sr;
                } else if constexpr (std::is_same_v<T, SearchRequest>) {
                    SearchResponse res;
                    res.totalCount = r.limit;
                    SearchResult result;
                    result.id = "1";
                    result.path = "/test";
                    result.title = "Test";
                    result.snippet = "snippet";
                    result.score = 0.9;
                    res.results.push_back(result);
                    return res;
                }

                return ErrorResponse{ErrorCode::NotImplemented, "Not implemented"};
            },
            req);
    });

    auto startResult = server_->start();
    ASSERT_TRUE(startResult);

    // Give server time to start listening
    std::this_thread::sleep_for(50ms);

    // Connect as client
    int clientFd = createClientSocket();
    ASSERT_GT(clientFd, 0) << "Failed to connect to server";

    // Send ping request
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 123;
    msg.timestamp = std::chrono::steady_clock::now();
    msg.payload = PingRequest{};

    auto sendResult = sendFramedMessage(clientFd, msg);
    ASSERT_TRUE(sendResult) << "Failed to send message";

    // Receive response
    auto recvResult = receiveFramedMessage(clientFd);
    ASSERT_TRUE(recvResult) << "Failed to receive response";

    auto& response = recvResult.value();
    EXPECT_EQ(response.requestId, 123);
    EXPECT_TRUE(std::holds_alternative<Response>(response.payload));

    if (auto* pong = std::get_if<PongResponse>(&std::get<Response>(response.payload))) {
        EXPECT_NE(pong->serverTime.time_since_epoch().count(), 0);
    } else {
        FAIL() << "Expected PongResponse";
    }

    close(clientFd);
}

// Test multiple concurrent connections
TEST_F(IpcServerTest, ConcurrentConnections) {
    server_ = std::make_unique<AsyncIpcServer>(serverConfig_);

    std::atomic<int> requestCount{0};

    server_->set_handler([&requestCount](const Request& req) -> Response {
        requestCount++;
        return std::visit(
            [](auto&& r) -> Response {
                using T = std::decay_t<decltype(r)>;
                if constexpr (std::is_same_v<T, PingRequest>) {
                    return PongResponse{std::chrono::steady_clock::now(),
                                        std::chrono::milliseconds{0}};
                }
                return ErrorResponse{ErrorCode::NotImplemented, "Not implemented"};
            },
            req);
    });

    auto startResult = server_->start();
    ASSERT_TRUE(startResult);

    std::this_thread::sleep_for(50ms);

    // Create multiple client threads
    const int numClients = 5;
    const int requestsPerClient = 10;
    std::atomic<int> successCount{0};

    std::vector<std::thread> clients;
    for (int c = 0; c < numClients; ++c) {
        clients.emplace_back([this, c, &successCount]() {
            int requestsPerClient = 10;
            int fd = createClientSocket();
            if (fd < 0)
                return;

            for (int i = 0; i < requestsPerClient; ++i) {
                Message msg;
                msg.version = PROTOCOL_VERSION;
                msg.requestId = c * 1000 + i;
                msg.payload = PingRequest{};

                if (sendFramedMessage(fd, msg)) {
                    if (receiveFramedMessage(fd)) {
                        successCount++;
                    }
                }
            }

            close(fd);
        });
    }

    for (auto& t : clients) {
        t.join();
    }

    EXPECT_EQ(requestCount, successCount);
    EXPECT_GT(successCount, numClients * requestsPerClient * 0.9);
}

// Test error handling
TEST_F(IpcServerTest, ErrorHandling) {
    server_ = std::make_unique<AsyncIpcServer>(serverConfig_);

    server_->set_handler([](const Request& req) -> Response {
        return std::visit(
            [](auto&& r) -> Response {
                using T = std::decay_t<decltype(r)>;

                // SearchRequest returns error
                if constexpr (std::is_same_v<T, SearchRequest>) {
                    return ErrorResponse{ErrorCode::InvalidArgument, "Invalid search query"};
                }

                return ErrorResponse{ErrorCode::NotImplemented, "Not implemented"};
            },
            req);
    });

    auto startResult = server_->start();
    ASSERT_TRUE(startResult);

    std::this_thread::sleep_for(50ms);

    int clientFd = createClientSocket();
    ASSERT_GT(clientFd, 0);

    // Send search request
    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 456;
    msg.payload = SearchRequest{"test", 10};

    auto sendResult = sendFramedMessage(clientFd, msg);
    ASSERT_TRUE(sendResult);

    auto recvResult = receiveFramedMessage(clientFd);
    ASSERT_TRUE(recvResult);

    auto& response = recvResult.value();
    EXPECT_TRUE(std::holds_alternative<Response>(response.payload));

    if (auto* err = std::get_if<ErrorResponse>(&std::get<Response>(response.payload))) {
        EXPECT_EQ(err->code, ErrorCode::InvalidArgument);
        EXPECT_EQ(err->message, "Invalid search query");
    } else {
        FAIL() << "Expected ErrorResponse";
    }

    close(clientFd);
}

// Test connection limit
TEST_F(IpcServerTest, ConnectionLimit) {
    serverConfig_.max_connections = 2; // Very low limit
    server_ = std::make_unique<AsyncIpcServer>(serverConfig_);

    server_->set_handler([](const Request&) -> Response {
        std::this_thread::sleep_for(100ms); // Slow handler
        return PongResponse{std::chrono::steady_clock::now(), std::chrono::milliseconds{0}};
    });

    auto startResult = server_->start();
    ASSERT_TRUE(startResult);

    std::this_thread::sleep_for(50ms);

    // Open max connections
    std::vector<int> fds;
    for (int i = 0; i < 2; ++i) {
        int fd = createClientSocket();
        if (fd > 0) {
            fds.push_back(fd);
        }
    }

    EXPECT_EQ(fds.size(), 2);

    // Try one more - might fail or queue
    int extraFd = createClientSocket();

    // Clean up
    for (int fd : fds) {
        close(fd);
    }
    if (extraFd > 0) {
        close(extraFd);
    }
}

// Test server shutdown while handling requests
TEST_F(IpcServerTest, ShutdownWhileBusy) {
    server_ = std::make_unique<AsyncIpcServer>(serverConfig_);

    std::atomic<bool> inHandler{false};
    std::atomic<bool> shouldExit{false};

    server_->set_handler([&inHandler, &shouldExit](const Request&) -> Response {
        inHandler = true;
        while (!shouldExit) {
            std::this_thread::sleep_for(10ms);
        }
        return PongResponse{std::chrono::steady_clock::now(), std::chrono::milliseconds{0}};
    });

    auto startResult = server_->start();
    ASSERT_TRUE(startResult);

    std::this_thread::sleep_for(50ms);

    // Start a request in background
    std::thread clientThread([this, &inHandler]() {
        int fd = createClientSocket();
        if (fd > 0) {
            Message msg;
            msg.version = PROTOCOL_VERSION;
            msg.requestId = 789;
            msg.payload = PingRequest{};
            sendFramedMessage(fd, msg);

            // Wait for handler to start
            while (!inHandler) {
                std::this_thread::sleep_for(1ms);
            }

            close(fd);
        }
    });

    // Wait for request to be in progress
    while (!inHandler) {
        std::this_thread::sleep_for(1ms);
    }

    // Stop server while request is being handled
    server_->stop();

    // Allow handler to complete
    shouldExit = true;

    clientThread.join();

    // Server should have stopped cleanly
    EXPECT_FALSE(fs::exists(socketPath_));
}

// Test invalid protocol handling
TEST_F(IpcServerTest, InvalidProtocol) {
    server_ = std::make_unique<AsyncIpcServer>(serverConfig_);

    server_->set_handler([](const Request&) -> Response {
        return PongResponse{std::chrono::steady_clock::now(), std::chrono::milliseconds{0}};
    });

    auto startResult = server_->start();
    ASSERT_TRUE(startResult);

    std::this_thread::sleep_for(50ms);

    int clientFd = createClientSocket();
    ASSERT_GT(clientFd, 0);

    // Send garbage data
    const char* garbage = "This is not a valid protocol message!";
    send(clientFd, garbage, strlen(garbage), MSG_NOSIGNAL);

    // Server should handle gracefully (close connection)
    char buffer[100];
    [[maybe_unused]] ssize_t received = recv(clientFd, buffer, sizeof(buffer), 0);

    // Connection might be closed or we get an error response
    // Either is acceptable

    close(clientFd);
}

} // namespace yams::daemon::test