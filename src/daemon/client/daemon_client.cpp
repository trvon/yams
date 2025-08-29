#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/message_serializer.h>

#include <spdlog/spdlog.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <thread>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#else
#include <afunix.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif
#endif

namespace yams::daemon {

namespace {
inline std::string sanitize_for_terminal(std::string_view in) {
    std::string out;
    out.reserve(in.size());
    for (unsigned char c : in) {
        if (c >= 0x20 && c <= 0x7E) {
            out.push_back(static_cast<char>(c));
        } else if (c == '\n' || c == '\r' || c == '\t') {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('?');
        }
    }
    return out;
}

#ifndef _WIN32
bool canWriteToDirectory(const std::filesystem::path& dir) {
    namespace fs = std::filesystem;
    if (!fs::exists(dir))
        return false;
    auto testFile = dir / (".yams-test-" + std::to_string(getpid()));
    std::ofstream test(testFile);
    if (test.good()) {
        test.close();
        std::error_code ec;
        fs::remove(testFile, ec);
        return true;
    }
    return false;
}

std::filesystem::path getXDGRuntimeDir() {
    const char* xdgRuntime = std::getenv("XDG_RUNTIME_DIR");
    return xdgRuntime ? std::filesystem::path(xdgRuntime) : std::filesystem::path();
}
#endif

} // namespace

// Implementation class
class DaemonClient::Impl {
public:
    explicit Impl(const ClientConfig& config)
        : config_(config), headerTimeout_(config.headerTimeout), bodyTimeout_(config.bodyTimeout) {}

    ~Impl() {
        if (socketFd_ >= 0) {
#ifdef _WIN32
            closesocket(socketFd_);
#else
            close(socketFd_);
#endif
        }
    }

    Result<void> connect() {
        if (socketFd_ >= 0) {
            return Result<void>(); // Already connected
        }

#ifdef _WIN32
        // Initialize Winsock once
        static bool wsInit = false;
        if (!wsInit) {
            WSADATA wsa;
            if (WSAStartup(MAKEWORD(2, 2), &wsa) == 0) {
                wsInit = true;
            }
        }
#endif
        // Create socket
        socketFd_ = socket(AF_UNIX, SOCK_STREAM, 0);
        if (socketFd_ < 0) {
            return Error{ErrorCode::NetworkError,
                         "Failed to create socket: " + std::string(strerror(errno))};
        }
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
        // Prevent SIGPIPE on send; errors will be reported via errno (EPIPE)
        {
            int on = 1;
            ::setsockopt(socketFd_, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
        }
#endif

        // Connect to server
        struct sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, config_.socketPath.c_str(), sizeof(addr.sun_path) - 1);

        // Connect to server with short retry window to avoid races with an already-running daemon
        int rc = -1;
        int lastErr = 0;
        for (int attempt = 0; attempt < 10; ++attempt) {
            rc = ::connect(socketFd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
            if (rc == 0) {
                break;
            }
            lastErr = errno;
            if (lastErr == ENOENT || lastErr == ECONNREFUSED) {
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
                continue;
            }
            // Other errors: break and report
            break;
        }

        if (rc < 0) {
#ifdef _WIN32
            closesocket(socketFd_);
#else
            close(socketFd_);
#endif
            socketFd_ = -1;
            auto errorMsg = std::string(strerror(lastErr));
            if (lastErr == ENOENT || lastErr == ECONNREFUSED) {
                return Error{ErrorCode::NetworkError,
                             "Daemon not running. Start it with: yams daemon start"};
            }
            return Error{ErrorCode::NetworkError, "Failed to connect to daemon: " + errorMsg};
        }

        // Ensure client socket is non-blocking so our manual timeouts work.
        // Without this, recv()/send() may block indefinitely and ignore our deadlines.
#ifndef _WIN32
        int flags = fcntl(socketFd_, F_GETFL, 0);
        if (flags >= 0) {
            (void)fcntl(socketFd_, F_SETFL, flags | O_NONBLOCK);
        }
#else
        u_long mode = 1;
        ioctlsocket(socketFd_, FIONBIO, &mode);
#endif

        spdlog::debug("Connected to daemon at {}",
                      sanitize_for_terminal(config_.socketPath.string()));
        return Result<void>();
    }

    void disconnect() {
        if (socketFd_ >= 0) {
#ifdef _WIN32
            closesocket(socketFd_);
#else
            close(socketFd_);
#endif
            socketFd_ = -1;
        }
    }

    bool isConnected() const { return socketFd_ >= 0; }

    ClientConfig config_;
    int socketFd_ = -1;
    CircuitBreaker breaker_;
    std::chrono::milliseconds headerTimeout_{30000}; // 30s default
    std::chrono::milliseconds bodyTimeout_{60000};   // 60s default
};

// DaemonClient implementation
DaemonClient::DaemonClient(const ClientConfig& config) : pImpl(std::make_unique<Impl>(config)) {
    // Resolve socket path if not specified
    if (pImpl->config_.socketPath.empty()) {
        pImpl->config_.socketPath = resolveSocketPath();
    }
}

DaemonClient::~DaemonClient() = default;

DaemonClient::DaemonClient(DaemonClient&&) noexcept = default;
DaemonClient& DaemonClient::operator=(DaemonClient&&) noexcept = default;

void DaemonClient::setHeaderTimeout(std::chrono::milliseconds timeout) {
    if (pImpl) {
        pImpl->headerTimeout_ = timeout;
    }
}

void DaemonClient::setBodyTimeout(std::chrono::milliseconds timeout) {
    if (pImpl) {
        pImpl->bodyTimeout_ = timeout;
    }
}

Result<void> DaemonClient::connect() {
    // Auto-start daemon if configured and not running
    if (pImpl->config_.autoStart) {
        // Quick retries before deciding it's not running, to avoid respawning when an existing
        // daemon is just (re)creating/binding its socket.
        const int quickChecks = 5;
        const auto quickDelay = std::chrono::milliseconds(50);
        bool alreadyRunning = false;
        for (int i = 0; i < quickChecks; ++i) {
            if (isDaemonRunning(pImpl->config_.socketPath)) {
                alreadyRunning = true;
                break;
            }
            std::this_thread::sleep_for(quickDelay);
        }

        if (!alreadyRunning && !isDaemonRunning(pImpl->config_.socketPath)) {
            spdlog::info("Daemon not running, attempting to auto-start...");
            if (auto result = startDaemon(pImpl->config_); !result) {
                spdlog::warn("Failed to auto-start daemon: {}",
                             sanitize_for_terminal(result.error().message));
                spdlog::info("Please manually start the daemon with: yams daemon start");
            } else {
                // Wait for daemon to start with exponential backoff
                const int maxRetries = 10;
                const auto baseDelay = std::chrono::milliseconds(100);

                for (int i = 0; i < maxRetries; ++i) {
                    auto delay = baseDelay * (1 << std::min(i, 5)); // Cap at 3.2 seconds
                    std::this_thread::sleep_for(delay);

                    // Check if daemon is now running and ready
                    if (isDaemonRunning(pImpl->config_.socketPath)) {
                        spdlog::debug("Daemon started successfully after {} retries", i + 1);
                        // Give it a bit more time to fully initialize
                        std::this_thread::sleep_for(std::chrono::milliseconds(200));
                        break;
                    }

                    if (i == maxRetries - 1) {
                        spdlog::warn("Daemon failed to start after {} retries", maxRetries);
                    }
                }
            }
        }
    }

    return pImpl->connect();
}

void DaemonClient::disconnect() {
    pImpl->disconnect();
}

bool DaemonClient::isConnected() const {
    return pImpl->isConnected();
}

Result<SearchResponse> DaemonClient::search(const SearchRequest& req) {
    // Use streaming search if enabled
    if (pImpl->config_.enableChunkedResponses) {
        return streamingSearch(req);
    }

    // Fall back to traditional request/response
    auto response = sendRequest(req);
    if (!response) {
        return Error{response.error().code, response.error().message};
    }

    if (auto* res = std::get_if<SearchResponse>(&response.value())) {
        return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        return Error{err->code, err->message};
    }

    return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Result<SearchResponse> DaemonClient::streamingSearch(const SearchRequest& req) {
    auto handler = std::make_shared<StreamingSearchHandler>(req.pathsOnly, req.limit);

    auto result = sendRequestStreaming(req, handler);
    if (!result) {
        return result.error();
    }

    return handler->getResults();
}

Result<GetResponse> DaemonClient::get(const GetRequest& req) {
    auto response = sendRequest(req);
    if (!response) {
        return response.error();
    }

    if (auto* res = std::get_if<GetResponse>(&response.value())) {
        return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        return Error{err->code, err->message};
    }

    return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Result<ListResponse> DaemonClient::list(const ListRequest& req) {
    // Use streaming list if enabled
    if (pImpl->config_.enableChunkedResponses) {
        return streamingList(req);
    }

    // Fall back to traditional request/response
    auto response = sendRequest(req);
    if (!response) {
        return Error{response.error().code, response.error().message};
    }

    if (auto* res = std::get_if<ListResponse>(&response.value())) {
        return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        return Error{err->code, err->message};
    }

    return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Result<GrepResponse> DaemonClient::grep(const GrepRequest& req) {
    // Prefer streaming if enabled
    if (pImpl->config_.enableChunkedResponses) {
        return streamingGrep(req);
    }

    // Fallback to traditional request/response
    auto response = sendRequest(req);
    if (!response) {
        return Error{response.error().code, response.error().message};
    }

    if (auto* res = std::get_if<GrepResponse>(&response.value())) {
        return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        return Error{err->code, err->message};
    }

    return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Result<StatusResponse> DaemonClient::status() {
    StatusRequest req;
    req.detailed = true;

    auto response = sendRequest(req);
    if (!response) {
        return Error{response.error().code, response.error().message};
    }

    if (auto* res = std::get_if<StatusResponse>(&response.value())) {
        return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        return Error{err->code, err->message};
    }

    return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Result<Response> DaemonClient::executeRequest(const Request& req) {
    return sendRequest(req);
}

Result<void> DaemonClient::shutdown(bool graceful) {
    ShutdownRequest req;
    req.graceful = graceful;

    auto response = sendRequest(req);
    if (!response) {
        return Error{response.error().code, response.error().message};
    }

    if (std::holds_alternative<SuccessResponse>(response.value())) {
        return Result<void>();
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        return Error{err->code, err->message};
    }

    return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Result<void> DaemonClient::ping() {
    PingRequest req;

    auto response = sendRequest(req);
    if (!response) {
        return Error{response.error().code, response.error().message};
    }

    if (std::holds_alternative<PongResponse>(response.value())) {
        return Result<void>();
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        return Error{err->code, err->message};
    }

    return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Result<Response> DaemonClient::sendRequest(const Request& req) {
    if (!isConnected()) {
        if (auto result = connect(); !result) {
            return result.error();
        }
    }

    // Check circuit breaker
    if (!pImpl->breaker_.shouldAllow()) {
        return Error{ErrorCode::NetworkError, "Circuit breaker is open"};
    }

    // Read environment variables for timeout configuration
    const char* headerTimeoutEnv = std::getenv("YAMS_HEADER_TIMEOUT");
    const char* bodyTimeoutEnv = std::getenv("YAMS_BODY_TIMEOUT");

    if (headerTimeoutEnv) {
        try {
            pImpl->headerTimeout_ = std::chrono::milliseconds(std::stoi(headerTimeoutEnv));
        } catch (...) {
            // Ignore invalid values
        }
    }

    if (bodyTimeoutEnv) {
        try {
            pImpl->bodyTimeout_ = std::chrono::milliseconds(std::stoi(bodyTimeoutEnv));
        } catch (...) {
            // Ignore invalid values
        }
    }

    try {
        // Create message
        Message msg;
        msg.version = PROTOCOL_VERSION;
        msg.requestId =
            static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
        msg.timestamp = std::chrono::steady_clock::now();
        msg.payload = req;
        msg.clientVersion = "yams-client-0.3.4";

        // Use MessageFramer to frame the message with CRC32 checksum
        MessageFramer framer;
        auto framedResult = framer.frame_message(msg);
        if (!framedResult) {
            pImpl->breaker_.recordFailure();
            return framedResult.error();
        }

        auto& framedData = framedResult.value();
        spdlog::debug("Sending {} bytes to daemon (framed with CRC32)", framedData.size());

        // Send framed message data with timeout
        size_t totalSent = 0;
        const auto deadline = std::chrono::steady_clock::now() + pImpl->config_.requestTimeout;

        // Allow a single transparent reconnect+retry on EPIPE/ECONNRESET to handle servers that
        // close connections after a response while the pool reuses sockets.
        bool retriedAfterPipe = false;
        while (totalSent < framedData.size()) {
            if (std::chrono::steady_clock::now() > deadline) {
                pImpl->breaker_.recordFailure();
                return Error{ErrorCode::NetworkError, "Send timeout"};
            }

            ssize_t sent = send(pImpl->socketFd_, framedData.data() + totalSent,
                                framedData.size() - totalSent, MSG_NOSIGNAL);

            if (sent < 0) {
                int err = errno;
                if (err == EAGAIN || err == EWOULDBLOCK) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }
                // Some platforms may surface errno==0 here when SIGPIPE is suppressed;
                // map to a meaningful error (prefer EPIPE, fall back to ECONNRESET).
                if (err == 0) {
#ifdef EPIPE
                    err = EPIPE;
#else
                    err = ECONNRESET;
#endif
                }
                // If the peer closed (EPIPE/ECONNRESET), attempt a single reconnect+retry.
                if ((err == EPIPE || err == ECONNRESET) && !retriedAfterPipe) {
                    spdlog::warn(
                        "[DaemonClient] Send failed with {}. Reconnecting and retrying once...",
                        strerror(err));
                    // Reset connection and retry from the beginning.
                    disconnect();
                    auto rc = connect();
                    if (!rc) {
                        pImpl->breaker_.recordFailure();
                        return Error{ErrorCode::NetworkError,
                                     std::string("Reconnect failed after send error: ") +
                                         rc.error().message};
                    }
                    retriedAfterPipe = true;
                    totalSent = 0;
                    continue;
                }
                pImpl->breaker_.recordFailure();
                return Error{ErrorCode::NetworkError, "Send failed: " + std::string(strerror(err))};
            }

            totalSent += static_cast<size_t>(sent);
        }

        // Read framed response header first with the header timeout
        auto headerDeadline = std::chrono::steady_clock::now() + pImpl->headerTimeout_;
        size_t totalReceived = 0;
        const size_t headerSize = sizeof(MessageFramer::FrameHeader);
        std::vector<uint8_t> headerData(headerSize);

        while (totalReceived < headerSize) {
            if (std::chrono::steady_clock::now() > headerDeadline) {
                pImpl->breaker_.recordFailure();
                return Error{ErrorCode::NetworkError, "Receive timeout (header)"};
            }

            ssize_t received = recv(pImpl->socketFd_, headerData.data() + totalReceived,
                                    headerSize - totalReceived, 0);

            if (received < 0) {
                int err = errno;
                if (err == EAGAIN || err == EWOULDBLOCK) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }
                // Treat connection reset/pipe as remote close
                if (err == ECONNRESET || err == EPIPE) {
                    pImpl->breaker_.recordFailure();
                    return Error{ErrorCode::NetworkError, "Connection closed by daemon"};
                }
                pImpl->breaker_.recordFailure();
                return Error{ErrorCode::NetworkError,
                             "Receive failed (header): " + std::string(strerror(err))};
            } else if (received == 0) {
                pImpl->breaker_.recordFailure();
                return Error{ErrorCode::NetworkError, "Connection closed by daemon"};
            }

            totalReceived += static_cast<size_t>(received);
        }

        // Parse frame header manually since we need just the header info
        MessageFramer::FrameHeader networkHeader;
        std::memcpy(&networkHeader, headerData.data(), sizeof(MessageFramer::FrameHeader));

        // Convert from network byte order
        MessageFramer::FrameHeader parsedHeader;
        parsedHeader.magic = ntohl(networkHeader.magic);
        parsedHeader.version = ntohl(networkHeader.version);
        parsedHeader.payload_size = ntohl(networkHeader.payload_size);
        parsedHeader.checksum = ntohl(networkHeader.checksum);

        // Validate header
        if (parsedHeader.magic != MessageFramer::FRAME_MAGIC) {
            pImpl->breaker_.recordFailure();
            return Error{ErrorCode::InvalidData, "Invalid frame magic"};
        }

        if (parsedHeader.version != MessageFramer::FRAME_VERSION) {
            pImpl->breaker_.recordFailure();
            return Error{ErrorCode::InvalidData, "Invalid frame version"};
        }

        if (parsedHeader.payload_size > MAX_MESSAGE_SIZE) {
            pImpl->breaker_.recordFailure();
            return Error{ErrorCode::InvalidData,
                         "Response size too large: " + std::to_string(parsedHeader.payload_size)};
        }

        // Read the response payload with the body timeout
        auto bodyDeadline = std::chrono::steady_clock::now() + pImpl->bodyTimeout_;
        std::vector<uint8_t> responseData(parsedHeader.payload_size);
        totalReceived = 0;

        while (totalReceived < parsedHeader.payload_size) {
            if (std::chrono::steady_clock::now() > bodyDeadline) {
                pImpl->breaker_.recordFailure();
                return Error{ErrorCode::NetworkError, "Receive timeout (data)"};
            }

            ssize_t received = recv(pImpl->socketFd_, responseData.data() + totalReceived,
                                    parsedHeader.payload_size - totalReceived, 0);

            if (received < 0) {
                int err = errno;
                if (err == EAGAIN || err == EWOULDBLOCK) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }
                // Treat connection reset/pipe as remote close
                if (err == ECONNRESET || err == EPIPE) {
                    pImpl->breaker_.recordFailure();
                    return Error{ErrorCode::NetworkError, "Connection closed during data transfer"};
                }
                pImpl->breaker_.recordFailure();
                return Error{ErrorCode::NetworkError,
                             "Receive failed (data): " + std::string(strerror(err))};
            } else if (received == 0) {
                pImpl->breaker_.recordFailure();
                return Error{ErrorCode::NetworkError, "Connection closed during data transfer"};
            }

            totalReceived += static_cast<size_t>(received);
        }

        spdlog::debug("Received {} bytes from daemon", responseData.size());

        // Build complete frame and parse it
        std::vector<uint8_t> completeFrame;
        completeFrame.reserve(headerSize + responseData.size());
        completeFrame.insert(completeFrame.end(), headerData.begin(), headerData.end());
        completeFrame.insert(completeFrame.end(), responseData.begin(), responseData.end());

        // Parse and verify the complete frame (includes CRC32 check)
        auto responseMsg = framer.parse_frame(completeFrame);
        if (!responseMsg) {
            pImpl->breaker_.recordFailure();
            return responseMsg.error();
        }

        // Verify response ID matches request
        if (responseMsg.value().requestId != msg.requestId) {
            pImpl->breaker_.recordFailure();
            return Error{ErrorCode::InvalidData, "Response ID mismatch"};
        }

        // Extract response from message
        if (!std::holds_alternative<Response>(responseMsg.value().payload)) {
            pImpl->breaker_.recordFailure();
            return Error{ErrorCode::InvalidData, "Expected response, got request"};
        }

        pImpl->breaker_.recordSuccess();
        auto out = std::get<Response>(responseMsg.value().payload);
        if (pImpl->config_.singleUseConnections) {
            disconnect();
        }
        return out;
    } catch (const std::exception& e) {
        pImpl->breaker_.recordFailure();
        return Error{ErrorCode::InternalError, "Error sending request: " + std::string(e.what())};
    }
}

Result<MessageFramer::FrameHeader> DaemonClient::readFrameHeader(int socketFd) {
    // Read framed response header first with the header timeout
    auto headerDeadline = std::chrono::steady_clock::now() + pImpl->headerTimeout_;
    size_t totalReceived = 0;
    const size_t headerSize = sizeof(MessageFramer::FrameHeader);
    std::vector<uint8_t> headerData(headerSize);

    while (totalReceived < headerSize) {
        if (std::chrono::steady_clock::now() > headerDeadline) {
            pImpl->breaker_.recordFailure();
            return Error{ErrorCode::NetworkError, "Receive timeout (header)"};
        }

        ssize_t received =
            recv(socketFd, headerData.data() + totalReceived, headerSize - totalReceived, 0);

        if (received < 0) {
            int err = errno;
            if (err == EAGAIN || err == EWOULDBLOCK) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            // Treat connection reset/pipe as remote close
            if (err == ECONNRESET || err == EPIPE) {
                pImpl->breaker_.recordFailure();
                return Error{ErrorCode::NetworkError, "Connection closed by daemon"};
            }
            pImpl->breaker_.recordFailure();
            return Error{ErrorCode::NetworkError,
                         "Receive failed (header): " + std::string(strerror(err))};
        } else if (received == 0) {
            pImpl->breaker_.recordFailure();
            return Error{ErrorCode::NetworkError, "Connection closed by daemon"};
        }

        totalReceived += static_cast<size_t>(received);
    }

    // Parse frame header
    MessageFramer::FrameHeader header;
    std::memcpy(&header, headerData.data(), sizeof(header));
    header.from_network();

    // Validate header
    if (!header.is_valid()) {
        pImpl->breaker_.recordFailure();
        return Error{ErrorCode::InvalidData, "Invalid frame magic"};
    }

    if (header.payload_size > MAX_MESSAGE_SIZE) {
        pImpl->breaker_.recordFailure();
        return Error{ErrorCode::InvalidData,
                     "Response size too large: " + std::to_string(header.payload_size)};
    }

    return header;
}

Result<std::vector<uint8_t>>
DaemonClient::readFramedData(int socketFd, std::chrono::milliseconds timeout, size_t size) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    std::vector<uint8_t> data(size);
    size_t totalReceived = 0;

    while (totalReceived < size) {
        if (std::chrono::steady_clock::now() > deadline) {
            pImpl->breaker_.recordFailure();
            return Error{ErrorCode::NetworkError, "Receive timeout (data)"};
        }

        ssize_t received = recv(socketFd, data.data() + totalReceived, size - totalReceived, 0);

        if (received < 0) {
            int err = errno;
            if (err == EAGAIN || err == EWOULDBLOCK) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            // Treat connection reset/pipe as remote close
            if (err == ECONNRESET || err == EPIPE) {
                pImpl->breaker_.recordFailure();
                return Error{ErrorCode::NetworkError, "Connection closed during data transfer"};
            }
            pImpl->breaker_.recordFailure();
            return Error{ErrorCode::NetworkError,
                         "Receive failed (data): " + std::string(strerror(err))};
        } else if (received == 0) {
            pImpl->breaker_.recordFailure();
            return Error{ErrorCode::NetworkError, "Connection closed during data transfer"};
        }

        totalReceived += static_cast<size_t>(received);
    }

    return data;
}

Result<std::vector<uint8_t>> DaemonClient::readFullFrame(int socketFd,
                                                         const MessageFramer::FrameHeader& header) {
    // Read the header data first
    const size_t headerSize = sizeof(MessageFramer::FrameHeader);
    std::vector<uint8_t> headerData(headerSize);

    // Convert header back to network byte order
    MessageFramer::FrameHeader networkHeader = header;
    networkHeader.to_network();

    // Copy header to buffer
    std::memcpy(headerData.data(), &networkHeader, headerSize);

    // Read the payload with body timeout
    auto payloadResult = readFramedData(socketFd, pImpl->bodyTimeout_, header.payload_size);
    if (!payloadResult) {
        return payloadResult.error();
    }

    // Combine header and payload
    std::vector<uint8_t> completeFrame;
    completeFrame.reserve(headerSize + header.payload_size);
    completeFrame.insert(completeFrame.end(), headerData.begin(), headerData.end());
    completeFrame.insert(completeFrame.end(), payloadResult.value().begin(),
                         payloadResult.value().end());

    return completeFrame;
}

// StreamingListHandler implementation
void DaemonClient::StreamingListHandler::onHeaderReceived(const Response& headerResponse) {
    // Parse header information from response
    if (auto* listRes = std::get_if<ListResponse>(&headerResponse)) {
        // Store total count from header
        totalCount_ = listRes->totalCount;

        // Pre-allocate results vector if we know the size
        if (totalCount_ > 0 && limit_ > 0) {
            items_.reserve(static_cast<size_t>(
                std::min<uint64_t>(totalCount_, static_cast<uint64_t>(limit_))));
        } else if (totalCount_ > 0) {
            items_.reserve(totalCount_);
        } else {
            items_.reserve(100); // Default reservation
        }

        // Add any items that might be in the header
        for (const auto& item : listRes->items) {
            if (limit_ > 0 && count_ >= limit_) {
                break;
            }

            items_.push_back(item);
            count_++;

            // Print result immediately if progressive output is enabled
            if (pathsOnly_) {
                std::cout << item.path << std::endl;
            }
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&headerResponse)) {
        // Store error
        error_ = Error{errRes->code, errRes->message};
    }
}

// StreamingSearchHandler implementation
void DaemonClient::StreamingSearchHandler::onHeaderReceived(const Response& headerResponse) {
    // Parse header information from response
    if (auto* searchRes = std::get_if<SearchResponse>(&headerResponse)) {
        // Store total count and elapsed time from header
        totalCount_ = searchRes->totalCount;
        elapsed_ = searchRes->elapsed;

        // Pre-allocate results vector if we know the size
        if (totalCount_ > 0 && limit_ > 0) {
            results_.reserve(static_cast<size_t>(
                std::min<uint64_t>(totalCount_, static_cast<uint64_t>(limit_))));
        } else if (totalCount_ > 0) {
            results_.reserve(totalCount_);
        } else {
            results_.reserve(100); // Default reservation
        }

        // Add any results that might be in the header
        for (const auto& result : searchRes->results) {
            if (limit_ > 0 && count_ >= limit_) {
                break;
            }

            results_.push_back(result);
            count_++;

            // Print result immediately if progressive output is enabled
            if (pathsOnly_) {
                std::cout << result.path << std::endl;
            }
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&headerResponse)) {
        // Store error
        error_ = Error{errRes->code, errRes->message};
    }
}

bool DaemonClient::StreamingListHandler::onChunkReceived(const Response& chunkResponse,
                                                         bool isLastChunk) {
    (void)isLastChunk;
    // Process chunk data
    if (auto* listRes = std::get_if<ListResponse>(&chunkResponse)) {
        // Update totals if they changed
        if (listRes->totalCount > 0) {
            totalCount_ = listRes->totalCount;
        }

        // Process items in this chunk
        for (const auto& item : listRes->items) {
            if (limit_ > 0 && count_ >= limit_) {
                return false; // Stop processing if we reached the limit
            }

            items_.push_back(item);
            count_++;

            // Print result immediately if progressive output is enabled
            if (pathsOnly_) {
                std::cout << item.path << std::endl;
            }
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&chunkResponse)) {
        // Store error
        error_ = Error{errRes->code, errRes->message};
        return false; // Stop processing on error
    }

    return true; // Continue processing next chunks
}

bool DaemonClient::StreamingSearchHandler::onChunkReceived(const Response& chunkResponse,
                                                           bool isLastChunk) {
    (void)isLastChunk;
    // Process chunk data
    if (auto* searchRes = std::get_if<SearchResponse>(&chunkResponse)) {
        // Update totals if they changed
        if (searchRes->totalCount > 0) {
            totalCount_ = searchRes->totalCount;
        }

        // Process results in this chunk
        for (const auto& result : searchRes->results) {
            if (limit_ > 0 && count_ >= limit_) {
                return false; // Stop processing if we reached the limit
            }

            results_.push_back(result);
            count_++;

            // Print result immediately if progressive output is enabled
            if (pathsOnly_) {
                std::cout << result.path << std::endl;
            }
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&chunkResponse)) {
        // Store error
        error_ = Error{errRes->code, errRes->message};
        return false; // Stop processing on error
    }

    return true; // Continue processing next chunks
}

void DaemonClient::StreamingListHandler::onError(const Error& error) {
    error_ = error;

    // Log error for immediate feedback
    spdlog::error("List error: {}", error.message);
}

void DaemonClient::StreamingListHandler::onComplete() {
    // Final processing when all chunks have been received
    if (!error_ && !pathsOnly_) {
        spdlog::debug("List complete: found {} items (of {} total)", count_, totalCount_);
    }
}

Result<ListResponse> DaemonClient::StreamingListHandler::getResults() const {
    if (error_) {
        return *error_;
    }

    // Construct complete response
    ListResponse response;
    response.items = items_;
    response.totalCount = totalCount_;

    return response;
}

void DaemonClient::StreamingSearchHandler::onError(const Error& error) {
    error_ = error;

    // Log error for immediate feedback
    spdlog::error("Search error: {}", error.message);
}

void DaemonClient::StreamingSearchHandler::onComplete() {
    // Final processing when all chunks have been received
    if (!error_ && !pathsOnly_) {
        spdlog::debug("Search complete: found {} results (of {} total) in {}ms", count_,
                      totalCount_, elapsed_.count());
    }
}

Result<SearchResponse> DaemonClient::StreamingSearchHandler::getResults() const {
    if (error_) {
        return *error_;
    }

    // Construct complete response
    SearchResponse response;
    response.results = results_;
    response.totalCount = totalCount_;
    response.elapsed = elapsed_;

    return response;
}

// Static helper to set timeout environment variables
void DaemonClient::setTimeoutEnvVars(std::chrono::milliseconds headerTimeout,
                                     std::chrono::milliseconds bodyTimeout) {
    // Set environment variables
    setenv("YAMS_HEADER_TIMEOUT", std::to_string(headerTimeout.count()).c_str(), 1);
    setenv("YAMS_BODY_TIMEOUT", std::to_string(bodyTimeout.count()).c_str(), 1);
}

// Streaming list helper method
Result<ListResponse> DaemonClient::streamingList(const ListRequest& req) {
    auto handler = std::make_shared<StreamingListHandler>(req.pathsOnly, req.limit);

    auto result = sendRequestStreaming(req, handler);
    if (!result) {
        return result.error();
    }

    return handler->getResults();
}

// Streaming Grep handler methods
void DaemonClient::StreamingGrepHandler::onHeaderReceived(const Response& headerResponse) {
    if (auto* grepRes = std::get_if<GrepResponse>(&headerResponse)) {
        totalMatches_ = grepRes->totalMatches;
        filesSearched_ = grepRes->filesSearched;

        // Process any matches included in the header
        for (const auto& m : grepRes->matches) {
            // Enforce per-file cap if set
            if (perFileMax_ > 0) {
                auto& cnt = perFileCount_[m.file];
                if (cnt >= perFileMax_) {
                    continue;
                }
                cnt++;
            }
            matches_.push_back(m);
            if (pathsOnly_) {
                std::cout << m.file << std::endl;
            }
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&headerResponse)) {
        error_ = Error{errRes->code, errRes->message};
    }
}

bool DaemonClient::StreamingGrepHandler::onChunkReceived(const Response& chunkResponse,
                                                         bool /*isLastChunk*/) {
    if (auto* grepRes = std::get_if<GrepResponse>(&chunkResponse)) {
        // Update totals if present
        if (grepRes->totalMatches > 0) {
            totalMatches_ = grepRes->totalMatches;
        }
        if (grepRes->filesSearched > 0) {
            filesSearched_ = grepRes->filesSearched;
        }

        for (const auto& m : grepRes->matches) {
            if (perFileMax_ > 0) {
                auto& cnt = perFileCount_[m.file];
                if (cnt >= perFileMax_) {
                    continue;
                }
                cnt++;
            }
            matches_.push_back(m);
            if (pathsOnly_) {
                std::cout << m.file << std::endl;
            }
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&chunkResponse)) {
        error_ = Error{errRes->code, errRes->message};
        return false;
    }
    return true;
}

void DaemonClient::StreamingGrepHandler::onError(const Error& error) {
    error_ = error;
    spdlog::error("Grep error: {}", error.message);
}

void DaemonClient::StreamingGrepHandler::onComplete() {
    if (!error_ && !pathsOnly_) {
        spdlog::debug("Grep complete: {} matches across {} files", totalMatches_, filesSearched_);
    }
}

Result<GrepResponse> DaemonClient::StreamingGrepHandler::getResults() const {
    if (error_) {
        return *error_;
    }
    GrepResponse r;
    r.matches = matches_;
    r.totalMatches = totalMatches_;
    r.filesSearched = filesSearched_;
    return r;
}

// Streaming grep helper method
Result<GrepResponse> DaemonClient::streamingGrep(const GrepRequest& req) {
    // If pathsOnly or filesOnly/countOnly, we can progressively print in handler
    size_t perFileCap = 0;
    if (req.maxMatches > 0) {
        perFileCap = req.maxMatches;
    }
    auto handler =
        std::make_shared<StreamingGrepHandler>(req.pathsOnly || req.filesOnly, perFileCap);

    auto result = sendRequestStreaming(req, handler);
    if (!result) {
        return result.error();
    }

    return handler->getResults();
}

Result<void> DaemonClient::sendRequestStreaming(const Request& req,
                                                std::shared_ptr<ChunkedResponseHandler> handler) {
    if (!isConnected()) {
        if (auto result = connect(); !result) {
            return result.error();
        }
    }

    // Check circuit breaker
    if (!pImpl->breaker_.shouldAllow()) {
        return Error{ErrorCode::NetworkError, "Circuit breaker is open"};
    }

    try {
        // Create message
        Message msg;
        msg.version = PROTOCOL_VERSION;
        msg.requestId =
            static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
        msg.timestamp = std::chrono::steady_clock::now();
        msg.payload = req;
        msg.clientVersion = "yams-client-0.3.4";

        // Use MessageFramer to frame the message with CRC32 checksum
        MessageFramer framer;
        auto framedResult = framer.frame_message(msg);
        if (!framedResult) {
            pImpl->breaker_.recordFailure();
            return framedResult.error();
        }

        auto& framedData = framedResult.value();
        spdlog::debug("Sending {} bytes to daemon (framed with CRC32)", framedData.size());

        // Send framed message data with timeout
        const auto t_start = std::chrono::steady_clock::now();
        size_t totalSent = 0;
        const auto deadline = std::chrono::steady_clock::now() + pImpl->config_.requestTimeout;

        // Allow a single transparent reconnect+retry on EPIPE/ECONNRESET to handle servers that
        // close connections after a response while the pool reuses sockets.
        bool retriedAfterPipe = false;
        while (totalSent < framedData.size()) {
            if (std::chrono::steady_clock::now() > deadline) {
                pImpl->breaker_.recordFailure();
                return Error{ErrorCode::NetworkError, "Send timeout"};
            }

            ssize_t sent = send(pImpl->socketFd_, framedData.data() + totalSent,
                                framedData.size() - totalSent, MSG_NOSIGNAL);

            if (sent < 0) {
                int err = errno;
                if (err == EAGAIN || err == EWOULDBLOCK) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }
                // Some platforms may surface errno==0 here when SIGPIPE is suppressed;
                // map to a meaningful error (prefer EPIPE, fall back to ECONNRESET).
                if (err == 0) {
#ifdef EPIPE
                    err = EPIPE;
#else
                    err = ECONNRESET;
#endif
                }
                // If the peer closed (EPIPE/ECONNRESET), attempt a single reconnect+retry.
                if ((err == EPIPE || err == ECONNRESET) && !retriedAfterPipe) {
                    spdlog::warn(
                        "[DaemonClient] Send failed with {}. Reconnecting and retrying once...",
                        strerror(err));
                    // Reset connection and retry from the beginning.
                    disconnect();
                    auto rc = connect();
                    if (!rc) {
                        pImpl->breaker_.recordFailure();
                        Error e{ErrorCode::NetworkError,
                                std::string("Reconnect failed after send error: ") +
                                    rc.error().message};
                        handler->onError(e);
                        return e;
                    }
                    retriedAfterPipe = true;
                    totalSent = 0;
                    continue;
                }
                pImpl->breaker_.recordFailure();
                return Error{ErrorCode::NetworkError, "Send failed: " + std::string(strerror(err))};
            }

            totalSent += static_cast<size_t>(sent);
        }

        // Read framed response header first
        auto headerResult = readFrameHeader(pImpl->socketFd_);
        if (!headerResult) {
            handler->onError(headerResult.error());
            return headerResult.error();
        }

        auto header = headerResult.value();
        const auto ttfb = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t_start);
        spdlog::debug("[DaemonClient] TTFB={} ms (requestId={})", ttfb.count(), msg.requestId);

        // If server sent a header-only streaming frame, synthesize an empty header response
        // and skip reading a (non-existent) payload. Otherwise, read and parse the full frame.
        Response headerResponse;
        bool haveParsedHeaderPayload = false;
        if (header.is_header_only()) {
            // Synthesize a minimal header aligned with the request type
            if (std::holds_alternative<SearchRequest>(req)) {
                SearchResponse r; // empty; totals will arrive in chunks
                headerResponse = r;
            } else if (std::holds_alternative<ListRequest>(req)) {
                ListResponse r;
                headerResponse = r;
            } else if (std::holds_alternative<GrepRequest>(req)) {
                GrepResponse r;
                headerResponse = r;
            } else {
                // Generic success header for other types
                headerResponse = SuccessResponse{"Streaming"};
            }
        } else {
            // Read the complete frame for the header
            auto frameResult = readFullFrame(pImpl->socketFd_, header);
            if (!frameResult) {
                handler->onError(frameResult.error());
                return frameResult.error();
            }

            // Parse and verify the complete frame
            auto responseMsgResult = framer.parse_frame(frameResult.value());
            if (!responseMsgResult) {
                pImpl->breaker_.recordFailure();
                handler->onError(responseMsgResult.error());
                return responseMsgResult.error();
            }
            auto& responseMsg = responseMsgResult.value();

            // Verify response ID matches request
            if (responseMsg.requestId != msg.requestId) {
                pImpl->breaker_.recordFailure();
                Error err{ErrorCode::InvalidData, "Response ID mismatch"};
                handler->onError(err);
                return err;
            }

            // Extract response from message
            if (!std::holds_alternative<Response>(responseMsg.payload)) {
                pImpl->breaker_.recordFailure();
                Error err{ErrorCode::InvalidData, "Expected response, got request"};
                handler->onError(err);
                return err;
            }
            headerResponse = std::get<Response>(responseMsg.payload);
            haveParsedHeaderPayload = true;

            // If error response, report and exit
            if (auto* err = std::get_if<ErrorResponse>(&headerResponse)) {
                handler->onError(Error{err->code, err->message});
                pImpl->breaker_.recordFailure();
                return Error{err->code, err->message};
            }
        }

        // Process header frame
        handler->onHeaderReceived(headerResponse);

        // If not chunked or last chunk already, we're done
        if (!header.is_chunked() || header.is_last_chunk()) {
            // If we parsed a payload-bearing header, also pass it as the final chunk
            if (haveParsedHeaderPayload) {
                handler->onChunkReceived(headerResponse, true);
            }
            handler->onComplete();
            pImpl->breaker_.recordSuccess();
            if (pImpl->config_.singleUseConnections) {
                disconnect();
            }
            return Result<void>();
        }

        // Process chunks until we get the last one
        bool lastChunkReceived = false;
        while (!lastChunkReceived) {
            // Read next chunk header
            auto chunkHeaderResult = readFrameHeader(pImpl->socketFd_);
            if (!chunkHeaderResult) {
                handler->onError(chunkHeaderResult.error());
                return chunkHeaderResult.error();
            }

            auto chunkHeader = chunkHeaderResult.value();

            // Validate it's a chunk
            if (!chunkHeader.is_chunked()) {
                Error err{ErrorCode::InvalidData, "Expected chunked frame"};
                handler->onError(err);
                return err;
            }

            // Check if it's the last chunk
            lastChunkReceived = chunkHeader.is_last_chunk();

            // Read the complete chunk frame
            auto chunkFrameResult = readFullFrame(pImpl->socketFd_, chunkHeader);
            if (!chunkFrameResult) {
                handler->onError(chunkFrameResult.error());
                return chunkFrameResult.error();
            }

            // Parse and verify the chunk frame
            auto chunkResponseMsgResult = framer.parse_frame(chunkFrameResult.value());
            if (!chunkResponseMsgResult) {
                handler->onError(chunkResponseMsgResult.error());
                pImpl->breaker_.recordFailure();
                return chunkResponseMsgResult.error();
            }
            auto& chunkResponseMsg = chunkResponseMsgResult.value();

            // Verify chunk ID matches request
            if (chunkResponseMsg.requestId != msg.requestId) {
                Error err{ErrorCode::InvalidData, "Chunk response ID mismatch"};
                handler->onError(err);
                pImpl->breaker_.recordFailure();
                return err;
            }

            // Extract chunk response
            if (!std::holds_alternative<Response>(chunkResponseMsg.payload)) {
                Error err{ErrorCode::InvalidData, "Expected response in chunk, got request"};
                handler->onError(err);
                pImpl->breaker_.recordFailure();
                return err;
            }

            // Process the chunk response
            auto& chunkResponse = std::get<Response>(chunkResponseMsg.payload);

            // If error in chunk, report and exit
            if (auto* err = std::get_if<ErrorResponse>(&chunkResponse)) {
                handler->onError(Error{err->code, err->message});
                pImpl->breaker_.recordFailure();
                return Error{err->code, err->message};
            }

            // Process the chunk with handler
            if (!handler->onChunkReceived(chunkResponse, lastChunkReceived)) {
                break;
            }
        }

        // All chunks processed
        handler->onComplete();
        pImpl->breaker_.recordSuccess();
        if (pImpl->config_.singleUseConnections) {
            disconnect();
        }
        return Result<void>();

    } catch (const std::exception& e) {
        pImpl->breaker_.recordFailure();
        Error err{ErrorCode::InternalError, "Error in streaming request: " + std::string(e.what())};
        handler->onError(err);
        return err;
    }
}

Result<EmbeddingResponse> DaemonClient::generateEmbedding(const GenerateEmbeddingRequest& req) {
    auto response = sendRequest(req);
    if (!response) {
        return response.error();
    }

    if (auto* res = std::get_if<EmbeddingResponse>(&response.value())) {
        return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        return Error{err->code, err->message};
    }

    return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Result<BatchEmbeddingResponse>
DaemonClient::generateBatchEmbeddings(const BatchEmbeddingRequest& req) {
    auto response = sendRequest(req);
    if (!response) {
        return response.error();
    }

    if (auto* res = std::get_if<BatchEmbeddingResponse>(&response.value())) {
        return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        return Error{err->code, err->message};
    }

    return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Result<ModelLoadResponse> DaemonClient::loadModel(const LoadModelRequest& req) {
    auto response = sendRequest(req);
    if (!response) {
        return response.error();
    }

    if (auto* res = std::get_if<ModelLoadResponse>(&response.value())) {
        return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        return Error{err->code, err->message};
    }

    return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Result<SuccessResponse> DaemonClient::unloadModel(const UnloadModelRequest& req) {
    auto response = sendRequest(req);
    if (!response) {
        return response.error();
    }

    if (auto* res = std::get_if<SuccessResponse>(&response.value())) {
        return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        return Error{err->code, err->message};
    }

    return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Result<ModelStatusResponse> DaemonClient::getModelStatus(const ModelStatusRequest& req) {
    auto response = sendRequest(req);
    if (!response) {
        return response.error();
    }

    if (auto* res = std::get_if<ModelStatusResponse>(&response.value())) {
        return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        return Error{err->code, err->message};
    }

    return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

std::filesystem::path DaemonClient::resolveSocketPath() {
#ifdef _WIN32
    // Use temp directory on Windows; AF_UNIX is supported via afunix.h on recent Windows.
    return std::filesystem::temp_directory_path() / "yams-daemon.sock";
#else
    namespace fs = std::filesystem;

    // 1. Prefer explicit override from environment variable
    if (const char* env = std::getenv("YAMS_DAEMON_SOCKET")) {
        return fs::path(env);
    }

    // 2. Check if running as root
    bool isRoot = (geteuid() == 0);
    if (isRoot) {
        return fs::path("/var/run/yams-daemon.sock");
    }

    // 3. Check XDG_RUNTIME_DIR
    if (auto xdg = getXDGRuntimeDir(); !xdg.empty() && canWriteToDirectory(xdg)) {
        return xdg / "yams-daemon.sock";
    }

    // 4. Fallback to per-user socket under /tmp
    uid_t uid = getuid();
    return fs::path("/tmp") / ("yams-daemon-" + std::to_string(uid) + ".sock");
#endif
}

bool DaemonClient::isDaemonRunning(const std::filesystem::path& socketPath) {
    // Resolve socket path if not provided
    auto path = socketPath.empty() ? resolveSocketPath() : socketPath;

    // Retry a few times to avoid races when the daemon is starting or rotating the socket
    for (int attempt = 0; attempt < 10; ++attempt) {
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        struct sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);

        if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) {
#ifdef _WIN32
            closesocket(fd);
#else
            close(fd);
#endif
            return true;
        }

        int e = errno;
#ifdef _WIN32
        closesocket(fd);
#else
        close(fd);
#endif

        if (e == ENOENT || e == ECONNREFUSED) {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
            continue;
        }

        // Other errors imply not running or permission issues
        break;
    }

    return false;
}

Result<void> DaemonClient::startDaemon(const ClientConfig& config) {
#ifdef _WIN32
    return Error{ErrorCode::InternalError, "Auto-start not supported on Windows"};
#else
    spdlog::info("Starting YAMS daemon...");

    // Resolve socket path if not provided
    auto socketPath = config.socketPath.empty() ? resolveSocketPath() : config.socketPath;

    // Determine data dir from config or environment (YAMS_STORAGE)
    std::filesystem::path dataDir = config.dataDir;
    if (dataDir.empty()) {
        if (const char* env = std::getenv("YAMS_STORAGE")) {
            dataDir = std::filesystem::path(env);
        }
    }

    // Fork and exec yams-daemon
    pid_t pid = fork();
    if (pid < 0) {
        return Error{ErrorCode::InternalError, "Failed to fork: " + std::string(strerror(errno))};
    }

    if (pid == 0) {
        // Child process - exec yams-daemon
        // If a dataDir is provided, export it for daemon consumption
        // The daemon will use YAMS_STORAGE environment variable
        if (!dataDir.empty()) {
            setenv("YAMS_STORAGE", dataDir.c_str(), 1);
        }

        // Determine config file path
        std::string configPath;
        if (const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME")) {
            configPath = std::filesystem::path(xdgConfigHome) / "yams" / "config.toml";
        } else if (const char* homeEnv = std::getenv("HOME")) {
            configPath = std::filesystem::path(homeEnv) / ".config" / "yams" / "config.toml";
        }

        // Allow overriding daemon path for development via YAMS_DAEMON_BIN
        const char* daemonBin = std::getenv("YAMS_DAEMON_BIN");
        const char* exe = daemonBin && *daemonBin ? daemonBin : "yams-daemon";

        // Use execlp to search PATH (or direct path if overridden) for yams-daemon
        // Pass socket and optional config arguments
        if (!configPath.empty() && std::filesystem::exists(configPath)) {
            execlp(exe, exe, "--socket", socketPath.c_str(), "--config", configPath.c_str(),
                   nullptr);
        } else {
            // No config file, just pass socket
            execlp(exe, exe, "--socket", socketPath.c_str(), nullptr);
        }

        // If we get here, exec failed
        spdlog::error("Failed to exec yams-daemon: {}", strerror(errno));
        spdlog::error("Make sure yams-daemon is installed and in your PATH");
        spdlog::error("You can manually start the daemon with: yams daemon start");
        exit(1);
    }

    // Parent process - wait for daemon to start (poll for socket up to 5s)
    const auto start = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() - start < timeout) {
        if (isDaemonRunning(socketPath)) {
            spdlog::info("Daemon started successfully");
            return Result<void>();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // Build log path for error message - daemon logs to XDG_STATE_HOME or ~/.local/state
    std::filesystem::path logPath;

    // First check XDG_STATE_HOME
    auto xdgState = std::getenv("XDG_STATE_HOME");
    if (xdgState) {
        logPath = std::filesystem::path(xdgState) / "yams" / "daemon.log";
    } else if (auto home = std::getenv("HOME")) {
        // Default to ~/.local/state/yams/daemon.log
        logPath = std::filesystem::path(home) / ".local" / "state" / "yams" / "daemon.log";
    } else if (socketPath.parent_path() == "/var/run") {
        // System daemon
        logPath = "/var/log/yams-daemon.log";
    } else {
        // Last resort fallback
        logPath = socketPath.parent_path() / (socketPath.stem().string() + ".log");
    }

    return Error{ErrorCode::InternalError, "Daemon failed to start within timeout. Check " +
                                               logPath.string() + " for details."};
#endif
}

// Circuit Breaker implementation
CircuitBreaker::CircuitBreaker(const Config& config)
    : config_(config), lastStateChange_(std::chrono::steady_clock::now()) {}

bool CircuitBreaker::shouldAllow() {
    if (state_ == State::Closed) {
        return true;
    }

    if (state_ == State::Open) {
        if (shouldTransitionToHalfOpen()) {
            transitionTo(State::HalfOpen);
            return true;
        }
        return false;
    }

    // HalfOpen - allow limited requests
    return true;
}

void CircuitBreaker::recordSuccess() {
    if (state_ == State::HalfOpen) {
        consecutiveSuccesses_++;
        if (consecutiveSuccesses_ >= config_.successThreshold) {
            transitionTo(State::Closed);
        }
    }
    consecutiveFailures_ = 0;
}

void CircuitBreaker::recordFailure() {
    consecutiveFailures_++;
    consecutiveSuccesses_ = 0;

    if (state_ == State::Closed && consecutiveFailures_ >= config_.failureThreshold) {
        transitionTo(State::Open);
    } else if (state_ == State::HalfOpen) {
        transitionTo(State::Open);
    }
}

void CircuitBreaker::transitionTo(State newState) {
    state_ = newState;
    lastStateChange_ = std::chrono::steady_clock::now();

    if (newState == State::Open) {
        openedAt_ = lastStateChange_;
    }

    consecutiveFailures_ = 0;
    consecutiveSuccesses_ = 0;
}

bool CircuitBreaker::shouldTransitionToHalfOpen() const {
    auto now = std::chrono::steady_clock::now();
    auto timeSinceOpen = std::chrono::duration_cast<std::chrono::seconds>(now - openedAt_);
    return timeSinceOpen >= config_.openTimeout;
}

void DaemonClient::setStreamingEnabled(bool enabled) {
    if (pImpl) {
        pImpl->config_.enableChunkedResponses = enabled;
    }
}

} // namespace yams::daemon
