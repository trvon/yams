#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/client/global_io_context.h>

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

    auto rc = pImpl->connect();
    // If circuit breaker is disabled by config, ensure it doesn't gate subsequent requests
    // by resetting to a known good state after successful connect attempts.
    if (rc && !pImpl->config_.enableCircuitBreaker) {
        // A no-op success record will move HalfOpen->Closed if needed.
        pImpl->breaker_.recordSuccess();
    }
    return rc;
}

void DaemonClient::disconnect() {
    pImpl->disconnect();
}

bool DaemonClient::isConnected() const {
    return pImpl->isConnected();
}

Task<Result<SearchResponse>> DaemonClient::search(const SearchRequest& req) {
    // Use streaming search if enabled
    if (pImpl->config_.enableChunkedResponses) {
        co_return co_await streamingSearch(req);
    }

    // Fall back to traditional request/response
    auto response = co_await sendRequest(req);
    if (!response) {
        co_return Error{response.error().code, response.error().message};
    }

    if (auto* res = std::get_if<SearchResponse>(&response.value())) {
        co_return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        co_return Error{err->code, err->message};
    }

    co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Task<Result<SearchResponse>> DaemonClient::streamingSearch(const SearchRequest& req) {
    auto handler = std::make_shared<StreamingSearchHandler>(req.pathsOnly, req.limit);

    auto result = co_await sendRequestStreaming(req, handler);
    if (!result) {
        co_return result.error();
    }

    co_return handler->getResults();
}

Task<Result<GetResponse>> DaemonClient::get(const GetRequest& req) {
    auto response = co_await sendRequest(req);
    if (!response) {
        co_return response.error();
    }

    if (auto* res = std::get_if<GetResponse>(&response.value())) {
        co_return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        co_return Error{err->code, err->message};
    }

    co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Task<Result<ListResponse>> DaemonClient::list(const ListRequest& req) {
    // Use streaming list if enabled
    if (pImpl->config_.enableChunkedResponses) {
        co_return co_await streamingList(req);
    }

    // Fall back to traditional request/response
    auto response = co_await sendRequest(req);
    if (!response) {
        co_return Error{response.error().code, response.error().message};
    }

    if (auto* res = std::get_if<ListResponse>(&response.value())) {
        co_return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        co_return Error{err->code, err->message};
    }

    co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Task<Result<GrepResponse>> DaemonClient::grep(const GrepRequest& req) {
    // Prefer streaming if enabled
    if (pImpl->config_.enableChunkedResponses) {
        co_return co_await streamingGrep(req);
    }

    // Fallback to traditional request/response
    auto response = co_await sendRequest(req);
    if (!response) {
        co_return Error{response.error().code, response.error().message};
    }

    if (auto* res = std::get_if<GrepResponse>(&response.value())) {
        co_return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        co_return Error{err->code, err->message};
    }

    co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Task<Result<StatusResponse>> DaemonClient::status() {
    StatusRequest req;
    req.detailed = true;

    // Transient-aware retry loop for early startup/socket closure races
    Error lastErr{ErrorCode::NetworkError, "Uninitialized"};
    for (int attempt = 0; attempt < 5; ++attempt) {
    auto response = co_await sendRequest(req);
        if (response) {
            if (auto* res = std::get_if<StatusResponse>(&response.value())) {
                co_return *res;
            }
            if (auto* er = std::get_if<ErrorResponse>(&response.value())) {
                co_return Error{er->code, er->message};
            }
            co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
        }

        lastErr = response.error();
        const std::string& msg = lastErr.message;
        bool transient = (lastErr.code == ErrorCode::NetworkError) &&
                         (msg.find("Connection closed") != std::string::npos ||
                          msg.find("Connection reset") != std::string::npos ||
                          msg.find("ECONNRESET") != std::string::npos ||
                          msg.find("EPIPE") != std::string::npos ||
                          msg.find("Broken pipe") != std::string::npos ||
                          msg.find("read header failed") != std::string::npos ||
                          msg.find("read payload failed") != std::string::npos ||
                          msg.find("Read failed") != std::string::npos);
        if (!transient) {
            co_return lastErr;
        }
        using namespace std::chrono_literals;
        co_await GlobalIOContext::instance().get_io_context().sleep_for(std::chrono::milliseconds(75 * (attempt + 1)));
    }
    co_return lastErr;
}

Task<Result<Response>> DaemonClient::executeRequest(const Request& req) {
    co_return co_await sendRequest(req);
}

Task<Result<void>> DaemonClient::shutdown(bool graceful) {
    ShutdownRequest req;
    req.graceful = graceful;

    auto response = co_await sendRequest(req);
    if (!response) {
        co_return Error{response.error().code, response.error().message};
    }

    if (std::holds_alternative<SuccessResponse>(response.value())) {
        co_return Result<void>();
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        co_return Error{err->code, err->message};
    }

    co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Task<Result<void>> DaemonClient::ping() {
    PingRequest req;

    auto response = co_await sendRequest(req);
    if (!response) {
        co_return Error{response.error().code, response.error().message};
    }

    if (std::holds_alternative<PongResponse>(response.value())) {
        co_return Result<void>();
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        co_return Error{err->code, err->message};
    }

    int mt = static_cast<int>(getMessageType(response.value()));
    spdlog::error("Ping: unexpected response variant (type={})", mt);
    co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Task<Result<Response>> DaemonClient::sendRequest(const Request& req) {
    if (auto rc = connect(); !rc) {
        co_return rc.error();
    }
    // Do not disconnect here; the transport adapter manages the connection lifecycle.

    AsioTransportAdapter::Options opts;
    opts.socketPath = pImpl->config_.socketPath;
    opts.headerTimeout = pImpl->headerTimeout_;
    opts.bodyTimeout = pImpl->bodyTimeout_;
    opts.requestTimeout = pImpl->config_.requestTimeout;
    AsioTransportAdapter adapter(opts);
    auto r = co_await adapter.send_request(req);
    if (!r)
        co_return r.error();
    co_return r.value();
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

Result<MessageFramer::FrameHeader>
DaemonClient::readFrameHeaderWithTimeout(int socketFd, std::chrono::milliseconds timeout) {
    // Read framed response header with a custom timeout (used for chunk headers)
    auto deadline = std::chrono::steady_clock::now() + timeout;
    size_t totalReceived = 0;
    const size_t headerSize = sizeof(MessageFramer::FrameHeader);
    std::vector<uint8_t> headerData(headerSize);

    while (totalReceived < headerSize) {
        if (std::chrono::steady_clock::now() > deadline) {
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

    MessageFramer::FrameHeader header;
    std::memcpy(&header, headerData.data(), sizeof(header));
    header.from_network();

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
    // Use an inactivity-based timeout: reset the deadline after each successful read.
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
        // Reset inactivity deadline on progress
        deadline = std::chrono::steady_clock::now() + timeout;
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
Task<Result<ListResponse>> DaemonClient::streamingList(const ListRequest& req) {
    auto handler = std::make_shared<StreamingListHandler>(req.pathsOnly, req.limit);

    auto result = co_await sendRequestStreaming(req, handler);
    if (!result) {
        co_return result.error();
    }

    co_return handler->getResults();
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
Task<Result<GrepResponse>> DaemonClient::streamingGrep(const GrepRequest& req) {
    // If pathsOnly or filesOnly/countOnly, we can progressively print in handler
    size_t perFileCap = 0;
    if (req.maxMatches > 0) {
        perFileCap = req.maxMatches;
    }
    auto handler =
        std::make_shared<StreamingGrepHandler>(req.pathsOnly || req.filesOnly, perFileCap);

    auto result = co_await sendRequestStreaming(req, handler);
    if (!result) {
        co_return result.error();
    }

    co_return handler->getResults();
}

Task<Result<void>> DaemonClient::sendRequestStreaming(const Request& req,
                                                std::shared_ptr<ChunkedResponseHandler> handler) {
    if (auto rc = connect(); !rc) {
        co_return rc.error();
    }
    // Do not disconnect here; the transport adapter manages the connection lifecycle.

    AsioTransportAdapter::Options opts;
    opts.socketPath = pImpl->config_.socketPath;
    opts.headerTimeout = pImpl->headerTimeout_;
    opts.bodyTimeout = pImpl->bodyTimeout_;
    opts.requestTimeout = pImpl->config_.requestTimeout;

    auto onHeader = [handler](const Response& r) { handler->onHeaderReceived(r); };
    auto onChunk = [handler](const Response& r, bool last) {
        return handler->onChunkReceived(r, last);
    };
    auto onError = [handler](const Error& e) { handler->onError(e); };
    auto onComplete = [handler]() { handler->onComplete(); };

    AsioTransportAdapter adapter(opts);
    auto res = co_await adapter.send_request_streaming(
        req, onHeader, onChunk, onError, onComplete);
    if (!res)
        co_return res.error();
    co_return Result<void>();
}

// Streaming AddDocument helper: header-first then final chunk contains full response
Task<Result<AddDocumentResponse>> DaemonClient::streamingAddDocument(const AddDocumentRequest& req) {
    struct AddDocHandler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& /*headerResponse*/) override {}
        bool onChunkReceived(const Response& chunkResponse, bool isLastChunk) override {
            if (auto* err = std::get_if<ErrorResponse>(&chunkResponse)) {
                error = Error{err->code, err->message};
                return false;
            }
            if (auto* add = std::get_if<AddDocumentResponse>(&chunkResponse)) {
                if (!isLastChunk) {
                    // We expect the full response as the last chunk; ignore interim
                    return true;
                }
                value = *add;
                return true;
            }
            // Ignore other chunk types
            return true;
        }
        void onError(const Error& e) override { error = e; }
        void onComplete() override {}
        std::optional<Error> error;
        std::optional<AddDocumentResponse> value;
    };

    auto handler = std::make_shared<AddDocHandler>();
    auto result = co_await sendRequestStreaming(req, handler);
    if (!result) {
        co_return result.error();
    }
    if (handler->error) {
        co_return *handler->error;
    }
    if (handler->value) {
        co_return *handler->value;
    }
    co_return Error{ErrorCode::InvalidData, "Missing AddDocumentResponse in stream"};
}

Task<Result<EmbeddingResponse>> DaemonClient::generateEmbedding(const GenerateEmbeddingRequest& req) {
    auto response = co_await sendRequest(req);
    if (!response) {
        co_return response.error();
    }

    if (auto* res = std::get_if<EmbeddingResponse>(&response.value())) {
        co_return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        co_return Error{err->code, err->message};
    }

    co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Task<Result<BatchEmbeddingResponse>>
DaemonClient::generateBatchEmbeddings(const BatchEmbeddingRequest& req) {
    auto response = co_await sendRequest(req);
    if (!response) {
        co_return response.error();
    }

    if (auto* res = std::get_if<BatchEmbeddingResponse>(&response.value())) {
        co_return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        co_return Error{err->code, err->message};
    }

    co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Task<Result<ModelLoadResponse>> DaemonClient::loadModel(const LoadModelRequest& req) {
    auto response = co_await sendRequest(req);
    if (!response) {
        co_return response.error();
    }

    if (auto* res = std::get_if<ModelLoadResponse>(&response.value())) {
        co_return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        co_return Error{err->code, err->message};
    }

    co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Task<Result<SuccessResponse>> DaemonClient::unloadModel(const UnloadModelRequest& req) {
    auto response = co_await sendRequest(req);
    if (!response) {
        co_return response.error();
    }

    if (auto* res = std::get_if<SuccessResponse>(&response.value())) {
        co_return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        co_return Error{err->code, err->message};
    }

    co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Task<Result<ModelStatusResponse>> DaemonClient::getModelStatus(const ModelStatusRequest& req) {
    auto response = co_await sendRequest(req);
    if (!response) {
        co_return response.error();
    }

    if (auto* res = std::get_if<ModelStatusResponse>(&response.value())) {
        co_return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        co_return Error{err->code, err->message};
    }

    co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
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

        // Determine config file path (env override > XDG/HOME)
        std::string configPath;
        if (const char* cfgEnv = std::getenv("YAMS_CONFIG"); cfgEnv && *cfgEnv) {
            configPath = cfgEnv;
        } else if (const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME")) {
            configPath = (std::filesystem::path(xdgConfigHome) / "yams" / "config.toml").string();
        } else if (const char* homeEnv = std::getenv("HOME")) {
            configPath = (std::filesystem::path(homeEnv) / ".config" / "yams" / "config.toml").string();
        }

        // Allow overriding daemon path for development via YAMS_DAEMON_BIN
        std::string exePath;
        if (const char* daemonBin = std::getenv("YAMS_DAEMON_BIN"); daemonBin && *daemonBin) {
            exePath = daemonBin;
        } else {
            // Try to auto-detect relative to this process path
            // On Linux, read /proc/self/exe
            std::error_code ec;
            std::filesystem::path selfExe;
#ifndef _WIN32
            char buf[4096];
            ssize_t n = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
            if (n > 0) {
                buf[n] = '\0';
                selfExe = std::filesystem::path(buf);
            }
#endif
            if (!selfExe.empty()) {
                auto cliDir = selfExe.parent_path();
                // Common build-tree locations
                std::vector<std::filesystem::path> candidates = {
                    cliDir / "yams-daemon",
                    cliDir.parent_path() / "yams-daemon",
                    cliDir.parent_path() / "daemon" / "yams-daemon",
                    cliDir.parent_path().parent_path() / "daemon" / "yams-daemon",
                    cliDir.parent_path().parent_path() / "yams-daemon"
                };
                for (const auto& p : candidates) {
                    if (std::filesystem::exists(p)) {
                        exePath = p.string();
                        break;
                    }
                }
            }
            if (exePath.empty()) {
                // Fall back to PATH lookup
                exePath = "yams-daemon";
            }
        }

        // Use execlp to search PATH (or direct path if overridden) for yams-daemon
        // Pass socket and optional config/log-level arguments
        const char* ll = std::getenv("YAMS_LOG_LEVEL");
        bool haveCfg = !configPath.empty() && std::filesystem::exists(configPath);
        if (haveCfg && ll && *ll) {
            execlp(exePath.c_str(), exePath.c_str(), "--socket", socketPath.c_str(), "--config",
                   configPath.c_str(), "--log-level", ll, nullptr);
        } else if (haveCfg) {
            execlp(exePath.c_str(), exePath.c_str(), "--socket", socketPath.c_str(), "--config",
                   configPath.c_str(), nullptr);
        } else if (ll && *ll) {
            execlp(exePath.c_str(), exePath.c_str(), "--socket", socketPath.c_str(),
                   "--log-level", ll, nullptr);
        } else {
            // No config file or log level, just pass socket
            execlp(exePath.c_str(), exePath.c_str(), "--socket", socketPath.c_str(), nullptr);
        }

        // If we get here, exec failed
        spdlog::error("Failed to exec yams-daemon: {}", strerror(errno));
        spdlog::error("Make sure yams-daemon is installed and in your PATH");
        spdlog::error("You can manually start the daemon with: yams daemon start");
        exit(1);
    }

    // Parent process: do not enforce a hard readiness timeout here.
    // Perform a brief best-effort check for the socket, then return success so
    // the caller (CLI) can show a live spinner and poll detailed status.
    // Optional override via env: YAMS_STARTUP_SOCKET_WAIT_MS (default ~500ms)
    int wait_ms = 500;
    if (const char* env = std::getenv("YAMS_STARTUP_SOCKET_WAIT_MS")) {
        try {
            wait_ms = std::max(0, std::stoi(env));
        } catch (...) {
        }
    }
    if (wait_ms > 0) {
        const auto start = std::chrono::steady_clock::now();
        const auto timeout = std::chrono::milliseconds(wait_ms);
        while (std::chrono::steady_clock::now() - start < timeout) {
            if (isDaemonRunning(socketPath)) {
                spdlog::info("Daemon process spawned and socket accepting");
                return Result<void>();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    spdlog::info("Daemon process spawned; socket not accepting yet (continuing)");
    return Result<void>();
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

Task<Result<AddResponse>> DaemonClient::add(const AddRequest& req) {
    auto response = co_await sendRequest(req);
    if (!response) {
        co_return response.error();
    }

    if (auto* res = std::get_if<AddResponse>(&response.value())) {
        co_return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        co_return Error{err->code, err->message};
    }

    co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

Task<Result<SuccessResponse>> DaemonClient::remove(const DeleteRequest& req) {
    auto response = co_await sendRequest(req);
    if (!response) {
        co_return response.error();
    }

    if (auto* res = std::get_if<SuccessResponse>(&response.value())) {
        co_return *res;
    }

    if (auto* err = std::get_if<ErrorResponse>(&response.value())) {
        co_return Error{err->code, err->message};
    }

    co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

} // namespace yams::daemon
