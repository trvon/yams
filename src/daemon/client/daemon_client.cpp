#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include <spdlog/spdlog.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <string>
#include <string_view>
#include <thread>

#ifndef _WIN32
#include <unistd.h>
#include <sys/wait.h>
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

    ~Impl() = default;

    ClientConfig config_;
    CircuitBreaker breaker_;
    std::chrono::milliseconds headerTimeout_{30000}; // 30s default
    std::chrono::milliseconds bodyTimeout_{60000};   // 60s default
};

// DaemonClient implementation
DaemonClient::DaemonClient(const ClientConfig& config) : pImpl(std::make_unique<Impl>(config)) {
    if (pImpl->config_.socketPath.empty()) {
        pImpl->config_.socketPath = resolveSocketPathConfigFirst();
    }
    spdlog::debug("DaemonClient init: resolved socket='{}'", pImpl->config_.socketPath.string());
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

// New: lightweight readiness probe that sends a real Ping and waits briefly
static bool pingDaemonSync(const std::filesystem::path& socketPath,
                           std::chrono::milliseconds requestTimeout = std::chrono::milliseconds(500),
                           std::chrono::milliseconds headerTimeout = std::chrono::milliseconds(250),
                           std::chrono::milliseconds bodyTimeout   = std::chrono::milliseconds(250)) {
    using namespace yams::daemon;
    try {
        AsioTransportAdapter::Options opts{};
        opts.socketPath     = socketPath.empty() ? DaemonClient::resolveSocketPath() : socketPath;
        if (opts.socketPath.empty()) return false;
        opts.requestTimeout = requestTimeout;
        opts.headerTimeout  = headerTimeout;
        opts.bodyTimeout    = bodyTimeout;

        AsioTransportAdapter adapter(opts);
        
        // Create a synchronous wrapper for the async operation
        auto& io = GlobalIOContext::instance().get_io_context();
        std::promise<Result<Response>> promise;
        auto future = promise.get_future();
        
        // Run the async operation and capture result
        auto task = adapter.send_request(Request{PingRequest{}});
        
        // Since Task is a coroutine type, we need to run it to completion
        // by spawning a coroutine that waits for it
        auto runner = [](Task<Result<Response>> t, std::promise<Result<Response>>& p) -> Task<void> {
            try {
                auto result = co_await t;
                p.set_value(std::move(result));
            } catch (...) {
                p.set_exception(std::current_exception());
            }
            co_return;
        };
        
        // Create and start the runner task
        auto runnerTask = runner(std::move(task), promise);
        
        // Process IO events until the future is ready or timeout
        auto deadline = std::chrono::steady_clock::now() + requestTimeout + headerTimeout + bodyTimeout;
        while (future.wait_for(std::chrono::milliseconds(1)) != std::future_status::ready) {
            if (std::chrono::steady_clock::now() > deadline) {
                return false;  // Timeout
            }
            io.run_one();
        }
        
        // Get the result
        try {
            auto result = future.get();
            if (!result) return false;
            return std::holds_alternative<PongResponse>(result.value());
        } catch (...) {
            return false;
        }
    } catch (...) {
        return false;
    }
}

Result<void> DaemonClient::connect() {
    // Treat connect() as a liveness gate. If the daemon is not reachable and
    // autoStart is disabled, surface a failure so callers (e.g., pools/tests)
    // can react accordingly.
    const bool alive = pingDaemonSync(pImpl->config_.socketPath);
    if (!alive && !pImpl->config_.autoStart) {
        return Error{ErrorCode::NetworkError, "Daemon not running"};
    }

    // Ensure daemon is running if auto-start is enabled; no persistent socket is kept.
    if (!alive && pImpl->config_.autoStart) {
        spdlog::info("Daemon not running, attempting to auto-start...");
        if (auto result = startDaemon(pImpl->config_); !result) {
            spdlog::warn("Failed to auto-start daemon: {}",
                         sanitize_for_terminal(result.error().message));
            spdlog::info("Please manually start the daemon with: yams daemon start");
            return result.error();
        }
        // Wait for daemon readiness with exponential backoff
        const int maxRetries = 10;
        const auto baseDelay = std::chrono::milliseconds(100);
        for (int i = 0; i < maxRetries; ++i) {
            auto delay = baseDelay * (1 << std::min(i, 5));
            std::this_thread::sleep_for(delay);
            if (pingDaemonSync(pImpl->config_.socketPath)) {
                spdlog::debug("Daemon started successfully after {} retries", i + 1);
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                break;
            }
            if (i == maxRetries - 1) {
                return Error{ErrorCode::Timeout, "Daemon failed to start after retries"};
            }
        }
    }
    // Reset breaker state if disabled
    if (!pImpl->config_.enableCircuitBreaker) {
        pImpl->breaker_.recordSuccess();
    }
    return Result<void>();
}

void DaemonClient::disconnect() {
    // No-op for Asio transport; connections are per-request and scoped in adapter
}

bool DaemonClient::isConnected() const {
    // Treat connectivity as liveness of the daemon (socket + ping), not a persistent socket
    return pingDaemonSync(pImpl->config_.socketPath,
                          std::chrono::milliseconds(250),
                          std::chrono::milliseconds(150),
                          std::chrono::milliseconds(300));
}

Task<Result<SearchResponse>> DaemonClient::search(const SearchRequest& req) {
    // Always use streaming search
    co_return co_await streamingSearch(req);
}

Task<Result<SearchResponse>> DaemonClient::streamingSearch(const SearchRequest& req) {
    spdlog::debug("DaemonClient::streamingSearch called");
    auto handler = std::make_shared<StreamingSearchHandler>(req.pathsOnly, req.limit);

    spdlog::debug("DaemonClient::streamingSearch calling sendRequestStreaming");
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
    spdlog::debug("DaemonClient::list entry: [{}] streaming=true", getRequestName(Request{req}));
    co_return co_await streamingList(req);
}

Task<Result<GrepResponse>> DaemonClient::grep(const GrepRequest& req) {
    spdlog::debug("DaemonClient::grep entry: [{}] streaming=true", getRequestName(Request{req}));
    co_return co_await streamingGrep(req);
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
        // Can't mix Task with boost::asio::awaitable, use sync sleep
        std::this_thread::sleep_for(std::chrono::milliseconds(75 * (attempt + 1)));
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
    spdlog::debug("DaemonClient::sendRequest: [{}] streaming={} sock='{}'", getRequestName(req), false, pImpl->config_.socketPath.string());
    // Skip the legacy connect() call when using AsioTransportAdapter
    // The adapter creates its own connection, and calling connect() here
    // causes a double connection issue where the POSIX socket immediately EOFs
    
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
        spdlog::debug("StreamingSearchHandler: header received (totalCount={}, elapsed={}ms)",
                      searchRes->totalCount, searchRes->elapsed.count());
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
                const std::string* p = &result.path;
                auto it = result.metadata.find("path");
                if ((p->empty()) && it != result.metadata.end()) {
                    std::cout << it->second << std::endl;
                } else if (!p->empty()) {
                    std::cout << *p << std::endl;
                } else if (!result.id.empty()) {
                    std::cout << result.id << std::endl;
                }
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
    if (!error_ && pathsOnly_ && count_ == 0) {
        std::cout << "(no results)" << std::endl;
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
    if (!error_ && pathsOnly_ && count_ == 0) {
        // Explicitly indicate no results for paths-only output
        std::cout << "(no results)" << std::endl;
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
    if (!error_ && pathsOnly_ && matches_.empty()) {
        std::cout << "(no results)" << std::endl;
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

Task<Result<void>>
DaemonClient::sendRequestStreaming(const Request& req,
                                   std::shared_ptr<ChunkedResponseHandler> handler) {
    spdlog::debug("DaemonClient::sendRequestStreaming: [{}] streaming={} sock='{}'", getRequestName(req), true, pImpl->config_.socketPath.string());
    // Skip the legacy connect() call when using AsioTransportAdapter
    // The adapter creates its own connection, and calling connect() here
    // causes a double connection issue where the POSIX socket immediately EOFs
    
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
    spdlog::debug("DaemonClient::sendRequestStreaming calling adapter.send_request_streaming");
    auto res = co_await adapter.send_request_streaming(req, onHeader, onChunk, onError, onComplete);
    if (!res)
        co_return res.error();
    co_return Result<void>();
}

// Streaming AddDocument helper: header-first then final chunk contains full response
Task<Result<AddDocumentResponse>>
DaemonClient::streamingAddDocument(const AddDocumentRequest& req) {
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
    if (handler->error.has_value()) {
        co_return handler->error.value();
    }
    if (handler->value.has_value()) {
        co_return handler->value.value();
    }
    co_return Error{ErrorCode::InvalidData, "Missing AddDocumentResponse in stream"};
}

Task<Result<EmbeddingResponse>>
DaemonClient::generateEmbedding(const GenerateEmbeddingRequest& req) {
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

std::filesystem::path DaemonClient::resolveSocketPathConfigFirst() {
#ifndef _WIN32
    namespace fs = std::filesystem;
    if (const char* env = std::getenv("YAMS_DAEMON_SOCKET"); env && *env) {
        return fs::path(env);
    }
    try {
        fs::path cfgPath;
        if (const char* xdg = std::getenv("XDG_CONFIG_HOME")) {
            cfgPath = fs::path(xdg) / "yams" / "config.toml";
        } else if (const char* home = std::getenv("HOME")) {
            cfgPath = fs::path(home) / ".config" / "yams" / "config.toml";
        }
        if (!cfgPath.empty() && fs::exists(cfgPath)) {
            // Lightweight scanner: find [daemon] section, then socket_path = "..."
            std::ifstream in(cfgPath);
            if (in) {
                std::string line;
                bool inDaemon = false;
                while (std::getline(in, line)) {
                    // strip spaces
                    auto trim = [](std::string s) {
                        auto issp = [](unsigned char c){ return std::isspace(c); };
                        s.erase(s.begin(), std::find_if(s.begin(), s.end(), [&](unsigned char c){return !issp(c);}));
                        s.erase(std::find_if(s.rbegin(), s.rend(), [&](unsigned char c){return !issp(c);} ).base(), s.end());
                        return s;
                    };
                    line = trim(line);
                    if (line.empty() || line[0] == '#') continue;
                    if (line.size() >= 9 && line.rfind("[daemon]", 0) == 0) { inDaemon = true; continue; }
                    if (inDaemon && !line.empty() && line[0] == '[') { inDaemon = false; }
                    if (inDaemon) {
                        // look for socket_path = "..."
                        const std::string key = "socket_path";
                        auto pos = line.find(key);
                        if (pos != std::string::npos) {
                            auto eq = line.find('=', pos + key.size());
                            if (eq != std::string::npos) {
                                std::string rhs = trim(line.substr(eq + 1));
                                // remove optional quotes
                                if (!rhs.empty() && (rhs.front() == '"' || rhs.front() == '\'')) {
                                    char q = rhs.front();
                                    auto endq = rhs.find_last_of(q);
                                    if (endq != std::string::npos && endq > 0) {
                                        std::string val = rhs.substr(1, endq - 1);
                                        if (!val.empty()) return fs::path(val);
                                    }
                                } else if (!rhs.empty()) {
                                    return fs::path(rhs);
                                }
                            }
                        }
                    }
                }
            }
        }
    } catch (...) {
        // ignore and fall back
    }
    return resolveSocketPath();
#else
    return resolveSocketPath();
#endif
}

bool DaemonClient::isDaemonRunning(const std::filesystem::path& socketPath) {
    auto path = socketPath.empty() ? resolveSocketPath() : socketPath;
    // Lightweight readiness probe using real Ping via Asio transport
    return pingDaemonSync(path, std::chrono::milliseconds(400), std::chrono::milliseconds(200),
                          std::chrono::milliseconds(300));
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
            configPath =
                (std::filesystem::path(homeEnv) / ".config" / "yams" / "config.toml").string();
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
                    cliDir / "yams-daemon", cliDir.parent_path() / "yams-daemon",
                    cliDir.parent_path() / "daemon" / "yams-daemon",
                    cliDir.parent_path().parent_path() / "daemon" / "yams-daemon",
                    cliDir.parent_path().parent_path() / "yams-daemon"};
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
            execlp(exePath.c_str(), exePath.c_str(), "--socket", socketPath.c_str(), "--log-level",
                   ll, nullptr);
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

    // Parent process: detach a lightweight reaper so short-lived intermediary
    // children (e.g., if the daemon double-forks) do not become zombies. This
    // thread will block until the first child exits and will then clean up.
#ifndef _WIN32
    {
        std::thread([pid]() {
            int status = 0;
            (void)::waitpid(pid, &status, 0);
        }).detach();
    }
#endif

    // Do not enforce a hard readiness timeout here.
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
            if (pingDaemonSync(socketPath)) {
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
