#pragma once

#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <unordered_map>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/local/stream_protocol.hpp>

namespace yams::daemon {

// Forward declaration to avoid adding heavy includes in this header
class ConnectionFsm;
class RequestDispatcher;

// Request processor interface
class RequestProcessor {
public:
    virtual ~RequestProcessor() = default;

    [[nodiscard]] virtual boost::asio::awaitable<Response> process(const Request& request) = 0;

    // Process a request with streaming response support
    [[nodiscard]] virtual boost::asio::awaitable<std::optional<Response>> process_streaming(const Request& request) {
        // Default implementation wraps the normal process method
        auto response = co_await process(request);
        co_return response;
    }

    // Check if a request type should be handled with streaming
    [[nodiscard]] virtual bool supports_streaming(const Request& request) const {
        // Check if this is a request type that benefits from streaming
        if (std::holds_alternative<SearchRequest>(request) ||
            std::holds_alternative<ListRequest>(request) ||
            std::holds_alternative<GrepRequest>(request) ||
            std::holds_alternative<DownloadRequest>(request)) {
            return true;
        }
        return false;
    }

    // Stream chunks for a response (for implementation by derived classes)
    struct ResponseChunk {
        Response data;
        bool is_last_chunk;
    };

    // Stream response chunks (called by process_streaming when it returns std::nullopt)
    [[nodiscard]] virtual boost::asio::awaitable<ResponseChunk> next_chunk() {
        // Default implementation returns an empty last chunk
        co_return ResponseChunk{.data = SuccessResponse{"End of stream"}, .is_last_chunk = true};
    }
};

// Coroutine-based request handler
class RequestHandler {
public:
    using ProcessorFunc = std::function<boost::asio::awaitable<Response>(const Request&)>;

    struct Config {
        bool enable_streaming = true;     // Use streaming response model
        size_t chunk_size = 64 * 1024;    // Default chunk size (64KB)
        size_t header_flush_delay_ms = 0; // Time to wait before flushing header (0 = immediate)
        size_t chunk_flush_delay_ms = 0;  // Time to wait before flushing chunks (0 = immediate)
        std::chrono::seconds write_timeout{30}; // Timeout for write operations
        std::chrono::seconds read_timeout{30};  // Timeout for read operations
        bool auto_detect_streaming = true;      // Auto-detect requests that benefit from streaming
        bool force_streaming = false;           // Force streaming for all responses
        size_t streaming_threshold = 1024 * 1024; // Size threshold for auto-streaming (1MB)
        bool close_after_response = true; // Close connection after sending complete response
                                          // (one-request-per-connection)
        // Maximum allowed frame size (bytes) for inbound messages; must align with server limits
        size_t max_frame_size = 10 * 1024 * 1024; // 10MB default

        Config();
    };

    explicit RequestHandler(std::shared_ptr<RequestProcessor> processor = nullptr,
                            Config config = Config{});
    
    // Constructor for direct RequestDispatcher usage (simplified interface)
    explicit RequestHandler(RequestDispatcher* dispatcher, Config config = Config{});
    
    ~RequestHandler();

    // Set the request processor
    void set_processor(std::shared_ptr<RequestProcessor> processor) {
        processor_ = std::move(processor);
    }

    // Update configuration at runtime (used by AsyncIpcServer to align limits/timeouts)
    void set_config(const Config& cfg) {
        config_ = cfg;
        // Keep framer's max message size aligned with config
        framer_ = MessageFramer(cfg.max_frame_size);
    }

    // Handle a connection with coroutines using native boost::asio socket
    [[nodiscard]] boost::asio::awaitable<void> 
    handle_connection(boost::asio::local::stream_protocol::socket socket,
                      std::stop_token token);

    // Handle a single request-response cycle (internal use)
    [[nodiscard]] boost::asio::awaitable<Result<void>> 
    handle_request(boost::asio::local::stream_protocol::socket& socket,
                   const Request& request, uint64_t request_id);

    // Simple interface for handling raw request data (for use with native Boost.ASIO)
    [[nodiscard]] boost::asio::awaitable<std::vector<uint8_t>> 
    handle_request(const std::vector<uint8_t>& request_data, std::stop_token token);

    // Handle a streaming response
    [[nodiscard]] boost::asio::awaitable<Result<void>> 
    handle_streaming_request(boost::asio::local::stream_protocol::socket& socket,
                            const Request& request,
                            uint64_t request_id,
                            ConnectionFsm* fsm = nullptr);

    // Write a header frame and optionally flush
    [[nodiscard]] boost::asio::awaitable<Result<void>> 
    write_header(boost::asio::local::stream_protocol::socket& socket,
                 const Response& response, uint64_t request_id,
                 bool flush = true);

    // Write a chunk frame and optionally flush
    [[nodiscard]] boost::asio::awaitable<Result<void>> 
    write_chunk(boost::asio::local::stream_protocol::socket& socket,
                const Response& response, uint64_t request_id,
                bool last_chunk = false, bool flush = true);

    // Stream chunks from a processor
    [[nodiscard]] boost::asio::awaitable<Result<void>> 
    stream_chunks(boost::asio::local::stream_protocol::socket& socket,
                  const Request& request, uint64_t request_id,
                  std::shared_ptr<RequestProcessor> processor,
                  ConnectionFsm* fsm = nullptr);

    // Determine if a request should be streamed based on config and request type
    [[nodiscard]] bool should_stream_request(const Request& request) const;

    // Statistics
    struct Stats {
        size_t requests_processed{0};
        size_t requests_failed{0};
        size_t bytes_received{0};
        size_t bytes_sent{0};
        std::chrono::nanoseconds total_processing_time{0};
        std::chrono::nanoseconds min_latency{std::chrono::nanoseconds::max()};
        std::chrono::nanoseconds max_latency{0};
    };

    [[nodiscard]] Stats get_stats() const {
        Stats copy;
        copy.requests_processed = stats_.requests_processed.load();
        copy.requests_failed = stats_.requests_failed.load();
        copy.bytes_received = stats_.bytes_received.load();
        copy.bytes_sent = stats_.bytes_sent.load();
        copy.total_processing_time = stats_.total_processing_time;
        copy.min_latency = stats_.min_latency;
        copy.max_latency = stats_.max_latency;
        return copy;
    }
    void reset_stats() {
        stats_.requests_processed = 0;
        stats_.requests_failed = 0;
        stats_.bytes_received = 0;
        stats_.bytes_sent = 0;
        stats_.total_processing_time = std::chrono::nanoseconds{0};
        stats_.min_latency = std::chrono::nanoseconds::max();
        stats_.max_latency = std::chrono::nanoseconds{0};
    }

private:
    // Internal stats with atomics
    struct InternalStats {
        std::atomic<size_t> requests_processed{0};
        std::atomic<size_t> requests_failed{0};
        std::atomic<size_t> bytes_received{0};
        std::atomic<size_t> bytes_sent{0};
        std::chrono::nanoseconds total_processing_time{0};
        std::chrono::nanoseconds min_latency{std::chrono::nanoseconds::max()};
        std::chrono::nanoseconds max_latency{0};
    };
    [[nodiscard]] boost::asio::awaitable<Result<Message>> 
    read_message(boost::asio::local::stream_protocol::socket& socket,
                 FrameReader& reader);

    [[nodiscard]] boost::asio::awaitable<Result<void>> 
    write_message(boost::asio::local::stream_protocol::socket& socket,
                  const Message& message);

    // Streaming write operations
    [[nodiscard]] boost::asio::awaitable<Result<void>> 
    write_header_frame(boost::asio::local::stream_protocol::socket& socket,
                       const Message& message, bool flush = true);
    [[nodiscard]] boost::asio::awaitable<Result<void>> 
    write_chunk_frame(boost::asio::local::stream_protocol::socket& socket,
                      const Message& message,
                      bool last_chunk = false, bool flush = true);

    [[nodiscard]] boost::asio::awaitable<Response> process_request(const Request& request);

    // Process request with streaming support
    [[nodiscard]] boost::asio::awaitable<std::optional<Response>> process_streaming_request(const Request& request);

    // Check if the processor can handle streaming for this request
    [[nodiscard]] bool can_stream_request(const Request& request) const;

    // Helper method to accumulate chunks for bounded memory usage
    [[nodiscard]] boost::asio::awaitable<std::vector<Response>> collect_limited_chunks(const Request& request,
                                                                     size_t max_chunks = 1000);

    // Send an error response. If request_id is provided, the error will be correlated
    // with that request; otherwise 0 indicates an out-of-band error and callers should
    // generally close the connection to avoid desynchronization in persistent mode.
    [[nodiscard]] boost::asio::awaitable<Result<void>> 
    send_error(boost::asio::local::stream_protocol::socket& socket, ErrorCode code,
               const std::string& message,
               uint64_t request_id = 0);

    std::shared_ptr<RequestProcessor> processor_;
    RequestDispatcher* dispatcher_ = nullptr;  // Alternative to processor_
    MessageFramer framer_;
    Config config_;
    mutable InternalStats stats_;
};

inline RequestHandler::Config::Config() = default;

// Request router for dispatching to specific handlers
class RequestRouter : public RequestProcessor {
public:
    using Handler = std::function<boost::asio::awaitable<Response>(const Request&)>;

    // Register handlers for specific request types
    template <typename T> void register_handler(Handler handler) {
        handlers_[typeid(T).hash_code()] = std::move(handler);
    }

    [[nodiscard]] boost::asio::awaitable<Response> process(const Request& request) override;

private:
    std::unordered_map<size_t, Handler> handlers_;

    [[nodiscard]] size_t get_request_type_hash(const Request& request) const;
};

// Middleware for request processing pipeline
class RequestMiddleware {
public:
    virtual ~RequestMiddleware() = default;

    using Next = std::function<boost::asio::awaitable<Response>(const Request&)>;

    [[nodiscard]] virtual boost::asio::awaitable<Response> process(const Request& request, Next next) = 0;
};

// Pipeline of middleware
class MiddlewarePipeline : public RequestProcessor {
public:
    void add(std::shared_ptr<RequestMiddleware> middleware) {
        middleware_.push_back(std::move(middleware));
    }

    void set_final_handler(RequestHandler::ProcessorFunc handler) {
        final_handler_ = std::move(handler);
    }

    [[nodiscard]] boost::asio::awaitable<Response> process(const Request& request) override;

private:
    std::vector<std::shared_ptr<RequestMiddleware>> middleware_;
    RequestHandler::ProcessorFunc final_handler_;
};

// Logging middleware
class LoggingMiddleware : public RequestMiddleware {
public:
    [[nodiscard]] boost::asio::awaitable<Response> process(const Request& request, Next next) override;
};

// Rate limiting middleware
class RateLimitMiddleware : public RequestMiddleware {
public:
    struct Config {
        size_t requests_per_second;
        size_t burst_size;

        Config() : requests_per_second(100), burst_size(200) {}
    };

    explicit RateLimitMiddleware(Config config = {});

    [[nodiscard]] boost::asio::awaitable<Response> process(const Request& request, Next next) override;

private:
    Config config_;
    std::atomic<size_t> tokens_;
    std::chrono::steady_clock::time_point last_refill_;
    std::mutex refill_mutex_;
};

// Authentication middleware
class AuthMiddleware : public RequestMiddleware {
public:
    using Validator = std::function<boost::asio::awaitable<bool>(const std::string&)>;

    explicit AuthMiddleware(Validator validator) : validator_(std::move(validator)) {}

    [[nodiscard]] boost::asio::awaitable<Response> process(const Request& request, Next next) override;

private:
    Validator validator_;
};

} // namespace yams::daemon
