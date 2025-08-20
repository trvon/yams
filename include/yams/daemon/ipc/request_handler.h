#pragma once

#include <yams/core/types.h>
#include <yams/daemon/ipc/async_socket.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

#include <functional>
#include <memory>
#include <stop_token>
#include <unordered_map>

namespace yams::daemon {

// Request processor interface
class RequestProcessor {
public:
    virtual ~RequestProcessor() = default;

    [[nodiscard]] virtual Task<Response> process(const Request& request) = 0;
};

// Coroutine-based request handler
class RequestHandler {
public:
    using ProcessorFunc = std::function<Task<Response>(const Request&)>;

    explicit RequestHandler(std::shared_ptr<RequestProcessor> processor = nullptr);
    ~RequestHandler();

    // Set the request processor
    void set_processor(std::shared_ptr<RequestProcessor> processor) {
        processor_ = std::move(processor);
    }

    // Handle a connection with coroutines
    [[nodiscard]] Task<void> handle_connection(AsyncSocket socket, std::stop_token token);

    // Handle a single request-response cycle
    [[nodiscard]] Task<Result<void>> handle_request(AsyncSocket& socket, const Request& request,
                                                    uint64_t request_id);

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
    [[nodiscard]] Task<Result<Message>> read_message(AsyncSocket& socket, FrameReader& reader);

    [[nodiscard]] Task<Result<void>> write_message(AsyncSocket& socket, const Message& message);

    [[nodiscard]] Task<Response> process_request(const Request& request);

    [[nodiscard]] Task<Result<void>> send_error(AsyncSocket& socket, ErrorCode code,
                                                const std::string& message);

    std::shared_ptr<RequestProcessor> processor_;
    MessageFramer framer_;
    mutable InternalStats stats_;
};

// Request router for dispatching to specific handlers
class RequestRouter : public RequestProcessor {
public:
    using Handler = std::function<Task<Response>(const Request&)>;

    // Register handlers for specific request types
    template <typename T> void register_handler(Handler handler) {
        handlers_[typeid(T).hash_code()] = std::move(handler);
    }

    [[nodiscard]] Task<Response> process(const Request& request) override;

private:
    std::unordered_map<size_t, Handler> handlers_;

    [[nodiscard]] size_t get_request_type_hash(const Request& request) const;
};

// Middleware for request processing pipeline
class RequestMiddleware {
public:
    virtual ~RequestMiddleware() = default;

    using Next = std::function<Task<Response>(const Request&)>;

    [[nodiscard]] virtual Task<Response> process(const Request& request, Next next) = 0;
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

    [[nodiscard]] Task<Response> process(const Request& request) override;

private:
    std::vector<std::shared_ptr<RequestMiddleware>> middleware_;
    RequestHandler::ProcessorFunc final_handler_;
};

// Logging middleware
class LoggingMiddleware : public RequestMiddleware {
public:
    [[nodiscard]] Task<Response> process(const Request& request, Next next) override;
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

    [[nodiscard]] Task<Response> process(const Request& request, Next next) override;

private:
    Config config_;
    std::atomic<size_t> tokens_;
    std::chrono::steady_clock::time_point last_refill_;
    std::mutex refill_mutex_;
};

// Authentication middleware
class AuthMiddleware : public RequestMiddleware {
public:
    using Validator = std::function<Task<bool>(const std::string&)>;

    explicit AuthMiddleware(Validator validator) : validator_(std::move(validator)) {}

    [[nodiscard]] Task<Response> process(const Request& request, Next next) override;

private:
    Validator validator_;
};

} // namespace yams::daemon
