#include <yams/daemon/ipc/message_serializer.h>
#include <yams/daemon/ipc/request_handler.h>

#include <spdlog/spdlog.h>
#include <array>
#include <chrono>

namespace yams::daemon {

// ============================================================================
// RequestHandler Implementation
// ============================================================================

RequestHandler::RequestHandler(std::shared_ptr<RequestProcessor> processor)
    : processor_(std::move(processor)) {}

RequestHandler::~RequestHandler() = default;

Task<void> RequestHandler::handle_connection(AsyncSocket socket, std::stop_token token) {
    try {
        spdlog::debug("RequestHandler::handle_connection coroutine started");
        spdlog::debug("New connection established");
        // Note: downstream clients may close early (e.g., pager exits). Treat write-side resets
        // as non-fatal for the daemon; logs will be at debug level when detected.

        FrameReader reader(1024 * 1024); // 1MB max frame size
        bool should_exit = false;

        while (!token.stop_requested() && socket.is_valid()) {
            // Read as much as is available (up to 4096), do not block for exact size
            std::array<uint8_t, 4096> buf{};
            spdlog::debug("About to co_await socket.async_read");
            auto read_bytes = co_await socket.async_read(buf.data(), buf.size());
            spdlog::debug("socket.async_read returned with result={}", read_bytes.has_value());
            if (!read_bytes) {
                if (read_bytes.error().code == ErrorCode::NetworkError) {
                    spdlog::debug("Connection closed or network error from peer");
                } else {
                    spdlog::error("Read error: {}", read_bytes.error().message);
                }
                break;
            }

            if (read_bytes.value() == 0) {
                // Transient zero-byte read can occur with non-blocking IO; peek for real EOF
                std::array<uint8_t, 1> peek{};
                auto try_peek = co_await socket.async_read(peek.data(), peek.size());
                if (!try_peek) {
                    spdlog::debug("Zero-byte read followed by error: {}", try_peek.error().message);
                    break;
                }
                if (try_peek.value() == 0) {
                    spdlog::debug("Connection closed (EOF confirmed)");
                    break;
                }
                // We read 1 byte; feed it first, then continue with main buffer below
                spdlog::debug("Recovered from transient zero-byte read; feeding 1-byte peek");
                auto [consumed_peek, status_peek] = reader.feed(peek.data(), try_peek.value());
                stats_.bytes_received += consumed_peek;
                if (status_peek == FrameReader::FrameStatus::InvalidFrame) {
                    spdlog::error("Invalid frame received (after peek)");
                    co_await send_error(socket, ErrorCode::InvalidArgument, "Invalid frame format");
                    break;
                }
                if (status_peek == FrameReader::FrameStatus::FrameTooLarge) {
                    spdlog::error("Frame too large (after peek)");
                    co_await send_error(socket, ErrorCode::InvalidArgument,
                                        "Frame exceeds maximum size");
                    break;
                }
                // After handling peeked byte(s), continue loop to read more normally
                continue;
            }

            // Feed data to frame reader
            spdlog::debug("Feeding {} bytes to frame reader", read_bytes.value());
            auto [consumed, status] = reader.feed(buf.data(), read_bytes.value());
            stats_.bytes_received += consumed;
            spdlog::debug("Frame reader consumed {} bytes, status={}", consumed,
                          static_cast<int>(status));

            if (status == FrameReader::FrameStatus::InvalidFrame) {
                spdlog::error("Invalid frame received");
                co_await send_error(socket, ErrorCode::InvalidArgument, "Invalid frame format");
                break;
            }

            if (status == FrameReader::FrameStatus::FrameTooLarge) {
                spdlog::error("Frame too large");
                co_await send_error(socket, ErrorCode::InvalidArgument,
                                    "Frame exceeds maximum size");
                break;
            }

            // Process complete frames
            while (reader.has_frame()) {
                auto frame_result = reader.get_frame();
                if (!frame_result) {
                    spdlog::error("Failed to get frame: {}", frame_result.error().message);
                    continue;
                }

                // Parse the frame
                auto message_result = framer_.parse_frame(frame_result.value());
                if (!message_result) {
                    spdlog::error("Failed to parse frame: {}", message_result.error().message);
                    co_await send_error(socket, ErrorCode::SerializationError,
                                        "Failed to parse message");
                    continue;
                }

                // Extract request from message
                const auto& message = message_result.value();
                auto* request_ptr = std::get_if<Request>(&message.payload);
                if (!request_ptr) {
                    spdlog::error("Received non-request message");
                    co_await send_error(socket, ErrorCode::InvalidArgument,
                                        "Expected request message");
                    continue;
                }

                // Handle the request with correlation id
                auto handle_result =
                    co_await handle_request(socket, *request_ptr, message.requestId);
                if (!handle_result) {
                    // Downgrade common client-initiated close errors to debug to avoid noisy logs
                    const auto& msg = handle_result.error().message;
                    if (msg.find("Connection reset by peer") != std::string::npos ||
                        msg.find("Broken pipe") != std::string::npos ||
                        msg.find("EPIPE") != std::string::npos ||
                        msg.find("ECONNRESET") != std::string::npos) {
                        spdlog::debug("Request handling ended by client: {}", msg);
                    } else {
                        spdlog::error("Request handling failed: {}", msg);
                    }
                    should_exit = true;
                    break;
                }
            }
            if (should_exit)
                break;
        }

        spdlog::debug("Connection handler exiting normally");
    } catch (const std::exception& e) {
        spdlog::error("RequestHandler::handle_connection unhandled exception: {}", e.what());
    } catch (...) {
        spdlog::error("RequestHandler::handle_connection unhandled unknown exception");
    }
}

Task<Result<void>> RequestHandler::handle_request(AsyncSocket& socket, const Request& request,
                                                  uint64_t request_id) {
    auto start_time = std::chrono::steady_clock::now();

    try {
        // Process the request
        Response response = co_await process_request(request);

        // Create message envelope for response (preserve requestId for correlation)
        Message response_msg;
        response_msg.version = PROTOCOL_VERSION;
        response_msg.requestId = request_id;
        response_msg.timestamp = std::chrono::steady_clock::now();
        response_msg.payload = response;

        // Send response
        auto write_result = co_await write_message(socket, response_msg);

        auto duration = std::chrono::steady_clock::now() - start_time;
        stats_.requests_processed++;
        stats_.total_processing_time += duration;

        if (duration < stats_.min_latency) {
            stats_.min_latency = duration;
        }
        if (duration > stats_.max_latency) {
            stats_.max_latency = duration;
        }

        co_return write_result;

    } catch (const std::exception& e) {
        stats_.requests_failed++;
        spdlog::error("Exception handling request: {}", e.what());
        co_return Error{ErrorCode::InternalError, e.what()};
    }
}

Task<Result<Message>> RequestHandler::read_message(AsyncSocket& socket, FrameReader& reader) {
    // Read until we have a complete frame
    while (!reader.has_frame()) {
        auto read_result = co_await socket.async_read_exact(4096);

        if (!read_result) {
            co_return read_result.error();
        }

        auto& data = read_result.value();
        if (data.empty()) {
            co_return Error{ErrorCode::NetworkError, "Connection closed"};
        }

        auto [consumed, status] = reader.feed(data.data(), data.size());
        stats_.bytes_received += consumed;

        if (status == FrameReader::FrameStatus::InvalidFrame) {
            co_return Error{ErrorCode::InvalidArgument, "Invalid frame"};
        }

        if (status == FrameReader::FrameStatus::FrameTooLarge) {
            co_return Error{ErrorCode::InvalidArgument, "Frame too large"};
        }
    }

    auto frame_result = reader.get_frame();
    if (!frame_result) {
        co_return frame_result.error();
    }

    co_return framer_.parse_frame(frame_result.value());
}

Task<Result<void>> RequestHandler::write_message(AsyncSocket& socket, const Message& message) {
    auto frame_result = framer_.frame_message(message);
    if (!frame_result) {
        co_return frame_result.error();
    }

    auto& frame = frame_result.value();
    stats_.bytes_sent += frame.size();

    // Write in smaller chunks to reduce partial send issues and handle EPIPE better.
    // Additionally, when EAGAIN is observed from the async write path (reported as 0 bytes),
    // briefly yield/sleep to provide backpressure and avoid busy-spinning.
    constexpr std::size_t kChunk = 16 * 1024;
    std::size_t offset = 0;
    while (offset < frame.size()) {
        std::size_t to_write = std::min<std::size_t>(kChunk, frame.size() - offset);
        std::span<const uint8_t> chunk{frame.data() + offset, to_write};
        auto r = co_await socket.async_write_all(chunk);
        if (!r) {
            // Downgrade common client-initiated close errors to debug
            const auto& msg = r.error().message;
            if (msg.find("Connection reset by peer") != std::string::npos ||
                msg.find("Broken pipe") != std::string::npos ||
                msg.find("EPIPE") != std::string::npos ||
                msg.find("ECONNRESET") != std::string::npos) {
                spdlog::debug("Client closed during write: {}", msg);
            }
            co_return r.error();
        }
        // Backpressure: if the async write loop indicated a non-progress scenario earlier
        // (e.g., returned 0 from internal EAGAIN handling), apply a tiny delay.
        if (to_write == 0) {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(1ms);
        }
        offset += to_write;
    }
    co_return Result<void>();
}

Task<Response> RequestHandler::process_request(const Request& request) {
    if (processor_) {
        co_return co_await processor_->process(request);
    }

    // Default: not implemented
    co_return ErrorResponse{ErrorCode::NotImplemented, "No processor configured"};
}

Task<Result<void>> RequestHandler::send_error(AsyncSocket& socket, ErrorCode code,
                                              const std::string& message) {
    ErrorResponse error{code, message};

    // Create message envelope
    Message error_msg;
    error_msg.version = PROTOCOL_VERSION;
    error_msg.requestId = 0;
    error_msg.timestamp = std::chrono::steady_clock::now();
    error_msg.payload = Response{error};

    co_return co_await write_message(socket, error_msg);
}

// ============================================================================
// RequestRouter Implementation
// ============================================================================

Task<Response> RequestRouter::process(const Request& request) {
    auto type_hash = get_request_type_hash(request);

    auto it = handlers_.find(type_hash);
    if (it == handlers_.end()) {
        co_return ErrorResponse{ErrorCode::NotImplemented,
                                "No handler registered for request type"};
    }

    co_return co_await it->second(request);
}

size_t RequestRouter::get_request_type_hash(const Request& request) const {
    return std::visit([](const auto& req) { return typeid(req).hash_code(); }, request);
}

// ============================================================================
// MiddlewarePipeline Implementation
// ============================================================================

Task<Response> MiddlewarePipeline::process(const Request& request) {
    if (middleware_.empty()) {
        if (final_handler_) {
            co_return co_await final_handler_(request);
        }
        co_return ErrorResponse{ErrorCode::NotImplemented, "No handler configured"};
    }

    // Build the middleware chain
    std::function<Task<Response>(const Request&)> chain = final_handler_;

    for (auto it = middleware_.rbegin(); it != middleware_.rend(); ++it) {
        auto middleware = *it;
        auto next = chain;
        chain = [middleware, next](const Request& req) -> Task<Response> {
            co_return co_await middleware->process(req, next);
        };
    }

    co_return co_await chain(request);
}

// ============================================================================
// LoggingMiddleware Implementation
// ============================================================================

Task<Response> LoggingMiddleware::process(const Request& request, Next next) {
    auto start_time = std::chrono::steady_clock::now();

    // Log request
    spdlog::debug("Processing request type: {}",
                  std::visit([](const auto& req) { return typeid(req).name(); }, request));

    // Call next handler
    Response response = co_await next(request);

    // Log response
    auto duration = std::chrono::steady_clock::now() - start_time;
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

    bool is_error = std::holds_alternative<ErrorResponse>(response);
    if (is_error) {
        auto& error = std::get<ErrorResponse>(response);
        spdlog::warn("Request failed: {} ({}ms)", error.message, duration_ms);
    } else {
        spdlog::debug("Request completed successfully ({}ms)", duration_ms);
    }

    co_return response;
}

// ============================================================================
// RateLimitMiddleware Implementation
// ============================================================================

RateLimitMiddleware::RateLimitMiddleware(Config config)
    : config_(config), tokens_(config.burst_size), last_refill_(std::chrono::steady_clock::now()) {}

Task<Response> RateLimitMiddleware::process(const Request& request, Next next) {
    // Refill tokens based on time elapsed
    {
        std::lock_guard lock(refill_mutex_);
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_refill_).count();

        if (elapsed > 0) {
            size_t new_tokens = elapsed * config_.requests_per_second;
            size_t current = tokens_.load();
            size_t max_tokens = config_.burst_size;

            if (current + new_tokens > max_tokens) {
                tokens_ = max_tokens;
            } else {
                tokens_ += new_tokens;
            }

            last_refill_ = now;
        }
    }

    // Try to consume a token
    size_t current = tokens_.load();
    while (current > 0) {
        if (tokens_.compare_exchange_weak(current, current - 1)) {
            // Token acquired, process request
            co_return co_await next(request);
        }
        // current was updated by compare_exchange, retry
    }

    // No tokens available
    co_return ErrorResponse{ErrorCode::RateLimited, "Rate limit exceeded"};
}

// ============================================================================
// AuthMiddleware Implementation
// ============================================================================

Task<Response> AuthMiddleware::process(const Request& request, Next next) {
    // Extract auth token from request metadata (placeholder)
    std::string token = ""; // TODO: Extract from request

    bool is_valid = co_await validator_(token);

    if (!is_valid) {
        co_return ErrorResponse{ErrorCode::Unauthorized, "Authentication failed"};
    }

    co_return co_await next(request);
}

} // namespace yams::daemon
