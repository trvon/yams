#pragma once

#include <spdlog/spdlog.h>
#include <functional>
#include <memory>
#include <optional>
#include <variant>
#include <yams/core/types.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/async_socket.h>
#include <yams/daemon/ipc/connection_pool.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/message_serializer.h>
#include <yams/daemon/ipc/response_of.hpp>

namespace yams::cli {

// A thread-safe, pooled manager for sending requests to the yams-daemon.
// This class is designed to be a singleton or a long-lived object within a client process (like MCP
// or CLI).
template <typename TRequest, typename TResponse = yams::daemon::ResponseOfT<TRequest>>
class PooledRequestManager {
public:
    using RenderFunc = std::function<Result<void>(const TResponse&)>;

    explicit PooledRequestManager(daemon::ConnectionPool::Config pool_config) {
        pool_ = std::make_unique<daemon::ConnectionPool>(pool_config);
        // The factory must be set by the user of this class, as it depends on the IO context.
    }

    // Configure the underlying pool with a socket factory (Unix or TCP).
    // This enables real socket pooling instead of only concurrency gating.
    void set_socket_factory(std::shared_ptr<daemon::ConnectionFactory> factory) {
        if (!pool_)
            return;
        pool_->set_factory(std::move(factory));
    }

    // Executes a request using a pooled connection via IPC framing (no fallback).
    Result<void> execute(const TRequest& req, auto&& /*fallback_unused*/, RenderFunc render) {
        if (!pool_) {
            return Error{ErrorCode::InvalidState, "Connection pool not initialized"};
        }

        // Acquire pooled connection (RAII)
        auto handle_result = pool_->try_acquire();
        if (!handle_result) {
            return handle_result.error();
        }
        auto handle = std::move(handle_result).value();

        // Build IPC message
        daemon::Message message;
        message.version = daemon::PROTOCOL_VERSION;
        message.requestId = 0; // Not used for simple request/response in this client
        message.timestamp = std::chrono::steady_clock::now();
        message.payload = daemon::Request{req};

        // Frame the message
        daemon::MessageFramer framer;
        auto frame_res = framer.frame_message(message);
        if (!frame_res) {
            return frame_res.error();
        }
        auto& frame = frame_res.value();

        // Send frame over pooled socket
        auto write_res = handle->async_write_all(frame).get();
        if (!write_res) {
            return write_res.error();
        }

        // Read a single frame back
        daemon::FrameReader reader;
        std::array<uint8_t, 2048> buf{};
        for (;;) {
            auto chunk_res = handle->async_read_exact(512).get();
            if (!chunk_res) {
                return chunk_res.error();
            }
            auto& data = chunk_res.value();
            if (data.empty()) {
                return Error{ErrorCode::NetworkError, "Connection closed by peer"};
            }
            auto feed = reader.feed(data.data(), data.size());
            if (feed.status == daemon::FrameReader::FrameStatus::InvalidFrame) {
                return Error{ErrorCode::InvalidData, "Invalid frame received"};
            }
            if (feed.status == daemon::FrameReader::FrameStatus::FrameTooLarge) {
                return Error{ErrorCode::InvalidData, "Frame too large"};
            }
            if (reader.has_frame())
                break;
        }

        auto frame_buf = reader.get_frame();
        if (!frame_buf) {
            return frame_buf.error();
        }

        // Parse the frame into a Message
        auto parsed = framer.parse_frame(frame_buf.value());
        if (!parsed) {
            return parsed.error();
        }

        // Extract Response variant
        auto* resp_ptr = std::get_if<daemon::Response>(&parsed.value().payload);
        if (!resp_ptr) {
            return Error{ErrorCode::InvalidData, "Expected response payload"};
        }

        // Extract typed response
        if (auto* ok = std::get_if<TResponse>(resp_ptr)) {
            return render(*ok);
        }
        if (auto* er = std::get_if<daemon::ErrorResponse>(resp_ptr)) {
            return Error{er->code, er->message};
        }

        return Error{ErrorCode::InvalidData, "Unexpected response type"};
    }

private:
    std::unique_ptr<daemon::ConnectionPool> pool_;
};

// Helper retained for compatibility, but uses pooled IPC path (no fallback).
template <typename TRequest, typename TResponse = yams::daemon::ResponseOfT<TRequest>>
Result<void> daemon_first_pooled(PooledRequestManager<TRequest, TResponse>& manager,
                                 const TRequest& req, auto&& /*fallback_unused*/, auto&& render) {
    return manager.execute(req, [] { return Result<void>(); }, render);
}

// Reintroduce daemon_first shim so existing CLI code compiles.
// Internally, route to pooled IPC path (no legacy DaemonClient fallback).
template <typename TRequest, typename TResponse = yams::daemon::ResponseOfT<TRequest>>
inline Result<void> daemon_first(const TRequest& req, auto&& fallback, auto&& render) {
    (void)fallback;

    // Use legacy DaemonClient (blocking) by default for CLI paths.
    yams::daemon::DaemonClient client;
    auto conn = client.connect();
    if (!conn) {
        return conn.error();
    }

    auto resp = client.call<TRequest>(req);
    if (!resp) {
        return resp.error();
    }

    return render(resp.value());
}

} // namespace yams::cli
