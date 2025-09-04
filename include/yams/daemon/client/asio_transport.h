#pragma once

#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <yams/core/task.h>
#include <yams/core/types.h>
#include <yams/daemon/ipc/connection_fsm.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

namespace yams::daemon {

// A minimal transport that uses native boost::asio to send framed requests
// and receive framed responses (unary and streaming).
class AsioTransportAdapter {
public:
    // Multiplexing: per-socket connection shared across requests
    struct Connection;
    struct Options {
        std::filesystem::path socketPath;
        std::chrono::milliseconds headerTimeout{30000};
        std::chrono::milliseconds bodyTimeout{60000};
        std::chrono::milliseconds requestTimeout{5000};
        std::size_t maxInflight{128};
    };

    explicit AsioTransportAdapter(const Options& opts);

    Task<Result<Response>> send_request(const Request& req);

    using HeaderCallback = std::function<void(const Response&)>;
    using ChunkCallback = std::function<bool(const Response&, bool)>;
    using ErrorCallback = std::function<void(const Error&)>;
    using CompleteCallback = std::function<void()>;

    Task<Result<void>> send_request_streaming(const Request& req, HeaderCallback onHeader,
                                              ChunkCallback onChunk, ErrorCallback onError,
                                              CompleteCallback onComplete);

public:
    // Toggle FSM metrics and snapshot logging for transport observability
    void enableFsmMetrics(bool on) noexcept { fsm_.enable_metrics(on); }
    void enableFsmSnapshots(bool on) noexcept { fsm_.enable_snapshots(on); }
    void debugDumpFsmSnapshots(std::size_t maxEntries = 10) const noexcept {
        fsm_.debug_dump_snapshots(maxEntries);
    }
    ConnectionFsm::State fsmState() const noexcept { return fsm_.state(); }

private:
    // Multiplexing: per-socket connection shared across requests
    static std::shared_ptr<Connection> get_or_create_connection(const Options& opts);

    // Helper to connect with timeout
    boost::asio::awaitable<Result<std::unique_ptr<boost::asio::local::stream_protocol::socket>>>
    async_connect_with_timeout(const std::filesystem::path& path,
                               std::chrono::milliseconds timeout);

    // Helper to read exact number of bytes with timeout
    boost::asio::awaitable<Result<std::vector<uint8_t>>>
    async_read_exact(boost::asio::local::stream_protocol::socket& socket, size_t size,
                     std::chrono::milliseconds timeout);

    // Helper to write all data with timeout
    boost::asio::awaitable<Result<void>>
    async_write_all(boost::asio::local::stream_protocol::socket& socket,
                    const std::vector<uint8_t>& data, std::chrono::milliseconds timeout);

    // Unified receive loop for both non-chunked and chunked responses
    boost::asio::awaitable<Result<void>>
    receive_frames(boost::asio::local::stream_protocol::socket& socket, MessageFramer& framer,
                   HeaderCallback onHeader, ChunkCallback onChunk, ErrorCallback onError,
                   CompleteCallback onComplete);

    Options opts_;
    ConnectionFsm fsm_;
};

} // namespace yams::daemon
