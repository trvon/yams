#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>

#include <yams/core/types.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/client/transport_options.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::daemon {

class ConnectionRegistry {
public:
    static ConnectionRegistry& instance();
    void add(std::weak_ptr<struct AsioConnection> conn);
    void closeAll();

private:
    std::mutex mutex_;
    std::vector<std::weak_ptr<struct AsioConnection>> connections_;
};

struct AsioConnection {
    using socket_t = boost::asio::local::stream_protocol::socket;
    // Use std::promise/std::future for thread-safe one-shot response delivery
    // Replaces experimental channels that had data race issues
    using response_promise_t = std::promise<Result<Response>>;
    using void_promise_t = std::promise<Result<void>>;

    explicit AsioConnection(const TransportOptions& o)
        : opts(o),
          strand(o.executor ? *o.executor
                            : GlobalIOContext::instance().get_io_context().get_executor()) {}

    ~AsioConnection() {
        alive.store(false, std::memory_order_release);
        if (socket) {
            (void)socket.release();
        }
    }

    void close() {
        alive.store(false, std::memory_order_release);
        if (socket) {
            if (socket->is_open()) {
                boost::system::error_code ec;
                socket->close(ec);
            }
            socket.reset();
        }
    }

    void cancel() {
        alive.store(false, std::memory_order_release);
        cancel_signal.emit(boost::asio::cancellation_type::terminal);
        if (socket && socket->is_open()) {
            boost::system::error_code ec;
            socket->cancel(ec);
            socket->close(ec);
        }
    }

    // Get cancellation slot for binding to async operations
    boost::asio::cancellation_slot cancellation_slot() { return cancel_signal.slot(); }

    TransportOptions opts;
    boost::asio::strand<boost::asio::any_io_executor> strand;
    boost::asio::cancellation_signal cancel_signal;
    std::unique_ptr<socket_t> socket;
    std::atomic<bool> read_started{false};
    std::atomic<bool> alive{false};
    std::atomic<bool> streaming_started{false};
    std::atomic<bool> in_use{false};
    std::future<void> read_loop_future;

    struct UnaryHandler {
        std::shared_ptr<response_promise_t> promise;
        std::shared_ptr<boost::asio::steady_timer> notify_timer; // Cancelled when response arrives
    };
    struct StreamingHandler {
        using HeaderCallback = std::function<void(const Response&)>;
        using ChunkCallback = std::function<bool(const Response&, bool)>;
        using ErrorCallback = std::function<void(const Error&)>;
        using CompleteCallback = std::function<void()>;

        HeaderCallback onHeader;
        ChunkCallback onChunk;
        ErrorCallback onError;
        CompleteCallback onComplete;
        std::shared_ptr<void_promise_t> done_promise;
        std::shared_ptr<boost::asio::steady_timer> notify_timer; // Cancelled when done

        StreamingHandler() = default;
        StreamingHandler(HeaderCallback h, ChunkCallback c, ErrorCallback e, CompleteCallback comp)
            : onHeader(std::move(h)), onChunk(std::move(c)), onError(std::move(e)),
              onComplete(std::move(comp)) {}
    };
    struct Handler {
        std::optional<UnaryHandler> unary;
        std::optional<StreamingHandler> streaming;
    };

    std::unordered_map<uint64_t, Handler> handlers;
    std::deque<std::vector<uint8_t>> write_queue;
    bool writing{false};
    std::atomic<uint64_t> total_bytes_written{0};
    std::atomic<uint64_t> total_batches{0};
    std::atomic<uint64_t> total_frames{0};
    std::atomic<size_t> peak_handlers{0};
    std::atomic<size_t> batch_cap{256 * 1024};
    std::chrono::steady_clock::time_point last_adjust{std::chrono::steady_clock::now()};

    boost::asio::awaitable<Result<void>> async_write_frame(std::vector<uint8_t> frame);
};

} // namespace yams::daemon
