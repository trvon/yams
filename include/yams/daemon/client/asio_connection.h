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
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/strand.hpp>

#include <yams/core/types.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/client/transport_options.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::daemon {

struct AsioConnection {
    using socket_t = boost::asio::local::stream_protocol::socket;
    // Use std::mutex in channel_traits for thread-safe access across multiple threads
    // Template params: <Executor, Traits, Signatures...>
    using response_channel_t = boost::asio::experimental::basic_channel<
        boost::asio::any_io_executor, boost::asio::experimental::channel_traits<std::mutex>,
        void(boost::system::error_code, std::shared_ptr<Result<Response>>)>;
    using void_channel_t = boost::asio::experimental::basic_channel<
        boost::asio::any_io_executor, boost::asio::experimental::channel_traits<std::mutex>,
        void(boost::system::error_code, Result<void>)>;
    explicit AsioConnection(const TransportOptions& o)
        : opts(o),
          strand(o.executor ? *o.executor
                            : GlobalIOContext::instance().get_io_context().get_executor()) {}

    TransportOptions opts;
    boost::asio::strand<boost::asio::any_io_executor> strand;
    std::unique_ptr<socket_t> socket;
    std::atomic<bool> read_started{false};
    std::atomic<bool> alive{false};
    std::atomic<bool> streaming_started{false};
    std::atomic<bool> in_use{false};
    std::future<void> read_loop_future;

    struct UnaryHandler {
        std::shared_ptr<response_channel_t> channel;
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
        std::shared_ptr<void_channel_t> done_channel;

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
