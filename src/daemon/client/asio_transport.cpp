#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/cancellation_state.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>

#include <spdlog/spdlog.h>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <thread>
#include <unordered_map>
#include <vector>
#include <yams/core/format.h>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#endif

namespace yams::daemon {

using boost::asio::as_tuple;
using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
namespace this_coro = boost::asio::this_coro;

AsioTransportAdapter::AsioTransportAdapter(const Options& opts) : opts_(opts) {
    bool metrics_on = FsmMetricsRegistry::instance().enabled();
    const char* s = std::getenv("YAMS_FSM_SNAPSHOTS");
    bool snaps_on = (s && (std::strcmp(s, "1") == 0 || std::strcmp(s, "true") == 0 ||
                           std::strcmp(s, "TRUE") == 0 || std::strcmp(s, "on") == 0 ||
                           std::strcmp(s, "ON") == 0));
    fsm_.enable_metrics(metrics_on);
    fsm_.enable_snapshots(snaps_on);
}

boost::asio::awaitable<std::shared_ptr<AsioConnection>>
AsioTransportAdapter::get_or_create_connection(const Options& opts) {
    auto pool = AsioConnectionPool::get_or_create(opts);
    co_return co_await pool->acquire();
}

awaitable<Result<std::unique_ptr<boost::asio::local::stream_protocol::socket>>>
AsioTransportAdapter::async_connect_with_timeout(const std::filesystem::path& path,
                                                 std::chrono::milliseconds timeout) {
    // Check cancellation before proceeding
    auto cs = co_await this_coro::cancellation_state;
    if (cs.cancelled() != boost::asio::cancellation_type::none) {
        co_return Error{ErrorCode::OperationCancelled, "Operation cancelled"};
    }

    auto executor = opts_.executor ? *opts_.executor
                                   : GlobalIOContext::instance().get_io_context().get_executor();
    auto socket = std::make_unique<boost::asio::local::stream_protocol::socket>(executor);
    boost::asio::local::stream_protocol::endpoint endpoint(path.string());

    // Use error_code overload to avoid exceptions on Windows for Unix sockets
    std::error_code exists_ec;
    if (!std::filesystem::exists(path, exists_ec)) {
        std::string msg = "Daemon not started (socket not found at '" + path.string() +
                          "'). Set YAMS_DAEMON_SOCKET or update config (daemon.socket_path).";
        spdlog::debug("AsioTransportAdapter preflight: {}", msg);
        co_return Error{ErrorCode::NetworkError, std::move(msg)};
    }
#ifndef _WIN32
    {
        struct stat st;
        if (::stat(path.c_str(), &st) == 0) {
            if (!S_ISSOCK(st.st_mode)) {
                std::string msg = "Path exists but is not a socket: '" + path.string() + "'";
                spdlog::debug("AsioTransportAdapter preflight: {}", msg);
                co_return Error{ErrorCode::NetworkError, std::move(msg)};
            }
        }
    }
#endif
    // Race connect against timeout using async_initiate (no experimental APIs)
    using ConnectResult = std::tuple<boost::system::error_code>;
    using RaceResult = std::variant<ConnectResult, bool>; // ConnectResult or timedOut

    auto connect_result = co_await boost::asio::async_initiate<
        decltype(use_awaitable), void(std::exception_ptr, RaceResult)>(
        [&socket, &endpoint, executor, timeout](auto handler) mutable {
            auto completed = std::make_shared<std::atomic<bool>>(false);
            auto timer = std::make_shared<boost::asio::steady_timer>(executor);
            timer->expires_after(timeout);

            using HandlerT = std::decay_t<decltype(handler)>;
            auto handlerPtr = std::make_shared<HandlerT>(std::move(handler));
            auto completion_exec = boost::asio::get_associated_executor(*handlerPtr, executor);

            timer->async_wait([completed, handlerPtr,
                               completion_exec](const boost::system::error_code& ec) mutable {
                if (ec == boost::asio::error::operation_aborted)
                    return;
                if (!completed->exchange(true, std::memory_order_acq_rel)) {
                    boost::asio::post(completion_exec, [h = std::move(*handlerPtr)]() mutable {
                        std::move(h)(std::exception_ptr{},
                                     RaceResult(std::in_place_index<1>, true));
                    });
                }
            });

            socket->async_connect(endpoint, [timer, completed, handlerPtr, completion_exec](
                                                const boost::system::error_code& ec) mutable {
                if (!completed->exchange(true, std::memory_order_acq_rel)) {
                    timer->cancel();
                    boost::asio::post(completion_exec, [h = std::move(*handlerPtr), ec]() mutable {
                        std::move(h)(std::exception_ptr{},
                                     RaceResult(std::in_place_index<0>, ConnectResult{ec}));
                    });
                }
            });
        },
        use_awaitable);

    if (connect_result.index() == 1) {
        socket->close();
        co_return Error{ErrorCode::Timeout,
                        yams::format("Connection timeout (socket='{}')", path.string())};
    }

    auto& [ec] = std::get<0>(connect_result);
    if (ec) {
        if (ec == boost::asio::error::connection_refused ||
            ec == make_error_code(boost::system::errc::connection_refused)) {
            co_return Error{ErrorCode::NetworkError,
                            yams::format("Connection refused (socket='{}'). Is the daemon running? "
                                         "Try 'yams daemon start' or verify daemon.socket_path.",
                                         path.string())};
        }
        co_return Error{ErrorCode::NetworkError,
                        yams::format("Connection failed: {}", ec.message())};
    }

    co_return std::move(socket);
}

awaitable<Result<std::vector<uint8_t>>>
AsioTransportAdapter::async_read_exact(boost::asio::local::stream_protocol::socket& socket,
                                       size_t size, std::chrono::milliseconds timeout) {
    // Check cancellation before proceeding
    auto cs = co_await this_coro::cancellation_state;
    if (cs.cancelled() != boost::asio::cancellation_type::none) {
        co_return Error{ErrorCode::OperationCancelled, "Operation cancelled"};
    }

    std::vector<uint8_t> buffer(size);
    auto executor = co_await this_coro::executor;

    // Race read against timeout using async_initiate (no experimental APIs)
    using ReadResult = std::tuple<boost::system::error_code, std::size_t>;
    using RaceResult = std::variant<ReadResult, bool>;

    auto read_result = co_await boost::asio::async_initiate<decltype(use_awaitable),
                                                            void(std::exception_ptr, RaceResult)>(
        [&socket, &buffer, executor, timeout](auto handler) mutable {
            auto completed = std::make_shared<std::atomic<bool>>(false);
            auto timer = std::make_shared<boost::asio::steady_timer>(executor);
            timer->expires_after(timeout);

            using HandlerT = std::decay_t<decltype(handler)>;
            auto handlerPtr = std::make_shared<HandlerT>(std::move(handler));
            auto completion_exec = boost::asio::get_associated_executor(*handlerPtr, executor);

            timer->async_wait([completed, handlerPtr,
                               completion_exec](const boost::system::error_code& ec) mutable {
                if (ec == boost::asio::error::operation_aborted)
                    return;
                if (!completed->exchange(true, std::memory_order_acq_rel)) {
                    boost::asio::post(completion_exec, [h = std::move(*handlerPtr)]() mutable {
                        std::move(h)(std::exception_ptr{},
                                     RaceResult(std::in_place_index<1>, true));
                    });
                }
            });

            boost::asio::async_read(
                socket, boost::asio::buffer(buffer),
                [timer, completed, handlerPtr, completion_exec](const boost::system::error_code& ec,
                                                                std::size_t bytes) mutable {
                    if (!completed->exchange(true, std::memory_order_acq_rel)) {
                        timer->cancel();
                        boost::asio::post(completion_exec, [h = std::move(*handlerPtr), ec,
                                                            bytes]() mutable {
                            std::move(h)(std::exception_ptr{},
                                         RaceResult(std::in_place_index<0>, ReadResult{ec, bytes}));
                        });
                    }
                });
        },
        use_awaitable);

    if (read_result.index() == 1) {
        co_return Error{ErrorCode::Timeout, "Read timeout"};
    }

    auto& [ec, bytes_read] = std::get<0>(read_result);
    if (ec) {
        co_return Error{ErrorCode::NetworkError, yams::format("Read failed: {}", ec.message())};
    }

    co_return buffer;
}

awaitable<Result<void>>
AsioTransportAdapter::async_write_all(boost::asio::local::stream_protocol::socket& socket,
                                      const std::vector<uint8_t>& data,
                                      std::chrono::milliseconds timeout) {
    // Check cancellation before proceeding
    auto cs = co_await this_coro::cancellation_state;
    if (cs.cancelled() != boost::asio::cancellation_type::none) {
        co_return Error{ErrorCode::OperationCancelled, "Operation cancelled"};
    }

    auto executor = co_await this_coro::executor;

    // Race write against timeout using async_initiate (no experimental APIs)
    using WriteResult = std::tuple<boost::system::error_code, std::size_t>;
    using RaceResult = std::variant<WriteResult, bool>;

    auto write_result = co_await boost::asio::async_initiate<decltype(use_awaitable),
                                                             void(std::exception_ptr, RaceResult)>(
        [&socket, &data, executor, timeout](auto handler) mutable {
            auto completed = std::make_shared<std::atomic<bool>>(false);
            auto timer = std::make_shared<boost::asio::steady_timer>(executor);
            timer->expires_after(timeout);

            using HandlerT = std::decay_t<decltype(handler)>;
            auto handlerPtr = std::make_shared<HandlerT>(std::move(handler));
            auto completion_exec = boost::asio::get_associated_executor(*handlerPtr, executor);

            timer->async_wait([completed, handlerPtr,
                               completion_exec](const boost::system::error_code& ec) mutable {
                if (ec == boost::asio::error::operation_aborted)
                    return;
                if (!completed->exchange(true, std::memory_order_acq_rel)) {
                    boost::asio::post(completion_exec, [h = std::move(*handlerPtr)]() mutable {
                        std::move(h)(std::exception_ptr{},
                                     RaceResult(std::in_place_index<1>, true));
                    });
                }
            });

            boost::asio::async_write(
                socket, boost::asio::buffer(data),
                [timer, completed, handlerPtr, completion_exec](const boost::system::error_code& ec,
                                                                std::size_t bytes) mutable {
                    if (!completed->exchange(true, std::memory_order_acq_rel)) {
                        timer->cancel();
                        boost::asio::post(completion_exec, [h = std::move(*handlerPtr), ec,
                                                            bytes]() mutable {
                            std::move(h)(std::exception_ptr{}, RaceResult(std::in_place_index<0>,
                                                                          WriteResult{ec, bytes}));
                        });
                    }
                });
        },
        use_awaitable);

    if (write_result.index() == 1) {
        co_return Error{ErrorCode::Timeout, "Write timeout"};
    }

    auto& [ec, bytes_written] = std::get<0>(write_result);
    if (ec) {
        co_return Error{ErrorCode::NetworkError, yams::format("Write failed: {}", ec.message())};
    }

    co_return Result<void>{};
}

namespace {
// Shared request-id generator for all client request paths (unary + streaming)
std::atomic<uint64_t>& request_id_counter() {
    static std::atomic<uint64_t> counter{
        static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count())};
    return counter;
}

uint64_t next_request_id() {
    return request_id_counter().fetch_add(1, std::memory_order_relaxed);
}
} // namespace

boost::asio::awaitable<Result<Response>> AsioTransportAdapter::send_request(const Request& req) {
    Request copy = req;
    co_return co_await send_request(std::move(copy));
}

boost::asio::awaitable<Result<Response>> AsioTransportAdapter::send_request(Request&& req) {
    // Check cancellation before proceeding
    auto cs = co_await this_coro::cancellation_state;
    if (cs.cancelled() != boost::asio::cancellation_type::none) {
        co_return Error{ErrorCode::OperationCancelled, "Operation cancelled"};
    }

    auto conn = co_await get_or_create_connection(opts_);
    if (!conn || !conn->alive) {
        co_return Error{ErrorCode::NetworkError, "Failed to establish connection"};
    }

    const auto req_type = getMessageType(req);

    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = next_request_id();
    msg.timestamp = std::chrono::steady_clock::now();
    msg.payload = std::move(req);
    msg.clientVersion = "yams-client-0.3.4";
    msg.expectsStreamingResponse = false;

    MessageFramer framer;
    std::vector<uint8_t> frame;
    frame.reserve(MessageFramer::HEADER_SIZE + 4096);
    auto frame_res = framer.frame_message_into(msg, frame);
    if (!frame_res) {
        conn->in_use.store(false, std::memory_order_release);
        co_return Error{ErrorCode::InvalidData, "Frame build failed"};
    }

    // Use promise/future for thread-safe one-shot response delivery
    auto response_promise = std::make_shared<AsioConnection::response_promise_t>();
    auto response_future = response_promise->get_future();

    co_await boost::asio::dispatch(conn->strand, use_awaitable);
    if (conn->handlers.size() >= conn->opts.maxInflight) {
        conn->in_use.store(false, std::memory_order_release);
        co_return Error{ErrorCode::ResourceExhausted, "Too many in-flight requests"};
    }
    {
        AsioConnection::Handler h;
        h.unary.emplace(AsioConnection::UnaryHandler{response_promise});
        conn->handlers.emplace(msg.requestId, std::move(h));
    }

    auto pool = AsioConnectionPool::get_or_create(opts_);
    co_await pool->ensure_read_loop_started(conn);

    auto frame_size = frame.size();
    spdlog::debug("AsioTransportAdapter::send_request about to write frame req_id={} type={} "
                  "socket_open={} bytes={}",
                  msg.requestId, static_cast<int>(req_type),
                  conn->socket && conn->socket->is_open(), frame_size);

    auto wres = co_await conn->async_write_frame(std::move(frame));
    if (!wres) {
        co_await boost::asio::dispatch(conn->strand, use_awaitable);
        conn->handlers.erase(msg.requestId);
        // Release connection before returning error
        conn->in_use.store(false, std::memory_order_release);
        spdlog::error("AsioTransportAdapter::send_request write failed req_id={}: {}",
                      msg.requestId, wres.error().message);
        co_return wres.error();
    }
    spdlog::debug("AsioTransportAdapter::send_request wrote frame req_id={} type={} bytes={}",
                  msg.requestId, static_cast<int>(req_type), frame_size);

    // Release connection AFTER writing frame - connection can now be reused while we wait for
    // response
    conn->in_use.store(false, std::memory_order_release);

    // Poll future with timeout (similar to ServiceManager pattern)
    using namespace std::chrono_literals;
    auto deadline = std::chrono::steady_clock::now() + opts_.requestTimeout;
    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    while (std::chrono::steady_clock::now() < deadline) {
        // Check cancellation at each iteration
        cs = co_await this_coro::cancellation_state;
        if (cs.cancelled() != boost::asio::cancellation_type::none) {
            co_await boost::asio::dispatch(conn->strand, use_awaitable);
            conn->handlers.erase(msg.requestId);
            co_return Error{ErrorCode::OperationCancelled, "Operation cancelled"};
        }

        if (response_future.wait_for(0ms) == std::future_status::ready) {
            auto result = response_future.get();
            co_return result;
        }
        timer.expires_after(10ms);
        co_await timer.async_wait(use_awaitable);
    }

    // Timeout - clean up handler
    co_await boost::asio::dispatch(conn->strand, use_awaitable);
    conn->handlers.erase(msg.requestId);
    conn->timed_out_requests.insert(msg.requestId);
    if (conn->timed_out_requests.size() > 256) {
        conn->timed_out_requests.clear();
    }
    co_return Error{ErrorCode::Timeout, "Request timeout waiting for response"};
}

boost::asio::awaitable<Result<void>>
AsioTransportAdapter::send_request_streaming(const Request& req, HeaderCallback onHeader,
                                             ChunkCallback onChunk, ErrorCallback onError,
                                             CompleteCallback onComplete) {
    // Check cancellation before proceeding
    auto cs = co_await this_coro::cancellation_state;
    if (cs.cancelled() != boost::asio::cancellation_type::none) {
        co_return Error{ErrorCode::OperationCancelled, "Operation cancelled"};
    }

    constexpr int kMaxRetries = 1;
    for (int attempt = 0; attempt <= kMaxRetries; ++attempt) {
        // Check cancellation at each retry attempt
        cs = co_await this_coro::cancellation_state;
        if (cs.cancelled() != boost::asio::cancellation_type::none) {
            co_return Error{ErrorCode::OperationCancelled, "Operation cancelled"};
        }

        auto conn = co_await get_or_create_connection(opts_);
        if (!conn || !conn->alive) {
            if (attempt < kMaxRetries) {
                spdlog::debug("Failed to establish connection, retrying (attempt {}/{})",
                              attempt + 1, kMaxRetries + 1);
                continue;
            }
            co_return Error{ErrorCode::NetworkError, "Failed to establish connection"};
        }

        Message msg;
        msg.version = PROTOCOL_VERSION;
        msg.requestId = next_request_id();
        msg.timestamp = std::chrono::steady_clock::now();
        msg.payload = req;
        msg.clientVersion = "yams-client-0.3.4";
        msg.expectsStreamingResponse = true;

        MessageFramer framer;
        std::vector<uint8_t> frame;
        frame.reserve(MessageFramer::HEADER_SIZE + 4096);
        auto frame_res = framer.frame_message_into(msg, frame);
        if (!frame_res) {
            conn->in_use.store(false, std::memory_order_release);
            co_return Error{ErrorCode::InvalidData, "Frame build failed"};
        }

        auto done_promise = std::make_shared<AsioConnection::void_promise_t>();
        auto done_future = done_promise->get_future();

        co_await boost::asio::dispatch(conn->strand, use_awaitable);
        if (conn->handlers.size() >= conn->opts.maxInflight) {
            conn->in_use.store(false, std::memory_order_release);
            co_return Error{ErrorCode::ResourceExhausted, "Too many in-flight requests"};
        }
        {
            AsioConnection::Handler h;
            h.streaming.emplace(onHeader, onChunk, onError, onComplete);
            h.streaming->done_promise = done_promise;
            conn->handlers.emplace(msg.requestId, std::move(h));
        }

        auto pool = AsioConnectionPool::get_or_create(opts_);
        co_await pool->ensure_read_loop_started(conn);

        auto wres = co_await conn->async_write_frame(std::move(frame));
        if (!wres) {
            co_await boost::asio::dispatch(conn->strand, use_awaitable);
            conn->handlers.erase(msg.requestId);
            conn->in_use.store(false, std::memory_order_release);
            co_return wres.error();
        }
        spdlog::debug("AsioTransportAdapter::send_request_streaming wrote frame req_id={} type={}",
                      msg.requestId, static_cast<int>(getMessageType(req)));

        conn->in_use.store(false, std::memory_order_release);

        using namespace std::chrono_literals;
        auto deadline = std::chrono::steady_clock::now() + opts_.requestTimeout;
        boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

        while (std::chrono::steady_clock::now() < deadline) {
            // Check cancellation at each iteration
            cs = co_await this_coro::cancellation_state;
            if (cs.cancelled() != boost::asio::cancellation_type::none) {
                co_await boost::asio::dispatch(conn->strand, use_awaitable);
                conn->handlers.erase(msg.requestId);
                co_return Error{ErrorCode::OperationCancelled, "Operation cancelled"};
            }

            if (done_future.wait_for(0ms) == std::future_status::ready) {
                auto result = done_future.get();

                if (!result && attempt < kMaxRetries) {
                    const auto& err_msg = result.error().message;
                    bool is_eof_error = err_msg.find("End of file") != std::string::npos ||
                                        err_msg.find("Connection closed") != std::string::npos;

                    if (is_eof_error) {
                        spdlog::debug("Connection error detected, retrying with fresh connection");
                        break;
                    }
                }

                co_return result;
            }
            timer.expires_after(10ms);
            co_await timer.async_wait(use_awaitable);
        }

        if (std::chrono::steady_clock::now() >= deadline) {
            co_await boost::asio::dispatch(conn->strand, use_awaitable);
            conn->handlers.erase(msg.requestId);
            conn->timed_out_requests.insert(msg.requestId);
            if (conn->timed_out_requests.size() > 256) {
                conn->timed_out_requests.clear();
            }
            co_return Error{ErrorCode::Timeout, "Streaming request timeout"};
        }
    }

    co_return Error{ErrorCode::NetworkError, "Failed after retry"};
}

} // namespace yams::daemon
