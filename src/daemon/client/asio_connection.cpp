#include <yams/daemon/client/asio_connection.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/cancellation_state.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/error_code.hpp>

#include <spdlog/spdlog.h>

namespace yams::daemon {

using boost::asio::as_tuple;
using boost::asio::use_awaitable;
namespace this_coro = boost::asio::this_coro;

namespace {
constexpr auto kSocketCloseDispatchWait = std::chrono::milliseconds{500};

bool isExpectedDisconnectError(const boost::system::error_code& ec) {
    if (ec == boost::asio::error::broken_pipe || ec == boost::asio::error::connection_reset ||
        ec == boost::asio::error::eof) {
        return true;
    }

    const auto msg = ec.message();
    return msg.find("Broken pipe") != std::string::npos ||
           msg.find("Connection reset") != std::string::npos ||
           msg.find("End of file") != std::string::npos || msg.find("EPIPE") != std::string::npos ||
           msg.find("ECONNRESET") != std::string::npos;
}

bool isExecutorStopped(const boost::asio::strand<boost::asio::any_io_executor>& executor) {
    const auto& inner = executor.get_inner_executor();
    const auto* ioExecutor = inner.target<boost::asio::io_context::executor_type>();
    return ioExecutor != nullptr && ioExecutor->context().stopped();
}
} // namespace

AsioConnection::~AsioConnection() {
    alive.store(false, std::memory_order_release);
    closeSocketOnStrand(/*cancelFirst=*/true, /*resetSocket=*/true);
}

void AsioConnection::close() {
    alive.store(false, std::memory_order_release);
    closeSocketOnStrand(/*cancelFirst=*/false, /*resetSocket=*/true);
}

void AsioConnection::cancel() {
    alive.store(false, std::memory_order_release);
    cancel_signal.emit(boost::asio::cancellation_type::terminal);
    closeSocketOnStrand(/*cancelFirst=*/true, /*resetSocket=*/false);
}

void AsioConnection::closeSocketOnStrand(bool cancelFirst, bool resetSocket) {
    if (!socket) {
        return;
    }

    auto closeOp = [this, cancelFirst, resetSocket] {
        if (!socket) {
            return;
        }
        boost::system::error_code ec;
        if (socket->is_open()) {
            if (cancelFirst) {
                // NOLINTNEXTLINE(bugprone-unused-return-value): error_code overload reports via ec.
                (void)socket->cancel(ec);
            }
            // NOLINTNEXTLINE(bugprone-unused-return-value): error_code overload reports via ec.
            (void)socket->close(ec);
        }
        if (resetSocket) {
            socket.reset();
        }
    };

    if (strand.running_in_this_thread()) {
        closeOp();
        return;
    }

    auto done = std::make_shared<std::promise<void>>();
    auto completed = std::make_shared<std::atomic<bool>>(false);
    auto future = done->get_future();
    boost::asio::dispatch(strand, [closeOp, done, completed]() mutable {
        if (!completed->exchange(true, std::memory_order_acq_rel)) {
            closeOp();
        }
        done->set_value();
    });

    // Socket ownership is strand-affine: pending async read/write operations also touch the
    // socket on this strand. If the backing io_context is still running, keep waiting rather than
    // racing those operations. If it is already stopped, the dispatched close cannot make progress;
    // complete exactly one close/reset best-effort off-strand so shutdown does not hang or leak.
    if (future.wait_for(kSocketCloseDispatchWait) != std::future_status::ready) {
        const bool globalExecutorStopped =
            !opts.executor.has_value() && GlobalIOContext::instance().get_io_context().stopped();
        if (isExecutorStopped(strand) || globalExecutorStopped) {
            if (!completed->exchange(true, std::memory_order_acq_rel)) {
                closeOp();
            }
            return;
        }
        future.wait();
    }
}

boost::asio::awaitable<Result<void>> AsioConnection::async_write_frame(std::vector<uint8_t> frame) {
    // Check cancellation before proceeding
    auto cs = co_await this_coro::cancellation_state;
    if (cs.cancelled() != boost::asio::cancellation_type::none) {
        co_return Error{ErrorCode::OperationCancelled, "Operation cancelled"};
    }

    // Dispatch to strand for thread-safe access to write_queue and writing flag
    co_await boost::asio::dispatch(strand, use_awaitable);
    write_queue.emplace_back(std::move(frame));
    if (writing)
        co_return Result<void>();
    writing = true;

    // We must re-acquire strand after each suspension point to maintain thread safety.
    // The while loop accesses write_queue which is shared state.
    while (true) {
        // Re-acquire strand at the start of each iteration since async_write suspends
        co_await boost::asio::dispatch(strand, use_awaitable);

        // Check if queue is empty (must be done while holding strand)
        if (write_queue.empty()) {
            writing = false;
            co_return Result<void>();
        }

        // Check cancellation at each iteration
        cs = co_await this_coro::cancellation_state;
        if (cs.cancelled() != boost::asio::cancellation_type::none) {
            writing = false;
            co_return Error{ErrorCode::OperationCancelled, "Operation cancelled"};
        }
        auto cap = batch_cap.load(std::memory_order_relaxed);
        std::size_t batched = 0;
        std::size_t frames = 0;
        auto frames_batch = std::make_shared<std::vector<std::vector<uint8_t>>>();
        auto buffers = std::make_shared<std::vector<boost::asio::const_buffer>>();
        frames_batch->reserve(write_queue.size());
        buffers->reserve(write_queue.size());
        while (!write_queue.empty() && (batched < cap || frames == 0)) {
            auto frame_data = std::move(write_queue.front());
            write_queue.pop_front();
            batched += frame_data.size();
            frames_batch->emplace_back(std::move(frame_data));
            auto& stored = frames_batch->back();
            buffers->emplace_back(boost::asio::buffer(stored));
            frames++;
        }
        total_bytes_written.fetch_add(batched, std::memory_order_relaxed);
        total_batches.fetch_add(1, std::memory_order_relaxed);
        total_frames.fetch_add(frames, std::memory_order_relaxed);
        auto now = std::chrono::steady_clock::now();
        if (now - last_adjust > std::chrono::seconds(10)) {
            last_adjust = now;
            auto avg = total_bytes_written.load(std::memory_order_relaxed) /
                       std::max<uint64_t>(total_batches.load(std::memory_order_relaxed), 1);
            if (avg > cap && cap < (4ULL * 1024 * 1024)) {
                batch_cap.store(std::min<std::size_t>(cap * 2, 4ULL * 1024 * 1024),
                                std::memory_order_relaxed);
            } else if (avg < (cap / 4) && cap > (64ULL * 1024)) {
                batch_cap.store(std::max<std::size_t>(cap / 2, 64ULL * 1024),
                                std::memory_order_relaxed);
            }
            total_bytes_written.store(0, std::memory_order_relaxed);
            total_batches.store(0, std::memory_order_relaxed);
        }
        spdlog::debug("AsioConnection::async_write_frame: writing {} frames, {} bytes total",
                      frames, batched);

        // Race write against timeout using async_initiate (no experimental APIs)
        // Note: After this co_await, we are no longer on the strand! The next loop iteration
        // will re-acquire it before accessing write_queue.
        auto executor = co_await this_coro::executor;
        auto timeout = opts.requestTimeout;

        using WriteResult = std::tuple<boost::system::error_code, std::size_t>;
        using RaceResult = std::variant<WriteResult, bool>; // WriteResult or timedOut

        auto write_result = co_await boost::asio::async_initiate<
            decltype(use_awaitable), void(std::exception_ptr, RaceResult)>(
            [this, buffers, frames_batch, executor, timeout](auto handler) mutable {
                auto completed = std::make_shared<std::atomic<bool>>(false);
                auto timer = std::make_shared<boost::asio::steady_timer>(executor);
                timer->expires_after(timeout);

                using HandlerT = std::decay_t<decltype(handler)>;
                auto handlerPtr = std::make_shared<HandlerT>(std::move(handler));
                auto completion_exec = boost::asio::get_associated_executor(*handlerPtr, executor);

                // Timer handler
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

                // Write handler
                boost::asio::async_write(
                    *socket, *buffers,
                    [timer, completed, handlerPtr, completion_exec, buffers,
                     frames_batch](const boost::system::error_code& ec, std::size_t bytes) mutable {
                        if (!completed->exchange(true, std::memory_order_acq_rel)) {
                            timer->cancel();
                            boost::asio::post(completion_exec,
                                              [h = std::move(*handlerPtr), ec, bytes]() mutable {
                                                  std::move(h)(std::exception_ptr{},
                                                               RaceResult(std::in_place_index<0>,
                                                                          WriteResult{ec, bytes}));
                                              });
                        }
                    });
            },
            use_awaitable);

        if (write_result.index() == 1) {
            spdlog::error("AsioConnection::async_write_frame: write timeout after {}ms",
                          opts.requestTimeout.count());
            // Re-acquire strand before modifying shared state
            co_await boost::asio::dispatch(strand, use_awaitable);
            writing = false;
            alive = false;
            boost::system::error_code close_ec;
            socket->close(close_ec);
            if (close_ec) {
                spdlog::debug("AsioConnection::async_write_frame: close after timeout failed: {}",
                              close_ec.message());
            }
            co_return Error{ErrorCode::Timeout, "Write timeout"};
        }

        auto& [write_ec, result] = std::get<0>(write_result);
        if (write_ec) {
            if (isExpectedDisconnectError(write_ec)) {
                spdlog::debug(
                    "AsioConnection::async_write_frame: peer disconnected during write: {}",
                    write_ec.message());
            } else {
                spdlog::error("AsioConnection::async_write_frame: write error: {}",
                              write_ec.message());
            }
            // Re-acquire strand before modifying shared state
            co_await boost::asio::dispatch(strand, use_awaitable);
            writing = false;
            alive = false;
            co_return Error{ErrorCode::NetworkError, write_ec.message()};
        }
        if (result != batched) {
            spdlog::error("AsioConnection::async_write_frame: short write {} != {}", result,
                          batched);
            // Re-acquire strand before modifying shared state
            co_await boost::asio::dispatch(strand, use_awaitable);
            writing = false;
            alive = false;
            co_return Error{ErrorCode::NetworkError, "Short write on transport socket"};
        }
        spdlog::debug("AsioConnection::async_write_frame: successfully wrote {} bytes", result);
    }
}

} // namespace yams::daemon
