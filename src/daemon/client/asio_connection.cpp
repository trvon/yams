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

boost::asio::awaitable<Result<void>> AsioConnection::async_write_frame(std::vector<uint8_t> frame) {
    // Check cancellation before proceeding
    auto cs = co_await this_coro::cancellation_state;
    if (cs.cancelled() != boost::asio::cancellation_type::none) {
        co_return Error{ErrorCode::OperationCancelled, "Operation cancelled"};
    }

    co_await boost::asio::dispatch(strand, use_awaitable);
    write_queue.emplace_back(std::move(frame));
    if (writing)
        co_return Result<void>();
    writing = true;
    while (!write_queue.empty()) {
        // Check cancellation at each iteration
        cs = co_await this_coro::cancellation_state;
        if (cs.cancelled() != boost::asio::cancellation_type::none) {
            writing = false;
            co_return Error{ErrorCode::OperationCancelled, "Operation cancelled"};
        }
        auto cap = batch_cap.load(std::memory_order_relaxed);
        std::size_t batched = 0;
        std::size_t frames = 0;
        std::vector<std::vector<uint8_t>> frames_batch;
        std::vector<boost::asio::const_buffer> buffers;
        frames_batch.reserve(write_queue.size());
        buffers.reserve(write_queue.size());
        while (!write_queue.empty() && (batched < cap || frames == 0)) {
            auto frame_data = std::move(write_queue.front());
            write_queue.pop_front();
            batched += frame_data.size();
            frames_batch.emplace_back(std::move(frame_data));
            auto& stored = frames_batch.back();
            buffers.emplace_back(boost::asio::buffer(stored));
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
        auto executor = co_await this_coro::executor;
        auto timeout = opts.requestTimeout;

        using WriteResult = std::tuple<boost::system::error_code, std::size_t>;
        using RaceResult = std::variant<WriteResult, bool>; // WriteResult or timedOut

        auto write_result = co_await boost::asio::async_initiate<
            decltype(use_awaitable), void(std::exception_ptr, RaceResult)>(
            [this, &buffers, executor, timeout](auto handler) mutable {
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
                    *socket, buffers,
                    [timer, completed, handlerPtr, completion_exec](
                        const boost::system::error_code& ec, std::size_t bytes) mutable {
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
            writing = false;
            alive = false;
            boost::system::error_code close_ec;
            socket->close(close_ec);
            co_return Error{ErrorCode::Timeout, "Write timeout"};
        }

        auto& [write_ec, result] = std::get<0>(write_result);
        if (write_ec) {
            spdlog::error("AsioConnection::async_write_frame: write error: {}", write_ec.message());
            writing = false;
            alive = false;
            co_return Error{ErrorCode::NetworkError, write_ec.message()};
        }
        if (result != batched) {
            spdlog::error("AsioConnection::async_write_frame: short write {} != {}", result,
                          batched);
            writing = false;
            alive = false;
            co_return Error{ErrorCode::NetworkError, "Short write on transport socket"};
        }
        spdlog::debug("AsioConnection::async_write_frame: successfully wrote {} bytes", result);
    }
    writing = false;
    co_return Result<void>();
}

} // namespace yams::daemon
