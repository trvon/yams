#include <yams/daemon/client/asio_connection.h>

#include <boost/asio/buffer.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/error_code.hpp>

namespace yams::daemon {

using boost::asio::use_awaitable;

boost::asio::awaitable<Result<void>> AsioConnection::async_write_frame(std::vector<uint8_t> frame) {
    co_await boost::asio::dispatch(strand, use_awaitable);
    write_queue.emplace_back(std::move(frame));
    if (writing)
        co_return Result<void>();
    writing = true;
    while (!write_queue.empty()) {
        std::vector<uint8_t> batch;
        batch.reserve(write_queue.front().size());
        std::size_t batched = 0;
        auto cap = batch_cap.load(std::memory_order_relaxed);
        std::size_t frames = 0;
        while (!write_queue.empty() && batched < cap) {
            auto& f = write_queue.front();
            batched += f.size();
            frames++;
            batch.insert(batch.end(), f.begin(), f.end());
            write_queue.pop_front();
        }
        total_bytes_written.fetch_add(batched, std::memory_order_relaxed);
        total_batches.fetch_add(1, std::memory_order_relaxed);
        total_frames.fetch_add(frames, std::memory_order_relaxed);
        auto now = std::chrono::steady_clock::now();
        if (now - last_adjust > std::chrono::seconds(10)) {
            last_adjust = now;
            auto avg = total_bytes_written.load(std::memory_order_relaxed) /
                       std::max<uint64_t>(total_batches.load(std::memory_order_relaxed), 1);
            if (avg > cap && cap < (4 * 1024 * 1024)) {
                batch_cap.store(std::min<std::size_t>(cap * 2, 4 * 1024 * 1024),
                                std::memory_order_relaxed);
            } else if (avg < (cap / 4) && cap > (64 * 1024)) {
                batch_cap.store(std::max<std::size_t>(cap / 2, 64 * 1024),
                                std::memory_order_relaxed);
            }
        }
        boost::system::error_code ec;
        auto result = co_await boost::asio::async_write(
            *socket, boost::asio::buffer(batch), boost::asio::redirect_error(use_awaitable, ec));
        if (ec) {
            writing = false;
            alive = false;
            co_return Error{ErrorCode::NetworkError, ec.message()};
        }
        if (result != batched) {
            writing = false;
            alive = false;
            co_return Error{ErrorCode::NetworkError, "Short write on transport socket"};
        }
    }
    writing = false;
    co_return Result<void>();
}

} // namespace yams::daemon
