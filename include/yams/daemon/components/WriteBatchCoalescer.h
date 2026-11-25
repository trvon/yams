#pragma once

#include <yams/core/assert.hpp>
#include <yams/daemon/components/WriteCoordinator.h>

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

namespace yams::daemon {

namespace detail {
void logWriteBatchCoalescerWarn(std::string_view source, std::string_view message);
} // namespace detail

class WriteBatchCoalescer {
public:
    explicit WriteBatchCoalescer(std::string source, std::size_t threshold = kDefaultThreshold)
        : source_(std::move(source)), threshold_(threshold) {}

    void addOp(WriteOp op, WriteCoordinator* wc) {
        YAMS_PRECONDITION(wc != nullptr, "write batch coalescer requires a coordinator");
        addOp(std::move(op), *wc);
    }

    void addOp(WriteOp op, WriteCoordinator& wc) {
        std::unique_ptr<WriteBatch> toFlush;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (!batch_) {
                batch_ = std::make_unique<WriteBatch>();
                batch_->source = source_;
            }
            batch_->ops.push_back(std::move(op));
            if (batch_->ops.size() >= threshold_) {
                toFlush = std::move(batch_);
            }
        }
        if (toFlush && !wc.tryEnqueue(toFlush)) {
            detail::logWriteBatchCoalescerWarn(source_,
                                               "tryEnqueue rejected at threshold; retaining batch");
            std::lock_guard<std::mutex> lock(mtx_);
            batch_ = std::move(toFlush);
        }
    }

    void flush(WriteCoordinator* wc) {
        YAMS_PRECONDITION(wc != nullptr, "write batch coalescer requires a coordinator");
        flush(*wc);
    }

    void flush(WriteCoordinator& wc) {
        std::unique_ptr<WriteBatch> b;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            b = std::move(batch_);
        }
        if (b && !b->ops.empty() && !wc.tryEnqueue(b)) {
            detail::logWriteBatchCoalescerWarn(source_,
                                               "flush rejected; re-queuing batch for retry");
            std::lock_guard<std::mutex> lock(mtx_);
            if (!batch_) {
                batch_ = std::move(b);
            }
        }
    }

    static constexpr std::size_t kDefaultThreshold = 50;

private:
    std::mutex mtx_;
    std::string source_;
    std::size_t threshold_;
    std::unique_ptr<WriteBatch> batch_;
};

} // namespace yams::daemon
