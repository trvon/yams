#pragma once

#include <yams/daemon/components/WriteCoordinator.h>

#include <spdlog/spdlog.h>

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

namespace yams::daemon {

class WriteBatchCoalescer {
public:
    explicit WriteBatchCoalescer(std::string source, std::size_t threshold = kDefaultThreshold)
        : source_(std::move(source)), threshold_(threshold) {}

    void addOp(WriteOp op, WriteCoordinator* wc) {
        std::unique_ptr<WriteBatch> toFlush;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (!batch_ && !retryBatch_) {
                batch_ = std::make_unique<WriteBatch>();
                batch_->source = source_;
            }
            // If we have a retry batch from a previous failed enqueue,
            // accumulate into that to avoid reordering.
            auto* target = retryBatch_ ? &retryBatch_ : &batch_;
            (*target)->ops.push_back(std::move(op));
            if ((*target)->ops.size() >= threshold_) {
                toFlush = std::move(*target);
            }
        }
        if (toFlush) {
            enqueueOrRetain(std::move(toFlush), wc);
        }
    }

    void flush(WriteCoordinator* wc) {
        std::unique_ptr<WriteBatch> b;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            b = std::move(retryBatch_);
            if (!b) {
                b = std::move(batch_);
            }
        }
        if (b && !b->ops.empty()) {
            enqueueOrRetain(std::move(b), wc);
        }
    }

    static constexpr std::size_t kDefaultThreshold = 50;

private:
    void enqueueOrRetain(std::unique_ptr<WriteBatch> batch, WriteCoordinator* wc) {
        if (!wc->tryEnqueue(std::move(batch))) {
            // Queue is at capacity; retain the batch for retry on next
            // addOp/flush call so ops are not silently dropped.
            std::lock_guard<std::mutex> lock(mtx_);
            if (retryBatch_) {
                spdlog::warn("[WriteBatchCoalescer:{}] Queue at capacity with "
                             "pending retry batch; dropping oldest batch",
                             source_);
            }
            retryBatch_ = std::move(batch);
        } else {
            drops_ = 0;
        }
    }

    std::mutex mtx_;
    std::string source_;
    std::size_t threshold_;
    std::unique_ptr<WriteBatch> batch_;
    std::unique_ptr<WriteBatch> retryBatch_;
    int drops_{0};
};

} // namespace yams::daemon
