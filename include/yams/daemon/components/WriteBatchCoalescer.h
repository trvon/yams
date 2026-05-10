#pragma once

#include <yams/daemon/components/WriteCoordinator.h>

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
            if (!batch_) {
                batch_ = std::make_unique<WriteBatch>();
                batch_->source = source_;
            }
            batch_->ops.push_back(std::move(op));
            if (batch_->ops.size() >= threshold_) {
                toFlush = std::move(batch_);
            }
        }
        if (toFlush) {
            wc->tryEnqueue(std::move(toFlush));
        }
    }

    void flush(WriteCoordinator* wc) {
        std::unique_ptr<WriteBatch> b;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            b = std::move(batch_);
        }
        if (b && !b->ops.empty()) {
            wc->tryEnqueue(std::move(b));
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
