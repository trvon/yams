#include <yams/api/content_store_error.h>
#include <yams/api/progress_reporter.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <unordered_map>
#include <vector>

namespace yams::api {

// ProgressReporter implementation
struct ProgressReporter::Impl {
    std::atomic<uint64_t> bytesProcessed{0};
    std::atomic<uint64_t> totalBytes{0};
    std::atomic<bool> cancelled{false};
    std::chrono::steady_clock::time_point startTime;
    mutable std::mutex mutex;
    ProgressCallback callback;
    std::string currentOperation;

    Impl() : startTime(std::chrono::steady_clock::now()) {}

    explicit Impl(uint64_t total)
        : totalBytes(total), startTime(std::chrono::steady_clock::now()) {}

    void notifyProgress() {
        if (!callback)
            return;

        Progress progress;
        progress.bytesProcessed = bytesProcessed.load();
        progress.totalBytes = totalBytes.load();
        progress.percentage = (progress.totalBytes > 0)
                                  ? (static_cast<double>(progress.bytesProcessed) /
                                     static_cast<double>(progress.totalBytes) * 100.0)
                                  : 0.0;
        progress.startTime = startTime;
        progress.isCancelled = cancelled.load();

        // Calculate estimated remaining time
        auto elapsed = std::chrono::steady_clock::now() - startTime;
        if (progress.bytesProcessed > 0 && progress.percentage > 0) {
            auto totalEstimate = elapsed / (progress.percentage / 100.0);
            progress.estimatedRemaining =
                std::chrono::duration_cast<std::chrono::seconds>(totalEstimate - elapsed);
        }

        {
            std::lock_guard lock(mutex);
            progress.currentOperation = currentOperation;
        }

        callback(progress);
    }
};

ProgressReporter::ProgressReporter() : pImpl(std::make_unique<Impl>()) {}

ProgressReporter::ProgressReporter(uint64_t totalBytes)
    : pImpl(std::make_unique<Impl>(totalBytes)) {}

ProgressReporter::~ProgressReporter() = default;

ProgressReporter::ProgressReporter(ProgressReporter&&) noexcept = default;
ProgressReporter& ProgressReporter::operator=(ProgressReporter&&) noexcept = default;

void ProgressReporter::setCallback(ProgressCallback callback) {
    std::lock_guard lock(pImpl->mutex);
    pImpl->callback = std::move(callback);
}

void ProgressReporter::reportProgress(uint64_t processed) {
    pImpl->bytesProcessed = processed;
    pImpl->notifyProgress();
}

void ProgressReporter::reportProgress(uint64_t processed, const std::string& operation) {
    {
        std::lock_guard lock(pImpl->mutex);
        pImpl->currentOperation = operation;
    }
    pImpl->bytesProcessed = processed;
    pImpl->notifyProgress();
}

void ProgressReporter::addProgress(uint64_t delta) {
    pImpl->bytesProcessed += delta;
    pImpl->notifyProgress();
}

Progress ProgressReporter::getProgress() const {
    Progress progress;
    progress.bytesProcessed = pImpl->bytesProcessed.load();
    progress.totalBytes = pImpl->totalBytes.load();
    progress.percentage = (progress.totalBytes > 0)
                              ? (static_cast<double>(progress.bytesProcessed) /
                                 static_cast<double>(progress.totalBytes) * 100.0)
                              : 0.0;
    progress.startTime = pImpl->startTime;
    progress.isCancelled = pImpl->cancelled.load();

    auto elapsed = std::chrono::steady_clock::now() - pImpl->startTime;
    if (progress.bytesProcessed > 0 && progress.percentage > 0) {
        auto totalEstimate = elapsed / (progress.percentage / 100.0);
        progress.estimatedRemaining =
            std::chrono::duration_cast<std::chrono::seconds>(totalEstimate - elapsed);
    }

    {
        std::lock_guard lock(pImpl->mutex);
        progress.currentOperation = pImpl->currentOperation;
    }

    return progress;
}

uint64_t ProgressReporter::getBytesProcessed() const {
    return pImpl->bytesProcessed.load();
}

uint64_t ProgressReporter::getTotalBytes() const {
    return pImpl->totalBytes.load();
}

bool ProgressReporter::isComplete() const {
    uint64_t total = pImpl->totalBytes.load();
    return total > 0 && pImpl->bytesProcessed.load() >= total;
}

void ProgressReporter::cancel() {
    pImpl->cancelled = true;
    pImpl->notifyProgress();
}

bool ProgressReporter::isCancelled() const {
    return pImpl->cancelled.load();
}

void ProgressReporter::throwIfCancelled() const {
    if (pImpl->cancelled.load()) {
        throw OperationCancelledException("Operation was cancelled");
    }
}

void ProgressReporter::reset() {
    pImpl->bytesProcessed = 0;
    pImpl->totalBytes = 0;
    pImpl->cancelled = false;
    pImpl->startTime = std::chrono::steady_clock::now();
    {
        std::lock_guard lock(pImpl->mutex);
        pImpl->currentOperation.clear();
    }
    pImpl->notifyProgress();
}

void ProgressReporter::reset(uint64_t totalBytes) {
    pImpl->bytesProcessed = 0;
    pImpl->totalBytes = totalBytes;
    pImpl->cancelled = false;
    pImpl->startTime = std::chrono::steady_clock::now();
    {
        std::lock_guard lock(pImpl->mutex);
        pImpl->currentOperation.clear();
    }
    pImpl->notifyProgress();
}

std::unique_ptr<ProgressReporter> ProgressReporter::createSubReporter(uint64_t subTotalBytes) {
    auto subReporter = std::make_unique<ProgressReporter>(subTotalBytes);

    // Set up callback to forward progress to parent
    auto parentPtr = this;
    subReporter->setCallback([parentPtr, subTotalBytes](const Progress& subProgress) {
        // Calculate how much of the parent's progress this sub-task represents
        uint64_t parentTotal = parentPtr->getTotalBytes();
        if (parentTotal > 0) {
            double subRatio = static_cast<double>(subTotalBytes) / static_cast<double>(parentTotal);
            uint64_t parentBytes =
                static_cast<uint64_t>(static_cast<double>(subProgress.bytesProcessed) * subRatio);
            parentPtr->addProgress(parentBytes);
        }
    });

    return subReporter;
}

} // namespace yams::api