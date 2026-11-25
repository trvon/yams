#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>

namespace yams::api {

// Progress information
struct Progress {
    uint64_t bytesProcessed = 0;
    uint64_t totalBytes = 0;
    double percentage = 0.0;
    std::chrono::seconds estimatedRemaining{0};
    std::chrono::steady_clock::time_point startTime{};
    std::string currentOperation;
    bool isCancelled = false;
};

// Progress callback type
using ProgressCallback = std::function<void(const Progress&)>;

// Progress reporter for tracking operation progress
class ProgressReporter {
public:
    ProgressReporter();
    explicit ProgressReporter(uint64_t totalBytes);
    ~ProgressReporter();

    // Delete copy, enable move
    ProgressReporter(const ProgressReporter&) = delete;
    ProgressReporter& operator=(const ProgressReporter&) = delete;
    ProgressReporter(ProgressReporter&&) noexcept;
    ProgressReporter& operator=(ProgressReporter&&) noexcept;

    // Set callback
    void setCallback(ProgressCallback callback);

    // Report progress
    void reportProgress(uint64_t processed);
    void reportProgress(uint64_t processed, const std::string& operation);
    void addProgress(uint64_t delta);

    // Get current progress
    [[nodiscard]] Progress getProgress() const;
    [[nodiscard]] uint64_t getBytesProcessed() const;
    [[nodiscard]] uint64_t getTotalBytes() const;
    [[nodiscard]] bool isComplete() const;

    // Cancellation support
    void cancel();
    [[nodiscard]] bool isCancelled() const;
    void throwIfCancelled() const;

    // Reset progress
    void reset();
    void reset(uint64_t totalBytes);

    // Create sub-reporter for a portion of work
    std::unique_ptr<ProgressReporter> createSubReporter(uint64_t subTotalBytes);

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace yams::api