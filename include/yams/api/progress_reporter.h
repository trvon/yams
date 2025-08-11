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
    std::chrono::steady_clock::time_point startTime;
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
    
    // Set total bytes (if not known at construction)
    void setTotalBytes(uint64_t total);
    
    // Report progress
    void reportProgress(uint64_t processed);
    void reportProgress(uint64_t processed, const std::string& operation);
    void addProgress(uint64_t delta);
    
    // Get current progress
    [[nodiscard]] Progress getProgress() const;
    [[nodiscard]] uint64_t getBytesProcessed() const;
    [[nodiscard]] uint64_t getTotalBytes() const;
    [[nodiscard]] double getPercentage() const;
    [[nodiscard]] bool isComplete() const;
    
    // Cancellation support
    void cancel();
    [[nodiscard]] bool isCancelled() const;
    void throwIfCancelled() const;
    
    // Reset progress
    void reset();
    void reset(uint64_t totalBytes);
    
    // Estimate remaining time
    [[nodiscard]] std::chrono::seconds estimateRemaining() const;
    
    // Get elapsed time
    [[nodiscard]] std::chrono::milliseconds getElapsedTime() const;
    
    // Rate calculation
    [[nodiscard]] double getBytesPerSecond() const;
    
    // Create sub-reporter for a portion of work
    std::unique_ptr<ProgressReporter> createSubReporter(
        uint64_t subTotalBytes);
    
private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
};

// Scoped progress reporter for RAII-style progress tracking
class ScopedProgressReporter {
public:
    ScopedProgressReporter(ProgressReporter& parent, uint64_t bytes)
        : parent_(parent), bytes_(bytes), reported_(0) {}
    
    ~ScopedProgressReporter() {
        // Report any remaining progress
        if (reported_ < bytes_) {
            parent_.addProgress(bytes_ - reported_);
        }
    }
    
    // Delete copy and move
    ScopedProgressReporter(const ScopedProgressReporter&) = delete;
    ScopedProgressReporter& operator=(const ScopedProgressReporter&) = delete;
    ScopedProgressReporter(ScopedProgressReporter&&) = delete;
    ScopedProgressReporter& operator=(ScopedProgressReporter&&) = delete;
    
    void report(uint64_t processed) {
        if (processed > reported_) {
            uint64_t delta = processed - reported_;
            parent_.addProgress(delta);
            reported_ = processed;
        }
    }
    
private:
    ProgressReporter& parent_;
    uint64_t bytes_;
    uint64_t reported_;
};

// Multi-operation progress tracker
class MultiProgressTracker {
public:
    MultiProgressTracker();
    ~MultiProgressTracker();
    
    // Add operation to track
    std::shared_ptr<ProgressReporter> addOperation(
        const std::string& name,
        uint64_t totalBytes);
    
    // Get overall progress
    [[nodiscard]] Progress getOverallProgress() const;
    
    // Set callback for overall progress
    void setOverallCallback(ProgressCallback callback);
    
    // Get operation count
    [[nodiscard]] size_t getOperationCount() const;
    [[nodiscard]] size_t getCompletedOperations() const;
    
    // Cancel all operations
    void cancelAll();
    
private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace yams::api