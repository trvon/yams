#pragma once

#include <yams/core/types.h>
#include <yams/storage/storage_engine.h>
#include <yams/storage/reference_counter.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <queue>
#include <thread>
#include <vector>

namespace yams::integrity {

/**
 * Integrity verification status for individual blocks
 */
enum class VerificationStatus {
    Passed,      // Block verified successfully
    Failed,      // Hash mismatch or read error
    Missing,     // Block not found
    Corrupted,   // Data corruption detected
    Repaired     // Block was corrupted but successfully repaired
};

/**
 * Result of verifying a single block
 */
struct VerificationResult {
    std::string blockHash;
    VerificationStatus status;
    std::string errorDetails;
    std::chrono::system_clock::time_point timestamp;
    size_t blockSize = 0;
    
    bool isSuccess() const {
        return status == VerificationStatus::Passed || 
               status == VerificationStatus::Repaired;
    }
};

/**
 * Configuration for integrity verifier
 */
struct VerificationConfig {
    size_t maxConcurrentVerifications = 4;
    size_t blocksPerSecond = 1000;
    std::chrono::hours fullScanInterval{24 * 7};  // Weekly full scan
    double cpuThreshold = 0.25;                   // Max 25% CPU usage
    bool enableAutoRepair = true;
    size_t maxRepairAttempts = 3;
    std::chrono::minutes pauseBetweenScans{10};
};

/**
 * Comprehensive integrity report
 */
struct IntegrityReport {
    uint64_t blocksVerified = 0;
    uint64_t blocksPassed = 0;
    uint64_t blocksFailed = 0;
    uint64_t blocksRepaired = 0;
    uint64_t blocksMissing = 0;
    uint64_t totalBytes = 0;
    std::chrono::milliseconds duration{0};
    std::chrono::system_clock::time_point generatedAt;
    std::vector<VerificationResult> failures;
    
    double getSuccessRate() const {
        return blocksVerified > 0 ? 
            static_cast<double>(blocksPassed + blocksRepaired) / blocksVerified : 0.0;
    }
    
    double getErrorRate() const {
        return blocksVerified > 0 ? 
            static_cast<double>(blocksFailed + blocksMissing) / blocksVerified : 0.0;
    }
};

/**
 * Block information for scheduling verification
 */
struct BlockInfo {
    std::string hash;
    std::chrono::system_clock::time_point lastVerified;
    uint32_t failureCount = 0;
    uint64_t size = 0;
    uint32_t accessCount = 0;
    
    // Priority calculation for verification scheduling
    uint64_t getPriority() const {
        auto now = std::chrono::system_clock::now();
        auto age = std::chrono::duration_cast<std::chrono::hours>(now - lastVerified).count();
        return (failureCount * 1000) + static_cast<uint64_t>(age / 24) + (accessCount / 10);
    }
    
    bool operator<(const BlockInfo& other) const {
        return getPriority() < other.getPriority(); // Priority queue uses max-heap
    }
};

/**
 * Scheduling strategies for verification
 */
enum class SchedulingStrategy {
    ByAge,         // Verify oldest blocks first
    BySize,        // Verify largest blocks first
    ByFailures,    // Verify blocks with most failures first
    ByAccess,      // Verify most accessed blocks first
    Balanced       // Balanced approach considering all factors
};

/**
 * Forward declarations
 */
class VerificationScheduler;
class VerificationMonitor;

/**
 * Main integrity verification system
 * 
 * Provides background verification of stored content to detect corruption,
 * bit rot, and other storage failures. Supports automatic repair when possible.
 */
class IntegrityVerifier {
public:
    using ProgressCallback = std::function<void(const VerificationResult&)>;
    using AlertCallback = std::function<void(const IntegrityReport&)>;
    
    /**
     * Constructor
     * @param storage Reference to storage engine
     * @param refCounter Reference to reference counter
     * @param config Verification configuration
     */
    IntegrityVerifier(storage::StorageEngine& storage,
                     storage::ReferenceCounter& refCounter,
                     VerificationConfig config = {});
    
    ~IntegrityVerifier();
    
    // Disable copy, allow move
    IntegrityVerifier(const IntegrityVerifier&) = delete;
    IntegrityVerifier& operator=(const IntegrityVerifier&) = delete;
    IntegrityVerifier(IntegrityVerifier&&) noexcept;
    IntegrityVerifier& operator=(IntegrityVerifier&&) noexcept;
    
    /**
     * Background verification control
     */
    [[nodiscard]] Result<void> startBackgroundVerification();
    [[nodiscard]] Result<void> stopBackgroundVerification();
    [[nodiscard]] Result<void> pauseVerification();
    [[nodiscard]] Result<void> resumeVerification();
    
    /**
     * On-demand verification
     */
    [[nodiscard]] VerificationResult verifyBlock(const std::string& hash);
    [[nodiscard]] std::vector<VerificationResult> verifyBlocks(
        const std::vector<std::string>& hashes);
    [[nodiscard]] IntegrityReport verifyAll();
    
    /**
     * Scheduling and configuration
     */
    void setSchedulingStrategy(SchedulingStrategy strategy);
    void setProgressCallback(ProgressCallback callback);
    void setAlertCallback(AlertCallback callback);
    void updateConfig(const VerificationConfig& newConfig);
    
    /**
     * Status and reporting
     */
    [[nodiscard]] bool isRunning() const;
    [[nodiscard]] bool isPaused() const;
    [[nodiscard]] IntegrityReport generateReport(
        std::chrono::hours period = std::chrono::hours{24}) const;
    [[nodiscard]] std::vector<VerificationResult> getRecentFailures(
        size_t maxResults = 100) const;
    
    /**
     * Statistics
     */
    struct Statistics {
        std::atomic<uint64_t> blocksVerifiedTotal{0};
        std::atomic<uint64_t> verificationErrorsTotal{0};
        std::atomic<uint64_t> repairsAttemptedTotal{0};
        std::atomic<uint64_t> repairsSuccessfulTotal{0};
        std::atomic<uint64_t> bytesVerifiedTotal{0};
        
        std::chrono::system_clock::time_point startTime;
        std::chrono::milliseconds totalVerificationTime{0};
        
        double getVerificationRate() const {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now() - startTime);
            return elapsed.count() > 0 ? 
                static_cast<double>(blocksVerifiedTotal) / elapsed.count() : 0.0;
        }
    };
    
    [[nodiscard]] const Statistics& getStatistics() const;
    void resetStatistics();

private:
    // Pimpl pattern for implementation details
    struct Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Verification scheduler manages prioritization of blocks for verification
 */
class VerificationScheduler {
public:
    explicit VerificationScheduler(SchedulingStrategy strategy = SchedulingStrategy::Balanced);
    ~VerificationScheduler();
    
    // Add blocks to verification queue
    void addBlock(const BlockInfo& block);
    void addBlocks(const std::vector<BlockInfo>& blocks);
    
    // Get next block to verify
    std::optional<BlockInfo> getNextBlock();
    
    // Queue management
    size_t getQueueSize() const;
    void clearQueue();
    void setStrategy(SchedulingStrategy strategy);
    
    // Update block information after verification
    void updateBlockInfo(const std::string& hash, const VerificationResult& result);

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Monitoring and metrics for verification system
 */
class VerificationMonitor {
public:
    VerificationMonitor();
    ~VerificationMonitor();
    
    // Record verification events
    void recordVerification(const VerificationResult& result);
    void recordRepairAttempt(const std::string& hash, bool success);
    
    // Alerting
    void setErrorRateThreshold(double threshold);
    bool shouldAlert() const;
    
    // Metrics export
    struct Metrics {
        uint64_t verificationsPerSecond = 0;
        uint64_t errorsPerSecond = 0;
        double currentErrorRate = 0.0;
        uint64_t totalVerifications = 0;
        uint64_t totalErrors = 0;
        uint64_t repairAttempts = 0;
        uint64_t successfulRepairs = 0;
    };
    
    Metrics getCurrentMetrics() const;
    void exportMetrics(std::ostream& out) const;

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace yams::integrity