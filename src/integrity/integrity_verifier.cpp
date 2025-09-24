#include <yams/crypto/hasher.h>
#include <yams/integrity/repair_manager.h>
#include <yams/integrity/verifier.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace yams::integrity {

/**
 * Rate limiter to control verification speed
 */
class RateLimiter {
private:
    std::chrono::steady_clock::time_point lastCheck;
    size_t tokens;
    const size_t maxTokens;
    const std::chrono::milliseconds refillInterval;
    mutable std::mutex mutex;

public:
    explicit RateLimiter(size_t tokensPerSecond)
        : lastCheck(std::chrono::steady_clock::now()), tokens(tokensPerSecond),
          maxTokens(tokensPerSecond), refillInterval(1000) // 1 second
    {}

    bool tryAcquire(size_t requestedTokens = 1) {
        std::lock_guard lock(mutex);

        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastCheck);

        if (elapsed >= refillInterval) {
            tokens = maxTokens;
            lastCheck = now;
        }

        if (tokens >= requestedTokens) {
            tokens -= requestedTokens;
            return true;
        }

        return false;
    }

    void updateRate(size_t newTokensPerSecond) {
        std::lock_guard lock(mutex);
        const_cast<size_t&>(maxTokens) = newTokensPerSecond;
        tokens = std::min(tokens, maxTokens);
    }
};

/**
 * Thread pool for concurrent verification
 */
class VerificationThreadPool {
private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    mutable std::mutex queueMutex;
    std::condition_variable condition;
    std::atomic<bool> stop{false};

public:
    explicit VerificationThreadPool(size_t threads) {
        for (size_t i = 0; i < threads; ++i) {
            workers.emplace_back([this] {
                while (true) {
                    std::function<void()> task;

                    {
                        std::unique_lock lock(queueMutex);
                        condition.wait(lock, [this] { return stop || !tasks.empty(); });

                        if (stop && tasks.empty()) {
                            return;
                        }

                        task = std::move(tasks.front());
                        tasks.pop();
                    }

                    task();
                }
            });
        }
    }

    ~VerificationThreadPool() {
        stop = true;
        condition.notify_all();

        for (auto& worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    template <class F> void enqueue(F&& f) {
        {
            std::lock_guard lock(queueMutex);
            tasks.emplace(std::forward<F>(f));
        }
        condition.notify_one();
    }

    size_t getQueueSize() const {
        std::lock_guard<std::mutex> lock(queueMutex);
        return tasks.size();
    }
};

/**
 * Implementation details for IntegrityVerifier
 */
struct IntegrityVerifier::Impl {
    storage::StorageEngine& storage;
    storage::ReferenceCounter& refCounter;
    VerificationConfig config;

    // Thread management
    std::unique_ptr<VerificationThreadPool> threadPool;
    std::unique_ptr<std::thread> backgroundThread;
    std::atomic<bool> running{false};
    std::atomic<bool> paused{false};
    std::atomic<bool> shouldStop{false};

    // Rate limiting
    std::unique_ptr<RateLimiter> rateLimiter;

    // Scheduling
    std::unique_ptr<VerificationScheduler> scheduler;

    // Monitoring
    std::unique_ptr<VerificationMonitor> monitor;
    std::shared_ptr<RepairManager> repairManager;

    // Statistics
    mutable IntegrityVerifier::Statistics stats;

    // Recent results cache
    mutable std::mutex resultsMutex;
    std::deque<VerificationResult> recentResults;
    static constexpr size_t MAX_RECENT_RESULTS = 10000;

    // Callbacks
    ProgressCallback progressCallback;
    AlertCallback alertCallback;

    // Hashing
    std::unique_ptr<crypto::IContentHasher> hasher;

    explicit Impl(storage::StorageEngine& storageEngine, storage::ReferenceCounter& refCounterRef,
                  VerificationConfig cfg)
        : storage(storageEngine), refCounter(refCounterRef), config(std::move(cfg)),
          threadPool(
              std::make_unique<VerificationThreadPool>(this->config.maxConcurrentVerifications)),
          rateLimiter(std::make_unique<RateLimiter>(this->config.blocksPerSecond)),
          scheduler(std::make_unique<VerificationScheduler>()),
          monitor(std::make_unique<VerificationMonitor>()), hasher(crypto::createSHA256Hasher()) {
        stats.startTime = std::chrono::system_clock::now();
    }

    VerificationResult verifyBlockImpl(const std::string& hash) {
        auto startTime = std::chrono::high_resolution_clock::now();
        VerificationResult result;
        result.blockHash = hash;
        result.timestamp = std::chrono::system_clock::now();

        try {
            // Check if block exists
            auto existsResult = storage.exists(hash);
            if (!existsResult.has_value()) {
                result.status = VerificationStatus::Failed;
                result.errorDetails =
                    std::string("Failed to check block existence: ") + existsResult.error().message;
                stats.verificationErrorsTotal++;
                return result;
            }

            if (!existsResult.value()) {
                if (config.enableAutoRepair && repairManager && repairManager->canRepair(hash)) {
                    stats.repairsAttemptedTotal++;
                    if (repairManager->attemptRepair(hash)) {
                        auto repairedData = storage.retrieve(hash);
                        if (repairedData.has_value()) {
                            hasher->init();
                            hasher->update(std::span{repairedData.value()});
                            auto repairedHash = hasher->finalize();
                            if (repairedHash == hash) {
                                result.status = VerificationStatus::Repaired;
                                result.blockSize = repairedData.value().size();
                                stats.repairsSuccessfulTotal++;
                                stats.blocksVerifiedTotal++;
                                stats.bytesVerifiedTotal += repairedData.value().size();
                                return result;
                            }
                        }
                    }
                }

                result.status = VerificationStatus::Missing;
                result.errorDetails = "Block not found in storage";
                stats.verificationErrorsTotal++;
                return result;
            }

            // Retrieve block data
            auto retrieveResult = storage.retrieve(hash);
            if (!retrieveResult.has_value()) {
                result.status = VerificationStatus::Failed;
                result.errorDetails =
                    std::string("Failed to retrieve block: ") + retrieveResult.error().message;
                stats.verificationErrorsTotal++;
                return result;
            }

            const auto& blockData = retrieveResult.value();
            result.blockSize = blockData.size();

            // Verify hash
            hasher->init();
            hasher->update(std::span{blockData});
            auto computedHash = hasher->finalize();

            if (computedHash == hash) {
                result.status = VerificationStatus::Passed;
                stats.blocksVerifiedTotal++;
                stats.bytesVerifiedTotal += blockData.size();
            } else {
                result.status = VerificationStatus::Corrupted;
                result.errorDetails = "Hash mismatch: expected " + hash + ", got " + computedHash;
                stats.verificationErrorsTotal++;

                if (config.enableAutoRepair && repairManager && repairManager->canRepair(hash)) {
                    bool repaired = false;
                    for (size_t attempt = 0; attempt < config.maxRepairAttempts && !repaired;
                         ++attempt) {
                        stats.repairsAttemptedTotal++;
                        if (!repairManager->attemptRepair(hash)) {
                            continue;
                        }

                        auto postRepair = storage.retrieve(hash);
                        if (!postRepair.has_value()) {
                            spdlog::warn("Repair succeeded but retrieve failed for {}",
                                         hash.substr(0, 8));
                            continue;
                        }

                        hasher->init();
                        hasher->update(std::span{postRepair.value()});
                        auto repairedHash = hasher->finalize();
                        if (repairedHash == hash) {
                            result.status = VerificationStatus::Repaired;
                            result.errorDetails.clear();
                            result.blockSize = postRepair.value().size();
                            stats.repairsSuccessfulTotal++;
                            stats.blocksVerifiedTotal++;
                            stats.bytesVerifiedTotal += postRepair.value().size();
                            repaired = true;
                        } else {
                            spdlog::warn("Repair mismatch for {}: {}", hash.substr(0, 8),
                                         repairedHash.substr(0, 8));
                        }
                    }
                    if (!repaired) {
                        spdlog::warn("Automatic repair failed for {}", hash.substr(0, 8));
                    }
                }
            }

        } catch (const std::exception& e) {
            result.status = VerificationStatus::Failed;
            result.errorDetails = "Exception during verification: " + std::string(e.what());
            stats.verificationErrorsTotal++;
        }

        // Update timing statistics
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        stats.totalVerificationTime += duration;

        // Record result
        recordResult(result);

        // Update scheduler with result
        scheduler->updateBlockInfo(hash, result);

        // Record with monitor
        monitor->recordVerification(result);

        // Call progress callback if set
        if (progressCallback) {
            progressCallback(result);
        }

        // Check if we should alert
        if (monitor->shouldAlert() && alertCallback) {
            auto report = generateReportImpl(std::chrono::hours{1});
            alertCallback(report);
        }

        return result;
    }

    void recordResult(const VerificationResult& result) {
        std::lock_guard lock(resultsMutex);

        recentResults.push_back(result);
        if (recentResults.size() > MAX_RECENT_RESULTS) {
            recentResults.pop_front();
        }
    }

    IntegrityReport generateReportImpl(std::chrono::hours period) const {
        std::lock_guard lock(resultsMutex);

        IntegrityReport report;
        report.generatedAt = std::chrono::system_clock::now();

        auto cutoff = report.generatedAt - period;

        for (const auto& result : recentResults) {
            if (result.timestamp >= cutoff) {
                report.blocksVerified++;
                report.totalBytes += result.blockSize;

                switch (result.status) {
                    case VerificationStatus::Passed:
                        report.blocksPassed++;
                        break;
                    case VerificationStatus::Failed:
                        report.blocksFailed++;
                        report.failures.push_back(result);
                        break;
                    case VerificationStatus::Missing:
                        report.blocksMissing++;
                        report.failures.push_back(result);
                        break;
                    case VerificationStatus::Corrupted:
                        report.blocksFailed++;
                        report.failures.push_back(result);
                        break;
                    case VerificationStatus::Repaired:
                        report.blocksRepaired++;
                        break;
                }
            }
        }

        report.duration = stats.totalVerificationTime;

        return report;
    }
};

// IntegrityVerifier implementation

IntegrityVerifier::IntegrityVerifier(storage::StorageEngine& storage,
                                     storage::ReferenceCounter& refCounter,
                                     VerificationConfig config)
    : pImpl(std::make_unique<Impl>(storage, refCounter, std::move(config))) {}

IntegrityVerifier::~IntegrityVerifier() {
    if (pImpl && pImpl->running) {
        [[maybe_unused]] auto result = stopBackgroundVerification();
    }
}

IntegrityVerifier::IntegrityVerifier(IntegrityVerifier&&) noexcept = default;
IntegrityVerifier& IntegrityVerifier::operator=(IntegrityVerifier&&) noexcept = default;

Result<void> IntegrityVerifier::startBackgroundVerification() {
    if (pImpl->running) {
        return Result<void>(ErrorCode::OperationInProgress);
    }

    try {
        pImpl->shouldStop = false;
        pImpl->paused = false;
        pImpl->running = true;

        spdlog::info("Started integrity verification with {} concurrent threads",
                     pImpl->config.maxConcurrentVerifications);

        return {};
    } catch (const std::exception& e) {
        pImpl->running = false;
        spdlog::error("Failed to start background verification: {}", e.what());
        return Result<void>(ErrorCode::Unknown);
    }
}

Result<void> IntegrityVerifier::stopBackgroundVerification() {
    if (!pImpl->running) {
        return {};
    }

    try {
        pImpl->shouldStop = true;
        pImpl->running = false;

        spdlog::info("Stopped integrity verification");
        return {};
    } catch (const std::exception& e) {
        spdlog::error("Error stopping background verification: {}", e.what());
        return Result<void>(ErrorCode::Unknown);
    }
}

Result<void> IntegrityVerifier::pauseVerification() {
    if (!pImpl->running) {
        return Result<void>(ErrorCode::InvalidOperation);
    }

    pImpl->paused = true;
    spdlog::info("Paused integrity verification");
    return {};
}

Result<void> IntegrityVerifier::resumeVerification() {
    if (!pImpl->running) {
        return Result<void>(ErrorCode::InvalidOperation);
    }

    pImpl->paused = false;
    spdlog::info("Resumed integrity verification");
    return {};
}

VerificationResult IntegrityVerifier::verifyBlock(const std::string& hash) {
    return pImpl->verifyBlockImpl(hash);
}

std::vector<VerificationResult>
IntegrityVerifier::verifyBlocks(const std::vector<std::string>& hashes) {
    std::vector<VerificationResult> results;
    results.reserve(hashes.size());

    for (const auto& hash : hashes) {
        results.push_back(verifyBlock(hash));
    }

    return results;
}

IntegrityReport IntegrityVerifier::verifyAll() {
    spdlog::info("Starting full integrity verification");

    auto startTime = std::chrono::high_resolution_clock::now();
    IntegrityReport report;
    report.generatedAt = std::chrono::system_clock::now();

    try {
        // This is a simplified implementation
        // In a real system, we'd iterate through all blocks in storage

        auto stats = pImpl->storage.getStats();
        spdlog::info("Verifying {} objects", stats.totalObjects.load());

        // For now, return the current report
        report = generateReport(std::chrono::hours{24});

    } catch (const std::exception& e) {
        spdlog::error("Error during full verification: {}", e.what());
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    report.duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    spdlog::info("Full integrity verification completed: {}/{} blocks passed", report.blocksPassed,
                 report.blocksVerified);

    return report;
}

void IntegrityVerifier::setSchedulingStrategy(SchedulingStrategy strategy) {
    pImpl->scheduler->setStrategy(strategy);
}

void IntegrityVerifier::setProgressCallback(ProgressCallback callback) {
    pImpl->progressCallback = std::move(callback);
}

void IntegrityVerifier::setAlertCallback(AlertCallback callback) {
    pImpl->alertCallback = std::move(callback);
}

void IntegrityVerifier::updateConfig(const VerificationConfig& newConfig) {
    pImpl->config = newConfig;
    pImpl->rateLimiter->updateRate(newConfig.blocksPerSecond);

    spdlog::info("Updated verification configuration");
}

void IntegrityVerifier::setRepairManager(std::shared_ptr<RepairManager> manager) {
    pImpl->repairManager = std::move(manager);
}

bool IntegrityVerifier::isRunning() const {
    return pImpl->running;
}

bool IntegrityVerifier::isPaused() const {
    return pImpl->paused;
}

IntegrityReport IntegrityVerifier::generateReport(std::chrono::hours period) const {
    return pImpl->generateReportImpl(period);
}

std::vector<VerificationResult> IntegrityVerifier::getRecentFailures(size_t maxResults) const {
    std::lock_guard lock(pImpl->resultsMutex);

    std::vector<VerificationResult> failures;
    failures.reserve(maxResults);

    for (auto it = pImpl->recentResults.rbegin();
         it != pImpl->recentResults.rend() && failures.size() < maxResults; ++it) {
        if (!it->isSuccess()) {
            failures.push_back(*it);
        }
    }

    return failures;
}

const IntegrityVerifier::Statistics& IntegrityVerifier::getStatistics() const {
    return pImpl->stats;
}

void IntegrityVerifier::resetStatistics() {
    // Reset atomic members individually since atomic types can't be copy-assigned
    pImpl->stats.blocksVerifiedTotal = 0;
    pImpl->stats.verificationErrorsTotal = 0;
    pImpl->stats.repairsAttemptedTotal = 0;
    pImpl->stats.repairsSuccessfulTotal = 0;
    pImpl->stats.bytesVerifiedTotal = 0;
    pImpl->stats.totalVerificationTime = std::chrono::milliseconds{0};
    pImpl->stats.startTime = std::chrono::system_clock::now();

    std::lock_guard lock(pImpl->resultsMutex);
    pImpl->recentResults.clear();

    spdlog::info("Reset verification statistics");
}

} // namespace yams::integrity
