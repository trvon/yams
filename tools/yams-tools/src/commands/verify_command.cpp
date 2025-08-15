#include <yams/integrity/verifier.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>
#include <yams/tools/command.h>
#include <yams/tools/progress_bar.h>

#include <spdlog/spdlog.h>
#include <random>
#include <thread>

namespace yams::tools {

class VerifyCommand : public Command {
public:
    VerifyCommand() : Command("verify", "Verify storage integrity") {}

    void setupOptions(CLI::App& app) override {
        auto* verify = app.add_subcommand("verify", getDescription());

        verify->add_option("-h,--hash", specificHash_, "Verify specific content hash");

        verify->add_flag("-f,--full", fullScan_, "Full scan of all blocks");

        verify->add_option("-s,--sample", sampleRate_, "Sample rate percentage (1-100)")
            ->default_val(100)
            ->check(CLI::Range(1, 100));

        verify->add_option("-j,--jobs", parallelJobs_, "Number of parallel verification jobs")
            ->default_val(4);

        verify->add_flag("-r,--repair", attemptRepair_, "Attempt to repair corrupted blocks");

        verify
            ->add_option("--report-interval", reportIntervalHours_,
                         "Report interval in hours for summary")
            ->default_val(1);

        addCommonOptions(*verify);
        verify->callback([this]() { shouldExecute_ = true; });
    }

    int execute() override {
        if (!shouldExecute_)
            return 0;

        try {
            // Initialize storage components
            auto storageResult = initializeStorage();
            if (!storageResult)
                return 1;

            auto [storage, refCounter] = *storageResult;

            // Create integrity verifier
            integrity::VerificationConfig config;
            config.maxConcurrentVerifications = parallelJobs_;
            config.blocksPerSecond = 1000; // Default rate
            config.enableAutoRepair = attemptRepair_;

            integrity::IntegrityVerifier verifier(*storage, *refCounter, config);

            // Handle specific hash verification
            if (!specificHash_.empty()) {
                return verifySpecificHash(verifier);
            }

            // Handle full or sampled scan
            return performScan(verifier, *refCounter);

        } catch (const std::exception& e) {
            logError("Verification failed: " + std::string(e.what()));
            return 1;
        }
    }

private:
    int verifySpecificHash(integrity::IntegrityVerifier& verifier) {
        log("Verifying block: " + specificHash_);

        auto result = verifier.verifyBlock(specificHash_);

        // Display result
        std::cout << "\nVerification Result:\n";
        std::cout << "  Hash:   " << result.blockHash << "\n";
        std::cout << "  Status: " << statusToString(result.status) << "\n";

        if (!result.errorDetails.empty()) {
            std::cout << "  Error:  " << result.errorDetails << "\n";
        }

        if (result.blockSize > 0) {
            std::cout << "  Size:   " << formatBytes(result.blockSize) << "\n";
        }

        return result.isSuccess() ? 0 : 1;
    }

    int performScan(integrity::IntegrityVerifier& verifier, storage::ReferenceCounter& refCounter) {
        log(fullScan_
                ? "Starting full integrity scan..."
                : "Starting sampled integrity scan (" + std::to_string(sampleRate_) + "%)...");

        // Get all blocks to verify
        std::vector<std::string> blocksToVerify;
        size_t totalBlocks = 0;

        auto queryResult =
            refCounter.queryBlocks([&](const std::string& hash, uint64_t refCount, uint64_t size) {
                totalBlocks++;

                // Apply sampling if not full scan
                if (fullScan_ || shouldSample()) {
                    blocksToVerify.push_back(hash);
                }

                return true; // Continue iteration
            });

        if (!queryResult.has_value()) {
            logError("Failed to query blocks: " + errorToString(queryResult.error()));
            return 1;
        }

        log("Selected " + std::to_string(blocksToVerify.size()) + " out of " +
            std::to_string(totalBlocks) + " blocks for verification");

        // Set up progress tracking
        ProgressBar progress(blocksToVerify.size(), "Verifying");

        // Verification statistics
        std::atomic<size_t> passed{0};
        std::atomic<size_t> failed{0};
        std::atomic<size_t> missing{0};
        std::atomic<size_t> corrupted{0};
        std::atomic<size_t> repaired{0};

        // Set up progress callback
        verifier.setProgressCallback([&](const integrity::VerificationResult& result) {
            progress.increment();

            switch (result.status) {
                case integrity::VerificationStatus::Passed:
                    passed++;
                    break;
                case integrity::VerificationStatus::Failed:
                    failed++;
                    if (isVerbose()) {
                        logVerbose("Failed: " + result.blockHash.substr(0, 16) + " - " +
                                   result.errorDetails);
                    }
                    break;
                case integrity::VerificationStatus::Missing:
                    missing++;
                    break;
                case integrity::VerificationStatus::Corrupted:
                    corrupted++;
                    if (isVerbose()) {
                        logVerbose("Corrupted: " + result.blockHash.substr(0, 16));
                    }
                    break;
                case integrity::VerificationStatus::Repaired:
                    repaired++;
                    break;
            }
        });

        // Perform verification
        auto startTime = std::chrono::steady_clock::now();

        if (parallelJobs_ > 1) {
            // Parallel verification
            std::vector<std::thread> threads;
            std::atomic<size_t> blockIndex{0};

            for (size_t i = 0; i < parallelJobs_; ++i) {
                threads.emplace_back([&]() {
                    size_t idx;
                    while ((idx = blockIndex.fetch_add(1)) < blocksToVerify.size()) {
                        verifier.verifyBlock(blocksToVerify[idx]);
                    }
                });
            }

            for (auto& thread : threads) {
                thread.join();
            }
        } else {
            // Sequential verification
            for (const auto& hash : blocksToVerify) {
                verifier.verifyBlock(hash);
            }
        }

        progress.finish();

        auto endTime = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);

        // Generate and display report
        auto report = verifier.generateReport(std::chrono::hours(reportIntervalHours_));

        std::cout << "\n=== Integrity Verification Report ===\n\n";
        std::cout << "Verification Summary:\n";
        std::cout << "  Blocks verified:  " << blocksToVerify.size() << "\n";
        std::cout << "  Passed:          " << passed.load() << "\n";
        std::cout << "  Failed:          " << failed.load() << "\n";
        std::cout << "  Missing:         " << missing.load() << "\n";
        std::cout << "  Corrupted:       " << corrupted.load() << "\n";

        if (attemptRepair_) {
            std::cout << "  Repaired:        " << repaired.load() << "\n";
        }

        std::cout << "  Duration:        " << duration.count() << "s\n";
        std::cout << "  Rate:            "
                  << (blocksToVerify.size() / (duration.count() > 0 ? duration.count() : 1L))
                  << " blocks/s\n";

        double successRate =
            blocksToVerify.size() > 0
                ? (static_cast<double>(passed.load()) / blocksToVerify.size() * 100)
                : 0;
        std::cout << "  Success rate:    " << std::fixed << std::setprecision(2) << successRate
                  << "%\n";

        // Display detailed errors if any
        if (failed.load() > 0 || corrupted.load() > 0 || missing.load() > 0) {
            std::cout << "\nIntegrity Issues Detected:\n";

            auto failures = verifier.getRecentFailures(100);
            for (const auto& failure : failures) {
                std::cout << "  " << failure.blockHash.substr(0, 16) << "... - "
                          << statusToString(failure.status);
                if (!failure.errorDetails.empty()) {
                    std::cout << " (" << failure.errorDetails << ")";
                }
                std::cout << "\n";
            }

            if (failures.size() < (failed.load() + corrupted.load() + missing.load())) {
                std::cout << "  ... and "
                          << ((failed.load() + corrupted.load() + missing.load()) - failures.size())
                          << " more\n";
            }
        }

        return (failed.load() + corrupted.load() + missing.load()) > 0 ? 1 : 0;
    }

    bool shouldSample() {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static std::uniform_int_distribution<> dis(1, 100);

        return dis(gen) <= sampleRate_;
    }

    std::string statusToString(integrity::VerificationStatus status) const {
        switch (status) {
            case integrity::VerificationStatus::Passed:
                return "PASSED";
            case integrity::VerificationStatus::Failed:
                return "FAILED";
            case integrity::VerificationStatus::Missing:
                return "MISSING";
            case integrity::VerificationStatus::Corrupted:
                return "CORRUPTED";
            case integrity::VerificationStatus::Repaired:
                return "REPAIRED";
            default:
                return "UNKNOWN";
        }
    }

    std::optional<std::pair<std::unique_ptr<storage::StorageEngine>,
                            std::unique_ptr<storage::ReferenceCounter>>>
    initializeStorage() {
        // Create storage engine
        storage::StorageConfig storageConfig{
            .basePath = getStoragePath() / "blocks", .shardDepth = 2, .mutexPoolSize = 1024};

        auto storage = std::make_unique<storage::StorageEngine>(std::move(storageConfig));

        // Create reference counter
        storage::ReferenceCounter::Config refConfig{.databasePath = getStoragePath() / "refs.db",
                                                    .enableWAL = true,
                                                    .enableStatistics = true};

        auto refCounter = std::make_unique<storage::ReferenceCounter>(std::move(refConfig));

        return std::make_pair(std::move(storage), std::move(refCounter));
    }

    std::string formatBytes(uint64_t bytes) const {
        const char* units[] = {"B", "KB", "MB", "GB", "TB"};
        int unitIndex = 0;
        double size = static_cast<double>(bytes);

        while (size >= 1024.0 && unitIndex < 4) {
            size /= 1024.0;
            unitIndex++;
        }

        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << size << " " << units[unitIndex];
        return oss.str();
    }

    bool shouldExecute_ = false;
    std::string specificHash_;
    bool fullScan_ = false;
    int sampleRate_ = 100;
    size_t parallelJobs_ = 4;
    bool attemptRepair_ = false;
    int reportIntervalHours_ = 1;
};

} // namespace yams::tools