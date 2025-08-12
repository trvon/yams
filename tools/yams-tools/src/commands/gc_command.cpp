#include <yams/tools/command.h>
#include <yams/tools/progress_bar.h>
#include <yams/storage/storage_engine.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/garbage_collector.h>

#include <spdlog/spdlog.h>
#include <chrono>
#include <iomanip>

namespace yams::tools {

class GCCommand : public Command {
public:
    GCCommand() : Command("gc", "Run garbage collection to remove unreferenced blocks") {}
    
    void setupOptions(CLI::App& app) override {
        auto* gc = app.add_subcommand("gc", getDescription());
        
        gc->add_flag("-a,--aggressive", aggressive_, 
                     "Aggressive collection mode (shorter minimum age)");
        gc->add_option("-b,--batch-size", batchSize_,
                      "Number of blocks to process per batch")
           ->default_val(1000);
        gc->add_option("--min-age", minAgeHours_,
                      "Minimum block age in hours before collection")
           ->default_val(24);
        gc->add_flag("--force", force_,
                     "Force collection without confirmation");
        
        addCommonOptions(*gc);
        gc->callback([this]() { shouldExecute_ = true; });
    }
    
    int execute() override {
        if (!shouldExecute_) return 0;
        
        try {
            log("Starting garbage collection...");
            
            // Initialize storage components
            auto storageResult = initializeStorage();
            if (!storageResult) return 1;
            
            auto [storage, refCounter] = *storageResult;
            
            // Create garbage collector
            storage::GarbageCollector gc(*storage, *refCounter);
            
            // Configure options
            storage::GCOptions options;
            options.maxBlocksPerRun = batchSize_;
            options.minAgeSeconds = aggressive_ ? 3600 : (minAgeHours_ * 3600);
            options.dryRun = isDryRun();
            
            // Set up progress callback
            size_t totalScanned = 0;
            size_t totalDeleted = 0;
            
            options.progressCallback = [&](const std::string& hash, size_t size) {
                totalScanned++;
                if (isVerbose()) {
                    logVerbose("Checking block " + hash.substr(0, 8) + 
                              "... (" + std::to_string(size) + " bytes)");
                }
            };
            
            // Get initial statistics
            auto preStats = refCounter->getStatistics();
            
            if (!force_ && !isDryRun()) {
                std::cout << "This will scan " << preStats.unreferencedBlocks 
                         << " unreferenced blocks." << std::endl;
                std::cout << "Continue? [y/N] ";
                
                std::string response;
                std::getline(std::cin, response);
                if (response != "y" && response != "Y") {
                    log("Garbage collection cancelled.");
                    return 0;
                }
            }
            
            // Run garbage collection
            auto startTime = std::chrono::steady_clock::now();
            
            if (isDryRun()) {
                log("Running in dry-run mode - no blocks will be deleted");
            }
            
            auto result = gc.collect(options);
            if (!result.has_value()) {
                logError("Garbage collection failed: " + errorToString(result.error()));
                return 1;
            }
            
            auto endTime = std::chrono::steady_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                endTime - startTime);
            
            const auto& stats = result.value();
            
            // Display results
            log("\nGarbage collection completed:");
            log("  Blocks scanned:    " + std::to_string(stats.blocksScanned));
            log("  Blocks deleted:    " + std::to_string(stats.blocksDeleted));
            log("  Space reclaimed:   " + formatBytes(stats.bytesReclaimed));
            log("  Duration:          " + std::to_string(duration.count()) + "s");
            
            if (stats.errors.size() > 0) {
                log("\nErrors encountered:");
                for (const auto& error : stats.errors) {
                    logError("  " + error);
                }
            }
            
            // Get post-collection statistics
            auto postStats = refCounter->getStatistics();
            
            if (isVerbose()) {
                log("\nStorage statistics after GC:");
                log("  Total blocks:      " + std::to_string(postStats.totalBlocks));
                log("  Total references:  " + std::to_string(postStats.totalReferences));
                log("  Unreferenced:      " + std::to_string(postStats.unreferencedBlocks));
            }
            
            return 0;
            
        } catch (const std::exception& e) {
            logError("Exception during garbage collection: " + std::string(e.what()));
            return 1;
        }
    }
    
private:
    std::optional<std::pair<std::unique_ptr<storage::StorageEngine>,
                           std::unique_ptr<storage::ReferenceCounter>>> 
    initializeStorage() {
        // Create storage engine
        storage::StorageConfig storageConfig{
            .basePath = getStoragePath() / "blocks",
            .shardDepth = 2,
            .mutexPoolSize = 1024
        };
        
        auto storage = std::make_unique<storage::StorageEngine>(std::move(storageConfig));
        
        // Create reference counter
        storage::ReferenceCounter::Config refConfig{
            .databasePath = getStoragePath() / "refs.db",
            .enableWAL = true,
            .enableStatistics = true
        };
        
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
    bool aggressive_ = false;
    size_t batchSize_ = 1000;
    size_t minAgeHours_ = 24;
    bool force_ = false;
};

// Register command
static bool registered = []() {
    // This will be called by the main application to register the command
    return true;
}();

} // namespace yams::tools