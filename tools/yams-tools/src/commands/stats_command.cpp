#include <yams/tools/command.h>
#include <yams/storage/storage_engine.h>
#include <yams/storage/reference_counter.h>
#include <yams/manifest/manifest_manager.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <map>
#include <vector>

namespace kronos::tools {

class StatsCommand : public Command {
public:
    StatsCommand() : Command("stats", "Display storage statistics and deduplication metrics") {}
    
    void setupOptions(CLI::App& app) override {
        auto* stats = app.add_subcommand("stats", getDescription());
        
        stats->add_option("-f,--format", outputFormat_, 
                         "Output format (human, json, csv)")
             ->default_val("human")
             ->check(CLI::IsMember({"human", "json", "csv"}));
        
        stats->add_flag("-d,--detailed", detailed_,
                       "Show detailed statistics including distribution");
        
        stats->add_option("--top", topCount_,
                         "Number of top items to show")
             ->default_val(10);
        
        addCommonOptions(*stats);
        stats->callback([this]() { shouldExecute_ = true; });
    }
    
    int execute() override {
        if (!shouldExecute_) return 0;
        
        try {
            logVerbose("Gathering storage statistics...");
            
            // Initialize storage components
            auto storageResult = initializeStorage();
            if (!storageResult) return 1;
            
            auto [storage, refCounter, manifestMgr] = *storageResult;
            
            // Gather statistics
            Stats stats = gatherStatistics(*storage, *refCounter, *manifestMgr);
            
            // Output based on format
            if (outputFormat_ == "json") {
                printJSON(stats);
            } else if (outputFormat_ == "csv") {
                printCSV(stats);
            } else {
                printHumanReadable(stats);
            }
            
            return 0;
            
        } catch (const std::exception& e) {
            logError("Error gathering statistics: " + std::string(e.what()));
            return 1;
        }
    }
    
private:
    struct Stats {
        // Storage statistics
        uint64_t totalBlocks = 0;
        uint64_t uniqueBlocks = 0;
        uint64_t totalSize = 0;
        uint64_t deduplicatedSize = 0;
        uint64_t unreferencedBlocks = 0;
        uint64_t unreferencedSize = 0;
        
        // Reference statistics  
        uint64_t totalReferences = 0;
        double averageReferences = 0.0;
        uint64_t maxReferences = 0;
        
        // Manifest statistics
        uint64_t totalManifests = 0;
        uint64_t totalFiles = 0;
        uint64_t totalFileSize = 0;
        
        // Deduplication metrics
        double deduplicationRatio = 0.0;
        double spacesSavings = 0.0;
        
        // Block size distribution
        std::map<std::string, uint64_t> blockSizeDistribution;
        
        // Top referenced blocks
        std::vector<std::pair<std::string, uint64_t>> topReferencedBlocks;
        
        // Largest files
        std::vector<std::pair<std::string, uint64_t>> largestFiles;
    };
    
    Stats gatherStatistics(storage::StorageEngine& storage,
                          storage::ReferenceCounter& refCounter,
                          manifest::ManifestManager& manifestMgr) {
        Stats stats;
        
        // Get storage statistics
        auto storageStats = storage.getStats();
        stats.totalBlocks = storageStats.totalObjects;
        stats.totalSize = storageStats.totalBytes;
        
        // Get reference counter statistics
        auto refStats = refCounter.getStatistics();
        stats.uniqueBlocks = refStats.totalBlocks;
        stats.totalReferences = refStats.totalReferences;
        stats.unreferencedBlocks = refStats.unreferencedBlocks;
        stats.unreferencedSize = refStats.unreferencedBytes;
        
        // Calculate averages
        if (stats.uniqueBlocks > 0) {
            stats.averageReferences = static_cast<double>(stats.totalReferences) / 
                                    stats.uniqueBlocks;
        }
        
        // Calculate deduplication metrics
        if (stats.totalReferences > 0) {
            // Deduplicated size is what would be stored without dedup
            stats.deduplicatedSize = stats.totalSize * stats.averageReferences;
            stats.deduplicationRatio = static_cast<double>(stats.deduplicatedSize) / 
                                     stats.totalSize;
            stats.spacesSavings = 1.0 - (static_cast<double>(stats.totalSize) / 
                                        stats.deduplicatedSize);
        }
        
        // Get detailed statistics if requested
        if (detailed_) {
            gatherDetailedStatistics(stats, storage, refCounter, manifestMgr);
        }
        
        return stats;
    }
    
    void gatherDetailedStatistics(Stats& stats,
                                 storage::StorageEngine& storage,
                                 storage::ReferenceCounter& refCounter,
                                 manifest::ManifestManager& manifestMgr) {
        // Block size distribution
        std::map<uint64_t, uint64_t> sizeHistogram;
        
        // Top referenced blocks
        std::vector<std::pair<std::string, uint64_t>> allBlocks;
        
        // Query all blocks with reference counts
        auto queryResult = refCounter.queryBlocks([&](const std::string& hash, 
                                                     uint64_t refCount,
                                                     uint64_t size) {
            // Update size histogram
            uint64_t bucket = (size / 1024) * 1024; // Round to KB
            sizeHistogram[bucket]++;
            
            // Track for top referenced
            if (refCount > 1) {
                allBlocks.push_back({hash, refCount});
            }
            
            // Track max references
            stats.maxReferences = std::max(stats.maxReferences, refCount);
            
            return true; // Continue iteration
        });
        
        // Convert size histogram to readable format
        for (const auto& [size, count] : sizeHistogram) {
            stats.blockSizeDistribution[formatBytes(size) + "-" + 
                                      formatBytes(size + 1024)] = count;
        }
        
        // Sort and get top referenced blocks
        std::sort(allBlocks.begin(), allBlocks.end(),
                 [](const auto& a, const auto& b) {
                     return a.second > b.second;
                 });
        
        size_t topN = std::min(static_cast<size_t>(topCount_), allBlocks.size());
        stats.topReferencedBlocks.assign(allBlocks.begin(), 
                                       allBlocks.begin() + topN);
        
        // Get manifest statistics
        // Note: This is simplified - in real implementation would query manifest DB
        stats.totalManifests = 0; // Would query manifest count
        stats.totalFiles = 0;     // Would sum file counts from manifests
        stats.totalFileSize = 0;  // Would sum file sizes from manifests
    }
    
    void printHumanReadable(const Stats& stats) {
        std::cout << "\n=== Kronos Storage Statistics ===\n\n";
        
        // Storage overview
        std::cout << "Storage Overview:\n";
        std::cout << "  Total blocks:        " << std::setw(12) << stats.totalBlocks << "\n";
        std::cout << "  Unique blocks:       " << std::setw(12) << stats.uniqueBlocks << "\n";
        std::cout << "  Total size:          " << std::setw(12) << formatBytes(stats.totalSize) << "\n";
        std::cout << "  Unreferenced blocks: " << std::setw(12) << stats.unreferencedBlocks;
        std::cout << " (" << formatBytes(stats.unreferencedSize) << ")\n\n";
        
        // Deduplication metrics
        std::cout << "Deduplication Metrics:\n";
        std::cout << "  Deduplication ratio: " << std::fixed << std::setprecision(2) 
                 << stats.deduplicationRatio << ":1\n";
        std::cout << "  Space savings:       " << std::fixed << std::setprecision(1) 
                 << (stats.spacesSavings * 100) << "%\n";
        std::cout << "  Logical size:        " << formatBytes(stats.deduplicatedSize) << "\n";
        std::cout << "  Average references:  " << std::fixed << std::setprecision(2) 
                 << stats.averageReferences << "\n";
        std::cout << "  Max references:      " << stats.maxReferences << "\n\n";
        
        if (detailed_) {
            // Block size distribution
            if (!stats.blockSizeDistribution.empty()) {
                std::cout << "Block Size Distribution:\n";
                for (const auto& [range, count] : stats.blockSizeDistribution) {
                    std::cout << "  " << std::setw(20) << range << ": " 
                             << std::setw(8) << count << " blocks\n";
                }
                std::cout << "\n";
            }
            
            // Top referenced blocks
            if (!stats.topReferencedBlocks.empty()) {
                std::cout << "Top Referenced Blocks:\n";
                for (const auto& [hash, refs] : stats.topReferencedBlocks) {
                    std::cout << "  " << hash.substr(0, 16) << "...: " 
                             << refs << " references\n";
                }
                std::cout << "\n";
            }
        }
        
        // Storage health
        std::cout << "Storage Health:\n";
        double unreferencedPercent = stats.uniqueBlocks > 0 ?
            (static_cast<double>(stats.unreferencedBlocks) / stats.uniqueBlocks * 100) : 0;
        std::cout << "  Unreferenced %:      " << std::fixed << std::setprecision(1) 
                 << unreferencedPercent << "%\n";
        
        if (unreferencedPercent > 10) {
            std::cout << "  Status:              Consider running garbage collection\n";
        } else {
            std::cout << "  Status:              Healthy\n";
        }
    }
    
    void printJSON(const Stats& stats) {
        nlohmann::json j;
        
        j["storage"]["totalBlocks"] = stats.totalBlocks;
        j["storage"]["uniqueBlocks"] = stats.uniqueBlocks;
        j["storage"]["totalSize"] = stats.totalSize;
        j["storage"]["unreferencedBlocks"] = stats.unreferencedBlocks;
        j["storage"]["unreferencedSize"] = stats.unreferencedSize;
        
        j["deduplication"]["ratio"] = stats.deduplicationRatio;
        j["deduplication"]["spaceSavings"] = stats.spacesSavings;
        j["deduplication"]["logicalSize"] = stats.deduplicatedSize;
        j["deduplication"]["averageReferences"] = stats.averageReferences;
        j["deduplication"]["maxReferences"] = stats.maxReferences;
        
        j["references"]["total"] = stats.totalReferences;
        
        if (detailed_) {
            j["blockSizeDistribution"] = stats.blockSizeDistribution;
            
            for (const auto& [hash, refs] : stats.topReferencedBlocks) {
                j["topReferencedBlocks"].push_back({
                    {"hash", hash},
                    {"references", refs}
                });
            }
        }
        
        std::cout << j.dump(2) << std::endl;
    }
    
    void printCSV(const Stats& stats) {
        // CSV header
        std::cout << "metric,value\n";
        
        // Output metrics
        std::cout << "total_blocks," << stats.totalBlocks << "\n";
        std::cout << "unique_blocks," << stats.uniqueBlocks << "\n";
        std::cout << "total_size," << stats.totalSize << "\n";
        std::cout << "unreferenced_blocks," << stats.unreferencedBlocks << "\n";
        std::cout << "unreferenced_size," << stats.unreferencedSize << "\n";
        std::cout << "deduplication_ratio," << stats.deduplicationRatio << "\n";
        std::cout << "space_savings," << stats.spacesSavings << "\n";
        std::cout << "logical_size," << stats.deduplicatedSize << "\n";
        std::cout << "average_references," << stats.averageReferences << "\n";
        std::cout << "max_references," << stats.maxReferences << "\n";
        std::cout << "total_references," << stats.totalReferences << "\n";
    }
    
    std::optional<std::tuple<std::unique_ptr<storage::StorageEngine>,
                            std::unique_ptr<storage::ReferenceCounter>,
                            std::unique_ptr<manifest::ManifestManager>>> 
    initializeStorage() {
        // Create storage engine
        storage::StorageConfig storageConfig{
            .basePath = getStoragePath() / "blocks",
            .shardDepth = 2,
            .mutexPoolSize = 64
        };
        
        auto storage = std::make_unique<storage::StorageEngine>(std::move(storageConfig));
        
        // Create reference counter
        storage::ReferenceCounter::Config refConfig{
            .databasePath = getStoragePath() / "refs.db",
            .enableWAL = true,
            .enableStatistics = true
        };
        
        auto refCounter = std::make_unique<storage::ReferenceCounter>(std::move(refConfig));
        
        // Create manifest manager
        manifest::ManifestManager::Config manifestConfig{
            .databasePath = getStoragePath() / "manifests.db",
            .enableCompression = true
        };
        
        auto manifestMgr = std::make_unique<manifest::ManifestManager>(std::move(manifestConfig));
        
        return std::make_tuple(std::move(storage), std::move(refCounter), std::move(manifestMgr));
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
    std::string outputFormat_ = "human";
    bool detailed_ = false;
    int topCount_ = 10;
};

} // namespace kronos::tools