/**
 * Simple garbage collection tool for Kronos storage
 * 
 * Direct implementation without GarbageCollector class
 */

#include <yams/storage/storage_engine.h>
#include <yams/storage/reference_counter.h>

#include <iostream>
#include <iomanip>
#include <filesystem>
#include <sstream>
#include <chrono>
#include <vector>

using namespace kronos;

std::string formatBytes(uint64_t bytes) {
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

void printUsage(const char* program) {
    std::cout << "Usage: " << program << " <storage-path> [options]\n";
    std::cout << "\nOptions:\n";
    std::cout << "  --dry-run    Perform a trial run with no changes made\n";
    std::cout << "  --force      Skip confirmation prompt\n";
    std::cout << "\nRemove unreferenced blocks from Kronos storage.\n";
}

int main(int argc, char** argv) {
    if (argc < 2) {
        printUsage(argv[0]);
        return 1;
    }
    
    std::filesystem::path storagePath = argv[1];
    bool dryRun = false;
    bool force = false;
    
    // Parse options
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--dry-run") {
            dryRun = true;
        } else if (arg == "--force") {
            force = true;
        } else {
            std::cerr << "Unknown option: " << arg << std::endl;
            printUsage(argv[0]);
            return 1;
        }
    }
    
    if (!std::filesystem::exists(storagePath)) {
        std::cerr << "Error: Storage path does not exist: " << storagePath << std::endl;
        return 1;
    }
    
    try {
        std::cout << "Initializing storage..." << std::endl;
        
        // Initialize storage engine
        storage::StorageConfig storageConfig{
            .basePath = storagePath / "blocks",
            .shardDepth = 2,
            .mutexPoolSize = 64
        };
        
        storage::StorageEngine storage(std::move(storageConfig));
        
        // Initialize reference counter
        storage::ReferenceCounter::Config refConfig{
            .databasePath = storagePath / "refs.db",
            .enableWAL = true,
            .enableStatistics = true
        };
        
        storage::ReferenceCounter refCounter(std::move(refConfig));
        
        // Get initial statistics
        auto preStatsResult = refCounter.getStats();
        if (!preStatsResult.has_value()) {
            std::cerr << "Error: Failed to get reference statistics" << std::endl;
            return 1;
        }
        const auto& preStats = preStatsResult.value();
        
        std::cout << "\nCurrent storage state:" << std::endl;
        std::cout << "  Total blocks:        " << preStats.totalBlocks << std::endl;
        std::cout << "  Unreferenced blocks: " << preStats.unreferencedBlocks << std::endl;
        std::cout << "  Unreferenced size:   " << formatBytes(preStats.unreferencedBytes) << std::endl;
        
        if (preStats.unreferencedBlocks == 0) {
            std::cout << "\nNo unreferenced blocks to collect." << std::endl;
            return 0;
        }
        
        // Confirm with user
        if (!force && !dryRun) {
            std::cout << "\nThis will delete " << preStats.unreferencedBlocks 
                     << " unreferenced blocks (" << formatBytes(preStats.unreferencedBytes) << ")." << std::endl;
            std::cout << "Continue? [y/N] ";
            
            std::string response;
            std::getline(std::cin, response);
            if (response != "y" && response != "Y") {
                std::cout << "Garbage collection cancelled." << std::endl;
                return 0;
            }
        }
        
        std::cout << "\nCollecting unreferenced blocks" << (dryRun ? " (dry-run mode)" : "") << "..." << std::endl;
        
        auto startTime = std::chrono::steady_clock::now();
        
        // Get unreferenced blocks
        auto unreferencedResult = refCounter.getUnreferencedBlocks(1000); // Batch size
        if (!unreferencedResult.has_value()) {
            std::cerr << "Error: Failed to get unreferenced blocks: " << unreferencedResult.error().message << std::endl;
            return 1;
        }
        
        const auto& unreferenced = unreferencedResult.value();
        
        size_t blocksDeleted = 0;
        uint64_t bytesReclaimed = 0;
        std::vector<std::string> errors;
        
        // Process unreferenced blocks
        for (const auto& hash : unreferenced) {
            uint64_t blockSize = 0;
            
            // Get block size before deletion
            if (std::filesystem::exists(storagePath / "blocks" / hash.substr(0, 2) / hash)) {
                blockSize = std::filesystem::file_size(storagePath / "blocks" / hash.substr(0, 2) / hash);
            }
            
            if (!dryRun) {
                // Remove from storage
                auto removeResult = storage.remove(hash);
                if (!removeResult.has_value()) {
                    errors.push_back("Failed to remove block " + hash.substr(0, 8) + ": " + 
                                   removeResult.error().message);
                    continue;
                }
                
                // Note: The reference counter should clean up unreferenced entries automatically
                // No need to manually delete references
            }
            
            blocksDeleted++;
            bytesReclaimed += blockSize;
            
            // Progress indicator
            if (blocksDeleted % 100 == 0) {
                std::cout << "\rDeleted " << blocksDeleted << " blocks..." << std::flush;
            }
        }
        
        auto endTime = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);
        
        // Clear progress line
        std::cout << "\r" << std::string(50, ' ') << "\r";
        
        // Display results
        std::cout << "\nGarbage collection completed:" << std::endl;
        std::cout << "  Blocks scanned:    " << unreferenced.size() << std::endl;
        std::cout << "  Blocks deleted:    " << blocksDeleted << std::endl;
        std::cout << "  Space reclaimed:   " << formatBytes(bytesReclaimed) << std::endl;
        std::cout << "  Duration:          " << duration.count() << "s" << std::endl;
        
        if (!errors.empty()) {
            std::cout << "\nErrors encountered:" << std::endl;
            for (const auto& error : errors) {
                std::cerr << "  " << error << std::endl;
            }
        }
        
        // Get post-collection statistics
        if (!dryRun) {
            auto postStatsResult = refCounter.getStats();
            if (!postStatsResult.has_value()) {
                std::cerr << "Error: Failed to get post-collection statistics" << std::endl;
                return 1;
            }
            const auto& postStats = postStatsResult.value();
            std::cout << "\nStorage state after GC:" << std::endl;
            std::cout << "  Total blocks:        " << postStats.totalBlocks << std::endl;
            std::cout << "  Unreferenced blocks: " << postStats.unreferencedBlocks << std::endl;
        }
        
        return errors.empty() ? 0 : 1;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}