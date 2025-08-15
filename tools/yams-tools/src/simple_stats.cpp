/**
 * Simple statistics tool for YAMS storage
 *
 * This is a simplified version that doesn't require external dependencies
 */

#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>

#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sstream>

using namespace yams;

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
    std::cout << "Usage: " << program << " <storage-path>\n";
    std::cout << "\nDisplay statistics for YAMS storage at the specified path.\n";
}

int main(int argc, char** argv) {
    if (argc != 2) {
        printUsage(argv[0]);
        return 1;
    }

    std::filesystem::path storagePath = argv[1];

    if (!std::filesystem::exists(storagePath)) {
        std::cerr << "Error: Storage path does not exist: " << storagePath << std::endl;
        return 1;
    }

    try {
        // Initialize storage engine
        storage::StorageConfig storageConfig{
            .basePath = storagePath / "blocks", .shardDepth = 2, .mutexPoolSize = 64};

        storage::StorageEngine storage(std::move(storageConfig));

        // Initialize reference counter
        storage::ReferenceCounter::Config refConfig{
            .databasePath = storagePath / "refs.db", .enableWAL = true, .enableStatistics = true};

        storage::ReferenceCounter refCounter(std::move(refConfig));

        // Gather statistics
        auto storageStats = storage.getStats();
        auto refStatsResult = refCounter.getStats();
        if (!refStatsResult.has_value()) {
            std::cerr << "Error: Failed to get reference statistics" << std::endl;
            return 1;
        }
        const auto& refStats = refStatsResult.value();

        // Calculate deduplication metrics
        double avgRefs = refStats.totalBlocks > 0
                             ? static_cast<double>(refStats.totalReferences) / refStats.totalBlocks
                             : 0.0;

        uint64_t deduplicatedSize = static_cast<uint64_t>(storageStats.totalBytes.load() * avgRefs);
        double dedupRatio = storageStats.totalBytes > 0
                                ? static_cast<double>(deduplicatedSize) / storageStats.totalBytes
                                : 0.0;
        double spaceSavings =
            deduplicatedSize > 0
                ? (1.0 - static_cast<double>(storageStats.totalBytes) / deduplicatedSize) * 100.0
                : 0.0;

        // Display statistics
        std::cout << "\n=== YAMS Storage Statistics ===\n\n";

        std::cout << "Storage Path: " << storagePath << "\n\n";

        std::cout << "Storage Overview:\n";
        std::cout << "  Total blocks:        " << std::setw(12) << storageStats.totalObjects.load()
                  << "\n";
        std::cout << "  Unique blocks:       " << std::setw(12) << refStats.totalBlocks << "\n";
        std::cout << "  Total size:          " << std::setw(12)
                  << formatBytes(storageStats.totalBytes) << "\n";
        std::cout << "  Unreferenced blocks: " << std::setw(12) << refStats.unreferencedBlocks;
        std::cout << " (" << formatBytes(refStats.unreferencedBytes) << ")\n\n";

        std::cout << "Deduplication Metrics:\n";
        std::cout << "  Deduplication ratio: " << std::fixed << std::setprecision(2) << dedupRatio
                  << ":1\n";
        std::cout << "  Space savings:       " << std::fixed << std::setprecision(1) << spaceSavings
                  << "%\n";
        std::cout << "  Logical size:        " << formatBytes(deduplicatedSize) << "\n";
        std::cout << "  Average references:  " << std::fixed << std::setprecision(2) << avgRefs
                  << "\n\n";

        std::cout << "Operation Statistics:\n";
        std::cout << "  Write operations:    " << std::setw(12)
                  << storageStats.writeOperations.load() << "\n";
        std::cout << "  Read operations:     " << std::setw(12)
                  << storageStats.readOperations.load() << "\n";
        std::cout << "  Delete operations:   " << std::setw(12)
                  << storageStats.deleteOperations.load() << "\n";
        std::cout << "  Failed operations:   " << std::setw(12)
                  << storageStats.failedOperations.load() << "\n\n";

        std::cout << "Reference Statistics:\n";
        std::cout << "  Total references:    " << std::setw(12) << refStats.totalReferences << "\n";
        std::cout << "  Transactions:        " << std::setw(12) << refStats.transactions << "\n";
        std::cout << "  Rollbacks:          " << std::setw(12) << refStats.rollbacks << "\n\n";

        // Storage health
        std::cout << "Storage Health:\n";
        double unreferencedPercent =
            refStats.totalBlocks > 0
                ? (static_cast<double>(refStats.unreferencedBlocks) / refStats.totalBlocks * 100)
                : 0;
        std::cout << "  Unreferenced %:      " << std::fixed << std::setprecision(1)
                  << unreferencedPercent << "%\n";

        if (unreferencedPercent > 10) {
            std::cout << "  Status:              Consider running garbage collection\n";
        } else {
            std::cout << "  Status:              Healthy\n";
        }

        std::cout << std::endl;

        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}