#include <yams/manifest/manifest_manager.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>
#include <yams/tools/command.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#if defined(_WIN32)
#include <io.h>
#define ISATTY _isatty
#define FILENO _fileno
#else
#include <unistd.h>
#include <sys/ioctl.h>
#define ISATTY isatty
#define FILENO fileno
#endif

namespace yams::tools {

class StatsCommand : public Command {
public:
    StatsCommand() : Command("stats", "Display storage statistics and deduplication metrics") {}

    void setupOptions(CLI::App& app) override {
        auto* stats = app.add_subcommand("stats", getDescription());

        stats->add_option("-f,--format", outputFormat_, "Output format (human, json, csv)")
            ->default_val("human")
            ->check(CLI::IsMember({"human", "json", "csv"}));

        stats->add_flag("-d,--detailed", detailed_,
                        "Show detailed statistics including distribution");

        stats->add_flag("-h,--human-readable", humanReadable_,
                        "Display sizes in human readable format (like df -h)");

        stats->add_option("--top", topCount_, "Number of top items to show")->default_val(10);

        addCommonOptions(*stats);
        stats->callback([this]() { shouldExecute_ = true; });
    }

    int execute() override {
        if (!shouldExecute_)
            return 0;

        try {
            logVerbose("Gathering storage statistics...");

            // Initialize storage components
            auto storageResult = initializeStorage();
            if (!storageResult)
                return 1;

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

    // === UI helpers ===
    struct Ansi {
        static constexpr const char* RESET = "\x1b[0m";
        static constexpr const char* BOLD = "\x1b[1m";
        static constexpr const char* DIM = "\x1b[2m";
        static constexpr const char* RED = "\x1b[31m";
        static constexpr const char* GREEN = "\x1b[32m";
        static constexpr const char* YELLOW = "\x1b[33m";
        static constexpr const char* BLUE = "\x1b[34m";
        static constexpr const char* MAGENTA = "\x1b[35m";
        static constexpr const char* CYAN = "\x1b[36m";
        static constexpr const char* WHITE = "\x1b[37m";
    };

    bool colorsEnabled() const {
        const char* noColor = std::getenv("NO_COLOR");
        if (noColor != nullptr)
            return false;
        return ISATTY(FILENO(stdout));
    }

    std::string colorize(const std::string& s, const char* code) const {
        if (!colorsEnabled())
            return s;
        return std::string(code) + s + Ansi::RESET;
    }

    int detectTerminalWidth() const {
#if defined(_WIN32)
        // Fallback to environment or default on Windows
        const char* cols = std::getenv("COLUMNS");
        if (cols) {
            try {
                return std::max(40, std::stoi(cols));
            } catch (...) {
            }
        }
        return 100;
#else
        struct winsize w{};
        if (ioctl(FILENO(stdout), TIOCGWINSZ, &w) == 0 && w.ws_col > 0) {
            return std::max<int>(60, w.ws_col);
        }
        const char* cols = std::getenv("COLUMNS");
        if (cols) {
            try {
                return std::max(60, std::stoi(cols));
            } catch (...) {
            }
        }
        return 100;
#endif
    }

    static std::string repeat(char ch, size_t n) { return std::string(n, ch); }

    static std::string padRight(const std::string& s, size_t width, char fill = ' ') {
        if (s.size() >= width)
            return s;
        return s + std::string(width - s.size(), fill);
    }

    static std::string padLeft(const std::string& s, size_t width, char fill = ' ') {
        if (s.size() >= width)
            return s;
        return std::string(width - s.size(), fill) + s;
    }

    std::string progressBar(double fraction, size_t width, const char* good = Ansi::GREEN,
                            const char* warn = Ansi::YELLOW, const char* bad = Ansi::RED) const {
        if (width == 0)
            return "";
        double f = fraction;
        if (f < 0.0)
            f = 0.0;
        if (f > 1.0)
            f = 1.0;
        size_t filled = static_cast<size_t>(std::llround(f * static_cast<double>(width)));
        std::string filledPart = repeat('#', filled);
        std::string emptyPart = repeat('-', width - filled);

        const char* colorCode = good;
        if (f >= 0.66)
            colorCode = bad;
        else if (f >= 0.33)
            colorCode = warn;

        if (colorsEnabled()) {
            return std::string(colorCode) + filledPart + Ansi::RESET + emptyPart;
        }
        return filledPart + emptyPart;
    }

    std::string sectionHeader(const std::string& title, int width) const {
        std::string shown = " " + title + " ";
        int lineLen = std::max(0, width - static_cast<int>(shown.size()) - 4);
        std::string line = repeat('-', static_cast<size_t>(lineLen));
        std::string decorated = "[ " + title + " ] " + line;
        return colorsEnabled() ? colorize(decorated, Ansi::CYAN) : decorated;
    }

    std::string titleBanner(const std::string& title, int width) const {
        std::string t = " " + title + " ";
        int side = std::max(0, (width - static_cast<int>(t.size()) - 4) / 2);
        std::string left = repeat('=', static_cast<size_t>(side));
        std::string right =
            repeat('=', static_cast<size_t>(width - side - static_cast<int>(t.size()) - 4));
        std::string line = "==" + left + t + right + "==";
        return colorsEnabled() ? colorize(line, Ansi::MAGENTA) : line;
    }

    Stats gatherStatistics(storage::StorageEngine& storage, storage::ReferenceCounter& refCounter,
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
            stats.averageReferences =
                static_cast<double>(stats.totalReferences) / stats.uniqueBlocks;
        }

        // Calculate deduplication metrics
        if (stats.totalReferences > 0) {
            // Deduplicated size is what would be stored without dedup
            stats.deduplicatedSize = stats.totalSize * stats.averageReferences;
            stats.deduplicationRatio =
                static_cast<double>(stats.deduplicatedSize) / stats.totalSize;
            stats.spacesSavings =
                1.0 - (static_cast<double>(stats.totalSize) / stats.deduplicatedSize);
        }

        // Get detailed statistics if requested
        if (detailed_) {
            gatherDetailedStatistics(stats, storage, refCounter, manifestMgr);
        }

        return stats;
    }

    void gatherDetailedStatistics(Stats& stats, storage::StorageEngine& storage,
                                  storage::ReferenceCounter& refCounter,
                                  manifest::ManifestManager& manifestMgr) {
        // Block size distribution
        std::map<uint64_t, uint64_t> sizeHistogram;

        // Top referenced blocks
        std::vector<std::pair<std::string, uint64_t>> allBlocks;

        // Query all blocks with reference counts
        auto queryResult =
            refCounter.queryBlocks([&](const std::string& hash, uint64_t refCount, uint64_t size) {
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
            stats.blockSizeDistribution[formatBytes(size) + "-" + formatBytes(size + 1024)] = count;
        }

        // Sort and get top referenced blocks
        std::sort(allBlocks.begin(), allBlocks.end(),
                  [](const auto& a, const auto& b) { return a.second > b.second; });

        size_t topN = std::min(static_cast<size_t>(topCount_), allBlocks.size());
        stats.topReferencedBlocks.assign(allBlocks.begin(), allBlocks.begin() + topN);

        // Get manifest statistics
        // Note: This is simplified - in real implementation would query manifest DB
        stats.totalManifests = 0; // Would query manifest count
        stats.totalFiles = 0;     // Would sum file counts from manifests
        stats.totalFileSize = 0;  // Would sum file sizes from manifests
    }

    void printHumanReadable(const Stats& stats) {
        std::cout << "\n=== YAMS Storage Statistics ===\n\n";

        // Storage Overview
        std::cout << "Storage Overview:\n";
        std::cout << "  Total blocks:       " << std::to_string(stats.totalBlocks) << "\n";
        std::cout << "  Unique blocks:      " << std::to_string(stats.uniqueBlocks) << "\n";
        std::cout << "  Total size:         " << formatSize(stats.totalSize) << "\n";
        std::cout << "  Unreferenced:       " << std::to_string(stats.unreferencedBlocks)
                  << " blocks (" << formatSize(stats.unreferencedSize) << ")\n";
        std::cout << "\n";

        // Deduplication Metrics
        std::cout << "Deduplication Metrics:\n";
        std::cout << "  Dedup ratio:        " << std::fixed << std::setprecision(2)
                  << stats.deduplicationRatio << ":1\n";

        double savings = stats.spacesSavings;
        if (savings < 0.0)
            savings = 0.0;
        if (savings > 0.999999)
            savings = 0.999999;
        std::cout << "  Space savings:      " << std::fixed << std::setprecision(1)
                  << (savings * 100.0) << "%\n";

        std::cout << "  Logical size:       " << formatSize(stats.deduplicatedSize) << "\n";
        std::cout << "  Avg references:     " << std::fixed << std::setprecision(2)
                  << stats.averageReferences << "\n";
        std::cout << "  Max references:     " << stats.maxReferences << "\n";
        std::cout << "\n";

        if (detailed_) {
            // Block Size Distribution
            if (!stats.blockSizeDistribution.empty()) {
                std::cout << "Block Size Distribution:\n";
                for (const auto& [range, count] : stats.blockSizeDistribution) {
                    std::cout << "  " << std::left << std::setw(20) << range << std::right
                              << std::setw(10) << count << " blocks\n";
                }
                std::cout << "\n";
            }

            // Top Referenced Blocks
            if (!stats.topReferencedBlocks.empty()) {
                std::cout << "Top Referenced Blocks:\n";
                for (const auto& [hash, refs] : stats.topReferencedBlocks) {
                    std::string h = hash.size() > 16 ? hash.substr(0, 16) + "..." : hash;
                    std::cout << "  " << std::left << std::setw(20) << h << std::right
                              << std::setw(8) << refs << " refs\n";
                }
                std::cout << "\n";
            }
        }

        // Storage Health
        std::cout << "Storage Health:\n";
        double unrefPct = stats.uniqueBlocks > 0
                              ? (static_cast<double>(stats.unreferencedBlocks) / stats.uniqueBlocks)
                              : 0.0;
        std::cout << "  Unreferenced:       " << std::fixed << std::setprecision(1)
                  << (unrefPct * 100.0) << "%\n";

        if (unrefPct > 0.10) {
            std::cout << "  Status:             Consider running garbage collection\n";
        } else {
            std::cout << "  Status:             Healthy\n";
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
                j["topReferencedBlocks"].push_back({{"hash", hash}, {"references", refs}});
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
            .basePath = getStoragePath() / "blocks", .shardDepth = 2, .mutexPoolSize = 64};

        auto storage = std::make_unique<storage::StorageEngine>(std::move(storageConfig));

        // Create reference counter
        storage::ReferenceCounter::Config refConfig{.databasePath = getStoragePath() / "refs.db",
                                                    .enableWAL = true,
                                                    .enableStatistics = true};

        auto refCounter = std::make_unique<storage::ReferenceCounter>(std::move(refConfig));

        // Create manifest manager
        manifest::ManifestManager::Config manifestConfig{
            .databasePath = getStoragePath() / "manifests.db", .enableCompression = true};

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

    std::string formatSize(uint64_t bytes) const {
        if (humanReadable_) {
            return formatBytes(bytes);
        } else {
            return std::to_string(bytes);
        }
    }

    bool shouldExecute_ = false;
    std::string outputFormat_ = "human";
    bool detailed_ = false;
    bool humanReadable_ = false;
    int topCount_ = 10;
};

} // namespace yams::tools