#include <yams/manifest/manifest_manager.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>
#include <yams/tools/command.h>
// Daemon metrics (prefer daemon-first, avoid local scans)
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

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
            logVerbose("Gathering storage statistics (daemon-first)...");

            // Try daemon-first lightweight status
            bool rendered = false;
            do {
                yams::daemon::ClientConfig cfg;
                cfg.enableChunkedResponses = false;
                cfg.requestTimeout = std::chrono::milliseconds(2000);
                auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
                if (!leaseRes)
                    break;
                auto lease = std::move(leaseRes.value());
                auto& client = **lease;
                yams::daemon::StatusRequest sreq;
                sreq.detailed = false; // never trigger physical scanning from CLI tools
                auto sres = client.call_sync(sreq);
                if (!sres)
                    break;
                const auto& st = sres.value();

                // Safe helper to get value from requestCounts without throwing
                auto safeGet = [&](const char* key) -> int64_t {
                    auto it = st.requestCounts.find(key);
                    return (it != st.requestCounts.end()) ? it->second : 0;
                };

                // Render a compact breakdown using requestCounts keys if present
                if (outputFormat_ == "json") {
                    renderDaemonStatsJson(st, safeGet);
                } else if (detailed_) {
                    renderDaemonStatsDetailed(st, safeGet);
                } else {
                    renderDaemonStatsCompact(st, safeGet);
                }
                rendered = true;
            } while (false);

            if (rendered)
                return 0;

            // Fallback to legacy local scans only if daemon not available
            auto storageResult = initializeStorage();
            if (!storageResult)
                return 1;
            auto [storage, refCounter, manifestMgr] = *storageResult;
            Stats stats = gatherStatistics(*storage, *refCounter, *manifestMgr);
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

    // Use shared UI helpers from yams::cli::ui
    using yams::cli::ui::Ansi;
    using yams::cli::ui::colorize;
    using yams::cli::ui::colors_enabled;
    using yams::cli::ui::format_bytes;
    using yams::cli::ui::format_number;
    using yams::cli::ui::format_percentage;
    using yams::cli::ui::pad_right;
    using yams::cli::ui::progress_bar;
    using yams::cli::ui::render_rows;
    using yams::cli::ui::Row;
    using yams::cli::ui::section_header;
    using yams::cli::ui::status_error;
    using yams::cli::ui::status_ok;
    using yams::cli::ui::status_warning;
    using yams::cli::ui::subsection_header;
    using yams::cli::ui::terminal_width;
    using yams::cli::ui::title_banner;

    // factory in TU
    std::unique_ptr<Command> createStatsCommand() { return std::make_unique<StatsCommand>(); }

    enum class Severity { Good, Warn, Bad };

    std::string paint(Severity sev, const std::string& text) const {
        const char* color = Ansi::GREEN;
        const char* icon = "✓";
        switch (sev) {
            case Severity::Good:
                break;
            case Severity::Warn:
                color = Ansi::YELLOW;
                icon = "⚠";
                break;
            case Severity::Bad:
                color = Ansi::RED;
                icon = "✗";
                break;
        }
        return colorize(std::string(icon) + " " + text, color);
    }

    std::string neutral(const std::string& text) const { return colorize(text, Ansi::WHITE); }

    // Render JSON output for daemon stats
    void renderDaemonStatsJson(const yams::daemon::StatusResponse& st,
                               std::function<int64_t(const char*)> safeGet) const {
        nlohmann::json j;
        j["docs"] = safeGet("storage_documents");
        j["logical_bytes"] = safeGet("storage_logical_bytes");
        j["physical_bytes"] = safeGet("storage_physical_bytes");

        // CAS savings
        int64_t dedupSaved = safeGet("casDedupSavedBytes");
        int64_t compressSaved = safeGet("casCompressSavedBytes");
        if (dedupSaved > 0)
            j["cas_dedup_saved_bytes"] = dedupSaved;
        if (compressSaved > 0)
            j["cas_compress_saved_bytes"] = compressSaved;

        // Disk usage breakdown
        const char* overheadKeys[] = {"metadataPhysicalBytes",       "indexPhysicalBytes",
                                      "vectorPhysicalBytes",         "logsTmpPhysicalBytes",
                                      "storageObjectsPhysicalBytes", "storageRefsDbPhysicalBytes"};
        for (auto* k : overheadKeys) {
            int64_t val = safeGet(k);
            if (val > 0)
                j["disk_usage"][k] = val;
        }

        // Dedup stats
        int64_t totalBlocks = safeGet("cas_total_blocks");
        int64_t uniqueBlocks = safeGet("cas_unique_blocks");
        if (totalBlocks > 0 || uniqueBlocks > 0) {
            j["dedup"]["total_blocks"] = totalBlocks;
            j["dedup"]["unique_blocks"] = uniqueBlocks;
            if (uniqueBlocks > 0 && totalBlocks > 0) {
                j["dedup"]["ratio"] = static_cast<double>(totalBlocks) / uniqueBlocks;
            }
        }

        std::cout << j.dump(2) << std::endl;
    }

    // Render compact one-line output
    void renderDaemonStatsCompact(const yams::daemon::StatusResponse& st,
                                  std::function<int64_t(const char*)> safeGet) const {
        auto fmt = [&](const char* k) -> std::string {
            int64_t val = safeGet(k);
            if (val == 0)
                return std::string("n/a");
            return format_bytes(static_cast<uint64_t>(val));
        };

        uint64_t docs = static_cast<uint64_t>(std::max<int64_t>(0, safeGet("storage_documents")));
        std::cout << "STOR : ok, docs=" << docs << ", logical=" << fmt("storage_logical_bytes")
                  << ", physical=" << fmt("storage_physical_bytes");

        // Show savings if available
        int64_t dedupSaved = safeGet("casDedupSavedBytes");
        int64_t compressSaved = safeGet("casCompressSavedBytes");
        int64_t totalSaved = dedupSaved + compressSaved;
        if (totalSaved > 0) {
            std::cout << ", saved=" << format_bytes(static_cast<uint64_t>(totalSaved))
                      << " (dedup=" << fmt("casDedupSavedBytes")
                      << ", compress=" << fmt("casCompressSavedBytes") << ")";
        }
        std::cout << "\n";
    }

    // Render detailed stats view (similar to daemon status -d)
    void renderDaemonStatsDetailed(const yams::daemon::StatusResponse& st,
                                   std::function<int64_t(const char*)> safeGet) const {
        std::cout << "\n" << title_banner("YAMS Storage Statistics") << "\n\n";

        // Storage Overview
        std::vector<Row> overview;
        uint64_t docs = static_cast<uint64_t>(std::max<int64_t>(0, safeGet("storage_documents")));
        uint64_t logicalBytes =
            static_cast<uint64_t>(std::max<int64_t>(0, safeGet("storage_logical_bytes")));
        uint64_t physicalBytes =
            static_cast<uint64_t>(std::max<int64_t>(0, safeGet("storage_physical_bytes")));

        overview.push_back({"Documents", format_number(docs), ""});
        overview.push_back({"Logical size", format_bytes(logicalBytes), "total content size"});
        overview.push_back({"Physical size", format_bytes(physicalBytes), "on disk"});

        // Compression ratio
        if (logicalBytes > 0 && physicalBytes > 0) {
            double ratio = static_cast<double>(logicalBytes) / static_cast<double>(physicalBytes);
            std::ostringstream ratioStr;
            ratioStr << std::fixed << std::setprecision(2) << ratio << ":1";
            Severity sev =
                ratio >= 1.5 ? Severity::Good : (ratio >= 1.0 ? Severity::Warn : Severity::Bad);
            overview.push_back({"Compression", paint(sev, ratioStr.str()), ""});
        }
        render_rows(std::cout, overview);

        // CAS Deduplication Section
        int64_t dedupSaved = safeGet("casDedupSavedBytes");
        int64_t compressSaved = safeGet("casCompressSavedBytes");
        int64_t totalSaved = dedupSaved + compressSaved;

        if (totalSaved > 0 || safeGet("cas_total_blocks") > 0) {
            std::cout << "\n" << section_header("Deduplication & Compression") << "\n\n";
            std::vector<Row> dedupRows;

            if (totalSaved > 0) {
                dedupRows.push_back(
                    {"Total saved",
                     paint(Severity::Good, format_bytes(static_cast<uint64_t>(totalSaved))), ""});
                if (dedupSaved > 0) {
                    dedupRows.push_back({"  Dedup savings",
                                         format_bytes(static_cast<uint64_t>(dedupSaved)),
                                         "block-level"});
                }
                if (compressSaved > 0) {
                    dedupRows.push_back({"  Compress savings",
                                         format_bytes(static_cast<uint64_t>(compressSaved)),
                                         "lz4/zstd"});
                }
            }

            int64_t totalBlocks = safeGet("cas_total_blocks");
            int64_t uniqueBlocks = safeGet("cas_unique_blocks");
            if (totalBlocks > 0 || uniqueBlocks > 0) {
                dedupRows.push_back(
                    {"Total blocks", format_number(static_cast<uint64_t>(totalBlocks)), ""});
                dedupRows.push_back(
                    {"Unique blocks", format_number(static_cast<uint64_t>(uniqueBlocks)), ""});
                if (uniqueBlocks > 0 && totalBlocks > uniqueBlocks) {
                    double dedupRatio =
                        static_cast<double>(totalBlocks) / static_cast<double>(uniqueBlocks);
                    std::ostringstream ratioStr;
                    ratioStr << std::fixed << std::setprecision(2) << dedupRatio << ":1";
                    dedupRows.push_back({"Dedup ratio", ratioStr.str(), ""});
                }
            }
            render_rows(std::cout, dedupRows);
        }

        // Disk Usage Breakdown
        int64_t storageObjects = safeGet("storageObjectsPhysicalBytes");
        int64_t storageRefsDb = safeGet("storageRefsDbPhysicalBytes");
        int64_t metadataBytes = safeGet("metadataPhysicalBytes");
        int64_t indexBytes = safeGet("indexPhysicalBytes");
        int64_t vectorBytes = safeGet("vectorPhysicalBytes");
        int64_t vectorIndexBytes = safeGet("vectorIndexPhysicalBytes");
        int64_t logsTmpBytes = safeGet("logsTmpPhysicalBytes");

        if (storageObjects > 0 || storageRefsDb > 0 || metadataBytes > 0 || indexBytes > 0 ||
            vectorBytes > 0 || vectorIndexBytes > 0 || logsTmpBytes > 0) {
            std::cout << "\n" << section_header("Disk Usage Breakdown") << "\n\n";
            std::vector<Row> diskRows;

            if (storageObjects > 0) {
                diskRows.push_back({"CAS objects",
                                    format_bytes(static_cast<uint64_t>(storageObjects)),
                                    "content blocks"});
            }
            if (storageRefsDb > 0) {
                diskRows.push_back({"Reference DB",
                                    format_bytes(static_cast<uint64_t>(storageRefsDb)), "refs.db"});
            }
            if (metadataBytes > 0) {
                diskRows.push_back({"Metadata", format_bytes(static_cast<uint64_t>(metadataBytes)),
                                    "metadata.db"});
            }
            if (indexBytes > 0) {
                diskRows.push_back(
                    {"Search index", format_bytes(static_cast<uint64_t>(indexBytes)), "tantivy"});
            }
            if (vectorBytes > 0) {
                diskRows.push_back(
                    {"Vector DB", format_bytes(static_cast<uint64_t>(vectorBytes)), "vectors.db"});
            }
            if (vectorIndexBytes > 0) {
                diskRows.push_back({"Vector index",
                                    format_bytes(static_cast<uint64_t>(vectorIndexBytes)), "HNSW"});
            }
            if (logsTmpBytes > 0) {
                diskRows.push_back(
                    {"Logs/tmp", format_bytes(static_cast<uint64_t>(logsTmpBytes)), ""});
            }

            // Total overhead (non-CAS)
            int64_t overhead = metadataBytes + indexBytes + vectorBytes + vectorIndexBytes +
                               logsTmpBytes + storageRefsDb;
            if (overhead > 0 && storageObjects > 0) {
                double overheadPct = 100.0 * static_cast<double>(overhead) /
                                     static_cast<double>(storageObjects + overhead);
                std::ostringstream pctStr;
                pctStr << std::fixed << std::setprecision(1) << overheadPct << "% overhead";
                diskRows.push_back({"", "", pctStr.str()});
            }
            render_rows(std::cout, diskRows);
        }

        // Vector Embeddings
        if (st.embeddingAvailable || st.vectorDbDim > 0 || st.embeddingDim > 0) {
            std::cout << "\n" << section_header("Vector Embeddings") << "\n\n";
            std::vector<Row> vecRows;

            Severity sev = st.embeddingAvailable ? Severity::Good : Severity::Warn;
            vecRows.push_back({"Embeddings",
                               paint(sev, st.embeddingAvailable ? "Available" : "Unavailable"),
                               ""});

            if (!st.embeddingModel.empty()) {
                vecRows.push_back({"Model", st.embeddingModel, ""});
            }
            if (!st.embeddingBackend.empty()) {
                vecRows.push_back({"Backend", st.embeddingBackend, ""});
            }
            if (st.embeddingDim > 0) {
                vecRows.push_back({"Dimension", std::to_string(st.embeddingDim), ""});
            }
            if (st.vectorDbDim > 0 && st.vectorDbDim != st.embeddingDim) {
                vecRows.push_back({"Vector DB dim", std::to_string(st.vectorDbDim), ""});
            }

            int64_t vectorCount = safeGet("vector_count");
            if (vectorCount > 0) {
                vecRows.push_back(
                    {"Vectors indexed", format_number(static_cast<uint64_t>(vectorCount)), ""});
            }
            render_rows(std::cout, vecRows);
        }

        // Search Metrics
        if (st.searchMetrics.executed > 0 || st.searchMetrics.active > 0) {
            std::cout << "\n" << section_header("Search Metrics") << "\n\n";
            std::vector<Row> searchRows;

            searchRows.push_back(
                {"Queries executed", format_number(st.searchMetrics.executed), ""});

            if (st.searchMetrics.active > 0 || st.searchMetrics.queued > 0) {
                std::ostringstream active;
                active << st.searchMetrics.active << " active";
                if (st.searchMetrics.queued > 0) {
                    active << " · " << st.searchMetrics.queued << " queued";
                }
                Severity sev =
                    st.searchMetrics.queued > 50
                        ? Severity::Bad
                        : (st.searchMetrics.queued > 10 ? Severity::Warn : Severity::Good);
                searchRows.push_back({"Active", paint(sev, active.str()), ""});
            }

            if (st.searchMetrics.cacheHitRate > 0) {
                std::ostringstream hitRate;
                hitRate << std::fixed << std::setprecision(1)
                        << (st.searchMetrics.cacheHitRate * 100.0) << "%";
                Severity sev =
                    st.searchMetrics.cacheHitRate >= 0.5
                        ? Severity::Good
                        : (st.searchMetrics.cacheHitRate >= 0.2 ? Severity::Warn : Severity::Bad);
                searchRows.push_back({"Cache hit rate", paint(sev, hitRate.str()), ""});
            }

            if (st.searchMetrics.avgLatencyUs > 0) {
                std::ostringstream latency;
                if (st.searchMetrics.avgLatencyUs >= 1000000) {
                    latency << std::fixed << std::setprecision(2)
                            << (st.searchMetrics.avgLatencyUs / 1000000.0) << "s";
                } else if (st.searchMetrics.avgLatencyUs >= 1000) {
                    latency << std::fixed << std::setprecision(1)
                            << (st.searchMetrics.avgLatencyUs / 1000.0) << "ms";
                } else {
                    latency << st.searchMetrics.avgLatencyUs << "µs";
                }
                searchRows.push_back({"Avg latency", latency.str(), ""});
            }
            render_rows(std::cout, searchRows);
        }

        std::cout << "\n";
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
            stats.blockSizeDistribution[format_bytes(size) + "-" + format_bytes(size + 1024)] =
                count;
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
        std::cout << "\n" << title_banner("YAMS Storage Statistics (Local)") << "\n\n";

        // Storage Overview
        std::vector<Row> overview;
        overview.push_back({"Total blocks", format_number(stats.totalBlocks), ""});
        overview.push_back({"Unique blocks", format_number(stats.uniqueBlocks), ""});
        overview.push_back({"Total size", localFormatSize(stats.totalSize), ""});

        std::ostringstream unrefStr;
        unrefStr << stats.unreferencedBlocks << " blocks";
        if (stats.unreferencedSize > 0) {
            unrefStr << " (" << localFormatSize(stats.unreferencedSize) << ")";
        }
        overview.push_back({"Unreferenced", unrefStr.str(), ""});
        render_rows(std::cout, overview);

        // Deduplication Metrics
        std::cout << "\n" << section_header("Deduplication Metrics") << "\n\n";
        std::vector<Row> dedupRows;

        std::ostringstream ratioStr;
        ratioStr << std::fixed << std::setprecision(2) << stats.deduplicationRatio << ":1";
        Severity ratioSev =
            stats.deduplicationRatio >= 1.5
                ? Severity::Good
                : (stats.deduplicationRatio >= 1.0 ? Severity::Warn : Severity::Bad);
        dedupRows.push_back({"Dedup ratio", paint(ratioSev, ratioStr.str()), ""});

        double savings = stats.spacesSavings;
        if (savings < 0.0)
            savings = 0.0;
        if (savings > 0.999999)
            savings = 0.999999;
        std::ostringstream savingsStr;
        savingsStr << std::fixed << std::setprecision(1) << (savings * 100.0) << "%";
        Severity savingsSev =
            savings >= 0.3 ? Severity::Good : (savings >= 0.1 ? Severity::Warn : Severity::Bad);
        dedupRows.push_back({"Space savings", paint(savingsSev, savingsStr.str()), ""});

        dedupRows.push_back({"Logical size", localFormatSize(stats.deduplicatedSize), ""});

        std::ostringstream avgRefStr;
        avgRefStr << std::fixed << std::setprecision(2) << stats.averageReferences;
        dedupRows.push_back({"Avg references", avgRefStr.str(), ""});
        dedupRows.push_back({"Max references", format_number(stats.maxReferences), ""});
        render_rows(std::cout, dedupRows);

        if (detailed_) {
            // Block Size Distribution
            if (!stats.blockSizeDistribution.empty()) {
                std::cout << "\n" << section_header("Block Size Distribution") << "\n\n";
                std::vector<Row> distRows;
                for (const auto& [range, count] : stats.blockSizeDistribution) {
                    distRows.push_back({range, format_number(count) + " blocks", ""});
                }
                render_rows(std::cout, distRows);
            }

            // Top Referenced Blocks
            if (!stats.topReferencedBlocks.empty()) {
                std::cout << "\n" << section_header("Top Referenced Blocks") << "\n\n";
                std::vector<Row> topRows;
                for (const auto& [hash, refs] : stats.topReferencedBlocks) {
                    std::string h = hash.size() > 16 ? hash.substr(0, 16) + "..." : hash;
                    topRows.push_back({h, format_number(refs) + " refs", ""});
                }
                render_rows(std::cout, topRows);
            }
        }

        // Storage Health
        std::cout << "\n" << section_header("Storage Health") << "\n\n";
        std::vector<Row> healthRows;
        double unrefPct = stats.uniqueBlocks > 0
                              ? (static_cast<double>(stats.unreferencedBlocks) / stats.uniqueBlocks)
                              : 0.0;
        std::ostringstream unrefPctStr;
        unrefPctStr << std::fixed << std::setprecision(1) << (unrefPct * 100.0) << "%";
        Severity unrefSev = unrefPct > 0.10 ? Severity::Warn : Severity::Good;
        healthRows.push_back({"Unreferenced", paint(unrefSev, unrefPctStr.str()), ""});

        if (unrefPct > 0.10) {
            healthRows.push_back(
                {"Status", paint(Severity::Warn, "Consider running garbage collection"), ""});
        } else {
            healthRows.push_back({"Status", paint(Severity::Good, "Healthy"), ""});
        }
        render_rows(std::cout, healthRows);
        std::cout << "\n";
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

    // Local size formatter that respects the humanReadable_ flag
    std::string localFormatSize(uint64_t bytes) const {
        if (humanReadable_) {
            return format_bytes(bytes);
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
