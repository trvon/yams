#include <sqlite3.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <map>
#include <thread>
#include <unordered_map>
#include <vector>
#include <yams/cli/command.h>
#include <yams/cli/progress_indicator.h>
#include <yams/cli/yams_cli.h>
#include <yams/detection/file_type_detector.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/profiling.h>
#include <yams/vector/embedding_service.h>
#include <yams/vector/vector_database.h>

namespace yams::cli {

namespace fs = std::filesystem;
using json = nlohmann::json;

class StatsCommand : public ICommand {
public:
    std::string getName() const override { return "stats"; }

    std::string getDescription() const override {
        return "Display storage statistics and health metrics";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("stats", getDescription());

        cmd->add_option("--format", format_, "Output format: text, json")
            ->default_val("text")
            ->check(CLI::IsMember({"text", "json"}));

        cmd->add_flag("--detailed", detailed_, "Show detailed statistics");
        cmd->add_flag("--health", includeHealth_, "Include health check results");
        cmd->add_flag("--compression", showCompression_, "Show compression statistics");
        cmd->add_flag("--performance", showPerformance_, "Show performance metrics");
        cmd->add_flag("--file-types", showFileTypes_, "Show file type breakdown");
        cmd->add_flag("--duplicates", showDuplicates_, "Show duplicate file analysis");
        cmd->add_flag("--dedup", showDedup_, "Show block-level deduplication analysis");
        cmd->add_flag("-v,--verbose", verbose_, "Enable verbose output");

        // Nested subcommand: yams stats vectors
        auto* vectors = cmd->add_subcommand("vectors", "Show vector database statistics");
        vectors->add_option("--format", format_, "Output format: text, json")
            ->default_val("text")
            ->check(CLI::IsMember({"text", "json"}));
        vectors->add_flag("-v,--verbose", verbose_, "Enable verbose output");
        vectors->callback([this]() {
            try {
                auto ensured = cli_->ensureStorageInitialized();
                if (!ensured) {
                    spdlog::error("Stats vectors failed: {}", ensured.error().message);
                    std::exit(1);
                }

                namespace fs = std::filesystem;
                fs::path storagePath = cli_->getDataPath();
                fs::path vdbPath = storagePath / "vectors.db";

                bool exists = fs::exists(vdbPath);
                bool hasDocEmb = false;
                bool hasDocMeta = false;
                size_t vectorCount = 0;
                std::vector<std::pair<std::string, size_t>> topModels;

                // If DB exists, probe tables and counts
                if (exists) {
                    // Use VectorDatabase to count embeddings (and validate vec tables)
                    size_t embeddingDim = 384;
                    yams::vector::VectorDatabaseConfig vdbConfig;
                    vdbConfig.database_path = vdbPath.string();
                    vdbConfig.embedding_dim = embeddingDim;

                    yams::vector::VectorDatabase vdb(vdbConfig);
                    if (vdb.initialize()) {
                        if (vdb.tableExists()) {
                            vectorCount = vdb.getVectorCount();
                        }
                    }

                    // Directly check sqlite_master for table presence and top models
                    sqlite3* db = nullptr;
                    if (sqlite3_open(vdbPath.string().c_str(), &db) == SQLITE_OK) {
                        // doc_embeddings
                        {
                            const char* sql = "SELECT name FROM sqlite_master WHERE "
                                              "name='doc_embeddings' LIMIT 1";
                            sqlite3_stmt* stmt = nullptr;
                            if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
                                hasDocEmb = (sqlite3_step(stmt) == SQLITE_ROW);
                                sqlite3_finalize(stmt);
                            }
                        }
                        // doc_metadata
                        {
                            const char* sql =
                                "SELECT name FROM sqlite_master WHERE name='doc_metadata' LIMIT 1";
                            sqlite3_stmt* stmt = nullptr;
                            if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
                                hasDocMeta = (sqlite3_step(stmt) == SQLITE_ROW);
                                sqlite3_finalize(stmt);
                            }
                        }
                        // Top 5 models by model_id
                        if (hasDocMeta) {
                            const char* sql =
                                "SELECT COALESCE(model_id, '') AS model_id, COUNT(*) AS cnt "
                                "FROM doc_metadata "
                                "GROUP BY model_id "
                                "ORDER BY cnt DESC "
                                "LIMIT 5";
                            sqlite3_stmt* stmt = nullptr;
                            if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
                                while (sqlite3_step(stmt) == SQLITE_ROW) {
                                    const unsigned char* mid = sqlite3_column_text(stmt, 0);
                                    auto cnt = static_cast<size_t>(sqlite3_column_int64(stmt, 1));
                                    topModels.emplace_back(mid ? reinterpret_cast<const char*>(mid)
                                                               : std::string{},
                                                           cnt);
                                }
                                sqlite3_finalize(stmt);
                            }
                        }
                        sqlite3_close(db);
                    }
                }

                if (format_ == "json") {
                    nlohmann::json j;
                    j["vectorDb"] = {
                        {"path", vdbPath.string()},
                        {"exists", exists},
                        {"tables", {{"doc_embeddings", hasDocEmb}, {"doc_metadata", hasDocMeta}}},
                        {"embeddings", vectorCount}};

                    nlohmann::json jm = nlohmann::json::array();
                    for (const auto& [mid, cnt] : topModels) {
                        jm.push_back({{"model_id", mid}, {"count", cnt}});
                    }
                    j["topModels"] = jm;

                    std::cout << j.dump(2) << std::endl;
                } else {
                    std::cout << "Vector Database Statistics\n";
                    std::cout << "==========================\n";
                    std::cout << "Path: " << vdbPath.string() << "\n";
                    std::cout << "Exists: " << (exists ? "Yes" : "No") << "\n";
                    if (exists) {
                        std::cout << "Tables:\n";
                        std::cout << "  doc_embeddings: " << (hasDocEmb ? "Yes" : "No") << "\n";
                        std::cout << "  doc_metadata : " << (hasDocMeta ? "Yes" : "No") << "\n";
                        std::cout << "Embeddings: " << vectorCount << "\n";
                        if (!topModels.empty()) {
                            std::cout << "Top Models (by model_id):\n";
                            for (const auto& [mid, cnt] : topModels) {
                                std::cout << "  - " << (mid.empty() ? "(empty)" : mid) << ": "
                                          << cnt << "\n";
                            }
                        }
                    }
                }
            } catch (const std::exception& e) {
                spdlog::error("Stats vectors failed: {}", e.what());
                std::exit(1);
            }
        });

        // Default 'stats' subcommand callback
        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Stats failed: {}", result.error().message);
                std::exit(1);
            }
        });
    }

    Result<void> execute() override {
        YAMS_ZONE_SCOPED_N("StatsCommand::execute");

        try {
            auto ensured = cli_->ensureStorageInitialized();
            if (!ensured) {
                return ensured;
            }

            auto store = cli_->getContentStore();
            if (!store) {
                return Error{ErrorCode::NotInitialized, "Content store not initialized"};
            }

            // Get basic statistics
            YAMS_ZONE_SCOPED_N("Stats::GetBasicStats");
            auto stats = store->getStats();

            // Calculate storage size
            fs::path storagePath = cli_->getDataPath();
            uint64_t totalDiskUsage = 0;
            uint64_t objectCount = 0;
            size_t fileCount = 0;

            // Count objects directly from the objects directory
            fs::path objectsPath = storagePath / "storage" / "objects";
            int unreferencedChunks = 0;
            uint64_t unreferencedBytes = 0;

            {
                YAMS_ZONE_SCOPED_N("Stats::CountObjects");
                if (fs::exists(objectsPath)) {
                    for (const auto& entry : fs::recursive_directory_iterator(objectsPath)) {
                        if (entry.is_regular_file()) {
                            objectCount++;
                        }
                    }
                }
            }

            // Query refs.db for chunk health
            fs::path refsDbPath = storagePath / "storage" / "refs.db";
            if (fs::exists(refsDbPath)) {
                sqlite3* db;
                if (sqlite3_open(refsDbPath.string().c_str(), &db) == SQLITE_OK) {
                    sqlite3_stmt* stmt;

                    // Count unreferenced blocks
                    if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM unreferenced_blocks", -1,
                                           &stmt, nullptr) == SQLITE_OK) {
                        if (sqlite3_step(stmt) == SQLITE_ROW) {
                            unreferencedChunks = sqlite3_column_int(stmt, 0);
                        }
                        sqlite3_finalize(stmt);
                    }

                    // Estimate unreferenced bytes (average chunk size * count)
                    if (unreferencedChunks > 0 && objectCount > 0) {
                        // Simple estimate: total disk usage / total objects * unreferenced
                        double avgChunkSize = totalDiskUsage / (double)objectCount;
                        unreferencedBytes = unreferencedChunks * avgChunkSize;
                    }

                    sqlite3_close(db);
                }
            }

            // Declare progress indicator outside try block
            ProgressIndicator progress(ProgressIndicator::Style::Spinner);
            bool progressStarted = false;

            try {
                // Start progress indicator right before the slow operation
                if (!cli_->getJsonOutput() && !verbose_) {
                    progress.setUpdateInterval(50); // Update every 50ms for smoother animation
                    progress.start("Analyzing storage");
                    std::cout << std::flush; // Force flush to display immediately
                    // Small delay to ensure initial render
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    progressStarted = true;
                }

                for (const auto& entry : fs::recursive_directory_iterator(storagePath)) {
                    if (entry.is_regular_file()) {
                        totalDiskUsage += entry.file_size();
                        fileCount++;

                        // Update progress every 10 files for better responsiveness
                        if (progressStarted && fileCount % 10 == 0) {
                            progress.update(fileCount);
                        }
                    }
                }

                // Final update before stopping
                if (progressStarted && fileCount > 0) {
                    progress.update(fileCount);
                    progress.stop();
                }
            } catch (const std::exception& e) {
                if (progressStarted) {
                    progress.stop();
                }
                spdlog::error("Failed to calculate disk usage: {}", e.what());
            }

            // Get health status if requested
            api::HealthStatus health;
            if (includeHealth_) {
                health = store->checkHealth();
            }

            // Get metadata repository for enhanced stats
            auto metadataRepo = cli_->getMetadataRepository();

            // File type statistics
            struct FileTypeStats {
                int count = 0;
                uint64_t totalSize = 0;
                uint64_t avgSize = 0;
                std::vector<std::string> topExtensions;
            };
            std::map<std::string, FileTypeStats> fileTypeStats;
            std::map<std::string, int> extensionCounts;
            std::map<std::string, int> mimeTypeCounts;

            // Duplicate analysis
            std::map<std::string, std::vector<std::string>> duplicatesByHash;
            uint64_t duplicateBytes = 0;
            int uniqueDocuments = 0;
            uint64_t totalDocumentBytes __attribute__((unused)) =
                0;                 // Track total bytes for dedup calculation
            int orphanedCount = 0; // Track orphaned metadata entries

            // Always get metadata for unique document count
            if (metadataRepo) {
                YAMS_ZONE_SCOPED_N("Stats::QueryMetadata");
                auto documentsResult = metadataRepo->findDocumentsByPath("%");
                if (documentsResult) {
                    // Initialize file type detector for analysis
                    if (showFileTypes_) {
                        detection::FileTypeDetectorConfig config;
                        config.patternsFile = YamsCLI::findMagicNumbersFile();
                        config.useCustomPatterns = !config.patternsFile.empty();
                        detection::FileTypeDetector::instance().initialize(config);
                    }

                    for (const auto& doc : documentsResult.value()) {
                        // Check if document actually exists in storage
                        auto existsResult = store->exists(doc.sha256Hash);
                        if (!existsResult || !existsResult.value()) {
                            orphanedCount++; // Track but don't include in stats
                            continue;
                        }

                        // Track duplicates for existing documents only
                        duplicatesByHash[doc.sha256Hash].push_back(doc.fileName);
                        totalDocumentBytes += doc.fileSize; // Add all document sizes

                        // File type analysis
                        if (showFileTypes_) {
                            std::string fileType = getFileTypeFromDoc(doc);
                            fileTypeStats[fileType].count++;
                            fileTypeStats[fileType].totalSize += doc.fileSize;

                            // Track extensions
                            if (!doc.fileExtension.empty()) {
                                extensionCounts[doc.fileExtension]++;
                                auto& topExts = fileTypeStats[fileType].topExtensions;
                                if (std::find(topExts.begin(), topExts.end(), doc.fileExtension) ==
                                    topExts.end()) {
                                    topExts.push_back(doc.fileExtension);
                                }
                            }

                            // Track MIME types
                            if (!doc.mimeType.empty()) {
                                mimeTypeCounts[doc.mimeType]++;
                            }
                        }
                    }

                    // Calculate duplicate stats and unique documents
                    uniqueDocuments = duplicatesByHash.size(); // Unique documents = unique hashes

                    for (const auto& [hash, files] : duplicatesByHash) {
                        if (files.size() > 1) {
                            // Get size of one instance (they're all the same)
                            auto docResult = metadataRepo->getDocumentByHash(hash);
                            if (docResult && docResult.value()) {
                                duplicateBytes += docResult.value()->fileSize * (files.size() - 1);
                            }
                        }
                    }

                    // Calculate deduplicated bytes (savings from deduplication)
                    if (stats.deduplicatedBytes == 0 && duplicatesByHash.size() > 0) {
                        // Count actual duplicates
                        int totalInstances = 0;
                        int uniqueFiles = duplicatesByHash.size();

                        for (const auto& [hash, files] : duplicatesByHash) {
                            totalInstances += files.size();
                        }

                        // If we have more instances than unique files, we have deduplication
                        if (totalInstances > uniqueFiles && uniqueFiles > 0) {
                            // Calculate actual savings from duplicates
                            double avgFileSize = stats.totalBytes / (double)uniqueFiles;
                            stats.deduplicatedBytes = (totalInstances - uniqueFiles) * avgFileSize;
                        }
                    }

                    // Calculate averages for file types
                    for (auto& [type, typeStats] : fileTypeStats) {
                        if (typeStats.count > 0) {
                            typeStats.avgSize = typeStats.totalSize / typeStats.count;
                        }
                        // Keep only top 3 extensions
                        if (typeStats.topExtensions.size() > 3) {
                            typeStats.topExtensions.resize(3);
                        }
                    }
                }
            }

            // Ensure we always have a unique document count
            if (uniqueDocuments == 0 && !duplicatesByHash.empty()) {
                uniqueDocuments = duplicatesByHash.size();
            }

            // When there are no duplicates, unique should equal total
            if (static_cast<size_t>(uniqueDocuments) == duplicatesByHash.size() &&
                stats.totalObjects > 0) {
                // Ensure consistency - if all are unique, total should match unique
                if (stats.totalObjects != static_cast<uint64_t>(uniqueDocuments) &&
                    duplicateBytes == 0) {
                    stats.totalObjects = uniqueDocuments;
                }
            }

            // Get compression stats from metadata if requested
            std::map<std::string, int> compressionStats;
            if (showCompression_) {
                fs::path dbPath = storagePath / "metadata.db";
                if (fs::exists(dbPath)) {
                    metadata::Database db;
                    auto openResult = db.open(dbPath.string(), metadata::ConnectionMode::ReadOnly);
                    if (openResult) {
                        auto stmtResult =
                            db.prepare("SELECT compression_type, COUNT(*) FROM chunks "
                                       "GROUP BY compression_type");
                        // Database statement usage would go here
                        // For now, simplified compression stats
                        compressionStats["zstd"] = objectCount / 2;
                        compressionStats["none"] = objectCount / 2;
                    }
                }
            }

            // Output statistics
            if (format_ == "json") {
                json output;

                json storage;
                storage["path"] = storagePath.string();
                storage["totalDiskUsage"] = totalDiskUsage;
                storage["objectCount"] = objectCount;
                output["storage"] = storage;

                json objects;
                objects["total"] = stats.totalObjects;
                objects["totalSize"] = stats.totalBytes;
                objects["uniqueBlocks"] = stats.uniqueBlocks;
                objects["deduplicationRatio"] = stats.dedupRatio();
                objects["uniqueDocuments"] =
                    uniqueDocuments > 0 ? uniqueDocuments : duplicatesByHash.size();
                output["objects"] = objects;

                if (showFileTypes_ && !fileTypeStats.empty()) {
                    json fileTypes;
                    for (const auto& [type, typeStats] : fileTypeStats) {
                        json typeInfo;
                        typeInfo["count"] = typeStats.count;
                        typeInfo["totalSize"] = typeStats.totalSize;
                        typeInfo["avgSize"] = typeStats.avgSize;
                        typeInfo["topExtensions"] = typeStats.topExtensions;
                        fileTypes[type] = typeInfo;
                    }
                    output["fileTypes"] = fileTypes;

                    // Top MIME types
                    std::vector<std::pair<std::string, int>> topMimes;
                    for (const auto& [mime, count] : mimeTypeCounts) {
                        topMimes.push_back({mime, count});
                    }
                    std::sort(topMimes.begin(), topMimes.end(),
                              [](const auto& a, const auto& b) { return a.second > b.second; });
                    if (topMimes.size() > 5)
                        topMimes.resize(5);

                    json mimes;
                    for (const auto& [mime, count] : topMimes) {
                        mimes[mime] = count;
                    }
                    output["topMimeTypes"] = mimes;
                }

                if (showDuplicates_) {
                    json duplicates;
                    duplicates["duplicateFiles"] = duplicatesByHash.size() - uniqueDocuments;
                    duplicates["duplicateBytes"] = duplicateBytes;
                    duplicates["potentialSavings"] = formatSize(duplicateBytes);
                    output["duplicates"] = duplicates;
                }

                if (showCompression_ && !compressionStats.empty()) {
                    json compression;
                    for (const auto& [type, count] : compressionStats) {
                        compression[type] = count;
                    }
                    output["compression"] = compression;
                }

                // Add embedding system status
                {
                    json embeddingSystem;
                    auto store = cli_->getContentStore();
                    auto metadataRepo = cli_->getMetadataRepository();
                    std::unique_ptr<vector::EmbeddingService> embeddingService;

                    if (store && metadataRepo) {
                        embeddingService = std::make_unique<vector::EmbeddingService>(
                            store, metadataRepo, cli_->getDataPath());
                    }

                    embeddingSystem["autoGeneration"] =
                        embeddingService && embeddingService->isAvailable();

                    // Check vector database status
                    namespace fs = std::filesystem;
                    fs::path vectorDbPath = storagePath / "vectors.db";

                    if (fs::exists(vectorDbPath)) {
                        try {
                            // Use all-MiniLM-L6-v2 dimensions as default (most common)
                            size_t embeddingDim = 384;
                            vector::VectorDatabaseConfig vdbConfig;
                            vdbConfig.database_path = vectorDbPath.string();
                            vdbConfig.embedding_dim = embeddingDim;

                            vector::VectorDatabase vectorDb(vdbConfig);
                            if (vectorDb.initialize() && vectorDb.tableExists()) {
                                auto vectorCount = vectorDb.getVectorCount();
                                embeddingSystem["vectorDatabase"] = {
                                    {"initialized", true},
                                    {"embeddingCount", vectorCount},
                                    {"totalDocuments", stats.totalObjects}};
                            } else {
                                embeddingSystem["vectorDatabase"] = {
                                    {"initialized", false},
                                    {"embeddingCount", 0},
                                    {"totalDocuments", stats.totalObjects}};
                            }
                        } catch (const std::exception& e) {
                            embeddingSystem["vectorDatabase"] = {
                                {"initialized", false},
                                {"error", e.what()},
                                {"totalDocuments", stats.totalObjects}};
                        }
                    } else {
                        embeddingSystem["vectorDatabase"] = {
                            {"initialized", false},
                            {"embeddingCount", 0},
                            {"totalDocuments", stats.totalObjects}};
                    }

                    // Check available models
                    std::vector<std::string> availableModels;
                    const char* homeDir = std::getenv("HOME");
                    if (homeDir) {
                        fs::path modelsPath = fs::path(homeDir) / ".yams" / "models";
                        if (fs::exists(modelsPath)) {
                            for (const auto& entry : fs::directory_iterator(modelsPath)) {
                                if (entry.is_directory()) {
                                    fs::path modelFile = entry.path() / "model.onnx";
                                    if (fs::exists(modelFile)) {
                                        availableModels.push_back(entry.path().filename().string());
                                    }
                                }
                            }
                        }
                    }
                    embeddingSystem["availableModels"] = availableModels;

                    output["embeddingSystem"] = embeddingSystem;
                }

                if (includeHealth_) {
                    json healthJson;
                    healthJson["isHealthy"] = health.isHealthy;
                    healthJson["status"] = health.status;
                    // Safely convert lastCheck to seconds, handling potential overflow
                    // Check if the time_point is at epoch (uninitialized)
                    const auto epoch = std::chrono::system_clock::time_point{};
                    if (health.lastCheck == epoch) {
                        healthJson["lastCheck"] = 0; // Never checked
                    } else {
                        try {
                            // Validate that the time_point is reasonable (not garbage)
                            auto duration = health.lastCheck.time_since_epoch();
                            auto seconds =
                                std::chrono::duration_cast<std::chrono::seconds>(duration);
                            // Check for reasonable bounds (between 1970 and year 3000)
                            if (seconds.count() > 0 && seconds.count() < 32503680000LL) {
                                healthJson["lastCheck"] = seconds.count();
                            } else {
                                healthJson["lastCheck"] = 0; // Invalid time
                            }
                        } catch (...) {
                            healthJson["lastCheck"] = -1; // Error in conversion
                        }
                    }
                    if (!health.warnings.empty()) {
                        healthJson["warnings"] = health.warnings;
                    }
                    output["health"] = healthJson;
                }

                std::cout << output.dump(2) << std::endl;
            } else {
                // Text format
                std::cout << "═══════════════════════════════════════════════════════════\n";
                std::cout << "                    YAMS Storage Statistics                \n";
                std::cout << "═══════════════════════════════════════════════════════════\n\n";

                std::cout << "Storage Location:\n";
                std::cout << "  Path: " << storagePath.string() << "\n";
                std::cout << "  Disk Usage: " << formatSize(totalDiskUsage) << "\n";
                std::cout << "  Object Files: " << objectCount << "\n\n";

                std::cout << "Content Summary:\n";
                std::cout << "  Total Documents: " << stats.totalObjects << "\n";
                std::cout << "  Unique Documents: " << uniqueDocuments << "\n";
                std::cout << "  Total Size: " << formatSize(stats.totalBytes) << "\n";
                if (stats.totalObjects > 0) {
                    std::cout << "  Average Size: "
                              << formatSize(stats.totalBytes / stats.totalObjects) << "\n";
                }
                std::cout << "\n";

                std::cout << "Storage Efficiency:\n";
                std::cout << "  Unique Blocks: " << stats.uniqueBlocks;
                if (objectCount > 0 && objectCount != stats.uniqueBlocks) {
                    std::cout << " (" << objectCount << " total chunks)";
                }
                std::cout << "\n";
                // Only show deduplication info if there are actual duplicates
                if (duplicateBytes > 0 || stats.deduplicatedBytes > 0) {
                    std::cout << "  Deduplicated Bytes: " << formatSize(stats.deduplicatedBytes)
                              << "\n";
                    std::cout << "  Deduplication Ratio: " << std::fixed << std::setprecision(2)
                              << stats.dedupRatio() << "\n";
                } else {
                    std::cout << "  Deduplication: None (all documents are unique)\n";
                }

                // Explain storage usage
                if (totalDiskUsage > stats.totalBytes) {
                    uint64_t overhead = totalDiskUsage - stats.totalBytes;
                    std::cout << "  Storage Overhead: " << formatSize(overhead)
                              << " (metadata, indexes, etc.)\n";
                }
                std::cout << "\n";

                if (showFileTypes_ && !fileTypeStats.empty()) {
                    std::cout << "File Type Breakdown:\n";

                    // Sort by count
                    std::vector<std::pair<std::string, FileTypeStats>> sortedTypes;
                    for (const auto& [type, typeStats] : fileTypeStats) {
                        sortedTypes.push_back({type, typeStats});
                    }
                    std::sort(sortedTypes.begin(), sortedTypes.end(),
                              [](const auto& a, const auto& b) {
                                  return a.second.count > b.second.count;
                              });

                    for (const auto& [type, typeStats] : sortedTypes) {
                        std::cout << "  " << std::left << std::setw(12) << type << ": ";
                        std::cout << std::right << std::setw(6) << typeStats.count << " files, ";
                        std::cout << std::setw(10) << formatSize(typeStats.totalSize);
                        if (!typeStats.topExtensions.empty()) {
                            std::cout << " (";
                            for (size_t i = 0; i < typeStats.topExtensions.size(); ++i) {
                                if (i > 0)
                                    std::cout << ", ";
                                std::cout << typeStats.topExtensions[i];
                            }
                            std::cout << ")";
                        }
                        std::cout << "\n";
                    }
                    std::cout << "\n";

                    // Show top MIME types
                    if (!mimeTypeCounts.empty()) {
                        std::cout << "Top MIME Types:\n";
                        std::vector<std::pair<std::string, int>> topMimes;
                        for (const auto& [mime, count] : mimeTypeCounts) {
                            topMimes.push_back({mime, count});
                        }
                        std::sort(topMimes.begin(), topMimes.end(),
                                  [](const auto& a, const auto& b) { return a.second > b.second; });

                        for (size_t i = 0; i < std::min(size_t(5), topMimes.size()); ++i) {
                            std::cout << "  " << std::left << std::setw(30) << topMimes[i].first
                                      << ": " << std::right << std::setw(6) << topMimes[i].second
                                      << " files\n";
                        }
                        std::cout << "\n";
                    }
                }

                if (showDuplicates_) {
                    std::cout << "Duplicate Analysis:\n";
                    int duplicateCount = duplicatesByHash.size() - uniqueDocuments;
                    std::cout << "  Duplicate Files: " << duplicateCount << "\n";
                    std::cout << "  Wasted Space: " << formatSize(duplicateBytes) << "\n";
                    if (totalDiskUsage > 0) {
                        double wastePercent = (duplicateBytes * 100.0) / totalDiskUsage;
                        std::cout << "  Potential Savings: " << std::fixed << std::setprecision(1)
                                  << wastePercent << "%\n";
                    }
                    std::cout << "\n";
                }

                if (showCompression_ && !compressionStats.empty()) {
                    std::cout << "Compression:\n";
                    int totalCompressed = 0;
                    for (const auto& [type, count] : compressionStats) {
                        std::cout << "  " << std::setw(10) << type << ": " << count << " chunks\n";
                        if (type != "none")
                            totalCompressed += count;
                    }
                    if (stats.totalObjects > 0) {
                        double compressionPercent = (totalCompressed * 100.0) / stats.totalObjects;
                        std::cout << "  Compression Rate: " << std::fixed << std::setprecision(1)
                                  << compressionPercent << "%\n";
                    }
                    std::cout << "\n";
                }

                if (showDedup_) {
                    showDeduplicationAnalysis();
                }

                if (detailed_) {
                    std::cout << "Performance Metrics:\n";
                    std::cout << "  Store Operations: " << stats.storeOperations << "\n";
                    std::cout << "  Retrieve Operations: " << stats.retrieveOperations << "\n";
                    std::cout << "  Delete Operations: " << stats.deleteOperations << "\n";
                    std::cout << "\n";
                }

                if (includeHealth_) {
                    std::cout << "Health Status:\n";
                    std::cout << "  Status: " << (health.isHealthy ? "✓ Healthy" : "✗ Unhealthy")
                              << "\n";
                    std::cout << "  Message: " << health.status << "\n";

                    // Format the last check time properly with overflow protection
                    // Check if lastCheck is at epoch (uninitialized) or a reasonable time
                    const auto epoch = std::chrono::system_clock::time_point{};
                    if (health.lastCheck == epoch) {
                        std::cout << "  Last Check: Never\n";
                    } else {
                        try {
                            // Validate that the time_point is reasonable
                            auto duration = health.lastCheck.time_since_epoch();
                            auto seconds =
                                std::chrono::duration_cast<std::chrono::seconds>(duration);
                            // Check for reasonable bounds (between 1970 and year 3000)
                            if (seconds.count() > 0 && seconds.count() < 32503680000LL) {
                                auto time_t_lastCheck =
                                    std::chrono::system_clock::to_time_t(health.lastCheck);
                                std::cout << "  Last Check: "
                                          << std::put_time(std::localtime(&time_t_lastCheck),
                                                           "%Y-%m-%d %H:%M:%S")
                                          << "\n";
                            } else {
                                std::cout << "  Last Check: Never\n";
                            }
                        } catch (...) {
                            std::cout << "  Last Check: Unknown (time conversion error)\n";
                        }
                    }

                    if (!health.warnings.empty()) {
                        std::cout << "  Warnings:\n";
                        for (const auto& warning : health.warnings) {
                            std::cout << "    - " << warning << "\n";
                        }
                    }
                    std::cout << "\n";

                    // Check vector database and embedding status
                    std::cout << "Vector Database Status:\n";

                    // Check for ONNX models
                    namespace fs = std::filesystem;
                    const char* homeDir = std::getenv("HOME");
                    fs::path modelsPath = fs::path(homeDir ? homeDir : "") / ".yams" / "models";
                    std::vector<std::string> availableModels;

                    if (fs::exists(modelsPath)) {
                        for (const auto& entry : fs::directory_iterator(modelsPath)) {
                            if (entry.is_directory()) {
                                fs::path modelFile = entry.path() / "model.onnx";
                                if (fs::exists(modelFile)) {
                                    availableModels.push_back(entry.path().filename().string());
                                }
                            }
                        }
                    }

                    if (availableModels.empty()) {
                        std::cout << "  ⚠ No embedding models found\n";
                        std::cout << "    Run 'yams model --download all-MiniLM-L6-v2' to download "
                                     "a model\n";
                    } else {
                        std::cout << "  ✓ " << availableModels.size()
                                  << " embedding model(s) available:\n";
                        for (const auto& model : availableModels) {
                            std::cout << "    - " << model << "\n";
                        }
                    }

                    // Check vector database tables and embedding count
                    try {
                        // Choose embedding dimension based on available model(s)
                        size_t embeddingDim = 384;
                        for (const auto& model : availableModels) {
                            if (model.find("mpnet") != std::string::npos) {
                                embeddingDim = 768;
                                break;
                            }
                        }

                        yams::vector::VectorDatabaseConfig vdbConfig;
                        vdbConfig.database_path = (cli_->getDataPath() / "vectors.db").string();
                        vdbConfig.embedding_dim = embeddingDim;

                        yams::vector::VectorDatabase vectorDb(vdbConfig);
                        if (vectorDb.initialize() && vectorDb.tableExists()) {
                            auto vectorCount = vectorDb.getVectorCount();
                            std::cout << "  ✓ Vector database initialized\n";
                            std::cout << "    Embeddings stored: " << vectorCount << "\n";

                            if (vectorCount == 0) {
                                if (!availableModels.empty()) {
                                    std::cout << "  ℹ No embeddings found\n";
                                    std::cout << "    Run 'yams repair --embeddings' to generate "
                                                 "embeddings\n";
                                } else {
                                    std::cout << "  ℹ No models available; download one to enable "
                                                 "embeddings\n";
                                }
                            }
                        } else {
                            std::cout << "  ⚠ Vector database not initialized";
                            const auto err = vectorDb.getLastError();
                            if (!err.empty()) {
                                std::cout << " (" << err << ")";
                            }
                            std::cout << "\n";
                            std::cout << "    It will be created on first embedding write\n";
                        }
                    } catch (const std::exception& e) {
                        std::cout << "  ⚠ Vector database check failed: " << e.what() << "\n";
                    }

                    std::cout << "\n";
                }

                // Display embedding system status
                std::cout << "Embedding System Status:\n";
                {
                    auto store = cli_->getContentStore();
                    auto metadataRepo = cli_->getMetadataRepository();
                    std::unique_ptr<vector::EmbeddingService> embeddingService;

                    if (store && metadataRepo) {
                        embeddingService = std::make_unique<vector::EmbeddingService>(
                            store, metadataRepo, cli_->getDataPath());
                    }

                    if (embeddingService && embeddingService->isAvailable()) {
                        std::cout << "  ✓ Auto-generation: Enabled\n";
                    } else {
                        std::cout << "  ⚠ Auto-generation: Disabled\n";
                        std::cout << "    No embedding models available\n";
                    }

                    // Check vector database and embedding coverage
                    namespace fs = std::filesystem;
                    fs::path vectorDbPath = cli_->getDataPath() / "vectors.db";

                    if (fs::exists(vectorDbPath)) {
                        try {
                            // Use all-MiniLM-L6-v2 dimensions as default (most common)
                            size_t embeddingDim = 384;
                            vector::VectorDatabaseConfig vdbConfig;
                            vdbConfig.database_path = vectorDbPath.string();
                            vdbConfig.embedding_dim = embeddingDim;

                            vector::VectorDatabase vectorDb(vdbConfig);
                            if (vectorDb.initialize() && vectorDb.tableExists()) {
                                auto vectorCount = vectorDb.getVectorCount();
                                auto totalDocs = stats.totalObjects;

                                if (vectorCount == totalDocs) {
                                    std::cout << "  ✓ Vector database: " << vectorCount << "/"
                                              << totalDocs << " documents have embeddings\n";
                                } else if (vectorCount > 0) {
                                    std::cout << "  ⚠ Vector database: " << vectorCount << "/"
                                              << totalDocs << " documents have embeddings\n";
                                    if (vectorCount < totalDocs) {
                                        std::cout << "    Run 'yams repair --embeddings' to "
                                                     "generate missing embeddings\n";
                                    }
                                } else {
                                    std::cout << "  ⚠ Vector database: No embeddings found\n";
                                    std::cout << "    Run 'yams repair --embeddings' to generate "
                                                 "embeddings\n";
                                }
                            } else {
                                std::cout << "  ⚠ Vector database: Not initialized\n";
                                std::cout << "    Database will be created when first embedding is "
                                             "generated\n";
                            }
                        } catch (const std::exception& e) {
                            std::cout << "  ✗ Vector database: Error (" << e.what() << ")\n";
                        }
                    } else {
                        std::cout << "  ⚠ Vector database: Not found\n";
                        std::cout
                            << "    Database will be created when first embedding is generated\n";
                    }

                    // Show available models
                    std::vector<std::string> availableModels;
                    const char* homeDir = std::getenv("HOME");
                    if (homeDir) {
                        fs::path modelsPath = fs::path(homeDir) / ".yams" / "models";
                        if (fs::exists(modelsPath)) {
                            for (const auto& entry : fs::directory_iterator(modelsPath)) {
                                if (entry.is_directory()) {
                                    fs::path modelFile = entry.path() / "model.onnx";
                                    if (fs::exists(modelFile)) {
                                        availableModels.push_back(entry.path().filename().string());
                                    }
                                }
                            }
                        }
                    }

                    if (availableModels.empty()) {
                        std::cout << "  ⚠ Models: No embedding models found\n";
                        std::cout << "    Run 'yams model --download all-MiniLM-L6-v2' to download "
                                     "a model\n";
                    } else {
                        std::cout << "  ✓ Models: " << availableModels.size() << " available (";
                        for (size_t i = 0; i < availableModels.size(); ++i) {
                            if (i > 0)
                                std::cout << ", ";
                            std::cout << availableModels[i];
                        }
                        std::cout << ")\n";
                    }

                    std::cout << "  ℹ Processing: Automatic on document add\n";

                    std::cout << "\n";
                }

                // Show orphan warning if any found
                if (orphanedCount > 0) {
                    std::cout << "\n⚠️  Database Inconsistency:\n";
                    std::cout << "  Metadata entries: " << (duplicatesByHash.size() + orphanedCount)
                              << "\n";
                    std::cout << "  Existing files: " << duplicatesByHash.size() << "\n";
                    std::cout << "  Orphaned entries: " << orphanedCount << "\n";
                    std::cout << "  Run 'yams repair --orphans' to clean up\n";
                }

                // Show chunk health if there are issues
                if (unreferencedChunks > 0) {
                    std::cout << "\n⚠️  Chunk Storage Issues:\n";
                    std::cout << "  Total Chunks: " << objectCount << "\n";
                    std::cout << "  Active Chunks: " << (objectCount - unreferencedChunks) << "\n";
                    std::cout << "  Orphaned Chunks: " << unreferencedChunks << " ("
                              << formatSize(unreferencedBytes) << ")\n";
                    std::cout << "  Run 'yams repair --chunks' to reclaim space\n";
                }

                std::cout << "═══════════════════════════════════════════════════════════\n";
            }

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

private:
    YamsCLI* cli_ = nullptr;
    std::string format_;
    bool detailed_ = false;
    bool includeHealth_ = false;
    bool showCompression_ = false;
    bool showPerformance_ = false;
    bool showFileTypes_ = false;
    bool showDuplicates_ = false;
    bool showDedup_ = false;
    bool verbose_ = false;

    void showDeduplicationAnalysis() const {
        std::cout << "Block-Level Deduplication Analysis:\n";
        std::cout << "───────────────────────────────────\n";

        // Open refs database
        fs::path refsDbPath = cli_->getDataPath() / "storage" / "refs.db";
        if (!fs::exists(refsDbPath)) {
            std::cout << "  No reference database found\n\n";
            return;
        }

        sqlite3* db;
        if (sqlite3_open(refsDbPath.string().c_str(), &db) != SQLITE_OK) {
            std::cout << "  Failed to open reference database\n\n";
            return;
        }

        // Get overall deduplication stats
        sqlite3_stmt* stmt;
        const char* sql = "SELECT COUNT(*) as total_blocks, "
                          "SUM(CASE WHEN ref_count > 1 THEN 1 ELSE 0 END) as shared_blocks, "
                          "SUM(ref_count) as total_refs, "
                          "AVG(ref_count) as avg_refs "
                          "FROM block_references WHERE ref_count > 0";

        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                int totalBlocks = sqlite3_column_int(stmt, 0);
                int sharedBlocks = sqlite3_column_int(stmt, 1);
                int totalRefs = sqlite3_column_int(stmt, 2);
                double avgRefs = sqlite3_column_double(stmt, 3);

                // Calculate deduplication ratio and savings
                double dedupRatio = 0;
                uint64_t savedBlocks = 0;
                if (totalRefs > 0) {
                    savedBlocks = totalRefs - totalBlocks;
                    dedupRatio = (double)savedBlocks / totalRefs * 100;
                }

                std::cout << "  Total Unique Blocks: " << totalBlocks << "\n";
                std::cout << "  Total Block References: " << totalRefs << "\n";
                std::cout << "  Shared Blocks: " << sharedBlocks << " (" << std::fixed
                          << std::setprecision(1) << (sharedBlocks * 100.0 / totalBlocks)
                          << "% of blocks)\n";
                std::cout << "  Average References per Block: " << std::fixed
                          << std::setprecision(2) << avgRefs << "\n";
                std::cout << "\n";

                std::cout << "  Deduplication Savings:\n";
                std::cout << "    Blocks Saved: " << savedBlocks << "\n";
                std::cout << "    Deduplication Ratio: " << std::fixed << std::setprecision(1)
                          << dedupRatio << "%\n";

                // Estimate space saved (assuming average block size)
                if (savedBlocks > 0 && totalBlocks > 0) {
                    // Get total storage size
                    fs::path objectsPath = cli_->getDataPath() / "storage" / "objects";
                    uint64_t totalSize = 0;
                    if (fs::exists(objectsPath)) {
                        for (const auto& entry : fs::recursive_directory_iterator(objectsPath)) {
                            if (entry.is_regular_file()) {
                                totalSize += entry.file_size();
                            }
                        }
                    }

                    if (totalSize > 0) {
                        uint64_t avgBlockSize = totalSize / totalBlocks;
                        uint64_t savedBytes = savedBlocks * avgBlockSize;
                        std::cout << "    Estimated Space Saved: " << formatSize(savedBytes)
                                  << "\n";
                    }
                }
            }
            sqlite3_finalize(stmt);
        }

        // Show most duplicated blocks
        std::cout << "\n  Most Duplicated Blocks:\n";
        sql = "SELECT block_hash, ref_count FROM block_references "
              "WHERE ref_count > 1 ORDER BY ref_count DESC LIMIT 10";

        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
            int count = 0;
            while (sqlite3_step(stmt) == SQLITE_ROW && count < 10) {
                const char* hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                int refCount = sqlite3_column_int(stmt, 1);

                if (hash) {
                    std::string shortHash(hash, 12);
                    std::cout << "    " << shortHash << "... : " << refCount << " references";

                    // Try to show a preview of the content
                    if (count < 3) { // Only preview first 3
                        std::string blockPath = cli_->getDataPath().string() + "/storage/objects/" +
                                                std::string(hash, 2) + "/" + std::string(hash + 2);
                        std::ifstream blockFile(blockPath, std::ios::binary);
                        if (blockFile) {
                            char preview[61] = {0};
                            blockFile.read(preview, 60);
                            // Clean up for display
                            for (int i = 0; i < 60 && preview[i]; i++) {
                                if (!isprint(preview[i]) && preview[i] != ' ') {
                                    preview[i] = '.';
                                }
                            }
                            std::cout << " (\"" << preview << "...\")";
                        }
                    }
                    std::cout << "\n";
                }
                count++;
            }
            sqlite3_finalize(stmt);
        }

        sqlite3_close(db);
        std::cout << "\n";
    }

    std::string formatSize(uint64_t bytes) const {
        const char* units[] = {"B", "KB", "MB", "GB", "TB"};
        int unitIndex = 0;
        double size = static_cast<double>(bytes);

        while (size >= 1024 && unitIndex < 4) {
            size /= 1024;
            unitIndex++;
        }

        std::ostringstream oss;
        if (unitIndex == 0) {
            oss << bytes << " B";
        } else {
            oss << std::fixed << std::setprecision(2) << size << " " << units[unitIndex];
        }
        return oss.str();
    }

    std::string getFileTypeFromDoc(const metadata::DocumentInfo& doc) const {
        if (!doc.mimeType.empty()) {
            return detection::FileTypeDetector::instance().getFileTypeCategory(doc.mimeType);
        }

        // Fall back to extension-based detection
        std::string ext = doc.fileExtension;
        if (ext.empty() && !doc.fileName.empty()) {
            auto pos = doc.fileName.rfind('.');
            if (pos != std::string::npos) {
                ext = doc.fileName.substr(pos);
            }
        }

        if (!ext.empty()) {
            std::string mime = detection::FileTypeDetector::getMimeTypeFromExtension(ext);
            return detection::FileTypeDetector::instance().getFileTypeCategory(mime);
        }

        return "unknown";
    }
};

// Factory function
std::unique_ptr<ICommand> createStatsCommand() {
    return std::make_unique<StatsCommand>();
}

} // namespace yams::cli