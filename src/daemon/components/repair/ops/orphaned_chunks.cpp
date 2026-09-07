// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Repair operation "chunks" (wire code 7); see repair_operation.h.

#include <yams/daemon/components/repair/repair_operation.h>

#include "../repair_operation_support.h"
#include "../repair_operations_internal.h"

#include <spdlog/spdlog.h>
#include <yams/core/repair_fsm.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/components/db_salvage.h>
#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/MetadataWriteFacade.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningManager.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/VectorIndexCoordinator.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/daemon/metric_keys.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/detection/file_type_detector.h>
#include <yams/extraction/content_extractor.h>
#include <yams/extraction/extraction_util.h>
#include <yams/integrity/repair_manager.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>
#include <yams/profiling.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>

#include <sqlite3.h>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/thread_pool.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <set>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace yams::daemon::repair {

namespace {

RepairOperationResult cleanOrphanedChunks(OperationEnv& env, bool dryRun, bool verbose,
                                          const RepairService::ProgressFn& progress) {
    RepairOperationResult result;
    result.operation = "chunks";

    namespace fs = std::filesystem;
    fs::path refsDbPath = env.cfg.dataDir / "storage" / "refs.db";
    fs::path objectsPath = env.cfg.dataDir / "storage" / "objects";

    if (!fs::exists(refsDbPath) || !fs::exists(objectsPath)) {
        result.message = "No refs.db or objects directory found";
        return result;
    }

    sqlite3* db = nullptr;
    if (sqlite3_open(refsDbPath.string().c_str(), &db) != SQLITE_OK) {
        result.message = "Failed to open refs.db";
        return result;
    }

    // Quick-check: count unreferenced block_references entries.
    // If zero, the common case (no orphans expected) can skip the expensive FS walk.
    {
        sqlite3_stmt* countStmt = nullptr;
        const char* countSql = "SELECT COUNT(*) FROM block_references WHERE ref_count = 0";
        int zeroRefCount = 0;
        if (sqlite3_prepare_v2(db, countSql, -1, &countStmt, nullptr) == SQLITE_OK) {
            if (sqlite3_step(countStmt) == SQLITE_ROW)
                zeroRefCount = sqlite3_column_int(countStmt, 0);
            sqlite3_finalize(countStmt);
        }

        if (zeroRefCount == 0) {
            // Also do a quick FS sanity check: count prefix dirs. If the count matches
            // the number of referenced hashes' unique prefixes, no orphans are possible.
            // For now, if refs.db tracks no zero-ref entries, trust it and skip the walk.
            sqlite3_close(db);
            result.message = "No orphaned chunks (quick-check: 0 unreferenced blocks)";
            return result;
        }

        spdlog::info("[RepairService] chunks: {} unreferenced block_references, proceeding "
                     "with filesystem scan",
                     zeroRefCount);
    }

    // Get referenced hashes
    std::set<std::string> referencedHashes;
    sqlite3_stmt* stmt = nullptr;
    const char* sql = "SELECT block_hash FROM block_references WHERE ref_count > 0";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const char* hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            if (hash)
                referencedHashes.insert(hash);
        }
        sqlite3_finalize(stmt);
    }

    if (progress) {
        RepairEvent ev;
        ev.phase = "repairing";
        ev.operation = "chunks";
        ev.message = "Loaded " + std::to_string(referencedHashes.size()) +
                     " referenced hashes, scanning filesystem...";
        progress(ev);
    }

    // Scan manifests for additional references
    size_t manifestDirsScanned = 0;
    if (fs::exists(objectsPath)) {
        for (const auto& dirEntry : fs::directory_iterator(objectsPath)) {
            if (!fs::is_directory(dirEntry))
                continue;
            ++manifestDirsScanned;
            if (progress && manifestDirsScanned % 64 == 0) {
                RepairEvent ev;
                ev.phase = "repairing";
                ev.operation = "chunks";
                ev.message =
                    "Scanning manifests (" + std::to_string(manifestDirsScanned) + " dirs)...";
                progress(ev);
            }
            for (const auto& fileEntry : fs::directory_iterator(dirEntry.path())) {
                if (!fs::is_regular_file(fileEntry))
                    continue;
                std::string filename = fileEntry.path().filename().string();
                if (filename.size() > 9 && filename.substr(filename.size() - 9) == ".manifest") {
                    try {
                        std::ifstream mf(fileEntry.path(), std::ios::binary);
                        if (!mf)
                            continue;
                        std::vector<char> data((std::istreambuf_iterator<char>(mf)),
                                               std::istreambuf_iterator<char>());
                        if (data.size() >= 28 && data[0] == 'Y' && data[1] == 'M' &&
                            data[2] == 'N' && data[3] == 'F') {
                            size_t pos = 24;
                            uint32_t numChunks = 0;
                            if (pos + 4 <= data.size()) {
                                std::memcpy(&numChunks, data.data() + pos, 4);
                                pos += 4;
                            }
                            for (uint32_t i = 0; i < numChunks && pos + 4 <= data.size(); ++i) {
                                uint32_t hashLen = 0;
                                std::memcpy(&hashLen, data.data() + pos, 4);
                                pos += 4;
                                if (hashLen > 0 && hashLen < 256 && pos + hashLen <= data.size()) {
                                    referencedHashes.insert(
                                        std::string(data.data() + pos, hashLen));
                                    pos += hashLen + 16;
                                } else {
                                    break;
                                }
                            }
                        }
                    } catch (const std::exception& e) {
                        spdlog::debug("RepairService: failed to parse block references for {}: {}",
                                      fileEntry.path().string(), e.what());
                    } catch (...) {
                        spdlog::debug("RepairService: failed to parse block references for {}",
                                      fileEntry.path().string());
                    }
                }
            }
        }
    }

    // Find orphaned chunks
    std::vector<std::pair<std::string, fs::path>> orphanedChunks;
    size_t dirsScanned = 0;
    if (fs::exists(objectsPath)) {
        for (const auto& dirEntry : fs::directory_iterator(objectsPath)) {
            if (!fs::is_directory(dirEntry))
                continue;
            ++dirsScanned;
            // Emit progress every 64 prefix dirs to keep the stream alive
            if (progress && dirsScanned % 64 == 0) {
                RepairEvent ev;
                ev.phase = "repairing";
                ev.operation = "chunks";
                ev.message = "Scanning directory " + std::to_string(dirsScanned) + "...";
                progress(ev);
            }
            std::string dirName = dirEntry.path().filename().string();
            for (const auto& fileEntry : fs::directory_iterator(dirEntry.path())) {
                if (!fs::is_regular_file(fileEntry))
                    continue;
                std::string fn = fileEntry.path().filename().string();
                if (fn.size() > 9 && fn.substr(fn.size() - 9) == ".manifest")
                    continue;
                std::string fullHash = dirName + fn;
                if (referencedHashes.find(fullHash) == referencedHashes.end()) {
                    orphanedChunks.push_back({fullHash, fileEntry.path()});
                }
            }
        }
    }

    result.processed = orphanedChunks.size();

    if (orphanedChunks.empty()) {
        result.message = "No orphaned chunks";
        sqlite3_close(db);
        return result;
    }

    if (dryRun) {
        result.skipped = orphanedChunks.size();
        result.message = "Would delete " + std::to_string(orphanedChunks.size()) + " chunks";
        sqlite3_close(db);
        return result;
    }

    for (const auto& [hash, path] : orphanedChunks) {
        try {
            fs::remove(path);
            result.succeeded++;
        } catch (...) {
            result.failed++;
        }
    }

    // Clean zero-ref entries from refs.db
    sqlite3_exec(db, "DELETE FROM block_references WHERE ref_count = 0", nullptr, nullptr, nullptr);

    sqlite3_close(db);
    result.message = "Deleted " + std::to_string(result.succeeded) + " chunks";
    return result;
}

class OrphanedChunksOperation final : public IRepairOperation {
public:
    std::string_view name() const noexcept override { return "chunks"; }
    std::uint64_t code() const noexcept override { return 7; }

    RepairOperationResult run(OperationEnv& env, const RepairRequest& req,
                              const RepairService::ProgressFn& progress,
                              std::atomic<bool>* cancelRequested) override {
        (void)cancelRequested;
        return cleanOrphanedChunks(env, req.dryRun, req.verbose, progress);
    }
};

} // namespace

std::unique_ptr<IRepairOperation> makeOrphanedChunksOperation() {
    return std::make_unique<OrphanedChunksOperation>();
}

} // namespace yams::daemon::repair
