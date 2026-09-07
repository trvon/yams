// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

// Repair operation "optimize" (wire code 13); see repair_operation.h.

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

RepairOperationResult optimizeDatabase(OperationEnv& env, bool dryRun, bool verbose,
                                       RepairService::ProgressFn progress) {
    RepairOperationResult result;
    result.operation = "optimize";

    namespace fs = std::filesystem;
    fs::path dbPath = env.cfg.dataDir / "yams.db";
    fs::path vecDbPath = env.cfg.dataDir / "vectors.db";

    int dbCount = 0;
    if (fs::exists(dbPath))
        dbCount++;
    if (fs::exists(vecDbPath))
        dbCount++;

    if (dbCount == 0) {
        result.message = "No databases found";
        return result;
    }

    result.processed = dbCount;

    if (dryRun) {
        result.skipped = dbCount;
        result.message = "Would optimize " + std::to_string(dbCount) + " database(s)";
        return result;
    }

    int succeeded = 0;
    std::string failMsg;

    // Helper: run live-safe SQLite maintenance for a single database.
    //
    // Avoid VACUUM here. `repair` runs inside the daemon while the same process owns long-lived
    // metadata/vector connections. A full VACUUM needs heavyweight locks and can create a large
    // transient database copy; on large stores this can stall repair streaming or exhaust disk/RSS,
    // surfacing to the CLI as `[ipc:eof]` if the daemon disconnects. Keep daemon repair maintenance
    // bounded: checkpoint opportunistically and let SQLite's PRAGMA optimize choose cheap work.
    auto optimizeOne = [&](const fs::path& path, const char* label) {
        sqlite3* db = nullptr;
        if (sqlite3_open_v2(path.string().c_str(), &db, SQLITE_OPEN_READWRITE, nullptr) !=
            SQLITE_OK) {
            failMsg += std::string(label) + ": failed to open; ";
            if (db)
                sqlite3_close(db);
            return false;
        }
        sqlite3_busy_timeout(db, 5000);

        auto emit = [&](std::string phase, std::string message) {
            if (!progress)
                return;
            RepairEvent ev;
            ev.operation = "optimize";
            ev.phase = std::move(phase);
            ev.processed = result.processed;
            ev.succeeded = succeeded;
            ev.failed = result.failed;
            ev.message = std::string(label) + ": " + message;
            progress(ev);
        };

        emit("repairing", "checkpointing WAL");
        int walLog = 0, walCkpt = 0;
        sqlite3_wal_checkpoint_v2(db, nullptr, SQLITE_CHECKPOINT_PASSIVE, &walLog, &walCkpt);
        spdlog::info("[Optimize] {} WAL checkpoint: log={} checkpointed={}", label, walLog,
                     walCkpt);

        auto exec = [&](const char* sql) -> bool {
            char* errMsg = nullptr;
            const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &errMsg);
            if (rc == SQLITE_OK)
                return true;
            std::string error = errMsg ? errMsg : sqlite3_errmsg(db);
            sqlite3_free(errMsg);
            failMsg += std::string(label) + ": " + sql + " failed: " + error + "; ";
            return false;
        };

        emit("repairing", "running PRAGMA optimize");
        bool ok = exec("PRAGMA analysis_limit=1000") && exec("PRAGMA optimize");
        if (ok) {
            spdlog::info("[Optimize] {} PRAGMA optimize completed", label);
            emit("repairing", "optimized");
        }
        sqlite3_close(db);
        return ok;
    };

    if (fs::exists(dbPath) && optimizeOne(dbPath, "yams.db")) {
        succeeded++;
    }
    if (fs::exists(vecDbPath) && optimizeOne(vecDbPath, "vectors.db")) {
        succeeded++;
    }

    result.succeeded = succeeded;
    result.failed = dbCount - succeeded;

    if (result.failed == 0) {
        result.message = "All databases optimized";
    } else {
        result.message = "Optimize partially failed: " + failMsg;
    }

    return result;
}

class OptimizeDatabaseOperation final : public IRepairOperation {
public:
    std::string_view name() const noexcept override { return "optimize"; }
    std::uint64_t code() const noexcept override { return 13; }

    RepairOperationResult run(OperationEnv& env, const RepairRequest& req,
                              const RepairService::ProgressFn& progress,
                              std::atomic<bool>* cancelRequested) override {
        (void)cancelRequested;
        return optimizeDatabase(env, req.dryRun, req.verbose, progress);
    }
};

} // namespace

std::unique_ptr<IRepairOperation> makeOptimizeDatabaseOperation() {
    return std::make_unique<OptimizeDatabaseOperation>();
}

} // namespace yams::daemon::repair
