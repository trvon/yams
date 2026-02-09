#include <yams/cli/daemon_helpers.h>
#include <yams/cli/doctor_checks.h>
#include <yams/cli/vector_db_util.h>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>

// Namespace alias for vector utilities (avoid collision with yams::vector)
namespace vecutil = yams::cli::vecutil;

#include <sqlite3.h>
#include <cstdlib>
#include <filesystem>

namespace yams::cli::doctor {

namespace fs = std::filesystem;

// ============================================================================
// Daemon Health Check
// ============================================================================

DaemonCheckResult checkDaemon(YamsCLI* cli, std::optional<daemon::StatusResponse>& cachedStatus) {
    DaemonCheckResult result;

    std::string socketPath = daemon::DaemonClient::resolveSocketPathConfigFirst().string();

    // Check if daemon is running
    if (!cachedStatus) {
        if (!daemon::DaemonClient::isDaemonRunning(socketPath)) {
            result.issues.push_back("Daemon not running on socket: " + socketPath);
            return result;
        }
    }

    result.running = true;

    // Fetch status if not cached
    if (!cachedStatus) {
        try {
            daemon::ClientConfig cfg;
            if (cli && cli->hasExplicitDataDir()) {
                cfg.dataDir = cli->getDataPath();
            }
            cfg.requestTimeout = std::chrono::milliseconds(10000);

            auto leaseRes = acquire_cli_daemon_client_shared(cfg);
            if (!leaseRes) {
                result.issues.push_back("Failed to connect: " + leaseRes.error().message);
                return result;
            }

            auto leaseHandle = std::move(leaseRes.value());
            auto& client = **leaseHandle;

            auto statusRes =
                run_result<daemon::StatusResponse>(client.status(), std::chrono::seconds(10));
            if (!statusRes) {
                result.issues.push_back("Failed to get status: " + statusRes.error().message);
                return result;
            }

            cachedStatus = std::move(statusRes.value());
        } catch (const std::exception& e) {
            result.issues.push_back(std::string("Exception: ") + e.what());
            return result;
        }
    }

    // Extract status information
    const auto& status = cachedStatus.value();
    result.ready = status.ready;
    result.lifecycleState =
        status.lifecycleState.empty() ? status.overallStatus : status.lifecycleState;
    result.version = status.version;
    result.activeConnections = status.activeConnections;
    result.memoryMb = status.memoryUsageMb;
    result.cpuPercent = status.cpuUsagePercent;

    // Vector scoring status
    if (auto it = status.readinessStates.find("vector_embeddings_available");
        it != status.readinessStates.end()) {
        result.vectorEmbeddingsAvailable = it->second;
    }
    if (auto it = status.readinessStates.find("vector_scoring_enabled");
        it != status.readinessStates.end()) {
        result.vectorScoringEnabled = it->second;
    }

    // Worker pool info
    if (auto it = status.requestCounts.find("worker_threads"); it != status.requestCounts.end()) {
        result.workerThreads = it->second;
    }
    if (auto it = status.requestCounts.find("worker_active"); it != status.requestCounts.end()) {
        result.workerActive = it->second;
    }
    if (auto it = status.requestCounts.find("worker_queued"); it != status.requestCounts.end()) {
        result.workerQueued = it->second;
    }

    return result;
}

// ============================================================================
// Installed Models Check
// ============================================================================

ModelsCheckResult checkInstalledModels(YamsCLI* cli) {
    ModelsCheckResult result;

    const char* home = std::getenv("HOME");
    if (!home) {
        return result;
    }

    // Deprecated location
    result.deprecatedPath = fs::path(home) / ".yams" / "models";

    // Current location
    result.currentPath = cli ? cli->getDataPath() / "models" : fs::path();
    if (result.currentPath.empty()) {
        result.currentPath = yams::config::get_data_dir() / "models";
    }

    std::error_code ec;

    // Check deprecated location
    if (fs::exists(result.deprecatedPath, ec) && fs::is_directory(result.deprecatedPath, ec)) {
        for (const auto& entry : fs::directory_iterator(result.deprecatedPath, ec)) {
            if (entry.is_directory() && fs::exists(entry.path() / "model.onnx", ec)) {
                result.oldLocationCount++;
            }
        }
    }

    if (result.oldLocationCount > 0) {
        result.warnings.push_back(
            "Found " + std::to_string(result.oldLocationCount) +
            " model(s) in deprecated location: " + result.deprecatedPath.string());
        result.warnings.push_back("Models should be in: " + result.currentPath.string());
    }

    // Scan current location
    if (!fs::exists(result.currentPath, ec) || !fs::is_directory(result.currentPath, ec)) {
        return result;
    }

    for (const auto& entry : fs::directory_iterator(result.currentPath, ec)) {
        if (!entry.is_directory()) {
            continue;
        }

        const auto& dir = entry.path();
        if (!fs::exists(dir / "model.onnx", ec)) {
            continue;
        }

        ModelInfo model;
        model.name = dir.filename().string();
        model.path = dir;
        model.hasConfig = fs::exists(dir / "config.json", ec) ||
                          fs::exists(dir / "sentence_bert_config.json", ec);
        model.hasTokenizer = fs::exists(dir / "tokenizer.json", ec);

        // Dimension heuristic
        if (auto dim = vecutil::getModelDimensionHeuristic(model.name)) {
            model.dimension = static_cast<int>(*dim);
        }

        // Generate warnings
        if (!model.hasConfig) {
            result.warnings.push_back(
                model.name + ": missing config.json - run 'yams model download --apply-config " +
                model.name + " --force'");
        }

        if (model.name.find("nomic") != std::string::npos && !model.hasTokenizer) {
            result.warnings.push_back(
                model.name + ": missing tokenizer.json - run 'yams model download --apply-config " +
                model.name + " --force'");
        }

        result.installed.push_back(std::move(model));
    }

    return result;
}

// ============================================================================
// Vector Database Check
// ============================================================================

VectorDbCheckResult checkVectorDb(YamsCLI* cli) {
    VectorDbCheckResult result;

    result.dbPath =
        cli ? cli->getDataPath() / "vectors.db" : yams::config::get_data_dir() / "vectors.db";

    auto validation = vecutil::validateVecSchema(result.dbPath);

    result.exists = validation.exists;
    result.vec0Available = validation.vec0Available;
    result.schemaValid = validation.schemaValid;
    result.usesVec0 = validation.usesVec0;
    result.dimension = validation.dimension;

    if (!validation.error.empty()) {
        result.issues.push_back(validation.error);
    }

    // Determine schema type
    if (!result.exists) {
        result.schemaType = "none";
    } else if (result.usesVec0) {
        result.schemaType = "vec0";
    } else if (result.schemaValid) {
        result.schemaType = "legacy";
        result.issues.push_back("doc_embeddings table not using vec0 virtual table");
    } else {
        result.schemaType = "none";
    }

    return result;
}

// ============================================================================
// Dimension Mismatch Check
// ============================================================================

DimensionCheckResult checkDimensions(YamsCLI* cli,
                                     const std::optional<daemon::StatusResponse>& cachedStatus) {
    DimensionCheckResult result;

    // Get DB status from daemon or validate locally
    if (cachedStatus) {
        result.dbReady = cachedStatus->vectorDbReady;
        if (auto it = cachedStatus->readinessStates.find("vector_db");
            it != cachedStatus->readinessStates.end()) {
            result.dbReady = it->second;
        }
        result.dbDimension = cachedStatus->vectorDbDim;
    } else {
        fs::path dbPath =
            cli ? cli->getDataPath() / "vectors.db" : yams::config::get_data_dir() / "vectors.db";
        if (auto dim = vecutil::getDimensionFromDb(dbPath)) {
            result.dbReady = true;
            result.dbDimension = *dim;
        }
    }

    // Resolve target dimension
    fs::path dataPath = cli ? cli->getDataPath() : yams::config::get_data_dir();
    auto resolved = vecutil::resolveEmbeddingDimension(cli, dataPath);
    result.targetDimension = resolved.dimension;
    result.targetSource = resolved.source;

    // If we couldn't get target from resolution but daemon has it, use that
    if (result.targetDimension == 0 && cachedStatus && cachedStatus->embeddingAvailable &&
        cachedStatus->embeddingDim > 0) {
        result.targetDimension = cachedStatus->embeddingDim;
        result.targetSource = "daemon_provider";
    }

    // Check for mismatch
    if (result.dbReady && result.targetDimension > 0) {
        result.matches = (result.dbDimension == result.targetDimension);

        if (!result.matches) {
            result.recommendations.push_back("DB has " + std::to_string(result.dbDimension) +
                                             "-dim vectors but target requires " +
                                             std::to_string(result.targetDimension) + "-dim");
            result.recommendations.push_back("Option 1: Change model/config to match database (" +
                                             std::to_string(result.dbDimension) + "-dim)");
            result.recommendations.push_back(
                "Option 2: Recreate vector database with 'yams doctor --recreate-vectors'");
        }
    } else if (!result.dbReady) {
        result.recommendations.push_back(
            "Vector database not ready - will be created on first use");
    }

    return result;
}

// ============================================================================
// Knowledge Graph Check
// ============================================================================

KnowledgeGraphCheckResult checkKnowledgeGraph(YamsCLI* cli) {
    KnowledgeGraphCheckResult result;

    if (!cli) {
        result.issues.push_back("CLI context unavailable");
        return result;
    }

    try {
        auto db = cli->getDatabase();
        if (!db || !db->isOpen()) {
            result.issues.push_back("Database not available");
            return result;
        }

        result.available = true;

        auto countTable = [&](const char* sql) -> int64_t {
            auto stmtRes = db->prepare(sql);
            if (!stmtRes) {
                return -1;
            }
            auto stmt = std::move(stmtRes).value();
            auto stepRes = stmt.step();
            if (stepRes && stepRes.value()) {
                return stmt.getInt64(0);
            }
            return -1;
        };

        result.nodeCount = countTable("SELECT COUNT(1) FROM kg_nodes");
        result.edgeCount = countTable("SELECT COUNT(1) FROM kg_edges");
        result.aliasCount = countTable("SELECT COUNT(1) FROM kg_aliases");
        result.embeddingCount = countTable("SELECT COUNT(1) FROM kg_node_embeddings");
        result.docEntityCount = countTable("SELECT COUNT(1) FROM doc_entities");

        result.isEmpty = (result.nodeCount <= 0 && result.docEntityCount <= 0);

        if (result.isEmpty) {
            result.issues.push_back("Knowledge graph empty - run 'yams doctor repair --graph' to "
                                    "build from tags/metadata");
        }

    } catch (const std::exception& e) {
        result.issues.push_back(std::string("Exception: ") + e.what());
    }

    return result;
}

// ============================================================================
// Database Migration Check
// ============================================================================

MigrationCheckResult checkMigrations(YamsCLI* cli) {
    MigrationCheckResult result;

    if (!cli) {
        return result;
    }

    fs::path dbPath = cli->getDataPath() / "yams.db";
    if (!fs::exists(dbPath)) {
        return result;
    }

    sqlite3* db = nullptr;
    if (sqlite3_open(dbPath.string().c_str(), &db) != SQLITE_OK) {
        if (db) {
            sqlite3_close(db);
        }
        return result;
    }

    // Check for failed migrations
    const char* failedSql = "SELECT version, name, error FROM migration_history "
                            "WHERE success=0 ORDER BY applied_at DESC LIMIT 5";
    sqlite3_stmt* stmt = nullptr;

    if (sqlite3_prepare_v2(db, failedSql, -1, &stmt, nullptr) == SQLITE_OK) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            result.hasFailures = true;
            int ver = sqlite3_column_int(stmt, 0);
            const unsigned char* name = sqlite3_column_text(stmt, 1);
            const unsigned char* error = sqlite3_column_text(stmt, 2);

            std::string msg = "version=" + std::to_string(ver);
            if (name) {
                msg += ", name='" + std::string(reinterpret_cast<const char*>(name)) + "'";
            }
            if (error && *error) {
                msg += ", error=\"" + std::string(reinterpret_cast<const char*>(error)) + "\"";
            }
            result.failedMigrations.push_back(msg);
        }
        sqlite3_finalize(stmt);
    }

    // Check for leftover temp tables from failed migrations
    const char* leftoverSql =
        "SELECT name FROM sqlite_master WHERE type='table' AND name='documents_fts_new'";
    if (sqlite3_prepare_v2(db, leftoverSql, -1, &stmt, nullptr) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            result.hasLeftoverTables = true;
            result.leftoverTables.push_back("documents_fts_new");
        }
        sqlite3_finalize(stmt);
    }

    sqlite3_close(db);
    return result;
}

} // namespace yams::cli::doctor
