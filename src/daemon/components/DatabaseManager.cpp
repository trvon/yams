// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/daemon/components/DatabaseManager.h>
#include <yams/daemon/components/init_utils.hpp>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/metric_keys.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/wal/wal_manager.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

namespace yams::daemon {

namespace {

std::string getenvCopy(std::string_view name) {
    static std::mutex envMutex;
    std::lock_guard<std::mutex> lock(envMutex);
    const std::string key(name);
    const char* env = std::getenv(key.c_str()); // NOLINT(concurrency-mt-unsafe)
    if (!env || !*env) {
        return {};
    }
    return std::string(env);
}

std::optional<size_t> parseSizeEnv(const char* raw) {
    if (!raw || !*raw || *raw == '-') {
        return std::nullopt;
    }
    size_t value{};
    const char* end = raw + std::char_traits<char>::length(raw);
    auto [ptr, ec] = std::from_chars(raw, end, value);
    if (ec != std::errc{} || ptr != end) {
        return std::nullopt;
    }
    return value;
}

size_t readPoolPrewarmTarget(size_t defaultTarget) {
    if (const std::string env = getenvCopy("YAMS_DB_READ_POOL_PREWARM"); !env.empty()) {
        if (auto parsed = parseSizeEnv(env.c_str())) {
            return *parsed;
        }
    }
    return defaultTarget;
}

void prewarmReadPool(const std::shared_ptr<metadata::ConnectionPool>& readPool,
                     size_t targetConnections) {
    if (!readPool || targetConnections == 0) {
        return;
    }

    std::vector<std::unique_ptr<metadata::PooledConnection>> held;
    held.reserve(targetConnections);

    for (size_t i = 0; i < targetConnections; ++i) {
        auto connResult =
            readPool->acquire(std::chrono::milliseconds(150), metadata::ConnectionPriority::High,
                              "DatabaseManager::prewarmReadPool");
        if (!connResult) {
            break;
        }
        held.push_back(std::move(connResult).value());
    }

    size_t warmed = 0;
    for (auto& conn : held) {
        try {
            auto& db = **conn;
            for (const char* sql :
                 {"SELECT 1 FROM documents LIMIT 1", "SELECT rowid FROM documents_fts LIMIT 1",
                  "SELECT 1 FROM metadata LIMIT 1"}) {
                auto stmtResult = db.prepareCached(sql);
                if (!stmtResult) {
                    continue;
                }
                auto stmt = std::move(stmtResult).value();
                (void)stmt->step();
            }
            ++warmed;
        } catch (const std::exception& e) {
            spdlog::debug("[DatabaseManager] Read-pool prewarm probe failed: {}", e.what());
        } catch (...) {
            spdlog::debug("[DatabaseManager] Read-pool prewarm probe failed: unknown exception");
        }
    }

    spdlog::info("[DatabaseManager] Read pool prewarm complete: warmed {} connection(s)", warmed);
}

} // namespace

DatabaseManager::DatabaseManager(Dependencies deps) : deps_(std::move(deps)) {}

DatabaseManager::~DatabaseManager() {
    shutdown();
}

Result<void> DatabaseManager::initialize() {
    // Actual initialization is deferred to open() which takes path and executor
    return Result<void>{};
}

void DatabaseManager::shutdown() {
    spdlog::debug("[DatabaseManager] Shutting down");

    shutdownWal();

    kgStore_.reset();
    if (metadataRepo_) {
        try {
            metadataRepo_->shutdown();
        } catch (const std::exception& e) {
            spdlog::debug("[DatabaseManager] MetadataRepository shutdown failed: {}", e.what());
        } catch (...) {
            spdlog::debug(
                "[DatabaseManager] MetadataRepository shutdown failed: unknown exception");
        }
    }
    metadataRepo_.reset();
    std::shared_ptr<metadata::ConnectionPool> readPool;
    std::shared_ptr<metadata::ConnectionPool> writePool;
    {
        std::lock_guard<std::mutex> lk(poolMutex_);
        readPool = std::move(readConnectionPool_);
        writePool = std::move(connectionPool_);
    }
    if (readPool) {
        try {
            readPool->shutdown();
        } catch (const std::exception& e) {
            spdlog::debug("[DatabaseManager] ReadConnectionPool shutdown failed: {}", e.what());
        } catch (...) {
            spdlog::debug(
                "[DatabaseManager] ReadConnectionPool shutdown failed: unknown exception");
        }
    }
    if (writePool) {
        try {
            writePool->shutdown();
        } catch (const std::exception& e) {
            spdlog::debug("[DatabaseManager] ConnectionPool shutdown failed: {}", e.what());
        } catch (...) {
            spdlog::debug("[DatabaseManager] ConnectionPool shutdown failed: unknown exception");
        }
    }

    if (database_ && database_->isOpen()) {
        auto checkpointResult = database_->execute("PRAGMA wal_checkpoint(TRUNCATE)");
        if (!checkpointResult) {
            spdlog::warn("[DatabaseManager] Shutdown WAL checkpoint (TRUNCATE) failed: {}",
                         checkpointResult.error().message);
        } else {
            spdlog::info("[DatabaseManager] Shutdown WAL checkpoint (TRUNCATE) completed");
        }
    }

    if (database_) {
        try {
            database_->close();
        } catch (const std::exception& e) {
            spdlog::debug("[DatabaseManager] database close failed: {}", e.what());
        } catch (...) {
            spdlog::debug("[DatabaseManager] database close failed: unknown exception");
        }
        database_.reset();
    }
}

// NOTE: async open()/migrate() helpers were removed (orphaned).
// ServiceManager now owns the coroutine-driven DB open and migration paths
// via co_openDatabase / co_migrateDatabase which post blocking work to a
// dedicated thread_pool and await real completion with init::co_await_future.

void DatabaseManager::initializeWal(const std::filesystem::path& dataDir) {
    try {
        yams::wal::WALManager::Config walConfig;
        walConfig.walDirectory = dataDir / "wal";
        walManager_ = std::make_shared<yams::wal::WALManager>(walConfig);
        if (auto result = walManager_->initialize(); !result) {
            spdlog::warn("[DatabaseManager] WALManager initialization failed: {}",
                         result.error().message);
            walManager_.reset();
            return;
        }
        spdlog::info("[DatabaseManager] WALManager initialized");
        attachWalManager(walManager_);
        spdlog::debug("[DatabaseManager] WALManager attached to metrics provider");
    } catch (const std::exception& e) {
        spdlog::warn("[DatabaseManager] WALManager initialization threw: {}", e.what());
        walManager_.reset();
    }
}

void DatabaseManager::interruptPendingConnectionAcquiresForShutdown() {
    std::shared_ptr<metadata::ConnectionPool> readPool;
    std::shared_ptr<metadata::ConnectionPool> writePool;
    {
        std::lock_guard<std::mutex> lk(poolMutex_);
        readPool = readConnectionPool_;
        writePool = connectionPool_;
    }
    if (readPool) {
        readPool->interruptPendingAcquires();
    }
    if (writePool) {
        writePool->interruptPendingAcquires();
    }
}

void DatabaseManager::shutdownWal() {
    if (!walManager_) {
        return;
    }
    try {
        auto result = walManager_->shutdown();
        if (!result) {
            spdlog::warn("[DatabaseManager] WALManager shutdown failed: {}",
                         result.error().message);
        } else {
            spdlog::info("[DatabaseManager] WALManager shutdown complete");
        }
    } catch (const std::exception& e) {
        spdlog::warn("[DatabaseManager] WALManager shutdown threw: {}", e.what());
    } catch (...) {
        spdlog::warn("[DatabaseManager] WALManager shutdown threw unknown exception");
    }
    walManager_.reset();
}

void DatabaseManager::attachWalManager(std::shared_ptr<yams::wal::WALManager> wal) {
    if (!walMetricsProvider_) {
        walMetricsProvider_ = std::make_shared<WalMetricsProvider>();
    }
    walMetricsProvider_->setManager(std::move(wal));
}

bool DatabaseManager::initializePools(const std::filesystem::path& dbPath) {
    if (!database_) {
        spdlog::warn("[DatabaseManager] Cannot init pools: database not open");
        return false;
    }

    std::map<std::string, uint64_t> fallbackDurations;
    auto& durations = deps_.state ? deps_.state->initDurationsMs : fallbackDurations;

    size_t readRecommended = [&]() {
        try {
            return std::max<size_t>(1, TuneAdvisor::recommendedThreads(0.25));
        } catch (...) {
            size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
            return std::max<size_t>(1, hw / 2);
        }
    }();
    size_t readPoolMin = std::min<size_t>(std::max<size_t>(2, readRecommended), 8);
    size_t readPoolMax = 64;
    if (const std::string envMax = getenvCopy("YAMS_DB_POOL_MAX"); !envMax.empty()) {
        if (auto v = parseSizeEnv(envMax.c_str()); v && *v >= readPoolMin) {
            readPoolMax = *v;
        }
    }
    if (const std::string envMin = getenvCopy("YAMS_DB_POOL_MIN"); !envMin.empty()) {
        if (auto v = parseSizeEnv(envMin.c_str()); v && *v > 0) {
            readPoolMin = *v;
        }
    }

    metadata::ConnectionPoolConfig dbPoolCfg;
    dbPoolCfg.minConnections = 1;
    dbPoolCfg.maxConnections = 1;

    std::shared_ptr<metadata::ConnectionPool> writePool;
    {
        std::lock_guard<std::mutex> lk(poolMutex_);
        connectionPool_ = std::make_shared<metadata::ConnectionPool>(dbPath.string(), dbPoolCfg);
        writePool = connectionPool_;
    }
    // The daemon intentionally has a single metadata write connection while DB writes are being
    // centralized through WriteCoordinator. Under migration, any legacy direct write that holds
    // this connection too long can starve all writers; lower the slow-holder threshold so the
    // culprit is visible before callers hit the 30s acquisition timeout.
    writePool->setSlowHolderThreshold(std::chrono::milliseconds(1000));
    TuneAdvisor::setStoragePoolSize(static_cast<uint32_t>(readPoolMax));
    TuneAdvisor::setEnableParallelIngest(true);

    auto poolInit =
        init::record_duration("db_pool", [&]() { return writePool->initialize(); }, durations);
    if (!poolInit) {
        spdlog::warn("[DatabaseManager] Connection pool init failed: {} — continuing degraded",
                     poolInit.error().message);
        stats_.repositoryInitErrors.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    bool dualPoolEnabled = true;
    if (std::string value = getenvCopy("YAMS_DB_DUAL_POOL"); !value.empty()) {
        std::transform(value.begin(), value.end(), value.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        dualPoolEnabled = (value != "0" && value != "false" && value != "off" && value != "no");
    }

    if (dualPoolEnabled) {
        metadata::ConnectionPoolConfig readCfg;
        readCfg.minConnections = readPoolMin;
        readCfg.maxConnections = readPoolMax;
        readCfg.readOnly = true;
        {
            const size_t kMaxReadConnections =
                TuneAdvisor::readPoolMaxConnections(readCfg.maxConnections);
            if (readCfg.maxConnections > kMaxReadConnections) {
                readCfg.maxConnections = kMaxReadConnections;
                spdlog::info("[DatabaseManager] Read pool max capped to {} (I/O concurrency limit)",
                             kMaxReadConnections);
            }
        }
        if (const std::string envReadMax = getenvCopy("YAMS_DB_READ_POOL_MAX");
            !envReadMax.empty()) {
            if (auto v = parseSizeEnv(envReadMax.c_str()); v && *v >= readCfg.minConnections) {
                readCfg.maxConnections = *v;
            }
        }
        if (const std::string envReadMin = getenvCopy("YAMS_DB_READ_POOL_MIN");
            !envReadMin.empty()) {
            if (auto v = parseSizeEnv(envReadMin.c_str()); v && *v > 0) {
                readCfg.minConnections = *v;
            }
        }

        std::shared_ptr<metadata::ConnectionPool> readPool;
        {
            std::lock_guard<std::mutex> lk(poolMutex_);
            readConnectionPool_ =
                std::make_shared<metadata::ConnectionPool>(dbPath.string(), readCfg);
            readPool = readConnectionPool_;
        }
        auto readPoolInit = init::record_duration(
            "db_read_pool", [&]() { return readPool->initialize(); }, durations);
        if (!readPoolInit) {
            spdlog::warn(
                "[DatabaseManager] Read connection pool init failed (falling back to single "
                "pool): {}",
                readPoolInit.error().message);
            std::lock_guard<std::mutex> lk(poolMutex_);
            readConnectionPool_.reset();
            readPool.reset();
        } else {
            const size_t defaultPrewarm = std::min<size_t>(readCfg.maxConnections, 8);
            prewarmReadPool(readPool, readPoolPrewarmTarget(defaultPrewarm));
            spdlog::info("[DatabaseManager] Dual DB pool mode enabled (write/work + read-only)");
        }
    }

    std::shared_ptr<metadata::ConnectionPool> readPoolSnap;
    {
        std::lock_guard<std::mutex> lk(poolMutex_);
        readPoolSnap = readConnectionPool_;
    }

    auto repoRes = init::record_duration(
        std::string(readiness::kMetadataRepo),
        [&]() -> yams::Result<void> {
            metadataRepo_ =
                std::make_shared<metadata::MetadataRepository>(*writePool, readPoolSnap.get());
            if (deps_.state) {
                deps_.state->readiness.metadataRepoReady = true;
            }
            metadataRepo_->initializeCounters();
            spdlog::info("[DatabaseManager] Metadata repository initialized successfully");
            return yams::Result<void>();
        },
        durations);
    if (!repoRes) {
        spdlog::warn("[DatabaseManager] Metadata repository init failed: {}",
                     repoRes.error().message);
        stats_.repositoryInitErrors.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    auto journalRes = writePool->withConnection(
        [](metadata::Database& db) -> Result<std::string> {
            auto stmtRes = db.prepare("PRAGMA journal_mode");
            if (!stmtRes) {
                return stmtRes.error();
            }
            auto stmt = std::move(stmtRes).value();
            auto stepRes = stmt.step();
            if (!stepRes) {
                return stepRes.error();
            }
            if (!stepRes.value()) {
                return Error{ErrorCode::NotFound, "PRAGMA journal_mode returned no rows"};
            }
            return stmt.getString(0);
        },
        metadata::ConnectionPriority::Normal, "DatabaseManager::journalMode");
    if (journalRes) {
        spdlog::info("[DatabaseManager] Metadata DB journal_mode={}", journalRes.value());
    } else {
        spdlog::warn("[DatabaseManager] Failed to read Metadata DB journal_mode: {}",
                     journalRes.error().message);
    }

    return true;
}

} // namespace yams::daemon
