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
#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

namespace yams::daemon {

namespace {

size_t readPoolPrewarmTarget(size_t defaultTarget) {
    if (const char* env = std::getenv("YAMS_DB_READ_POOL_PREWARM"); env && *env) {
        try {
            return static_cast<size_t>(std::stoul(env));
        } catch (...) {
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
            readPool->acquire(std::chrono::milliseconds(150), metadata::ConnectionPriority::High);
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
        } catch (...) {
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

    if (database_) {
        try {
            database_->close();
        } catch (...) {
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

    metadata::ConnectionPoolConfig dbPoolCfg;
    size_t rec = [&]() {
        try {
            return std::max<size_t>(1, TuneAdvisor::recommendedThreads(0.25));
        } catch (...) {
            size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
            return std::max<size_t>(1, hw / 2);
        }
    }();
    dbPoolCfg.minConnections = std::min<size_t>(std::max<size_t>(2, rec), 8);
    dbPoolCfg.maxConnections = 64;
    if (const char* envMax = std::getenv("YAMS_DB_POOL_MAX"); envMax && *envMax) {
        try {
            auto v = static_cast<size_t>(std::stoul(envMax));
            if (v >= dbPoolCfg.minConnections)
                dbPoolCfg.maxConnections = v;
        } catch (...) {
        }
    }
    if (const char* envMin = std::getenv("YAMS_DB_POOL_MIN"); envMin && *envMin) {
        try {
            auto v = static_cast<size_t>(std::stoul(envMin));
            if (v > 0)
                dbPoolCfg.minConnections = v;
        } catch (...) {
        }
    }

    std::shared_ptr<metadata::ConnectionPool> writePool;
    {
        std::lock_guard<std::mutex> lk(poolMutex_);
        connectionPool_ = std::make_shared<metadata::ConnectionPool>(dbPath.string(), dbPoolCfg);
        writePool = connectionPool_;
    }
    TuneAdvisor::setStoragePoolSize(static_cast<uint32_t>(dbPoolCfg.maxConnections));
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
    if (const char* envDual = std::getenv("YAMS_DB_DUAL_POOL"); envDual && *envDual) {
        std::string value(envDual);
        std::transform(value.begin(), value.end(), value.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        dualPoolEnabled = (value != "0" && value != "false" && value != "off" && value != "no");
    }

    if (dualPoolEnabled) {
        auto readCfg = dbPoolCfg;
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
        if (const char* envReadMax = std::getenv("YAMS_DB_READ_POOL_MAX");
            envReadMax && *envReadMax) {
            try {
                auto v = static_cast<size_t>(std::stoul(envReadMax));
                if (v >= readCfg.minConnections)
                    readCfg.maxConnections = v;
            } catch (...) {
            }
        }
        if (const char* envReadMin = std::getenv("YAMS_DB_READ_POOL_MIN");
            envReadMin && *envReadMin) {
            try {
                auto v = static_cast<size_t>(std::stoul(envReadMin));
                if (v > 0)
                    readCfg.minConnections = v;
            } catch (...) {
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

    auto journalRes = writePool->withConnection([](metadata::Database& db) -> Result<std::string> {
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
    });
    if (journalRes) {
        spdlog::info("[DatabaseManager] Metadata DB journal_mode={}", journalRes.value());
    } else {
        spdlog::warn("[DatabaseManager] Failed to read Metadata DB journal_mode: {}",
                     journalRes.error().message);
    }

    return true;
}

} // namespace yams::daemon
