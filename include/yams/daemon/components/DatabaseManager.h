// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/core/types.h>
#include <yams/daemon/components/IComponent.h>
#include <yams/daemon/components/WalMetricsProvider.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>

#include <atomic>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>

namespace yams::api {
class IContentStore;
} // namespace yams::api

namespace yams::metadata {
class Database;
class ConnectionPool;
class MetadataRepository;
class KnowledgeGraphStore;
} // namespace yams::metadata

namespace yams::wal {
class WALManager;
} // namespace yams::wal

namespace yams::daemon {

struct StateComponent;

/**
 * @brief Manages metadata database lifecycle.
 *
 * Extracted from ServiceManager (PBI-088) to centralize database concerns.
 *
 * ## Responsibilities
 * - Database open/close with timeout handling
 * - Schema migration
 * - Connection pool management
 * - MetadataRepository lifecycle
 * - KnowledgeGraphStore lifecycle
 *
 * ## Thread Safety
 * - All operations are awaitable coroutines for async execution
 * - Accessors are thread-safe
 */
class DatabaseManager : public IComponent {
public:
    /**
     * @brief Dependency injection for DatabaseManager.
     */
    struct Dependencies {
        /// State component for readiness tracking
        StateComponent* state{nullptr};
    };

    explicit DatabaseManager(Dependencies deps);
    ~DatabaseManager() override;

    // IComponent interface
    const char* getName() const override { return "DatabaseManager"; }
    Result<void> initialize() override;
    void shutdown() override;

    /**
     * @brief Open database asynchronously with timeout.
     *
     * @param dbPath Path to database file
     * @param timeoutMs Timeout in milliseconds
     * @param executor Executor for async operations
     * @return Awaitable<bool> - true if opened successfully
     */
    boost::asio::awaitable<bool> open(const std::filesystem::path& dbPath, int timeoutMs,
                                      boost::asio::any_io_executor executor);

    /**
     * @brief Run database migrations with timeout.
     *
     * @param timeoutMs Timeout in milliseconds
     * @param executor Executor for async operations
     * @return Awaitable<bool> - true if migrations succeeded
     */
    boost::asio::awaitable<bool> migrate(int timeoutMs, boost::asio::any_io_executor executor);

    /**
     * @brief Initialize connection pool and repositories.
     *
     * Should be called after open() and migrate() succeed.
     *
     * @param dbPath Path to database file
     * @return true if initialization succeeded
     */
    bool initializeRepositories(const std::filesystem::path& dbPath);

    /**
     * @brief Initialize write + optional read-only connection pools and MetadataRepository.
     *
     * Honors env overrides (YAMS_DB_POOL_MIN/MAX, YAMS_DB_DUAL_POOL,
     * YAMS_DB_READ_POOL_MIN/MAX, YAMS_DB_READ_POOL_PREWARM). Updates TuneAdvisor
     * with the chosen pool size and prewarms the read pool.
     *
     * @param dbPath Path to database file
     * @return true if pools and repository initialized successfully
     */
    bool initializePools(const std::filesystem::path& dbPath);

    // Accessors
    std::shared_ptr<metadata::Database> getDatabase() const { return database_; }
    std::shared_ptr<metadata::ConnectionPool> getConnectionPool() const {
        std::lock_guard<std::mutex> lk(poolMutex_);
        return connectionPool_;
    }
    std::shared_ptr<metadata::ConnectionPool> getReadConnectionPool() const {
        std::lock_guard<std::mutex> lk(poolMutex_);
        return readConnectionPool_;
    }
    std::shared_ptr<metadata::MetadataRepository> getMetadataRepo() const { return metadataRepo_; }
    std::shared_ptr<metadata::KnowledgeGraphStore> getKgStore() const { return kgStore_; }

    /**
     * @brief Publish a ServiceManager-constructed KG store into DatabaseManager.
     * Used when ServiceManager initialization creates the KG store from the
     * shared write pool (outside initializeRepositories/initializePools).
     */
    void setKgStore(std::shared_ptr<metadata::KnowledgeGraphStore> store) {
        kgStore_ = std::move(store);
    }

    void setMetadataRepo(std::shared_ptr<metadata::MetadataRepository> repo) {
        metadataRepo_ = std::move(repo);
    }

    /**
     * @brief Adopt an already-open Database handle.
     *
     * ServiceManager currently opens the database directly (Phase 6a was
     * partial). Publishing the handle here lets initializePools() satisfy
     * its "database open" precondition so the MetadataRepository gets
     * constructed.
     */
    void setDatabase(std::shared_ptr<metadata::Database> database) {
        database_ = std::move(database);
    }

    /**
     * @brief Store the content store shared_ptr (atomic release).
     */
    void setContentStore(std::shared_ptr<api::IContentStore> store) {
        std::atomic_store_explicit(&contentStore_, std::move(store), std::memory_order_release);
    }

    /**
     * @brief Load the content store shared_ptr (atomic acquire).
     */
    std::shared_ptr<api::IContentStore> getContentStore() const {
        return std::atomic_load_explicit(&contentStore_, std::memory_order_acquire);
    }

    /**
     * @brief Set the content store initialization error diagnostic.
     */
    void setContentStoreError(std::string err) {
        std::lock_guard<std::mutex> lk(contentStoreErrorMutex_);
        contentStoreError_ = std::move(err);
    }

    /**
     * @brief Read the content store initialization error diagnostic.
     */
    std::string getContentStoreError() const {
        std::lock_guard<std::mutex> lk(contentStoreErrorMutex_);
        return contentStoreError_;
    }

    /**
     * @brief Initialize the WAL manager against a data directory.
     *
     * Creates a WALManager rooted at dataDir/wal and attaches it to the
     * internal metrics provider. Safe to call when the platform has no
     * WAL support — the method simply no-ops on failure.
     */
    void initializeWal(const std::filesystem::path& dataDir);

    /**
     * @brief Shutdown and release the WAL manager (idempotent).
     */
    void shutdownWal();

    /**
     * @brief Attach an externally-created WAL manager to the metrics provider.
     */
    void attachWalManager(std::shared_ptr<yams::wal::WALManager> wal);

    std::shared_ptr<yams::wal::WALManager> getWalManager() const { return walManager_; }
    std::shared_ptr<WalMetricsProvider> getWalMetricsProvider() const {
        return walMetricsProvider_;
    }

    /**
     * @brief Check if database is ready for queries.
     */
    bool isReady() const { return database_ != nullptr && metadataRepo_ != nullptr; }

    /**
     * @brief Database operation statistics.
     */
    struct Stats {
        std::atomic<uint64_t> openDurationMs{0};
        std::atomic<uint64_t> migrationDurationMs{0};
        std::atomic<uint64_t> openErrors{0};
        std::atomic<uint64_t> migrationErrors{0};
        std::atomic<uint64_t> connectionPoolSize{0};
        std::atomic<uint64_t> activeConnections{0};
        std::atomic<uint64_t> repositoryInitErrors{0};
    };

    /**
     * @brief Get database statistics.
     */
    const Stats& getStats() const { return stats_; }

private:
    Dependencies deps_;

    std::shared_ptr<metadata::Database> database_;
    mutable std::mutex poolMutex_;
    std::shared_ptr<metadata::ConnectionPool> connectionPool_;
    std::shared_ptr<metadata::ConnectionPool> readConnectionPool_;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore_;

    std::shared_ptr<yams::wal::WALManager> walManager_;
    std::shared_ptr<WalMetricsProvider> walMetricsProvider_;

    std::shared_ptr<api::IContentStore> contentStore_;
    mutable std::mutex contentStoreErrorMutex_;
    std::string contentStoreError_;

    std::filesystem::path dbPath_;

    mutable Stats stats_;
};

} // namespace yams::daemon
