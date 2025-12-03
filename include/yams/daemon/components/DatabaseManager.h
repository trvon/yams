// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <yams/core/types.h>
#include <yams/daemon/components/IComponent.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>

#include <filesystem>
#include <memory>

namespace yams::metadata {
class Database;
class ConnectionPool;
class MetadataRepository;
class KnowledgeGraphStore;
} // namespace yams::metadata

namespace yams::daemon {

class StateComponent;

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

    // Accessors
    std::shared_ptr<metadata::Database> getDatabase() const { return database_; }
    std::shared_ptr<metadata::ConnectionPool> getConnectionPool() const { return connectionPool_; }
    std::shared_ptr<metadata::MetadataRepository> getMetadataRepo() const { return metadataRepo_; }
    std::shared_ptr<metadata::KnowledgeGraphStore> getKgStore() const { return kgStore_; }

    /**
     * @brief Check if database is ready for queries.
     */
    bool isReady() const { return database_ != nullptr && metadataRepo_ != nullptr; }

private:
    Dependencies deps_;

    std::shared_ptr<metadata::Database> database_;
    std::shared_ptr<metadata::ConnectionPool> connectionPool_;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore_;

    std::filesystem::path dbPath_;
};

} // namespace yams::daemon
