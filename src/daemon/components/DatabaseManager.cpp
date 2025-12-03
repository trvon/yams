// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: Apache-2.0

#include <yams/daemon/components/DatabaseManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <spdlog/spdlog.h>

namespace yams::daemon {

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

    kgStore_.reset();
    metadataRepo_.reset();
    connectionPool_.reset();

    if (database_) {
        try {
            database_->close();
        } catch (...) {
        }
        database_.reset();
    }
}

boost::asio::awaitable<bool> DatabaseManager::open(const std::filesystem::path& dbPath,
                                                   int timeoutMs,
                                                   boost::asio::any_io_executor executor) {
    dbPath_ = dbPath;

    // Create database instance
    database_ = std::make_shared<metadata::Database>();

    // Set up timeout timer
    boost::asio::steady_timer timeoutTimer(executor);
    timeoutTimer.expires_after(std::chrono::milliseconds(timeoutMs));

    try {
        using ResultPtr = std::shared_ptr<Result<void>>;
        using Channel = boost::asio::experimental::basic_channel<
            boost::asio::any_io_executor, boost::asio::experimental::channel_traits<std::mutex>,
            void(std::exception_ptr, ResultPtr)>;
        auto resultChannel = std::make_shared<Channel>(executor, 2);

        // Spawn database open in background
        boost::asio::co_spawn(
            executor,
            [this, resultChannel]() -> boost::asio::awaitable<void> {
                std::exception_ptr opException;
                ResultPtr opResult;
                try {
                    auto r = database_->open(dbPath_.string(), metadata::ConnectionMode::Create);
                    opResult = std::make_shared<Result<void>>(std::move(r));
                } catch (...) {
                    opException = std::current_exception();
                }
                resultChannel->try_send(opException, std::move(opResult));
                co_return;
            },
            boost::asio::detached);

        using namespace boost::asio::experimental::awaitable_operators;

        auto race = co_await (
            resultChannel->async_receive(boost::asio::as_tuple(boost::asio::use_awaitable)) ||
            timeoutTimer.async_wait(boost::asio::as_tuple(boost::asio::use_awaitable)));

        if (race.index() == 1) {
            spdlog::warn("[DatabaseManager] open timed out after {} ms — degraded mode", timeoutMs);
            resultChannel->close();
            co_return false;
        }

        timeoutTimer.cancel();

        const auto channelTuple = std::get<0>(race);
        auto opException = std::get<0>(channelTuple);
        auto opResult = std::get<1>(channelTuple);

        if (opException) {
            try {
                std::rethrow_exception(opException);
            } catch (const std::exception& e) {
                spdlog::warn("[DatabaseManager] open threw: {} — degraded mode", e.what());
            }
            co_return false;
        }

        if (!opResult || !*opResult) {
            auto msg = opResult ? opResult->error().message : "unknown error";
            spdlog::warn("[DatabaseManager] open failed: {} — degraded mode", msg);
            co_return false;
        }

        if (deps_.state) {
            deps_.state->readiness.databaseReady = true;
        }

        spdlog::info("[DatabaseManager] Database opened successfully");
        co_return true;

    } catch (const std::exception& e) {
        spdlog::warn("[DatabaseManager] open exception: {} — degraded mode", e.what());
        co_return false;
    }
}

boost::asio::awaitable<bool> DatabaseManager::migrate(int timeoutMs,
                                                      boost::asio::any_io_executor executor) {
    if (!database_) {
        spdlog::warn("[DatabaseManager] Cannot migrate: database not open");
        co_return false;
    }

    metadata::MigrationManager mm(*database_);
    auto initResult = mm.initialize();
    if (!initResult) {
        spdlog::error("[DatabaseManager] Failed to initialize migration system: {}",
                      initResult.error().message);
        co_return false;
    }
    mm.registerMigrations(metadata::YamsMetadataMigrations::getAllMigrations());

    // Set up timeout timer
    boost::asio::steady_timer timeoutTimer(executor);
    timeoutTimer.expires_after(std::chrono::milliseconds(timeoutMs));

    try {
        using ResultPtr = std::shared_ptr<Result<void>>;
        using Channel = boost::asio::experimental::basic_channel<
            boost::asio::any_io_executor, boost::asio::experimental::channel_traits<std::mutex>,
            void(std::exception_ptr, ResultPtr)>;
        auto resultChannel = std::make_shared<Channel>(executor, 2);

        boost::asio::co_spawn(
            executor,
            [&mm, resultChannel]() -> boost::asio::awaitable<void> {
                std::exception_ptr opException;
                ResultPtr opResult;
                try {
                    auto r = mm.migrate();
                    opResult = std::make_shared<Result<void>>(std::move(r));
                } catch (...) {
                    opException = std::current_exception();
                }
                resultChannel->try_send(opException, std::move(opResult));
                co_return;
            },
            boost::asio::detached);

        using namespace boost::asio::experimental::awaitable_operators;

        auto race = co_await (
            resultChannel->async_receive(boost::asio::as_tuple(boost::asio::use_awaitable)) ||
            timeoutTimer.async_wait(boost::asio::as_tuple(boost::asio::use_awaitable)));

        if (race.index() == 1) {
            spdlog::warn("[DatabaseManager] migration timed out after {} ms", timeoutMs);
            resultChannel->close();
            co_return false;
        }

        timeoutTimer.cancel();

        const auto channelTuple = std::get<0>(race);
        auto opException = std::get<0>(channelTuple);
        auto opResult = std::get<1>(channelTuple);

        if (opException) {
            try {
                std::rethrow_exception(opException);
            } catch (const std::exception& e) {
                spdlog::warn("[DatabaseManager] migration threw: {}", e.what());
            }
            co_return false;
        }

        if (!opResult) {
            spdlog::warn("[DatabaseManager] migration failed before producing result");
            co_return false;
        }

        spdlog::info("[DatabaseManager] Database migrations completed");
        co_return static_cast<bool>(*opResult);

    } catch (const std::exception& e) {
        spdlog::warn("[DatabaseManager] migration exception: {}", e.what());
        co_return false;
    }
}

bool DatabaseManager::initializeRepositories(const std::filesystem::path& dbPath) {
    if (!database_) {
        spdlog::warn("[DatabaseManager] Cannot init repositories: database not open");
        return false;
    }

    try {
        // Create connection pool
        metadata::ConnectionPoolConfig poolCfg{};
        connectionPool_ = std::make_shared<metadata::ConnectionPool>(dbPath.string(), poolCfg);

        // Create metadata repository
        metadataRepo_ = std::make_shared<metadata::MetadataRepository>(*connectionPool_);

        // Create knowledge graph store using factory
        metadata::KnowledgeGraphStoreConfig kgCfg{};
        auto kgRes = metadata::makeSqliteKnowledgeGraphStore(*connectionPool_, kgCfg);
        if (!kgRes) {
            spdlog::error("[DatabaseManager] Failed to create KnowledgeGraphStore: {}",
                          kgRes.error().message);
            return false;
        }
        // Promote unique_ptr to shared_ptr for broader use
        auto uniqueKg = std::move(kgRes).value();
        kgStore_ = std::shared_ptr<metadata::KnowledgeGraphStore>(std::move(uniqueKg));

        if (deps_.state) {
            deps_.state->readiness.metadataRepoReady = true;
        }

        spdlog::info("[DatabaseManager] Repositories initialized");
        return true;

    } catch (const std::exception& e) {
        spdlog::error("[DatabaseManager] Failed to initialize repositories: {}", e.what());
        return false;
    }
}

} // namespace yams::daemon
