// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/daemon/components/DatabaseManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <spdlog/spdlog.h>

#include <chrono>

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

    auto startTime = std::chrono::steady_clock::now();

    try {
        co_return co_await boost::asio::async_initiate<decltype(boost::asio::use_awaitable),
                                                       void(std::exception_ptr, bool)>(
            [this, executor, timeoutMs, startTime](auto handler) mutable {
                auto completed = std::make_shared<std::atomic<bool>>(false);
                auto timer = std::make_shared<boost::asio::steady_timer>(executor);
                timer->expires_after(std::chrono::milliseconds(timeoutMs));

                using HandlerT = std::decay_t<decltype(handler)>;
                auto handlerPtr = std::make_shared<HandlerT>(std::move(handler));
                auto completion_exec = boost::asio::get_associated_executor(*handlerPtr, executor);

                timer->async_wait([completed, handlerPtr, completion_exec,
                                   timeoutMs](const boost::system::error_code& ec) mutable {
                    if (ec)
                        return;
                    if (!completed->exchange(true, std::memory_order_acq_rel)) {
                        spdlog::warn("[DatabaseManager] open timed out after {} ms — degraded mode",
                                     timeoutMs);
                        boost::asio::post(completion_exec, [h = std::move(*handlerPtr)]() mutable {
                            std::move(h)(std::exception_ptr{}, false);
                        });
                    }
                });

                boost::asio::post(executor, [this, timer, completed, handlerPtr, completion_exec,
                                             startTime]() mutable {
                    bool success = false;
                    std::exception_ptr ep;
                    try {
                        auto r =
                            database_->open(dbPath_.string(), metadata::ConnectionMode::Create);
                        success = static_cast<bool>(r);
                        if (!success) {
                            spdlog::warn("[DatabaseManager] open failed: {} — degraded mode",
                                         r.error().message);
                            stats_.openErrors.fetch_add(1, std::memory_order_relaxed);
                        } else if (deps_.state) {
                            deps_.state->readiness.databaseReady = true;
                        }
                    } catch (const std::exception& e) {
                        spdlog::warn("[DatabaseManager] open threw: {} — degraded mode", e.what());
                        stats_.openErrors.fetch_add(1, std::memory_order_relaxed);
                        ep = std::current_exception();
                    } catch (...) {
                        stats_.openErrors.fetch_add(1, std::memory_order_relaxed);
                        ep = std::current_exception();
                    }

                    auto endTime = std::chrono::steady_clock::now();
                    auto durationMs =
                        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime)
                            .count();
                    stats_.openDurationMs.store(static_cast<uint64_t>(durationMs),
                                                std::memory_order_relaxed);

                    if (!completed->exchange(true, std::memory_order_acq_rel)) {
                        timer->cancel();
                        if (success) {
                            spdlog::info("[DatabaseManager] Database opened successfully");
                        }
                        boost::asio::post(completion_exec,
                                          [h = std::move(*handlerPtr), ep, success]() mutable {
                                              std::move(h)(ep, success);
                                          });
                    }
                });
            },
            boost::asio::use_awaitable);

    } catch (const std::exception& e) {
        spdlog::warn("[DatabaseManager] open exception: {} — degraded mode", e.what());
        stats_.openErrors.fetch_add(1, std::memory_order_relaxed);
        co_return false;
    }
}

boost::asio::awaitable<bool> DatabaseManager::migrate(int timeoutMs,
                                                      boost::asio::any_io_executor executor) {
    if (!database_) {
        spdlog::warn("[DatabaseManager] Cannot migrate: database not open");
        co_return false;
    }

    auto mm = std::make_shared<metadata::MigrationManager>(*database_);
    auto initResult = mm->initialize();
    if (!initResult) {
        spdlog::error("[DatabaseManager] Failed to initialize migration system: {}",
                      initResult.error().message);
        stats_.migrationErrors.fetch_add(1, std::memory_order_relaxed);
        co_return false;
    }
    mm->registerMigrations(metadata::YamsMetadataMigrations::getAllMigrations());

    auto startTime = std::chrono::steady_clock::now();

    try {
        co_return co_await boost::asio::async_initiate<decltype(boost::asio::use_awaitable),
                                                       void(std::exception_ptr, bool)>(
            [this, mm, executor, timeoutMs, startTime](auto handler) mutable {
                auto completed = std::make_shared<std::atomic<bool>>(false);
                auto timer = std::make_shared<boost::asio::steady_timer>(executor);
                timer->expires_after(std::chrono::milliseconds(timeoutMs));

                using HandlerT = std::decay_t<decltype(handler)>;
                auto handlerPtr = std::make_shared<HandlerT>(std::move(handler));
                auto completion_exec = boost::asio::get_associated_executor(*handlerPtr, executor);

                timer->async_wait([completed, handlerPtr, completion_exec,
                                   timeoutMs](const boost::system::error_code& ec) mutable {
                    if (ec)
                        return;
                    if (!completed->exchange(true, std::memory_order_acq_rel)) {
                        spdlog::warn("[DatabaseManager] migration timed out after {} ms",
                                     timeoutMs);
                        boost::asio::post(completion_exec, [h = std::move(*handlerPtr)]() mutable {
                            std::move(h)(std::exception_ptr{}, false);
                        });
                    }
                });

                boost::asio::post(executor, [this, mm, timer, completed, handlerPtr,
                                             completion_exec, startTime]() mutable {
                    bool success = false;
                    std::exception_ptr ep;
                    try {
                        auto r = mm->migrate();
                        success = static_cast<bool>(r);
                        if (!success) {
                            spdlog::warn("[DatabaseManager] migration failed: {}",
                                         r.error().message);
                            stats_.migrationErrors.fetch_add(1, std::memory_order_relaxed);
                        }
                    } catch (const std::exception& e) {
                        spdlog::warn("[DatabaseManager] migration threw: {}", e.what());
                        stats_.migrationErrors.fetch_add(1, std::memory_order_relaxed);
                        ep = std::current_exception();
                    } catch (...) {
                        stats_.migrationErrors.fetch_add(1, std::memory_order_relaxed);
                        ep = std::current_exception();
                    }

                    auto endTime = std::chrono::steady_clock::now();
                    auto durationMs =
                        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime)
                            .count();
                    stats_.migrationDurationMs.store(static_cast<uint64_t>(durationMs),
                                                     std::memory_order_relaxed);

                    if (!completed->exchange(true, std::memory_order_acq_rel)) {
                        timer->cancel();
                        if (success) {
                            spdlog::info("[DatabaseManager] Database migrations completed");
                        }
                        boost::asio::post(completion_exec,
                                          [h = std::move(*handlerPtr), ep, success]() mutable {
                                              std::move(h)(ep, success);
                                          });
                    }
                });
            },
            boost::asio::use_awaitable);

    } catch (const std::exception& e) {
        spdlog::warn("[DatabaseManager] migration exception: {}", e.what());
        stats_.migrationErrors.fetch_add(1, std::memory_order_relaxed);
        co_return false;
    }
}

bool DatabaseManager::initializeRepositories(const std::filesystem::path& dbPath) {
    if (!database_) {
        spdlog::warn("[DatabaseManager] Cannot init repositories: database not open");
        return false;
    }

    try {
        metadata::ConnectionPoolConfig poolCfg{};
        connectionPool_ = std::make_shared<metadata::ConnectionPool>(dbPath.string(), poolCfg);

        metadataRepo_ = std::make_shared<metadata::MetadataRepository>(*connectionPool_);

        metadata::KnowledgeGraphStoreConfig kgCfg{};
        auto kgRes = metadata::makeSqliteKnowledgeGraphStore(*connectionPool_, kgCfg);
        if (!kgRes) {
            spdlog::error("[DatabaseManager] Failed to create KnowledgeGraphStore: {}",
                          kgRes.error().message);
            stats_.repositoryInitErrors.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        auto uniqueKg = std::move(kgRes).value();
        kgStore_ = std::shared_ptr<metadata::KnowledgeGraphStore>(std::move(uniqueKg));

        if (deps_.state) {
            deps_.state->readiness.metadataRepoReady = true;
        }

        spdlog::info("[DatabaseManager] Repositories initialized");
        return true;

    } catch (const std::exception& e) {
        spdlog::error("[DatabaseManager] Failed to initialize repositories: {}", e.what());
        stats_.repositoryInitErrors.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
}

} // namespace yams::daemon
