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
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
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

    try {
        // Use async_initiate pattern with timeout racing (no experimental APIs)
        co_return co_await boost::asio::async_initiate<decltype(boost::asio::use_awaitable),
                                                       void(std::exception_ptr, bool)>(
            [this, executor, timeoutMs](auto handler) mutable {
                // Shared state for race coordination
                auto completed = std::make_shared<std::atomic<bool>>(false);
                auto timer = std::make_shared<boost::asio::steady_timer>(executor);
                timer->expires_after(std::chrono::milliseconds(timeoutMs));

                // Capture handler in shared_ptr for safe sharing between timer and work
                using HandlerT = std::decay_t<decltype(handler)>;
                auto handlerPtr = std::make_shared<HandlerT>(std::move(handler));
                auto completion_exec = boost::asio::get_associated_executor(*handlerPtr, executor);

                // Set up timeout
                timer->async_wait([completed, handlerPtr, completion_exec,
                                   timeoutMs](const boost::system::error_code& ec) mutable {
                    if (ec)
                        return; // Timer cancelled
                    if (!completed->exchange(true, std::memory_order_acq_rel)) {
                        // Timeout won
                        spdlog::warn("[DatabaseManager] open timed out after {} ms — degraded mode",
                                     timeoutMs);
                        boost::asio::post(completion_exec, [h = std::move(*handlerPtr)]() mutable {
                            std::move(h)(nullptr, false);
                        });
                    }
                });

                // Post blocking work to executor
                boost::asio::post(executor, [this, timer, completed, handlerPtr,
                                             completion_exec]() mutable {
                    bool success = false;
                    std::exception_ptr ep;
                    try {
                        auto r =
                            database_->open(dbPath_.string(), metadata::ConnectionMode::Create);
                        success = static_cast<bool>(r);
                        if (!success) {
                            spdlog::warn("[DatabaseManager] open failed: {} — degraded mode",
                                         r.error().message);
                        } else if (deps_.state) {
                            deps_.state->readiness.databaseReady = true;
                        }
                    } catch (const std::exception& e) {
                        spdlog::warn("[DatabaseManager] open threw: {} — degraded mode", e.what());
                        ep = std::current_exception();
                    } catch (...) {
                        ep = std::current_exception();
                    }

                    if (!completed->exchange(true, std::memory_order_acq_rel)) {
                        // Work won
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
        co_return false;
    }
}

boost::asio::awaitable<bool> DatabaseManager::migrate(int timeoutMs,
                                                      boost::asio::any_io_executor executor) {
    if (!database_) {
        spdlog::warn("[DatabaseManager] Cannot migrate: database not open");
        co_return false;
    }

    // Create migration manager on heap so it survives async operations
    auto mm = std::make_shared<metadata::MigrationManager>(*database_);
    auto initResult = mm->initialize();
    if (!initResult) {
        spdlog::error("[DatabaseManager] Failed to initialize migration system: {}",
                      initResult.error().message);
        co_return false;
    }
    mm->registerMigrations(metadata::YamsMetadataMigrations::getAllMigrations());

    try {
        // Use async_initiate pattern with timeout racing (no experimental APIs)
        co_return co_await boost::asio::async_initiate<decltype(boost::asio::use_awaitable),
                                                       void(std::exception_ptr, bool)>(
            [mm, executor, timeoutMs](auto handler) mutable {
                // Shared state for race coordination
                auto completed = std::make_shared<std::atomic<bool>>(false);
                auto timer = std::make_shared<boost::asio::steady_timer>(executor);
                timer->expires_after(std::chrono::milliseconds(timeoutMs));

                // Capture handler in shared_ptr for safe sharing between timer and work
                using HandlerT = std::decay_t<decltype(handler)>;
                auto handlerPtr = std::make_shared<HandlerT>(std::move(handler));
                auto completion_exec = boost::asio::get_associated_executor(*handlerPtr, executor);

                // Set up timeout
                timer->async_wait([completed, handlerPtr, completion_exec,
                                   timeoutMs](const boost::system::error_code& ec) mutable {
                    if (ec)
                        return; // Timer cancelled
                    if (!completed->exchange(true, std::memory_order_acq_rel)) {
                        // Timeout won
                        spdlog::warn("[DatabaseManager] migration timed out after {} ms",
                                     timeoutMs);
                        boost::asio::post(completion_exec, [h = std::move(*handlerPtr)]() mutable {
                            std::move(h)(nullptr, false);
                        });
                    }
                });

                // Post blocking work to executor
                boost::asio::post(
                    executor, [mm, timer, completed, handlerPtr, completion_exec]() mutable {
                        bool success = false;
                        std::exception_ptr ep;
                        try {
                            auto r = mm->migrate();
                            success = static_cast<bool>(r);
                            if (!success) {
                                spdlog::warn("[DatabaseManager] migration failed: {}",
                                             r.error().message);
                            }
                        } catch (const std::exception& e) {
                            spdlog::warn("[DatabaseManager] migration threw: {}", e.what());
                            ep = std::current_exception();
                        } catch (...) {
                            ep = std::current_exception();
                        }

                        if (!completed->exchange(true, std::memory_order_acq_rel)) {
                            // Work won
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
