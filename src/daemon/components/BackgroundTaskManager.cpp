// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: Apache-2.0

#include <yams/daemon/components/BackgroundTaskManager.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/extraction/extraction_util.h>
#include <yams/repair/embedding_repair_util.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <limits>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

namespace yams::daemon {

#include <yams/daemon/client/global_io_context.h>

BackgroundTaskManager::BackgroundTaskManager(Dependencies deps)
    : deps_(std::move(deps)), stopRequested_(std::make_shared<std::atomic<bool>>(false)) {
    // Ensure a valid executor; fall back to global executor to avoid bad_executor on restart paths.
    if (!deps_.executor) {
        try {
            deps_.executor = yams::daemon::GlobalIOContext::global_executor();
            spdlog::warn("[BackgroundTaskManager] No executor provided; using global executor");
        } catch (...) {
            throw std::invalid_argument("BackgroundTaskManager: executor cannot be null");
        }
    }
}

BackgroundTaskManager::~BackgroundTaskManager() {
    // Coroutines will exit naturally when the executor stops.
    // Just set the stop flag so they can observe it.
    if (running_.load(std::memory_order_acquire)) {
        stopRequested_->store(true, std::memory_order_release);
    }
}

void BackgroundTaskManager::start() {
    // Idempotent check
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        spdlog::debug("[BackgroundTaskManager] Already running, skipping start");
        return;
    }

    spdlog::debug("[BackgroundTaskManager] Starting background task coroutines");

    // Validate ServiceManager is available
    if (deps_.serviceManager.expired()) {
        spdlog::error(
            "[BackgroundTaskManager] Cannot launch consumers: ServiceManager weak_ptr expired");
        running_.store(false, std::memory_order_release);
        throw std::runtime_error("BackgroundTaskManager: ServiceManager weak_ptr expired");
    }

    // Launch all consumer coroutines
    try {
        launchEmbedJobConsumer();
        launchFts5JobConsumer();
        launchOrphanScanTask();
        spdlog::info("[BackgroundTaskManager] Background tasks launched successfully");
    } catch (const std::exception& e) {
        spdlog::error("[BackgroundTaskManager] Failed to launch background tasks: {}", e.what());
        running_.store(false, std::memory_order_release);
        deps_.lifecycleFsm.setSubsystemDegraded("background_tasks", true, e.what());
        throw;
    }
}

void BackgroundTaskManager::stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
        spdlog::warn("[BackgroundTaskManager] Stop called but not running");
        return;
    }

    spdlog::info("[BackgroundTaskManager] Stopping background tasks");

    // Signal coroutines to stop
    stopRequested_->store(true, std::memory_order_release);

    // Timer cancellation happens in destructor for RAII cleanup
}

void BackgroundTaskManager::launchEmbedJobConsumer() {
    auto self = deps_.serviceManager.lock();
    if (!self) {
        throw std::runtime_error(
            "BackgroundTaskManager: ServiceManager weak_ptr expired in launchEmbedJobConsumer");
    }

    auto exec = deps_.executor;
    auto stopFlag = stopRequested_;

    spdlog::debug("[BackgroundTaskManager] Launching EmbedJob consumer");
    boost::asio::co_spawn(
        exec,
        [self, stopFlag]() -> boost::asio::awaitable<void> {
            spdlog::debug("[EmbedJob] Consumer started");
            using Bus = yams::daemon::InternalEventBus;
            auto channel = Bus::instance().get_or_create_channel<Bus::EmbedJob>("embed_jobs", 512);
            using namespace std::chrono_literals;

            // Create local timer (not a member, no shared state)
            auto executor = co_await boost::asio::this_coro::executor;
            boost::asio::steady_timer timer(executor);

            while (!stopFlag->load(std::memory_order_acquire)) {
                Bus::EmbedJob job;
                if (channel && channel->try_pop(job)) {
                    auto store = self->getContentStore();
                    auto meta = self->getMetadataRepo();
                    auto vecDb = self->getVectorDatabase();
                    auto provider = self->getModelProvider();

                    std::string modelName;
                    if (provider && provider->isAvailable()) {
                        try {
                            modelName = self->getEmbeddingModelName();
                        } catch (...) {
                        }
                    }

                    if (!meta || !vecDb || !provider || modelName.empty()) {
                        spdlog::debug("[EmbedJob] Services not ready, dropping {} docs",
                                      job.hashes.size());
                        Bus::instance().incEmbedDropped();
                    } else {
                        const auto& extractors = self->getContentExtractors();
                        yams::repair::EmbeddingRepairConfig cfg;
                        cfg.batchSize = job.batchSize;
                        cfg.skipExisting = job.skipExisting;

                        try {
                            cfg.dataPath = self->getResolvedDataDir();
                        } catch (...) {
                        }

                        auto result = yams::repair::repairMissingEmbeddings(
                            store, meta, provider, modelName, cfg, job.hashes, nullptr, extractors);

                        if (result) {
                            spdlog::debug("[EmbedJob] Processed {} docs (gen={}, skip={}, fail={})",
                                          job.hashes.size(), result.value().embeddingsGenerated,
                                          result.value().embeddingsSkipped,
                                          result.value().failedOperations);
                        } else {
                            spdlog::warn("[EmbedJob] Batch failed: {}", result.error().message);
                        }
                    }

                    Bus::instance().incEmbedConsumed();
                    continue;
                }

                timer.expires_after(100ms);
                try {
                    co_await timer.async_wait(boost::asio::use_awaitable);
                } catch (const boost::system::system_error& e) {
                    if (e.code() == boost::asio::error::operation_aborted) {
                        break; // Exit gracefully on cancellation
                    }
                    throw;
                }
            }
            spdlog::debug("[EmbedJob] Consumer stopped");
            co_return;
        },
        boost::asio::detached);
}

void BackgroundTaskManager::launchFts5JobConsumer() {
    auto self = deps_.serviceManager.lock();
    if (!self) {
        throw std::runtime_error(
            "BackgroundTaskManager: ServiceManager weak_ptr expired in launchFts5JobConsumer");
    }

    auto exec = deps_.executor;
    auto stopFlag = stopRequested_;

    spdlog::debug("[BackgroundTaskManager] Launching Fts5Job consumer");
    boost::asio::co_spawn(
        exec,
        [self, stopFlag]() -> boost::asio::awaitable<void> {
            spdlog::debug("[Fts5Job] Consumer started");
            using Bus = yams::daemon::InternalEventBus;
            auto channel = Bus::instance().get_or_create_channel<Bus::Fts5Job>("fts5_jobs", 512);
            using namespace std::chrono_literals;

            // Create local timer (not a member, no shared state)
            auto executor = co_await boost::asio::this_coro::executor;
            boost::asio::steady_timer timer(executor);

            while (!stopFlag->load(std::memory_order_acquire)) {
                Bus::Fts5Job job;
                if (channel && channel->try_pop(job)) {
                    const auto [store, meta] =
                        std::tuple{self->getContentStore(), self->getMetadataRepo()};

                    if (!meta) {
                        spdlog::debug("[Fts5Job] Metadata not ready, dropping {} docs",
                                      job.hashes.size());
                    } else if (job.operation == Bus::Fts5Operation::ExtractAndIndex) {
                        // Delegate to PostIngestQueue - it already handles extraction in worker
                        // threads
                        auto postIngest = self->getPostIngestQueue();
                        if (!postIngest) {
                            spdlog::debug("[Fts5Job] PostIngestQueue not ready, dropping {} docs",
                                          job.hashes.size());
                            continue;
                        }

                        size_t enqueued{0}, skipped{0};
                        for (const auto& h : job.hashes) {
                            // Get MIME type for the task
                            std::string mime;
                            if (meta) {
                                auto docRes = meta->getDocumentByHash(h);
                                if (docRes && docRes.value().has_value()) {
                                    mime = docRes.value()->mimeType;
                                }
                            }

                            // Enqueue to PostIngestQueue for extraction in worker threads
                            PostIngestQueue::Task task{h, mime,
                                                       "", // session
                                                       std::chrono::steady_clock::now(),
                                                       PostIngestQueue::Task::Stage::Metadata};

                            if (postIngest->tryEnqueue(std::move(task))) {
                                ++enqueued;
                            } else {
                                ++skipped;
                            }
                        }

                        if (enqueued > 0) {
                            spdlog::debug("[Fts5Job] Enqueued {} docs to PostIngestQueue ({} "
                                          "skipped/duplicate)",
                                          enqueued, skipped);
                        }
                    } else if (job.operation == Bus::Fts5Operation::RemoveOrphans) {
                        size_t removed{0}, skipped{0};
                        for (const auto& h : job.hashes) {
                            auto docRes = meta->getDocumentByHash(h);
                            if (!docRes || !docRes.value().has_value()) {
                                if (auto removeRes = meta->removeFromIndexByHash(h); removeRes) {
                                    ++removed;
                                } else {
                                    ++skipped;
                                }
                            } else {
                                ++skipped;
                            }
                        }

                        if (removed > 0) {
                            spdlog::info("[Fts5Job] Removed {} orphans ({} skipped)", removed,
                                         skipped);
                            Bus::instance().incOrphansRemoved(removed);
                        } else if (skipped > 0) {
                            spdlog::debug(
                                "[Fts5Job] No orphans removed ({} skipped - docs still exist)",
                                skipped);
                        }
                    }

                    Bus::instance().incFts5Consumed();
                    continue;
                }

                timer.expires_after(200ms);
                try {
                    co_await timer.async_wait(boost::asio::use_awaitable);
                } catch (const boost::system::system_error& e) {
                    if (e.code() == boost::asio::error::operation_aborted) {
                        break; // Exit gracefully on cancellation
                    }
                    throw;
                }
            }
            spdlog::debug("[Fts5Job] Consumer stopped");
            co_return;
        },
        boost::asio::detached);
}

void BackgroundTaskManager::launchOrphanScanTask() {
    auto self = deps_.serviceManager.lock();
    if (!self) {
        throw std::runtime_error(
            "BackgroundTaskManager: ServiceManager weak_ptr expired in launchOrphanScanTask");
    }

    auto exec = deps_.executor;
    auto stopFlag = stopRequested_;

    spdlog::debug("[BackgroundTaskManager] Launching OrphanScan task");
    boost::asio::co_spawn(
        exec,
        [self, stopFlag]() -> boost::asio::awaitable<void> {
            using Bus = yams::daemon::InternalEventBus;
            using namespace std::chrono_literals;

            // Create local timer (not a member, no shared state)
            auto executor = co_await boost::asio::this_coro::executor;
            boost::asio::steady_timer timer(executor);

            timer.expires_after(5min);
            try {
                co_await timer.async_wait(boost::asio::use_awaitable);
            } catch (const boost::system::system_error& e) {
                if (e.code() == boost::asio::error::operation_aborted) {
                    co_return; // Exit if cancelled during initial delay
                }
                throw;
            }

            while (!stopFlag->load(std::memory_order_acquire)) {
                if (auto meta = self->getMetadataRepo(); meta) {
                    spdlog::debug("[OrphanScan] Starting scan");

                    auto fts5IdsRes = meta->getAllFts5IndexedDocumentIds();
                    if (!fts5IdsRes) {
                        spdlog::warn("[OrphanScan] Query failed: {}", fts5IdsRes.error().message);
                    } else if (const auto& fts5Ids = fts5IdsRes.value(); !fts5Ids.empty()) {
                        yams::metadata::DocumentQueryOptions opts;
                        opts.limit = std::numeric_limits<int>::max();
                        auto allDocsRes = meta->queryDocuments(opts);

                        if (allDocsRes && !allDocsRes.value().empty()) {
                            std::unordered_set<int64_t> validDocIds;
                            std::unordered_map<int64_t, std::string> docIdToHash;
                            for (const auto& doc : allDocsRes.value()) {
                                validDocIds.insert(doc.id);
                                docIdToHash[doc.id] = doc.sha256Hash;
                            }

                            std::vector<std::string> orphanHashes;
                            for (int64_t fts5Id : fts5Ids) {
                                if (!validDocIds.contains(fts5Id)) {
                                    auto it = docIdToHash.find(fts5Id);
                                    orphanHashes.push_back(it != docIdToHash.end()
                                                               ? it->second
                                                               : "orphan_id_" +
                                                                     std::to_string(fts5Id));
                                }
                            }

                            if (orphanHashes.empty()) {
                                spdlog::debug("[OrphanScan] No orphans ({} entries checked)",
                                              fts5Ids.size());
                            } else {
                                spdlog::info("[OrphanScan] Detected {} orphans",
                                             orphanHashes.size());
                                Bus::instance().incOrphansDetected(orphanHashes.size());

                                auto fts5Q = Bus::instance().get_or_create_channel<Bus::Fts5Job>(
                                    "fts5_jobs", 512);
                                constexpr size_t BATCH_SIZE = 50;

                                for (size_t i = 0; i < orphanHashes.size(); i += BATCH_SIZE) {
                                    size_t batchEnd = std::min(i + BATCH_SIZE, orphanHashes.size());
                                    std::vector<std::string> batch(orphanHashes.begin() + i,
                                                                   orphanHashes.begin() + batchEnd);

                                    Bus::Fts5Job orphanJob{std::move(batch),
                                                           static_cast<uint32_t>(BATCH_SIZE),
                                                           Bus::Fts5Operation::RemoveOrphans};

                                    if (!fts5Q->try_push(std::move(orphanJob))) {
                                        spdlog::warn("[OrphanScan] Queue full, batch dropped");
                                    }
                                }
                            }
                        }
                    }
                }

                auto now = std::chrono::system_clock::now();
                auto epochMs =
                    std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch())
                        .count();
                Bus::instance().setLastOrphanScanTime(static_cast<uint64_t>(epochMs));

                auto intervalHours = TuneAdvisor::orphanScanIntervalHours();
                timer.expires_after(std::chrono::hours(intervalHours));
                try {
                    co_await timer.async_wait(boost::asio::use_awaitable);
                } catch (const boost::system::system_error& e) {
                    if (e.code() == boost::asio::error::operation_aborted) {
                        break; // Exit if cancelled
                    }
                    throw;
                }
            }
            spdlog::debug("[OrphanScan] Task stopped");
            co_return;
        },
        boost::asio::detached);
}

} // namespace yams::daemon
