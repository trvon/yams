#include <yams/compat/thread_stop_compat.h>
#include <yams/daemon/components/RepairCoordinator.h>

#include <spdlog/spdlog.h>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <yams/core/repair_fsm.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/ipc/repair_scheduling_adapter.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/vector_database.h>

namespace yams::daemon {

RepairCoordinator::RepairCoordinator(ServiceManager* services, StateComponent* state,
                                     std::function<size_t()> activeConnFn, Config cfg)
    : services_(services), state_(state), activeConnFn_(std::move(activeConnFn)), cfg_(cfg) {}

RepairCoordinator::~RepairCoordinator() {
    stop();
}

void RepairCoordinator::start() {
    if (!cfg_.enable || running_.exchange(true)) {
        return;
    }
    // Initialize maintenance tokens based on config
    tokens_.store(cfg_.maintenanceTokens);
    thread_ = yams::compat::jthread([this](yams::compat::stop_token st) { run(st); });
    spdlog::info("RepairCoordinator started in event-driven mode (enable={}, batch={})",
                 cfg_.enable, cfg_.maxBatch);
}

void RepairCoordinator::stop() {
    if (!running_.exchange(false)) {
        return;
    }
    if (thread_.joinable()) {
        thread_.request_stop();
        thread_.join();
    }
}

bool RepairCoordinator::maintenance_allowed() const {
    if (!activeConnFn_)
        return false;
    return activeConnFn_() == 0;
}

void RepairCoordinator::onDocumentAdded(const DocumentAddedEvent& event) {
    if (!cfg_.enable || !running_) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        pendingDocuments_.push(event.hash);
    }
    queueCv_.notify_one();
    spdlog::debug("RepairCoordinator: queued document {} for embedding check", event.hash);
}

void RepairCoordinator::onDocumentRemoved(const DocumentRemovedEvent& event) {
    if (!cfg_.enable || !running_) {
        return;
    }

    // For now, just log - in future could clean up orphaned embeddings
    spdlog::debug("RepairCoordinator: document {} removed", event.hash);
}

void RepairCoordinator::run(yams::compat::stop_token st) {
    core::RepairFsm::Config fsmConfig;
    fsmConfig.enable_online_repair = true;
    fsmConfig.max_repair_concurrency = cfg_.maintenanceTokens;
    fsmConfig.repair_backoff_ms = 250;
    fsmConfig.max_retries = 3;

    core::RepairFsm fsm(fsmConfig);
    fsm.set_on_state_change([](core::RepairFsm::State state) {
        spdlog::debug("RepairFsm state changed to: {}", core::RepairFsm::to_string(state));
    });

    bool initialScanEnqueued = false;
    while (!st.stop_requested()) {
        // Wait for work (no timeout - purely event driven)
        std::unique_lock<std::mutex> lock(queueMutex_);
        // If no pending work yet, opportunistically enqueue an initial backlog scan once
        if (pendingDocuments_.empty() && !initialScanEnqueued && maintenance_allowed()) {
            lock.unlock();
            try {
                auto meta = services_ ? services_->getMetadataRepo() : nullptr;
                if (meta) {
                    // Scan all docs to find those without embeddings
                    vector::VectorDatabaseConfig vdbConfig;
                    vdbConfig.database_path = (cfg_.dataDir / "vectors.db").string();
                    vdbConfig.embedding_dim = 384;
                    auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
                    if (vectorDb->initialize()) {
                        auto allDocs = meta->findDocumentsByPath("%");
                        if (allDocs && !allDocs.value().empty()) {
                            size_t enq = 0;
                            {
                                std::lock_guard<std::mutex> ql(queueMutex_);
                                for (const auto& d : allDocs.value()) {
                                    if (st.stop_requested())
                                        break;
                                    if (!vectorDb->hasEmbedding(d.sha256Hash)) {
                                        pendingDocuments_.push(d.sha256Hash);
                                        ++enq;
                                    }
                                }
                            }
                            if (enq > 0) {
                                spdlog::info(
                                    "RepairCoordinator: enqueued {} backlog documents for repair",
                                    enq);
                                queueCv_.notify_one();
                            }
                        }
                    }
                }
            } catch (const std::exception& e) {
                spdlog::debug("Initial backlog scan failed: {}", e.what());
            }
            initialScanEnqueued = true;
            // Re-acquire lock before proceeding to wait
            lock.lock();
        }

        queueCv_.wait(lock,
                      [this, &st] { return !pendingDocuments_.empty() || st.stop_requested(); });

        if (st.stop_requested()) {
            break;
        }

        // Collect batch of documents to process
        std::vector<std::string> batch;
        while (!pendingDocuments_.empty() && batch.size() < cfg_.maxBatch) {
            batch.push_back(pendingDocuments_.front());
            pendingDocuments_.pop();
        }
        lock.unlock();

        // Check scheduling hints
        RepairSchedulingAdapter::SchedulingHints adapterHints{};
        size_t active = activeConnFn_ ? activeConnFn_() : 0;
        adapterHints.streaming_high_load = (active > 0);
        adapterHints.maintenance_allowed = maintenance_allowed();
        adapterHints.closing = st.stop_requested();

        // Convert to RepairFsm hints
        core::RepairFsm::SchedulingHints fsmHints;
        fsmHints.streaming_high_load = adapterHints.streaming_high_load;
        fsmHints.maintenance_allowed = adapterHints.maintenance_allowed;
        fsmHints.closing = adapterHints.closing;

        fsm.set_scheduling_hints(fsmHints);

        if (adapterHints.closing || (adapterHints.streaming_high_load && batch.empty())) {
            if (state_)
                state_->stats.repairBusyTicks++;
            continue;
        }

        // Process batch if we have documents
        if (!batch.empty()) {
            if (state_)
                state_->stats.repairIdleTicks++;

            try {
                auto content = services_ ? services_->getContentStore() : nullptr;
                auto meta = services_ ? services_->getMetadataRepo() : nullptr;
                auto embed = services_ ? services_->getEmbeddingGenerator() : nullptr;

                if (content && meta && embed) {
                    // Start repair FSM
                    if (fsm.start()) {
                        // Scan phase
                        fsm.on_scan_done();

                        // Detect phase - check which documents need embeddings
                        std::vector<std::string> missing;
                        if (!batch.empty()) {
                            // Check specific documents from the queue
                            vector::VectorDatabaseConfig vdbConfig;
                            vdbConfig.database_path = (cfg_.dataDir / "vectors.db").string();
                            vdbConfig.embedding_dim = 384;

                            auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
                            if (vectorDb->initialize()) {
                                for (const auto& hash : batch) {
                                    if (!vectorDb->hasEmbedding(hash)) {
                                        missing.push_back(hash);
                                    }
                                }
                            }
                        }

                        fsm.on_detect_done();

                        if (!missing.empty()) {
                            // Classify phase
                            fsm.on_classify_done();

                            // Isolate phase
                            fsm.on_isolate_done();

                            // Fix phase - generate embeddings
                            if (state_)
                                state_->stats.repairBatchesAttempted++;

                            repair::EmbeddingRepairConfig rcfg;
                            rcfg.batchSize = cfg_.maxBatch;
                            rcfg.skipExisting = true;
                            rcfg.dataPath = cfg_.dataDir;

                            std::unique_ptr<yams::Result<repair::EmbeddingRepairStats>> statsPtr;
                            bool executed = with_token([&] {
                                auto r = repair::repairMissingEmbeddings(content, meta, embed, rcfg,
                                                                         missing);
                                statsPtr =
                                    std::make_unique<yams::Result<repair::EmbeddingRepairStats>>(
                                        std::move(r));
                            });

                            if (!executed) {
                                fsm.on_fix_done(false);
                                continue;
                            }

                            bool fixSuccess = statsPtr && *statsPtr;
                            fsm.on_fix_done(fixSuccess);

                            if (fixSuccess) {
                                // Verify phase
                                fsm.on_verify_done(true);

                                // Reindex phase
                                fsm.on_reindex_done(true);

                                if (state_) {
                                    state_->stats.repairEmbeddingsGenerated +=
                                        static_cast<uint64_t>(
                                            statsPtr->value().embeddingsGenerated);
                                    state_->stats.repairEmbeddingsSkipped +=
                                        static_cast<uint64_t>(statsPtr->value().embeddingsSkipped);
                                    state_->stats.repairFailedOperations +=
                                        static_cast<uint64_t>(statsPtr->value().failedOperations);
                                }

                                spdlog::info("RepairCoordinator: repaired batch (generated={}, "
                                             "skipped={}, failed={})",
                                             statsPtr->value().embeddingsGenerated,
                                             statsPtr->value().embeddingsSkipped,
                                             statsPtr->value().failedOperations);
                            } else {
                                if (statsPtr) {
                                    spdlog::debug("RepairCoordinator: repair failed: {}",
                                                  statsPtr->error().message);
                                } else {
                                    spdlog::debug("RepairCoordinator: repair skipped/no result");
                                }
                            }
                        }
                    }
                }
            } catch (const std::exception& e) {
                spdlog::debug("RepairCoordinator exception: {}", e.what());
            }
        }
    }

    spdlog::info("RepairCoordinator stopped");
}

// token helpers implemented inline in header

} // namespace yams::daemon
