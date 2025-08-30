#include <yams/daemon/components/RepairCoordinator.h>

#include <spdlog/spdlog.h>
#include <memory>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/ipc/connection_fsm.h>
#include <yams/daemon/ipc/repair_scheduling_adapter.h>
#include <yams/repair/embedding_repair_util.h>

namespace yams::daemon {

RepairCoordinator::RepairCoordinator(ServiceManager* services, StateComponent* state,
                                     std::function<size_t()> activeConnFn, Config cfg)
    : services_(services), state_(state), activeConnFn_(std::move(activeConnFn)),
      cfg_(std::move(cfg)) {}

RepairCoordinator::~RepairCoordinator() {
    stop();
}

void RepairCoordinator::start() {
    if (!cfg_.enable || running_.exchange(true)) {
        return;
    }
    // Initialize maintenance tokens based on config
    tokens_.store(cfg_.maintenanceTokens);
    thread_ = std::jthread([this](std::stop_token st) { run(st); });
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

void RepairCoordinator::run(std::stop_token st) {
    spdlog::info("RepairCoordinator started (enable={}, batch={}, tickMs={})", cfg_.enable,
                 cfg_.maxBatch, cfg_.tickMs);
    while (!st.stop_requested()) {
        // Derive coarse hints from connection activity; when there are active connections,
        // treat as streaming_high_load; when stopping, treat as closing.
        RepairSchedulingAdapter::SchedulingHints hints{};
        size_t active = activeConnFn_ ? activeConnFn_() : 0;
        hints.streaming_high_load = (active > 0);
        hints.maintenance_allowed = maintenance_allowed();
        // We can't observe daemon closing here directly; rely on stop_requested() for now.
        hints.closing = st.stop_requested();

        if (hints.closing || hints.streaming_high_load) {
            if (state_)
                state_->stats.repairBusyTicks++;
        } else if (hints.maintenance_allowed) {
            if (state_)
                state_->stats.repairIdleTicks++;
            try {
                auto content = services_ ? services_->getContentStore() : nullptr;
                auto meta = services_ ? services_->getMetadataRepo() : nullptr;
                auto embed = services_ ? services_->getEmbeddingGenerator() : nullptr;
                if (content && meta && embed) {
                    // Discover a small batch of documents missing embeddings
                    auto missing =
                        repair::getDocumentsMissingEmbeddings(meta, cfg_.dataDir, cfg_.maxBatch);
                    if (missing && !missing.value().empty()) {
                        if (state_)
                            state_->stats.repairBatchesAttempted++;
                        repair::EmbeddingRepairConfig rcfg;
                        rcfg.batchSize = cfg_.maxBatch;
                        rcfg.skipExisting = true;
                        rcfg.dataPath = cfg_.dataDir;
                        std::unique_ptr<yams::Result<repair::EmbeddingRepairStats>> statsPtr;
                        // Stage-aware gating: use a token to guard heavy embedding generation
                        bool executed = with_token([&] {
                            auto r = repair::repairMissingEmbeddings(content, meta, embed, rcfg,
                                                                     missing.value());
                            statsPtr = std::make_unique<yams::Result<repair::EmbeddingRepairStats>>(
                                std::move(r));
                        });
                        if (!executed) {
                            using namespace std::chrono_literals;
                            std::this_thread::sleep_for(std::chrono::milliseconds(cfg_.tickMs));
                            continue;
                        }
                        if (statsPtr && *statsPtr) {
                            if (state_) {
                                state_->stats.repairEmbeddingsGenerated +=
                                    static_cast<uint64_t>(statsPtr->value().embeddingsGenerated);
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
            } catch (const std::exception& e) {
                spdlog::debug("RepairCoordinator exception: {}", e.what());
            }
        } else {
            if (state_)
                state_->stats.repairBusyTicks++;
        }

        using namespace std::chrono_literals;
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg_.tickMs));
    }
    spdlog::info("RepairCoordinator stopped");
}

// token helpers implemented inline in header

} // namespace yams::daemon
