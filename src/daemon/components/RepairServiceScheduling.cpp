#include <yams/daemon/components/RepairService.h>

#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/knowledge_graph_store.h>

#include <spdlog/spdlog.h>
#include <yams/profiling.h>

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::daemon {

bool RepairService::maintenanceAllowed() const {
    YAMS_ZONE_SCOPED_N("RepairSched::maintenanceAllowed");
    if (!activeConnFn_)
        return false;
    return activeConnFn_() == 0;
}

std::shared_ptr<GraphComponent> RepairService::getGraphComponentForScheduling() const {
    YAMS_ZONE_SCOPED_N("RepairSched::getGraphComponentForScheduling");
    return ctx_.getGraphComponent ? ctx_.getGraphComponent() : nullptr;
}

std::shared_ptr<metadata::KnowledgeGraphStore> RepairService::getKgStoreForScheduling() const {
    YAMS_ZONE_SCOPED_N("RepairSched::getKgStoreForScheduling");
    return ctx_.getKgStore ? ctx_.getKgStore() : nullptr;
}

void RepairService::onDocumentAdded(const DocumentAddedEvent& event) {
    YAMS_ZONE_SCOPED_N("RepairSched::onDocumentAdded");
    if (!cfg_.enable || !running_)
        return;

    if (ctx_.getPostIngestQueue) {
        auto piq = ctx_.getPostIngestQueue();
        if (piq && piq->started()) {
            spdlog::debug("RepairService: skipping DocumentAdded {} -- handled by PostIngestQueue",
                          event.hash);
            return;
        }
    }

    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        if (pendingSet_.find(event.hash) == pendingSet_.end()) {
            if (cfg_.maxPendingRepairs > 0 && pendingDocuments_.size() >= cfg_.maxPendingRepairs) {
                if (state_) {
                    state_->stats.repairQueueDepth.store(
                        static_cast<uint64_t>(pendingDocuments_.size()));
                }
                spdlog::warn("RepairService: dropping DocumentAdded {} -- pending queue at cap {}",
                             event.hash, cfg_.maxPendingRepairs);
                return;
            }
            pendingSet_.insert(event.hash);
            pendingDocuments_.push(event.hash);
        }
        if (state_)
            state_->stats.repairQueueDepth.store(static_cast<uint64_t>(pendingDocuments_.size()));
    }
    queueCv_.notify_one();
}

void RepairService::onDocumentRemoved(const DocumentRemovedEvent& event) {
    YAMS_ZONE_SCOPED_N("RepairSched::onDocumentRemoved");
    if (!cfg_.enable || !running_)
        return;
    spdlog::debug("RepairService: document {} removed", event.hash);
}

void RepairService::enqueueEmbeddingRepair(const std::vector<std::string>& hashes) {
    YAMS_ZONE_SCOPED_N("RepairSched::enqueueEmbeddingRepair");
    if (!cfg_.enable || !running_ || hashes.empty())
        return;
    size_t enqueuedCount = 0;
    size_t droppedAtCap = 0;
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        for (const auto& hash : hashes) {
            if (pendingSet_.find(hash) == pendingSet_.end()) {
                if (cfg_.maxPendingRepairs > 0 &&
                    pendingDocuments_.size() >= cfg_.maxPendingRepairs) {
                    ++droppedAtCap;
                    continue;
                }
                pendingSet_.insert(hash);
                pendingDocuments_.push(hash);
                ++enqueuedCount;
            }
        }
        if (state_)
            state_->stats.repairQueueDepth.store(static_cast<uint64_t>(pendingDocuments_.size()));
    }
    queueCv_.notify_one();
    spdlog::debug("RepairService: queued {} docs for embedding repair", enqueuedCount);
    if (droppedAtCap > 0) {
        spdlog::warn("RepairService: dropped {} embedding-repair hashes -- pending queue at cap {}",
                     droppedAtCap, cfg_.maxPendingRepairs);
    }
}

} // namespace yams::daemon
