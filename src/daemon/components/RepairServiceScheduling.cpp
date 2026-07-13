#include <yams/daemon/components/RepairService.h>

#include <yams/daemon/components/GraphComponent.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
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

namespace {

std::unordered_map<std::string, std::string> buildExtensionLanguageMap(
    const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>>& extractors) {
    std::unordered_map<std::string, std::string> result;
    for (const auto& extractor : extractors) {
        if (!extractor || !extractor->table())
            continue;
        auto supported = extractor->getSupportedExtensions();
        for (const auto& [ext, lang] : supported)
            result[ext] = lang;
    }
    return result;
}

} // namespace

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

const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>>&
RepairService::getSymbolExtractorsForScheduling() const {
    static const std::vector<std::shared_ptr<AbiSymbolExtractorAdapter>> kEmpty;
    return ctx_.getSymbolExtractors ? ctx_.getSymbolExtractors() : kEmpty;
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

    try {
        auto gc = getGraphComponentForScheduling();
        const auto& symbolExtractors = getSymbolExtractorsForScheduling();
        if (gc && !symbolExtractors.empty()) {
            static thread_local std::unordered_map<std::string, std::string> extToLang;
            static thread_local bool mapInitialized = false;
            if (!mapInitialized) {
                extToLang = buildExtensionLanguageMap(symbolExtractors);
                mapInitialized = true;
            }
            std::string extension;
            auto dotPos = event.path.find_last_of('.');
            if (dotPos != std::string::npos)
                extension = event.path.substr(dotPos + 1);
            std::string dottedExt;
            if (!extension.empty())
                dottedExt = "." + extension;
            auto it = extToLang.find(extension);
            if (it == extToLang.end() && !dottedExt.empty())
                it = extToLang.find(dottedExt);
            if (it != extToLang.end()) {
                auto kg = getKgStoreForScheduling();
                std::optional<std::int64_t> docId;
                if (kg) {
                    auto idRes = kg->getDocumentIdByHash(event.hash);
                    if (idRes.has_value())
                        docId = idRes.value();
                }
                bool alreadyExtracted = false;
                if (kg && docId.has_value()) {
                    auto entRes = kg->getDocEntitiesForDocument(docId.value(), 1, 0);
                    if (entRes.has_value() && !entRes.value().empty())
                        alreadyExtracted = true;
                }
                if (!alreadyExtracted) {
                    GraphComponent::EntityExtractionJob job{.documentHash = event.hash,
                                                            .filePath = event.path,
                                                            .contentUtf8 = {},
                                                            .language = {}};
                    job.language.append(it->second.data(), it->second.size());
                    (void)gc->submitEntityExtraction(std::move(job));
                }
            }
        }
    } catch (const std::exception& e) {
        spdlog::warn("RepairService: symbol extraction scheduling failed: {}", e.what());
    } catch (...) {
    }
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
