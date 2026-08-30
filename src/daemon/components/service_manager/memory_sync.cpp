#include <yams/daemon/components/ServiceManager.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <spdlog/spdlog.h>

#include <yams/daemon/components/DatabaseManager.h>
#include <yams/daemon/components/VectorIndexCoordinator.h>
#include <yams/memory_sync/content_blob_sync.h>
#include <yams/memory_sync/memory_sync_service.h>
#include <yams/memory_sync/records.h>
#include <yams/metadata/metadata_sync_adapter.h>
#include <yams/metadata/topology_sync_adapter.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_sync_adapter.h>

namespace yams::daemon {

namespace {

/// A document whose blob cannot be replicated because its stored content is
/// unrecoverable (corrupt object manifest, digest mismatch, malformed stored
/// bytes) is skipped permanently: retrying the same bytes cannot succeed. Any
/// other failure (backend unreachable, resource limits, cancellation, identity
/// misconfiguration) propagates so the document is retried on a later cycle.
bool isUnrecoverableBlobFailure(yams::ErrorCode code) {
    return code == yams::ErrorCode::ManifestInvalid || code == yams::ErrorCode::HashMismatch ||
           code == yams::ErrorCode::InvalidData;
}

} // namespace

using yams::Error;
using yams::ErrorCode;
using yams::Result;

Result<void> ServiceManager::publishMemorySync(const std::string& key, const std::string& value) {
    if (!memorySync_) {
        return Error{ErrorCode::InvalidState, "memory sync service is not enabled"};
    }
    auto namespacedKey = memory_sync::userLogicalKey(key);
    if (!namespacedKey) {
        return namespacedKey.error();
    }
    std::vector<std::byte> bytes;
    bytes.reserve(value.size());
    for (const unsigned char byte : value) {
        bytes.push_back(static_cast<std::byte>(byte));
    }
    return memorySync_->publish(namespacedKey.value(), bytes);
}

Result<void> ServiceManager::deleteMemorySync(const std::string& key) {
    if (!memorySync_) {
        return Error{ErrorCode::InvalidState, "memory sync service is not enabled"};
    }
    auto namespacedKey = memory_sync::userLogicalKey(key);
    if (!namespacedKey) {
        return namespacedKey.error();
    }
    return memorySync_->erase(namespacedKey.value(), key);
}

Result<std::string> ServiceManager::readMemorySyncCached(const std::string& key) const {
    if (!memorySync_) {
        return Error{ErrorCode::InvalidState, "memory sync service is not enabled"};
    }
    auto namespacedKey = memory_sync::userLogicalKey(key);
    if (!namespacedKey) {
        return namespacedKey.error();
    }
    auto value = memorySync_->readCached(namespacedKey.value());
    if (!value) {
        return value.error();
    }
    const auto& bytes = value.value();
    return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

Result<ServiceManager::MemorySyncStatus> ServiceManager::getMemorySyncStatus() const {
    if (!memorySync_) {
        return Error{ErrorCode::InvalidState, "memory sync service is not enabled"};
    }
    std::uint64_t peerCount = 0;
    if (p2pManager_) {
        auto peers = p2pManager_->peers();
        if (peers) {
            peerCount = peers.value().size();
        } else {
            // A transient peer-registry read failure must not fail the whole status IPC; degrade
            // to peerCount=0 so the daemon stays observable while the registry recovers.
            spdlog::warn("[ServiceManager] p2p peer registry read failed for status: {}",
                         peers.error().message);
        }
    }
    return MemorySyncStatus{
        memorySync_->started(),
        memorySync_->mergedRecordCount(),
        memorySync_->quarantinedRecordCount(),
        memorySync_->authFailureCount(),
        memorySync_->successfulSyncCycles(),
        memorySync_->failedSyncCycles(),
        memorySync_->lastSuccessfulSyncAgeMs(),
        config_.memorySync.transport == "direct" ? "direct" : config_.memorySync.backend,
        config_.memorySync.nodeId,
        config_.memorySync.corpusId,
        config_.memorySync.corpusEpoch,
        config_.memorySync.mode,
        config_.memorySync.transport == "direct"
            ? (config_.memorySync.allowFirstContact ? "mutual-tls-legacy-tofu"
                                                    : "mutual-tls-operator-pinned")
            : (config_.memorySync.writerAuthRequired ? "authenticated-writers" : "backend-acl"),
        peerCount};
}

Result<void> ServiceManager::stageMemorySyncDocumentDelete(std::string_view contentHash,
                                                           bool retainContent) {
    if (!memorySync_) {
        return {};
    }
    if (!memory_sync::isSha256Digest(contentHash)) {
        return Error{ErrorCode::InvalidArgument,
                     "metadata deletion requires a SHA-256 content hash"};
    }
    auto repository = getMetadataRepo();
    if (!repository) {
        return Error{ErrorCode::InvalidState,
                     "metadata repository is unavailable for replicated deletion"};
    }
    auto document = repository->getDocumentByHash(std::string(contentHash));
    if (!document) {
        return document.error();
    }
    if (retainContent && !document.value()) {
        return Error{ErrorCode::InvalidArgument, "retaining content requires an existing document"};
    }
    const std::string documentKey =
        std::string(memory_sync::memoryStoreName(memory_sync::MemoryStore::Document)) + "/" +
        std::string(contentHash);
    const std::string blobKey =
        std::string(memory_sync::memoryStoreName(memory_sync::MemoryStore::ContentBlob)) + "/" +
        std::string(contentHash);
    auto probe = memory_sync::EraseReadinessProbe::MetadataAbsent;
    if (!document.value()) {
        auto contentStore = getContentStore();
        if (!contentStore) {
            return Error{ErrorCode::InvalidState,
                         "content store is unavailable for replicated deletion"};
        }
        auto exists = contentStore->exists(std::string(contentHash));
        if (!exists) {
            return exists.error();
        }
        if (!exists.value()) {
            return Error{ErrorCode::NotFound,
                         "cannot stage replicated deletion for absent content"};
        }
        probe = memory_sync::EraseReadinessProbe::ContentAbsent;
    }
    if (retainContent) {
        const std::array requests{
            memory_sync::EraseStageRequest{documentKey, std::string(contentHash),
                                           memory_sync::EraseReadinessProbe::MetadataAbsent}};
        return memorySync_->stageErases(requests);
    }

    // Content bytes are independently addressable by hash, so standard deletion requires two
    // durable intents. Reserve both bounded outbox slots before exposing either to the drainer.
    const std::array requests{
        memory_sync::EraseStageRequest{blobKey, std::string(contentHash),
                                       memory_sync::EraseReadinessProbe::ContentAbsent},
        memory_sync::EraseStageRequest{documentKey, std::string(contentHash), probe}};
    return memorySync_->stageErases(requests);
}

Result<void> ServiceManager::publishMemorySyncDocumentDelete(std::string_view contentHash,
                                                             bool retainContent) {
    if (!memorySync_) {
        return {};
    }
    std::lock_guard<std::mutex> deleteLock(memorySyncDeleteOutboxMutex_);
    notifyMemorySyncDeleteOutboxStage("delete_publish_locked");
    const std::string documentKey =
        std::string(memory_sync::memoryStoreName(memory_sync::MemoryStore::Document)) + "/" +
        std::string(contentHash);
    const std::string blobKey =
        std::string(memory_sync::memoryStoreName(memory_sync::MemoryStore::ContentBlob)) + "/" +
        std::string(contentHash);
    auto intents = memorySync_->pendingErases();
    if (!intents) {
        return intents.error();
    }

    bool found = false;
    const std::vector<std::string> keys = retainContent
                                              ? std::vector<std::string>{documentKey}
                                              : std::vector<std::string>{blobKey, documentKey};
    for (const auto& key : keys) {
        const auto intent = std::ranges::find_if(
            intents.value(), [&](const auto& candidate) { return candidate.logicalKey == key; });
        if (intent == intents.value().end()) {
            continue;
        }
        found = true;
        const auto probe = intent->readinessProbe;
        if (auto published = memorySync_->publishStagedErase(
                key, [this, contentHash = std::string(contentHash),
                      probe] { return memorySyncDeleteLocallyAbsent(contentHash, probe); });
            !published) {
            if (published.error().code == ErrorCode::NotFound) {
                auto refreshed = memorySync_->pendingErases();
                if (!refreshed) {
                    return refreshed.error();
                }
                const bool stillPending =
                    std::ranges::any_of(refreshed.value(), [&](const auto& candidate) {
                        return candidate.logicalKey == key;
                    });
                if (!stillPending && memorySync_->hasCommittedTombstone(key)) {
                    continue;
                }
            }
            return published.error();
        }
    }
    if (!found) {
        if (memorySync_->hasCommittedTombstone(documentKey) &&
            (retainContent || memorySync_->hasCommittedTombstone(blobKey))) {
            return {};
        }
        return Error{ErrorCode::NotFound, "replicated deletion was not durably staged"};
    }
    return {};
}

Result<std::size_t> ServiceManager::applyMemorySyncContentBlobs() {
    if (!memorySync_) {
        return Error{ErrorCode::InvalidState, "memory sync service is not enabled"};
    }
    auto contentStore = getContentStore();
    if (!contentStore) {
        return Error{ErrorCode::InvalidState, "content store is not available"};
    }

    auto merged = memorySync_->syncOnce();
    if (!merged) {
        return merged.error();
    }

    memory_sync::ContentBlobSyncAdapter adapter{*contentStore, *memorySync_};
    const std::string prefix =
        std::string(memory_sync::memoryStoreName(memory_sync::MemoryStore::ContentBlob)) + "/";
    std::size_t applied = 0;
    for (const auto& [key, envelope] : merged.value()) {
        (void)envelope;
        if (!key.starts_with(prefix)) {
            continue;
        }
        const std::string_view hash{key.data() + prefix.size(), key.size() - prefix.size()};
        if (!memory_sync::isSha256Digest(hash)) {
            return Error{ErrorCode::InvalidData, "invalid content-blob memory-sync key"};
        }
        if (envelope.isTombstone()) {
            auto removed = adapter.applyDelete(hash);
            if (!removed) {
                return removed.error();
            }
            applied += removed.value() ? 1 : 0;
            continue;
        }
        auto exists = contentStore->exists(std::string(hash));
        if (!exists) {
            return exists.error();
        }
        if (exists.value()) {
            continue;
        }
        if (auto stored = adapter.applyCached(hash); !stored) {
            return stored.error();
        }
        ++applied;
    }
    return applied;
}

void ServiceManager::notifyMemorySyncStage(std::string_view stage) noexcept {
    std::function<void(std::string_view)> observer;
    {
        std::lock_guard<std::mutex> lock(memorySyncStageObserverMutex_);
        observer = memorySyncStageObserver_;
    }
    if (!observer) {
        return;
    }
    try {
        observer(stage);
    } catch (...) {
        spdlog::warn("[ServiceManager] memory_sync stage observer threw at {}", stage);
    }
}

void ServiceManager::notifyMemorySyncDeleteOutboxStage(std::string_view stage) noexcept {
    std::function<void(std::string_view)> observer;
    {
        std::lock_guard<std::mutex> lock(memorySyncDeleteOutboxObserverMutex_);
        observer = memorySyncDeleteOutboxObserver_;
    }
    if (!observer) {
        return;
    }
    try {
        observer(stage);
    } catch (...) {
        spdlog::warn("[ServiceManager] memory_sync delete outbox observer threw at {}", stage);
    }
}

Result<bool>
ServiceManager::memorySyncDeleteLocallyAbsent(std::string_view contentHash,
                                              memory_sync::EraseReadinessProbe probe) const {
    if (probe == memory_sync::EraseReadinessProbe::MetadataAbsent) {
        auto repository = getMetadataRepo();
        if (!repository) {
            return Error{ErrorCode::InvalidState, "metadata repository is unavailable"};
        }
        auto document = repository->getDocumentByHash(std::string(contentHash));
        if (!document) {
            return document.error();
        }
        return !document.value().has_value();
    }
    if (probe == memory_sync::EraseReadinessProbe::ContentAbsent) {
        auto contentStore = getContentStore();
        if (!contentStore) {
            return Error{ErrorCode::InvalidState, "content store is unavailable"};
        }
        auto exists = contentStore->exists(std::string(contentHash));
        if (!exists) {
            return exists.error();
        }
        return !exists.value();
    }
    return Error{ErrorCode::InvalidArgument,
                 "document delete outbox requires a typed absence probe"};
}

bool ServiceManager::drainMemorySyncDocumentDeleteOutbox() noexcept {
    try {
        notifyMemorySyncDeleteOutboxStage("delete_drain_waiting");
        std::lock_guard<std::mutex> deleteLock(memorySyncDeleteOutboxMutex_);
        auto intents = memorySync_->pendingErases();
        if (!intents) {
            spdlog::warn("[ServiceManager] memory_sync delete outbox scan failed: {}",
                         intents.error().message);
            return true;
        }
        const std::string documentPrefix =
            std::string(memory_sync::memoryStoreName(memory_sync::MemoryStore::Document)) + "/";
        const std::string blobPrefix =
            std::string(memory_sync::memoryStoreName(memory_sync::MemoryStore::ContentBlob)) + "/";
        auto pending = std::move(intents.value());
        std::ranges::sort(pending, [](const auto& lhs, const auto& rhs) {
            const auto lhsCounter =
                lhs.prepared ? lhs.preparedCounter : std::numeric_limits<std::uint64_t>::max();
            const auto rhsCounter =
                rhs.prepared ? rhs.preparedCounter : std::numeric_limits<std::uint64_t>::max();
            return std::tuple{lhsCounter, lhs.logicalKey} < std::tuple{rhsCounter, rhs.logicalKey};
        });
        const auto validDocumentIntent = [&](const auto& intent) {
            return intent.logicalKey == documentPrefix + intent.tombstonePayload;
        };
        const auto validBlobIntent = [&](const auto& intent) {
            return intent.logicalKey == blobPrefix + intent.tombstonePayload;
        };

        bool eligible = false;
        for (const auto& intent : pending) {
            if (!memory_sync::isSha256Digest(intent.tombstonePayload) ||
                (!validDocumentIntent(intent) && !validBlobIntent(intent))) {
                continue;
            }
            auto absent =
                memorySyncDeleteLocallyAbsent(intent.tombstonePayload, intent.readinessProbe);
            if (!absent) {
                spdlog::warn("[ServiceManager] memory_sync delete outbox probe failed: {}",
                             absent.error().message);
                return true;
            }
            if (!absent.value()) {
                if (intent.ready) {
                    eligible = true;
                    if (auto cancelled = memorySync_->cancelStagedErase(intent.logicalKey);
                        !cancelled) {
                        spdlog::warn("[ServiceManager] stale delete outbox cancellation failed: {}",
                                     cancelled.error().message);
                    }
                }
                continue;
            }
            eligible = true;
            if (auto published = memorySync_->publishStagedErase(
                    intent.logicalKey,
                    [this, contentHash = intent.tombstonePayload, probe = intent.readinessProbe] {
                        return memorySyncDeleteLocallyAbsent(contentHash, probe);
                    });
                !published) {
                spdlog::warn("[ServiceManager] memory_sync delete outbox retry failed: {}",
                             published.error().message);
            }
        }
        return eligible;
    } catch (const std::exception& error) {
        spdlog::warn("[ServiceManager] memory_sync delete outbox threw: {}", error.what());
        return true;
    } catch (...) {
        spdlog::warn("[ServiceManager] memory_sync delete outbox threw (unknown)");
        return true;
    }
}

void ServiceManager::applyMemorySyncWinners() noexcept {
    memorySyncApplyAttempts_.fetch_add(1, std::memory_order_acq_rel);
    std::lock_guard<std::mutex> applyLock(memorySyncApplyMutex_);
    if (!memorySync_ || memorySync_->stopRequested()) {
        return;
    }
    // A pre-delete intent can survive a crash between local deletion and tombstone publication.
    // Promote it only after local metadata is absent, then skip this stale callback snapshot.
    if (drainMemorySyncDocumentDeleteOutbox()) {
        return;
    }
    try {
        // Log quarantine reasons after each cycle (the sync lock is free here, so this never
        // blocks the IPC status path).
        for (const auto& [key, reason] : memorySync_->quarantinedReasons()) {
            spdlog::debug("[memory_sync] quarantined {}: {}", key, reason);
        }
    } catch (...) {
        spdlog::debug("[memory_sync] quarantine reason logging skipped");
    }
    try {
        if (auto result = applyMemorySyncContentBlobs(); !result) {
            spdlog::warn("[ServiceManager] memory_sync content apply failed: {}",
                         result.error().message);
            return;
        }
    } catch (const std::exception& e) {
        spdlog::warn("[ServiceManager] memory_sync content apply threw: {}", e.what());
        return;
    } catch (...) {
        spdlog::warn("[ServiceManager] memory_sync content apply threw (unknown)");
        return;
    }
    notifyMemorySyncStage("apply.after_content");

    if (memorySync_->stopRequested()) {
        return;
    }
    try {
        if (auto repository = getMetadataRepo()) {
            auto contentStore = getContentStore();
            if (!contentStore) {
                spdlog::warn("[ServiceManager] memory_sync metadata apply requires content store");
                return;
            }
            metadata::MetadataSyncAdapter adapter{
                *repository, *memorySync_,
                [contentStore](std::string_view hash) {
                    return contentStore->exists(std::string(hash));
                },
                [this](const metadata::DocumentInfo& document) {
                    enqueuePostIngest(document.sha256Hash, document.mimeType);
                }};
            if (auto result = adapter.apply(); !result) {
                spdlog::warn("[ServiceManager] memory_sync metadata apply failed: {}",
                             result.error().message);
                return;
            }
        }
    } catch (const std::exception& e) {
        spdlog::warn("[ServiceManager] memory_sync metadata apply threw: {}", e.what());
        return;
    } catch (...) {
        spdlog::warn("[ServiceManager] memory_sync metadata apply threw (unknown)");
        return;
    }
    notifyMemorySyncStage("apply.after_metadata");

    if (memorySync_->stopRequested()) {
        return;
    }
    try {
        if (auto vectorDatabase = getVectorDatabase()) {
            auto contentStore = getContentStore();
            if (!contentStore) {
                spdlog::warn("[ServiceManager] memory_sync vector apply requires content store");
                return;
            }
            vector::VectorSyncAdapter::RebuildCallback rebuild;
            if (vectorIndexCoordinator_) {
                rebuild = [coordinator = vectorIndexCoordinator_]() {
                    return coordinator->requestRebuildBlocking(RebuildReason::EmbeddingBatch);
                };
            }
            vector::VectorSyncAdapter adapter{*vectorDatabase, *memorySync_, std::move(rebuild),
                                              &memorySyncVectorRebuildDirty_,
                                              [contentStore](std::string_view hash) {
                                                  return contentStore->exists(std::string(hash));
                                              }};
            if (auto result = adapter.apply(); !result) {
                spdlog::warn("[ServiceManager] memory_sync vector apply failed: {}",
                             result.error().message);
                return;
            }
        }
    } catch (const std::exception& e) {
        spdlog::warn("[ServiceManager] memory_sync vector apply threw: {}", e.what());
        return;
    } catch (...) {
        spdlog::warn("[ServiceManager] memory_sync vector apply threw (unknown)");
        return;
    }
    notifyMemorySyncStage("apply.after_vector");

    if (memorySync_->stopRequested()) {
        return;
    }
    try {
        if (auto kgStore = getKgStore()) {
            metadata::TopologySyncAdapter adapter{*kgStore, *memorySync_};
            if (auto result = adapter.apply(); !result) {
                spdlog::warn("[ServiceManager] memory_sync topology apply failed: {}",
                             result.error().message);
                return;
            }
        }
    } catch (const std::exception& e) {
        spdlog::warn("[ServiceManager] memory_sync topology apply threw: {}", e.what());
        return;
    } catch (...) {
        spdlog::warn("[ServiceManager] memory_sync topology apply threw (unknown)");
        return;
    }
    notifyMemorySyncStage("apply.after_topology");

    if (memorySync_->stopRequested()) {
        return;
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= nextMemorySyncBackfill_) {
        nextMemorySyncBackfill_ = now + std::chrono::seconds{5};
        publishMemorySyncBackfill();
    }
}

void ServiceManager::publishMemorySyncBackfill() noexcept try {
    memorySyncBackfillAttempts_.fetch_add(1, std::memory_order_acq_rel);
    std::lock_guard<std::mutex> backfillLock(memorySyncBackfillMutex_);
    if (!memorySync_) {
        return;
    }

    auto repository = getMetadataRepo();
    auto contentStore = getContentStore();
    auto vectorDatabase = getVectorDatabase();
    auto kgStore = getKgStore();
    auto& state = memorySyncBackfillState_;
    std::size_t remainingItems = state.itemBudgetPerCycle;
    const auto deadline = std::chrono::steady_clock::now() + state.timeBudgetPerCycle;
    const auto shouldStop = [&] {
        return !memorySync_ || memorySync_->stopRequested() || remainingItems == 0 ||
               std::chrono::steady_clock::now() >= deadline;
    };

    std::size_t documentsPublished = 0;
    std::size_t blobsPublished = 0;
    std::size_t vectorsPublished = 0;
    std::size_t nodesPublished = 0;
    std::size_t edgesPublished = 0;
    std::size_t skippedDocuments = 0;

    const auto publishDocument = [&]() -> bool {
        if (!repository || !contentStore) {
            return false;
        }
        metadata::DocumentQueryOptions options;
        options.idGreaterThan = state.documentIdCursor;
        options.limit = 1;
        options.orderByIdAsc = true;
        auto documents = repository->queryDocuments(options);
        if (!documents) {
            throw std::runtime_error(documents.error().message);
        }
        if (documents.value().empty()) {
            return false;
        }

        const auto& document = documents.value().front();
        auto metadataValues = repository->getAllMetadata(document.id);
        if (!metadataValues) {
            throw std::runtime_error(metadataValues.error().message);
        }
        std::vector<std::pair<std::string, metadata::MetadataValue>> tags;
        tags.reserve(metadataValues.value().size());
        for (const auto& [key, value] : metadataValues.value()) {
            tags.emplace_back(key, value);
        }

        memory_sync::ContentBlobSyncAdapter contentAdapter{*contentStore, *memorySync_};
        if (!document.sha256Hash.empty()) {
            auto published = contentAdapter.publishExisting(document.sha256Hash);
            if (!published) {
                // A poisoned blob (corrupt manifest, digest mismatch, malformed stored bytes)
                // can never publish; skip it so the backfill continues with the next document
                // instead of wedging this domain on every cycle. Transient failures propagate
                // so the document is retried later.
                if (isUnrecoverableBlobFailure(published.error().code)) {
                    spdlog::warn("[ServiceManager] memory_sync backfill skipping document {} "
                                 "(blob {} unrecoverable): {}",
                                 document.id, document.sha256Hash, published.error().message);
                    state.documentIdCursor = std::max(state.documentIdCursor, document.id);
                    ++skippedDocuments;
                    notifyMemorySyncStage("backfill.skip_document");
                    return true;
                }
                throw std::runtime_error(published.error().message);
            }
            blobsPublished += published.value() ? 1U : 0U;
        }
        metadata::MetadataSyncAdapter metadataAdapter{*repository, *memorySync_};
        auto published = metadataAdapter.publish(document, tags);
        if (!published) {
            if (isUnrecoverableBlobFailure(published.error().code)) {
                spdlog::warn("[ServiceManager] memory_sync backfill skipping document {} "
                             "(record unrecoverable): {}",
                             document.id, published.error().message);
                state.documentIdCursor = std::max(state.documentIdCursor, document.id);
                ++skippedDocuments;
                notifyMemorySyncStage("backfill.skip_document");
                return true;
            }
            throw std::runtime_error(published.error().message);
        }
        state.documentIdCursor = std::max(state.documentIdCursor, document.id);
        ++documentsPublished;
        notifyMemorySyncStage("backfill.after_document");
        return true;
    };

    const auto publishVector = [&]() -> bool {
        if (!vectorDatabase) {
            return false;
        }
        auto records = vectorDatabase->getVectorsPage(state.vectorDocumentHashCursor,
                                                      state.vectorChunkIdCursor, 1);
        if (!records) {
            throw std::runtime_error(records.error().message);
        }
        if (records.value().empty()) {
            return false;
        }
        const auto& record = records.value().front();
        vector::VectorSyncAdapter adapter{*vectorDatabase, *memorySync_};
        auto published = adapter.publish(record);
        if (!published) {
            throw std::runtime_error(published.error().message);
        }
        state.vectorDocumentHashCursor = record.document_hash;
        state.vectorChunkIdCursor = record.chunk_id;
        ++vectorsPublished;
        notifyMemorySyncStage("backfill.after_vector");
        return true;
    };

    const auto publishTopology = [&]() -> bool {
        if (!kgStore) {
            return false;
        }
        metadata::TopologySyncAdapter adapter{*kgStore, *memorySync_};
        if (!state.topologySnapshotInitialized) {
            auto nodeTypes = kgStore->getNodeTypeCounts();
            if (!nodeTypes) {
                throw std::runtime_error(nodeTypes.error().message);
            }
            state.topologyNodeTypes.reserve(nodeTypes.value().size());
            for (const auto& [nodeType, count] : nodeTypes.value()) {
                (void)count;
                state.topologyNodeTypes.push_back(nodeType);
            }
            std::sort(state.topologyNodeTypes.begin(), state.topologyNodeTypes.end());
            state.topologySnapshotInitialized = true;
        }

        while (!shouldStop()) {
            if (state.topologyNodeActive) {
                auto edges = kgStore->getEdgesFrom(state.topologyNodeId, std::nullopt, 1,
                                                   state.topologyEdgeOffset);
                if (!edges) {
                    throw std::runtime_error(edges.error().message);
                }
                if (!edges.value().empty()) {
                    const auto& edge = edges.value().front();
                    auto target = kgStore->getNodeById(edge.dstNodeId);
                    if (!target) {
                        throw std::runtime_error(target.error().message);
                    }
                    ++state.topologyEdgeOffset;
                    if (!target.value()) {
                        continue;
                    }
                    auto published =
                        adapter.publishEdge(state.topologyNodeKey, edge, target.value()->nodeKey);
                    if (!published) {
                        --state.topologyEdgeOffset;
                        throw std::runtime_error(published.error().message);
                    }
                    ++edgesPublished;
                    notifyMemorySyncStage("backfill.after_edge");
                    return true;
                }
                state.topologyNodeActive = false;
                state.topologyNodeId = 0;
                state.topologyNodeKey.clear();
                state.topologyEdgeOffset = 0;
                ++state.topologyNodeOffsets[state.topologyNodeTypes[state.topologyTypeIndex]];
            }

            if (state.topologyTypeIndex >= state.topologyNodeTypes.size()) {
                // Refresh the bounded type snapshot on the next cycle so node types created after
                // startup become visible. Per-type offsets prevent replaying nodes already swept.
                state.topologySnapshotInitialized = false;
                state.topologyNodeTypes.clear();
                state.topologyTypeIndex = 0;
                return false;
            }
            const auto& nodeType = state.topologyNodeTypes[state.topologyTypeIndex];
            auto nodes = kgStore->findNodesByType(nodeType, 1, state.topologyNodeOffsets[nodeType]);
            if (!nodes) {
                throw std::runtime_error(nodes.error().message);
            }
            if (nodes.value().empty()) {
                ++state.topologyTypeIndex;
                continue;
            }

            const auto& node = nodes.value().front();
            auto published = adapter.publishNode(node);
            if (!published) {
                throw std::runtime_error(published.error().message);
            }
            state.topologyNodeId = node.id;
            state.topologyNodeKey = node.nodeKey;
            state.topologyNodeActive = true;
            ++nodesPublished;
            notifyMemorySyncStage("backfill.after_node");
            return true;
        }
        return false;
    };

    const auto advanceDomain = [&] {
        using Domain = MemorySyncBackfillState::Domain;
        switch (state.nextDomain) {
            case Domain::Documents:
                state.nextDomain = Domain::Vectors;
                break;
            case Domain::Vectors:
                state.nextDomain = Domain::Topology;
                break;
            case Domain::Topology:
                state.nextDomain = Domain::Documents;
                break;
        }
    };

    std::size_t consecutiveEmptyDomains = 0;
    while (!shouldStop() && consecutiveEmptyDomains < 3) {
        const auto domain = state.nextDomain;
        advanceDomain();
        bool published = false;
        try {
            using Domain = MemorySyncBackfillState::Domain;
            switch (domain) {
                case Domain::Documents:
                    published = publishDocument();
                    break;
                case Domain::Vectors:
                    published = publishVector();
                    break;
                case Domain::Topology:
                    published = publishTopology();
                    break;
            }
        } catch (const std::exception& error) {
            spdlog::warn("[ServiceManager] memory_sync backfill domain failed: {}", error.what());
        } catch (...) {
            spdlog::warn("[ServiceManager] memory_sync backfill domain failed (unknown)");
        }

        if (published) {
            --remainingItems;
            consecutiveEmptyDomains = 0;
        } else {
            ++consecutiveEmptyDomains;
        }
    }

    spdlog::debug(
        "[ServiceManager] memory_sync backfill scanned documents={} blobs={} vectors={} nodes={} "
        "edges={} skipped={}",
        documentsPublished, blobsPublished, vectorsPublished, nodesPublished, edgesPublished,
        skippedDocuments);
} catch (const std::exception& error) {
    spdlog::warn("[ServiceManager] memory_sync backfill setup failed: {}", error.what());
} catch (...) {
    spdlog::warn("[ServiceManager] memory_sync backfill setup failed (unknown)");
}

Result<void> ServiceManager::configureMemorySyncApply() {
    if (!memorySync_) {
        return {};
    }
    memorySync_->setAfterSyncCallback([this] { applyMemorySyncWinners(); });
    // Recover or revalidate durable local delete intents before the worker and direct-P2P
    // listener can expose a checkpoint whose exact tombstone index is still incomplete.
    (void)drainMemorySyncDocumentDeleteOutbox();
    if (auto started = memorySync_->start(); !started) {
        return Error{started.error().code,
                     "memory_sync service failed to start: " + started.error().message};
    }
    spdlog::info("[ServiceManager] memory_sync apply path enabled and worker started");
    return {};
}

} // namespace yams::daemon
