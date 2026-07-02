/// @file kg_write_buffer.cpp
/// @brief Implementation of the KGWriteBuffer in-memory edge/entity buffer.

#include <yams/metadata/kg_write_buffer.h>

#include <algorithm>
#include <string>

namespace yams::metadata {

KGWriteBuffer::KGWriteBuffer(KnowledgeGraphStore& kgStore, KGWriteBufferConfig config)
    : kgStore_(kgStore), config_(std::move(config)) {
    // Reserve reasonable capacity to avoid early rehashes.
    if (config_.maxEdges > 0) {
        edgeBuffer_.reserve(std::min(config_.maxEdges, std::size_t{5000}));
    }
    if (config_.maxEntities > 0) {
        entityBuffer_.reserve(std::min(config_.maxEntities, std::size_t{5000}));
    }
}

KGWriteBuffer::~KGWriteBuffer() {
    // Best-effort flush on destruction — do not throw from destructor.
    static_cast<void>(flush());
}

Result<void> KGWriteBuffer::addEdge(KGEdge edge) {
    if (!config_.enabled) {
        ++totalEdgesAdded_;
        return kgStore_.addEdgesUnique({std::move(edge)});
    }

    EdgeKey key{edge.srcNodeId, edge.dstNodeId, edge.relation};
    auto it = edgeBuffer_.find(key);
    if (it != edgeBuffer_.end()) {
        mergeEdge(it->second, edge);
    } else {
        edgeBuffer_.emplace(std::move(key), std::move(edge));
    }
    ++totalEdgesAdded_;

    if (config_.autoFlush && config_.maxEdges > 0 && edgeBuffer_.size() >= config_.maxEdges) {
        return flush();
    }
    return Result<void>();
}

Result<void> KGWriteBuffer::addEdges(const std::vector<KGEdge>& edges) {
    for (const auto& e : edges) {
        auto r = addEdge(e);
        if (!r)
            return r;
    }
    return Result<void>();
}

Result<void> KGWriteBuffer::addDocEntity(DocEntity entity) {
    if (!config_.enabled) {
        return kgStore_.addDocEntities({std::move(entity)});
    }

    entityBuffer_.push_back(std::move(entity));

    if (config_.autoFlush && config_.maxEntities > 0 &&
        entityBuffer_.size() >= config_.maxEntities) {
        return flush();
    }
    return Result<void>();
}

Result<void> KGWriteBuffer::addDocEntities(const std::vector<DocEntity>& entities) {
    for (const auto& e : entities) {
        auto r = addDocEntity(e);
        if (!r)
            return r;
    }
    return Result<void>();
}

Result<void> KGWriteBuffer::flush() {
    if (!config_.enabled) {
        return Result<void>();
    }

    if (edgeBuffer_.empty() && entityBuffer_.empty()) {
        return Result<void>();
    }

    // Open a single WriteBatch for both edges and entities.
    auto batchResult = kgStore_.beginWriteBatch();
    if (!batchResult) {
        return batchResult.error();
    }
    auto& wb = *batchResult.value();

    auto edgesSnapshot = edgeBuffer_;
    auto entitiesSnapshot = entityBuffer_;
    const auto flushedBefore = totalEdgesFlushed_;

    auto staged = flushInto(wb);
    if (!staged) {
        return staged;
    }

    auto commitResult = wb.commit();
    if (!commitResult) {
        edgeBuffer_ = std::move(edgesSnapshot);
        entityBuffer_ = std::move(entitiesSnapshot);
        totalEdgesFlushed_ = flushedBefore;
        return commitResult;
    }
    return Result<void>();
}

Result<void> KGWriteBuffer::flushInto(KnowledgeGraphStore::WriteBatch& wb) {
    const bool hasEdges = !edgeBuffer_.empty();
    const bool hasEntities = !entityBuffer_.empty();

    if (!hasEdges && !hasEntities) {
        return Result<void>();
    }

    std::unordered_map<EdgeKey, KGEdge> stagedEdges;
    std::vector<DocEntity> stagedEntities;
    if (hasEdges) {
        stagedEdges.swap(edgeBuffer_);
    }
    if (hasEntities) {
        stagedEntities.swap(entityBuffer_);
    }

    const auto restoreBuffers = [&]() {
        if (hasEdges) {
            edgeBuffer_.swap(stagedEdges);
        }
        if (hasEntities) {
            entityBuffer_.swap(stagedEntities);
        }
    };

    std::size_t flushedEdgeCount = 0;

    // Flush edges: convert deduplicated map to vector and use addEdgesUnique
    // (no per-row dedup, since we already deduped in memory).
    if (hasEdges) {
        std::vector<KGEdge> deduped;
        deduped.reserve(stagedEdges.size());
        for (const auto& [key, edge] : stagedEdges) {
            deduped.push_back(edge);
        }
        auto r = wb.addEdgesUnique(deduped);
        if (!r) {
            restoreBuffers();
            return r;
        }
        flushedEdgeCount = deduped.size();
    }

    // Flush entities.
    if (hasEntities) {
        auto r = wb.addDocEntities(stagedEntities);
        if (!r) {
            restoreBuffers();
            return r;
        }
    }

    totalEdgesFlushed_ += flushedEdgeCount;
    return Result<void>();
}

std::size_t KGWriteBuffer::edgeCount() const noexcept {
    return edgeBuffer_.size();
}

std::size_t KGWriteBuffer::entityCount() const noexcept {
    return entityBuffer_.size();
}

std::size_t KGWriteBuffer::totalEdgesAdded() const noexcept {
    return totalEdgesAdded_;
}

std::size_t KGWriteBuffer::totalEdgesFlushed() const noexcept {
    return totalEdgesFlushed_;
}

void KGWriteBuffer::incrementDocCount() {
    ++docCount_;
    if (config_.autoFlush && config_.maxDocs > 0 && docCount_ >= config_.maxDocs) {
        static_cast<void>(flush());
        docCount_ = 0;
    }
}

std::size_t KGWriteBuffer::docCount() const noexcept {
    return docCount_;
}

void KGWriteBuffer::resetDocCount() {
    docCount_ = 0;
}

void KGWriteBuffer::resetCounters() {
    totalEdgesAdded_ = 0;
    totalEdgesFlushed_ = 0;
}

void KGWriteBuffer::mergeEdge(KGEdge& existing, const KGEdge& incoming) {
    // Per addEdgesUnique ON CONFLICT semantics: keep max weight.
    existing.weight = std::max(existing.weight, incoming.weight);

    // Coalesce created_time: prefer incoming if set.
    if (incoming.createdTime.has_value()) {
        existing.createdTime = incoming.createdTime;
    }

    // Coalesce properties: prefer incoming if its weight >= existing weight.
    if (incoming.weight >= existing.weight && incoming.properties.has_value()) {
        existing.properties = incoming.properties;
    }
}

void KGWriteBuffer::mergeDocEntity(DocEntity& existing, const DocEntity& incoming) {
    if (incoming.confidence.has_value() &&
        (!existing.confidence.has_value() ||
         incoming.confidence.value() > existing.confidence.value())) {
        existing.confidence = incoming.confidence;
    }
}

} // namespace yams::metadata
