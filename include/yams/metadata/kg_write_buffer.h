#pragma once

/// @file kg_write_buffer.h
/// @brief In-memory write buffer for KnowledgeGraphStore edge/entity inserts.
///
/// Accumulates edges and entities in memory, coalesces duplicates by
/// (src_node_id, dst_node_id, relation) key, and flushes in batch via
/// addEdges() (no per-row dedup, since dedup already happened in memory).
///
/// Inspired by the LSM-tree level-0 buffer pattern (sLSM, arXiv:1809.03261):
/// accumulate writes in a fast memory structure, then merge into the
/// persistent store in one batch. This avoids the one-at-a-time
/// addEdgesUnique() overhead when edges are inserted per-document during
/// ingest (e.g., indexing_service.cpp).
///
/// Thread-safety: NOT thread-safe. Designed for single-threaded ingest
/// use. The caller is responsible for external synchronization.
///
/// Usage:
///   KGWriteBuffer buffer(kgStore);
///   buffer.addEdge(e1);
///   buffer.addEdge(e2);  // coalesces with e1 if same key
///   buffer.flush();      // writes deduplicated edges via addEdges()

#include <yams/core/types.h>
#include <yams/metadata/knowledge_graph_store.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::metadata {

/// Configuration for KGWriteBuffer.
struct KGWriteBufferConfig {
    /// Maximum number of documents between auto-flushes (0 = disabled).
    /// When documentCount >= maxDocs, the next addEdge/addEntity triggers a flush.
    std::size_t maxDocs = 1000;

    /// Maximum number of buffered edges before auto-flush (0 = disabled).
    std::size_t maxEdges = 5000;

    /// Maximum number of buffered entities before auto-flush (0 = disabled).
    std::size_t maxEntities = 5000;

    /// Enable buffer (when false, all calls pass through directly).
    bool enabled = true;

    /// When true, auto-flush on threshold; when false, caller must flush().
    bool autoFlush = true;
};

/// Edge key for deduplication: (srcNodeId, dstNodeId, relation).
struct EdgeKey {
    std::int64_t srcNodeId;
    std::int64_t dstNodeId;
    std::string relation;

    bool operator==(const EdgeKey& other) const noexcept {
        return srcNodeId == other.srcNodeId && dstNodeId == other.dstNodeId &&
               relation == other.relation;
    }
};

} // namespace yams::metadata

// Hash specialization for EdgeKey.
template <> struct std::hash<yams::metadata::EdgeKey> {
    std::size_t operator()(const yams::metadata::EdgeKey& k) const noexcept {
        std::size_t h = std::hash<std::int64_t>{}(k.srcNodeId);
        h ^= std::hash<std::int64_t>{}(k.dstNodeId) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<std::string>{}(k.relation) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

namespace yams::metadata {

/// In-memory write buffer for KnowledgeGraphStore edge/entity inserts.
///
/// Accumulates edges and entities and coalesces duplicates by edge key
/// (src, dst, relation). On flush, writes the deduplicated set via
/// addEdges() (no per-row dedup) inside a WriteBatch.
class KGWriteBuffer {
public:
    /// Construct a buffer wrapping a KnowledgeGraphStore.
    /// @param kgStore The store to flush into. Must outlive the buffer.
    /// @param config Buffer configuration.
    explicit KGWriteBuffer(KnowledgeGraphStore& kgStore, KGWriteBufferConfig config = {});

    ~KGWriteBuffer();

    KGWriteBuffer(const KGWriteBuffer&) = delete;
    KGWriteBuffer& operator=(const KGWriteBuffer&) = delete;

    /// Add an edge to the buffer. If an edge with the same
    /// (src, dst, relation) already exists in the buffer, it is merged
    /// (max weight, properties coalesced) instead of duplicated.
    ///
    /// May trigger auto-flush if thresholds are exceeded.
    Result<void> addEdge(KGEdge edge);

    /// Add multiple edges to the buffer.
    Result<void> addEdges(const std::vector<KGEdge>& edges);

    /// Add a single doc entity to the buffer.
    Result<void> addDocEntity(DocEntity entity);

    /// Add multiple doc entities to the buffer.
    Result<void> addDocEntities(const std::vector<DocEntity>& entities);

    /// Flush all buffered edges and entities to the store via a single
    /// WriteBatch. The buffer is cleared after a successful flush.
    Result<void> flush();

    /// Flush all buffered edges and entities into an already-open WriteBatch
    /// owned by the caller. Does not commit. The buffer is cleared after a
    /// successful flush; on failure the buffered contents are restored.
    Result<void> flushInto(KnowledgeGraphStore::WriteBatch& wb);

    /// Number of buffered edges (after dedup).
    [[nodiscard]] std::size_t edgeCount() const noexcept;

    /// Number of buffered entities.
    [[nodiscard]] std::size_t entityCount() const noexcept;

    /// Total edges added (before dedup). For telemetry.
    [[nodiscard]] std::size_t totalEdgesAdded() const noexcept;

    /// Total edges actually flushed (after dedup). For telemetry.
    [[nodiscard]] std::size_t totalEdgesFlushed() const noexcept;

    /// Configure a document count for auto-flush tracking.
    /// Each call to incrementDocCount() adds 1; when count >= maxDocs
    /// and autoFlush is enabled, the buffer flushes automatically.
    void incrementDocCount();

    /// Current document count for auto-flush tracking.
    [[nodiscard]] std::size_t docCount() const noexcept;

    /// Reset the document counter without flushing.
    void resetDocCount();

    /// Reset all counters (edges/entities/docs flushed count).
    void resetCounters();

private:
    KnowledgeGraphStore& kgStore_;
    KGWriteBufferConfig config_;

    // Edge buffer: keyed by (src, dst, relation) for coalescing.
    std::unordered_map<EdgeKey, KGEdge> edgeBuffer_;

    // Entity buffer: accumulates until flush.
    std::vector<DocEntity> entityBuffer_;

    // Counters for auto-flush and telemetry.
    std::size_t docCount_ = 0;
    std::size_t totalEdgesAdded_ = 0;
    std::size_t totalEdgesFlushed_ = 0;

    /// Merge 'incoming' into 'existing' per addEdgesUnique() semantics:
    /// keep max weight, COALESCE properties.
    static void mergeEdge(KGEdge& existing, const KGEdge& incoming);

    /// Merge 'incoming' into 'existing' per addDocEntities semantics:
    /// highest confidence wins, extractor coalesced.
    static void mergeDocEntity(DocEntity& existing, const DocEntity& incoming);
};

} // namespace yams::metadata
