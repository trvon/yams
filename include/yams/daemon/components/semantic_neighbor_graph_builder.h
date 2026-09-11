// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/daemon/components/embedding_service_config.h>
#include <yams/metadata/knowledge_graph_store.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace yams::vector {
class VectorDatabase;
}

namespace yams::daemon {

/**
 * Maintains the semantic_neighbor layer of the knowledge graph: for each source document it
 * finds the top-K most similar document embeddings and emits a forward and a reverse
 * `semantic_neighbor` edge per neighbor. Two shapes of update exist and share every policy:
 *
 *  - streaming: sources are a handful of freshly embedded documents; candidates come from a
 *    per-process corpus cache (plus HNSW when the pair count is large);
 *  - corpus-wide: every document-level vector is streamed once and scored against itself.
 *
 * The builder owns its caches and counters and talks to the outside only through the KG store,
 * the vector database, an EdgeSink (whoever persists the edges) and an optional phase timer. It
 * does not serialize its callers; the owner holds one mutation lock around update().
 */
class SemanticNeighborGraphBuilder {
public:
    /// Persist a batch of edges. Return false when no writer is available (the batch is dropped
    /// and counted as an update error).
    using EdgeSink =
        std::function<bool(std::vector<metadata::KGEdge> edges, std::string_view source)>;
    using PhaseTimer =
        std::function<void(std::string_view phase, std::chrono::steady_clock::time_point start)>;

    explicit SemanticNeighborGraphBuilder(SemanticNeighborGraphConfig config = {});

    // Checked forward/reverse edge capacity, including the vector's representable size limit.
    static std::optional<std::size_t> checkedEdgeCapacity(std::size_t sources,
                                                          std::size_t topK) noexcept;

    void setEdgeSink(EdgeSink sink);
    void setPhaseTimer(PhaseTimer timer);
    const SemanticNeighborGraphConfig& config() const noexcept { return cfg_; }

    void update(const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
                const std::shared_ptr<yams::vector::VectorDatabase>& vdb,
                const std::string& modelName,
                const std::vector<std::pair<std::string, std::string>>& sourceDocuments,
                bool sourceAllCorpus);

    /// Drop the corpus and node-id caches (shutdown); counters are kept.
    void clearCaches();

    std::uint64_t edgesCreated() const noexcept {
        return semanticEdgesCreated_.load(std::memory_order_relaxed);
    }
    std::uint64_t docsProcessed() const noexcept {
        return semanticDocsProcessed_.load(std::memory_order_relaxed);
    }
    std::uint64_t updateErrors() const noexcept {
        return semanticUpdateErrors_.load(std::memory_order_relaxed);
    }

private:
    bool emitEdges(std::vector<metadata::KGEdge> edges, std::string_view source);
    void recordPhaseTiming(std::string_view phase, std::chrono::steady_clock::time_point start);

    SemanticNeighborGraphConfig cfg_;
    EdgeSink edgeSink_;
    PhaseTimer phaseTimer_;

    std::atomic<std::uint64_t> semanticEdgesCreated_{0};
    std::atomic<std::uint64_t> semanticDocsProcessed_{0};
    std::atomic<std::uint64_t> semanticUpdateErrors_{0};

    struct SemanticCorpusEntry {
        std::string hash;
        std::string filePath;
        std::vector<float> embedding;
        float invNorm{0.0f};
    };
    mutable std::mutex semanticCorpusMutex_;
    std::unordered_map<std::string, SemanticCorpusEntry> semanticCorpusCache_;
    mutable std::mutex semanticNodeIdCacheMutex_;
    std::unordered_map<std::string, std::optional<std::int64_t>> semanticNodeIdCache_;
};

} // namespace yams::daemon
