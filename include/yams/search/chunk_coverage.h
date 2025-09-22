#pragma once

#include <yams/vector/vector_index_manager.h>

#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace yams::search {

// Simple pooling options for aggregating chunk scores per base document.
enum class Pooling { MAX, AVG };

// Aggregated stats for a base document grouped from chunk-level vector hits.
struct DocGroupStats {
    std::string base_id;                // e.g., document hash or path (id before '#')
    size_t contributing_chunks = 0;     // unique chunk ids that contributed
    std::optional<size_t> total_chunks; // total annotated chunks for the document, if known
    float pooled_score = 0.0f;          // pooled similarity for the group

    // Coverage in [0,1] when total_chunks is known; otherwise std::nullopt
    std::optional<float> coverage() const {
        if (!total_chunks || *total_chunks == 0)
            return std::nullopt;
        const float denom = static_cast<float>(*total_chunks);
        return static_cast<float>(contributing_chunks) / denom;
    }
};

// Extract a base document id from a chunk id. Convention: "<base>#<idx>".
// If no delimiter is found, returns the original id.
std::string baseIdFromChunkId(std::string_view chunk_id);

// Group chunk-level vector results by base id and compute pooled scores and coverage.
// - results: vector search results at chunk granularity
// - pooling: MAX or AVG across chunk similarities for each base document
// - total_chunk_resolver: optional callback to look up the annotated chunk count per base id
//   (e.g., from metadata DB or vector DB). If absent or returns std::nullopt, coverage() is null.
std::vector<DocGroupStats> groupAndAggregate(
    const std::vector<yams::vector::SearchResult>& results, Pooling pooling,
    const std::function<std::optional<size_t>(std::string_view)>& total_chunk_resolver = {});

} // namespace yams::search
