#pragma once

#include <yams/core/types.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/search/search_results.h>

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::search {

/**
 * Symbol enrichment helper (PBI-059)
 *
 * Enriches search results with symbol context by querying the Knowledge Graph.
 * Leverages the symbol nodes and relationships populated by EntityGraphService.
 */
class SymbolEnricher {
public:
    explicit SymbolEnricher(std::shared_ptr<yams::metadata::KnowledgeGraphStore> kg_store)
        : kg_store_(std::move(kg_store)) {}

    /**
     * Enrich a search result with symbol context if applicable.
     *
     * Queries the KG for:
     * - Symbol nodes defined in the document
     * - Symbol metadata (kind, signature, callers/callees)
     * - Match type (definition vs usage)
     *
     * @param result The search result to enrich
     * @param query_text Original query text for match scoring
     * @return true if enrichment was applied
     */
    bool enrichResult(SearchResultItem& result, const std::string& query_text);

    /**
     * Extract symbol information from a KG node.
     *
     * @param node_id KG node ID
     * @return SymbolInfo if node is a symbol, nullopt otherwise
     */
    std::optional<SymbolInfo> extractSymbolInfo(std::int64_t node_id);

    /**
     * Extract symbol info from a pre-fetched node (avoids additional query).
     * Used by batch processing path.
     */
    std::optional<SymbolInfo> extractSymbolInfoFromNode(const yams::metadata::KGNode& node);

    /**
     * Batch extract symbol info for multiple node IDs using single query.
     * Much more efficient than calling extractSymbolInfo() N times.
     *
     * @param node_ids Vector of node IDs to fetch
     * @return Map from node_id to SymbolInfo for valid symbol nodes
     */
    std::unordered_map<std::int64_t, SymbolInfo>
    extractSymbolInfoBatch(const std::vector<std::int64_t>& node_ids);

private:
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kg_store_;
};

} // namespace yams::search
