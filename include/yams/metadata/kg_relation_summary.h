#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace yams::metadata {

class KnowledgeGraphStore;

struct KGRelationSummary {
    std::size_t totalEdges{0};
    std::vector<std::pair<std::string, std::size_t>> topRelations;
};

std::string normalizeRelationName(std::string relation);

std::optional<KGRelationSummary>
collectFileRelationSummary(KnowledgeGraphStore* kgStore, std::string_view path,
                           std::optional<std::string_view> hash = std::nullopt,
                           std::size_t edgeLimit = 256, std::size_t topLimit = 4);

std::string formatRelationSummary(const std::vector<std::pair<std::string, std::size_t>>& pairs,
                                  bool humanReadable);

} // namespace yams::metadata
