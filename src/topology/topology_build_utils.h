#pragma once

#include <yams/topology/topology_engine.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>
#include <vector>

namespace yams::topology::detail {

using PairKey = std::pair<std::size_t, std::size_t>;

struct PairKeyHash {
    std::size_t operator()(const PairKey& key) const noexcept {
        const std::uint64_t first = static_cast<std::uint64_t>(key.first);
        const std::uint64_t second = static_cast<std::uint64_t>(key.second);
        std::uint64_t mixed = first * 0x9E3779B97F4A7C15ULL + second;
        mixed ^= mixed >> 33;
        mixed *= 0xFF51AFD7ED558CCDULL;
        mixed ^= mixed >> 33;
        return static_cast<std::size_t>(mixed);
    }
};

inline std::vector<float> meanEmbedding(std::span<const TopologyDocumentInput> documents,
                                        const std::vector<std::size_t>& members) {
    std::vector<float> centroid;
    std::size_t count = 0;
    for (const std::size_t index : members) {
        if (index >= documents.size() || documents[index].embedding.empty()) {
            continue;
        }

        const auto& embedding = documents[index].embedding;
        if (centroid.empty()) {
            centroid = embedding;
            count = 1;
            continue;
        }
        if (centroid.size() != embedding.size()) {
            continue;
        }

        for (std::size_t dimension = 0; dimension < embedding.size(); ++dimension) {
            centroid[dimension] += embedding[dimension];
        }
        ++count;
    }

    if (count == 0) {
        return {};
    }
    if (count > 1) {
        for (float& value : centroid) {
            value /= static_cast<float>(count);
        }
    }
    return centroid;
}

} // namespace yams::topology::detail
