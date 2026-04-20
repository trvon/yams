#include <yams/topology/topology_sgc.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <unordered_map>
#include <utility>
#include <vector>

namespace yams::topology {

namespace {

struct Edge {
    std::size_t to{0};
    float weight{0.0F};
};

} // namespace

void applySGCSmoothing(std::vector<TopologyDocumentInput>& documents,
                       const TopologyBuildConfig& config, std::size_t hops) {
    if (hops == 0 || documents.size() < 2) {
        return;
    }

    const std::size_t n = documents.size();
    std::size_t dim = 0;
    for (const auto& doc : documents) {
        if (!doc.embedding.empty()) {
            dim = doc.embedding.size();
            break;
        }
    }
    if (dim == 0) {
        return;
    }

    std::unordered_map<std::string, std::size_t> indexByHash;
    indexByHash.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        if (!documents[i].documentHash.empty()) {
            indexByHash[documents[i].documentHash] = i;
        }
    }

    std::vector<std::vector<Edge>> adjacency(n);
    const auto minEdge = static_cast<float>(config.minEdgeScore);
    for (std::size_t i = 0; i < n; ++i) {
        for (const auto& neighbor : documents[i].neighbors) {
            if (neighbor.documentHash.empty()) {
                continue;
            }
            const auto it = indexByHash.find(neighbor.documentHash);
            if (it == indexByHash.end()) {
                continue;
            }
            const std::size_t j = it->second;
            if (i == j) {
                continue;
            }
            if (config.reciprocalOnly && !neighbor.reciprocal) {
                continue;
            }
            if (neighbor.score < minEdge) {
                continue;
            }
            const float w = std::max(0.0F, neighbor.score);
            adjacency[i].push_back(Edge{j, w});
        }
    }

    std::vector<std::vector<Edge>> symmetric(n);
    {
        std::unordered_map<std::uint64_t, float> dedup;
        dedup.reserve(n * 4);
        const auto key = [](std::size_t a, std::size_t b) {
            const auto lo = static_cast<std::uint64_t>(std::min(a, b));
            const auto hi = static_cast<std::uint64_t>(std::max(a, b));
            return (hi << 32U) | lo;
        };
        for (std::size_t i = 0; i < n; ++i) {
            for (const auto& e : adjacency[i]) {
                const auto k = key(i, e.to);
                auto existing = dedup.find(k);
                if (existing == dedup.end()) {
                    dedup.emplace(k, e.weight);
                } else {
                    existing->second = std::max(existing->second, e.weight);
                }
            }
        }
        for (const auto& [packed, weight] : dedup) {
            const auto lo = static_cast<std::size_t>(packed & 0xFFFFFFFFU);
            const auto hi = static_cast<std::size_t>(packed >> 32U);
            symmetric[lo].push_back(Edge{hi, weight});
            symmetric[hi].push_back(Edge{lo, weight});
        }
    }

    std::vector<double> degree(n, 1.0);
    for (std::size_t i = 0; i < n; ++i) {
        double sum = 1.0;
        for (const auto& e : symmetric[i]) {
            sum += static_cast<double>(e.weight);
        }
        degree[i] = sum;
    }
    std::vector<double> invSqrtDeg(n, 0.0);
    for (std::size_t i = 0; i < n; ++i) {
        invSqrtDeg[i] = degree[i] > 0.0 ? 1.0 / std::sqrt(degree[i]) : 0.0;
    }

    std::vector<std::vector<float>> features(n, std::vector<float>(dim, 0.0F));
    for (std::size_t i = 0; i < n; ++i) {
        if (documents[i].embedding.size() == dim) {
            features[i] = documents[i].embedding;
        }
    }

    std::vector<std::vector<float>> next(n, std::vector<float>(dim, 0.0F));
    for (std::size_t hop = 0; hop < hops; ++hop) {
        for (std::size_t i = 0; i < n; ++i) {
            std::vector<float>& row = next[i];
            std::fill(row.begin(), row.end(), 0.0F);
            const double selfScale = invSqrtDeg[i] * invSqrtDeg[i];
            for (std::size_t d = 0; d < dim; ++d) {
                row[d] = static_cast<float>(selfScale * static_cast<double>(features[i][d]));
            }
            for (const auto& e : symmetric[i]) {
                const double scale =
                    static_cast<double>(e.weight) * invSqrtDeg[i] * invSqrtDeg[e.to];
                if (scale == 0.0) {
                    continue;
                }
                const auto& src = features[e.to];
                for (std::size_t d = 0; d < dim; ++d) {
                    row[d] += static_cast<float>(scale * static_cast<double>(src[d]));
                }
            }
        }
        features.swap(next);
    }

    for (std::size_t i = 0; i < n; ++i) {
        if (documents[i].embedding.size() == dim) {
            documents[i].embedding = std::move(features[i]);
        }
    }
}

} // namespace yams::topology
