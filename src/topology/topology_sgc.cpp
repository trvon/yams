#include <yams/topology/topology_sgc.h>

#include <yams/core/assert.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string>
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
    std::vector<std::string> originalHashes;
    std::vector<std::size_t> originalEmbeddingSizes;
    if constexpr (yams::core::detail::kDcheckEnabled) {
        originalHashes.reserve(n);
        originalEmbeddingSizes.reserve(n);
        for (const auto& doc : documents) {
            originalHashes.push_back(doc.documentHash);
            originalEmbeddingSizes.push_back(doc.embedding.size());
        }
    }

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

    // Only documents with an embedding of the shared dimension join the propagation graph; the
    // rest keep their values and contribute neither features nor degree.
    std::vector<char> participates(n, 0);
    for (std::size_t i = 0; i < n; ++i) {
        participates[i] = documents[i].embedding.size() == dim ? 1 : 0;
    }

    std::unordered_map<std::string, std::size_t> indexByHash;
    indexByHash.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        if (participates[i] && !documents[i].documentHash.empty()) {
            indexByHash[documents[i].documentHash] = i;
        }
    }

    // Undirected edge set, keeping the maximum weight per pair.
    std::unordered_map<std::uint64_t, float> dedup;
    dedup.reserve(n * 4);
    const auto key = [](std::size_t a, std::size_t b) {
        const auto lo = static_cast<std::uint64_t>(std::min(a, b));
        const auto hi = static_cast<std::uint64_t>(std::max(a, b));
        return (hi << 32U) | lo;
    };
    const auto minEdge = static_cast<float>(config.minEdgeScore);
    for (std::size_t i = 0; i < n; ++i) {
        if (!participates[i]) {
            continue;
        }
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
            if (!std::isfinite(neighbor.score) || neighbor.score < minEdge) {
                continue;
            }
            const float w = std::max(0.0F, neighbor.score);
            auto [slot, inserted] = dedup.try_emplace(key(i, j), w);
            if (!inserted) {
                slot->second = std::max(slot->second, w);
            }
        }
    }

    std::vector<std::vector<Edge>> symmetric(n);
    for (const auto& [packed, weight] : dedup) {
        const auto lo = static_cast<std::size_t>(packed & 0xFFFFFFFFU);
        const auto hi = static_cast<std::size_t>(packed >> 32U);
        symmetric[lo].push_back(Edge{hi, weight});
        symmetric[hi].push_back(Edge{lo, weight});
    }
    // Hash-map iteration order is unspecified; order each neighbor list by document hash so the
    // floating-point accumulation order, and therefore the output bits, do not depend on input
    // or neighbor ordering (snapshot fingerprints hash these values).
    for (auto& edges : symmetric) {
        std::ranges::sort(edges, [&](const Edge& lhs, const Edge& rhs) {
            return documents[lhs.to].documentHash < documents[rhs.to].documentHash;
        });
    }

    std::vector<double> invSqrtDeg(n, 0.0);
    for (std::size_t i = 0; i < n; ++i) {
        double degree = 1.0; // self loop
        for (const auto& e : symmetric[i]) {
            degree += static_cast<double>(e.weight);
        }
        invSqrtDeg[i] = 1.0 / std::sqrt(degree);
    }

    // Hop 0 reads the input embeddings directly, so a single hop needs one n*dim buffer; later
    // hops ping-pong between two flat buffers.
    const auto inputRow = [&](const std::vector<float>& source, bool fromDocuments,
                              std::size_t row) -> const float* {
        return fromDocuments ? documents[row].embedding.data() : source.data() + row * dim;
    };
    std::vector<float> out(n * dim, 0.0F);
    std::vector<float> in;
    for (std::size_t hop = 0; hop < hops; ++hop) {
        const bool fromDocuments = hop == 0;
        if (!fromDocuments) {
            in.swap(out);
            if (out.size() != n * dim) {
                out.assign(n * dim, 0.0F);
            }
        }
        for (std::size_t i = 0; i < n; ++i) {
            if (!participates[i]) {
                continue;
            }
            float* row = out.data() + i * dim;
            const float* self = inputRow(in, fromDocuments, i);
            const double selfScale = invSqrtDeg[i] * invSqrtDeg[i];
            for (std::size_t d = 0; d < dim; ++d) {
                row[d] = static_cast<float>(selfScale * static_cast<double>(self[d]));
            }
            for (const auto& e : symmetric[i]) {
                const double scale =
                    static_cast<double>(e.weight) * invSqrtDeg[i] * invSqrtDeg[e.to];
                if (scale == 0.0) {
                    continue;
                }
                const float* src = inputRow(in, fromDocuments, e.to);
                for (std::size_t d = 0; d < dim; ++d) {
                    row[d] += static_cast<float>(scale * static_cast<double>(src[d]));
                }
            }
        }
    }

    for (std::size_t i = 0; i < n; ++i) {
        if (participates[i]) {
            const float* row = out.data() + i * dim;
            std::copy(row, row + dim, documents[i].embedding.begin());
        }
    }

    if constexpr (yams::core::detail::kDcheckEnabled) {
        YAMS_DCHECK(documents.size() == n, "SGC smoothing must preserve document count");
        for (std::size_t i = 0; i < n; ++i) {
            YAMS_DCHECK(documents[i].documentHash == originalHashes[i],
                        "SGC smoothing must preserve document hash identity");
            YAMS_DCHECK(documents[i].embedding.size() == originalEmbeddingSizes[i],
                        "SGC smoothing must preserve embedding dimensionality");
        }
    }
}

} // namespace yams::topology
