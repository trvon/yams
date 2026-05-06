// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <yams/search/topological_quality.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <numeric>
#include <random>

namespace yams::search {

namespace {

struct UnionFind {
    std::vector<std::size_t> parent;
    std::vector<std::size_t> rank;
    explicit UnionFind(std::size_t n) : parent(n), rank(n, 0) {
        std::iota(parent.begin(), parent.end(), std::size_t{0});
    }
    std::size_t find(std::size_t x) {
        while (parent[x] != x) {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        return x;
    }
    // Returns true if the two components were distinct (i.e., this edge
    // produced an H_0 death event).
    bool unite(std::size_t a, std::size_t b) {
        auto ra = find(a);
        auto rb = find(b);
        if (ra == rb)
            return false;
        if (rank[ra] < rank[rb])
            std::swap(ra, rb);
        parent[rb] = ra;
        if (rank[ra] == rank[rb])
            ++rank[ra];
        return true;
    }
};

struct Edge {
    std::size_t i;
    std::size_t j;
    double distance;
};

double pairwiseDistance(std::span<const float> embeddings, std::size_t dim, std::size_t a,
                        std::size_t b) {
    const float* pa = embeddings.data() + a * dim;
    const float* pb = embeddings.data() + b * dim;
    double acc = 0.0;
    for (std::size_t k = 0; k < dim; ++k) {
        const double d = static_cast<double>(pa[k]) - static_cast<double>(pb[k]);
        acc += d * d;
    }
    return std::sqrt(acc);
}

double percentile(std::vector<double> values, double q) {
    if (values.empty())
        return 0.0;
    const std::size_t idx = static_cast<std::size_t>(std::clamp(
        q * static_cast<double>(values.size() - 1), 0.0, static_cast<double>(values.size() - 1)));
    std::nth_element(values.begin(), values.begin() + static_cast<std::ptrdiff_t>(idx),
                     values.end());
    return values[idx];
}

} // namespace

double computePersistenceH0(std::span<const float> embeddings, std::size_t count,
                            std::size_t dim) noexcept {
    if (count < 2 || dim == 0 || embeddings.size() < count * dim) {
        return 0.0;
    }
    // Build the edge list: all pairwise distances.
    const std::size_t edgeCount = count * (count - 1) / 2;
    std::vector<Edge> edges;
    edges.reserve(edgeCount);
    std::vector<double> allDistances;
    allDistances.reserve(edgeCount);
    for (std::size_t i = 0; i < count; ++i) {
        for (std::size_t j = i + 1; j < count; ++j) {
            const double d = pairwiseDistance(embeddings, dim, i, j);
            edges.push_back(Edge{i, j, d});
            allDistances.push_back(d);
        }
    }
    if (edges.empty()) {
        return 0.0;
    }

    // Sort edges ascending by distance (stable so ties are reproducible).
    std::stable_sort(edges.begin(), edges.end(),
                     [](const Edge& a, const Edge& b) { return a.distance < b.distance; });

    // 95th-percentile pairwise distance as the normalization denominator.
    const double norm = percentile(std::move(allDistances), 0.95);
    if (norm <= 0.0) {
        return 0.0;
    }

    // Union-find over sorted edges. Each edge that unites two distinct
    // components is an H_0 "death" event at radius = edge.distance.
    // Accumulate (death - 0) / norm for all non-essential deaths.
    // The last (n-1)th merge produces the essential feature — we skip the
    // very last unite event by counting merges and stopping before it would
    // happen. Equivalently: we can stop after n-2 merges or just subtract
    // the last merge's contribution. We use the stop-after-n-2 approach.
    UnionFind uf(count);
    double totalPersistence = 0.0;
    std::size_t merges = 0;
    const std::size_t maxMerges = (count >= 2) ? (count - 2) : 0;
    for (const auto& e : edges) {
        if (merges >= maxMerges) {
            break;
        }
        if (uf.unite(e.i, e.j)) {
            totalPersistence += e.distance / norm;
            ++merges;
        }
    }
    return totalPersistence;
}

double computePersistenceH0(const std::vector<std::vector<float>>& embeddings) noexcept {
    if (embeddings.empty()) {
        return 0.0;
    }
    const std::size_t count = embeddings.size();
    const std::size_t dim = embeddings.front().size();
    if (dim == 0) {
        return 0.0;
    }
    std::vector<float> flat;
    flat.reserve(count * dim);
    for (const auto& row : embeddings) {
        if (row.size() != dim) {
            return 0.0;
        }
        flat.insert(flat.end(), row.begin(), row.end());
    }
    return computePersistenceH0(std::span<const float>(flat), count, dim);
}

std::vector<std::size_t> deterministicSubsample(std::size_t total, std::size_t maxCount,
                                                std::uint64_t seed) {
    if (total == 0) {
        return {};
    }
    if (total <= maxCount) {
        std::vector<std::size_t> ids(total);
        std::iota(ids.begin(), ids.end(), std::size_t{0});
        return ids;
    }
    std::vector<std::size_t> ids(total);
    std::iota(ids.begin(), ids.end(), std::size_t{0});
    std::mt19937_64 gen{seed};
    std::shuffle(ids.begin(), ids.end(), gen);
    ids.resize(maxCount);
    std::sort(ids.begin(), ids.end());
    return ids;
}

} // namespace yams::search
