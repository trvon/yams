// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/topology/topology_member_order.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <numeric>
#include <string>
#include <tuple>

namespace yams::topology {

namespace {

constexpr double kSimilarityTie = 1e-12;

const TopologyDocumentInput*
lookup(const std::unordered_map<std::string_view, const TopologyDocumentInput*>& documentsByHash,
       const std::string& hash) {
    const auto it = documentsByHash.find(hash);
    return it == documentsByHash.end() ? nullptr : it->second;
}

std::optional<std::int64_t> eventTimeOf(const TopologyDocumentInput* document) {
    return document == nullptr ? std::nullopt : document->eventTime;
}

/// Ascending time, members without a time last; ties resolve to the smaller index.
bool timeThenIndexLess(std::optional<std::int64_t> lhsTime, uint32_t lhsIndex,
                       std::optional<std::int64_t> rhsTime, uint32_t rhsIndex) {
    if (lhsTime.has_value() != rhsTime.has_value()) {
        return lhsTime.has_value();
    }
    if (lhsTime.has_value() && *lhsTime != *rhsTime) {
        return *lhsTime < *rhsTime;
    }
    return lhsIndex < rhsIndex;
}

std::vector<uint32_t> timeOrder(
    const ClusterArtifact& cluster,
    const std::unordered_map<std::string_view, const TopologyDocumentInput*>& documentsByHash) {
    const auto count = static_cast<uint32_t>(cluster.memberDocumentHashes.size());
    std::vector<std::optional<std::int64_t>> times(count);
    for (uint32_t index = 0; index < count; ++index) {
        times[index] = eventTimeOf(lookup(documentsByHash, cluster.memberDocumentHashes[index]));
    }
    std::vector<uint32_t> order(count);
    std::iota(order.begin(), order.end(), 0U);
    std::ranges::sort(order, [&](uint32_t lhs, uint32_t rhs) {
        return timeThenIndexLess(times[lhs], lhs, times[rhs], rhs);
    });
    return order;
}

std::vector<uint32_t> sessionOrder(
    const ClusterArtifact& cluster,
    const std::unordered_map<std::string_view, const TopologyDocumentInput*>& documentsByHash) {
    const auto count = static_cast<uint32_t>(cluster.memberDocumentHashes.size());
    struct Group {
        std::optional<std::int64_t> earliest;
        std::vector<uint32_t> members;
    };
    // std::map keeps session ids sorted, which is the tie-break between groups.
    std::map<std::string_view, Group> groups;
    Group unsessioned;
    std::vector<std::optional<std::int64_t>> times(count);
    for (uint32_t index = 0; index < count; ++index) {
        const auto* document = lookup(documentsByHash, cluster.memberDocumentHashes[index]);
        times[index] = eventTimeOf(document);
        Group& group = document == nullptr || document->sessionId.empty()
                           ? unsessioned
                           : groups[std::string_view{document->sessionId}];
        group.members.push_back(index);
        if (times[index].has_value() &&
            (!group.earliest.has_value() || *times[index] < *group.earliest)) {
            group.earliest = times[index];
        }
    }

    std::vector<std::pair<std::string_view, Group*>> ordered;
    ordered.reserve(groups.size());
    for (auto& [sessionId, group] : groups) {
        ordered.emplace_back(sessionId, &group);
    }
    std::ranges::sort(ordered, [](const auto& lhs, const auto& rhs) {
        const auto& lhsEarliest = lhs.second->earliest;
        const auto& rhsEarliest = rhs.second->earliest;
        if (lhsEarliest.has_value() != rhsEarliest.has_value()) {
            return lhsEarliest.has_value();
        }
        if (lhsEarliest.has_value() && *lhsEarliest != *rhsEarliest) {
            return *lhsEarliest < *rhsEarliest;
        }
        return lhs.first < rhs.first;
    });

    auto byTime = [&](uint32_t lhs, uint32_t rhs) {
        return timeThenIndexLess(times[lhs], lhs, times[rhs], rhs);
    };
    std::vector<uint32_t> order;
    order.reserve(count);
    for (auto& [sessionId, group] : ordered) {
        std::ranges::sort(group->members, byTime);
        order.insert(order.end(), group->members.begin(), group->members.end());
    }
    std::ranges::sort(unsessioned.members, byTime);
    order.insert(order.end(), unsessioned.members.begin(), unsessioned.members.end());
    return order;
}

/// Unit-normalized copy, or empty when the embedding is unusable (empty, non-finite, zero norm,
/// or a dimension other than `dimension` when that is non-zero).
std::vector<double> unitEmbedding(const TopologyDocumentInput* document, std::size_t dimension) {
    if (document == nullptr || document->embedding.empty() ||
        (dimension != 0 && document->embedding.size() != dimension)) {
        return {};
    }
    double normSq = 0.0;
    for (const float value : document->embedding) {
        if (!std::isfinite(value)) {
            return {};
        }
        normSq += static_cast<double>(value) * static_cast<double>(value);
    }
    if (normSq <= 0.0) {
        return {};
    }
    const double inv = 1.0 / std::sqrt(normSq);
    std::vector<double> unit(document->embedding.size());
    for (std::size_t d = 0; d < unit.size(); ++d) {
        unit[d] = static_cast<double>(document->embedding[d]) * inv;
    }
    return unit;
}

std::vector<uint32_t> similarityChainOrder(
    const ClusterArtifact& cluster,
    const std::unordered_map<std::string_view, const TopologyDocumentInput*>& documentsByHash) {
    const auto count = static_cast<uint32_t>(cluster.memberDocumentHashes.size());
    // The tour's dimension is the representative's when it is usable, else the first usable
    // member's (by hash); members of another dimension are unusable.
    std::optional<uint32_t> start;
    if (cluster.medoid.has_value()) {
        for (uint32_t index = 0; index < count; ++index) {
            if (cluster.memberDocumentHashes[index] == cluster.medoid->documentHash &&
                !unitEmbedding(lookup(documentsByHash, cluster.memberDocumentHashes[index]), 0)
                     .empty()) {
                start = index;
                break;
            }
        }
    }
    std::vector<std::vector<double>> unit(count);
    std::size_t dimension = 0;
    if (start.has_value()) {
        unit[*start] =
            unitEmbedding(lookup(documentsByHash, cluster.memberDocumentHashes[*start]), 0);
        dimension = unit[*start].size();
    }
    std::vector<uint32_t> usable;
    std::vector<uint32_t> unusable;
    for (uint32_t index = 0; index < count; ++index) {
        if (start.has_value() && index == *start) {
            usable.push_back(index);
            continue;
        }
        unit[index] =
            unitEmbedding(lookup(documentsByHash, cluster.memberDocumentHashes[index]), dimension);
        if (unit[index].empty()) {
            unusable.push_back(index);
            continue;
        }
        if (dimension == 0) {
            dimension = unit[index].size();
        }
        usable.push_back(index);
    }
    if (!start.has_value() && !usable.empty()) {
        start = usable.front(); // smallest usable hash
    }

    std::vector<uint32_t> order;
    order.reserve(count);
    if (start.has_value()) {
        std::vector<bool> visited(count, false);
        uint32_t current = *start;
        visited[current] = true;
        order.push_back(current);
        for (std::size_t step = 1; step < usable.size(); ++step) {
            uint32_t best = count;
            double bestSimilarity = -std::numeric_limits<double>::infinity();
            for (const uint32_t candidate : usable) {
                if (visited[candidate]) {
                    continue;
                }
                double dot = 0.0;
                for (std::size_t d = 0; d < dimension; ++d) {
                    dot += unit[current][d] * unit[candidate][d];
                }
                // Strictly better, or a tie resolved to the smaller index (usable is ascending).
                if (dot > bestSimilarity + kSimilarityTie) {
                    bestSimilarity = dot;
                    best = candidate;
                }
            }
            if (best == count) {
                break;
            }
            visited[best] = true;
            order.push_back(best);
            current = best;
        }
    }
    order.insert(order.end(), unusable.begin(), unusable.end());
    return order;
}

} // namespace

std::unordered_map<std::string_view, const TopologyDocumentInput*>
indexTopologyDocumentsByHash(std::span<const TopologyDocumentInput> documents) {
    std::unordered_map<std::string_view, const TopologyDocumentInput*> byHash;
    byHash.reserve(documents.size());
    for (const auto& document : documents) {
        if (!document.documentHash.empty()) {
            byHash.emplace(document.documentHash, &document);
        }
    }
    return byHash;
}

std::vector<uint32_t> computeClusterMemberOrder(
    RoadOrderKind kind, const ClusterArtifact& cluster,
    const std::unordered_map<std::string_view, const TopologyDocumentInput*>& documentsByHash) {
    if (kind == RoadOrderKind::None || cluster.memberDocumentHashes.size() < 2 ||
        cluster.memberDocumentHashes.size() > std::numeric_limits<uint32_t>::max()) {
        return {};
    }
    switch (kind) {
        case RoadOrderKind::Time:
            return timeOrder(cluster, documentsByHash);
        case RoadOrderKind::Session:
            return sessionOrder(cluster, documentsByHash);
        case RoadOrderKind::SimilarityChain:
            return similarityChainOrder(cluster, documentsByHash);
        case RoadOrderKind::None:
            break;
    }
    return {};
}

void applyClusterMemberOrders(TopologyArtifactBatch& batch,
                              std::span<const TopologyDocumentInput> documents,
                              RoadOrderKind kind) {
    if (kind == RoadOrderKind::None) {
        for (auto& cluster : batch.clusters) {
            cluster.orderKind = RoadOrderKind::None;
            cluster.orderedMembers.clear();
        }
        return;
    }
    const auto byHash = indexTopologyDocumentsByHash(documents);
    for (auto& cluster : batch.clusters) {
        cluster.orderedMembers = computeClusterMemberOrder(kind, cluster, byHash);
        cluster.orderKind = cluster.orderedMembers.empty() ? RoadOrderKind::None : kind;
    }
}

std::optional<std::string_view> validateMemberPermutation(std::span<const uint32_t> permutation,
                                                          std::size_t memberCount) {
    if (permutation.size() != memberCount) {
        return "order_length_mismatch";
    }
    std::vector<bool> seen(memberCount, false);
    for (const uint32_t index : permutation) {
        if (index >= memberCount) {
            return "order_index_out_of_range";
        }
        if (seen[index]) {
            return "order_index_repeated";
        }
        seen[index] = true;
    }
    return std::nullopt;
}

} // namespace yams::topology
