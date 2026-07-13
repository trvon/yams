#include <yams/topology/topology_alternate_engines.h>
#include <yams/topology/topology_representatives.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <numeric>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::topology {

namespace {

using PairKey = std::pair<std::size_t, std::size_t>;

struct PairKeyHash {
    std::size_t operator()(const PairKey& k) const noexcept {
        const std::uint64_t a = static_cast<std::uint64_t>(k.first);
        const std::uint64_t b = static_cast<std::uint64_t>(k.second);
        std::uint64_t x = a * 0x9E3779B97F4A7C15ULL + b;
        x ^= x >> 33;
        x *= 0xFF51AFD7ED558CCDULL;
        x ^= x >> 33;
        return static_cast<std::size_t>(x);
    }
};

using PairWeightMap = std::unordered_map<PairKey, float, PairKeyHash>;

std::string makeSnapshotId(std::uint64_t unixMillis) {
    return "topology-" + std::to_string(unixMillis);
}

std::string makeClusterId(const std::string& anchorHash) {
    return "topology.cluster." + anchorHash;
}

std::vector<float> meanEmbedding(std::span<const TopologyDocumentInput> documents,
                                 const std::vector<std::size_t>& members) {
    std::vector<float> centroid;
    std::size_t count = 0;
    for (std::size_t idx : members) {
        if (idx >= documents.size() || documents[idx].embedding.empty()) {
            continue;
        }
        const auto& emb = documents[idx].embedding;
        if (centroid.empty()) {
            centroid.assign(emb.size(), 0.0F);
        } else if (centroid.size() != emb.size()) {
            continue;
        }
        for (std::size_t i = 0; i < emb.size(); ++i) {
            centroid[i] += emb[i];
        }
        ++count;
    }
    if (count == 0) {
        return {};
    }
    for (auto& v : centroid) {
        v /= static_cast<float>(count);
    }
    return centroid;
}

struct TimeStamps {
    std::uint64_t unixSeconds{0};
    std::uint64_t unixMillis{0};
};

TimeStamps nowStamps() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    TimeStamps ts;
    ts.unixSeconds =
        static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(now).count());
    ts.unixMillis = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
    return ts;
}

PairWeightMap buildPairWeights(std::span<const TopologyDocumentInput> documents,
                               const std::unordered_map<std::string, std::size_t>& indexByHash,
                               const TopologyBuildConfig& config) {
    PairWeightMap pairWeights;
    for (std::size_t i = 0; i < documents.size(); ++i) {
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
            if (neighbor.score < static_cast<float>(config.minEdgeScore)) {
                continue;
            }
            const PairKey key{std::min(i, j), std::max(i, j)};
            auto wt = pairWeights.find(key);
            if (wt == pairWeights.end()) {
                pairWeights.emplace(key, neighbor.score);
            } else {
                wt->second = std::max(wt->second, neighbor.score);
            }
        }
    }
    return pairWeights;
}

std::vector<std::vector<std::pair<std::size_t, float>>>
makeAdjacency(std::size_t n, const PairWeightMap& pairWeights) {
    std::vector<std::vector<std::pair<std::size_t, float>>> adjacency(n);
    for (const auto& [key, weight] : pairWeights) {
        adjacency[key.first].emplace_back(key.second, weight);
        adjacency[key.second].emplace_back(key.first, weight);
    }
    return adjacency;
}

// Build a TopologyArtifactBatch from a per-document cluster assignment.
//
// `assignment[i]` is the cluster id for documents[i]; any value is accepted as
// an opaque bucket key. Cohesion / persistence are computed from `pairWeights`
// when provided (graph-based engines); otherwise only structural metrics
// populate. `algorithm` is stamped into the batch.
TopologyArtifactBatch buildBatchFromAssignment(std::span<const TopologyDocumentInput> documents,
                                               const std::vector<std::int64_t>& assignment,
                                               const PairWeightMap& pairWeights,
                                               std::string algorithm, const TimeStamps& ts,
                                               const TopologyBuildConfig& config) {
    TopologyArtifactBatch batch;
    batch.snapshotId = makeSnapshotId(ts.unixMillis);
    batch.algorithm = std::move(algorithm);
    batch.generatedAtUnixSeconds = ts.unixSeconds;

    if (documents.empty()) {
        return batch;
    }

    std::unordered_map<std::int64_t, std::vector<std::size_t>> buckets;
    buckets.reserve(assignment.size());
    for (std::size_t i = 0; i < assignment.size(); ++i) {
        buckets[assignment[i]].push_back(i);
    }

    const auto adjacency = makeAdjacency(documents.size(), pairWeights);

    std::vector<std::pair<std::int64_t, std::vector<std::size_t>>> ordered(buckets.begin(),
                                                                           buckets.end());
    for (auto& [bucketId, members] : ordered) {
        std::sort(members.begin(), members.end(), [&documents](std::size_t lhs, std::size_t rhs) {
            return documents[lhs].documentHash < documents[rhs].documentHash;
        });
    }
    std::sort(ordered.begin(), ordered.end(), [&documents](const auto& lhs, const auto& rhs) {
        return documents[lhs.second.front()].documentHash <
               documents[rhs.second.front()].documentHash;
    });

    for (auto& [bucketId, members] : ordered) {
        if (members.empty()) {
            continue;
        }
        const std::string clusterId = makeClusterId(documents[members.front()].documentHash);
        std::unordered_set<std::size_t> memberSet(members.begin(), members.end());

        double cohesion = 0.0;
        double persistence = 0.0;
        std::size_t internalEdgeCount = 0;
        std::unordered_map<std::size_t, double> weightedDegree;
        weightedDegree.reserve(members.size());
        std::size_t bridgeCount = 0;

        for (std::size_t idx : members) {
            std::size_t degree = 0;
            for (const auto& [neighborIdx, weight] : adjacency[idx]) {
                if (!memberSet.contains(neighborIdx)) {
                    continue;
                }
                weightedDegree[idx] += weight;
                ++degree;
                // Undirected edge stored twice in adjacency; accumulate
                // cohesion/persistence only on the ordered half.
                if (idx < neighborIdx) {
                    cohesion += weight;
                    persistence =
                        internalEdgeCount == 0 ? weight : std::min<double>(persistence, weight);
                    ++internalEdgeCount;
                }
            }
            if (members.size() > 2 && degree >= 2) {
                ++bridgeCount;
            }
        }

        if (internalEdgeCount > 0) {
            cohesion /= static_cast<double>(internalEdgeCount);
        } else {
            persistence = 0.0;
        }

        std::size_t medoidIdx = members.front();
        double medoidScore = -1.0;
        for (std::size_t idx : members) {
            const double candidate = weightedDegree[idx];
            if (candidate > medoidScore ||
                (std::abs(candidate - medoidScore) < 1e-9 &&
                 documents[idx].documentHash < documents[medoidIdx].documentHash)) {
                medoidIdx = idx;
                medoidScore = candidate;
            }
        }

        ClusterArtifact cluster;
        cluster.clusterId = clusterId;
        cluster.level = 0;
        cluster.memberCount = members.size();
        cluster.persistenceScore = persistence;
        cluster.cohesionScore = cohesion;
        const double possibleEdges = members.size() > 1
                                         ? static_cast<double>(members.size()) *
                                               static_cast<double>(members.size() - 1) / 2.0
                                         : 0.0;
        cluster.densityScore =
            possibleEdges > 0.0 ? static_cast<double>(internalEdgeCount) / possibleEdges : 0.0;
        cluster.bridgeMass =
            members.empty() ? 0.0 : static_cast<double>(bridgeCount) / members.size();
        cluster.medoid = ClusterRepresentative{.clusterId = clusterId,
                                               .documentHash = documents[medoidIdx].documentHash,
                                               .filePath = documents[medoidIdx].filePath,
                                               .representativeScore = std::max(0.0, medoidScore)};
        cluster.centroidEmbedding = meanEmbedding(documents, members);
        cluster.routingRepresentatives = selectDiverseRoutingRepresentatives(
            documents, members, cluster.centroidEmbedding, config.routingRepresentativeCount);
        cluster.memberDocumentHashes.reserve(members.size());
        for (std::size_t idx : members) {
            cluster.memberDocumentHashes.push_back(documents[idx].documentHash);
        }
        batch.clusters.push_back(std::move(cluster));

        const std::size_t maxDegree = members.size() > 1 ? members.size() - 1 : 0;
        for (std::size_t idx : members) {
            const double bridgeScore =
                maxDegree == 0 ? 0.0 : weightedDegree[idx] / static_cast<double>(maxDegree);
            DocumentTopologyRole role = DocumentTopologyRole::Core;
            if (members.size() == 1) {
                role = DocumentTopologyRole::Outlier;
            } else if (idx == medoidIdx) {
                role = DocumentTopologyRole::Medoid;
            } else if (members.size() > 2 && weightedDegree[idx] >= 2.0) {
                role = DocumentTopologyRole::Bridge;
            }

            batch.memberships.push_back(DocumentClusterMembership{
                .documentHash = documents[idx].documentHash,
                .clusterId = clusterId,
                .parentClusterId = std::nullopt,
                .clusterLevel = 0,
                .persistenceScore = persistence,
                .cohesionScore = cohesion,
                .bridgeScore = bridgeScore,
                .role = role,
                .overlapClusterIds = {},
            });
        }
    }

    std::sort(batch.clusters.begin(), batch.clusters.end(),
              [](const ClusterArtifact& lhs, const ClusterArtifact& rhs) {
                  return lhs.clusterId < rhs.clusterId;
              });
    std::sort(batch.memberships.begin(), batch.memberships.end(),
              [](const DocumentClusterMembership& lhs, const DocumentClusterMembership& rhs) {
                  return lhs.documentHash < rhs.documentHash;
              });

    (void)applyOrthogonalBoundarySpill(documents, config, batch);

    return batch;
}

TopologyDirtyRegion defaultDirtyRegion(std::span<const TopologyDocumentInput> changed) {
    TopologyDirtyRegion region;
    region.requiresWiderRebuild = true;
    region.seedDocumentHashes.reserve(changed.size());
    region.expandedDocumentHashes.reserve(changed.size());
    std::unordered_set<std::string> seen;
    for (const auto& doc : changed) {
        if (doc.documentHash.empty()) {
            continue;
        }
        if (seen.insert(doc.documentHash).second) {
            region.seedDocumentHashes.push_back(doc.documentHash);
            region.expandedDocumentHashes.push_back(doc.documentHash);
        }
    }
    return region;
}

// -------------------- K-means helpers --------------------

double cosineDistance(std::span<const float> a, std::span<const float> b) {
    if (a.size() != b.size() || a.empty()) {
        return 2.0;
    }
    double dot = 0.0;
    double na = 0.0;
    double nb = 0.0;
    for (std::size_t i = 0; i < a.size(); ++i) {
        dot += static_cast<double>(a[i]) * static_cast<double>(b[i]);
        na += static_cast<double>(a[i]) * static_cast<double>(a[i]);
        nb += static_cast<double>(b[i]) * static_cast<double>(b[i]);
    }
    if (na <= 0.0 || nb <= 0.0) {
        return 2.0;
    }
    const double cos = dot / (std::sqrt(na) * std::sqrt(nb));
    return 1.0 - std::clamp(cos, -1.0, 1.0);
}

std::vector<float> normalized(std::vector<float> v) {
    double norm = 0.0;
    for (float x : v) {
        norm += static_cast<double>(x) * static_cast<double>(x);
    }
    if (norm > 0.0) {
        const float inv = static_cast<float>(1.0 / std::sqrt(norm));
        for (auto& x : v) {
            x *= inv;
        }
    }
    return v;
}

std::size_t nearestCentroid(const std::vector<float>& emb,
                            const std::vector<std::vector<float>>& centroids) {
    std::size_t best = 0;
    double bestDist = std::numeric_limits<double>::max();
    for (std::size_t c = 0; c < centroids.size(); ++c) {
        if (centroids[c].empty()) {
            continue;
        }
        const double d = cosineDistance(emb, centroids[c]);
        if (d < bestDist) {
            bestDist = d;
            best = c;
        }
    }
    return best;
}

// Spherical k-means over document embeddings. Deterministic (farthest-first / Gonzalez init, no
// RNG) so topology snapshots reproduce across rebuilds. Returns a per-document cluster assignment;
// documents without a usable embedding become their own singleton clusters.
std::vector<std::int64_t> runKMeans(std::span<const TopologyDocumentInput> documents,
                                    std::size_t requestedK, std::size_t maxIterations) {
    const std::size_t n = documents.size();
    std::vector<std::int64_t> assignment(n, -1);
    if (n == 0) {
        return assignment;
    }

    std::vector<std::size_t> usable;
    usable.reserve(n);
    std::size_t embeddingDim = 0;
    for (std::size_t i = 0; i < n; ++i) {
        if (!documents[i].embedding.empty()) {
            if (embeddingDim == 0) {
                embeddingDim = documents[i].embedding.size();
            }
            if (documents[i].embedding.size() == embeddingDim) {
                usable.push_back(i);
            }
        }
    }
    if (usable.size() < 2 || embeddingDim == 0) {
        std::iota(assignment.begin(), assignment.end(), 0);
        return assignment;
    }

    std::size_t k = requestedK;
    if (k == 0) {
        k = static_cast<std::size_t>(std::round(std::sqrt(static_cast<double>(usable.size()))));
    }
    k = std::clamp<std::size_t>(k, 2, usable.size());

    std::vector<std::vector<float>> centroids;
    centroids.reserve(k);
    std::vector<bool> selected(usable.size(), false);
    centroids.push_back(normalized(documents[usable.front()].embedding));
    selected[0] = true;
    std::vector<double> minDist(usable.size(), std::numeric_limits<double>::max());
    while (centroids.size() < k) {
        std::size_t farthest = usable.size();
        double farthestDist = -1.0;
        for (std::size_t u = 0; u < usable.size(); ++u) {
            if (selected[u]) {
                continue;
            }
            const double d = cosineDistance(documents[usable[u]].embedding, centroids.back());
            if (d < minDist[u]) {
                minDist[u] = d;
            }
            if (minDist[u] > farthestDist) {
                farthestDist = minDist[u];
                farthest = u;
            }
        }
        if (farthest == usable.size()) {
            break;
        }
        selected[farthest] = true;
        centroids.push_back(normalized(documents[usable[farthest]].embedding));
    }
    k = centroids.size();

    const auto centroidOf = [&](const std::vector<std::size_t>& memberUsable) {
        std::vector<std::size_t> docIdx;
        docIdx.reserve(memberUsable.size());
        for (std::size_t u : memberUsable) {
            docIdx.push_back(usable[u]);
        }
        return normalized(meanEmbedding(documents, docIdx));
    };

    std::vector<std::size_t> membership(usable.size(), 0);
    const std::size_t iterations = maxIterations == 0 ? 10 : maxIterations;
    for (std::size_t iter = 0; iter < iterations; ++iter) {
        bool changed = false;
        for (std::size_t u = 0; u < usable.size(); ++u) {
            const std::size_t c = nearestCentroid(documents[usable[u]].embedding, centroids);
            if (c != membership[u]) {
                membership[u] = c;
                changed = true;
            }
        }

        std::vector<std::vector<std::size_t>> members(k);
        for (std::size_t u = 0; u < usable.size(); ++u) {
            members[membership[u]].push_back(u);
        }
        for (std::size_t c = 0; c < k; ++c) {
            if (!members[c].empty()) {
                centroids[c] = centroidOf(members[c]);
            }
        }
        for (std::size_t c = 0; c < k; ++c) {
            if (!members[c].empty()) {
                continue;
            }
            std::size_t worstU = usable.size();
            std::size_t donor = k;
            double worstDist = -1.0;
            for (std::size_t u = 0; u < usable.size(); ++u) {
                const std::size_t mc = membership[u];
                if (members[mc].size() <= 1) {
                    continue;
                }
                const double d = cosineDistance(documents[usable[u]].embedding, centroids[mc]);
                if (d > worstDist) {
                    worstDist = d;
                    worstU = u;
                    donor = mc;
                }
            }
            if (worstU == usable.size()) {
                continue;
            }
            auto& donorMembers = members[donor];
            donorMembers.erase(std::find(donorMembers.begin(), donorMembers.end(), worstU));
            membership[worstU] = c;
            members[c].push_back(worstU);
            centroids[c] = normalized(documents[usable[worstU]].embedding);
            centroids[donor] = centroidOf(donorMembers);
            changed = true;
        }
        if (!changed) {
            break;
        }
    }

    for (std::size_t u = 0; u < usable.size(); ++u) {
        assignment[usable[u]] = static_cast<std::int64_t>(membership[u]);
    }
    std::int64_t singleton = static_cast<std::int64_t>(k);
    for (std::size_t i = 0; i < n; ++i) {
        if (assignment[i] < 0) {
            assignment[i] = singleton++;
        }
    }
    return assignment;
}

} // namespace

// ============================================================================
// LouvainTopologyEngine — Phase H alternative cluster engine
// ============================================================================

namespace {

// Single-pass Louvain Phase 1: greedy node-move modularity optimization.
// adjacency[i] is a list of (neighbor_index, edge_weight) for node i.
// Returns assignment[i] = community id (compact integers starting at 0).
std::vector<std::int64_t>
runLouvain(std::span<const TopologyDocumentInput> documents,
           const std::vector<std::vector<std::pair<std::size_t, float>>>& adjacency,
           std::size_t maxIterations = 10) {
    const std::size_t n = documents.size();
    std::vector<std::int64_t> assignment(n, 0);
    if (n == 0) {
        return assignment;
    }
    // Each node starts in its own community.
    std::iota(assignment.begin(), assignment.end(), std::int64_t{0});

    // Per-node weighted degree (sum of incident edge weights).
    std::vector<double> nodeDegree(n, 0.0);
    double totalWeight = 0.0;
    for (std::size_t i = 0; i < n; ++i) {
        for (const auto& [j, w] : adjacency[i]) {
            nodeDegree[i] += static_cast<double>(w);
        }
        totalWeight += nodeDegree[i];
    }
    // 2m in modularity formulas = totalWeight (each edge counted from both ends).
    const double twoM = totalWeight;
    if (twoM <= 0.0) {
        // No edges → every node is its own community. Standard Louvain
        // convention: collapse all isolated nodes into one community to
        // produce a usable batch. We return the per-node assignment as-is;
        // downstream `buildBatchFromAssignment` will produce singleton clusters
        // which the persistence-aware reward correctly penalizes.
        return assignment;
    }

    // Per-community weighted degree (Σ_tot in modularity formulas).
    std::vector<double> commDegree(n, 0.0);
    for (std::size_t i = 0; i < n; ++i) {
        commDegree[assignment[i]] = nodeDegree[i];
    }

    auto computeDeltaQ = [&](std::size_t node, std::int64_t targetComm,
                             double weightToTarget) -> double {
        const double k = nodeDegree[node];
        const double sigmaTot = commDegree[targetComm];
        // ΔQ = [k_i_in / m] - [Σ_tot * k_i / (2m²)]
        // Simplified for greedy comparison: use raw modularity gain.
        return weightToTarget / twoM - (sigmaTot * k) / (twoM * twoM);
    };

    bool improved = true;
    std::size_t iter = 0;
    while (improved && iter < maxIterations) {
        improved = false;
        ++iter;
        for (std::size_t i = 0; i < n; ++i) {
            const std::int64_t fromComm = assignment[i];
            const double k = nodeDegree[i];

            // Aggregate weights from node i to each neighboring community
            // (including its current community).
            std::unordered_map<std::int64_t, double> weightToComm;
            for (const auto& [j, w] : adjacency[i]) {
                weightToComm[assignment[j]] += static_cast<double>(w);
            }
            const double weightToFrom = weightToComm[fromComm];

            // Removing i from its current community: delta of -computeDeltaQ
            // (without i's self-edge contribution). For modularity-greedy we
            // compare net gain when moving to each candidate.
            std::int64_t bestComm = fromComm;
            double bestGain = 0.0;
            // Tentatively remove i from fromComm.
            commDegree[fromComm] -= k;
            for (const auto& [candComm, weightToCand] : weightToComm) {
                if (candComm == fromComm) {
                    continue;
                }
                const double gain = computeDeltaQ(i, candComm, weightToCand) -
                                    (-computeDeltaQ(i, fromComm, weightToFrom));
                if (gain > bestGain) {
                    bestGain = gain;
                    bestComm = candComm;
                }
            }
            // Apply best move (or restore i to fromComm).
            assignment[i] = bestComm;
            commDegree[bestComm] += k;
            if (bestComm != fromComm) {
                improved = true;
            }
        }
    }

    // Compact community IDs into [0, k) so downstream iteration is clean.
    std::unordered_map<std::int64_t, std::int64_t> remap;
    std::int64_t next = 0;
    for (auto& a : assignment) {
        auto [it, inserted] = remap.emplace(a, next);
        if (inserted) {
            ++next;
        }
        a = it->second;
    }
    return assignment;
}

} // namespace

Result<TopologyArtifactBatch>
LouvainTopologyEngine::buildArtifacts(std::span<const TopologyDocumentInput> documents,
                                      const TopologyBuildConfig& config) {
    const auto ts = nowStamps();
    if (documents.empty()) {
        TopologyArtifactBatch batch;
        batch.snapshotId = makeSnapshotId(ts.unixMillis);
        batch.algorithm = "louvain_v1";
        batch.inputKind = config.inputKind;
        batch.generatedAtUnixSeconds = ts.unixSeconds;
        return batch;
    }

    std::unordered_map<std::string, std::size_t> indexByHash;
    indexByHash.reserve(documents.size());
    for (std::size_t i = 0; i < documents.size(); ++i) {
        if (!documents[i].documentHash.empty()) {
            indexByHash[documents[i].documentHash] = i;
        }
    }
    auto pairWeights = buildPairWeights(documents, indexByHash, config);
    auto adjacency = makeAdjacency(documents.size(), pairWeights);
    auto assignment = runLouvain(documents, adjacency);
    auto batch =
        buildBatchFromAssignment(documents, assignment, pairWeights, "louvain_v1", ts, config);
    batch.inputKind = config.inputKind;
    return batch;
}

Result<TopologyDirtyRegion>
LouvainTopologyEngine::defineDirtyRegion(const TopologyArtifactBatch&,
                                         std::span<const TopologyDocumentInput> changedDocuments,
                                         const TopologyBuildConfig&) const {
    return defaultDirtyRegion(changedDocuments);
}

Result<TopologyArtifactBatch> LouvainTopologyEngine::updateArtifacts(
    const TopologyArtifactBatch&, std::span<const TopologyDocumentInput> changedDocuments,
    const TopologyBuildConfig& config, TopologyUpdateStats* stats) {
    if (stats != nullptr) {
        stats->fallbackFullRebuilds = 1;
    }
    return buildArtifacts(changedDocuments, config);
}

// ============================================================================
// KMeansTopologyEngine — IVF-style balanced coarse quantizer
// ============================================================================

Result<TopologyArtifactBatch>
KMeansTopologyEngine::buildArtifacts(std::span<const TopologyDocumentInput> documents,
                                     const TopologyBuildConfig& config) {
    const auto ts = nowStamps();
    if (documents.empty()) {
        TopologyArtifactBatch batch;
        batch.snapshotId = makeSnapshotId(ts.unixMillis);
        batch.algorithm = "kmeans_v1";
        batch.inputKind = config.inputKind;
        batch.generatedAtUnixSeconds = ts.unixSeconds;
        return batch;
    }

    std::unordered_map<std::string, std::size_t> indexByHash;
    indexByHash.reserve(documents.size());
    for (std::size_t i = 0; i < documents.size(); ++i) {
        if (!documents[i].documentHash.empty()) {
            indexByHash[documents[i].documentHash] = i;
        }
    }
    auto pairWeights = buildPairWeights(documents, indexByHash, config);
    auto assignment = runKMeans(documents, config.kmeansK, config.kmeansMaxIterations);
    auto batch =
        buildBatchFromAssignment(documents, assignment, pairWeights, "kmeans_v1", ts, config);
    batch.inputKind = config.inputKind;
    return batch;
}

Result<TopologyDirtyRegion>
KMeansTopologyEngine::defineDirtyRegion(const TopologyArtifactBatch&,
                                        std::span<const TopologyDocumentInput> changedDocuments,
                                        const TopologyBuildConfig&) const {
    return defaultDirtyRegion(changedDocuments);
}

Result<TopologyArtifactBatch> KMeansTopologyEngine::updateArtifacts(
    const TopologyArtifactBatch&, std::span<const TopologyDocumentInput> changedDocuments,
    const TopologyBuildConfig& config, TopologyUpdateStats* stats) {
    if (stats != nullptr) {
        stats->fallbackFullRebuilds = 1;
    }
    return buildArtifacts(changedDocuments, config);
}

} // namespace yams::topology
