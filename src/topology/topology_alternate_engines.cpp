#include <yams/topology/topology_alternate_engines.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstddef>
#include <limits>
#include <map>
#include <numeric>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::topology {

namespace {

using PairKey = std::pair<std::size_t, std::size_t>;

std::string makeSnapshotId(std::uint64_t unixMillis) {
    return "topology-" + std::to_string(unixMillis);
}

std::string makeClusterId(const std::string& anchorHash) {
    return "topology.cluster." + anchorHash;
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

std::map<PairKey, float>
buildPairWeights(std::span<const TopologyDocumentInput> documents,
                 const std::unordered_map<std::string, std::size_t>& indexByHash,
                 const TopologyBuildConfig& config) {
    std::map<PairKey, float> pairWeights;
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
makeAdjacency(std::size_t n, const std::map<PairKey, float>& pairWeights) {
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
                                               const std::map<PairKey, float>& pairWeights,
                                               std::string algorithm, const TimeStamps& ts) {
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
            }
            if (members.size() > 2 && degree >= 2) {
                ++bridgeCount;
            }
        }

        for (const auto& [key, weight] : pairWeights) {
            if (memberSet.contains(key.first) && memberSet.contains(key.second)) {
                cohesion += weight;
                persistence =
                    internalEdgeCount == 0 ? weight : std::min<double>(persistence, weight);
                ++internalEdgeCount;
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
        cluster.bridgeMass =
            members.empty() ? 0.0 : static_cast<double>(bridgeCount) / members.size();
        cluster.medoid = ClusterRepresentative{.clusterId = clusterId,
                                               .documentHash = documents[medoidIdx].documentHash,
                                               .filePath = documents[medoidIdx].filePath,
                                               .representativeScore = std::max(0.0, medoidScore)};
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

// -------------------- Louvain --------------------

std::vector<std::int64_t> runLouvain(std::span<const TopologyDocumentInput> documents,
                                     const std::map<PairKey, float>& pairWeights) {
    const std::size_t n = documents.size();
    std::vector<std::int64_t> community(n);
    std::iota(community.begin(), community.end(), 0);

    if (pairWeights.empty() || n == 0) {
        return community;
    }

    const auto adjacency = makeAdjacency(n, pairWeights);

    std::vector<double> k(n, 0.0);
    for (const auto& [key, weight] : pairWeights) {
        k[key.first] += weight;
        k[key.second] += weight;
    }
    double twoM = std::accumulate(k.begin(), k.end(), 0.0);
    if (twoM <= 0.0) {
        return community;
    }

    std::unordered_map<std::int64_t, double> commWeightedDeg;
    commWeightedDeg.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        commWeightedDeg[community[i]] = k[i];
    }

    constexpr int kMaxIterations = 16;
    bool improvement = true;
    std::vector<std::size_t> order(n);
    std::iota(order.begin(), order.end(), 0);
    std::mt19937_64 rng{0xC0FFEEULL};

    int iteration = 0;
    while (improvement && iteration < kMaxIterations) {
        improvement = false;
        std::shuffle(order.begin(), order.end(), rng);
        for (std::size_t idx : order) {
            const std::int64_t currentComm = community[idx];
            std::unordered_map<std::int64_t, double> weightToComm;
            for (const auto& [nbr, w] : adjacency[idx]) {
                weightToComm[community[nbr]] += w;
            }
            // Remove self from its community for tentative evaluation.
            commWeightedDeg[currentComm] -= k[idx];
            const double selfToCurrent = weightToComm[currentComm];

            std::int64_t bestComm = currentComm;
            double bestGain = 0.0;
            for (const auto& [cand, edgeWeight] : weightToComm) {
                const double sigmaTot = commWeightedDeg[cand];
                const double gain = edgeWeight - (sigmaTot * k[idx]) / twoM;
                // Compare against staying (which yields selfToCurrent - ...)
                const double keep = selfToCurrent - (commWeightedDeg[currentComm] * k[idx]) / twoM;
                const double delta = gain - keep;
                if (delta > bestGain + 1e-12 ||
                    (std::abs(delta - bestGain) < 1e-12 && cand < bestComm)) {
                    bestGain = delta;
                    bestComm = cand;
                }
            }
            community[idx] = bestComm;
            commWeightedDeg[bestComm] += k[idx];
            if (bestComm != currentComm) {
                improvement = true;
            }
        }
        ++iteration;
    }

    // Assign singletons (no edges) to their own community (already true via iota).
    // Normalize community ids to be deterministic (smallest member index per community).
    std::unordered_map<std::int64_t, std::int64_t> canon;
    for (std::size_t i = 0; i < n; ++i) {
        const auto c = community[i];
        auto it = canon.find(c);
        if (it == canon.end()) {
            canon.emplace(c, static_cast<std::int64_t>(i));
        } else if (static_cast<std::int64_t>(i) < it->second) {
            it->second = static_cast<std::int64_t>(i);
        }
    }
    for (auto& c : community) {
        c = canon[c];
    }
    return community;
}

// -------------------- Label Propagation --------------------

std::vector<std::int64_t> runLabelPropagation(std::span<const TopologyDocumentInput> documents,
                                              const std::map<PairKey, float>& pairWeights) {
    const std::size_t n = documents.size();
    std::vector<std::int64_t> labels(n);
    std::iota(labels.begin(), labels.end(), 0);
    if (pairWeights.empty() || n == 0) {
        return labels;
    }

    const auto adjacency = makeAdjacency(n, pairWeights);

    constexpr int kMaxIterations = 20;
    std::vector<std::size_t> order(n);
    std::iota(order.begin(), order.end(), 0);
    std::mt19937_64 rng{0xF00DCAFEULL};

    for (int iter = 0; iter < kMaxIterations; ++iter) {
        bool changed = false;
        std::shuffle(order.begin(), order.end(), rng);
        for (std::size_t idx : order) {
            if (adjacency[idx].empty()) {
                continue;
            }
            std::unordered_map<std::int64_t, double> weightByLabel;
            for (const auto& [nbr, w] : adjacency[idx]) {
                weightByLabel[labels[nbr]] += w;
            }
            std::int64_t bestLabel = labels[idx];
            double bestWeight = -1.0;
            for (const auto& [label, wsum] : weightByLabel) {
                if (wsum > bestWeight + 1e-12 ||
                    (std::abs(wsum - bestWeight) < 1e-12 && label < bestLabel)) {
                    bestWeight = wsum;
                    bestLabel = label;
                }
            }
            if (bestLabel != labels[idx]) {
                labels[idx] = bestLabel;
                changed = true;
            }
        }
        if (!changed) {
            break;
        }
    }

    std::unordered_map<std::int64_t, std::int64_t> canon;
    for (std::size_t i = 0; i < n; ++i) {
        const auto c = labels[i];
        auto it = canon.find(c);
        if (it == canon.end()) {
            canon.emplace(c, static_cast<std::int64_t>(i));
        } else if (static_cast<std::int64_t>(i) < it->second) {
            it->second = static_cast<std::int64_t>(i);
        }
    }
    for (auto& c : labels) {
        c = canon[c];
    }
    return labels;
}

// -------------------- K-Means on embeddings --------------------

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

std::vector<std::int64_t> runKMeansEmbedding(std::span<const TopologyDocumentInput> documents,
                                             std::size_t requestedK) {
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
    if (usable.empty() || embeddingDim == 0) {
        std::iota(assignment.begin(), assignment.end(), 0);
        return assignment;
    }

    std::size_t k = requestedK;
    if (k == 0) {
        k = static_cast<std::size_t>(
            std::max<double>(1.0, std::round(std::sqrt(static_cast<double>(usable.size())))));
    }
    k = std::min(k, usable.size());
    if (k < 2) {
        for (std::size_t idx : usable) {
            assignment[idx] = 0;
        }
        // Docs without embeddings become singleton outliers with unique ids.
        std::int64_t nextId = 1;
        for (std::size_t i = 0; i < n; ++i) {
            if (assignment[i] < 0) {
                assignment[i] = nextId++;
            }
        }
        return assignment;
    }

    std::mt19937_64 rng{0xBABEF00DULL};
    // kmeans++ initialization
    std::vector<std::size_t> centers;
    centers.reserve(k);
    std::uniform_int_distribution<std::size_t> firstPick(0, usable.size() - 1);
    centers.push_back(usable[firstPick(rng)]);
    std::vector<double> distances(usable.size(), std::numeric_limits<double>::infinity());
    while (centers.size() < k) {
        const auto& latestEmbedding = documents[centers.back()].embedding;
        for (std::size_t u = 0; u < usable.size(); ++u) {
            const double d = cosineDistance(documents[usable[u]].embedding, latestEmbedding);
            if (d < distances[u]) {
                distances[u] = d;
            }
        }
        double total = 0.0;
        for (double d : distances) {
            total += d * d;
        }
        if (total <= 0.0) {
            // All remaining points identical; pick any unchosen.
            for (std::size_t u = 0; u < usable.size(); ++u) {
                if (std::find(centers.begin(), centers.end(), usable[u]) == centers.end()) {
                    centers.push_back(usable[u]);
                    break;
                }
            }
            continue;
        }
        std::uniform_real_distribution<double> pick(0.0, total);
        double r = pick(rng);
        std::size_t chosenU = usable.size() - 1;
        for (std::size_t u = 0; u < usable.size(); ++u) {
            r -= distances[u] * distances[u];
            if (r <= 0.0) {
                chosenU = u;
                break;
            }
        }
        centers.push_back(usable[chosenU]);
    }

    std::vector<std::vector<float>> centroids(k);
    for (std::size_t c = 0; c < k; ++c) {
        centroids[c] = documents[centers[c]].embedding;
    }

    constexpr int kMaxIterations = 16;
    std::vector<std::int64_t> usableAssign(usable.size(), -1);
    for (int iter = 0; iter < kMaxIterations; ++iter) {
        bool changed = false;
        for (std::size_t u = 0; u < usable.size(); ++u) {
            const auto& emb = documents[usable[u]].embedding;
            double bestDist = std::numeric_limits<double>::infinity();
            std::int64_t bestCluster = 0;
            for (std::size_t c = 0; c < k; ++c) {
                const double d = cosineDistance(emb, centroids[c]);
                if (d < bestDist) {
                    bestDist = d;
                    bestCluster = static_cast<std::int64_t>(c);
                }
            }
            if (usableAssign[u] != bestCluster) {
                usableAssign[u] = bestCluster;
                changed = true;
            }
        }
        if (!changed && iter > 0) {
            break;
        }
        // Update centroids.
        std::vector<std::vector<double>> sums(k, std::vector<double>(embeddingDim, 0.0));
        std::vector<std::size_t> counts(k, 0);
        for (std::size_t u = 0; u < usable.size(); ++u) {
            const auto c = static_cast<std::size_t>(usableAssign[u]);
            const auto& emb = documents[usable[u]].embedding;
            for (std::size_t d = 0; d < embeddingDim; ++d) {
                sums[c][d] += static_cast<double>(emb[d]);
            }
            ++counts[c];
        }
        for (std::size_t c = 0; c < k; ++c) {
            if (counts[c] == 0) {
                continue;
            }
            for (std::size_t d = 0; d < embeddingDim; ++d) {
                centroids[c][d] = static_cast<float>(sums[c][d] / static_cast<double>(counts[c]));
            }
        }
    }

    // Canonicalize: cluster id = smallest usable index per cluster.
    std::unordered_map<std::int64_t, std::int64_t> canon;
    for (std::size_t u = 0; u < usable.size(); ++u) {
        const auto c = usableAssign[u];
        const auto docIdx = static_cast<std::int64_t>(usable[u]);
        auto it = canon.find(c);
        if (it == canon.end()) {
            canon.emplace(c, docIdx);
        } else if (docIdx < it->second) {
            it->second = docIdx;
        }
    }
    for (std::size_t u = 0; u < usable.size(); ++u) {
        assignment[usable[u]] = canon[usableAssign[u]];
    }
    // Docs without embeddings: assign unique ids (singletons).
    std::int64_t orphanId = static_cast<std::int64_t>(n);
    for (std::size_t i = 0; i < n; ++i) {
        if (assignment[i] < 0) {
            assignment[i] = orphanId++;
        }
    }
    return assignment;
}

} // namespace

// ============================================================================
// LouvainTopologyEngine
// ============================================================================

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
    auto assignment = runLouvain(documents, pairWeights);
    auto batch = buildBatchFromAssignment(documents, assignment, pairWeights, "louvain_v1", ts);
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
// LabelPropagationTopologyEngine
// ============================================================================

Result<TopologyArtifactBatch>
LabelPropagationTopologyEngine::buildArtifacts(std::span<const TopologyDocumentInput> documents,
                                               const TopologyBuildConfig& config) {
    const auto ts = nowStamps();
    if (documents.empty()) {
        TopologyArtifactBatch batch;
        batch.snapshotId = makeSnapshotId(ts.unixMillis);
        batch.algorithm = "label_propagation_v1";
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
    auto assignment = runLabelPropagation(documents, pairWeights);
    auto batch =
        buildBatchFromAssignment(documents, assignment, pairWeights, "label_propagation_v1", ts);
    batch.inputKind = config.inputKind;
    return batch;
}

Result<TopologyDirtyRegion> LabelPropagationTopologyEngine::defineDirtyRegion(
    const TopologyArtifactBatch&, std::span<const TopologyDocumentInput> changedDocuments,
    const TopologyBuildConfig&) const {
    return defaultDirtyRegion(changedDocuments);
}

Result<TopologyArtifactBatch> LabelPropagationTopologyEngine::updateArtifacts(
    const TopologyArtifactBatch&, std::span<const TopologyDocumentInput> changedDocuments,
    const TopologyBuildConfig& config, TopologyUpdateStats* stats) {
    if (stats != nullptr) {
        stats->fallbackFullRebuilds = 1;
    }
    return buildArtifacts(changedDocuments, config);
}

// ============================================================================
// KMeansEmbeddingTopologyEngine
// ============================================================================

Result<TopologyArtifactBatch>
KMeansEmbeddingTopologyEngine::buildArtifacts(std::span<const TopologyDocumentInput> documents,
                                              const TopologyBuildConfig& config) {
    const auto ts = nowStamps();
    if (documents.empty()) {
        TopologyArtifactBatch batch;
        batch.snapshotId = makeSnapshotId(ts.unixMillis);
        batch.algorithm = "kmeans_embedding_v1";
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
    // pairWeights still computed so cohesion/persistence metrics populate
    // when the neighbor graph is available, even though the partition itself
    // is embedding-driven. This mirrors the baseline's metric semantics.
    auto pairWeights = buildPairWeights(documents, indexByHash, config);
    auto assignment = runKMeansEmbedding(documents, config.kmeansK);
    auto batch =
        buildBatchFromAssignment(documents, assignment, pairWeights, "kmeans_embedding_v1", ts);
    batch.inputKind = config.inputKind;
    return batch;
}

Result<TopologyDirtyRegion> KMeansEmbeddingTopologyEngine::defineDirtyRegion(
    const TopologyArtifactBatch&, std::span<const TopologyDocumentInput> changedDocuments,
    const TopologyBuildConfig&) const {
    return defaultDirtyRegion(changedDocuments);
}

Result<TopologyArtifactBatch> KMeansEmbeddingTopologyEngine::updateArtifacts(
    const TopologyArtifactBatch&, std::span<const TopologyDocumentInput> changedDocuments,
    const TopologyBuildConfig& config, TopologyUpdateStats* stats) {
    if (stats != nullptr) {
        stats->fallbackFullRebuilds = 1;
    }
    return buildArtifacts(changedDocuments, config);
}

} // namespace yams::topology
