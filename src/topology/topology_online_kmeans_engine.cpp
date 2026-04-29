#include <yams/topology/topology_online_kmeans_engine.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::topology {

namespace {

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

std::string makeSnapshotId(std::uint64_t unixMillis) {
    return "topology-" + std::to_string(unixMillis);
}

std::string makeClusterIdForAnchor(const std::string& anchorHash) {
    return "topology.cluster." + anchorHash;
}

float dot(const std::vector<float>& a, const std::vector<float>& b) {
    if (a.size() != b.size() || a.empty()) {
        return 0.0F;
    }
    double acc = 0.0;
    for (std::size_t i = 0; i < a.size(); ++i) {
        acc += static_cast<double>(a[i]) * static_cast<double>(b[i]);
    }
    return static_cast<float>(acc);
}

float norm(const std::vector<float>& a) {
    if (a.empty()) {
        return 0.0F;
    }
    double acc = 0.0;
    for (const float v : a) {
        acc += static_cast<double>(v) * static_cast<double>(v);
    }
    return static_cast<float>(std::sqrt(acc));
}

float cosineSimilarity(const std::vector<float>& a, const std::vector<float>& b) {
    const float na = norm(a);
    const float nb = norm(b);
    if (na <= 0.0F || nb <= 0.0F) {
        return 0.0F;
    }
    return dot(a, b) / (na * nb);
}

std::size_t resolveK(std::size_t configuredK, std::size_t usableSize) {
    if (configuredK > 0) {
        return std::min(configuredK, usableSize);
    }
    const auto sqrtN = static_cast<std::size_t>(std::sqrt(static_cast<double>(usableSize)));
    std::size_t k = std::max<std::size_t>(64, sqrtN);
    k = std::min<std::size_t>(300, k);
    return std::min(k, usableSize);
}

// KMeans++ seeding: pick first centroid uniformly at random, then each
// subsequent centroid with probability proportional to its squared distance
// to the nearest already-chosen centroid.
std::vector<std::size_t> kmeansPlusPlusSeed(std::span<const TopologyDocumentInput> documents,
                                            const std::vector<std::size_t>& usable, std::size_t k,
                                            std::uint64_t seed) {
    std::vector<std::size_t> chosen;
    chosen.reserve(k);
    if (usable.empty() || k == 0) {
        return chosen;
    }
    std::mt19937_64 rng(seed);
    {
        std::uniform_int_distribution<std::size_t> uni(0, usable.size() - 1);
        chosen.push_back(usable[uni(rng)]);
    }
    std::vector<float> minSq(usable.size(), std::numeric_limits<float>::max());
    while (chosen.size() < k) {
        const std::size_t lastIdx = chosen.back();
        const auto& lastEmb = documents[lastIdx].embedding;
        for (std::size_t i = 0; i < usable.size(); ++i) {
            const auto& emb = documents[usable[i]].embedding;
            const float cs = cosineSimilarity(emb, lastEmb);
            const float dsq = (1.0F - cs) * (1.0F - cs);
            if (dsq < minSq[i]) {
                minSq[i] = dsq;
            }
        }
        double total = 0.0;
        for (const float v : minSq) {
            total += static_cast<double>(v);
        }
        if (total <= 0.0) {
            std::uniform_int_distribution<std::size_t> uni(0, usable.size() - 1);
            chosen.push_back(usable[uni(rng)]);
            continue;
        }
        std::uniform_real_distribution<double> ur(0.0, total);
        double pick = ur(rng);
        std::size_t pickedI = 0;
        for (std::size_t i = 0; i < usable.size(); ++i) {
            pick -= static_cast<double>(minSq[i]);
            if (pick <= 0.0) {
                pickedI = i;
                break;
            }
        }
        chosen.push_back(usable[pickedI]);
    }
    return chosen;
}

std::size_t nearestCentroidIdx(const std::vector<float>& emb,
                               const std::vector<std::vector<float>>& centroids,
                               float* bestCosOut) {
    std::size_t best = 0;
    float bestCos = -1.0F;
    for (std::size_t c = 0; c < centroids.size(); ++c) {
        if (centroids[c].empty()) {
            continue;
        }
        const float cs = cosineSimilarity(emb, centroids[c]);
        if (cs > bestCos) {
            bestCos = cs;
            best = c;
        }
    }
    if (bestCosOut != nullptr) {
        *bestCosOut = bestCos;
    }
    return best;
}

ClusterArtifact buildClusterArtifact(const std::string& clusterId,
                                     const std::vector<float>& centroid,
                                     std::span<const TopologyDocumentInput> documents,
                                     const std::vector<std::size_t>& memberIndices) {
    ClusterArtifact cluster;
    cluster.clusterId = clusterId;
    cluster.level = 0;
    cluster.memberCount = memberIndices.size();
    cluster.persistenceScore = 0.0;
    cluster.cohesionScore = 0.0;
    cluster.bridgeMass = 0.0;
    cluster.centroidEmbedding = centroid;
    cluster.memberDocumentHashes.reserve(memberIndices.size());

    std::size_t medoidLocal = 0;
    float bestMedoidCos = -1.0F;
    double cohesionSum = 0.0;
    for (std::size_t mi = 0; mi < memberIndices.size(); ++mi) {
        const std::size_t idx = memberIndices[mi];
        cluster.memberDocumentHashes.push_back(documents[idx].documentHash);
        const float cs = cosineSimilarity(documents[idx].embedding, centroid);
        cohesionSum += static_cast<double>(cs);
        if (cs > bestMedoidCos) {
            bestMedoidCos = cs;
            medoidLocal = mi;
        }
    }
    if (!memberIndices.empty()) {
        cluster.cohesionScore = cohesionSum / static_cast<double>(memberIndices.size());
        cluster.persistenceScore = static_cast<double>(bestMedoidCos);
        const std::size_t medoidIdx = memberIndices[medoidLocal];
        cluster.medoid =
            ClusterRepresentative{.clusterId = cluster.clusterId,
                                  .documentHash = documents[medoidIdx].documentHash,
                                  .filePath = documents[medoidIdx].filePath,
                                  .representativeScore = static_cast<double>(bestMedoidCos)};
    }
    std::sort(cluster.memberDocumentHashes.begin(), cluster.memberDocumentHashes.end());
    return cluster;
}

void emitMemberships(const ClusterArtifact& cluster,
                     std::span<const TopologyDocumentInput> documents,
                     const std::vector<std::size_t>& memberIndices,
                     std::vector<DocumentClusterMembership>& out) {
    for (std::size_t idx : memberIndices) {
        DocumentTopologyRole role = DocumentTopologyRole::Core;
        if (cluster.medoid.has_value() &&
            cluster.medoid->documentHash == documents[idx].documentHash) {
            role = DocumentTopologyRole::Medoid;
        } else if (memberIndices.size() == 1) {
            role = DocumentTopologyRole::Outlier;
        }
        out.push_back(DocumentClusterMembership{.documentHash = documents[idx].documentHash,
                                                .clusterId = cluster.clusterId,
                                                .parentClusterId = std::nullopt,
                                                .clusterLevel = 0,
                                                .persistenceScore = cluster.persistenceScore,
                                                .cohesionScore = cluster.cohesionScore,
                                                .bridgeScore = 0.0,
                                                .role = role,
                                                .overlapClusterIds = {}});
    }
}

TopologyDirtyRegion changedRegion(std::span<const TopologyDocumentInput> changed) {
    TopologyDirtyRegion region;
    region.requiresWiderRebuild = false;
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

} // namespace

Result<TopologyArtifactBatch>
OnlineKMeansTopologyEngine::buildArtifacts(std::span<const TopologyDocumentInput> documents,
                                           const TopologyBuildConfig& config) {
    const auto ts = nowStamps();
    TopologyArtifactBatch batch;
    batch.snapshotId = makeSnapshotId(ts.unixMillis);
    batch.algorithm = "kmeans_online_v1";
    batch.inputKind = config.inputKind;
    batch.generatedAtUnixSeconds = ts.unixSeconds;
    if (documents.empty()) {
        return batch;
    }

    std::vector<std::size_t> usable;
    usable.reserve(documents.size());
    for (std::size_t i = 0; i < documents.size(); ++i) {
        if (!documents[i].documentHash.empty() && !documents[i].embedding.empty()) {
            usable.push_back(i);
        }
    }
    if (usable.empty()) {
        return batch;
    }

    const std::size_t dim = documents[usable.front()].embedding.size();
    const std::size_t k = resolveK(config.kmeansK, usable.size());
    if (k == 0) {
        return batch;
    }

    auto seedIndices = kmeansPlusPlusSeed(documents, usable, k, ts.unixMillis);
    std::vector<std::vector<float>> centroids(k);
    for (std::size_t c = 0; c < k; ++c) {
        centroids[c] = documents[seedIndices[c]].embedding;
    }

    std::vector<std::size_t> assignment(usable.size(), 0);
    const std::size_t maxIter = config.kmeansMaxIterations == 0 ? 10 : config.kmeansMaxIterations;
    for (std::size_t iter = 0; iter < maxIter; ++iter) {
        bool changed = false;
        for (std::size_t ui = 0; ui < usable.size(); ++ui) {
            const std::size_t newC =
                nearestCentroidIdx(documents[usable[ui]].embedding, centroids, nullptr);
            if (newC != assignment[ui]) {
                assignment[ui] = newC;
                changed = true;
            }
        }
        std::vector<std::vector<double>> sums(k, std::vector<double>(dim, 0.0));
        std::vector<std::size_t> counts(k, 0);
        for (std::size_t ui = 0; ui < usable.size(); ++ui) {
            const std::size_t c = assignment[ui];
            ++counts[c];
            const auto& emb = documents[usable[ui]].embedding;
            for (std::size_t d = 0; d < dim; ++d) {
                sums[c][d] += static_cast<double>(emb[d]);
            }
        }
        for (std::size_t c = 0; c < k; ++c) {
            if (counts[c] == 0) {
                continue;
            }
            const auto inv = 1.0 / static_cast<double>(counts[c]);
            for (std::size_t d = 0; d < dim; ++d) {
                centroids[c][d] = static_cast<float>(sums[c][d] * inv);
            }
        }
        if (!changed) {
            break;
        }
    }

    std::vector<std::vector<std::size_t>> buckets(k);
    for (std::size_t ui = 0; ui < usable.size(); ++ui) {
        buckets[assignment[ui]].push_back(usable[ui]);
    }

    for (std::size_t c = 0; c < k; ++c) {
        if (buckets[c].empty()) {
            continue;
        }
        std::sort(buckets[c].begin(), buckets[c].end(),
                  [&documents](std::size_t lhs, std::size_t rhs) {
                      return documents[lhs].documentHash < documents[rhs].documentHash;
                  });
        const std::string clusterId =
            makeClusterIdForAnchor(documents[buckets[c].front()].documentHash);
        auto cluster = buildClusterArtifact(clusterId, centroids[c], documents, buckets[c]);
        emitMemberships(cluster, documents, buckets[c], batch.memberships);
        batch.clusters.push_back(std::move(cluster));
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

Result<TopologyDirtyRegion> OnlineKMeansTopologyEngine::defineDirtyRegion(
    const TopologyArtifactBatch&, std::span<const TopologyDocumentInput> changedDocuments,
    const TopologyBuildConfig&) const {
    return changedRegion(changedDocuments);
}

Result<TopologyArtifactBatch> OnlineKMeansTopologyEngine::updateArtifacts(
    const TopologyArtifactBatch& existing, std::span<const TopologyDocumentInput> changedDocuments,
    const TopologyBuildConfig& config, TopologyUpdateStats* stats) {
    if (existing.clusters.empty()) {
        // Bootstrap: no prior buckets, treat as full build on the changed docs.
        if (stats != nullptr) {
            stats->fallbackFullRebuilds = 1;
            stats->documentsProcessed = changedDocuments.size();
        }
        return buildArtifacts(changedDocuments, config);
    }

    TopologyArtifactBatch updated = existing;
    const auto ts = nowStamps();
    updated.snapshotId = makeSnapshotId(ts.unixMillis);
    updated.algorithm = "kmeans_online_v1";
    updated.inputKind = config.inputKind;
    updated.generatedAtUnixSeconds = ts.unixSeconds;
    updated.topologyEpoch = existing.topologyEpoch + 1;

    std::unordered_map<std::string, std::size_t> clusterIndex;
    clusterIndex.reserve(updated.clusters.size());
    for (std::size_t c = 0; c < updated.clusters.size(); ++c) {
        clusterIndex.emplace(updated.clusters[c].clusterId, c);
    }
    std::unordered_map<std::string, std::size_t> membershipIndex;
    membershipIndex.reserve(updated.memberships.size());
    for (std::size_t m = 0; m < updated.memberships.size(); ++m) {
        membershipIndex.emplace(updated.memberships[m].documentHash, m);
    }

    std::size_t newClusters = 0;
    std::size_t reassigned = 0;
    std::size_t processed = 0;

    for (const auto& doc : changedDocuments) {
        if (doc.documentHash.empty() || doc.embedding.empty()) {
            continue;
        }
        ++processed;

        // Find nearest existing centroid.
        std::size_t bestC = 0;
        float bestCos = -1.0F;
        for (std::size_t c = 0; c < updated.clusters.size(); ++c) {
            if (updated.clusters[c].centroidEmbedding.empty()) {
                continue;
            }
            const float cs = cosineSimilarity(doc.embedding, updated.clusters[c].centroidEmbedding);
            if (cs > bestCos) {
                bestCos = cs;
                bestC = c;
            }
        }

        const float threshold = std::clamp(config.minSimilarityToJoin, 0.0F, 1.0F);
        if (updated.clusters.empty() || bestCos < threshold) {
            // Spawn new bucket.
            ClusterArtifact c;
            c.clusterId = makeClusterIdForAnchor(doc.documentHash);
            c.level = 0;
            c.memberCount = 1;
            c.persistenceScore = 1.0;
            c.cohesionScore = 1.0;
            c.centroidEmbedding = doc.embedding;
            c.memberDocumentHashes = {doc.documentHash};
            c.medoid = ClusterRepresentative{.clusterId = c.clusterId,
                                             .documentHash = doc.documentHash,
                                             .filePath = doc.filePath,
                                             .representativeScore = 1.0};
            const std::size_t newIdx = updated.clusters.size();
            clusterIndex.emplace(c.clusterId, newIdx);
            updated.clusters.push_back(std::move(c));

            DocumentClusterMembership mb;
            mb.documentHash = doc.documentHash;
            mb.clusterId = updated.clusters[newIdx].clusterId;
            mb.role = DocumentTopologyRole::Medoid;
            mb.persistenceScore = 1.0;
            mb.cohesionScore = 1.0;
            mb.bridgeScore = 0.0;
            membershipIndex[doc.documentHash] = updated.memberships.size();
            updated.memberships.push_back(std::move(mb));
            ++newClusters;
            continue;
        }

        // Assign to nearest cluster, update centroid via running mean.
        auto& cluster = updated.clusters[bestC];
        const auto n = cluster.memberCount;
        if (cluster.centroidEmbedding.size() != doc.embedding.size()) {
            // Dim mismatch — skip rather than corrupt.
            continue;
        }
        const auto denom = static_cast<double>(n + 1);
        for (std::size_t d = 0; d < cluster.centroidEmbedding.size(); ++d) {
            cluster.centroidEmbedding[d] = static_cast<float>(
                (static_cast<double>(cluster.centroidEmbedding[d]) * static_cast<double>(n) +
                 static_cast<double>(doc.embedding[d])) /
                denom);
        }
        cluster.memberCount = n + 1;
        cluster.memberDocumentHashes.push_back(doc.documentHash);
        // Keep memberDocumentHashes sorted for stable comparison.
        const auto insertPos =
            std::lower_bound(cluster.memberDocumentHashes.begin(),
                             cluster.memberDocumentHashes.end() - 1, doc.documentHash);
        std::rotate(insertPos, cluster.memberDocumentHashes.end() - 1,
                    cluster.memberDocumentHashes.end());

        // Upsert membership.
        if (auto it = membershipIndex.find(doc.documentHash); it != membershipIndex.end()) {
            auto& mb = updated.memberships[it->second];
            mb.clusterId = cluster.clusterId;
            mb.persistenceScore = cluster.persistenceScore;
            mb.cohesionScore = cluster.cohesionScore;
            mb.role = DocumentTopologyRole::Core;
            ++reassigned;
        } else {
            DocumentClusterMembership mb;
            mb.documentHash = doc.documentHash;
            mb.clusterId = cluster.clusterId;
            mb.role = DocumentTopologyRole::Core;
            mb.persistenceScore = cluster.persistenceScore;
            mb.cohesionScore = cluster.cohesionScore;
            membershipIndex[doc.documentHash] = updated.memberships.size();
            updated.memberships.push_back(std::move(mb));
        }
    }

    if (stats != nullptr) {
        stats->documentsProcessed = processed;
        stats->clustersCreated = newClusters;
        stats->clustersUpdated = processed > newClusters ? processed - newClusters : 0;
        stats->membershipsUpdated = reassigned;
        stats->fallbackFullRebuilds = 0;
    }

    return updated;
}

} // namespace yams::topology
