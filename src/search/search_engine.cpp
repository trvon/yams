#include <yams/search/search_engine.h>

#include <spdlog/spdlog.h>
#include <yams/core/cpp23_features.hpp>
#include <yams/core/magic_numbers.hpp>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/search/graph_expansion.h>
#include <yams/search/kg_scorer.h>
#include <yams/search/kg_scorer_simple.h>
#include <yams/search/query_expansion.h>
#include <yams/search/query_router.h>
#include <yams/search/query_text_utils.h>
#include <yams/search/search_execution_context.h>
#include <yams/search/search_tracing.h>
#include <yams/search/search_tuner.h>
#include <yams/search/tuning_features.h>
#include <yams/search/tuning_pipeline.h>
#include <yams/search/turboquant_packed_reranker.h>
#include <yams/search/vector_reranker.h>
#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_metadata_store.h>
#include <yams/vector/compressed_ann.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <future>
#include <limits>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>

#include <nlohmann/json.hpp>
#include <boost/asio/post.hpp>

#include "yams/profiling.h"
#include <yams/app/services/simd_memmem.hpp>

#if YAMS_HAS_FLAT_MAP
#include <flat_map>
#endif

#if YAMS_HAS_RANGES
#include <ranges>
#endif

namespace yams::search {
namespace compat {

#if YAMS_HAS_FLAT_MAP
template <typename Key, typename Value, typename Compare = std::less<Key>,
          typename KeyContainer = std::vector<Key>, typename MappedContainer = std::vector<Value>>
using flat_map = std::flat_map<Key, Value, Compare, KeyContainer, MappedContainer>;

template <typename Map> inline void reserve_if_needed(Map&, size_t) {
    // std::flat_map doesn't have reserve(), do nothing
}
#else
template <typename Key, typename Value, typename Compare = std::less<Key>>
using flat_map = std::unordered_map<Key, Value>;

template <typename Map> inline void reserve_if_needed(Map& m, size_t n) {
    m.reserve(n);
}
#endif

} // namespace compat

namespace {

using json = nlohmann::json;

std::unordered_set<std::string> buildTopologyWeakQueryCandidates(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore, std::string_view query,
    const std::unordered_set<std::string>& seedDocumentHashes,
    const std::vector<std::string>& overlayDocumentHashes, const SearchEngineConfig& config,
    std::uint64_t expectedTopologyEpoch, bool* epochMismatch) {
    std::unordered_set<std::string> routed;
    if (epochMismatch) {
        *epochMismatch = false;
    }
    if (!metadataRepo || !kgStore || seedDocumentHashes.empty() ||
        !config.enableTopologyWeakQueryRouting || config.topologyWeakQueryMaxClusters == 0 ||
        config.topologyWeakQueryMaxDocs == 0) {
        return routed;
    }

    auto metadataIface =
        std::static_pointer_cast<yams::metadata::IMetadataRepository>(metadataRepo);
    yams::topology::MetadataKgTopologyArtifactStore store(std::move(metadataIface), kgStore);
    auto latestResult = store.loadLatest();
    if (!latestResult || !latestResult.value().has_value()) {
        return routed;
    }

    // Fail-soft: if the freshness snapshot reported a different epoch than the artifacts
    // we just loaded, the topology state drifted between request dispatch and routing.
    // Skip the topology boost for this request and let the caller record it in stats.
    if (expectedTopologyEpoch != 0 && latestResult.value()->topologyEpoch != 0 &&
        latestResult.value()->topologyEpoch != expectedTopologyEpoch) {
        if (epochMismatch) {
            *epochMismatch = true;
        }
        spdlog::debug("Topology epoch mismatch: expected={}, artifacts={} — skipping weak-query "
                      "topology boost",
                      expectedTopologyEpoch, latestResult.value()->topologyEpoch);
        return routed;
    }

    yams::topology::StableClusterTopologyRouter router;
    yams::topology::TopologyRouteRequest routeRequest;
    routeRequest.queryText = std::string(query);
    routeRequest.seedDocumentHashes.assign(seedDocumentHashes.begin(), seedDocumentHashes.end());
    routeRequest.limit = config.topologyWeakQueryMaxClusters;
    routeRequest.preferStableClusters = true;
    routeRequest.weakQueryOnly = true;

    auto routesResult = router.route(routeRequest, *latestResult.value());
    if (!routesResult) {
        return routed;
    }

    std::unordered_map<std::string, const yams::topology::ClusterArtifact*> clustersById;
    clustersById.reserve(latestResult.value()->clusters.size());
    for (const auto& cluster : latestResult.value()->clusters) {
        clustersById.emplace(cluster.clusterId, &cluster);
    }

    auto appendClusterDocuments = [&](const yams::topology::ClusterArtifact& cluster) {
        if (cluster.medoid.has_value() && !cluster.medoid->documentHash.empty()) {
            routed.insert(cluster.medoid->documentHash);
        }
        for (const auto& documentHash : cluster.memberDocumentHashes) {
            if (routed.size() >= config.topologyWeakQueryMaxDocs) {
                break;
            }
            routed.insert(documentHash);
        }
    };

    for (const auto& route : routesResult.value()) {
        auto clusterIt = clustersById.find(route.clusterId);
        if (clusterIt == clustersById.end()) {
            continue;
        }
        appendClusterDocuments(*clusterIt->second);
        if (routed.size() >= config.topologyWeakQueryMaxDocs) {
            break;
        }
    }

    if (!overlayDocumentHashes.empty() && routed.size() < config.topologyWeakQueryMaxDocs) {
        auto overlayMembershipsResult = store.loadMemberships(overlayDocumentHashes);
        if (overlayMembershipsResult) {
            std::unordered_set<std::string> overlayClusters;
            for (const auto& membership : overlayMembershipsResult.value()) {
                if (!membership.clusterId.empty()) {
                    overlayClusters.insert(membership.clusterId);
                }
            }
            for (const auto& clusterId : overlayClusters) {
                auto clusterIt = clustersById.find(clusterId);
                if (clusterIt == clustersById.end()) {
                    continue;
                }
                appendClusterDocuments(*clusterIt->second);
                if (routed.size() >= config.topologyWeakQueryMaxDocs) {
                    break;
                }
            }
        }
    }

    return routed;
}

std::vector<ComponentResult> buildTopologyWeakQueryComponentResults(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore,
    const std::unordered_set<std::string>& routedDocumentHashes, const SearchEngineConfig& config) {
    std::vector<ComponentResult> results;
    if (!metadataRepo || routedDocumentHashes.empty()) {
        return results;
    }

    std::unordered_map<std::string, yams::topology::DocumentClusterMembership> membershipByHash;
    if (kgStore) {
        auto metadataIface =
            std::static_pointer_cast<yams::metadata::IMetadataRepository>(metadataRepo);
        yams::topology::MetadataKgTopologyArtifactStore store(std::move(metadataIface), kgStore);
        std::vector<std::string> routedHashes(routedDocumentHashes.begin(),
                                              routedDocumentHashes.end());
        auto membershipsResult = store.loadMemberships(routedHashes);
        if (membershipsResult) {
            membershipByHash.reserve(membershipsResult.value().size());
            for (auto& membership : membershipsResult.value()) {
                membershipByHash.emplace(membership.documentHash, std::move(membership));
            }
        }
    }

    yams::metadata::DocumentQueryOptions options;
    options.limit = static_cast<int>(routedDocumentHashes.size());
    options.pathsOnly = true;

    auto docsResult = metadataRepo->queryDocuments(options);
    if (!docsResult) {
        return results;
    }

    results.reserve(routedDocumentHashes.size());
    size_t rank = 0;
    for (const auto& doc : docsResult.value()) {
        if (!routedDocumentHashes.contains(doc.sha256Hash)) {
            continue;
        }

        ComponentResult cr;
        cr.documentHash = doc.sha256Hash;
        cr.filePath = doc.filePath;
        cr.score = std::clamp(config.graphExpansionFtsPenalty * 0.85f, 0.0f, 1.0f);
        cr.source = ComponentResult::Source::GraphText;
        cr.rank = rank++;
        cr.snippet = std::optional<std::string>(doc.filePath);
        cr.debugInfo["topology_routed"] = "true";
        if (auto membershipIt = membershipByHash.find(doc.sha256Hash);
            membershipIt != membershipByHash.end()) {
            const auto& membership = membershipIt->second;
            cr.debugInfo["topology_cluster_id"] = membership.clusterId;
            switch (membership.role) {
                case yams::topology::DocumentTopologyRole::Medoid:
                    cr.debugInfo["topology_role"] = "medoid";
                    cr.score = std::clamp(cr.score + 0.05f, 0.0f, 1.0f);
                    break;
                case yams::topology::DocumentTopologyRole::Bridge:
                    cr.debugInfo["topology_role"] = "bridge";
                    cr.score = std::clamp(cr.score + 0.03f, 0.0f, 1.0f);
                    break;
                case yams::topology::DocumentTopologyRole::Outlier:
                    cr.debugInfo["topology_role"] = "outlier";
                    break;
                case yams::topology::DocumentTopologyRole::Core:
                    cr.debugInfo["topology_role"] = "core";
                    break;
            }
            cr.debugInfo["topology_persistence"] =
                fmt::format("{:.4f}", membership.persistenceScore);
            cr.debugInfo["topology_cohesion"] = fmt::format("{:.4f}", membership.cohesionScore);
            cr.debugInfo["topology_bridge_score"] = fmt::format("{:.4f}", membership.bridgeScore);
        }
        results.push_back(std::move(cr));
        if (results.size() >= config.topologyWeakQueryMaxDocs) {
            break;
        }
    }

    return results;
}

template <typename Work>
auto postWork(Work work, const std::optional<boost::asio::any_io_executor>& executor)
    -> std::future<decltype(work())> {
    using ResultType = decltype(work());
    std::packaged_task<ResultType()> task(std::move(work));
    auto future = task.get_future();
    if (executor) {
        boost::asio::post(*executor, [task = std::move(task)]() mutable { task(); });
    } else {
        // No executor available - run synchronously on current thread.
        // NOTE: std::async futures block in destructor, so discarding one
        // would actually block anyway. Better to be explicit about sync execution.
        task();
    }
    return future;
}

std::string truncateSnippet(const std::string& content, size_t maxLen) {
    if (content.empty()) {
        return {};
    }
    if (content.size() <= maxLen) {
        return content;
    }
    std::string out = content.substr(0, maxLen);
    out.append("...");
    return out;
}

std::string makeHeadTailSnippet(std::string_view content, size_t maxLen) {
    if (content.size() <= maxLen) {
        return std::string(content);
    }
    if (maxLen <= 3) {
        return std::string(content.substr(0, maxLen));
    }

    const size_t bodyBudget = maxLen - 3;
    const size_t prefixLen = bodyBudget / 2;
    const size_t suffixLen = bodyBudget - prefixLen;

    std::string out;
    out.reserve(maxLen);
    out.append(content.substr(0, prefixLen));
    out.append("...");
    out.append(content.substr(content.size() - suffixLen, suffixLen));
    return out;
}

std::string buildRerankSnippet(const std::string& query, const std::string& content,
                               size_t maxLen) {
    if (content.empty() || maxLen == 0) {
        return {};
    }
    if (content.size() <= maxLen) {
        return content;
    }

    struct TokenHit {
        size_t pos = 0;
        size_t len = 0;
    };

    const auto queryTokens = tokenizeLower(query);
    const std::string loweredContent = toLowerCopy(content);
    std::vector<TokenHit> hits;
    hits.reserve(queryTokens.size());

    for (const auto& token : queryTokens) {
        if (token.size() < 3) {
            continue;
        }
        size_t pos = loweredContent.find(token);
        while (pos != std::string::npos) {
            hits.push_back(TokenHit{pos, token.size()});
            pos = loweredContent.find(token, pos + 1);
        }
    }

    if (hits.empty()) {
        if (content.size() <= maxLen + (maxLen / 2)) {
            return truncateSnippet(content, maxLen);
        }
        return makeHeadTailSnippet(content, maxLen);
    }

    size_t bestStart = 0;
    size_t bestHitCount = 0;
    size_t bestCovered = 0;
    for (const auto& hit : hits) {
        const size_t start = hit.pos > (maxLen / 3) ? hit.pos - (maxLen / 3) : 0;
        const size_t end = std::min(content.size(), start + maxLen);

        size_t hitCount = 0;
        size_t covered = 0;
        for (const auto& candidate : hits) {
            if (candidate.pos >= start && candidate.pos < end) {
                hitCount++;
                covered += candidate.len;
            }
        }

        if (hitCount > bestHitCount || (hitCount == bestHitCount && covered > bestCovered)) {
            bestStart = start;
            bestHitCount = hitCount;
            bestCovered = covered;
        }
    }

    const bool clippedLeft = bestStart > 0;
    const size_t ellipsisBudget = clippedLeft ? 3 : 0;
    if (ellipsisBudget >= maxLen) {
        return truncateSnippet(content, maxLen);
    }

    const size_t bodyBudget = maxLen - ellipsisBudget;
    const size_t bodyEnd = std::min(content.size(), bestStart + bodyBudget);

    std::string out;
    out.reserve(maxLen);
    if (clippedLeft) {
        out.append("...");
    }
    out.append(content.substr(bestStart, bodyEnd - bestStart));
    if (out.size() < maxLen && bodyEnd < content.size()) {
        const size_t remaining = maxLen - out.size();
        out.append(content.substr(bodyEnd, remaining));
    }
    return out;
}

std::optional<std::int64_t>
resolveGraphCandidateDocumentId(const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore,
                                std::string_view candidateId) {
    if (!kgStore || candidateId.empty()) {
        return std::nullopt;
    }

    auto byHash = kgStore->getDocumentIdByHash(candidateId);
    if (byHash && byHash.value().has_value()) {
        return byHash.value();
    }

    auto byPath = kgStore->getDocumentIdByPath(candidateId);
    if (byPath && byPath.value().has_value()) {
        return byPath.value();
    }

    return std::nullopt;
}

std::optional<std::int64_t>
resolveGraphCandidateNodeId(const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore,
                            std::string_view candidateId) {
    if (!kgStore) {
        return std::nullopt;
    }

    auto directNode = kgStore->getNodeByKey(std::string(candidateId));
    if (directNode && directNode.value().has_value()) {
        return directNode.value()->id; // NOLINT(bugprone-unchecked-optional-access) — guarded above
    }

    auto docId = resolveGraphCandidateDocumentId(kgStore, candidateId);
    if (!docId.has_value()) {
        return std::nullopt;
    }

    auto docHash = kgStore->getDocumentHashById(*docId);
    if (!docHash || !docHash.value().has_value() ||
        docHash.value()->empty()) { // NOLINT(bugprone-unchecked-optional-access) — short-circuit:
                                    // .value() only called when docHash is non-empty
        return std::nullopt;
    }

    auto node = kgStore->getNodeByKey(
        "doc:" + *docHash.value()); // NOLINT(bugprone-unchecked-optional-access) — guarded above
    if (!node || !node.value().has_value()) {
        auto fallbackNode = kgStore->getNodeByKey("doc:" + std::string(candidateId));
        if (fallbackNode && fallbackNode.value().has_value()) {
            return fallbackNode.value()
                ->id; // NOLINT(bugprone-unchecked-optional-access) — guarded above
        }
        return std::nullopt;
    }
    return node.value()->id; // NOLINT(bugprone-unchecked-optional-access) — guarded above
}

std::vector<float> computeReciprocalCommunitySupport(
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore,
    const std::vector<std::string>& candidateIds, std::size_t maxNeighbors,
    float referenceCommunitySize = 0.0f, std::size_t* supportedDocCountOut = nullptr,
    std::size_t* edgeCountOut = nullptr, std::size_t* largestCommunityOut = nullptr,
    float decayHalfLifeDays = 0.0f, float minEdgeWeight = 0.0f) {
    std::vector<float> support(candidateIds.size(), 0.0f);
    if (!kgStore || candidateIds.size() < 2) {
        return support;
    }

    std::unordered_map<std::int64_t, std::size_t> indexByNodeId;
    indexByNodeId.reserve(candidateIds.size());
    for (std::size_t i = 0; i < candidateIds.size(); ++i) {
        auto nodeId = resolveGraphCandidateNodeId(kgStore, candidateIds[i]);
        if (nodeId.has_value()) {
            indexByNodeId.emplace(*nodeId, i);
        }
    }
    if (indexByNodeId.size() < 2) {
        return support;
    }

    // P8: compute decayed weight from (edge.weight, edge.createdTime).
    // halfLife==0 disables decay so the effective weight is just edge.weight,
    // which preserves pre-P8 binary behavior when minEdgeWeight==0 (any
    // reciprocal link counts) and every incoming edge has weight>0.
    const std::int64_t nowSeconds = std::chrono::duration_cast<std::chrono::seconds>(
                                        std::chrono::system_clock::now().time_since_epoch())
                                        .count();
    const double kLn2 = 0.6931471805599453;
    auto decayedWeight = [&](const yams::metadata::KGEdge& edge) -> float {
        float w = edge.weight > 0.0f ? edge.weight : 0.0f;
        if (decayHalfLifeDays > 0.0f && edge.createdTime.has_value()) {
            const double ageSeconds = static_cast<double>(nowSeconds - *edge.createdTime);
            if (ageSeconds > 0.0) {
                const double ageDays = ageSeconds / 86400.0;
                const double factor =
                    std::exp(-kLn2 * ageDays / static_cast<double>(decayHalfLifeDays));
                w = static_cast<float>(static_cast<double>(w) * factor);
            }
        }
        return w;
    };

    // outgoingWeight[i][j] is the decayed weight of the edge from i->j, if any.
    std::vector<std::unordered_map<std::size_t, float>> outgoingWeight(candidateIds.size());
    std::vector<std::vector<std::size_t>> reciprocalAdj(candidateIds.size());

    for (const auto& [nodeId, idx] : indexByNodeId) {
        auto edgesResult =
            kgStore->getEdgesFrom(nodeId, std::string_view("semantic_neighbor"), maxNeighbors, 0);
        if (!edgesResult) {
            continue;
        }
        for (const auto& edge : edgesResult.value()) {
            auto it = indexByNodeId.find(edge.dstNodeId);
            if (it == indexByNodeId.end() || it->second == idx) {
                continue;
            }
            const float w = decayedWeight(edge);
            if (w < minEdgeWeight) {
                continue;
            }
            auto& slot = outgoingWeight[idx][it->second];
            slot = std::max(slot, w);
        }
    }

    std::size_t reciprocalEdgeCount = 0;
    for (std::size_t i = 0; i < outgoingWeight.size(); ++i) {
        for (const auto& [neighbor, w] : outgoingWeight[i]) {
            if (neighbor <= i) {
                continue;
            }
            auto reverseIt = outgoingWeight[neighbor].find(i);
            if (reverseIt != outgoingWeight[neighbor].end()) {
                reciprocalAdj[i].push_back(neighbor);
                reciprocalAdj[neighbor].push_back(i);
                ++reciprocalEdgeCount;
            }
        }
    }

    std::vector<bool> visited(candidateIds.size(), false);
    std::size_t supportedDocCount = 0;
    std::size_t largestCommunity = 0;
    for (std::size_t i = 0; i < reciprocalAdj.size(); ++i) {
        if (visited[i] || reciprocalAdj[i].empty()) {
            continue;
        }
        std::vector<std::size_t> component;
        std::vector<std::size_t> stack = {i};
        visited[i] = true;
        while (!stack.empty()) {
            const auto cur = stack.back();
            stack.pop_back();
            component.push_back(cur);
            for (const auto neighbor : reciprocalAdj[cur]) {
                if (visited[neighbor]) {
                    continue;
                }
                visited[neighbor] = true;
                stack.push_back(neighbor);
            }
        }

        if (component.size() < 2) {
            continue;
        }

        supportedDocCount += component.size();
        largestCommunity = std::max(largestCommunity, component.size());
        // Corpus-adaptive normalization: when a reference community size is supplied, normalize
        // against it so the absolute signal magnitude is stable across candidate-set widths.
        // Fall back to the legacy candidate-set-adaptive denominator when no reference is given.
        const float denom = referenceCommunitySize > 1.0f
                                ? referenceCommunitySize - 1.0f
                                : static_cast<float>(candidateIds.size() - 1);
        const float normalized =
            std::clamp(static_cast<float>(component.size() - 1) / denom, 0.0f, 1.0f);
        for (const auto member : component) {
            support[member] = std::max(support[member], normalized);
        }
    }

    if (supportedDocCountOut) {
        *supportedDocCountOut = supportedDocCount;
    }
    if (edgeCountOut) {
        *edgeCountOut = reciprocalEdgeCount;
    }
    if (largestCommunityOut) {
        *largestCommunityOut = largestCommunity;
    }
    return support;
}

bool hasQueryTokenHit(const std::string& query, const std::string& content) {
    const auto queryTokens = tokenizeLower(query);
    const std::string loweredContent = toLowerCopy(content);
    for (const auto& token : queryTokens) {
        if (token.size() < 3) {
            continue;
        }
        if (loweredContent.find(token) != std::string::npos) {
            return true;
        }
    }
    return false;
}

std::vector<std::string> buildRerankPassages(const std::string& query, const std::string& content,
                                             size_t maxLen) {
    std::vector<std::string> passages;
    if (content.empty() || maxLen == 0) {
        return passages;
    }

    const std::string primary = buildRerankSnippet(query, content, maxLen);
    if (!primary.empty()) {
        passages.push_back(primary);
    }

    if (!hasQueryTokenHit(query, content) && content.size() > maxLen * 2) {
        struct Segment {
            size_t start = 0;
            std::string text;
        };

        std::vector<Segment> segments;
        size_t segmentStart = 0;
        for (size_t i = 0; i < content.size(); ++i) {
            const char ch = content[i];
            if (ch != '.' && ch != ';' && ch != '\n') {
                continue;
            }

            size_t rawStart = segmentStart;
            size_t rawEnd = i + 1;
            while (rawStart < rawEnd &&
                   std::isspace(static_cast<unsigned char>(content[rawStart]))) {
                ++rawStart;
            }
            while (rawEnd > rawStart &&
                   std::isspace(static_cast<unsigned char>(content[rawEnd - 1]))) {
                --rawEnd;
            }

            if (rawEnd > rawStart) {
                segments.push_back(Segment{rawStart, content.substr(rawStart, rawEnd - rawStart)});
            }
            segmentStart = i + 1;
        }
        if (segmentStart < content.size()) {
            size_t rawStart = segmentStart;
            size_t rawEnd = content.size();
            while (rawStart < rawEnd &&
                   std::isspace(static_cast<unsigned char>(content[rawStart]))) {
                ++rawStart;
            }
            while (rawEnd > rawStart &&
                   std::isspace(static_cast<unsigned char>(content[rawEnd - 1]))) {
                --rawEnd;
            }
            if (rawEnd > rawStart) {
                segments.push_back(Segment{rawStart, content.substr(rawStart, rawEnd - rawStart)});
            }
        }

        const size_t prefixCoverage = std::min(content.size(), maxLen);
        size_t supplementalCount = 0;
        for (auto it = segments.rbegin(); it != segments.rend() && supplementalCount < 3; ++it) {
            if (it->start < prefixCoverage / 2) {
                continue;
            }
            std::string passage = truncateSnippet(it->text, maxLen);
            if (passage.empty() ||
                std::find(passages.begin(), passages.end(), passage) != passages.end()) {
                continue;
            }
            passages.push_back(std::move(passage));
            supplementalCount++;
        }

        if (supplementalCount == 0) {
            const size_t maxStart = content.size() > maxLen ? content.size() - maxLen : 0;
            std::vector<size_t> supplementalStarts = {
                (content.size() - maxLen) / 2,
                std::min(maxStart, (content.size() * 2) / 3 > maxLen / 2
                                       ? (content.size() * 2) / 3 - maxLen / 2
                                       : size_t(0)),
            };

            for (size_t start : supplementalStarts) {
                const std::string passage = content.substr(start, maxLen);
                if (!passage.empty() &&
                    std::find(passages.begin(), passages.end(), passage) == passages.end()) {
                    passages.push_back(passage);
                }
            }
        }
    }

    return passages;
}

struct QueryExpansionStats {
    size_t generatedSubPhrases = 0;
    size_t subPhraseClauseCount = 0;
    size_t subPhraseFtsHitCount = 0;
    size_t subPhraseFtsAddedCount = 0;
    size_t aggressiveClauseCount = 0;
    size_t aggressiveFtsHitCount = 0;
    size_t aggressiveFtsAddedCount = 0;
    size_t graphExpansionTermCount = 0;
    size_t graphExpansionFtsHitCount = 0;
    size_t graphExpansionFtsAddedCount = 0;
    size_t graphTextBlockedLowScoreCount = 0;
};

bool envFlagEnabled(const char* name) {
    if (const char* env = std::getenv(name)) { // NOLINT(concurrency-mt-unsafe) — read-only; env
                                               // vars not modified concurrently
        std::string value(env);
        std::transform(value.begin(), value.end(), value.begin(), [](char c) {
            return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        });
        return value == "1" || value == "true" || value == "yes" || value == "on";
    }
    return false;
}

size_t envSizeTOrDefault(const char* name, size_t defaultValue, size_t minValue, size_t maxValue) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe) — read-only; env vars not modified concurrently
    if (const char* env = std::getenv(name); env && *env) {
        size_t parsed{};
        const auto* end = env + std::strlen(env);
        auto [ptr, ec] = std::from_chars(env, end, parsed);
        if (ec == std::errc{} && ptr == end) {
            return std::clamp(parsed, minValue, maxValue);
        }
    }
    return std::clamp(defaultValue, minValue, maxValue);
}

struct PreFusionDocSignal {
    bool hasAnchoring = false;
    bool hasVector = false;
    double maxVectorRaw = 0.0;
    size_t bestVectorRank = std::numeric_limits<size_t>::max();
    std::unordered_set<ComponentResult::Source> sources;
};

using PreFusionSignalMap = std::unordered_map<std::string, PreFusionDocSignal>;

std::vector<GraphExpansionSeedDoc>
collectGraphSeedDocs(const std::vector<ComponentResult>& componentResults, size_t maxDocs) {
    struct SeedAccumulator {
        std::string documentHash;
        std::string filePath;
        float score = 0.0f;
    };

    std::unordered_map<std::string, SeedAccumulator> bestByDoc;
    bestByDoc.reserve(componentResults.size());
    for (const auto& comp : componentResults) {
        const std::string docKey = documentIdForTrace(comp.filePath, comp.documentHash);
        if (docKey.empty()) {
            continue;
        }

        float sourceBoost;
        switch (comp.source) {
            case ComponentResult::Source::Text:
            case ComponentResult::Source::GraphText:
            case ComponentResult::Source::Vector:
            case ComponentResult::Source::CompressedANN:
            case ComponentResult::Source::GraphVector:
            case ComponentResult::Source::EntityVector:
            case ComponentResult::Source::KnowledgeGraph:
                sourceBoost = 1.0f;
                break;
            case ComponentResult::Source::PathTree:
                sourceBoost = 0.70f;
                break;
            case ComponentResult::Source::Symbol:
                sourceBoost = 0.80f;
                break;
            case ComponentResult::Source::Tag:
            case ComponentResult::Source::Metadata:
                sourceBoost = 0.60f;
                break;
            case ComponentResult::Source::Unknown:
                sourceBoost = 0.50f;
                break;
        }
        const float weightedScore = comp.score * sourceBoost;

        auto it = bestByDoc.find(docKey);
        if (it == bestByDoc.end() || weightedScore > it->second.score) {
            bestByDoc[docKey] = SeedAccumulator{comp.documentHash, comp.filePath, weightedScore};
        }
    }

    std::vector<GraphExpansionSeedDoc> docs;
    docs.reserve(bestByDoc.size());
    for (const auto& [_, seed] : bestByDoc) {
        docs.push_back({seed.documentHash, seed.filePath, seed.score});
    }
    std::stable_sort(docs.begin(), docs.end(),
                     [](const auto& a, const auto& b) { return a.score > b.score; });
    if (docs.size() > maxDocs) {
        docs.resize(maxDocs);
    }
    return docs;
}

PreFusionSignalMap buildPreFusionSignalMap(const std::vector<ComponentResult>& componentResults) {
    PreFusionSignalMap signals;
    signals.reserve(componentResults.size());

    for (const auto& comp : componentResults) {
        const std::string docId = documentIdForTrace(comp.filePath, comp.documentHash);
        if (docId.empty()) {
            continue;
        }

        auto& signal = signals[docId];
        signal.sources.insert(comp.source);
        if (isVectorComponent(comp.source)) {
            signal.hasVector = true;
            signal.maxVectorRaw = std::max(signal.maxVectorRaw,
                                           std::clamp(static_cast<double>(comp.score), 0.0, 1.0));
            signal.bestVectorRank = std::min(signal.bestVectorRank, comp.rank);
        }
        if (isTextAnchoringComponent(comp.source)) {
            signal.hasAnchoring = true;
        }
    }

    return signals;
}

std::string_view::size_type ci_find(std::string_view haystack, std::string_view needle) {
    if (needle.empty()) {
        return 0;
    }
    if (needle.size() > haystack.size()) {
        return std::string_view::npos;
    }

    // Pre-lowercase the needle once, then use SIMD-accelerated CI memmem.
    std::string needleLower(needle);
    std::transform(needleLower.begin(), needleLower.end(), needleLower.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

    size_t pos = yams::app::services::simdMemmemCI(haystack.data(), haystack.size(),
                                                   needleLower.data(), needleLower.size());
    return (pos == yams::app::services::kMemmemNpos) ? std::string_view::npos : pos;
}

constexpr auto kRerankerErrorCooldown = std::chrono::seconds(60);

bool isRerankerCooldownError(ErrorCode code) {
    return code == ErrorCode::NotImplemented || code == ErrorCode::InvalidState;
}

bool containsFast(std::string_view haystack, std::string_view needle) {
    if (needle.empty()) {
        return true;
    }
    if (needle.size() > haystack.size()) {
        return false;
    }
    return yams::app::services::simdMemmem(haystack, needle) != yams::app::services::kMemmemNpos;
}

float normalizedBm25Score(double rawScore, float divisor, double minScore, double maxScore) {
    if (maxScore > minScore) {
        const double norm = (rawScore - minScore) / (maxScore - minScore);
        return std::clamp(static_cast<float>(1.0 - norm), 0.0f, 1.0f);
    }
    return std::clamp(static_cast<float>(-rawScore) / divisor, 0.0f, 1.0f);
}

float filenamePathBoost(const std::string& query, const std::string& filePath,
                        const std::string& fileName) {
    const auto queryTokens = tokenizeLower(query);
    if (queryTokens.empty()) {
        return 1.0f;
    }

    const auto nameTokens = tokenizeLower(fileName);
    const auto pathTokens = tokenizeLower(filePath);
    if (nameTokens.empty() && pathTokens.empty()) {
        return 1.0f;
    }

    std::unordered_set<std::string> nameSet(nameTokens.begin(), nameTokens.end());
    std::unordered_set<std::string> pathSet(pathTokens.begin(), pathTokens.end());

    std::size_t nameMatches = 0;
    std::size_t pathMatches = 0;
    for (const auto& tok : queryTokens) {
        if (nameSet.count(tok)) {
            nameMatches++;
        } else if (pathSet.count(tok)) {
            pathMatches++;
        } else {
            for (const auto& nameTok : nameTokens) {
                if (nameTok.rfind(tok, 0) == 0) {
                    nameMatches++;
                    break;
                }
            }
            if (nameMatches == 0) {
                for (const auto& pathTok : pathTokens) {
                    if (pathTok.rfind(tok, 0) == 0) {
                        pathMatches++;
                        break;
                    }
                }
            }
        }
    }

    if (nameMatches > 0) {
        return 1.0f + std::min(2.0f, 0.5f + static_cast<float>(nameMatches) * 0.5f);
    }
    if (pathMatches > 0) {
        return 1.0f + std::min(1.0f, 0.25f + static_cast<float>(pathMatches) * 0.25f);
    }
    return 1.0f;
}

double scoreBasedRerankSignal(const SearchResult& result, QueryIntent intent,
                              SearchEngineConfig::NavigationZoomLevel zoomLevel) {
    const double text = result.keywordScore.value_or(0.0);
    const double vector = result.vectorScore.value_or(0.0);
    const double graphText = result.graphTextScore.value_or(0.0);
    const double graphVector = result.graphVectorScore.value_or(0.0);
    const double kg = result.kgScore.value_or(0.0);
    const double path = result.pathScore.value_or(0.0);
    const double symbol = result.symbolScore.value_or(0.0);
    const double tag = result.tagScore.value_or(0.0);

    const bool hasText = text > 0.0 || graphText > 0.0;
    const bool hasVector = vector > 0.0 || graphVector > 0.0;
    const bool hasPath = path > 0.0;
    const bool hasSymbol = symbol > 0.0;
    const bool hasKg = kg > 0.0;

    double signal = result.score;
    if (hasText && hasVector) {
        signal += 0.12;
    }
    if (hasText && hasKg) {
        signal += 0.05;
    }
    if (hasText && hasPath) {
        signal += 0.03;
    }
    if (hasSymbol && hasVector) {
        signal += 0.04;
    }
    if (tag > 0.0) {
        signal += std::min(0.02, tag * 0.2);
    }

    switch (intent) {
        case QueryIntent::Code:
            signal += path * 0.10 + symbol * 0.10;
            break;
        case QueryIntent::Path:
            signal += path * 0.15;
            break;
        case QueryIntent::Prose:
            signal += text * 0.08 + vector * 0.04 + kg * 0.04;
            break;
        case QueryIntent::Mixed:
            signal += text * 0.05 + vector * 0.05 + kg * 0.03 + path * 0.02;
            break;
    }

    switch (zoomLevel) {
        case SearchEngineConfig::NavigationZoomLevel::Auto:
            break;
        case SearchEngineConfig::NavigationZoomLevel::Map:
            signal += kg * 0.08 + graphText * 0.06 + graphVector * 0.05;
            break;
        case SearchEngineConfig::NavigationZoomLevel::Neighborhood:
            signal += kg * 0.04 + graphText * 0.03 + path * 0.02;
            break;
        case SearchEngineConfig::NavigationZoomLevel::Street:
            signal += text * 0.10 + path * 0.08 + symbol * 0.08;
            if (!hasText && hasVector) {
                signal -= 0.10;
            }
            break;
    }

    return signal;
}

} // namespace

#if YAMS_HAS_RANGES
namespace ranges_helpers {
template <typename Range, typename Predicate>
auto filter_not_empty(const Range& range, Predicate pred) {
    return range | std::views::filter(pred) |
           std::views::transform([](const auto& elem) { return elem; });
}
} // namespace ranges_helpers
#endif

// ============================================================================
// ResultFusion Implementation
// ============================================================================

ResultFusion::ResultFusion(const SearchEngineConfig& config) : config_(config) {}

std::vector<SearchResult> ResultFusion::fuse(const std::vector<ComponentResult>& componentResults) {
    if (componentResults.empty()) [[unlikely]] {
        return {};
    }

    switch (config_.fusionStrategy) {
        case SearchEngineConfig::FusionStrategy::WEIGHTED_SUM:
            return fuseWeightedSum(componentResults);
        case SearchEngineConfig::FusionStrategy::RECIPROCAL_RANK:
            return fuseReciprocalRank(componentResults);
        case SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL:
            return fuseWeightedReciprocal(componentResults);
        case SearchEngineConfig::FusionStrategy::COMB_MNZ:
            return fuseCombMNZ(componentResults);
        case SearchEngineConfig::FusionStrategy::CONVEX:
            // P7: convex combination is adaptive — any unexpected error falls
            // back to RRF rather than surfacing failures to the user.
            try {
                return fuseConvex(componentResults);
            } catch (const std::exception& e) {
                spdlog::warn("[search] convex fusion failed ({}); falling back to RRF", e.what());
                return fuseReciprocalRank(componentResults);
            } catch (...) {
                spdlog::warn("[search] convex fusion failed (unknown); falling back to RRF");
                return fuseReciprocalRank(componentResults);
            }
    }
    return fuseCombMNZ(componentResults); // Default fallback
}

// All fusion strategies now use fuseSinglePass (defined in header as template).
// This replaces the previous 3-pass pattern (groupByDocument -> iterate -> sort)
// with a single-pass accumulation directly into result map, then one sort.

std::vector<SearchResult>
ResultFusion::fuseWeightedSum(const std::vector<ComponentResult>& results) {
    return fuseSinglePass(results, [this](const ComponentResult& comp) {
        return comp.score * getComponentWeight(comp.source);
    });
}

std::vector<SearchResult>
ResultFusion::fuseReciprocalRank(const std::vector<ComponentResult>& results) {
    const float k = config_.rrfK;
    return fuseSinglePass(results, [k](const ComponentResult& comp) {
        const double rank = static_cast<double>(comp.rank) + 1.0; // RRF uses 1-based ranks
        return 1.0 / (k + rank);
    });
}

std::vector<SearchResult>
ResultFusion::fuseWeightedReciprocal(const std::vector<ComponentResult>& results) {
    const float k = config_.rrfK;
    return fuseSinglePass(results, [this, k](const ComponentResult& comp) {
        // Weighted RRF with score boost:
        // - RRF provides rank-based fusion across components
        // - Score provides a multiplicative boost to reward high-confidence matches
        // - Formula: weight * rrfScore * (1 + score) where score is in [0,1]
        //   This gives rank-1 with score=0.9 a 1.9x boost vs rank-1 with score=0
        float weight = getComponentWeight(comp.source);
        const double rank = static_cast<double>(comp.rank) + 1.0;
        double rrfScore = 1.0 / (k + rank);
        double scoreScale = 1.0;
        if (config_.enableFieldAwareWeightedRrf) {
            scoreScale = 0.60;
            switch (comp.source) {
                case ComponentResult::Source::Text:
                case ComponentResult::Source::GraphText:
                    scoreScale = 1.00;
                    break;
                case ComponentResult::Source::PathTree:
                    scoreScale = 0.85;
                    break;
                case ComponentResult::Source::KnowledgeGraph:
                    scoreScale = 0.80;
                    break;
                case ComponentResult::Source::Tag:
                case ComponentResult::Source::Metadata:
                    scoreScale = 0.65;
                    break;
                case ComponentResult::Source::Vector:
                    scoreScale = 0.75;
                    break;
                case ComponentResult::Source::CompressedANN:
                    scoreScale = 0.25;
                    break;
                case ComponentResult::Source::GraphVector:
                    scoreScale = 0.45;
                    break;
                case ComponentResult::Source::EntityVector:
                    scoreScale = 0.35;
                    break;
                case ComponentResult::Source::Symbol:
                    scoreScale = 0.75;
                    break;
                case ComponentResult::Source::Unknown:
                    scoreScale = 0.60;
                    break;
            }
        }

        double scoreBoost =
            1.0 + scoreScale * std::clamp(static_cast<double>(comp.score), 0.0, 1.0);
        return weight * rrfScore * scoreBoost;
    });
}

std::vector<SearchResult> ResultFusion::fuseCombMNZ(const std::vector<ComponentResult>& results) {
    struct Accumulator {
        double score = 0.0;
        size_t componentCount = 0;
        size_t bestTextRank = std::numeric_limits<size_t>::max();
        size_t bestVectorRank = std::numeric_limits<size_t>::max();
        bool hasAnchoring = false;
        double maxVectorRaw = 0.0;
        double keywordScore = 0.0;
        double pathScore = 0.0;
        double tagScore = 0.0;
        double symbolScore = 0.0;
        double graphTextScore = 0.0;
        double vectorScore = 0.0;
        double graphVectorScore = 0.0;
        std::string documentHash;
        std::string filePath;
        std::string snippet;
    };
    std::unordered_map<std::string, Accumulator> accumMap;
    accumMap.reserve(results.size());

    const float k = config_.rrfK;

    for (const auto& comp : results) {
        const std::string dedupKey =
            detail::makeFusionDedupKey(comp, config_.enablePathDedupInFusion);
        auto& acc = accumMap[dedupKey];

        if (acc.componentCount == 0) {
            acc.documentHash = comp.documentHash;
            acc.filePath = comp.filePath;
            if (comp.snippet.has_value()) {
                acc.snippet = comp.snippet.value();
            }
        } else if (acc.filePath.empty() && !comp.filePath.empty()) {
            acc.filePath = comp.filePath;
        }

        if (acc.snippet.empty() && comp.snippet.has_value()) {
            acc.snippet = comp.snippet.value();
        }

        if (comp.source == ComponentResult::Source::Text) {
            acc.bestTextRank = std::min(acc.bestTextRank, comp.rank);
        }
        if (isTextAnchoringComponent(comp.source)) {
            acc.hasAnchoring = true;
        }
        if (isVectorComponent(comp.source)) {
            acc.maxVectorRaw =
                std::max(acc.maxVectorRaw, std::clamp(static_cast<double>(comp.score), 0.0, 1.0));
            acc.bestVectorRank = std::min(acc.bestVectorRank, comp.rank);
        }

        float weight = getComponentWeight(comp.source);
        const double rank = static_cast<double>(comp.rank) + 1.0;
        double rrfScore = 1.0 / (k + rank);
        double contribution = weight * rrfScore;

        acc.score += contribution;
        acc.componentCount++;

        switch (comp.source) {
            case ComponentResult::Source::Text:
                acc.keywordScore += contribution;
                break;
            case ComponentResult::Source::GraphText:
                acc.graphTextScore += contribution;
                break;
            case ComponentResult::Source::PathTree:
                acc.pathScore += contribution;
                break;
            case ComponentResult::Source::Tag:
            case ComponentResult::Source::Metadata:
                acc.tagScore += contribution;
                break;
            case ComponentResult::Source::Symbol:
                acc.symbolScore += contribution;
                break;
            case ComponentResult::Source::Vector:
            case ComponentResult::Source::CompressedANN:
            case ComponentResult::Source::EntityVector:
                acc.vectorScore += contribution;
                break;
            case ComponentResult::Source::GraphVector:
                acc.graphVectorScore += contribution;
                break;
            case ComponentResult::Source::KnowledgeGraph:
            case ComponentResult::Source::Unknown:
                break;
        }
    }

    std::vector<SearchResult> fusedResults;
    fusedResults.reserve(accumMap.size());
    std::vector<std::pair<double, SearchResult>> semanticRescueReserve;
    semanticRescueReserve.reserve(std::min(config_.semanticRescueSlots, accumMap.size()));
    std::unordered_map<std::string, double> rawVectorScoreByDedupKey;
    rawVectorScoreByDedupKey.reserve(accumMap.size());

    for (auto& entry : accumMap) {
        rawVectorScoreByDedupKey[entry.first] = entry.second.maxVectorRaw;

        SearchResult r;
        r.document.sha256Hash = std::move(entry.second.documentHash);
        r.document.filePath = std::move(entry.second.filePath);
        r.score = static_cast<float>(entry.second.score *
                                     static_cast<double>(entry.second.componentCount));
        if (entry.second.keywordScore > 0.0) {
            r.keywordScore = entry.second.keywordScore;
        }
        if (entry.second.pathScore > 0.0) {
            r.pathScore = entry.second.pathScore;
        }
        if (entry.second.tagScore > 0.0) {
            r.tagScore = entry.second.tagScore;
        }
        if (entry.second.symbolScore > 0.0) {
            r.symbolScore = entry.second.symbolScore;
        }
        if (entry.second.graphTextScore > 0.0) {
            r.graphTextScore = entry.second.graphTextScore;
        }
        if (entry.second.vectorScore > 0.0) {
            r.vectorScore = entry.second.vectorScore;
        }
        if (entry.second.graphVectorScore > 0.0) {
            r.graphVectorScore = entry.second.graphVectorScore;
        }

        if (config_.lexicalFloorBoost > 0.0f &&
            entry.second.bestTextRank != std::numeric_limits<size_t>::max()) {
            const bool floorEnabledForRank = (config_.lexicalFloorTopN == 0) ||
                                             (entry.second.bestTextRank < config_.lexicalFloorTopN);
            if (floorEnabledForRank) {
                const double floorBoost =
                    std::clamp(static_cast<double>(config_.lexicalFloorBoost), 0.0, 1.0) /
                    (1.0 + static_cast<double>(entry.second.bestTextRank));
                r.score += floorBoost;
            }
        }

        if (entry.second.maxVectorRaw > 0.0 && !entry.second.hasAnchoring) {
            const double vectorOnlyThreshold =
                std::clamp(static_cast<double>(config_.vectorOnlyThreshold), 0.0, 1.0);
            const double nearMissSlack =
                std::clamp(static_cast<double>(config_.vectorOnlyNearMissSlack), 0.0, 1.0);
            const double nearMissPenalty =
                std::clamp(static_cast<double>(config_.vectorOnlyNearMissPenalty), 0.0, 1.0);
            const double semanticRescueMinVector =
                std::max(0.0, static_cast<double>(config_.semanticRescueMinVectorScore));
            const bool strongRelief = strongVectorOnlyReliefEligible(
                config_, entry.second.maxVectorRaw, entry.second.bestVectorRank);
            const double effectivePenalty = effectiveVectorOnlyPenalty(
                config_, entry.second.maxVectorRaw, entry.second.bestVectorRank);
            const bool semanticRescueEligible =
                config_.semanticRescueSlots > 0 &&
                entry.second.maxVectorRaw >= semanticRescueMinVector;

            if (entry.second.maxVectorRaw < vectorOnlyThreshold) {
                const bool reserveEnabled = config_.vectorOnlyNearMissReserve > 0;
                const bool isNearMiss =
                    reserveEnabled && vectorOnlyThreshold > 0.0 &&
                    entry.second.maxVectorRaw + nearMissSlack >= vectorOnlyThreshold;
                if (!isNearMiss && !strongRelief && !semanticRescueEligible) {
                    continue;
                }

                if (strongRelief) {
                    r.score = static_cast<float>(r.score * effectivePenalty);
                } else {
                    if (semanticRescueEligible && !isNearMiss) {
                        r.score = static_cast<float>(r.score * effectivePenalty);
                        semanticRescueReserve.emplace_back(entry.second.maxVectorRaw, std::move(r));
                        continue;
                    }

                    const double thresholdRatio =
                        vectorOnlyThreshold > 0.0
                            ? std::clamp(entry.second.maxVectorRaw / vectorOnlyThreshold, 0.0, 1.0)
                            : std::clamp(entry.second.maxVectorRaw, 0.0, 1.0);
                    r.score = static_cast<float>(r.score * effectivePenalty * nearMissPenalty *
                                                 thresholdRatio);
                }
            } else {
                r.score = static_cast<float>(r.score * effectivePenalty);
            }
        }

        r.snippet = std::move(entry.second.snippet);
        fusedResults.push_back(std::move(r));
    }

    if (!semanticRescueReserve.empty()) {
        std::sort(semanticRescueReserve.begin(), semanticRescueReserve.end(),
                  [](const auto& a, const auto& b) {
                      if (a.first != b.first) {
                          return a.first > b.first;
                      }
                      return a.second.score > b.second.score;
                  });

        for (auto& [_, result] : semanticRescueReserve) {
            fusedResults.push_back(std::move(result));
        }
    }

    const bool pathDedup = config_.enablePathDedupInFusion;
    const auto rawVectorScoreForResult = [&rawVectorScoreByDedupKey,
                                          pathDedup](const SearchResult& r) {
        if (auto it = rawVectorScoreByDedupKey.find(detail::makeFusionDedupKey(r, pathDedup));
            it != rawVectorScoreByDedupKey.end()) {
            return it->second;
        }
        return 0.0;
    };

    const auto lexicalAnchorScore = [](const SearchResult& r) {
        return r.keywordScore.value_or(0.0) + r.pathScore.value_or(0.0) + r.tagScore.value_or(0.0) +
               r.symbolScore.value_or(0.0);
    };

    const auto isVectorOnlyRescueCandidate =
        [this, &lexicalAnchorScore, &rawVectorScoreForResult](const SearchResult& r) -> bool {
        const double lexical = lexicalAnchorScore(r);
        const double vector = rawVectorScoreForResult(r);
        return lexical <= 0.0 &&
               vector >= std::max(0.0, static_cast<double>(config_.semanticRescueMinVectorScore));
    };

    const auto lexicalAwareLess = [this, &lexicalAnchorScore](const SearchResult& a,
                                                              const SearchResult& b) {
        const double scoreDiff = a.score - b.score;
        const double tieEpsilon =
            std::max(0.0, static_cast<double>(config_.lexicalTieBreakEpsilon));

        if (!config_.enableLexicalTieBreak || std::abs(scoreDiff) > tieEpsilon) {
            if (a.score != b.score) {
                return a.score > b.score;
            }
        } else {
            const double lexicalA = lexicalAnchorScore(a);
            const double lexicalB = lexicalAnchorScore(b);
            if (lexicalA != lexicalB) {
                return lexicalA > lexicalB;
            }

            const double keywordA = a.keywordScore.value_or(0.0);
            const double keywordB = b.keywordScore.value_or(0.0);
            if (keywordA != keywordB) {
                return keywordA > keywordB;
            }

            const double vectorA = a.vectorScore.value_or(0.0);
            const double vectorB = b.vectorScore.value_or(0.0);
            if (vectorA != vectorB) {
                return vectorA < vectorB;
            }
        }

        if (a.document.filePath != b.document.filePath) {
            return a.document.filePath < b.document.filePath;
        }
        return a.document.sha256Hash < b.document.sha256Hash;
    };

    const auto semanticRescueBetter = [&rawVectorScoreForResult, &lexicalAwareLess](
                                          const SearchResult& a, const SearchResult& b) {
        const double rawVectorA = rawVectorScoreForResult(a);
        const double rawVectorB = rawVectorScoreForResult(b);
        if (rawVectorA != rawVectorB) {
            return rawVectorA > rawVectorB;
        }
        return lexicalAwareLess(a, b);
    };

    const auto applySemanticRescueWindow = [&]() {
        if (config_.semanticRescueSlots == 0 || fusedResults.empty()) {
            return;
        }

        const size_t topK = std::min((config_.enableReranking && config_.rerankTopK > 0)
                                         ? std::min(config_.maxResults, config_.rerankTopK)
                                         : config_.maxResults,
                                     fusedResults.size());
        if (topK == 0 || topK >= fusedResults.size()) {
            return;
        }

        const size_t rescueTarget = std::min(config_.semanticRescueSlots, topK);
        size_t rescuePresent = 0;
        for (size_t i = 0; i < topK; ++i) {
            if (isVectorOnlyRescueCandidate(fusedResults[i])) {
                rescuePresent++;
            }
        }

        while (rescuePresent < rescueTarget) {
            size_t bestTailIndex = fusedResults.size();
            for (size_t i = topK; i < fusedResults.size(); ++i) {
                if (!isVectorOnlyRescueCandidate(fusedResults[i])) {
                    continue;
                }
                if (bestTailIndex >= fusedResults.size() ||
                    semanticRescueBetter(fusedResults[i], fusedResults[bestTailIndex])) {
                    bestTailIndex = i;
                }
            }
            if (bestTailIndex >= fusedResults.size()) {
                break;
            }

            size_t victimIndex = topK;
            for (size_t i = topK; i > 0; --i) {
                const size_t idx = i - 1;
                if (!isVectorOnlyRescueCandidate(fusedResults[idx])) {
                    victimIndex = idx;
                    break;
                }
            }
            if (victimIndex >= topK) {
                break;
            }

            std::swap(fusedResults[victimIndex], fusedResults[bestTailIndex]);
            rescuePresent++;
        }

        std::sort(fusedResults.begin(), fusedResults.begin() + static_cast<ptrdiff_t>(topK),
                  lexicalAwareLess);
    };

    if (fusedResults.size() > config_.maxResults) {
        std::partial_sort(fusedResults.begin(),
                          fusedResults.begin() + static_cast<ptrdiff_t>(config_.maxResults),
                          fusedResults.end(), lexicalAwareLess);
        applySemanticRescueWindow();

        fusedResults.resize(config_.maxResults);
    } else {
        std::sort(fusedResults.begin(), fusedResults.end(), lexicalAwareLess);
        applySemanticRescueWindow();
    }

    return fusedResults;
}

float ResultFusion::getComponentWeight(ComponentResult::Source source) const {
    return componentSourceWeight(config_, source);
}

// P7: Convex combination fusion.
// Per-component scores are normalized to [0,1] (divided by that component's max),
// then combined as sum(alpha_c * norm_score_c). Component weights come from
// SearchEngineConfig (already populated from SearchTuner::getParams().weights via
// TunedParams::applyTo, which is normalized to sum~1.0). Bruch et al. (2210.11934)
// shows this beats RRF once tuner weights have converged.
std::vector<SearchResult> ResultFusion::fuseConvex(const std::vector<ComponentResult>& results) {
    // 1) Find per-component max score for normalization.
    std::unordered_map<int, double> maxByComponent;
    maxByComponent.reserve(8);
    for (const auto& comp : results) {
        const int key = static_cast<int>(comp.source);
        auto& cur = maxByComponent[key];
        const double s = static_cast<double>(comp.score);
        if (s > cur) {
            cur = s;
        }
    }

    return fuseSinglePass(results, [this, &maxByComponent](const ComponentResult& comp) {
        const double weight = static_cast<double>(getComponentWeight(comp.source));
        if (weight <= 0.0) {
            return 0.0;
        }
        const int key = static_cast<int>(comp.source);
        auto it = maxByComponent.find(key);
        const double maxScore = (it != maxByComponent.end()) ? it->second : 0.0;
        if (maxScore <= 0.0) {
            return 0.0;
        }
        const double norm = std::clamp(static_cast<double>(comp.score) / maxScore, 0.0, 1.0);
        return weight * norm;
    });
}

// ============================================================================
// SearchEngine::Impl
// ============================================================================

class SearchEngine::Impl {
public:
    Impl(std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo,
         std::shared_ptr<vector::VectorDatabase> vectorDb,
         std::shared_ptr<vector::EmbeddingGenerator> embeddingGen,
         std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore,
         const SearchEngineConfig& config)
        : metadataRepo_(std::move(metadataRepo)), vectorDb_(std::move(vectorDb)),
          embeddingGen_(std::move(embeddingGen)), kgStore_(std::move(kgStore)), config_(config) {
        if (kgStore_) {
            kgScorer_ = makeSimpleKGScorer(kgStore_);
        }
    }

    Result<std::vector<SearchResult>> search(const std::string& query, const SearchParams& params);

    void setConfig(const SearchEngineConfig& config) { config_ = config; }

    const SearchEngineConfig& getConfig() const { return config_; }

    const SearchEngine::Statistics& getStatistics() const { return stats_; }

    void resetStatistics() {
        stats_.totalQueries.store(0, std::memory_order_relaxed);
        stats_.successfulQueries.store(0, std::memory_order_relaxed);
        stats_.failedQueries.store(0, std::memory_order_relaxed);

        stats_.textQueries.store(0, std::memory_order_relaxed);
        stats_.pathTreeQueries.store(0, std::memory_order_relaxed);
        stats_.kgQueries.store(0, std::memory_order_relaxed);
        stats_.vectorQueries.store(0, std::memory_order_relaxed);
        stats_.entityVectorQueries.store(0, std::memory_order_relaxed);
        stats_.tagQueries.store(0, std::memory_order_relaxed);
        stats_.metadataQueries.store(0, std::memory_order_relaxed);

        stats_.totalQueryTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgQueryTimeMicros.store(0, std::memory_order_relaxed);

        stats_.avgTextTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgPathTreeTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgKgTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgVectorTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgEntityVectorTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgTagTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgMetadataTimeMicros.store(0, std::memory_order_relaxed);

        stats_.avgResultsPerQuery.store(0, std::memory_order_relaxed);
        stats_.avgComponentsPerResult.store(0, std::memory_order_relaxed);
    }

    Result<void> healthCheck();

    Result<SearchResponse> searchWithResponse(const std::string& query,
                                              const SearchParams& params = {});

    void setExecutor(std::optional<boost::asio::any_io_executor> executor) {
        executor_ = std::move(executor);
    }

    void setConceptExtractor(EntityExtractionFunc extractor) {
        conceptExtractor_ = std::move(extractor);
    }

    void setReranker(std::shared_ptr<IReranker> reranker) { reranker_ = std::move(reranker); }

    void setSearchTuner(std::shared_ptr<SearchTuner> tuner) { tuner_ = std::move(tuner); }

    std::shared_ptr<SearchTuner> getSearchTuner() const { return tuner_; }

    void invalidateCompressedANNIndex() {
        if (compressedAnnIndex_) {
            compressedAnnIndex_->invalidate();
        }
        compressedAnnDocumentHashes_.clear();
        compressedAnnIndexReady_ = false;
    }

private:
    Result<SearchResponse> searchInternal(const std::string& query, const SearchParams& params);

    Result<std::vector<ComponentResult>>
    queryFullText(const std::string& query, QueryIntent queryIntent,
                  const SearchEngineConfig& config, size_t limit,
                  QueryExpansionStats* expansionStats = nullptr,
                  const std::vector<GraphExpansionTerm>* graphExpansionTerms = nullptr);
    Result<std::vector<ComponentResult>> queryPathTree(const std::string& query, size_t limit);
    Result<std::vector<ComponentResult>>
    queryKnowledgeGraph(const std::string& query, size_t limit,
                        const std::vector<QueryConcept>* concepts = nullptr);
    Result<std::vector<ComponentResult>> queryVectorIndex(const std::vector<float>& embedding,
                                                          const SearchEngineConfig& config,
                                                          size_t limit);
    Result<std::vector<ComponentResult>>
    queryVectorIndex(const std::vector<float>& embedding, const SearchEngineConfig& config,
                     size_t limit, const std::unordered_set<std::string>& candidates);
    Result<std::vector<ComponentResult>> queryEntityVectors(const std::vector<float>& embedding,
                                                            const SearchEngineConfig& config,
                                                            size_t limit);

    // Compressed ANN traversal using CompressedANNIndex over packed TurboQuant codes
    Result<std::vector<ComponentResult>> queryCompressedANN(const std::vector<float>& embedding,
                                                            const SearchEngineConfig& config,
                                                            size_t limit);
    Result<std::vector<ComponentResult>> queryTags(const std::vector<std::string>& tags,
                                                   bool matchAll, size_t limit);
    Result<std::vector<ComponentResult>> queryMetadata(const SearchParams& params, size_t limit);
    std::unordered_map<std::string, float> lookupQueryTermIdf(const std::string& query) const;
    std::vector<std::string> generateQuerySubPhrases(const std::string& query,
                                                     size_t maxPhrases) const;

    std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<vector::VectorDatabase> vectorDb_;
    std::shared_ptr<vector::EmbeddingGenerator> embeddingGen_;
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore_;
    std::shared_ptr<KGScorer> kgScorer_;
    std::optional<boost::asio::any_io_executor> executor_;
    SearchEngineConfig config_;
    mutable SearchEngine::Statistics stats_;
    EntityExtractionFunc conceptExtractor_;               // GLiNER concept extractor (optional)
    std::shared_ptr<IReranker> reranker_;                 // Cross-encoder reranker (optional)
    std::shared_ptr<IVectorReranker> turboQuantReranker_; // TurboQuant vector reranker (optional)

    // Compressed ANN index (built lazily from packed vectors when enabled)
    std::unique_ptr<vector::CompressedANNIndex> compressedAnnIndex_;
    std::vector<std::string> compressedAnnDocumentHashes_;
    bool compressedAnnIndexReady_ = false; // True once build() has been called
    std::shared_ptr<SearchTuner> tuner_;   // Adaptive runtime tuner (optional)
    std::atomic<int64_t> rerankerCooldownUntilMicros_{0};
};

Result<std::vector<SearchResult>> SearchEngine::Impl::search(const std::string& query,
                                                             const SearchParams& params) {
    auto response = searchInternal(query, params);
    if (!response) {
        return Error{response.error().code, response.error().message};
    }
    return response.value().results;
}

Result<SearchResponse> SearchEngine::Impl::searchWithResponse(const std::string& query,
                                                              const SearchParams& params) {
    return searchInternal(query, params);
}

std::unordered_map<std::string, float>
SearchEngine::Impl::lookupQueryTermIdf(const std::string& query) const {
    std::unordered_map<std::string, float> idfByToken;
    if (!metadataRepo_) {
        return idfByToken;
    }

    auto tokens = tokenizeQueryTokens(query);
    std::vector<std::string> uniqueTerms;
    uniqueTerms.reserve(tokens.size());
    std::unordered_set<std::string> seen;
    seen.reserve(tokens.size());
    for (const auto& token : tokens) {
        if (token.normalized.size() < 2) {
            continue;
        }
        if (seen.insert(token.normalized).second) {
            uniqueTerms.push_back(token.normalized);
        }
    }

    if (uniqueTerms.empty()) {
        return idfByToken;
    }

    auto idfResult = metadataRepo_->getTermIDFBatch(uniqueTerms);
    if (idfResult) {
        idfByToken = std::move(idfResult.value());
    } else {
        spdlog::debug("lookupQueryTermIdf: IDF batch lookup failed: {}", idfResult.error().message);
    }
    return idfByToken;
}

std::vector<std::string> SearchEngine::Impl::generateQuerySubPhrases(const std::string& query,
                                                                     size_t maxPhrases) const {
    if (maxPhrases == 0) {
        return {};
    }

    auto idfByToken = lookupQueryTermIdf(query);

    return generateAnchoredSubPhrases(query, maxPhrases,
                                      idfByToken.empty() ? nullptr : &idfByToken);
}

Result<SearchResponse> SearchEngine::Impl::searchInternal(const std::string& query,
                                                          const SearchParams& params) {
    YAMS_ZONE_SCOPED_N("search_engine::execute");
    YAMS_FRAME_MARK();

    auto startTime = std::chrono::steady_clock::now();
    stats_.totalQueries.fetch_add(1, std::memory_order_relaxed);

    SearchResponse response;
    std::vector<std::string> timedOut;
    std::vector<std::string> failed;
    std::vector<std::string> contributing;
    std::vector<std::string> skipped;
    std::map<std::string, int64_t> componentTiming;
    const bool stageTraceEnabled = envFlagEnabled("YAMS_SEARCH_STAGE_TRACE");
    std::vector<std::string> preFusionDocIds;
    PreFusionSignalMap preFusionSignals;
    std::vector<SearchResult> postFusionSnapshot;
    std::vector<SearchResult> graphlessPostFusionSnapshot;
    std::vector<SearchResult> postGraphSnapshot;
    bool graphRerankApplied = false;
    bool crossRerankApplied = false;
    double rerankGuardScoreGap = 0.0;
    bool rerankGuardCompetitiveAnchoredEvidence = false;
    std::vector<std::string> rerankGuardAnchoredDocIds;
    size_t graphWindowGuardReplacementCount = 0;
    size_t graphWindowCapReplacementCount = 0;
    size_t graphRerankGuardReplacementCount = 0;
    size_t graphMatchedCandidates = 0;
    size_t graphPositiveSignalCandidates = 0;
    size_t graphBoostedDocs = 0;
    float graphMaxSignal = 0.0f;
    size_t graphQueryConceptCount = 0;
    json rerankWindowTrace = json::array();
    QueryExpansionStats textExpansionStats;
    size_t multiVectorGeneratedPhrases = 0;
    size_t multiVectorPhraseHits = 0;
    size_t multiVectorAddedNewCount = 0;
    size_t multiVectorReplacedBaseCount = 0;
    size_t graphDocExpansionTermCount = 0;
    size_t graphDocExpansionFtsHitCount = 0;
    size_t graphDocExpansionFtsAddedCount = 0;
    size_t graphVectorGeneratedTerms = 0;
    size_t graphVectorRawHitCount = 0;
    size_t graphVectorAddedNewCount = 0;
    size_t graphVectorReplacedBaseCount = 0;
    size_t graphVectorBlockedUncorroboratedCount = 0;
    size_t graphVectorBlockedMissingTextAnchorCount = 0;
    size_t graphVectorBlockedMissingBaselineTextAnchorCount = 0;
    bool relaxedVectorRetryEnabled = false;
    std::atomic<bool> relaxedVectorRetryApplied{false};
    std::atomic<bool> relaxedVectorRetryAttempted{false};
    std::atomic<int> relaxedVectorPrimaryHitCount{0};
    std::atomic<int> relaxedVectorRetryThresholdMilli{0};
    bool weakQueryFanoutBoostApplied = false;
    size_t effectiveVectorMaxResults = 0;
    size_t effectiveEntityVectorMaxResults = 0;
    SearchEngineConfig workingConfig = config_;

    if (tuner_) {
        workingConfig = tuner_->getConfig();
    }

    // Snapshot the per-query search execution context once so the tuner
    // and the downstream budget / freshness reads see the same
    // topologyEpoch (previous version loaded twice and could observe a
    // torn read under concurrent topology rebuilds).
    const SearchExecutionContext searchExecutionContext = currentSearchExecutionContext();

    // R2: construct a TuningContext populated with corpus-slow features,
    // query token stats, and the topology epoch fingerprint. The context is
    // passed to both getParams() and observe() so the contextual policy
    // (R4+) can condition on it. SearchTuner (rules) ignores the context.
    TuningContext tuningCtx;
    if (tuner_) {
        fillCorpusFeatures(tuningCtx, tuner_->corpusStats());
    }
    fillQueryTokenFeature(tuningCtx, query);
    tuningCtx.topologyEpoch = searchExecutionContext.freshness.topologyEpoch;

    std::optional<TuningState> baselineState;
    TunedParams baseParams;
    if (tuner_) {
        baselineState = tuner_->currentState();
        baseParams = tuner_->getParams(tuningCtx);
    } else {
        baseParams = seedTunedParamsFromConfig(workingConfig);
    }

    const auto routingStart = std::chrono::steady_clock::now();
    QueryPolicyResolution policy =
        resolveQueryPolicy(query, workingConfig, baseParams, baselineState, params.semanticOnly);
    response.componentTimingMicros["query_routing"] =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() -
                                                              routingStart)
            .count();
    if (tuner_) {
        response.componentTimingMicros["community_detection"] =
            response.componentTimingMicros.at("query_routing");
    }

    const QueryRouteDecision routeDecision = std::move(policy.routeDecision);
    const QueryIntent intent = routeDecision.intent.label;
    const QueryRetrievalMode retrievalMode = routeDecision.retrievalMode.label;
    const auto effectiveZoomLevel = policy.effectiveZoomLevel;
    const bool zoomLevelInferredFromIntent = policy.zoomLevelInferredFromIntent;
    workingConfig = std::move(policy.config);

    // R2/audit: route-derived query-fast features populated after
    // classification so R4+ contextual policies see real signals instead
    // of zero-defaulted fields. Vector-path = any retrieval mode that
    // touches the semantic tier; KG anchors = a community route matched
    // or an explicit override landed.
    tuningCtx.queryHasVectorPath = (retrievalMode == QueryRetrievalMode::Semantic ||
                                    retrievalMode == QueryRetrievalMode::Hybrid)
                                       ? std::uint8_t{1}
                                       : std::uint8_t{0};
    tuningCtx.queryHasKgAnchors =
        (routeDecision.community.has_value() || policy.communityOverride.has_value())
            ? std::uint8_t{1}
            : std::uint8_t{0};

    if (policy.communityOverride.has_value() && baselineState.has_value()) {
        const std::string overrideState = tuningStateToString(*policy.communityOverride);
        response.debugStats["community_override"] = overrideState;
        spdlog::debug("[Search] community override: global={} → {} (routing={}μs, query='{}')",
                      tuningStateToString(*baselineState), overrideState,
                      response.componentTimingMicros.at("query_routing"), query);
    }

    // Long prose queries can miss the semantic tier entirely when the default threshold
    // is tuned for code/navigation queries. Retry only after a zero-hit semantic search.
    const size_t queryTokenCount = tokenizeLower(query).size();
    const bool shortQueryBudgeted = searchExecutionContext.shortQueryBudgeted;
    const bool pressureBudgeted = searchExecutionContext.pressureBudgeted;
    const auto& freshness = searchExecutionContext.freshness;
    const auto& topologyOverlayHashes = searchExecutionContext.topologyOverlayHashes;
    const bool corpusWarming = freshness.corpusWarming();
    const bool semanticBudgetActive = shortQueryBudgeted || pressureBudgeted;
    const bool delayEmbeddingUntilTier1 = semanticBudgetActive;
    response.debugStats["semantic_budget_short_query"] = shortQueryBudgeted ? "1" : "0";
    response.debugStats["semantic_budget_pressure"] = pressureBudgeted ? "1" : "0";
    response.debugStats["corpus_warming"] = corpusWarming ? "1" : "0";
    response.debugStats["ingest_queued"] = std::to_string(freshness.ingestQueued);
    response.debugStats["ingest_inflight"] = std::to_string(freshness.ingestInFlight);
    response.debugStats["post_ingest_queued"] = std::to_string(freshness.postIngestQueued);
    response.debugStats["post_ingest_inflight"] = std::to_string(freshness.postIngestInFlight);
    response.debugStats["lexical_delta_pending_docs"] =
        std::to_string(freshness.lexicalDeltaPendingDocs);
    response.debugStats["lexical_delta_queued_epoch"] =
        std::to_string(freshness.lexicalDeltaQueuedEpoch);
    response.debugStats["lexical_delta_published_epoch"] =
        std::to_string(freshness.lexicalDeltaPublishedEpoch);
    response.debugStats["lexical_delta_published_docs"] =
        std::to_string(freshness.lexicalDeltaPublishedDocs);
    response.debugStats["search_engine_ready"] = freshness.lexicalReady ? "1" : "0";
    response.debugStats["search_engine_awaiting_drain"] = freshness.awaitingDrain ? "1" : "0";
    response.debugStats["vector_ready"] = freshness.vectorReady ? "1" : "0";
    response.debugStats["kg_ready"] = freshness.kgReady ? "1" : "0";
    response.debugStats["topology_ready"] = freshness.topologyReady ? "1" : "0";
    response.debugStats["topology_epoch"] = std::to_string(freshness.topologyEpoch);
    response.debugStats["topology_overlay_hashes"] = std::to_string(topologyOverlayHashes.size());
    response.debugStats["semantic_budget_delay_embedding"] = delayEmbeddingUntilTier1 ? "1" : "0";
    if (shortQueryBudgeted) {
        workingConfig.vectorMaxResults = std::min(workingConfig.vectorMaxResults, size_t{8});
        workingConfig.entityVectorMaxResults =
            std::min(workingConfig.entityVectorMaxResults, size_t{4});
        workingConfig.kgMaxResults = 0;
        workingConfig.kgWeight = 0.0f;
        workingConfig.enableGraphRerank = false;
        workingConfig.graphRerankTopN = 0;
        workingConfig.graphScoringBudgetMs = 0;
        response.debugStats["semantic_budget_mode"] = "short_query";
    }
    if (pressureBudgeted) {
        workingConfig.vectorMaxResults = std::min(workingConfig.vectorMaxResults, size_t{16});
        workingConfig.entityVectorMaxResults =
            std::min(workingConfig.entityVectorMaxResults, size_t{8});
        workingConfig.kgMaxResults = std::min(workingConfig.kgMaxResults, size_t{24});
        workingConfig.graphRerankTopN = std::min(workingConfig.graphRerankTopN, size_t{10});
        workingConfig.graphScoringBudgetMs = std::min(workingConfig.graphScoringBudgetMs, 4);
        response.debugStats["semantic_budget_mode"] =
            shortQueryBudgeted ? "short_query+pressure" : "pressure";
    }
    if (corpusWarming) {
        if (topologyOverlayHashes.empty()) {
            workingConfig.enableTopologyWeakQueryRouting = false;
        }
        workingConfig.enableGraphRerank = false;
        workingConfig.graphRerankTopN = 0;
        workingConfig.graphScoringBudgetMs = 0;
        workingConfig.kgMaxResults = 0;
        workingConfig.kgWeight = 0.0f;
        response.debugStats["semantic_budget_mode"] =
            response.debugStats.contains("semantic_budget_mode")
                ? response.debugStats["semantic_budget_mode"] + "+warming"
                : "warming";
    }
    relaxedVectorRetryEnabled = intent == QueryIntent::Prose && queryTokenCount >= 6 &&
                                workingConfig.similarityThreshold > 0.40f;

    const auto queryVectorWithRelaxedRetry =
        [this, &workingConfig, relaxedVectorRetryEnabled, &relaxedVectorRetryApplied,
         &relaxedVectorRetryAttempted, &relaxedVectorPrimaryHitCount,
         &relaxedVectorRetryThresholdMilli](const std::vector<float>& embedding, size_t limit,
                                            const std::unordered_set<std::string>* candidates =
                                                nullptr) -> Result<std::vector<ComponentResult>> {
        const auto runVectorQuery = [&](const SearchEngineConfig& config) {
            return candidates != nullptr ? queryVectorIndex(embedding, config, limit, *candidates)
                                         : queryVectorIndex(embedding, config, limit);
        };

        auto primary = runVectorQuery(workingConfig);
        if (primary) {
            relaxedVectorPrimaryHitCount.store(static_cast<int>(primary.value().size()),
                                               std::memory_order_relaxed);
        }
        if (!relaxedVectorRetryEnabled || !primary || !primary.value().empty()) {
            return primary;
        }

        SearchEngineConfig relaxedConfig = workingConfig;
        const float relaxedThreshold = std::min(workingConfig.similarityThreshold, 0.40f);
        if (!(relaxedThreshold + 1e-6f < workingConfig.similarityThreshold)) {
            return primary;
        }

        relaxedVectorRetryAttempted.store(true, std::memory_order_relaxed);
        relaxedConfig.similarityThreshold = relaxedThreshold;
        auto retried = runVectorQuery(relaxedConfig);
        if (retried && !retried.value().empty()) {
            relaxedVectorRetryApplied.store(true, std::memory_order_relaxed);
            relaxedVectorRetryThresholdMilli.store(
                static_cast<int>(std::lround(static_cast<double>(relaxedThreshold) * 1000.0)),
                std::memory_order_relaxed);
            spdlog::debug(
                "Vector search retry: relaxed threshold {:.3f} -> {:.3f} yielded {} results",
                workingConfig.similarityThreshold, relaxedThreshold, retried.value().size());
            return retried;
        }

        return primary;
    };

    spdlog::debug("Query intent: {}, retrieval={}, zoom={} ({})", queryIntentToString(intent),
                  queryRetrievalModeToString(retrievalMode),
                  SearchEngineConfig::navigationZoomLevelToString(effectiveZoomLevel),
                  zoomLevelInferredFromIntent ? "intent_auto" : "configured");

    SearchTraceCollector traceCollector(workingConfig);

    // Embedding generation may be launched eagerly or lazily depending on tiering strategy.
    std::optional<std::vector<float>> queryEmbedding;
    const bool needsEmbedding =
        (workingConfig.vectorWeight > 0.0f || workingConfig.entityVectorWeight > 0.0f);
    std::future<std::vector<float>> embeddingFuture;
    std::chrono::steady_clock::time_point embStart;
    bool embeddingStarted = false;
    bool embeddingAwaited = false;
    bool embeddingFailed = false;
    std::string embeddingStatus = needsEmbedding ? "not_started" : "not_needed";
    const size_t vectorDbEmbeddingDim = vectorDb_ ? vectorDb_->getConfig().embedding_dim : 0;
    size_t queryEmbeddingDim = 0;
    bool semanticTierSkipped = false;
    std::string semanticTierSkipReason;
    std::string vectorTierSkipReason;
    auto launchEmbeddingIfNeeded = [&]() {
        if (!embeddingStarted && needsEmbedding && embeddingGen_) {
            traceCollector.markStageAttempted("embedding");
            embStart = std::chrono::steady_clock::now();
            embeddingFuture = postWork(
                [this, &query]() {
                    YAMS_ZONE_SCOPED_N("embedding::generate_async");
                    return embeddingGen_->generateEmbedding(query);
                },
                executor_);
            embeddingStarted = true;
            embeddingStatus = "launched";
        }
    };

    // Preserve overlap with the lexical tiers unless daemon-aware budgeting asks us to
    // prove lexical strength before paying semantic costs.
    if (!delayEmbeddingUntilTier1) {
        launchEmbeddingIfNeeded();
    }

    std::future<Result<QueryConceptResult>> conceptFuture;
    std::chrono::steady_clock::time_point conceptStart;
    std::vector<QueryConcept> concepts;
    bool conceptsMaterialized = false;
    std::vector<GraphExpansionTerm> graphExpansionTerms;
    std::vector<GraphExpansionTerm> docSeedGraphTerms;
    bool graphExpansionMaterialized = false;
    size_t graphQueryNeighborSeedDocCount = 0;
    if (conceptExtractor_ && !params.semanticOnly) {
        conceptStart = std::chrono::steady_clock::now();
        conceptFuture = postWork(
            [this, &query]() {
                YAMS_ZONE_SCOPED_N("concepts::extract_async");
                return conceptExtractor_(query, {});
            },
            executor_);
    }

    // Helper to await embedding result when needed (called before vector search)
    auto awaitEmbedding = [&]() {
        launchEmbeddingIfNeeded();
        if (embeddingFuture.valid() && !queryEmbedding.has_value()) {
            embeddingAwaited = true;
            try {
                auto embResult = embeddingFuture.get();
                if (!embResult.empty()) {
                    queryEmbedding = std::move(embResult);
                    embeddingStatus = "ready";
                } else {
                    embeddingStatus = "empty_result";
                }
            } catch (const std::exception& e) {
                spdlog::warn("Failed to generate query embedding: {}", e.what());
                embeddingFailed = true;
                embeddingStatus = "failed";
                auto embEnd = std::chrono::steady_clock::now();
                traceCollector.markStageFailure(
                    "embedding",
                    std::chrono::duration_cast<std::chrono::microseconds>(embEnd - embStart)
                        .count());
            }
            auto embEnd = std::chrono::steady_clock::now();
            componentTiming["embedding"] =
                std::chrono::duration_cast<std::chrono::microseconds>(embEnd - embStart).count();
            std::vector<ComponentResult> embeddingMarker;
            if (queryEmbedding.has_value()) {
                embeddingMarker.push_back(ComponentResult{.score = 1.0f});
            }
            traceCollector.markStageResult("embedding", embeddingMarker,
                                           componentTiming["embedding"],
                                           queryEmbedding.has_value());
        }
    };

    const size_t userLimit =
        params.limit > 0 ? static_cast<size_t>(params.limit) : workingConfig.maxResults;
    const size_t semanticRescueProbeWindow =
        workingConfig.semanticRescueSlots > 0
            ? std::max<size_t>(200, workingConfig.semanticRescueSlots * 100)
            : size_t(0);
    const size_t autoFusionLimit = std::max(
        userLimit, std::max(std::max(workingConfig.rerankTopK, workingConfig.graphRerankTopN),
                            workingConfig.rerankTopK + semanticRescueProbeWindow));
    const size_t fusionCandidateLimit = workingConfig.fusionCandidateLimit > 0
                                            ? workingConfig.fusionCandidateLimit
                                            : autoFusionLimit;

    traceCollector.markStageConfigured("embedding", needsEmbedding && embeddingGen_ != nullptr);
    traceCollector.markStageConfigured("concepts", conceptExtractor_ != nullptr);

    workingConfig.maxResults = fusionCandidateLimit;
    traceCollector.markStageConfigured("text", workingConfig.textWeight > 0.0f);
    traceCollector.markStageConfigured("kg", workingConfig.kgWeight > 0.0f && kgStore_ != nullptr);
    traceCollector.markStageConfigured("path", workingConfig.pathTreeWeight > 0.0f);
    traceCollector.markStageConfigured("vector", workingConfig.vectorWeight > 0.0f);
    traceCollector.markStageConfigured("entity_vector", workingConfig.entityVectorWeight > 0.0f);
    traceCollector.markStageConfigured("compressed_ann",
                                       workingConfig.enableCompressedANN && vectorDb_ != nullptr);
    traceCollector.markStageConfigured("tag",
                                       workingConfig.tagWeight > 0.0f && !params.tags.empty());
    traceCollector.markStageConfigured("metadata", workingConfig.metadataWeight > 0.0f);
    traceCollector.markStageConfigured("multi_vector", workingConfig.enableMultiVectorQuery ||
                                                           workingConfig.enableGraphQueryExpansion);
    traceCollector.markStageConfigured("topology_routing",
                                       workingConfig.enableTopologyWeakQueryRouting);
    traceCollector.markStageConfigured("graph_rerank",
                                       workingConfig.enableGraphRerank && kgScorer_ != nullptr);
    traceCollector.markStageConfigured("reranker",
                                       workingConfig.enableReranking && reranker_ != nullptr);

    auto hasVectorTierDimMismatch = [&]() {
        if (!queryEmbedding.has_value() || !vectorDb_) {
            return false;
        }
        queryEmbeddingDim = queryEmbedding.value().size();
        if (queryEmbeddingDim == 0 || vectorDbEmbeddingDim == 0 ||
            queryEmbeddingDim == vectorDbEmbeddingDim) {
            return false;
        }
        if (vectorTierSkipReason.empty()) {
            vectorTierSkipReason =
                "embedding_dim_mismatch(db=" + std::to_string(vectorDbEmbeddingDim) +
                ",query=" + std::to_string(queryEmbeddingDim) + ")";
        }
        return true;
    };

    auto markVectorTierDimMismatch = [&]() {
        if (vectorTierSkipReason.empty()) {
            return;
        }
        traceCollector.markStageSkipped("vector", vectorTierSkipReason);
        traceCollector.markStageSkipped("entity_vector", vectorTierSkipReason);
        if (workingConfig.enableCompressedANN) {
            traceCollector.markStageSkipped("compressed_ann", vectorTierSkipReason);
        }
    };

    auto computeLexicalEvidence = [&](const std::vector<ComponentResult>& componentResults) {
        size_t lexicalHits = 0;
        float topTextScore = 0.0f;
        for (const auto& componentResult : componentResults) {
            if (componentResult.source == ComponentResult::Source::Text) {
                ++lexicalHits;
                topTextScore = std::max(topTextScore, componentResult.score);
            } else if (componentResult.source == ComponentResult::Source::PathTree) {
                ++lexicalHits;
            }
        }
        return std::pair<size_t, float>{lexicalHits, topTextScore};
    };

    auto budgetGuardStrongLexical = [&](const std::vector<ComponentResult>& componentResults) {
        const auto [lexicalHits, topTextScore] = computeLexicalEvidence(componentResults);
        response.debugStats["semantic_budget_lexical_hits"] = std::to_string(lexicalHits);
        response.debugStats["semantic_budget_top_text_score"] = fmt::format("{:.4f}", topTextScore);
        return lexicalHits >= 5 || topTextScore >= 0.20f;
    };

    auto materializeConcepts = [&](bool waitIfConfigured) {
        if (conceptsMaterialized || !conceptFuture.valid()) {
            return;
        }

        std::future_status conceptStatus = std::future_status::deferred;
        if (waitIfConfigured) {
            if (workingConfig.componentTimeout.count() > 0) {
                conceptStatus = conceptFuture.wait_for(workingConfig.componentTimeout);
            } else {
                conceptFuture.wait();
                conceptStatus = std::future_status::ready;
            }
        } else {
            conceptStatus = conceptFuture.wait_for(std::chrono::seconds(0));
        }

        if (conceptStatus == std::future_status::ready) {
            auto conceptEnd = std::chrono::steady_clock::now();
            componentTiming["concepts"] =
                std::chrono::duration_cast<std::chrono::microseconds>(conceptEnd - conceptStart)
                    .count();
            auto conceptResult = conceptFuture.get();
            conceptsMaterialized = true;
            if (conceptResult) {
                const auto& extracted = conceptResult.value().concepts;
                concepts.reserve(extracted.size());
                for (const auto& conceptItem : extracted) {
                    if (conceptItem.confidence >= workingConfig.conceptMinConfidence) {
                        concepts.push_back(conceptItem);
                    }
                }
                if (concepts.size() > workingConfig.conceptMaxCount) {
                    concepts.resize(workingConfig.conceptMaxCount);
                }
            }
        } else if (waitIfConfigured && conceptStatus == std::future_status::timeout) {
            timedOut.push_back("concepts");
            conceptsMaterialized = true;
            traceCollector.markStageTimeout("concepts");
        }

        if (conceptsMaterialized && concepts.size() < workingConfig.conceptMaxCount) {
            auto idfByToken = lookupQueryTermIdf(query);
            auto fallbackConcepts =
                generateFallbackQueryConcepts(query, idfByToken, workingConfig.conceptMaxCount);
            if (!fallbackConcepts.empty()) {
                std::unordered_set<std::string> seenConceptKeys;
                seenConceptKeys.reserve(concepts.size() + fallbackConcepts.size());
                for (const auto& existingConcept : concepts) {
                    std::string key = normalizeEntityTextForKey(existingConcept.text);
                    key.push_back('|');
                    key.append(existingConcept.type);
                    seenConceptKeys.insert(std::move(key));
                }

                const size_t beforeFallback = concepts.size();
                for (auto& fallbackConcept : fallbackConcepts) {
                    if (concepts.size() >= workingConfig.conceptMaxCount) {
                        break;
                    }
                    std::string key = normalizeEntityTextForKey(fallbackConcept.text);
                    key.push_back('|');
                    key.append(fallbackConcept.type);
                    if (!seenConceptKeys.insert(std::move(key)).second) {
                        continue;
                    }
                    concepts.push_back(std::move(fallbackConcept));
                }

                if (concepts.size() > beforeFallback) {
                    spdlog::debug("concepts: added {} fallback query concepts for '{}'",
                                  concepts.size() - beforeFallback, query.substr(0, 60));
                }
            }
        }

        if (conceptsMaterialized) {
            std::vector<ComponentResult> conceptMarker;
            if (!concepts.empty()) {
                conceptMarker.push_back(ComponentResult{.score = 1.0f});
            }
            traceCollector.markStageResult("concepts", conceptMarker, componentTiming["concepts"],
                                           !concepts.empty());
        }
    };

    auto materializeGraphExpansionTerms = [&](bool waitForConcepts) {
        if (graphExpansionMaterialized || !workingConfig.enableGraphQueryExpansion) {
            return;
        }
        if (waitForConcepts) {
            materializeConcepts(true);
        }
        graphExpansionTerms = generateGraphExpansionTerms(
            kgStore_, query, concepts,
            GraphExpansionConfig{.maxTerms = workingConfig.graphExpansionMaxTerms,
                                 .maxSeeds = workingConfig.graphExpansionMaxSeeds,
                                 .maxNeighbors = workingConfig.graphMaxNeighbors});

        if (vectorDb_ && embeddingGen_ && workingConfig.graphExpansionQueryNeighborK > 0) {
            awaitEmbedding();
            if (queryEmbedding.has_value()) {
                yams::vector::VectorSearchParams params;
                params.k = workingConfig.graphExpansionQueryNeighborK + 6;
                params.similarity_threshold = workingConfig.graphExpansionQueryNeighborMinScore;
                auto neighbors = vectorDb_->search(queryEmbedding.value(), params);

                std::vector<GraphExpansionSeedDoc> seedDocs;
                seedDocs.reserve(workingConfig.graphExpansionQueryNeighborK);
                std::unordered_set<std::string> seenHashes;
                seenHashes.reserve(workingConfig.graphExpansionQueryNeighborK * 2);
                for (const auto& rec : neighbors) {
                    if (seedDocs.size() >= workingConfig.graphExpansionQueryNeighborK) {
                        break;
                    }
                    if (rec.level != yams::vector::EmbeddingLevel::DOCUMENT ||
                        rec.document_hash.empty() || !seenHashes.insert(rec.document_hash).second) {
                        continue;
                    }
                    auto pathIt = rec.metadata.find("path");
                    seedDocs.push_back(
                        {.documentHash = rec.document_hash,
                         .filePath = pathIt != rec.metadata.end() ? pathIt->second : "",
                         .score = rec.relevance_score});
                }
                graphQueryNeighborSeedDocCount = seedDocs.size();

                // Only let query-neighbor docs elaborate an already graph-grounded query.
                // Pure vector-neighbor fanout can drift and perturb ranking without any
                // graph/text anchor from the query itself.
                if (!seedDocs.empty() && !graphExpansionTerms.empty()) {
                    auto neighborTerms = generateGraphExpansionTermsFromDocuments(
                        kgStore_, query, concepts, seedDocs,
                        GraphExpansionConfig{.maxTerms = workingConfig.graphExpansionMaxTerms,
                                             .maxSeeds = workingConfig.graphExpansionMaxSeeds,
                                             .maxNeighbors = workingConfig.graphMaxNeighbors});
                    std::unordered_map<std::string, size_t> termIndex;
                    termIndex.reserve(graphExpansionTerms.size());
                    for (size_t i = 0; i < graphExpansionTerms.size(); ++i) {
                        termIndex[graphExpansionTerms[i].text] = i;
                    }
                    for (const auto& term : neighborTerms) {
                        if (auto it = termIndex.find(term.text); it != termIndex.end()) {
                            graphExpansionTerms[it->second].score =
                                std::max(graphExpansionTerms[it->second].score, term.score);
                        } else {
                            termIndex[term.text] = graphExpansionTerms.size();
                            graphExpansionTerms.push_back(term);
                        }
                    }
                    std::stable_sort(
                        graphExpansionTerms.begin(), graphExpansionTerms.end(),
                        [](const auto& a, const auto& b) { return a.score > b.score; });
                    if (graphExpansionTerms.size() > workingConfig.graphExpansionMaxTerms) {
                        graphExpansionTerms.resize(workingConfig.graphExpansionMaxTerms);
                    }
                }
            }
        }
        graphExpansionMaterialized = true;
    };

    if (workingConfig.enableGraphQueryExpansion && workingConfig.waitForConceptExtraction) {
        materializeGraphExpansionTerms(true);
    }

    spdlog::debug("Search limit: userLimit={}, fusionCandidateLimit={}, textMax={}, vectorMax={}",
                  userLimit, fusionCandidateLimit, workingConfig.textMaxResults,
                  workingConfig.vectorMaxResults);

    std::vector<ComponentResult> allComponentResults;
    size_t estimatedResults = 0;
    if (workingConfig.textWeight > 0.0f)
        estimatedResults += workingConfig.textMaxResults;
    if (workingConfig.kgWeight > 0.0f)
        estimatedResults += workingConfig.kgMaxResults;
    if (workingConfig.pathTreeWeight > 0.0f)
        estimatedResults += workingConfig.pathTreeMaxResults;
    if (workingConfig.vectorWeight > 0.0f)
        estimatedResults += workingConfig.vectorMaxResults;
    if (workingConfig.entityVectorWeight > 0.0f)
        estimatedResults += workingConfig.entityVectorMaxResults;
    if (workingConfig.tagWeight > 0.0f)
        estimatedResults += workingConfig.tagMaxResults;
    if (workingConfig.metadataWeight > 0.0f)
        estimatedResults += workingConfig.metadataMaxResults;
    allComponentResults.reserve(estimatedResults);

    const bool compressedAnnEnabled = workingConfig.enableCompressedANN;
    size_t compressedAnnResultCount = 0;
    bool compressedAnnApplied = false;
    bool compressedAnnAttempted = false;
    std::string compressedAnnSkipReason = compressedAnnEnabled ? "not_attempted" : "disabled";

    // Component result collection helper with timing
    enum class ComponentStatus { Success, Failed, TimedOut };

    auto collectResults = [&](auto& future, const char* name, std::atomic<uint64_t>& queryCount,
                              std::atomic<uint64_t>& avgTime) -> ComponentStatus {
        if (!future.valid())
            return ComponentStatus::Success;

        traceCollector.markStageAttempted(name);

        auto waitStart = std::chrono::steady_clock::now();

        // componentTimeout of 0 means no timeout (wait indefinitely)
        std::future_status status;
        if (workingConfig.componentTimeout.count() == 0) {
            future.wait(); // Wait indefinitely
            status = std::future_status::ready;
        } else {
            status = future.wait_for(workingConfig.componentTimeout);
        }

        if (status == std::future_status::ready) {
            try {
                auto results = future.get();
                auto waitEnd = std::chrono::steady_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::microseconds>(waitEnd - waitStart)
                        .count();

                componentTiming[name] = duration;

                if (results) {
                    traceCollector.markStageResult(name, results.value(), duration,
                                                   !results.value().empty());
                    if (!results.value().empty()) {
                        allComponentResults.insert(allComponentResults.end(),
                                                   results.value().begin(), results.value().end());
                        contributing.push_back(name);
                    }
                    queryCount.fetch_add(1, std::memory_order_relaxed);
                    avgTime.store(duration, std::memory_order_relaxed);
                    return ComponentStatus::Success;
                } else {
                    spdlog::debug("Parallel {} query returned error: {}", name,
                                  results.error().message);
                    traceCollector.markStageFailure(name, duration);
                    return ComponentStatus::Failed;
                }
            } catch (const std::exception& e) {
                spdlog::warn("Parallel {} query failed: {}", name, e.what());
                auto waitEnd = std::chrono::steady_clock::now();
                traceCollector.markStageFailure(
                    name, std::chrono::duration_cast<std::chrono::microseconds>(waitEnd - waitStart)
                              .count());
                return ComponentStatus::Failed;
            }
        } else {
            spdlog::warn("Parallel {} query timed out after {} ms", name,
                         workingConfig.componentTimeout.count());
            stats_.timedOutQueries.fetch_add(1, std::memory_order_relaxed);
            auto waitEnd = std::chrono::steady_clock::now();
            traceCollector.markStageTimeout(
                name,
                std::chrono::duration_cast<std::chrono::microseconds>(waitEnd - waitStart).count());
            return ComponentStatus::TimedOut;
        }
    };

    auto handleStatus = [&](ComponentStatus status, const char* name) {
        if (status == ComponentStatus::Failed) {
            failed.push_back(name);
        } else if (status == ComponentStatus::TimedOut) {
            timedOut.push_back(name);
        }
    };

    if (workingConfig.enableParallelExecution) [[likely]] {
        YAMS_ZONE_SCOPED_N("search_engine::fanout_parallel");
        std::future<Result<std::vector<ComponentResult>>> textFuture;
        std::future<Result<std::vector<ComponentResult>>> kgFuture;
        std::future<Result<std::vector<ComponentResult>>> pathFuture;
        std::future<Result<std::vector<ComponentResult>>> vectorFuture;
        std::future<Result<std::vector<ComponentResult>>> entityVectorFuture;
        std::future<Result<std::vector<ComponentResult>>> tagFuture;
        std::future<Result<std::vector<ComponentResult>>> metaFuture;
        std::future<Result<std::vector<ComponentResult>>> compressedAnnFuture;

        auto schedule = [&]([[maybe_unused]] const char* name, [[maybe_unused]] float weight,
                            [[maybe_unused]] std::atomic<uint64_t>& queryCount,
                            [[maybe_unused]] std::atomic<uint64_t>& avgTime,
                            auto&& fn) -> std::future<Result<std::vector<ComponentResult>>> {
            if (weight <= 0.0f) {
                return {};
            }
            return postWork(std::forward<decltype(fn)>(fn), executor_);
        };

        auto collectIf = [&](std::future<Result<std::vector<ComponentResult>>>& future,
                             const char* name, std::atomic<uint64_t>& queryCount,
                             std::atomic<uint64_t>& avgTime) {
            handleStatus(collectResults(future, name, queryCount, avgTime), name);
        };

        if (workingConfig.enableTieredExecution) {
            // === TIERED EXECUTION ===
            // Tier 1: Fast text-based components (FTS5 + path_tree)
            // Tier 2: Slower semantic components (vector) - only if Tier 1 insufficient
            YAMS_ZONE_SCOPED_N("search_engine::tiered_execution");

            // --- TIER 1: Text + Path (fast, high precision) ---
            textFuture = schedule(
                "text", workingConfig.textWeight, stats_.textQueries, stats_.avgTextTimeMicros,
                [this, &query, intent, &workingConfig, &textExpansionStats,
                 &graphExpansionTerms]() {
                    YAMS_ZONE_SCOPED_N("component::text");
                    return queryFullText(query, intent, workingConfig, workingConfig.textMaxResults,
                                         &textExpansionStats, &graphExpansionTerms);
                });

            pathFuture = schedule("path", workingConfig.pathTreeWeight, stats_.pathTreeQueries,
                                  stats_.avgPathTreeTimeMicros, [this, &query, &workingConfig]() {
                                      YAMS_ZONE_SCOPED_N("component::path");
                                      return queryPathTree(query, workingConfig.pathTreeMaxResults);
                                  });

            // Tags and metadata are also fast, run in Tier 1
            if (!params.tags.empty()) {
                tagFuture = schedule("tag", workingConfig.tagWeight, stats_.tagQueries,
                                     stats_.avgTagTimeMicros, [this, &params, &workingConfig]() {
                                         YAMS_ZONE_SCOPED_N("component::tag");
                                         return queryTags(params.tags, params.matchAllTags,
                                                          workingConfig.tagMaxResults);
                                     });
            }

            metaFuture =
                schedule("metadata", workingConfig.metadataWeight, stats_.metadataQueries,
                         stats_.avgMetadataTimeMicros, [this, &params, &workingConfig]() {
                             YAMS_ZONE_SCOPED_N("component::metadata");
                             return queryMetadata(params, workingConfig.metadataMaxResults);
                         });

            // Collect Tier 1 results
            collectIf(textFuture, "text", stats_.textQueries, stats_.avgTextTimeMicros);
            collectIf(pathFuture, "path", stats_.pathTreeQueries, stats_.avgPathTreeTimeMicros);
            collectIf(tagFuture, "tag", stats_.tagQueries, stats_.avgTagTimeMicros);
            collectIf(metaFuture, "metadata", stats_.metadataQueries, stats_.avgMetadataTimeMicros);

            // Extract Tier 1 candidate hashes only when needed (narrowing or adaptive fallback).
            const bool needTier1Candidates = workingConfig.tieredNarrowVectorSearch ||
                                             workingConfig.enableAdaptiveVectorFallback;
            std::unordered_set<std::string> tier1Candidates;
            if (needTier1Candidates) {
                tier1Candidates.reserve(allComponentResults.size());
                for (const auto& r : allComponentResults) {
                    if (!r.documentHash.empty()) {
                        tier1Candidates.insert(r.documentHash);
                    }
                }
            }

            const size_t tier1CandidateCount = tier1Candidates.size();
            if (needTier1Candidates) {
                spdlog::debug("Tiered search: {} unique candidates from Tier 1",
                              tier1CandidateCount);
            }

            std::unordered_set<std::string> tier2Candidates = tier1Candidates;
            std::size_t topologyWeakQuerySeedCount = 0;
            std::size_t topologyWeakQueryAddedDocs = 0;
            bool topologyWeakQueryRoutingApplied = false;

            // --- TIER 2: Vector search NARROWED to Tier 1 candidates ---
            // Always run vector search (never skip), but filter to Tier 1 candidates when
            // appropriate
            YAMS_ZONE_SCOPED_N("search_engine::tier2_semantic");

            const size_t adaptiveSkipMinHits =
                (workingConfig.adaptiveVectorSkipMinTier1Hits > 0)
                    ? workingConfig.adaptiveVectorSkipMinTier1Hits
                    : std::max<size_t>(workingConfig.maxResults * 2, static_cast<size_t>(50));

            size_t tier1TextHits = 0;
            float tier1TopTextScore = 0.0f;
            for (const auto& componentResult : allComponentResults) {
                if (componentResult.source == ComponentResult::Source::Text) {
                    tier1TextHits++;
                    tier1TopTextScore = std::max(tier1TopTextScore, componentResult.score);
                }
            }

            bool hasStrongTextSignal = true;
            if (workingConfig.adaptiveVectorSkipRequireTextSignal) {
                hasStrongTextSignal =
                    tier1TextHits >= workingConfig.adaptiveVectorSkipMinTextHits &&
                    tier1TopTextScore >= workingConfig.adaptiveVectorSkipMinTopTextScore;
            }

            effectiveVectorMaxResults = workingConfig.vectorMaxResults;
            effectiveEntityVectorMaxResults = workingConfig.entityVectorMaxResults;
            const bool weakTier1Query = !hasStrongTextSignal || tier1TextHits == 0;
            if (workingConfig.enableWeakQueryFanoutBoost && weakTier1Query) {
                effectiveVectorMaxResults = static_cast<size_t>(
                    std::ceil(static_cast<double>(workingConfig.vectorMaxResults) *
                              workingConfig.weakQueryVectorFanoutMultiplier));
                effectiveEntityVectorMaxResults = static_cast<size_t>(
                    std::ceil(static_cast<double>(workingConfig.entityVectorMaxResults) *
                              workingConfig.weakQueryEntityVectorFanoutMultiplier));
                weakQueryFanoutBoostApplied = true;
                spdlog::debug(
                    "Tiered search: weak-query fanout boost applied (text_hits={}, "
                    "top_text_score={:.3f}, vector_max={} -> {}, entity_vector_max={} -> {})",
                    tier1TextHits, tier1TopTextScore, workingConfig.vectorMaxResults,
                    effectiveVectorMaxResults, workingConfig.entityVectorMaxResults,
                    effectiveEntityVectorMaxResults);
            }

            if (weakTier1Query && !tier1Candidates.empty() &&
                workingConfig.enableTopologyWeakQueryRouting) {
                bool topologyEpochMismatch = false;
                const auto topologyCandidates = buildTopologyWeakQueryCandidates(
                    metadataRepo_, kgStore_, query, tier1Candidates, topologyOverlayHashes,
                    workingConfig, freshness.topologyEpoch, &topologyEpochMismatch);
                if (topologyEpochMismatch) {
                    response.debugStats["topology_epoch_mismatch"] = "1";
                }
                topologyWeakQuerySeedCount = tier1Candidates.size();
                const auto candidateCountBeforeRouting = tier2Candidates.size();
                for (const auto& hash : topologyCandidates) {
                    tier2Candidates.insert(hash);
                }
                topologyWeakQueryAddedDocs =
                    tier2Candidates.size() > candidateCountBeforeRouting
                        ? tier2Candidates.size() - candidateCountBeforeRouting
                        : 0;
                topologyWeakQueryRoutingApplied = topologyWeakQueryAddedDocs > 0;
                if (topologyWeakQueryRoutingApplied) {
                    spdlog::debug(
                        "Tiered search: topology weak-query routing applied (seed_docs={}, "
                        "added_docs={}, total_candidates={})",
                        topologyWeakQuerySeedCount, topologyWeakQueryAddedDocs,
                        tier2Candidates.size());
                }

                std::vector<ComponentResult> topologyRoutingTrace;
                topologyRoutingTrace.reserve(topologyCandidates.size());
                for (const auto& hash : topologyCandidates) {
                    ComponentResult marker;
                    marker.documentHash = hash;
                    marker.score = 1.0f;
                    marker.source = ComponentResult::Source::GraphText;
                    topologyRoutingTrace.push_back(std::move(marker));
                }

                auto topologyDocResults = buildTopologyWeakQueryComponentResults(
                    metadataRepo_, kgStore_, topologyCandidates, workingConfig);
                if (!topologyDocResults.empty()) {
                    allComponentResults.insert(allComponentResults.end(),
                                               topologyDocResults.begin(),
                                               topologyDocResults.end());
                    contributing.push_back("topology_routing");
                }

                traceCollector.markStageResult("topology_routing", topologyRoutingTrace, 0,
                                               topologyWeakQueryRoutingApplied ||
                                                   !topologyDocResults.empty());
            } else if (workingConfig.enableTopologyWeakQueryRouting) {
                traceCollector.markStageSkipped("topology_routing", weakTier1Query
                                                                        ? "no_seed_candidates"
                                                                        : "strong_tier1_query");
            }

            const bool strongBudgetLexical =
                semanticBudgetActive && (tier1TextHits >= 5 || tier1TopTextScore >= 0.20f);
            const bool shouldSkipSemantic =
                (workingConfig.enableAdaptiveVectorFallback &&
                 (tier1CandidateCount >= adaptiveSkipMinHits) && hasStrongTextSignal) ||
                strongBudgetLexical;

            if (shouldSkipSemantic) {
                semanticTierSkipped = true;
                semanticTierSkipReason =
                    strongBudgetLexical ? "budget_guard_strong_lexical" : "adaptive_vector_skip";
                skipped.push_back("vector");
                skipped.push_back("entity_vector");
                if (workingConfig.enableCompressedANN) {
                    skipped.push_back("compressed_ann");
                    traceCollector.markStageSkipped("compressed_ann", semanticTierSkipReason);
                }
                traceCollector.markStageSkipped("vector", semanticTierSkipReason);
                traceCollector.markStageSkipped("entity_vector", semanticTierSkipReason);
                response.debugStats["semantic_budget_skip_reason"] = semanticTierSkipReason;
                spdlog::debug("Tiered search: skipping embedding/vector tier (reason={}, tier1 "
                              "candidates={}, threshold={}, text_hits={}, top_text_score={:.3f})",
                              semanticTierSkipReason, tier1CandidateCount, adaptiveSkipMinHits,
                              tier1TextHits, tier1TopTextScore);
            } else if (workingConfig.enableAdaptiveVectorFallback &&
                       tier1CandidateCount >= adaptiveSkipMinHits && !hasStrongTextSignal) {
                spdlog::debug("Tiered search: retaining semantic tier due to weak text signal "
                              "(tier1 candidates={}, text_hits={}, top_text_score={:.3f}, "
                              "min_text_hits={}, min_top_text_score={:.3f})",
                              tier1CandidateCount, tier1TextHits, tier1TopTextScore,
                              workingConfig.adaptiveVectorSkipMinTextHits,
                              workingConfig.adaptiveVectorSkipMinTopTextScore);
            }

            // Always materialize the query embedding when semantic search is configured so later
            // post-fusion steps are not blocked by adaptive lexical skipping.
            awaitEmbedding();

            // Decide whether to narrow vector search to Tier 1 candidates
            // Narrow if: config enabled AND Tier 1 has enough candidates
            const bool shouldNarrow = workingConfig.tieredNarrowVectorSearch &&
                                      tier2Candidates.size() >= workingConfig.tieredMinCandidates;

            response.debugStats["topology_weak_query_routing_applied"] =
                topologyWeakQueryRoutingApplied ? "1" : "0";
            response.debugStats["topology_weak_query_seed_docs"] =
                std::to_string(topologyWeakQuerySeedCount);
            response.debugStats["topology_weak_query_added_docs"] =
                std::to_string(topologyWeakQueryAddedDocs);
            response.debugStats["topology_weak_query_total_candidates"] =
                std::to_string(tier2Candidates.size());

            if (!shouldSkipSemantic && queryEmbedding.has_value() && vectorDb_ &&
                !hasVectorTierDimMismatch()) {
                vectorFuture =
                    schedule("vector", workingConfig.vectorWeight, stats_.vectorQueries,
                             stats_.avgVectorTimeMicros,
                             [&queryEmbedding, &tier2Candidates, shouldNarrow,
                              effectiveVectorMaxResults, &queryVectorWithRelaxedRetry]() {
                                 YAMS_ZONE_SCOPED_N("component::vector");
                                 if (shouldNarrow) {
                                     return queryVectorWithRelaxedRetry(queryEmbedding.value(),
                                                                        effectiveVectorMaxResults,
                                                                        &tier2Candidates);
                                 } else {
                                     return queryVectorWithRelaxedRetry(queryEmbedding.value(),
                                                                        effectiveVectorMaxResults);
                                 }
                             });

                entityVectorFuture = schedule(
                    "entity_vector", workingConfig.entityVectorWeight, stats_.entityVectorQueries,
                    stats_.avgEntityVectorTimeMicros,
                    [this, &queryEmbedding, &workingConfig, effectiveEntityVectorMaxResults]() {
                        YAMS_ZONE_SCOPED_N("component::entity_vector");
                        return queryEntityVectors(queryEmbedding.value(), workingConfig,
                                                  effectiveEntityVectorMaxResults);
                    });
            } else if (!shouldSkipSemantic && hasVectorTierDimMismatch()) {
                markVectorTierDimMismatch();
            }

            if (workingConfig.enableCompressedANN && queryEmbedding.has_value() && vectorDb_ &&
                !hasVectorTierDimMismatch()) {
                compressedAnnFuture = schedule(
                    "compressed_ann", workingConfig.compressedAnnWeight, stats_.vectorQueries,
                    stats_.avgVectorTimeMicros, [this, &queryEmbedding, &workingConfig]() {
                        YAMS_ZONE_SCOPED_N("component::compressed_ann");
                        return queryCompressedANN(queryEmbedding.value(), workingConfig,
                                                  workingConfig.compressedAnnTopK);
                    });
            }

            if (kgStore_) {
                if (workingConfig.graphUseQueryConcepts && workingConfig.waitForConceptExtraction) {
                    materializeConcepts(true);
                }
                kgFuture = schedule(
                    "kg", workingConfig.kgWeight, stats_.kgQueries, stats_.avgKgTimeMicros,
                    [this, &query, &workingConfig, &concepts]() {
                        YAMS_ZONE_SCOPED_N("component::kg");
                        return queryKnowledgeGraph(query, workingConfig.kgMaxResults, &concepts);
                    });
            }

            // Collect Tier 2 results (always collect, never skip)
            collectIf(vectorFuture, "vector", stats_.vectorQueries, stats_.avgVectorTimeMicros);
            collectIf(entityVectorFuture, "entity_vector", stats_.entityVectorQueries,
                      stats_.avgEntityVectorTimeMicros);
            compressedAnnAttempted = compressedAnnFuture.valid();
            collectIf(compressedAnnFuture, "compressed_ann", stats_.vectorQueries,
                      stats_.avgVectorTimeMicros);
            collectIf(kgFuture, "kg", stats_.kgQueries, stats_.avgKgTimeMicros);
        } else {
            // === FLAT PARALLEL EXECUTION (original behavior) ===
            // All components run in parallel
            textFuture = schedule(
                "text", workingConfig.textWeight, stats_.textQueries, stats_.avgTextTimeMicros,
                [this, &query, intent, &workingConfig, &textExpansionStats,
                 &graphExpansionTerms]() {
                    YAMS_ZONE_SCOPED_N("component::text");
                    return queryFullText(query, intent, workingConfig, workingConfig.textMaxResults,
                                         &textExpansionStats, &graphExpansionTerms);
                });

            const bool deferSemanticStages = semanticBudgetActive;
            if (kgStore_ && !deferSemanticStages) {
                if (workingConfig.graphUseQueryConcepts && workingConfig.waitForConceptExtraction) {
                    materializeConcepts(true);
                }
                kgFuture = schedule(
                    "kg", workingConfig.kgWeight, stats_.kgQueries, stats_.avgKgTimeMicros,
                    [this, &query, &workingConfig, &concepts]() {
                        YAMS_ZONE_SCOPED_N("component::kg");
                        return queryKnowledgeGraph(query, workingConfig.kgMaxResults, &concepts);
                    });
            }

            pathFuture = schedule("path", workingConfig.pathTreeWeight, stats_.pathTreeQueries,
                                  stats_.avgPathTreeTimeMicros, [this, &query, &workingConfig]() {
                                      YAMS_ZONE_SCOPED_N("component::path");
                                      return queryPathTree(query, workingConfig.pathTreeMaxResults);
                                  });

            // NOTE: Vector components scheduled below after embedding is ready
            // This allows text/kg/path to run in parallel with embedding generation

            if (!params.tags.empty()) {
                tagFuture = schedule("tag", workingConfig.tagWeight, stats_.tagQueries,
                                     stats_.avgTagTimeMicros, [this, &params, &workingConfig]() {
                                         YAMS_ZONE_SCOPED_N("component::tag");
                                         return queryTags(params.tags, params.matchAllTags,
                                                          workingConfig.tagMaxResults);
                                     });
            }

            metaFuture =
                schedule("metadata", workingConfig.metadataWeight, stats_.metadataQueries,
                         stats_.avgMetadataTimeMicros, [this, &params, &workingConfig]() {
                             YAMS_ZONE_SCOPED_N("component::metadata");
                             return queryMetadata(params, workingConfig.metadataMaxResults);
                         });

            collectIf(textFuture, "text", stats_.textQueries, stats_.avgTextTimeMicros);
            collectIf(pathFuture, "path", stats_.pathTreeQueries, stats_.avgPathTreeTimeMicros);
            collectIf(tagFuture, "tag", stats_.tagQueries, stats_.avgTagTimeMicros);
            collectIf(metaFuture, "metadata", stats_.metadataQueries, stats_.avgMetadataTimeMicros);

            const bool strongBudgetLexical =
                deferSemanticStages && budgetGuardStrongLexical(allComponentResults);
            if (deferSemanticStages && strongBudgetLexical) {
                semanticTierSkipped = true;
                semanticTierSkipReason = "budget_guard_strong_lexical";
                response.debugStats["semantic_budget_skip_reason"] = semanticTierSkipReason;
                skipped.push_back("kg");
                skipped.push_back("vector");
                skipped.push_back("entity_vector");
                traceCollector.markStageSkipped("kg", semanticTierSkipReason);
                traceCollector.markStageSkipped("vector", semanticTierSkipReason);
                traceCollector.markStageSkipped("entity_vector", semanticTierSkipReason);
                if (workingConfig.enableCompressedANN) {
                    skipped.push_back("compressed_ann");
                    traceCollector.markStageSkipped("compressed_ann", semanticTierSkipReason);
                }
            } else {
                if (kgStore_) {
                    if (workingConfig.graphUseQueryConcepts &&
                        workingConfig.waitForConceptExtraction) {
                        materializeConcepts(true);
                    }
                    kgFuture = schedule("kg", workingConfig.kgWeight, stats_.kgQueries,
                                        stats_.avgKgTimeMicros,
                                        [this, &query, &workingConfig, &concepts]() {
                                            YAMS_ZONE_SCOPED_N("component::kg");
                                            return queryKnowledgeGraph(
                                                query, workingConfig.kgMaxResults, &concepts);
                                        });
                }

                // Await embedding (ran in parallel with early lexical stages unless budgeted)
                // then schedule vector work if still needed.
                awaitEmbedding();
                if (queryEmbedding.has_value() && vectorDb_ && !hasVectorTierDimMismatch()) {
                    effectiveVectorMaxResults = workingConfig.vectorMaxResults;
                    effectiveEntityVectorMaxResults = workingConfig.entityVectorMaxResults;
                    vectorFuture =
                        schedule("vector", workingConfig.vectorWeight, stats_.vectorQueries,
                                 stats_.avgVectorTimeMicros,
                                 [&queryEmbedding, &workingConfig, &queryVectorWithRelaxedRetry]() {
                                     YAMS_ZONE_SCOPED_N("component::vector");
                                     return queryVectorWithRelaxedRetry(
                                         queryEmbedding.value(), workingConfig.vectorMaxResults);
                                 });

                    entityVectorFuture = schedule(
                        "entity_vector", workingConfig.entityVectorWeight,
                        stats_.entityVectorQueries, stats_.avgEntityVectorTimeMicros,
                        [this, &queryEmbedding, &workingConfig]() {
                            YAMS_ZONE_SCOPED_N("component::entity_vector");
                            return queryEntityVectors(queryEmbedding.value(), workingConfig,
                                                      workingConfig.entityVectorMaxResults);
                        });

                    if (workingConfig.enableCompressedANN) {
                        compressedAnnFuture = schedule(
                            "compressed_ann", workingConfig.compressedAnnWeight,
                            stats_.vectorQueries, stats_.avgVectorTimeMicros,
                            [this, &queryEmbedding, &workingConfig]() {
                                YAMS_ZONE_SCOPED_N("component::compressed_ann");
                                return queryCompressedANN(queryEmbedding.value(), workingConfig,
                                                          workingConfig.compressedAnnTopK);
                            });
                    }
                } else if (hasVectorTierDimMismatch()) {
                    markVectorTierDimMismatch();
                }

                collectIf(kgFuture, "kg", stats_.kgQueries, stats_.avgKgTimeMicros);
                collectIf(vectorFuture, "vector", stats_.vectorQueries, stats_.avgVectorTimeMicros);
                collectIf(entityVectorFuture, "entity_vector", stats_.entityVectorQueries,
                          stats_.avgEntityVectorTimeMicros);
                compressedAnnAttempted = compressedAnnFuture.valid();
                collectIf(compressedAnnFuture, "compressed_ann", stats_.vectorQueries,
                          stats_.avgVectorTimeMicros);
            }
        }
    } else {
        auto runSequential = [&](auto queryFn, const char* name, float weight,
                                 std::atomic<uint64_t>& queryCount,
                                 std::atomic<uint64_t>& avgTime) {
            if (weight <= 0.0f) {
                traceCollector.markStageSkipped(name, "disabled_by_weight");
                return;
            }

            traceCollector.markStageAttempted(name);
            YAMS_ZONE_SCOPED_N(name);
            auto start = std::chrono::steady_clock::now();
            auto results = queryFn();
            auto end = std::chrono::steady_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            componentTiming[name] = duration;

            if (results) {
                traceCollector.markStageResult(name, results.value(), duration,
                                               !results.value().empty());
                if (!results.value().empty()) {
                    allComponentResults.insert(allComponentResults.end(), results.value().begin(),
                                               results.value().end());
                    contributing.push_back(name);
                }
                queryCount.fetch_add(1, std::memory_order_relaxed);
                avgTime.store(duration, std::memory_order_relaxed);
            } else {
                failed.push_back(name);
                traceCollector.markStageFailure(name, duration);
            }
        };

        runSequential(
            [&]() {
                return queryFullText(query, intent, workingConfig, workingConfig.textMaxResults,
                                     &textExpansionStats, &graphExpansionTerms);
            },
            "text", workingConfig.textWeight, stats_.textQueries, stats_.avgTextTimeMicros);

        runSequential([&]() { return queryPathTree(query, workingConfig.pathTreeMaxResults); },
                      "path", workingConfig.pathTreeWeight, stats_.pathTreeQueries,
                      stats_.avgPathTreeTimeMicros);

        if (!params.tags.empty()) {
            runSequential(
                [&]() {
                    return queryTags(params.tags, params.matchAllTags, workingConfig.tagMaxResults);
                },
                "tag", workingConfig.tagWeight, stats_.tagQueries, stats_.avgTagTimeMicros);
        }

        runSequential([&]() { return queryMetadata(params, workingConfig.metadataMaxResults); },
                      "metadata", workingConfig.metadataWeight, stats_.metadataQueries,
                      stats_.avgMetadataTimeMicros);

        const bool strongBudgetLexical =
            semanticBudgetActive && budgetGuardStrongLexical(allComponentResults);
        if (semanticBudgetActive && strongBudgetLexical) {
            semanticTierSkipped = true;
            semanticTierSkipReason = "budget_guard_strong_lexical";
            response.debugStats["semantic_budget_skip_reason"] = semanticTierSkipReason;
            skipped.push_back("kg");
            skipped.push_back("vector");
            skipped.push_back("entity_vector");
            traceCollector.markStageSkipped("kg", semanticTierSkipReason);
            traceCollector.markStageSkipped("vector", semanticTierSkipReason);
            traceCollector.markStageSkipped("entity_vector", semanticTierSkipReason);
            if (workingConfig.enableCompressedANN) {
                skipped.push_back("compressed_ann");
                traceCollector.markStageSkipped("compressed_ann", semanticTierSkipReason);
            }
        } else {
            if (kgStore_) {
                if (workingConfig.graphUseQueryConcepts && workingConfig.waitForConceptExtraction) {
                    materializeConcepts(true);
                }
                runSequential(
                    [&]() {
                        return queryKnowledgeGraph(query, workingConfig.kgMaxResults, &concepts);
                    },
                    "kg", workingConfig.kgWeight, stats_.kgQueries, stats_.avgKgTimeMicros);
            }

            // Await embedding after lexical stages if budgeted, otherwise it may already be ready.
            awaitEmbedding();
            if (queryEmbedding.has_value() && vectorDb_ && !hasVectorTierDimMismatch()) {
                effectiveVectorMaxResults = workingConfig.vectorMaxResults;
                effectiveEntityVectorMaxResults = workingConfig.entityVectorMaxResults;
                runSequential(
                    [&]() {
                        return queryVectorWithRelaxedRetry(queryEmbedding.value(),
                                                           workingConfig.vectorMaxResults);
                    },
                    "vector", workingConfig.vectorWeight, stats_.vectorQueries,
                    stats_.avgVectorTimeMicros);

                runSequential(
                    [&]() {
                        return queryEntityVectors(queryEmbedding.value(), workingConfig,
                                                  workingConfig.entityVectorMaxResults);
                    },
                    "entity_vector", workingConfig.entityVectorWeight, stats_.entityVectorQueries,
                    stats_.avgEntityVectorTimeMicros);

                if (workingConfig.enableCompressedANN) {
                    runSequential(
                        [&]() {
                            return queryCompressedANN(queryEmbedding.value(), workingConfig,
                                                      workingConfig.compressedAnnTopK);
                        },
                        "compressed_ann", workingConfig.compressedAnnWeight, stats_.vectorQueries,
                        stats_.avgVectorTimeMicros);
                }
            } else if (hasVectorTierDimMismatch()) {
                markVectorTierDimMismatch();
            }
        }
    }

    if (!compressedAnnEnabled) {
        compressedAnnSkipReason = "disabled";
    } else if (!queryEmbedding.has_value()) {
        compressedAnnSkipReason = "no_query_embedding";
    } else if (!vectorDb_) {
        compressedAnnSkipReason = "no_vector_db";
    } else if (!vectorTierSkipReason.empty()) {
        compressedAnnSkipReason = vectorTierSkipReason;
    } else if (semanticTierSkipped && !compressedAnnAttempted) {
        compressedAnnSkipReason =
            semanticTierSkipReason.empty() ? "adaptive_vector_skip" : semanticTierSkipReason;
    } else {
        size_t preDedupCompressedAnnCount = static_cast<size_t>(std::count_if(
            allComponentResults.begin(), allComponentResults.end(), [](const auto& comp) {
                auto it = comp.debugInfo.find("compressed_ann");
                return it != comp.debugInfo.end() && it->second == "true";
            }));
        std::unordered_set<std::string> exactVectorDocIds;
        std::unordered_set<std::string> compressedAnnDocIds;
        exactVectorDocIds.reserve(allComponentResults.size());
        compressedAnnDocIds.reserve(allComponentResults.size());
        for (const auto& comp : allComponentResults) {
            if (comp.source == ComponentResult::Source::Vector) {
                const auto docId = documentIdForTrace(comp.filePath, comp.documentHash);
                if (!docId.empty()) {
                    exactVectorDocIds.insert(docId);
                }
            }
        }
        if (!exactVectorDocIds.empty()) {
            allComponentResults.erase(
                std::remove_if(allComponentResults.begin(), allComponentResults.end(),
                               [&](const ComponentResult& comp) {
                                   if (comp.source != ComponentResult::Source::CompressedANN) {
                                       return false;
                                   }
                                   const auto docId =
                                       documentIdForTrace(comp.filePath, comp.documentHash);
                                   if (docId.empty()) {
                                       return false;
                                   }
                                   if (exactVectorDocIds.contains(docId)) {
                                       return true;
                                   }
                                   return !compressedAnnDocIds.insert(docId).second;
                               }),
                allComponentResults.end());
        }
        compressedAnnResultCount = static_cast<size_t>(std::count_if(
            allComponentResults.begin(), allComponentResults.end(), [](const auto& comp) {
                auto it = comp.debugInfo.find("compressed_ann");
                return it != comp.debugInfo.end() && it->second == "true";
            }));
        compressedAnnApplied = compressedAnnResultCount > 0;
        if (compressedAnnApplied) {
            compressedAnnSkipReason = "applied";
        } else if (preDedupCompressedAnnCount > 0) {
            compressedAnnSkipReason = "all_deduped";
        } else {
            compressedAnnSkipReason = "no_results";
        }
    }

    if (workingConfig.enableGraphQueryExpansion && kgStore_ && metadataRepo_ &&
        !allComponentResults.empty()) {
        const auto seedDocs =
            collectGraphSeedDocs(allComponentResults, workingConfig.graphExpansionMaxSeeds * 2);
        docSeedGraphTerms = generateGraphExpansionTermsFromDocuments(
            kgStore_, query, concepts, seedDocs,
            GraphExpansionConfig{.maxTerms = workingConfig.graphExpansionMaxTerms,
                                 .maxSeeds = workingConfig.graphExpansionMaxSeeds,
                                 .maxNeighbors = workingConfig.graphMaxNeighbors});
        graphDocExpansionTermCount = docSeedGraphTerms.size();

        if (!docSeedGraphTerms.empty()) {
            std::unordered_set<std::string> existingDocIds;
            existingDocIds.reserve(allComponentResults.size() * 2);
            for (const auto& comp : allComponentResults) {
                const auto docId = documentIdForTrace(comp.filePath, comp.documentHash);
                if (!docId.empty()) {
                    existingDocIds.insert(docId);
                }
            }

            for (const auto& term : docSeedGraphTerms) {
                auto searchResults = metadataRepo_->search(
                    term.text, static_cast<int>(workingConfig.textMaxResults), 0);
                if (!searchResults) {
                    continue;
                }
                graphDocExpansionFtsHitCount += searchResults.value().results.size();

                double minBm25 = 0.0;
                double maxBm25 = 0.0;
                bool rangeInitialized = false;
                for (const auto& sr : searchResults.value().results) {
                    if (!rangeInitialized) {
                        minBm25 = maxBm25 = sr.score;
                        rangeInitialized = true;
                    } else {
                        minBm25 = std::min(minBm25, sr.score);
                        maxBm25 = std::max(maxBm25, sr.score);
                    }
                }

                const size_t startRank = allComponentResults.size();
                for (size_t rank = 0; rank < searchResults.value().results.size(); ++rank) {
                    const auto& sr = searchResults.value().results[rank];
                    const auto docId =
                        documentIdForTrace(sr.document.filePath, sr.document.sha256Hash);
                    if (!docId.empty() && existingDocIds.contains(docId)) {
                        continue;
                    }

                    ComponentResult cr;
                    cr.documentHash = sr.document.sha256Hash;
                    cr.filePath = sr.document.filePath;
                    cr.score =
                        std::clamp(workingConfig.graphExpansionFtsPenalty *
                                       std::clamp(term.score, 0.2f, 1.0f) *
                                       normalizedBm25Score(sr.score, workingConfig.bm25NormDivisor,
                                                           minBm25, maxBm25),
                                   0.0f, 1.0f);
                    if (cr.score < workingConfig.graphTextMinAdmissionScore) {
                        ++textExpansionStats.graphTextBlockedLowScoreCount;
                        continue;
                    }
                    cr.source = ComponentResult::Source::GraphText;
                    cr.rank = startRank + rank;
                    cr.snippet =
                        sr.snippet.empty() ? std::nullopt : std::optional<std::string>(sr.snippet);
                    cr.debugInfo["graph_doc_expansion_term"] = term.text;
                    allComponentResults.push_back(std::move(cr));
                    if (!docId.empty()) {
                        existingDocIds.insert(docId);
                    }
                    ++graphDocExpansionFtsAddedCount;
                }
            }

            if (graphDocExpansionFtsAddedCount > 0) {
                contributing.push_back("graph_doc_text");
            }
            spdlog::debug(
                "graph_doc_text: seed_docs={} terms={} raw_hits={} added={} for query '{}'",
                seedDocs.size(), docSeedGraphTerms.size(), graphDocExpansionFtsHitCount,
                graphDocExpansionFtsAddedCount, query.substr(0, 60));
        }
    }

    // ============================================================================
    // Auxiliary vector expansion: sub-phrases and graph-derived labels both run additional
    // vector searches and merge into the vector component before fusion.
    // ============================================================================
    if ((workingConfig.enableMultiVectorQuery || workingConfig.enableGraphQueryExpansion) &&
        queryEmbedding.has_value() && vectorDb_ && embeddingGen_) {
        YAMS_ZONE_SCOPED_N("multi_vector::sub_phrase_search");
        auto mvStart = std::chrono::steady_clock::now();

        auto subPhrases = generateQuerySubPhrases(query, workingConfig.multiVectorMaxPhrases);
        if (!workingConfig.enableMultiVectorQuery) {
            subPhrases.clear();
        }
        multiVectorGeneratedPhrases = subPhrases.size();
        if (!graphExpansionMaterialized) {
            materializeGraphExpansionTerms(workingConfig.waitForConceptExtraction);
        }
        auto graphTerms = graphExpansionTerms;
        for (const auto& term : docSeedGraphTerms) {
            auto it = std::find_if(graphTerms.begin(), graphTerms.end(), [&](const auto& existing) {
                return existing.text == term.text;
            });
            if (it == graphTerms.end()) {
                graphTerms.push_back(term);
            } else {
                it->score = std::max(it->score, term.score);
            }
        }
        std::stable_sort(graphTerms.begin(), graphTerms.end(),
                         [](const auto& a, const auto& b) { return a.score > b.score; });
        if (graphTerms.size() > workingConfig.graphExpansionMaxTerms) {
            graphTerms.resize(workingConfig.graphExpansionMaxTerms);
        }
        if (!workingConfig.enableGraphQueryExpansion ||
            textExpansionStats.graphExpansionFtsAddedCount == 0) {
            graphTerms.clear();
        }
        graphVectorGeneratedTerms = graphTerms.size();
        size_t multiVecHits = 0;
        size_t baseVectorCount = 0;
        std::vector<ComponentResult> mergedVectorResults;
        std::vector<ComponentResult> mergedGraphVectorResults;

        if (!subPhrases.empty() || !graphTerms.empty()) {
            spdlog::debug("multi_vector: generated {} sub-phrases from query '{}'",
                          subPhrases.size(), query.substr(0, 60));
            spdlog::debug("graph_vector: generated {} graph terms from query '{}'",
                          graphTerms.size(), query.substr(0, 60));

            std::vector<ComponentResult> nonVectorResults;
            nonVectorResults.reserve(allComponentResults.size());

            std::unordered_map<std::string, size_t> bestVectorByHash;
            bestVectorByHash.reserve(workingConfig.vectorMaxResults * 2);
            std::unordered_map<std::string, size_t> bestGraphVectorByHash;
            bestGraphVectorByHash.reserve(workingConfig.vectorMaxResults * 2);
            std::unordered_set<std::string> graphVectorCorroboratedHashes;
            graphVectorCorroboratedHashes.reserve(allComponentResults.size() * 2);
            std::unordered_set<std::string> graphVectorTextAnchoredHashes;
            graphVectorTextAnchoredHashes.reserve(allComponentResults.size() * 2);
            std::unordered_set<std::string> graphVectorBaselineTextAnchoredHashes;
            graphVectorBaselineTextAnchoredHashes.reserve(allComponentResults.size() * 2);

            for (const auto& cr : allComponentResults) {
                if (cr.source == ComponentResult::Source::Vector ||
                    cr.source == ComponentResult::Source::CompressedANN ||
                    cr.source == ComponentResult::Source::EntityVector) {
                    if (cr.documentHash.empty()) {
                        continue;
                    }

                    ++baseVectorCount;
                    graphVectorCorroboratedHashes.insert(cr.documentHash);
                    auto it = bestVectorByHash.find(cr.documentHash);
                    if (it == bestVectorByHash.end()) {
                        bestVectorByHash.emplace(cr.documentHash, mergedVectorResults.size());
                        mergedVectorResults.push_back(cr);
                    } else if (cr.score > mergedVectorResults[it->second].score) {
                        mergedVectorResults[it->second] = cr;
                    }
                    continue;
                }
                if (cr.source == ComponentResult::Source::GraphVector) {
                    if (cr.documentHash.empty()) {
                        continue;
                    }
                    auto it = bestGraphVectorByHash.find(cr.documentHash);
                    if (it == bestGraphVectorByHash.end()) {
                        bestGraphVectorByHash.emplace(cr.documentHash,
                                                      mergedGraphVectorResults.size());
                        mergedGraphVectorResults.push_back(cr);
                    } else if (cr.score > mergedGraphVectorResults[it->second].score) {
                        mergedGraphVectorResults[it->second] = cr;
                    }
                    continue;
                }
                {
                    if (!cr.documentHash.empty()) {
                        if (isTextAnchoringComponent(cr.source) ||
                            cr.source == ComponentResult::Source::KnowledgeGraph) {
                            graphVectorCorroboratedHashes.insert(cr.documentHash);
                        }
                        if (cr.source == ComponentResult::Source::Text ||
                            cr.source == ComponentResult::Source::GraphText ||
                            cr.source == ComponentResult::Source::PathTree ||
                            cr.source == ComponentResult::Source::KnowledgeGraph ||
                            cr.source == ComponentResult::Source::Tag ||
                            cr.source == ComponentResult::Source::Metadata ||
                            cr.source == ComponentResult::Source::Symbol) {
                            graphVectorTextAnchoredHashes.insert(cr.documentHash);
                        }
                        if (cr.source == ComponentResult::Source::Text ||
                            cr.source == ComponentResult::Source::PathTree ||
                            cr.source == ComponentResult::Source::KnowledgeGraph ||
                            cr.source == ComponentResult::Source::Symbol) {
                            graphVectorBaselineTextAnchoredHashes.insert(cr.documentHash);
                        }
                    }
                    nonVectorResults.push_back(cr);
                }
            }

            const float decay = std::clamp(workingConfig.multiVectorScoreDecay, 0.1f, 1.0f);
            const float graphPenalty =
                std::clamp(workingConfig.graphExpansionVectorPenalty, 0.1f, 1.0f);

            for (size_t pi = 0; pi < subPhrases.size(); ++pi) {
                try {
                    auto subEmbedding = embeddingGen_->generateEmbedding(subPhrases[pi]);
                    if (subEmbedding.empty()) {
                        continue;
                    }

                    auto subResults = queryVectorIndex(subEmbedding, workingConfig,
                                                       workingConfig.vectorMaxResults);
                    if (!subResults || subResults.value().empty()) {
                        continue;
                    }
                    multiVectorPhraseHits += subResults.value().size();

                    for (const auto& rawResult : subResults.value()) {
                        ComponentResult cr = rawResult;
                        if (cr.documentHash.empty()) {
                            continue;
                        }

                        cr.score *= decay;
                        cr.debugInfo["multi_vector_phrase"] = subPhrases[pi];
                        cr.debugInfo["multi_vector_phrase_idx"] = std::to_string(pi);

                        auto it = bestVectorByHash.find(cr.documentHash);
                        if (it == bestVectorByHash.end()) {
                            bestVectorByHash.emplace(cr.documentHash, mergedVectorResults.size());
                            mergedVectorResults.push_back(std::move(cr));
                            ++multiVecHits;
                            ++multiVectorAddedNewCount;
                        } else if (cr.score > mergedVectorResults[it->second].score) {
                            mergedVectorResults[it->second] = std::move(cr);
                            ++multiVecHits;
                            ++multiVectorReplacedBaseCount;
                        }
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("multi_vector: sub-phrase embedding failed for '{}': {}",
                                 subPhrases[pi], e.what());
                }
            }

            for (size_t gi = 0; gi < graphTerms.size(); ++gi) {
                try {
                    auto graphEmbedding = embeddingGen_->generateEmbedding(graphTerms[gi].text);
                    if (graphEmbedding.empty()) {
                        continue;
                    }
                    auto graphResults = queryVectorIndex(graphEmbedding, workingConfig,
                                                         workingConfig.vectorMaxResults);
                    if (!graphResults || graphResults.value().empty()) {
                        continue;
                    }
                    graphVectorRawHitCount += graphResults.value().size();

                    for (const auto& rawResult : graphResults.value()) {
                        ComponentResult cr = rawResult;
                        if (cr.documentHash.empty()) {
                            continue;
                        }
                        if (workingConfig.graphVectorRequireCorroboration &&
                            !graphVectorCorroboratedHashes.contains(cr.documentHash)) {
                            ++graphVectorBlockedUncorroboratedCount;
                            continue;
                        }
                        if (workingConfig.graphVectorRequireTextAnchoring &&
                            !graphVectorTextAnchoredHashes.contains(cr.documentHash)) {
                            ++graphVectorBlockedMissingTextAnchorCount;
                            continue;
                        }
                        if (workingConfig.graphVectorRequireBaselineTextAnchoring &&
                            !graphVectorBaselineTextAnchoredHashes.contains(cr.documentHash)) {
                            ++graphVectorBlockedMissingBaselineTextAnchorCount;
                            continue;
                        }
                        cr.source = ComponentResult::Source::GraphVector;
                        cr.score *= (graphPenalty * std::clamp(graphTerms[gi].score, 0.2f, 1.0f));
                        cr.debugInfo["graph_vector_term"] = graphTerms[gi].text;
                        cr.debugInfo["graph_vector_term_idx"] = std::to_string(gi);

                        auto it = bestGraphVectorByHash.find(cr.documentHash);
                        if (it == bestGraphVectorByHash.end()) {
                            bestGraphVectorByHash.emplace(cr.documentHash,
                                                          mergedGraphVectorResults.size());
                            mergedGraphVectorResults.push_back(std::move(cr));
                            ++graphVectorAddedNewCount;
                        } else if (cr.score > mergedGraphVectorResults[it->second].score) {
                            mergedGraphVectorResults[it->second] = std::move(cr);
                            ++graphVectorReplacedBaseCount;
                        }
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("graph_vector: term embedding failed for '{}': {}",
                                 graphTerms[gi].text, e.what());
                }
            }

            std::sort(mergedVectorResults.begin(), mergedVectorResults.end(),
                      [](const auto& a, const auto& b) { return a.score > b.score; });
            if (mergedVectorResults.size() > workingConfig.vectorMaxResults) {
                mergedVectorResults.resize(workingConfig.vectorMaxResults);
            }
            for (size_t i = 0; i < mergedVectorResults.size(); ++i) {
                mergedVectorResults[i].rank = i;
            }

            std::sort(mergedGraphVectorResults.begin(), mergedGraphVectorResults.end(),
                      [](const auto& a, const auto& b) { return a.score > b.score; });
            if (mergedGraphVectorResults.size() > workingConfig.vectorMaxResults) {
                mergedGraphVectorResults.resize(workingConfig.vectorMaxResults);
            }
            for (size_t i = 0; i < mergedGraphVectorResults.size(); ++i) {
                mergedGraphVectorResults[i].rank = i;
            }

            allComponentResults = std::move(nonVectorResults);
            allComponentResults.insert(allComponentResults.end(), mergedVectorResults.begin(),
                                       mergedVectorResults.end());
            allComponentResults.insert(allComponentResults.end(), mergedGraphVectorResults.begin(),
                                       mergedGraphVectorResults.end());
        }

        auto mvEnd = std::chrono::steady_clock::now();
        auto mvDuration =
            std::chrono::duration_cast<std::chrono::microseconds>(mvEnd - mvStart).count();
        componentTiming["multi_vector"] = mvDuration;
        if (multiVecHits > 0) {
            contributing.push_back("multi_vector");
        }
        if (graphVectorAddedNewCount > 0 || graphVectorReplacedBaseCount > 0) {
            contributing.push_back("graph_vector");
        }
        std::vector<ComponentResult> traceMultiVectorResults;
        for (const auto& comp : allComponentResults) {
            if (comp.source == ComponentResult::Source::Vector ||
                comp.source == ComponentResult::Source::CompressedANN ||
                comp.source == ComponentResult::Source::EntityVector ||
                comp.source == ComponentResult::Source::GraphVector) {
                traceMultiVectorResults.push_back(comp);
            }
        }
        traceCollector.markStageResult("multi_vector", traceMultiVectorResults, mvDuration,
                                       multiVecHits > 0 || graphVectorAddedNewCount > 0 ||
                                           graphVectorReplacedBaseCount > 0);
        spdlog::debug(
            "multi_vector: merged {} vector updates from {} sub-phrases "
            "(raw_hits={}, added={}, replaced={}, base_vectors={}, final_components={}) in {}us",
            multiVecHits, subPhrases.size(), multiVectorPhraseHits, multiVectorAddedNewCount,
            multiVectorReplacedBaseCount, baseVectorCount, allComponentResults.size(), mvDuration);
        spdlog::debug(
            "graph_vector: merged graph terms={} raw_hits={} added={} replaced={} in {}us",
            graphTerms.size(), graphVectorRawHitCount, graphVectorAddedNewCount,
            graphVectorReplacedBaseCount, mvDuration);
    }

    YAMS_PLOT("component_results_count", static_cast<int64_t>(allComponentResults.size()));

    // Opt-in diagnostic: help explain "0 results" situations by showing per-component
    // hit counts (pre-fusion) and whether an embedding was available.
    // NOLINTNEXTLINE(concurrency-mt-unsafe) — debug env var; not modified concurrently
    if (const char* env = std::getenv("YAMS_SEARCH_DIAG"); env && std::string(env) == "1") {
        const bool embeddingsAvailable = queryEmbedding.has_value();
        spdlog::warn("[search_diag] query='{}' components: contributing={} failed={} timed_out={} "
                     "skipped={} embedding={} pre_fusion_total={} "
                     "weights(text={},vector={},entity_vector={},kg={},path={},tag={},meta={})",
                     query, contributing.size(), failed.size(), timedOut.size(), skipped.size(),
                     embeddingsAvailable ? "yes" : "no", allComponentResults.size(),
                     workingConfig.textWeight, workingConfig.vectorWeight,
                     workingConfig.entityVectorWeight, workingConfig.kgWeight,
                     workingConfig.pathTreeWeight, workingConfig.tagWeight,
                     workingConfig.metadataWeight);
        // Log per-component timing if available
        if (!componentTiming.empty()) {
            std::string timingStr;
            for (const auto& [name, micros] : componentTiming) {
                if (!timingStr.empty())
                    timingStr += ", ";
                timingStr += fmt::format("{}={}us", name, micros);
            }
            spdlog::warn("[search_diag] timing: {}", timingStr);
        }
    }

    preFusionDocIds = collectUniqueComponentDocIds(allComponentResults);
    if (stageTraceEnabled || workingConfig.semanticRescueSlots > 0) {
        preFusionSignals = buildPreFusionSignalMap(allComponentResults);
    }

    // P7: adaptive convex-fusion switch. Once the SearchTuner reports convergence
    // and the user has opted in via enableAdaptiveFusion, override the configured
    // fusion strategy to CONVEX. The convex dispatch itself falls back to RRF on
    // any exception so this is fail-soft.
    bool adaptiveFusionApplied = false;
    bool tunerConvergedForFusion = false;
    if (tuner_) {
        tunerConvergedForFusion = tuner_->hasConverged();
    }
    if (workingConfig.enableAdaptiveFusion && tunerConvergedForFusion &&
        workingConfig.fusionStrategy != SearchEngineConfig::FusionStrategy::CONVEX) {
        workingConfig.fusionStrategy = SearchEngineConfig::FusionStrategy::CONVEX;
        adaptiveFusionApplied = true;
    }
    response.debugStats["fusion_strategy"] =
        SearchEngineConfig::fusionStrategyToString(workingConfig.fusionStrategy);
    response.debugStats["fusion_adaptive_applied"] = adaptiveFusionApplied ? "1" : "0";
    response.debugStats["search_tuner_converged"] = tunerConvergedForFusion ? "1" : "0";

    ResultFusion fusion(workingConfig);
    {
        YAMS_ZONE_SCOPED_N("fusion::results");
        response.results = fusion.fuse(allComponentResults);
    }

    // TurboQuant packed-code reranking: operates on fused results using compressed codes
    // without full float reconstruction. Runs after fusion but before graph reranking.
    bool turboQuantRerankApplied = false;
    const bool turboQuantRerankEnabled = workingConfig.enableTurboQuantRerank;
    size_t turboQuantRequestedWindow = 0;
    size_t turboQuantCandidateCount = 0;
    size_t turboQuantPackedCandidatesScored = 0;
    std::string turboQuantSkipReason = turboQuantRerankEnabled ? "not_attempted" : "disabled";
    if (!workingConfig.enableTurboQuantRerank) {
        turboQuantSkipReason = "disabled";
    } else if (!queryEmbedding.has_value()) {
        if (embeddingFailed) {
            turboQuantSkipReason = "embedding_failed";
        } else if (!needsEmbedding) {
            turboQuantSkipReason = "embedding_not_needed";
        } else if (!embeddingStarted) {
            turboQuantSkipReason = "embedding_not_started";
        } else if (!embeddingAwaited) {
            turboQuantSkipReason = "embedding_not_awaited";
        } else if (embeddingStatus == "empty_result") {
            turboQuantSkipReason = "empty_query_embedding";
        } else if (semanticTierSkipped) {
            turboQuantSkipReason =
                semanticTierSkipReason.empty() ? "semantic_tier_skipped" : semanticTierSkipReason;
        } else {
            turboQuantSkipReason = "no_query_embedding";
        }
    } else if (!vectorDb_) {
        turboQuantSkipReason = "no_vector_db";
    } else if (!vectorTierSkipReason.empty()) {
        turboQuantSkipReason = vectorTierSkipReason;
    } else {
        const size_t window =
            std::min(workingConfig.turboQuantRerankWindow, response.results.size());
        turboQuantRequestedWindow = window;
        if (window == 0) {
            turboQuantSkipReason = "zero_window";
        } else {
            const bool turboQuantHasCompressedAnnSignal =
                std::any_of(allComponentResults.begin(), allComponentResults.end(),
                            [](const ComponentResult& comp) {
                                return comp.source == ComponentResult::Source::CompressedANN;
                            });
            const bool turboQuantHasVectorSignal = std::any_of(
                response.results.begin(), response.results.begin() + static_cast<ptrdiff_t>(window),
                [](const SearchResult& result) {
                    return result.vectorScore.value_or(0.0) > 0.0 ||
                           result.graphVectorScore.value_or(0.0) > 0.0;
                });
            if (!turboQuantHasCompressedAnnSignal) {
                turboQuantSkipReason = "no_compressed_ann_signal";
            } else if (!turboQuantHasVectorSignal) {
                turboQuantSkipReason = "no_vector_signal";
            } else {
                // Lazy-init the reranker (built once per SearchEngine lifetime)
                if (!turboQuantReranker_) {
                    TurboQuantPackedRerankerConfig cfg;
                    cfg.dimension = workingConfig.turboQuantRerankDim;
                    cfg.bits_per_channel = workingConfig.turboQuantRerankBits;
                    cfg.seed = 42;
                    cfg.rerank_weight = workingConfig.turboQuantRerankWeight;
                    cfg.require_packed_codes =
                        workingConfig.turboQuantRerankOnlyWhenPackedAvailable;
                    turboQuantReranker_ = createTurboQuantPackedReranker(cfg);
                }
                if (turboQuantReranker_ && turboQuantReranker_->isReady()) {
                    YAMS_ZONE_SCOPED_N("turboquant::rerank");
                    // Transform query once
                    yams::vector::TurboQuantConfig tq_cfg;
                    tq_cfg.dimension = workingConfig.turboQuantRerankDim;
                    tq_cfg.bits_per_channel = workingConfig.turboQuantRerankBits;
                    tq_cfg.seed = 42;
                    yams::vector::TurboQuantMSE quantizer(tq_cfg);
                    auto y_q = quantizer.transformQuery(queryEmbedding.value());

                    // Collect top-N candidates
                    std::vector<SearchResult> topN;
                    topN.reserve(window);
                    for (size_t i = 0; i < window; ++i) {
                        topN.push_back(response.results[i]);
                    }

                    // Build reranker input
                    VectorRerankInput input;
                    input.transformed_query = std::move(y_q);
                    for (size_t i = 0; i < topN.size(); ++i) {
                        const auto& doc = topN[i].document;
                        // Try to get vector records for this document
                        auto records = vectorDb_->getVectorsByDocument(doc.sha256Hash);
                        if (!records.empty()) {
                            // Use the best-matching chunk for this document
                            input.candidates[topN[i].document.filePath] = records[0];
                            input.initial_scores[topN[i].document.filePath] =
                                static_cast<float>(topN[i].score);
                        }
                    }
                    turboQuantCandidateCount = input.candidates.size();

                    if (input.candidates.empty()) {
                        turboQuantSkipReason = "no_vector_records";
                    } else {
                        // Run reranking
                        auto rerankResult = turboQuantReranker_->rerank(input);
                        if (rerankResult) {
                            const auto& ranked = rerankResult.value();
                            turboQuantPackedCandidatesScored = ranked.packed_candidates_scored;
                            // Update reranker scores in the top-N results
                            for (size_t i = 0; i < window; ++i) {
                                const auto& doc = topN[i].document;
                                for (const auto& rc : ranked.candidates) {
                                    if (rc.chunk_id == doc.filePath) {
                                        response.results[i].rerankerScore =
                                            static_cast<double>(rc.rerank_score);
                                        // Blend score into the result score if rerankReplaceScores
                                        // is false
                                        if (!workingConfig.rerankReplaceScores) {
                                            const float w = workingConfig.turboQuantRerankWeight;
                                            response.results[i].score =
                                                (1.0 - w) * response.results[i].score +
                                                w * static_cast<double>(rc.rerank_score);
                                        }
                                        break;
                                    }
                                }
                            }

                            turboQuantRerankApplied = ranked.packed_candidates_scored > 0;
                            turboQuantSkipReason =
                                turboQuantRerankApplied ? "applied" : "no_packed_candidates";
                        } else {
                            turboQuantSkipReason = "rerank_failed";
                        }
                    }
                } else {
                    turboQuantSkipReason = "reranker_unready";
                }
            }
        }
    }

    const bool hasGraphSources =
        std::any_of(allComponentResults.begin(), allComponentResults.end(), [](const auto& comp) {
            return comp.source == ComponentResult::Source::GraphText ||
                   comp.source == ComponentResult::Source::GraphVector;
        });
    if ((stageTraceEnabled || (workingConfig.enableGraphFusionWindowGuard && hasGraphSources))) {
        postFusionSnapshot = response.results;
        std::vector<ComponentResult> graphlessComponentResults;
        graphlessComponentResults.reserve(allComponentResults.size());
        for (const auto& comp : allComponentResults) {
            if (comp.source == ComponentResult::Source::GraphText ||
                comp.source == ComponentResult::Source::GraphVector) {
                continue;
            }
            graphlessComponentResults.push_back(comp);
        }
        if (graphlessComponentResults.size() != allComponentResults.size()) {
            graphlessPostFusionSnapshot = fusion.fuse(graphlessComponentResults);
        } else {
            graphlessPostFusionSnapshot = postFusionSnapshot;
        }
    }

    if (workingConfig.enableGraphFusionWindowGuard && hasGraphSources &&
        !graphlessPostFusionSnapshot.empty()) {
        const size_t guardWindow =
            std::min(response.results.size(),
                     std::min(graphlessPostFusionSnapshot.size(),
                              (workingConfig.enableReranking && workingConfig.rerankTopK > 0)
                                  ? workingConfig.rerankTopK
                                  : size_t(50)));
        const size_t guardDepth =
            std::min(graphlessPostFusionSnapshot.size(),
                     guardWindow * workingConfig.graphFusionGuardDepthMultiplier);
        const auto graphlessTopIds =
            collectRankedResultDocIds(graphlessPostFusionSnapshot, guardWindow);
        const auto graphlessDepthIds =
            collectRankedResultDocIds(graphlessPostFusionSnapshot, guardDepth);
        const auto actualTopIds = collectRankedResultDocIds(response.results, guardWindow);
        std::unordered_set<std::string> graphlessTopSet(graphlessTopIds.begin(),
                                                        graphlessTopIds.end());
        std::unordered_set<std::string> graphlessDepthSet(graphlessDepthIds.begin(),
                                                          graphlessDepthIds.end());
        std::unordered_set<std::string> actualTopSet(actualTopIds.begin(), actualTopIds.end());
        std::unordered_map<std::string, SearchResult> graphlessById;
        graphlessById.reserve(guardDepth);
        for (size_t i = 0; i < guardDepth; ++i) {
            const auto docId =
                documentIdForTrace(graphlessPostFusionSnapshot[i].document.filePath,
                                   graphlessPostFusionSnapshot[i].document.sha256Hash);
            if (!docId.empty()) {
                graphlessById.emplace(docId, graphlessPostFusionSnapshot[i]);
            }
        }

        std::vector<std::string> displacedGraphlessIds;
        for (const auto& docId : graphlessTopIds) {
            if (!actualTopSet.contains(docId)) {
                displacedGraphlessIds.push_back(docId);
            }
        }

        size_t displacedCursor = 0;
        for (size_t i = 0; i < guardWindow && displacedCursor < displacedGraphlessIds.size(); ++i) {
            const auto actualId = documentIdForTrace(response.results[i].document.filePath,
                                                     response.results[i].document.sha256Hash);
            if (graphlessTopSet.contains(actualId)) {
                continue;
            }
            if (graphlessDepthSet.contains(actualId)) {
                continue; // allow graph to promote graphless near-misses
            }

            const auto& restoreId = displacedGraphlessIds[displacedCursor++];
            auto restoreIt = graphlessById.find(restoreId);
            if (restoreIt == graphlessById.end()) {
                continue;
            }
            response.results[i] = restoreIt->second;
            ++graphWindowGuardReplacementCount;
        }
        if (stageTraceEnabled) {
            postFusionSnapshot = response.results;
        }
    }

    if (workingConfig.graphMaxAddedInFusionWindow > 0 && hasGraphSources &&
        !graphlessPostFusionSnapshot.empty()) {
        const size_t guardWindow =
            std::min(response.results.size(),
                     std::min(graphlessPostFusionSnapshot.size(),
                              (workingConfig.enableReranking && workingConfig.rerankTopK > 0)
                                  ? workingConfig.rerankTopK
                                  : size_t(50)));
        const auto graphlessTopIds =
            collectRankedResultDocIds(graphlessPostFusionSnapshot, guardWindow);
        const auto actualTopIds = collectRankedResultDocIds(response.results, guardWindow);
        std::unordered_set<std::string> graphlessTopSet(graphlessTopIds.begin(),
                                                        graphlessTopIds.end());
        std::unordered_map<std::string, SearchResult> graphlessById;
        graphlessById.reserve(guardWindow);
        for (size_t i = 0; i < guardWindow; ++i) {
            const auto docId =
                documentIdForTrace(graphlessPostFusionSnapshot[i].document.filePath,
                                   graphlessPostFusionSnapshot[i].document.sha256Hash);
            if (!docId.empty()) {
                graphlessById.emplace(docId, graphlessPostFusionSnapshot[i]);
            }
        }

        std::vector<size_t> graphAddedIndices;
        graphAddedIndices.reserve(guardWindow);
        for (size_t i = 0; i < guardWindow; ++i) {
            const auto actualId = documentIdForTrace(response.results[i].document.filePath,
                                                     response.results[i].document.sha256Hash);
            if (!graphlessTopSet.contains(actualId)) {
                graphAddedIndices.push_back(i);
            }
        }

        if (graphAddedIndices.size() > workingConfig.graphMaxAddedInFusionWindow) {
            std::vector<std::string> restoreIds;
            restoreIds.reserve(graphAddedIndices.size());
            for (const auto& docId : graphlessTopIds) {
                const bool present =
                    std::any_of(actualTopIds.begin(), actualTopIds.end(),
                                [&](const auto& actual) { return actual == docId; });
                if (!present) {
                    restoreIds.push_back(docId);
                }
            }

            const size_t replacements =
                std::min(restoreIds.size(),
                         graphAddedIndices.size() - workingConfig.graphMaxAddedInFusionWindow);
            for (size_t ri = 0; ri < replacements; ++ri) {
                const size_t replaceIndex = graphAddedIndices[graphAddedIndices.size() - 1 - ri];
                auto restoreIt = graphlessById.find(restoreIds[ri]);
                if (restoreIt == graphlessById.end()) {
                    continue;
                }
                response.results[replaceIndex] = restoreIt->second;
                ++graphWindowCapReplacementCount;
            }
            if (stageTraceEnabled) {
                postFusionSnapshot = response.results;
            }
        }
    }

    if (workingConfig.enableGraphRerank && kgScorer_ && !response.results.empty()) {
        YAMS_ZONE_SCOPED_N("graph::rerank");
        const auto graphRerankStart = std::chrono::steady_clock::now();

        materializeConcepts(workingConfig.graphUseQueryConcepts &&
                            workingConfig.waitForConceptExtraction);
        graphQueryConceptCount = concepts.size();

        const size_t rerankWindow =
            std::min(workingConfig.graphRerankTopN, response.results.size());
        if (rerankWindow > 0) {
            KGScoringConfig graphCfg = kgScorer_->getConfig();
            graphCfg.max_neighbors = workingConfig.graphMaxNeighbors;
            graphCfg.max_hops = workingConfig.graphMaxHops;
            graphCfg.budget =
                std::chrono::milliseconds(std::max(0, workingConfig.graphScoringBudgetMs));
            graphCfg.enable_path_enumeration = workingConfig.graphEnablePathEnumeration;
            graphCfg.max_paths = workingConfig.graphMaxPaths;
            graphCfg.hop_decay = std::clamp(workingConfig.graphHopDecay, 0.0f, 1.0f);
            kgScorer_->setConfig(graphCfg);

            std::vector<std::string> candidateIds;
            candidateIds.reserve(rerankWindow);
            for (size_t i = 0; i < rerankWindow; ++i) {
                const auto& res = response.results[i];
                if (!res.document.sha256Hash.empty()) {
                    candidateIds.push_back(res.document.sha256Hash);
                } else if (!res.document.filePath.empty()) {
                    candidateIds.push_back(res.document.filePath);
                } else {
                    candidateIds.push_back(
                        documentIdForTrace(res.document.filePath, res.document.sha256Hash));
                }
            }

            std::string graphQuery = query;
            if (workingConfig.graphUseQueryConcepts && !concepts.empty()) {
                std::unordered_set<std::string> seenConcepts;
                for (const auto& conceptItem : concepts) {
                    if (conceptItem.text.empty() || !seenConcepts.insert(conceptItem.text).second) {
                        continue;
                    }
                    if (graphQuery.find(conceptItem.text) == std::string::npos) {
                        graphQuery.push_back(' ');
                        graphQuery += conceptItem.text;
                    }
                }
            }

            auto graphScoresResult = kgScorer_->score(graphQuery, candidateIds);
            if (graphScoresResult) {
                const auto& graphScores = graphScoresResult.value();
                const auto graphExplanations = kgScorer_->getLastExplanations();
                bool boosted = false;
                const float minSignal = std::max(0.0f, workingConfig.graphRerankMinSignal);
                const float maxBoost = std::max(0.0f, workingConfig.graphRerankMaxBoost);
                const float rerankWeight = std::max(0.0f, workingConfig.graphRerankWeight);
                const float communityWeight =
                    std::clamp(workingConfig.graphCommunityWeight, 0.0f, 1.0f);
                const float baseSignalWeight = std::max(0.0f, 1.0f - communityWeight);
                std::size_t graphCommunitySupportedDocs = 0;
                std::size_t graphCommunityEdges = 0;
                std::size_t graphLargestCommunity = 0;
                double graphCommunitySignalMass = 0.0;
                std::size_t graphCommunityBoostedDocs = 0;
                const auto communitySupport = computeReciprocalCommunitySupport(
                    kgStore_, candidateIds,
                    std::max<std::size_t>(8, workingConfig.graphMaxNeighbors),
                    workingConfig.graphCommunityReferenceSize, &graphCommunitySupportedDocs,
                    &graphCommunityEdges, &graphLargestCommunity,
                    workingConfig.graphCommunityDecayHalfLifeDays,
                    workingConfig.graphCommunityMinEdgeWeight);

                std::vector<float> rawSignals(rerankWindow, 0.0f);
                std::vector<float> lexicalAnchors(rerankWindow, 0.0f);
                std::vector<float> queryCoverages(rerankWindow, 0.0f);
                float maxRawSignal = 0.0f;
                float maxLexicalAnchor = 0.0f;
                size_t topSignalIndex = rerankWindow;

                for (size_t i = 0; i < rerankWindow; ++i) {
                    const auto& candidateId = candidateIds[i];
                    const auto& result = response.results[i];
                    auto scoreIt = graphScores.find(candidateId);
                    if (scoreIt != graphScores.end()) {
                        graphMatchedCandidates++;
                    }

                    const auto getFeature = [&scoreIt, &graphScores](const char* key) {
                        if (scoreIt == graphScores.end()) {
                            return 0.0f;
                        }
                        auto featureIt = scoreIt->second.features.find(key);
                        return featureIt != scoreIt->second.features.end() ? featureIt->second
                                                                           : 0.0f;
                    };

                    const float queryCoverage =
                        std::clamp(getFeature("feature_query_coverage_ratio"), 0.0f, 1.0f);
                    queryCoverages[i] = queryCoverage;
                    const float pathSupport =
                        std::clamp(getFeature("feature_path_support_score"), 0.0f, 1.0f);
                    const float entitySignal =
                        scoreIt != graphScores.end() ? scoreIt->second.entity : 0.0f;
                    const float structuralSignal =
                        scoreIt != graphScores.end() ? scoreIt->second.structural : 0.0f;
                    const float communitySignal = i < communitySupport.size()
                                                      ? std::clamp(communitySupport[i], 0.0f, 1.0f)
                                                      : 0.0f;
                    const float lexicalAnchor = std::clamp(
                        static_cast<float>(
                            result.keywordScore.value_or(0.0) + result.pathScore.value_or(0.0) +
                            result.tagScore.value_or(0.0) + result.symbolScore.value_or(0.0)),
                        0.0f, 1.0f);
                    lexicalAnchors[i] = lexicalAnchor;
                    maxLexicalAnchor = std::max(maxLexicalAnchor, lexicalAnchor);
                    graphCommunitySignalMass += communitySignal;

                    // Composite graph relevance signal (per-profile weights).
                    const float rawSignal =
                        std::clamp((entitySignal * workingConfig.graphEntitySignalWeight +
                                    structuralSignal * workingConfig.graphStructuralSignalWeight +
                                    queryCoverage * workingConfig.graphCoverageSignalWeight +
                                    pathSupport * workingConfig.graphPathSignalWeight) *
                                           baseSignalWeight +
                                       communitySignal * communityWeight,
                                   0.0f, 1.0f);
                    rawSignals[i] = rawSignal;
                    maxRawSignal = std::max(maxRawSignal, rawSignal);
                    if (rawSignal > 0.0f) {
                        graphPositiveSignalCandidates++;
                    }
                    if (topSignalIndex >= rerankWindow || rawSignal > rawSignals[topSignalIndex]) {
                        topSignalIndex = i;
                    }
                }
                graphMaxSignal = maxRawSignal;

                for (size_t i = 0; i < rerankWindow; ++i) {
                    const auto& candidateId = candidateIds[i];
                    auto scoreIt = graphScores.find(candidateId);
                    if (scoreIt == graphScores.end()) {
                        continue;
                    }

                    const float signal = rawSignals[i];
                    if (signal < minSignal) {
                        continue;
                    }

                    const float normalizedSignal =
                        maxRawSignal > 0.0f ? signal / maxRawSignal : 0.0f;
                    const float effectiveSignal =
                        std::clamp(signal * 0.6f + normalizedSignal * 0.4f, 0.0f, 1.0f);
                    const float lexicalAnchorRatio =
                        maxLexicalAnchor > 0.0f ? lexicalAnchors[i] / maxLexicalAnchor : 0.0f;
                    const float corrobFloor = workingConfig.graphCorroborationFloor;
                    const float corroboration =
                        std::clamp(corrobFloor + (1.0f - corrobFloor) * std::max(lexicalAnchorRatio,
                                                                                 queryCoverages[i]),
                                   0.0f, 1.0f);
                    const float rankPrior = 1.0f / std::sqrt(1.0f + static_cast<float>(i));
                    const float guardedSignal =
                        std::clamp(effectiveSignal * corroboration * rankPrior, 0.0f, 1.0f);

                    const float boost = std::min(maxBoost, rerankWeight * guardedSignal);
                    if (boost <= 0.0f) {
                        continue;
                    }

                    response.results[i].score *= (1.0 + static_cast<double>(boost));
                    response.results[i].kgScore = response.results[i].kgScore.value_or(0.0) + boost;
                    boosted = true;
                    graphBoostedDocs++;
                    if (communityWeight > 0.0f && i < communitySupport.size() &&
                        communitySupport[i] > 0.0f) {
                        graphCommunityBoostedDocs++;
                    }
                }

                if (!boosted && workingConfig.graphFallbackToTopSignal &&
                    topSignalIndex < rerankWindow && rawSignals[topSignalIndex] > 0.0f) {
                    const float fallbackBoost =
                        std::min(maxBoost * 0.5f, rerankWeight * rawSignals[topSignalIndex]);
                    if (fallbackBoost > 0.0f) {
                        response.results[topSignalIndex].score *=
                            (1.0 + static_cast<double>(fallbackBoost));
                        response.results[topSignalIndex].kgScore =
                            response.results[topSignalIndex].kgScore.value_or(0.0) + fallbackBoost;
                        boosted = true;
                        graphBoostedDocs++;
                    }
                }

                if (boosted) {
                    std::sort(response.results.begin(), response.results.end(),
                              [](const SearchResult& a, const SearchResult& b) {
                                  return a.score > b.score;
                              });
                    graphRerankApplied = true;
                    contributing.push_back("graph_rerank");
                } else {
                    skipped.push_back("graph_rerank");
                    traceCollector.markStageSkipped("graph_rerank", "no_positive_graph_signal");
                }

                if (stageTraceEnabled) {
                    json graphExplainJson = json::array();
                    for (size_t i = 0; i < std::min(graphExplanations.size(), rerankWindow); ++i) {
                        const auto& expl = graphExplanations[i];
                        const float communitySignal =
                            i < communitySupport.size()
                                ? std::clamp(communitySupport[i], 0.0f, 1.0f)
                                : 0.0f;
                        graphExplainJson.push_back({
                            {"doc_id", expl.id},
                            {"components", expl.components},
                            {"community_signal", communitySignal},
                            {"reasons", expl.reasons},
                        });
                    }
                    response.debugStats["graph_explanations_json"] = graphExplainJson.dump();
                    response.debugStats["graph_doc_probe_json"] =
                        buildGraphDocProbeJson(kgStore_, response.results, rerankWindow).dump();
                }
                response.debugStats["graph_community_supported_docs"] =
                    std::to_string(graphCommunitySupportedDocs);
                response.debugStats["graph_community_edge_count"] =
                    std::to_string(graphCommunityEdges);
                response.debugStats["graph_community_largest_size"] =
                    std::to_string(graphLargestCommunity);
                response.debugStats["graph_community_signal_mass"] =
                    fmt::format("{:.4f}", graphCommunitySignalMass);
                response.debugStats["graph_community_boosted_docs"] =
                    std::to_string(graphCommunityBoostedDocs);
                response.debugStats["graph_community_weight"] =
                    fmt::format("{:.4f}", communityWeight);
            } else {
                failed.push_back("graph_rerank");
                traceCollector.markStageFailure("graph_rerank");
                spdlog::debug("[graph_rerank] KG scoring failed: {}",
                              graphScoresResult.error().message);
            }
        }

        if (graphRerankApplied && !graphlessPostFusionSnapshot.empty()) {
            const size_t guardWindow =
                std::min(response.results.size(), std::min(graphlessPostFusionSnapshot.size(),
                                                           workingConfig.graphRerankTopN));
            if (guardWindow > 0) {
                const auto lexicalAnchorScore = [](const SearchResult& r) {
                    return r.keywordScore.value_or(0.0) + r.pathScore.value_or(0.0) +
                           r.tagScore.value_or(0.0) + r.symbolScore.value_or(0.0);
                };

                std::vector<std::string> protectedIds;
                protectedIds.reserve(guardWindow);
                std::unordered_map<std::string, SearchResult> graphlessById;
                graphlessById.reserve(guardWindow);
                for (size_t i = 0; i < guardWindow; ++i) {
                    const auto docId =
                        documentIdForTrace(graphlessPostFusionSnapshot[i].document.filePath,
                                           graphlessPostFusionSnapshot[i].document.sha256Hash);
                    if (docId.empty() ||
                        lexicalAnchorScore(graphlessPostFusionSnapshot[i]) <= 0.0) {
                        continue;
                    }
                    protectedIds.push_back(docId);
                    graphlessById.emplace(docId, graphlessPostFusionSnapshot[i]);
                }

                if (!protectedIds.empty()) {
                    const auto actualTopIds =
                        collectRankedResultDocIds(response.results, guardWindow);
                    std::unordered_set<std::string> protectedSet(protectedIds.begin(),
                                                                 protectedIds.end());
                    std::unordered_set<std::string> actualTopSet(actualTopIds.begin(),
                                                                 actualTopIds.end());

                    std::vector<std::string> displacedProtectedIds;
                    displacedProtectedIds.reserve(protectedIds.size());
                    for (const auto& docId : protectedIds) {
                        if (!actualTopSet.contains(docId)) {
                            displacedProtectedIds.push_back(docId);
                        }
                    }

                    size_t displacedCursor = 0;
                    for (size_t i = 0;
                         i < guardWindow && displacedCursor < displacedProtectedIds.size(); ++i) {
                        const auto actualId =
                            documentIdForTrace(response.results[i].document.filePath,
                                               response.results[i].document.sha256Hash);
                        if (protectedSet.contains(actualId)) {
                            continue;
                        }

                        auto restoreIt =
                            graphlessById.find(displacedProtectedIds[displacedCursor++]);
                        if (restoreIt == graphlessById.end()) {
                            continue;
                        }
                        response.results[i] = restoreIt->second;
                        ++graphRerankGuardReplacementCount;
                    }
                }
            }
        }

        const auto graphRerankEnd = std::chrono::steady_clock::now();
        componentTiming["graph_rerank"] =
            std::chrono::duration_cast<std::chrono::microseconds>(graphRerankEnd - graphRerankStart)
                .count();
        if (graphRerankApplied) {
            traceCollector.markStageResult("graph_rerank", {}, componentTiming["graph_rerank"],
                                           true);
        }
    }

    if (stageTraceEnabled) {
        postGraphSnapshot = response.results;
    }

    materializeConcepts(workingConfig.waitForConceptExtraction);

    // Cross-encoder reranking: second-stage ranking for improved relevance
    const bool rerankAvailable = reranker_ && reranker_->isReady();
    if (!workingConfig.enableReranking && rerankAvailable && !response.results.empty()) {
        skipped.push_back("reranker");
        traceCollector.markStageSkipped("reranker", "disabled_in_config");
    }
    if (workingConfig.enableReranking && rerankAvailable && !response.results.empty()) {
        YAMS_ZONE_SCOPED_N("reranking");
        traceCollector.markStageAttempted("reranker");
        const auto rerankStart = std::chrono::steady_clock::now();
        const size_t rerankProbeWindow =
            workingConfig.semanticRescueSlots > 0
                ? std::max(workingConfig.rerankTopK,
                           workingConfig.rerankTopK +
                               std::max<size_t>(200, workingConfig.semanticRescueSlots * 100))
                : workingConfig.rerankTopK;
        const size_t rerankWindow = std::min(rerankProbeWindow, response.results.size());

        if (workingConfig.useScoreBasedReranking && rerankWindow > 1) {
            std::stable_sort(response.results.begin(),
                             response.results.begin() + static_cast<ptrdiff_t>(rerankWindow),
                             [&](const SearchResult& a, const SearchResult& b) {
                                 return scoreBasedRerankSignal(a, intent, effectiveZoomLevel) >
                                        scoreBasedRerankSignal(b, intent, effectiveZoomLevel);
                             });
            contributing.push_back("score_rerank");
        }

        const int64_t nowMicros = std::chrono::duration_cast<std::chrono::microseconds>(
                                      std::chrono::steady_clock::now().time_since_epoch())
                                      .count();
        const int64_t cooldownUntilMicros =
            rerankerCooldownUntilMicros_.load(std::memory_order_relaxed);
        const bool rerankerCoolingDown = cooldownUntilMicros > nowMicros;

        if (rerankerCoolingDown) {
            skipped.push_back("reranker");
            traceCollector.markStageSkipped("reranker", "cooldown_active");
            const int64_t remainingMs = (cooldownUntilMicros - nowMicros) / 1000;
            spdlog::debug("[reranker] Cooldown active; skipping rerank ({} ms remaining)",
                          std::max<int64_t>(remainingMs, 0));
        }

        const auto preFusionSignalForResult =
            [&preFusionSignals](const SearchResult& result) -> const PreFusionDocSignal* {
            const std::string docId =
                documentIdForTrace(result.document.filePath, result.document.sha256Hash);
            if (docId.empty()) {
                return nullptr;
            }
            auto it = preFusionSignals.find(docId);
            return it != preFusionSignals.end() ? &it->second : nullptr;
        };

        bool skipRerank = false;
        if (!rerankerCoolingDown && rerankWindow >= 2 &&
            workingConfig.rerankScoreGapThreshold > 0.0f) {
            double bestRemainingFusedScore = response.results[1].score;
            bool hasCompetitiveAnchoredEvidence = false;
            rerankGuardAnchoredDocIds.clear();
            // Minimum score for a multi-source doc to count as "competitive".
            // 0.0 means any multi-source doc counts (original behavior).
            const double anchoredMinScore =
                workingConfig.rerankAnchoredMinRelativeScore > 0.0f
                    ? response.results[0].score *
                          static_cast<double>(workingConfig.rerankAnchoredMinRelativeScore)
                    : 0.0;
            for (size_t i = 1; i < rerankWindow; ++i) {
                bestRemainingFusedScore =
                    std::max(bestRemainingFusedScore, response.results[i].score);
                const auto* signal = preFusionSignalForResult(response.results[i]);
                if (signal != nullptr && signal->hasAnchoring && signal->sources.size() > 1 &&
                    response.results[i].score >= anchoredMinScore) {
                    hasCompetitiveAnchoredEvidence = true;
                    rerankGuardAnchoredDocIds.push_back(
                        documentIdForTrace(response.results[i].document.filePath,
                                           response.results[i].document.sha256Hash));
                }
            }
            const double scoreGap = response.results[0].score - bestRemainingFusedScore;
            rerankGuardScoreGap = scoreGap;
            rerankGuardCompetitiveAnchoredEvidence = hasCompetitiveAnchoredEvidence;
            if (scoreGap >= static_cast<double>(workingConfig.rerankScoreGapThreshold) &&
                !hasCompetitiveAnchoredEvidence) {
                spdlog::debug(
                    "[reranker] Skipping rerank (top fused gap {:.4f} >= {:.4f}; window={})",
                    scoreGap, workingConfig.rerankScoreGapThreshold, rerankWindow);
                skipRerank = true;
                traceCollector.markStageSkipped("reranker", "score_gap_guard");
            }
        }

        if (!rerankerCoolingDown && !skipRerank) {
            std::vector<SearchResult> preRerankSnapshot;
            if (stageTraceEnabled) {
                preRerankSnapshot.assign(response.results.begin(),
                                         response.results.begin() +
                                             static_cast<ptrdiff_t>(rerankWindow));
            }

            // Extract document snippets for reranking.
            // Fallback order: fused snippet -> metadata content preview -> source file.
            // Metadata preview is preferred because benchmark/warm-cache file paths may
            // point at transient ingestion paths that no longer exist.
            std::vector<std::string> snippets;
            std::vector<size_t> rerankPassageDocIndices;
            snippets.reserve(rerankWindow);
            rerankPassageDocIndices.reserve(rerankWindow);

            std::unordered_map<size_t, std::string> metadataPreviewByIndex;
            if (metadataRepo_ && rerankWindow > 0) {
                std::unordered_map<std::string, std::vector<size_t>> hashToIndices;
                hashToIndices.reserve(rerankWindow);
                for (size_t i = 0; i < rerankWindow; ++i) {
                    const bool snippetLooksLikePath =
                        !response.results[i].snippet.empty() &&
                        response.results[i].snippet == response.results[i].document.filePath;
                    if (!response.results[i].snippet.empty() && !snippetLooksLikePath) {
                        continue;
                    }
                    const std::string& hash = response.results[i].document.sha256Hash;
                    if (!hash.empty()) {
                        hashToIndices[hash].push_back(i);
                    }
                }

                if (!hashToIndices.empty()) {
                    std::vector<std::string> hashes;
                    hashes.reserve(hashToIndices.size());
                    for (const auto& [hash, _] : hashToIndices) {
                        hashes.push_back(hash);
                    }

                    const size_t previewLimit = std::max<size_t>(
                        workingConfig.rerankSnippetMaxChars + 64,
                        std::max<size_t>(workingConfig.rerankSnippetMaxChars * 4, 1024));

                    auto combinedResult = metadataRepo_->batchGetDocumentsWithContentPreview(
                        hashes, static_cast<int>(previewLimit));
                    if (combinedResult) {
                        metadataPreviewByIndex.reserve(rerankWindow);
                        for (const auto& [hash, docAndPreview] : combinedResult.value()) {
                            const auto& [docInfo, preview] = docAndPreview;
                            if (preview.empty()) {
                                continue;
                            }
                            auto it = hashToIndices.find(hash);
                            if (it == hashToIndices.end()) {
                                continue;
                            }
                            for (size_t idx : it->second) {
                                metadataPreviewByIndex[idx] = preview;
                            }
                        }
                    }
                }
            }

            for (size_t i = 0; i < rerankWindow; ++i) {
                std::string text;
                const bool snippetLooksLikePath =
                    !response.results[i].snippet.empty() &&
                    response.results[i].snippet == response.results[i].document.filePath;
                if (!response.results[i].snippet.empty() &&
                    !snippetLooksLikePath) { // NOLINT(bugprone-branch-clone) — third branch is a
                                             // deliberate fallback that accepts path-like snippets
                                             // when no metadata preview is available
                    text = response.results[i].snippet;
                } else if (auto it = metadataPreviewByIndex.find(i);
                           it != metadataPreviewByIndex.end()) {
                    text = it->second;
                } else if (!response.results[i].snippet.empty()) {
                    text = response.results[i].snippet;
                } else if (!response.results[i].document.filePath.empty()) {
                    // Fallback: read content from source file on disk
                    std::ifstream ifs(response.results[i].document.filePath,
                                      std::ios::in | std::ios::binary);
                    if (ifs) {
                        const size_t readLimit = workingConfig.rerankSnippetMaxChars + 64;
                        std::string buf(readLimit, '\0');
                        ifs.read(buf.data(), static_cast<std::streamsize>(readLimit));
                        buf.resize(static_cast<size_t>(ifs.gcount()));
                        if (!buf.empty()) {
                            text = std::move(buf);
                            spdlog::debug("[reranker] Loaded {} bytes from file for doc {}",
                                          text.size(), i);
                        }
                    }
                }

                if (!text.empty()) {
                    auto passages =
                        buildRerankPassages(query, text, workingConfig.rerankSnippetMaxChars);
                    for (auto& passage : passages) {
                        snippets.push_back(std::move(passage));
                        rerankPassageDocIndices.push_back(i);
                    }
                } else {
                    spdlog::debug("[reranker] Skipping doc {} (no snippet or file content)", i);
                }
            }

            if (!snippets.empty()) {
                auto rerankResult = reranker_->scoreDocuments(query, snippets);
                if (rerankResult) {
                    rerankerCooldownUntilMicros_.store(0, std::memory_order_relaxed);
                    const auto& scores = rerankResult.value();
                    spdlog::debug("[reranker] Reranked {} documents", scores.size());

                    // Diagnostic: log score distribution for debugging reranker quality
                    if (!scores.empty()) {
                        float minScore = *std::min_element(scores.begin(), scores.end());
                        float maxScore = *std::max_element(scores.begin(), scores.end());
                        float sumScore = 0.0f;
                        for (float s : scores)
                            sumScore += s;
                        spdlog::info("[reranker] Score distribution: n={} min={:.4f} max={:.4f} "
                                     "mean={:.4f}",
                                     scores.size(), minScore, maxScore,
                                     sumScore / static_cast<float>(scores.size()));
                    }

                    // Apply reranker scores to eligible results.
                    // Compute effective blend weight (may be adaptive).
                    double effectiveWeight = workingConfig.rerankWeight;
                    if (!workingConfig.rerankReplaceScores && workingConfig.rerankAdaptiveBlend &&
                        !scores.empty()) {
                        float maxRerankScore = *std::max_element(scores.begin(), scores.end());
                        // Scale weight by reranker confidence, floor prevents near-zero
                        effectiveWeight = std::clamp(
                            static_cast<double>(workingConfig.rerankWeight) * maxRerankScore,
                            static_cast<double>(workingConfig.rerankAdaptiveFloor),
                            static_cast<double>(workingConfig.rerankWeight));
                        spdlog::debug("[reranker] Adaptive blend: maxScore={:.4f} "
                                      "effectiveWeight={:.4f} (base={:.3f})",
                                      maxRerankScore, effectiveWeight, workingConfig.rerankWeight);
                    }

                    std::vector<double> bestRerankScoreByDoc(rerankWindow, 0.0);
                    std::vector<bool> rerankScorePresentByDoc(rerankWindow, false);
                    for (size_t i = 0; i < scores.size() && i < rerankPassageDocIndices.size();
                         ++i) {
                        const size_t idx = rerankPassageDocIndices[i];
                        if (idx >= rerankWindow) {
                            continue;
                        }
                        const double rerankScore = static_cast<double>(scores[i]);
                        if (!rerankScorePresentByDoc[idx] ||
                            rerankScore > bestRerankScoreByDoc[idx]) {
                            bestRerankScoreByDoc[idx] = rerankScore;
                            rerankScorePresentByDoc[idx] = true;
                        }
                    }

                    for (size_t idx = 0; idx < rerankWindow; ++idx) {
                        if (!rerankScorePresentByDoc[idx]) {
                            continue;
                        }
                        const double originalScore = response.results[idx].score;
                        const double rerankScore = bestRerankScoreByDoc[idx];

                        if (workingConfig.rerankReplaceScores) {
                            response.results[idx].score = rerankScore;
                        } else {
                            response.results[idx].score = rerankScore * effectiveWeight +
                                                          originalScore * (1.0 - effectiveWeight);
                        }
                        response.results[idx].rerankerScore = rerankScore;
                    }

                    // Re-sort by new scores (only the top window needs sorting)
                    std::sort(response.results.begin(),
                              response.results.begin() + static_cast<ptrdiff_t>(rerankWindow),
                              [](const SearchResult& a, const SearchResult& b) {
                                  return a.score > b.score;
                              });

                    if (stageTraceEnabled) {
                        rerankWindowTrace = json::array();
                        std::unordered_map<std::string, size_t> finalRanks;
                        finalRanks.reserve(rerankWindow);
                        for (size_t i = 0; i < rerankWindow; ++i) {
                            finalRanks[documentIdForTrace(
                                response.results[i].document.filePath,
                                response.results[i].document.sha256Hash)] = i + 1;
                        }

                        for (size_t i = 0; i < preRerankSnapshot.size(); ++i) {
                            const auto& before = preRerankSnapshot[i];
                            const std::string docId = documentIdForTrace(
                                before.document.filePath, before.document.sha256Hash);
                            json entry = {
                                {"doc_id", docId},
                                {"pre_rank", i + 1},
                                {"original_score", before.score},
                                {"keyword_score", before.keywordScore.value_or(0.0)},
                                {"vector_score", before.vectorScore.value_or(0.0)},
                                {"kg_score", before.kgScore.value_or(0.0)},
                                {"path_score", before.pathScore.value_or(0.0)},
                                {"tag_score", before.tagScore.value_or(0.0)},
                                {"symbol_score", before.symbolScore.value_or(0.0)},
                            };
                            bool found = false;
                            for (size_t j = 0; j < rerankWindow; ++j) {
                                const auto& after = response.results[j];
                                if (documentIdForTrace(after.document.filePath,
                                                       after.document.sha256Hash) == docId) {
                                    entry["final_rank"] = j + 1;
                                    entry["final_score"] = after.score;
                                    entry["reranker_score"] = after.rerankerScore.value_or(0.0);
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                entry["final_rank"] = nullptr;
                                entry["final_score"] = nullptr;
                                entry["reranker_score"] = nullptr;
                            }
                            rerankWindowTrace.push_back(std::move(entry));
                        }
                    }

                    crossRerankApplied = true;
                    contributing.push_back("reranker");
                    auto rerankEnd = std::chrono::steady_clock::now();
                    const auto rerankDuration =
                        std::chrono::duration_cast<std::chrono::microseconds>(rerankEnd -
                                                                              rerankStart)
                            .count();
                    componentTiming["reranker"] = rerankDuration;
                    traceCollector.markStageResult("reranker", {}, rerankDuration, true);
                } else {
                    if (isRerankerCooldownError(rerankResult.error().code)) {
                        const int64_t nextCooldown =
                            std::chrono::duration_cast<std::chrono::microseconds>(
                                (std::chrono::steady_clock::now() + kRerankerErrorCooldown)
                                    .time_since_epoch())
                                .count();
                        rerankerCooldownUntilMicros_.store(nextCooldown, std::memory_order_relaxed);
                        spdlog::warn(
                            "[reranker] Reranking unavailable (code={}): {}. Cooling down "
                            "for {}s",
                            static_cast<int>(rerankResult.error().code),
                            rerankResult.error().message,
                            std::chrono::duration_cast<std::chrono::seconds>(kRerankerErrorCooldown)
                                .count());
                    } else {
                        spdlog::warn("[reranker] Reranking failed: {}",
                                     rerankResult.error().message);
                    }
                    auto rerankEnd = std::chrono::steady_clock::now();
                    traceCollector.markStageFailure(
                        "reranker", std::chrono::duration_cast<std::chrono::microseconds>(
                                        rerankEnd - rerankStart)
                                        .count());
                }
            } else {
                spdlog::debug("[reranker] Skipping rerank: no snippets available");
                auto rerankEnd = std::chrono::steady_clock::now();
                traceCollector.markStageSkipped("reranker", "no_snippets_available");
                componentTiming["reranker"] =
                    std::chrono::duration_cast<std::chrono::microseconds>(rerankEnd - rerankStart)
                        .count();
            }
        }
    } else if (workingConfig.enableReranking && !rerankAvailable) {
        spdlog::debug("[reranker] Unavailable; falling back to fused scores");
        traceCollector.markStageSkipped("reranker", "unavailable");
    }

    const size_t compactRerankWindow = std::min(
        response.results.size(), (workingConfig.enableReranking && workingConfig.rerankTopK > 0)
                                     ? workingConfig.rerankTopK
                                     : size_t(0));
    if (effectiveVectorMaxResults == 0) {
        effectiveVectorMaxResults = workingConfig.vectorMaxResults;
    }
    if (effectiveEntityVectorMaxResults == 0) {
        effectiveEntityVectorMaxResults = workingConfig.entityVectorMaxResults;
    }
    response.debugStats["compact_pre_fusion_count"] = std::to_string(preFusionDocIds.size());
    response.debugStats["compact_post_fusion_count"] = std::to_string(
        postFusionSnapshot.empty() ? response.results.size() : postFusionSnapshot.size());
    response.debugStats["compact_rerank_window_count"] = std::to_string(compactRerankWindow);
    response.debugStats["compact_final_count"] = std::to_string(response.results.size());
    response.debugStats["compact_effective_vector_max_results"] =
        std::to_string(effectiveVectorMaxResults);
    response.debugStats["compact_effective_entity_vector_max_results"] =
        std::to_string(effectiveEntityVectorMaxResults);
    response.debugStats["compact_weak_query_fanout_boost_applied"] =
        weakQueryFanoutBoostApplied ? "1" : "0";
    response.debugStats["compressed_ann_enabled"] = compressedAnnEnabled ? "1" : "0";
    response.debugStats["compressed_ann_attempted"] = compressedAnnAttempted ? "1" : "0";
    response.debugStats["compressed_ann_applied"] = compressedAnnApplied ? "1" : "0";
    response.debugStats["compressed_ann_result_count"] = std::to_string(compressedAnnResultCount);
    response.debugStats["compressed_ann_skip_reason"] = compressedAnnSkipReason;
    response.debugStats["turboquant_enabled"] = turboQuantRerankEnabled ? "1" : "0";
    response.debugStats["turboquant_rerank_applied"] = turboQuantRerankApplied ? "1" : "0";
    response.debugStats["turboquant_rerank_window"] = std::to_string(turboQuantRequestedWindow);
    response.debugStats["turboquant_candidate_count"] = std::to_string(turboQuantCandidateCount);
    response.debugStats["turboquant_packed_candidates_scored"] =
        std::to_string(turboQuantPackedCandidatesScored);
    response.debugStats["turboquant_skip_reason"] = turboQuantSkipReason;
    response.debugStats["embedding_status"] = embeddingStatus;
    response.debugStats["vector_db_dim"] = std::to_string(vectorDbEmbeddingDim);
    response.debugStats["query_embedding_dim"] = std::to_string(queryEmbeddingDim);
    response.debugStats["vector_tier_skip_reason"] = vectorTierSkipReason;

    if (!concepts.empty() && workingConfig.conceptBoostWeight > 0.0f &&
        workingConfig.conceptMaxBoost > 0.0f && !response.results.empty()) {
        YAMS_ZONE_SCOPED_N("concepts::boost");
        std::vector<std::string> conceptTerms;
        conceptTerms.reserve(concepts.size());
        for (const auto& conceptItem : concepts) {
            if (conceptItem.text.empty()) {
                continue;
            }
            std::string lowered = conceptItem.text;
            std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            conceptTerms.push_back(std::move(lowered));
        }
        if (!conceptTerms.empty()) {
            std::sort(conceptTerms.begin(), conceptTerms.end());
            conceptTerms.erase(std::unique(conceptTerms.begin(), conceptTerms.end()),
                               conceptTerms.end());
        }

        if (!conceptTerms.empty()) {
            const size_t totalResults = response.results.size();
            const size_t scanLimit = std::min(workingConfig.conceptMaxScanResults, totalResults);
            if (scanLimit > 0) {
                std::vector<uint32_t> matchCounts(scanLimit, 0);
                const size_t minChunkSize =
                    std::max<size_t>(1, workingConfig.minChunkSizeForParallel);
                const size_t maxThreads = std::max<size_t>(1, std::thread::hardware_concurrency());
                const size_t chunkTarget = (scanLimit + minChunkSize - 1) / minChunkSize;
                const size_t numThreads = std::min(maxThreads, std::max<size_t>(1, chunkTarget));
                const bool useParallelBoost = numThreads > 1;

                auto computeMatches = [&](size_t start, size_t end) {
                    std::vector<uint32_t> matches;
                    matches.reserve(end - start);
                    for (size_t idx = start; idx < end; ++idx) {
                        const auto& result = response.results[idx];
                        uint32_t count = 0;
                        for (const auto& term : conceptTerms) {
                            if (!term.empty() && (containsFast(result.snippet, term) ||
                                                  containsFast(result.document.fileName, term))) {
                                count++;
                            }
                        }
                        matches.push_back(count);
                    }
                    return matches;
                };

                if (useParallelBoost) {
                    const size_t chunkSize = (scanLimit + numThreads - 1) / numThreads;
                    std::vector<std::future<std::vector<uint32_t>>> futures;
                    futures.reserve(numThreads);
                    for (size_t i = 0; i < numThreads; ++i) {
                        const size_t start = i * chunkSize;
                        const size_t end = std::min(start + chunkSize, scanLimit);
                        if (start >= end) {
                            break;
                        }
                        futures.push_back(
                            std::async(std::launch::async, [start, end, &computeMatches]() {
                                return computeMatches(start, end);
                            }));
                    }

                    size_t offset = 0;
                    for (auto& future : futures) {
                        auto matches = future.get();
                        for (size_t i = 0; i < matches.size(); ++i) {
                            matchCounts[offset + i] = matches[i];
                        }
                        offset += matches.size();
                    }
                } else {
                    auto matches = computeMatches(0, scanLimit);
                    for (size_t i = 0; i < matches.size(); ++i) {
                        matchCounts[i] = matches[i];
                    }
                }

                bool boosted = false;
                float boostBudget = workingConfig.conceptMaxBoost;
                for (size_t i = 0; i < scanLimit; ++i) {
                    if (boostBudget <= 0.0f) {
                        break;
                    }
                    const uint32_t matchCount = matchCounts[i];
                    if (matchCount == 0) {
                        continue;
                    }
                    const float desiredBoost =
                        workingConfig.conceptBoostWeight * static_cast<float>(matchCount);
                    const float appliedBoost = std::min(desiredBoost, boostBudget);
                    response.results[i].score *= (1.0f + appliedBoost);
                    boostBudget -= appliedBoost;
                    boosted = true;
                }

                if (boosted) {
                    std::sort(response.results.begin(), response.results.end(),
                              [](const SearchResult& a, const SearchResult& b) {
                                  return a.score > b.score;
                              });
                }
            }
        }
    }

    const auto lexicalAnchorScore = [](const SearchResult& r) {
        return r.keywordScore.value_or(0.0) + r.pathScore.value_or(0.0) + r.tagScore.value_or(0.0) +
               r.symbolScore.value_or(0.0);
    };

    const auto docIdForResult = [](const SearchResult& result) {
        return documentIdForTrace(result.document.filePath, result.document.sha256Hash);
    };

    const double semanticRescueMinVectorScore =
        std::max(0.0, static_cast<double>(workingConfig.semanticRescueMinVectorScore));

    std::unordered_map<std::string, size_t> currentScoreRankByDoc;
    if (workingConfig.semanticRescueSlots > 0 && !response.results.empty()) {
        std::vector<size_t> rankOrder(response.results.size());
        std::iota(rankOrder.begin(), rankOrder.end(), 0);
        std::sort(rankOrder.begin(), rankOrder.end(), [&](size_t lhs, size_t rhs) {
            const auto& a = response.results[lhs];
            const auto& b = response.results[rhs];
            if (a.score != b.score) {
                return a.score > b.score;
            }
            return documentIdForTrace(a.document.filePath, a.document.sha256Hash) <
                   documentIdForTrace(b.document.filePath, b.document.sha256Hash);
        });

        for (size_t i = 0; i < rankOrder.size(); ++i) {
            const auto& result = response.results[rankOrder[i]];
            const std::string docId =
                documentIdForTrace(result.document.filePath, result.document.sha256Hash);
            if (!docId.empty()) {
                currentScoreRankByDoc.emplace(docId, i + 1);
            }
        }
    }

    const auto semanticRescueSignalForResult =
        [&preFusionSignals,
         &docIdForResult](const SearchResult& result) -> const PreFusionDocSignal* {
        const std::string docId = docIdForResult(result);
        if (docId.empty()) {
            return nullptr;
        }
        auto it = preFusionSignals.find(docId);
        return it != preFusionSignals.end() ? &it->second : nullptr;
    };

    const auto isFinalSemanticRescueCandidate =
        [&lexicalAnchorScore, &semanticRescueSignalForResult,
         semanticRescueMinVectorScore](const SearchResult& result) {
            const auto* signal = semanticRescueSignalForResult(result);
            return signal != nullptr && !signal->hasAnchoring && signal->hasVector &&
                   signal->maxVectorRaw >= semanticRescueMinVectorScore &&
                   lexicalAnchorScore(result) <= 0.0;
        };

    const size_t buriedVectorRankThreshold =
        (intent == QueryIntent::Prose && workingConfig.enableReranking &&
         workingConfig.rerankTopK > 0)
            ? std::max<size_t>(150, workingConfig.rerankTopK * 3)
            : std::numeric_limits<size_t>::max();
    const size_t buriedScoreRankThreshold =
        buriedVectorRankThreshold != std::numeric_limits<size_t>::max() &&
                buriedVectorRankThreshold > 10
            ? buriedVectorRankThreshold - 10
            : std::numeric_limits<size_t>::max();

    const auto isBuriedFinalSemanticRescueCandidate =
        [&isFinalSemanticRescueCandidate, &semanticRescueSignalForResult, &docIdForResult,
         &currentScoreRankByDoc, buriedVectorRankThreshold,
         buriedScoreRankThreshold](const SearchResult& result) {
            if (!isFinalSemanticRescueCandidate(result)) {
                return false;
            }
            const auto* signal = semanticRescueSignalForResult(result);
            if (signal == nullptr || result.rerankerScore.value_or(0.0) < 9e-4 ||
                signal->maxVectorRaw < 0.79) {
                return false;
            }

            const std::string docId = docIdForResult(result);
            const auto rankIt = currentScoreRankByDoc.find(docId);
            const size_t currentScoreRank =
                rankIt != currentScoreRankByDoc.end() ? rankIt->second : 0;

            return currentScoreRank >= buriedScoreRankThreshold &&
                   signal->bestVectorRank != std::numeric_limits<size_t>::max() &&
                   signal->bestVectorRank >= buriedVectorRankThreshold;
        };

    const auto finalSemanticRescueBetter = [&semanticRescueSignalForResult, &docIdForResult](
                                               const SearchResult& a, const SearchResult& b) {
        const bool rerankCoveredA = a.rerankerScore.has_value();
        const bool rerankCoveredB = b.rerankerScore.has_value();
        if (rerankCoveredA != rerankCoveredB) {
            return rerankCoveredA;
        }

        const double rerankA = a.rerankerScore.value_or(-1.0);
        const double rerankB = b.rerankerScore.value_or(-1.0);
        if (rerankA != rerankB) {
            return rerankA > rerankB;
        }

        const auto* signalA = semanticRescueSignalForResult(a);
        const auto* signalB = semanticRescueSignalForResult(b);
        const double rawVectorA = signalA != nullptr ? signalA->maxVectorRaw : 0.0;
        const double rawVectorB = signalB != nullptr ? signalB->maxVectorRaw : 0.0;
        if (rawVectorA != rawVectorB) {
            return rawVectorA > rawVectorB;
        }

        if (a.score != b.score) {
            return a.score > b.score;
        }

        return docIdForResult(a) < docIdForResult(b);
    };

    const auto buriedSemanticRescueBetter = [&semanticRescueSignalForResult, &docIdForResult](
                                                const SearchResult& a, const SearchResult& b) {
        const bool rerankCoveredA = a.rerankerScore.has_value();
        const bool rerankCoveredB = b.rerankerScore.has_value();
        if (rerankCoveredA != rerankCoveredB) {
            return rerankCoveredA;
        }

        constexpr double kBuriedRerankCap = 1e-3;
        constexpr double kBuriedRerankTieEpsilon = 2e-4;
        const double rerankA = std::min(a.rerankerScore.value_or(-1.0), kBuriedRerankCap);
        const double rerankB = std::min(b.rerankerScore.value_or(-1.0), kBuriedRerankCap);
        if (std::abs(rerankA - rerankB) > kBuriedRerankTieEpsilon) {
            return rerankA > rerankB;
        }

        const auto* signalA = semanticRescueSignalForResult(a);
        const auto* signalB = semanticRescueSignalForResult(b);
        const double rawVectorA = signalA != nullptr ? signalA->maxVectorRaw : 0.0;
        const double rawVectorB = signalB != nullptr ? signalB->maxVectorRaw : 0.0;
        if (rawVectorA != rawVectorB) {
            return rawVectorA > rawVectorB;
        }

        const double fullRerankA = a.rerankerScore.value_or(-1.0);
        const double fullRerankB = b.rerankerScore.value_or(-1.0);
        if (fullRerankA != fullRerankB) {
            return fullRerankA > fullRerankB;
        }

        if (a.score != b.score) {
            return a.score > b.score;
        }

        return docIdForResult(a) < docIdForResult(b);
    };

    const auto evidenceRescueScore = [&lexicalAnchorScore](const SearchResult& result) {
        const double lexical = lexicalAnchorScore(result);
        const double vector = result.vectorScore.value_or(0.0);
        const double kg = result.kgScore.value_or(0.0);
        const double bestSignal =
            std::max({lexical, vector, kg, result.pathScore.value_or(0.0),
                      result.symbolScore.value_or(0.0), result.tagScore.value_or(0.0)});
        const double componentBonus = (lexical > 0.0 && vector > 0.0) ? 0.01 : 0.0;
        const double rerankSignal = result.rerankerScore.value_or(0.0);
        return std::max(bestSignal + componentBonus, rerankSignal);
    };

    const double finalEvidenceRescueMinScore =
        std::max(0.0, static_cast<double>(workingConfig.fusionEvidenceRescueMinScore));
    const auto isFinalEvidenceRescueCandidate = [finalEvidenceRescueMinScore,
                                                 &evidenceRescueScore](const SearchResult& result) {
        return evidenceRescueScore(result) >= finalEvidenceRescueMinScore;
    };

    const auto finalEvidenceRescueBetter =
        [&evidenceRescueScore, &docIdForResult](const SearchResult& a, const SearchResult& b) {
            const double evidenceA = evidenceRescueScore(a);
            const double evidenceB = evidenceRescueScore(b);
            if (evidenceA != evidenceB) {
                return evidenceA > evidenceB;
            }
            if (a.score != b.score) {
                return a.score > b.score;
            }
            return docIdForResult(a) < docIdForResult(b);
        };

    const auto finalLexicalAwareLess = [&lexicalAnchorScore, &docIdForResult, &workingConfig](
                                           const SearchResult& a, const SearchResult& b) {
        if (workingConfig.rerankReplaceScores) {
            const bool rerankCoveredA = a.rerankerScore.has_value();
            const bool rerankCoveredB = b.rerankerScore.has_value();
            if (rerankCoveredA != rerankCoveredB) {
                return rerankCoveredA;
            }
            if (rerankCoveredA && rerankCoveredB) {
                const double rerankA = a.rerankerScore.value_or(0.0);
                const double rerankB = b.rerankerScore.value_or(0.0);
                if (rerankA != rerankB) {
                    return rerankA > rerankB;
                }
            }
        }

        const double scoreDiff = static_cast<double>(a.score) - static_cast<double>(b.score);
        const double tieEpsilon =
            std::max(0.0, static_cast<double>(workingConfig.lexicalTieBreakEpsilon));

        if (!workingConfig.enableLexicalTieBreak || std::abs(scoreDiff) > tieEpsilon) {
            if (a.score != b.score) {
                return a.score > b.score;
            }
        } else {
            const double lexicalA = lexicalAnchorScore(a);
            const double lexicalB = lexicalAnchorScore(b);
            if (lexicalA != lexicalB) {
                return lexicalA > lexicalB;
            }

            const double keywordA = a.keywordScore.value_or(0.0);
            const double keywordB = b.keywordScore.value_or(0.0);
            if (keywordA != keywordB) {
                return keywordA > keywordB;
            }

            const double vectorA = a.vectorScore.value_or(0.0);
            const double vectorB = b.vectorScore.value_or(0.0);
            if (vectorA != vectorB) {
                return vectorA < vectorB;
            }
        }

        return docIdForResult(a) < docIdForResult(b);
    };

    std::vector<std::string> semanticRescuePromotedDocIds;
    std::vector<std::string> semanticRescueDisplacedDocIds;
    std::vector<std::string> buriedSemanticRescuePromotedDocIds;
    std::vector<std::string> buriedSemanticRescueDisplacedDocIds;
    std::vector<std::string> evidenceRescuePromotedDocIds;
    std::vector<std::string> evidenceRescueDisplacedDocIds;

    // Enforce user-visible limit after all post-fusion stages have had a chance to reorder/boost.
    if (response.results.size() > userLimit) {
        std::partial_sort(response.results.begin(),
                          response.results.begin() + static_cast<ptrdiff_t>(userLimit),
                          response.results.end(), finalLexicalAwareLess);

        if (workingConfig.semanticRescueSlots > 0 && userLimit > 0) {
            const size_t finalWindow = std::min(userLimit, response.results.size());
            const size_t rescueTarget = std::min(workingConfig.semanticRescueSlots, finalWindow);
            size_t rescuePresent = 0;
            for (size_t i = 0; i < finalWindow; ++i) {
                if (isFinalSemanticRescueCandidate(response.results[i])) {
                    rescuePresent++;
                }
            }

            while (rescuePresent < rescueTarget) {
                size_t bestTailIndex = response.results.size();
                for (size_t i = finalWindow; i < response.results.size(); ++i) {
                    if (!isFinalSemanticRescueCandidate(response.results[i])) {
                        continue;
                    }
                    if (bestTailIndex >= response.results.size() ||
                        finalSemanticRescueBetter(response.results[i],
                                                  response.results[bestTailIndex])) {
                        bestTailIndex = i;
                    }
                }
                if (bestTailIndex >= response.results.size()) {
                    break;
                }

                size_t victimIndex = finalWindow;
                for (size_t i = finalWindow; i > 0; --i) {
                    const size_t idx = i - 1;
                    if (!isFinalSemanticRescueCandidate(response.results[idx])) {
                        victimIndex = idx;
                        break;
                    }
                }
                if (victimIndex >= finalWindow) {
                    break;
                }

                const std::string promotedId = docIdForResult(response.results[bestTailIndex]);
                const std::string displacedId = docIdForResult(response.results[victimIndex]);
                if (!promotedId.empty()) {
                    semanticRescuePromotedDocIds.push_back(promotedId);
                }
                if (!displacedId.empty()) {
                    semanticRescueDisplacedDocIds.push_back(displacedId);
                }

                std::swap(response.results[victimIndex], response.results[bestTailIndex]);
                rescuePresent++;
            }

            std::sort(response.results.begin(),
                      response.results.begin() + static_cast<ptrdiff_t>(finalWindow),
                      finalLexicalAwareLess);

            const size_t buriedRescueTarget =
                buriedVectorRankThreshold != std::numeric_limits<size_t>::max() ? size_t(1)
                                                                                : size_t(0);
            size_t buriedRescuePresent = 0;
            for (size_t i = 0; i < finalWindow; ++i) {
                if (isBuriedFinalSemanticRescueCandidate(response.results[i])) {
                    buriedRescuePresent++;
                }
            }

            while (buriedRescuePresent < buriedRescueTarget) {
                size_t bestTailIndex = response.results.size();
                for (size_t i = finalWindow; i < response.results.size(); ++i) {
                    if (!isBuriedFinalSemanticRescueCandidate(response.results[i])) {
                        continue;
                    }
                    if (bestTailIndex >= response.results.size() ||
                        buriedSemanticRescueBetter(response.results[i],
                                                   response.results[bestTailIndex])) {
                        bestTailIndex = i;
                    }
                }
                if (bestTailIndex >= response.results.size()) {
                    break;
                }

                size_t victimIndex = finalWindow;
                for (size_t i = finalWindow; i > 0; --i) {
                    const size_t idx = i - 1;
                    if (!isBuriedFinalSemanticRescueCandidate(response.results[idx])) {
                        victimIndex = idx;
                        break;
                    }
                }
                if (victimIndex >= finalWindow) {
                    break;
                }

                const std::string promotedId = docIdForResult(response.results[bestTailIndex]);
                const std::string displacedId = docIdForResult(response.results[victimIndex]);
                if (!promotedId.empty()) {
                    buriedSemanticRescuePromotedDocIds.push_back(promotedId);
                }
                if (!displacedId.empty()) {
                    buriedSemanticRescueDisplacedDocIds.push_back(displacedId);
                }

                std::swap(response.results[victimIndex], response.results[bestTailIndex]);
                buriedRescuePresent++;
            }

            if (buriedRescueTarget > 0) {
                std::sort(response.results.begin(),
                          response.results.begin() + static_cast<ptrdiff_t>(finalWindow),
                          finalLexicalAwareLess);
            }
        }

        if (workingConfig.fusionEvidenceRescueSlots > 0 && userLimit > 0) {
            const size_t finalWindow = std::min(userLimit, response.results.size());
            const size_t rescueTarget =
                std::min(workingConfig.fusionEvidenceRescueSlots, finalWindow);
            size_t rescuePresent = 0;
            for (size_t i = 0; i < finalWindow; ++i) {
                if (isFinalEvidenceRescueCandidate(response.results[i])) {
                    rescuePresent++;
                }
            }

            while (rescuePresent < rescueTarget) {
                size_t bestTailIndex = response.results.size();
                for (size_t i = finalWindow; i < response.results.size(); ++i) {
                    if (!isFinalEvidenceRescueCandidate(response.results[i])) {
                        continue;
                    }
                    if (bestTailIndex >= response.results.size() ||
                        finalEvidenceRescueBetter(response.results[i],
                                                  response.results[bestTailIndex])) {
                        bestTailIndex = i;
                    }
                }
                if (bestTailIndex >= response.results.size()) {
                    break;
                }

                size_t victimIndex = finalWindow;
                double victimEvidence = std::numeric_limits<double>::max();
                for (size_t i = 0; i < finalWindow; ++i) {
                    if (response.results[i].rerankerScore.has_value()) {
                        continue;
                    }
                    const double evidence = evidenceRescueScore(response.results[i]);
                    if (evidence < victimEvidence) {
                        victimEvidence = evidence;
                        victimIndex = i;
                    }
                }
                if (victimIndex >= finalWindow) {
                    break;
                }

                const double bestTailEvidence =
                    evidenceRescueScore(response.results[bestTailIndex]);
                if (bestTailEvidence <= victimEvidence) {
                    break;
                }

                const std::string promotedId = docIdForResult(response.results[bestTailIndex]);
                const std::string displacedId = docIdForResult(response.results[victimIndex]);
                if (!promotedId.empty()) {
                    evidenceRescuePromotedDocIds.push_back(promotedId);
                }
                if (!displacedId.empty()) {
                    evidenceRescueDisplacedDocIds.push_back(displacedId);
                }

                std::swap(response.results[victimIndex], response.results[bestTailIndex]);
                rescuePresent++;
            }

            std::sort(response.results.begin(),
                      response.results.begin() + static_cast<ptrdiff_t>(finalWindow),
                      finalLexicalAwareLess);
        }

        response.results.resize(userLimit);
    }

    size_t semanticRescueFinalCount = 0;
    std::vector<std::string> semanticRescueFinalDocIds;
    if (workingConfig.semanticRescueSlots > 0 && !response.results.empty()) {
        for (const auto& result : response.results) {
            if (isFinalSemanticRescueCandidate(result)) {
                semanticRescueFinalCount++;
                const std::string docId = docIdForResult(result);
                if (!docId.empty()) {
                    semanticRescueFinalDocIds.push_back(docId);
                }
            }
        }
    }
    const size_t semanticRescueTarget =
        std::min(workingConfig.semanticRescueSlots, response.results.size());
    const double semanticRescueRate = semanticRescueTarget > 0
                                          ? static_cast<double>(semanticRescueFinalCount) /
                                                static_cast<double>(semanticRescueTarget)
                                          : 0.0;

    response.debugStats["semantic_rescue_enabled"] =
        workingConfig.semanticRescueSlots > 0 ? "1" : "0";
    response.debugStats["semantic_rescue_slots"] =
        std::to_string(workingConfig.semanticRescueSlots);
    response.debugStats["semantic_rescue_target"] = std::to_string(semanticRescueTarget);
    response.debugStats["semantic_rescue_final_count"] = std::to_string(semanticRescueFinalCount);
    response.debugStats["semantic_rescue_final_doc_ids"] = joinWithTab(semanticRescueFinalDocIds);
    response.debugStats["semantic_rescue_promoted_doc_ids"] =
        joinWithTab(semanticRescuePromotedDocIds);
    response.debugStats["semantic_rescue_displaced_doc_ids"] =
        joinWithTab(semanticRescueDisplacedDocIds);
    response.debugStats["semantic_rescue_buried_promoted_doc_ids"] =
        joinWithTab(buriedSemanticRescuePromotedDocIds);
    response.debugStats["semantic_rescue_buried_displaced_doc_ids"] =
        joinWithTab(buriedSemanticRescueDisplacedDocIds);
    response.debugStats["evidence_rescue_enabled"] =
        workingConfig.fusionEvidenceRescueSlots > 0 ? "1" : "0";
    response.debugStats["evidence_rescue_slots"] =
        std::to_string(workingConfig.fusionEvidenceRescueSlots);
    response.debugStats["evidence_rescue_min_score"] =
        fmt::format("{:.4f}", workingConfig.fusionEvidenceRescueMinScore);
    response.debugStats["evidence_rescue_promoted_doc_ids"] =
        joinWithTab(evidenceRescuePromotedDocIds);
    response.debugStats["evidence_rescue_displaced_doc_ids"] =
        joinWithTab(evidenceRescueDisplacedDocIds);
    response.debugStats["semantic_rescue_rate"] = fmt::format("{:.3f}", semanticRescueRate);
    response.debugStats["multi_vector_generated_phrases"] =
        std::to_string(multiVectorGeneratedPhrases);
    response.debugStats["multi_vector_raw_hit_count"] = std::to_string(multiVectorPhraseHits);
    response.debugStats["multi_vector_added_new_count"] = std::to_string(multiVectorAddedNewCount);
    response.debugStats["multi_vector_replaced_base_count"] =
        std::to_string(multiVectorReplacedBaseCount);
    response.debugStats["subphrase_generated_count"] =
        std::to_string(textExpansionStats.generatedSubPhrases);
    response.debugStats["subphrase_clause_count"] =
        std::to_string(textExpansionStats.subPhraseClauseCount);
    response.debugStats["subphrase_fts_hit_count"] =
        std::to_string(textExpansionStats.subPhraseFtsHitCount);
    response.debugStats["subphrase_fts_added_count"] =
        std::to_string(textExpansionStats.subPhraseFtsAddedCount);
    response.debugStats["aggressive_fts_clause_count"] =
        std::to_string(textExpansionStats.aggressiveClauseCount);
    response.debugStats["aggressive_fts_hit_count"] =
        std::to_string(textExpansionStats.aggressiveFtsHitCount);
    response.debugStats["aggressive_fts_added_count"] =
        std::to_string(textExpansionStats.aggressiveFtsAddedCount);
    response.debugStats["graph_expansion_term_count"] =
        std::to_string(textExpansionStats.graphExpansionTermCount);
    response.debugStats["graph_expansion_fts_hit_count"] =
        std::to_string(textExpansionStats.graphExpansionFtsHitCount);
    response.debugStats["graph_expansion_fts_added_count"] =
        std::to_string(textExpansionStats.graphExpansionFtsAddedCount);
    response.debugStats["graph_doc_expansion_term_count"] =
        std::to_string(graphDocExpansionTermCount);
    response.debugStats["graph_doc_expansion_fts_hit_count"] =
        std::to_string(graphDocExpansionFtsHitCount);
    response.debugStats["graph_doc_expansion_fts_added_count"] =
        std::to_string(graphDocExpansionFtsAddedCount);
    response.debugStats["graph_text_blocked_low_score_count"] =
        std::to_string(textExpansionStats.graphTextBlockedLowScoreCount);
    if (!docSeedGraphTerms.empty()) {
        json graphDocTerms = json::array();
        for (const auto& term : docSeedGraphTerms) {
            graphDocTerms.push_back({{"text", term.text}, {"score", term.score}});
        }
        response.debugStats["graph_doc_expansion_terms_json"] = graphDocTerms.dump();
    }
    response.debugStats["graph_vector_generated_terms"] = std::to_string(graphVectorGeneratedTerms);
    response.debugStats["graph_vector_raw_hit_count"] = std::to_string(graphVectorRawHitCount);
    response.debugStats["graph_vector_added_new_count"] = std::to_string(graphVectorAddedNewCount);
    response.debugStats["graph_vector_replaced_base_count"] =
        std::to_string(graphVectorReplacedBaseCount);
    response.debugStats["graph_vector_blocked_uncorroborated_count"] =
        std::to_string(graphVectorBlockedUncorroboratedCount);
    response.debugStats["graph_vector_blocked_missing_text_anchor_count"] =
        std::to_string(graphVectorBlockedMissingTextAnchorCount);
    response.debugStats["graph_vector_blocked_missing_baseline_text_anchor_count"] =
        std::to_string(graphVectorBlockedMissingBaselineTextAnchorCount);
    response.debugStats["strong_vector_only_relief_enabled"] =
        workingConfig.enableStrongVectorOnlyRelief ? "1" : "0";
    response.debugStats["strong_vector_only_min_score"] =
        fmt::format("{:.3f}", workingConfig.strongVectorOnlyMinScore);
    response.debugStats["strong_vector_only_top_rank"] =
        std::to_string(workingConfig.strongVectorOnlyTopRank);
    response.debugStats["strong_vector_only_penalty"] =
        fmt::format("{:.3f}", workingConfig.strongVectorOnlyPenalty);
    response.debugStats["relaxed_vector_retry_enabled"] = relaxedVectorRetryEnabled ? "1" : "0";
    response.debugStats["relaxed_vector_retry_attempted"] =
        relaxedVectorRetryAttempted.load(std::memory_order_relaxed) ? "1" : "0";
    response.debugStats["relaxed_vector_retry_applied"] =
        relaxedVectorRetryApplied.load(std::memory_order_relaxed) ? "1" : "0";
    response.debugStats["relaxed_vector_primary_hit_count"] =
        std::to_string(relaxedVectorPrimaryHitCount.load(std::memory_order_relaxed));
    response.debugStats["relaxed_vector_retry_threshold"] = fmt::format(
        "{:.3f}",
        static_cast<double>(relaxedVectorRetryThresholdMilli.load(std::memory_order_relaxed)) /
            1000.0);
    response.debugStats["graph_query_concept_count"] = std::to_string(graphQueryConceptCount);
    response.debugStats["graph_query_neighbor_seed_docs"] =
        std::to_string(graphQueryNeighborSeedDocCount);
    response.debugStats["graph_window_guard_replacement_count"] =
        std::to_string(graphWindowGuardReplacementCount);
    response.debugStats["graph_window_cap_replacement_count"] =
        std::to_string(graphWindowCapReplacementCount);
    response.debugStats["graph_rerank_guard_replacement_count"] =
        std::to_string(graphRerankGuardReplacementCount);
    response.debugStats["graph_matched_candidates"] = std::to_string(graphMatchedCandidates);
    response.debugStats["graph_positive_signal_candidates"] =
        std::to_string(graphPositiveSignalCandidates);
    response.debugStats["graph_boosted_docs"] = std::to_string(graphBoostedDocs);
    response.debugStats["graph_max_signal"] = fmt::format("{:.4f}", graphMaxSignal);
    response.debugStats.try_emplace("graph_community_supported_docs", "0");
    response.debugStats.try_emplace("graph_community_edge_count", "0");
    response.debugStats.try_emplace("graph_community_largest_size", "0");
    response.debugStats.try_emplace("graph_community_signal_mass", "0.0000");
    response.debugStats.try_emplace("graph_community_boosted_docs", "0");
    response.debugStats.try_emplace(
        "graph_community_weight",
        fmt::format("{:.4f}", std::clamp(workingConfig.graphCommunityWeight, 0.0f, 1.0f)));
    response.debugStats["rerank_window_trace_json"] = rerankWindowTrace.dump();
    if (workingConfig.semanticRescueSlots > 0 && !response.results.empty()) {
        spdlog::debug(
            "[semantic_rescue] final_count={} target={} rate={:.3f} min_vector_score={:.4f}",
            semanticRescueFinalCount, semanticRescueTarget, semanticRescueRate,
            workingConfig.semanticRescueMinVectorScore);
    }

    if (!response.results.empty()) {
        std::unordered_map<std::string_view, size_t> extCounts;
        for (const auto& r : response.results) {
            std::string_view path = r.document.filePath;
            auto pos = path.rfind('.');
            std::string_view ext = (pos != std::string_view::npos) ? path.substr(pos) : "no ext";
            extCounts[ext]++;
        }

        std::vector<std::pair<std::string, size_t>> sortedExts;
        sortedExts.reserve(extCounts.size());
        for (auto& kv : extCounts) {
            sortedExts.emplace_back(std::string(kv.first), kv.second);
        }
        std::sort(sortedExts.begin(), sortedExts.end(),
                  [](const auto& a, const auto& b) { return a.second > b.second; });

        constexpr size_t kMaxFacetValues = 10;
        SearchFacet facet;
        facet.name = "extension";
        facet.displayName = "File Type";
        for (size_t i = 0; i < std::min(sortedExts.size(), kMaxFacetValues); ++i) {
            SearchFacet::FacetValue fv;
            fv.value = sortedExts[i].first;
            fv.display = sortedExts[i].first;
            fv.count = sortedExts[i].second;
            facet.values.push_back(std::move(fv));
        }
        facet.totalValues = sortedExts.size();
        response.facets.push_back(std::move(facet));
    }

    response.timedOutComponents = std::move(timedOut);
    response.failedComponents = std::move(failed);
    response.contributingComponents = std::move(contributing);
    response.skippedComponents = std::move(skipped);
    response.usedEarlyTermination = false;
    if (workingConfig.includeComponentTiming) {
        response.componentTimingMicros = std::move(componentTiming);
    }
    response.isDegraded =
        !response.timedOutComponents.empty() || !response.failedComponents.empty();

    if (stageTraceEnabled) {
        const size_t traceTopDefault =
            std::max(userLimit, std::max(workingConfig.rerankTopK, workingConfig.graphRerankTopN));
        const size_t traceTopCount =
            envSizeTOrDefault("YAMS_SEARCH_STAGE_TRACE_TOP_N", traceTopDefault, 1, 10000);
        const size_t componentTopDefault = std::min<size_t>(traceTopCount, 25);
        const size_t componentTopCount =
            std::min(traceTopCount, envSizeTOrDefault("YAMS_SEARCH_STAGE_TRACE_COMPONENT_TOP_N",
                                                      componentTopDefault, 1, 10000));

        const std::vector<std::string> postFusionAllDocIds =
            collectRankedResultDocIds(postFusionSnapshot);
        const std::vector<std::string> graphlessPostFusionAllDocIds = collectRankedResultDocIds(
            graphlessPostFusionSnapshot.empty() ? postFusionSnapshot : graphlessPostFusionSnapshot);
        const std::vector<std::string> postGraphAllDocIds = collectRankedResultDocIds(
            postGraphSnapshot.empty() ? postFusionSnapshot : postGraphSnapshot);
        const std::vector<std::string> finalAllDocIds = collectRankedResultDocIds(response.results);

        const std::vector<std::string> fusionDroppedDocIds =
            setDifferenceIds(preFusionDocIds, postFusionAllDocIds);
        const std::vector<std::string> graphAddedPostFusionDocIds =
            setDifferenceIds(postFusionAllDocIds, graphlessPostFusionAllDocIds);
        const std::vector<std::string> graphDisplacedPostFusionDocIds =
            setDifferenceIds(graphlessPostFusionAllDocIds, postFusionAllDocIds);

        size_t vectorOnlyDocs = 0;
        size_t vectorOnlyBelowThreshold = 0;
        size_t vectorOnlyAboveThreshold = 0;
        size_t vectorOnlyNearMissEligible = 0;
        size_t strongVectorOnlyDocs = 0;
        size_t strongVectorOnlyScoreEligibleDocs = 0;
        size_t strongVectorOnlyRankEligibleDocs = 0;
        size_t anchorAndVectorDocs = 0;
        size_t anchorOnlyDocs = 0;
        std::vector<std::pair<std::string, double>> vectorOnlyBelowDocs;
        std::vector<std::pair<std::string, double>> vectorOnlyAboveDocs;

        const double nearMissSlack =
            std::clamp(static_cast<double>(workingConfig.vectorOnlyNearMissSlack), 0.0, 1.0);
        const bool nearMissReserveEnabled = workingConfig.vectorOnlyNearMissReserve > 0;

        for (const auto& [docId, signal] : preFusionSignals) {
            if (signal.hasAnchoring && signal.hasVector) {
                anchorAndVectorDocs++;
            } else if (signal.hasAnchoring && !signal.hasVector) {
                anchorOnlyDocs++;
            }

            if (signal.hasVector && !signal.hasAnchoring) {
                vectorOnlyDocs++;
                const bool scoreEligible =
                    workingConfig.enableStrongVectorOnlyRelief &&
                    signal.maxVectorRaw >=
                        static_cast<double>(workingConfig.strongVectorOnlyMinScore);
                const bool rankEligible =
                    workingConfig.enableStrongVectorOnlyRelief &&
                    workingConfig.strongVectorOnlyTopRank > 0 &&
                    signal.bestVectorRank != std::numeric_limits<size_t>::max() &&
                    signal.bestVectorRank < workingConfig.strongVectorOnlyTopRank;
                if (scoreEligible) {
                    strongVectorOnlyScoreEligibleDocs++;
                }
                if (rankEligible) {
                    strongVectorOnlyRankEligibleDocs++;
                }
                if (scoreEligible || rankEligible) {
                    strongVectorOnlyDocs++;
                }

                if (signal.maxVectorRaw < static_cast<double>(workingConfig.vectorOnlyThreshold)) {
                    vectorOnlyBelowThreshold++;
                    vectorOnlyBelowDocs.emplace_back(docId, signal.maxVectorRaw);
                    if (nearMissReserveEnabled &&
                        signal.maxVectorRaw + nearMissSlack >=
                            static_cast<double>(workingConfig.vectorOnlyThreshold)) {
                        vectorOnlyNearMissEligible++;
                    }
                } else {
                    vectorOnlyAboveThreshold++;
                    vectorOnlyAboveDocs.emplace_back(docId, signal.maxVectorRaw);
                }
            }
        }

        auto byScoreDesc = [](const auto& a, const auto& b) {
            if (a.second != b.second) {
                return a.second > b.second;
            }
            return a.first < b.first;
        };
        std::sort(vectorOnlyBelowDocs.begin(), vectorOnlyBelowDocs.end(), byScoreDesc);
        std::sort(vectorOnlyAboveDocs.begin(), vectorOnlyAboveDocs.end(), byScoreDesc);

        std::vector<std::string> vectorOnlyBelowTop;
        std::vector<std::string> vectorOnlyAboveTop;
        vectorOnlyBelowTop.reserve(std::min(componentTopCount, vectorOnlyBelowDocs.size()));
        vectorOnlyAboveTop.reserve(std::min(componentTopCount, vectorOnlyAboveDocs.size()));
        for (size_t i = 0; i < std::min(componentTopCount, vectorOnlyBelowDocs.size()); ++i) {
            vectorOnlyBelowTop.push_back(vectorOnlyBelowDocs[i].first);
        }
        for (size_t i = 0; i < std::min(componentTopCount, vectorOnlyAboveDocs.size()); ++i) {
            vectorOnlyAboveTop.push_back(vectorOnlyAboveDocs[i].first);
        }

        const json componentSummary =
            buildComponentHitSummaryJson(allComponentResults, componentTopCount);
        const json fusionTopSummary = buildFusionTopSummaryJson(postFusionSnapshot, traceTopCount);
        const json graphlessFusionTopSummary = buildFusionTopSummaryJson(
            graphlessPostFusionSnapshot.empty() ? postFusionSnapshot : graphlessPostFusionSnapshot,
            traceTopCount);
        const json graphTopSummary = buildFusionTopSummaryJson(
            postGraphSnapshot.empty() ? postFusionSnapshot : postGraphSnapshot, traceTopCount);
        const json finalTopSummary = buildFusionTopSummaryJson(response.results, traceTopCount);
        const json graphDisplacementSummary = {
            {"graph_added_post_fusion_count", graphAddedPostFusionDocIds.size()},
            {"graph_displaced_post_fusion_count", graphDisplacedPostFusionDocIds.size()},
            {"graph_added_post_fusion_doc_ids", graphAddedPostFusionDocIds},
            {"graph_displaced_post_fusion_doc_ids", graphDisplacedPostFusionDocIds},
        };
        const json preFusionSignalSummary = {
            {"vector_only_docs", vectorOnlyDocs},
            {"vector_only_below_threshold", vectorOnlyBelowThreshold},
            {"vector_only_above_threshold", vectorOnlyAboveThreshold},
            {"vector_only_near_miss_eligible", vectorOnlyNearMissEligible},
            {"anchor_and_vector_docs", anchorAndVectorDocs},
            {"anchor_only_docs", anchorOnlyDocs},
            {"vector_only_threshold", workingConfig.vectorOnlyThreshold},
            {"vector_only_penalty", workingConfig.vectorOnlyPenalty},
            {"strong_vector_only_relief_enabled", workingConfig.enableStrongVectorOnlyRelief},
            {"strong_vector_only_min_score", workingConfig.strongVectorOnlyMinScore},
            {"strong_vector_only_top_rank", workingConfig.strongVectorOnlyTopRank},
            {"strong_vector_only_penalty", workingConfig.strongVectorOnlyPenalty},
            {"strong_vector_only_docs", strongVectorOnlyDocs},
            {"strong_vector_only_score_eligible_docs", strongVectorOnlyScoreEligibleDocs},
            {"strong_vector_only_rank_eligible_docs", strongVectorOnlyRankEligibleDocs},
            {"vector_only_near_miss_reserve", workingConfig.vectorOnlyNearMissReserve},
            {"vector_only_near_miss_slack", workingConfig.vectorOnlyNearMissSlack},
            {"vector_only_near_miss_penalty", workingConfig.vectorOnlyNearMissPenalty},
            {"semantic_rescue_slots", workingConfig.semanticRescueSlots},
            {"semantic_rescue_min_vector_score", workingConfig.semanticRescueMinVectorScore},
            {"semantic_rescue_target", semanticRescueTarget},
            {"semantic_rescue_final_count", semanticRescueFinalCount},
            {"semantic_rescue_rate", semanticRescueRate},
            {"adaptive_vector_fallback", workingConfig.enableAdaptiveVectorFallback},
            {"adaptive_vector_skip_min_tier1_hits", workingConfig.adaptiveVectorSkipMinTier1Hits},
            {"adaptive_vector_skip_require_text_signal",
             workingConfig.adaptiveVectorSkipRequireTextSignal},
            {"adaptive_vector_skip_min_text_hits", workingConfig.adaptiveVectorSkipMinTextHits},
            {"adaptive_vector_skip_min_top_text_score",
             workingConfig.adaptiveVectorSkipMinTopTextScore},
            {"vector_only_below_top_doc_ids", vectorOnlyBelowTop},
            {"vector_only_above_top_doc_ids", vectorOnlyAboveTop},
        };

        response.debugStats["trace_enabled"] = "1";
        response.debugStats["trace_query_intent"] = queryIntentToString(intent);
        response.debugStats["trace_query_intent_reason"] = routeDecision.intent.reason;
        response.debugStats["trace_retrieval_mode"] =
            queryRetrievalModeToString(routeDecision.retrievalMode.label);
        response.debugStats["trace_retrieval_mode_reason"] = routeDecision.retrievalMode.reason;
        response.debugStats["trace_query_community"] =
            routeDecision.community.has_value()
                ? queryCommunityToString(routeDecision.community->label)
                : "none";
        response.debugStats["trace_query_community_reason"] =
            routeDecision.community.has_value() ? routeDecision.community->reason : "";
        response.debugStats["trace_zoom_level"] =
            SearchEngineConfig::navigationZoomLevelToString(effectiveZoomLevel);
        response.debugStats["trace_zoom_source"] =
            zoomLevelInferredFromIntent ? "intent_auto" : "configured";
        response.debugStats["trace_user_limit"] = std::to_string(userLimit);
        response.debugStats["trace_fusion_candidate_limit"] = std::to_string(fusionCandidateLimit);
        response.debugStats["trace_top_window"] = std::to_string(traceTopCount);
        response.debugStats["trace_top_window_default"] = std::to_string(traceTopDefault);
        response.debugStats["trace_component_top_window"] = std::to_string(componentTopCount);
        response.debugStats["trace_component_top_window_default"] =
            std::to_string(componentTopDefault);
        response.debugStats["trace_graph_rerank_applied"] = graphRerankApplied ? "1" : "0";
        response.debugStats["trace_cross_rerank_applied"] = crossRerankApplied ? "1" : "0";
        response.debugStats["trace_turboquant_rerank_applied"] =
            turboQuantRerankApplied ? "1" : "0";
        response.debugStats["trace_rerank_guard_score_gap"] =
            fmt::format("{:.6f}", rerankGuardScoreGap);
        response.debugStats["trace_rerank_guard_has_competitive_anchored_evidence"] =
            rerankGuardCompetitiveAnchoredEvidence ? "1" : "0";
        response.debugStats["trace_rerank_guard_anchored_doc_ids"] =
            joinWithTab(rerankGuardAnchoredDocIds);

        response.debugStats["trace_pre_fusion_unique_count"] =
            std::to_string(preFusionDocIds.size());
        response.debugStats["trace_post_fusion_count"] = std::to_string(postFusionAllDocIds.size());
        response.debugStats["trace_post_graph_count"] = std::to_string(postGraphAllDocIds.size());
        response.debugStats["trace_final_count"] = std::to_string(finalAllDocIds.size());
        response.debugStats["trace_fusion_dropped_count"] =
            std::to_string(fusionDroppedDocIds.size());

        response.debugStats["trace_pre_fusion_doc_ids"] = joinWithTab(preFusionDocIds);
        response.debugStats["trace_post_fusion_doc_ids"] = joinWithTab(postFusionAllDocIds);
        response.debugStats["trace_graphless_post_fusion_doc_ids"] =
            joinWithTab(graphlessPostFusionAllDocIds);
        response.debugStats["trace_post_graph_doc_ids"] = joinWithTab(postGraphAllDocIds);
        response.debugStats["trace_final_doc_ids"] = joinWithTab(finalAllDocIds);
        response.debugStats["trace_fusion_dropped_doc_ids"] = joinWithTab(fusionDroppedDocIds);
        response.debugStats["trace_graph_added_post_fusion_doc_ids"] =
            joinWithTab(graphAddedPostFusionDocIds);
        response.debugStats["trace_graph_displaced_post_fusion_doc_ids"] =
            joinWithTab(graphDisplacedPostFusionDocIds);

        response.debugStats["trace_post_fusion_top_doc_ids"] =
            joinWithTab(collectRankedResultDocIds(postFusionSnapshot, traceTopCount));
        response.debugStats["trace_post_graph_top_doc_ids"] = joinWithTab(collectRankedResultDocIds(
            postGraphSnapshot.empty() ? postFusionSnapshot : postGraphSnapshot, traceTopCount));
        response.debugStats["trace_final_top_doc_ids"] =
            joinWithTab(collectRankedResultDocIds(response.results, traceTopCount));

        response.debugStats["trace_component_hits_json"] = componentSummary.dump();
        response.debugStats["trace_prefusion_signal_summary_json"] = preFusionSignalSummary.dump();
        response.debugStats["trace_fusion_top_json"] = fusionTopSummary.dump();
        response.debugStats["trace_graphless_fusion_top_json"] = graphlessFusionTopSummary.dump();
        response.debugStats["trace_post_graph_top_json"] = graphTopSummary.dump();
        response.debugStats["trace_final_top_json"] = finalTopSummary.dump();
        response.debugStats["trace_graph_displacement_summary_json"] =
            graphDisplacementSummary.dump();
    }

    response.debugStats["trace_stage_summary_json"] = traceCollector.buildStageSummaryJson().dump();
    response.debugStats["trace_fusion_source_summary_json"] =
        traceCollector
            .buildFusionSourceSummaryJson(allComponentResults, response.results,
                                          std::max<size_t>(userLimit, size_t{25}))
            .dump();

    if (tuner_) {
        SearchTuner::RuntimeTelemetry telemetry;
        telemetry.latencyMs =
            static_cast<double>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now() - startTime)
                                    .count());
        telemetry.finalResultCount = response.results.size();
        telemetry.topWindow = std::max<size_t>(userLimit, size_t{25});
        telemetry.zoomLevel = effectiveZoomLevel;

        try {
            auto stageSummary = traceCollector.buildStageSummaryJson();
            if (stageSummary.is_object()) {
                for (const auto& [name, data] : stageSummary.items()) {
                    if (!data.is_object()) {
                        continue;
                    }
                    SearchTuner::RuntimeStageSignal signal;
                    signal.enabled = data.value("enabled", false);
                    signal.attempted = data.value("attempted", false);
                    signal.contributed = data.value("contributed", false);
                    signal.skipped = data.value("skipped", false);
                    signal.durationMs = data.value("duration_ms", 0.0);
                    signal.rawHitCount = data.value("raw_hit_count", 0UL);
                    signal.uniqueDocCount = data.value("unique_doc_count", 0UL);
                    telemetry.stages.emplace(name, signal);
                }
            }
        } catch (...) { // NOLINT(bugprone-empty-catch) — best-effort telemetry; skip stage summary
                        // if trace data is malformed
        }

        try {
            auto fusionSummary = traceCollector.buildFusionSourceSummaryJson(
                allComponentResults, response.results, std::max<size_t>(userLimit, size_t{25}));
            if (fusionSummary.is_object()) {
                for (const auto& [name, data] : fusionSummary.items()) {
                    if (!data.is_object()) {
                        continue;
                    }
                    SearchTuner::RuntimeFusionSignal signal;
                    signal.enabled = data.value("enabled", false);
                    signal.contributedToFinal = data.value("contributed_to_final", false);
                    signal.configuredWeight = data.value("weight", 0.0);
                    signal.finalScoreMass = data.value("final_score_mass", 0.0);
                    signal.finalTopDocCount = data.value("final_top_doc_count", 0UL);
                    signal.rawHitCount = data.value("raw_hit_count", 0UL);
                    signal.uniqueDocCount = data.value("unique_doc_count", 0UL);
                    telemetry.fusionSources.emplace(name, signal);
                }
            }
        } catch (...) { // NOLINT(bugprone-empty-catch) — best-effort telemetry; skip fusion summary
                        // if trace data is malformed
        }

        tuner_->observe(tuningCtx, telemetry);
        const auto tunerState = tuner_->adaptiveStateToJson();
        response.debugStats["tuner_adaptive_active"] = "1";
        response.debugStats["tuner_decision_reason"] =
            tunerState.value("last_decision", std::string{"unknown"});
        response.debugStats["tuner_adjustments_json"] = tunerState.dump();
        response.debugStats["tuner_runtime_config_json"] = tuner_->getParams().toJson().dump();
        // R3: emit the stable bucket key so users can see which context
        // bucket drove a given query's policy decision. The rules policy
        // ignores the bucket; R5's orchestrator reads it for per-bucket
        // handoff gating.
        response.debugStats["tuner_context_bucket"] = bucketize(tuningCtx);
        response.debugStats["tuner_backend"] = "rules";
    } else {
        response.debugStats["tuner_adaptive_active"] = "0";
    }

    auto endTime = std::chrono::steady_clock::now();
    response.executionTimeMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();

    stats_.successfulQueries.fetch_add(1, std::memory_order_relaxed);
    auto durationMicros =
        std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime).count();
    stats_.totalQueryTimeMicros.fetch_add(durationMicros, std::memory_order_relaxed);

    uint64_t totalQueries = stats_.totalQueries.load(std::memory_order_relaxed);
    if (totalQueries > 0) {
        stats_.avgQueryTimeMicros.store(
            stats_.totalQueryTimeMicros.load(std::memory_order_relaxed) / totalQueries,
            std::memory_order_relaxed);
    }

    if (response.isDegraded && response.hasResults()) {
        spdlog::debug("Search returned {} results (degraded: {} timed out, {} failed)",
                      response.results.size(), response.timedOutComponents.size(),
                      response.failedComponents.size());
    }

    return response;
}

Result<std::vector<ComponentResult>> SearchEngine::Impl::queryPathTree(const std::string& query,
                                                                       size_t limit) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    if (!metadataRepo_) {
        return results;
    }

    try {
        yams::metadata::DocumentQueryOptions options;
        options.containsFragment = query;
        options.limit = static_cast<int>(limit);

        auto docResults = metadataRepo_->queryDocuments(options);
        if (!docResults) {
            spdlog::debug("Path tree query failed: {}", docResults.error().message);
            return results;
        }

        // Convert to ComponentResults
        for (size_t rank = 0; rank < docResults.value().size(); ++rank) {
            const auto& doc = docResults.value()[rank];

            ComponentResult result;
            result.documentHash = doc.sha256Hash;
            result.filePath = doc.filePath;

            std::string_view pathView = doc.filePath;
            std::string_view queryView = query;

            size_t pos = ci_find(pathView, queryView);

            if (pos != std::string_view::npos) {
                float positionScore =
                    1.0f - (static_cast<float>(pos) / static_cast<float>(pathView.length()));
                float lengthScore =
                    static_cast<float>(queryView.length()) / static_cast<float>(pathView.length());
                result.score = (positionScore * 0.3f + lengthScore * 0.7f);
            } else {
                result.score = 0.5f;
            }

            result.source = ComponentResult::Source::PathTree;
            result.rank = rank;
            result.snippet = std::optional<std::string>(doc.filePath);
            result.debugInfo["path"] = doc.filePath;
            result.debugInfo["path_depth"] = std::to_string(doc.pathDepth);

            results.push_back(std::move(result));
        }

        spdlog::debug("Path tree query returned {} results for query: {}", results.size(), query);

    } catch (const std::exception& e) {
        spdlog::warn("Path tree query exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryFullText(const std::string& query, QueryIntent queryIntent,
                                  const SearchEngineConfig& config, size_t limit,
                                  QueryExpansionStats* expansionStats,
                                  const std::vector<GraphExpansionTerm>* graphExpansionTerms) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    if (!metadataRepo_) {
        return results;
    }

    try {
        float nonCodeFileMultiplier = 1.0f;
        if (config.enableIntentAdaptiveWeighting) {
            switch (queryIntent) {
                case QueryIntent::Code:
                    nonCodeFileMultiplier = 0.5f;
                    break;
                case QueryIntent::Path:
                    nonCodeFileMultiplier = 0.65f;
                    break;
                case QueryIntent::Prose:
                    nonCodeFileMultiplier = 1.0f;
                    break;
                case QueryIntent::Mixed:
                    nonCodeFileMultiplier = 0.80f;
                    break;
            }
        }

        auto appendResults = [&](const yams::metadata::SearchResults& searchResults,
                                 float scorePenalty, bool dedupe,
                                 std::unordered_set<std::string>* seenHashes,
                                 ComponentResult::Source source = ComponentResult::Source::Text) {
            double minBm25 = 0.0;
            double maxBm25 = 0.0;
            bool bm25RangeInitialized = false;
            for (const auto& sr : searchResults.results) {
                double score = sr.score;
                if (!bm25RangeInitialized) {
                    minBm25 = score;
                    maxBm25 = score;
                    bm25RangeInitialized = true;
                } else {
                    minBm25 = std::min(minBm25, score);
                    maxBm25 = std::max(maxBm25, score);
                }
            }

            const size_t startRank = results.size();
            for (size_t rank = 0; rank < searchResults.results.size(); ++rank) {
                const auto& searchResult = searchResults.results[rank];
                if (dedupe && seenHashes != nullptr &&
                    seenHashes->contains(searchResult.document.sha256Hash)) {
                    continue;
                }

                const auto& filePath = searchResult.document.filePath;
                const auto& fileName = searchResult.document.fileName;

                auto pruneCategory = magic::getPruneCategory(filePath);
                bool isCodeFile = pruneCategory == magic::PruneCategory::BuildObject ||
                                  pruneCategory == magic::PruneCategory::None;

                float scoreMultiplier = isCodeFile ? 1.0f : nonCodeFileMultiplier;
                scoreMultiplier *= filenamePathBoost(query, filePath, fileName);
                scoreMultiplier *= scorePenalty;

                ComponentResult result;
                result.documentHash = searchResult.document.sha256Hash;
                result.filePath = filePath;
                float rawScore = static_cast<float>(searchResult.score);
                float normalizedScore =
                    normalizedBm25Score(rawScore, config.bm25NormDivisor, minBm25, maxBm25);
                result.score = std::clamp(scoreMultiplier * normalizedScore, 0.0f, 1.0f);
                if (source == ComponentResult::Source::GraphText &&
                    result.score < config.graphTextMinAdmissionScore) {
                    if (expansionStats != nullptr) {
                        ++expansionStats->graphTextBlockedLowScoreCount;
                    }
                    continue;
                }
                result.source = source;
                result.rank = startRank + rank;
                result.snippet = searchResult.snippet.empty()
                                     ? std::nullopt
                                     : std::optional<std::string>(searchResult.snippet);
                result.debugInfo["score_multiplier"] = fmt::format("{:.3f}", scoreMultiplier);

                if (seenHashes != nullptr) {
                    seenHashes->insert(result.documentHash);
                }
                results.push_back(std::move(result));
            }
        };

        auto fts5Results = metadataRepo_->search(query, static_cast<int>(limit), 0);
        size_t baseFtsHitCount = 0;
        const bool baseFtsSucceeded = static_cast<bool>(fts5Results);
        if (!baseFtsSucceeded) {
            spdlog::debug("FTS5 search failed, continuing with fallback expansion: {}",
                          fts5Results.error().message);
        }

        std::unordered_set<std::string> seenHashes;
        seenHashes.reserve(limit * 2);
        if (baseFtsSucceeded) {
            baseFtsHitCount = fts5Results.value().results.size();
            appendResults(fts5Results.value(), 1.0f, true, &seenHashes);
        }

        if (config.enableLexicalExpansion && baseFtsHitCount < config.lexicalExpansionMinHits) {
            std::vector<std::string> tokens = tokenizeLower(query);
            std::vector<std::string> expansionTerms;
            expansionTerms.reserve(tokens.size());
            std::unordered_set<std::string> uniqueTokens;
            uniqueTokens.reserve(tokens.size());

            for (const auto& token : tokens) {
                if (token.size() < 3) {
                    continue;
                }
                if (uniqueTokens.insert(token).second) {
                    expansionTerms.push_back(token);
                }
                if (expansionTerms.size() >= 6) {
                    break;
                }
            }

            if (expansionTerms.size() >= 2) {
                std::string expandedQuery;
                expandedQuery.reserve(expansionTerms.size() * 8);
                for (size_t i = 0; i < expansionTerms.size(); ++i) {
                    if (i > 0) {
                        expandedQuery += " OR ";
                    }
                    expandedQuery += expansionTerms[i];
                }

                auto expandedResults =
                    metadataRepo_->search(expandedQuery, static_cast<int>(limit), 0);
                if (expandedResults) {
                    const float penalty =
                        std::clamp(config.lexicalExpansionScorePenalty, 0.1f, 1.0f);
                    appendResults(expandedResults.value(), penalty, true, &seenHashes);
                    spdlog::debug("queryFullText lexical expansion: base_hits={} expanded_hits={} "
                                  "query='{}' expanded='{}'",
                                  baseFtsHitCount, results.size(), query, expandedQuery);
                }
            }
        }

        if (config.enableSubPhraseExpansion && baseFtsHitCount < config.subPhraseExpansionMinHits) {
            const size_t maxSubPhrases = std::max<size_t>(config.multiVectorMaxPhrases, 3);
            auto subPhrases = generateQuerySubPhrases(query, maxSubPhrases);
            if (expansionStats != nullptr) {
                expansionStats->generatedSubPhrases = subPhrases.size();
            }

            std::vector<std::string> clauses;
            clauses.reserve(subPhrases.size());
            std::unordered_set<std::string> seenClauses;
            seenClauses.reserve(subPhrases.size());

            for (const auto& phrase : subPhrases) {
                auto tokens = tokenizeQueryTokens(phrase);
                if (tokens.size() < 2) {
                    continue;
                }

                std::string clause = "(";
                for (size_t i = 0; i < tokens.size(); ++i) {
                    if (i > 0) {
                        clause += " AND ";
                    }
                    clause += tokens[i].normalized;
                }
                clause += ")";

                if (seenClauses.insert(clause).second) {
                    clauses.push_back(std::move(clause));
                }
            }

            if (expansionStats != nullptr) {
                expansionStats->subPhraseClauseCount = clauses.size();
            }

            if (!clauses.empty()) {
                std::string expandedQuery;
                for (size_t i = 0; i < clauses.size(); ++i) {
                    if (i > 0) {
                        expandedQuery += " OR ";
                    }
                    expandedQuery += clauses[i];
                }

                auto expandedResults =
                    metadataRepo_->search(expandedQuery, static_cast<int>(limit), 0);
                if (expandedResults) {
                    if (expansionStats != nullptr) {
                        expansionStats->subPhraseFtsHitCount =
                            expandedResults.value().results.size();
                    }
                    const float penalty = std::clamp(config.subPhraseExpansionPenalty, 0.1f, 1.0f);
                    const size_t beforeAppend = results.size();
                    appendResults(expandedResults.value(), penalty, true, &seenHashes);
                    if (expansionStats != nullptr) {
                        expansionStats->subPhraseFtsAddedCount =
                            results.size() > beforeAppend ? (results.size() - beforeAppend) : 0;
                    }
                    spdlog::debug("queryFullText sub-phrase expansion: base_hits={} "
                                  "expanded_hits={} query='{}' expanded='{}'",
                                  baseFtsHitCount, results.size(), query, expandedQuery);
                }
            }
        }

        // Sub-phrase rescoring: unconditionally re-score already-retrieved documents
        // via AND-clause sub-phrase queries. Unlike expansion (which adds new docs),
        // this pass updates scores in-place, so it helps even when base FTS returns the
        // entire corpus at low scores and the expansion gates (baseFtsHitCount < N)
        // never fire. Controlled by enableSubPhraseRescoring in tuner profiles.
        if (config.enableSubPhraseRescoring && !results.empty()) {
            std::unordered_map<std::string, size_t> rescoreIndex;
            rescoreIndex.reserve(results.size());
            for (size_t i = 0; i < results.size(); ++i) {
                rescoreIndex.emplace(results[i].documentHash, i);
            }

            const size_t maxPhrases = std::max<size_t>(config.multiVectorMaxPhrases, 4);
            auto subPhrases = generateQuerySubPhrases(query, maxPhrases);
            const float rescorePenalty = std::clamp(config.subPhraseScoringPenalty, 0.1f, 1.0f);

            for (const auto& phrase : subPhrases) {
                auto phraseTokens = tokenizeQueryTokens(phrase);
                if (phraseTokens.size() < 2) {
                    continue;
                }

                std::string clause = "(";
                for (size_t i = 0; i < phraseTokens.size(); ++i) {
                    if (i > 0) {
                        clause += " AND ";
                    }
                    clause += phraseTokens[i].normalized;
                }
                clause += ")";

                auto phraseResults = metadataRepo_->search(clause, static_cast<int>(limit), 0);
                if (!phraseResults || phraseResults.value().results.empty()) {
                    continue;
                }

                // BM25 min/max for normalization within this sub-phrase result set.
                // Results are returned sorted descending, so front=max, back=min.
                const double phMax = phraseResults.value().results.front().score;
                const double phMin = phraseResults.value().results.back().score;

                for (const auto& pr : phraseResults.value().results) {
                    auto it = rescoreIndex.find(pr.document.sha256Hash);
                    if (it == rescoreIndex.end()) {
                        continue;
                    }
                    float phraseNorm = normalizedBm25Score(static_cast<float>(pr.score),
                                                           config.bm25NormDivisor, phMin, phMax);
                    float boost = std::clamp(phraseNorm * rescorePenalty, 0.0f, 1.0f);
                    results[it->second].score = std::max(results[it->second].score, boost);
                }
            }
        }

        if ((!baseFtsSucceeded || baseFtsHitCount == 0 || results.size() < 3) && metadataRepo_) {
            const size_t maxAggressiveClauses =
                (!baseFtsSucceeded || baseFtsHitCount == 0) ? 10 : 6;
            auto aggressiveClauses = generateAggressiveFtsFallbackClauses(
                query, maxAggressiveClauses, lookupQueryTermIdf(query));
            if (expansionStats != nullptr) {
                expansionStats->aggressiveClauseCount = aggressiveClauses.size();
            }

            size_t totalAggressiveHits = 0;
            const size_t beforeAggressive = results.size();
            for (const auto& clause : aggressiveClauses) {
                auto clauseResults =
                    metadataRepo_->search(clause.query, static_cast<int>(limit), 0);
                if (!clauseResults) {
                    continue;
                }
                totalAggressiveHits += clauseResults.value().results.size();
                appendResults(clauseResults.value(), clause.penalty, true, &seenHashes);
            }

            if (expansionStats != nullptr) {
                expansionStats->aggressiveFtsHitCount = totalAggressiveHits;
                expansionStats->aggressiveFtsAddedCount =
                    results.size() > beforeAggressive ? (results.size() - beforeAggressive) : 0;
            }

            if (!aggressiveClauses.empty()) {
                std::vector<std::string> debugClauses;
                debugClauses.reserve(aggressiveClauses.size());
                for (const auto& clause : aggressiveClauses) {
                    debugClauses.push_back(clause.query);
                }
                spdlog::debug("queryFullText aggressive fallback: base_succeeded={} base_hits={} "
                              "clauses={} added_hits={} query='{}' clauses='{}'",
                              baseFtsSucceeded ? 1 : 0, baseFtsHitCount, aggressiveClauses.size(),
                              results.size(), query, debugClauses.size());
            }
        }

        if (config.enableGraphQueryExpansion && baseFtsHitCount < config.graphExpansionMinHits) {
            std::vector<GraphExpansionTerm> graphTerms;
            if (graphExpansionTerms != nullptr) {
                graphTerms = *graphExpansionTerms;
            } else {
                graphTerms = generateGraphExpansionTerms(
                    kgStore_, query, {},
                    GraphExpansionConfig{.maxTerms = config.graphExpansionMaxTerms,
                                         .maxSeeds = config.graphExpansionMaxSeeds,
                                         .maxNeighbors = config.graphMaxNeighbors});
            }
            if (expansionStats != nullptr) {
                expansionStats->graphExpansionTermCount = graphTerms.size();
            }

            size_t totalGraphHits = 0;
            const size_t beforeGraph = results.size();
            for (const auto& term : graphTerms) {
                auto graphResults = metadataRepo_->search(term.text, static_cast<int>(limit), 0);
                if (!graphResults) {
                    continue;
                }
                totalGraphHits += graphResults.value().results.size();
                const float penalty =
                    std::clamp(config.graphExpansionFtsPenalty * std::clamp(term.score, 0.2f, 1.0f),
                               0.1f, 1.0f);
                appendResults(graphResults.value(), penalty, true, &seenHashes,
                              ComponentResult::Source::GraphText);
            }

            if (expansionStats != nullptr) {
                expansionStats->graphExpansionFtsHitCount = totalGraphHits;
                expansionStats->graphExpansionFtsAddedCount =
                    results.size() > beforeGraph ? (results.size() - beforeGraph) : 0;
            }

            if (!graphTerms.empty()) {
                std::vector<std::string> debugTerms;
                debugTerms.reserve(graphTerms.size());
                for (const auto& term : graphTerms) {
                    debugTerms.push_back(term.text);
                }
                spdlog::debug("queryFullText graph expansion: base_hits={} graph_terms={} "
                              "graph_hits={} final_hits={} query='{}' terms='{}'",
                              baseFtsHitCount, graphTerms.size(), totalGraphHits, results.size(),
                              query, debugTerms.size());
            }
        }

        spdlog::debug("queryFullText: {} results for query '{}' (limit={})", results.size(),
                      query.substr(0, 50), limit);

        // NOLINTNEXTLINE(concurrency-mt-unsafe) — debug env var; not modified concurrently
        if (const char* env = std::getenv("YAMS_SEARCH_DIAG"); env && std::string(env) == "1") {
            spdlog::warn("[search_diag] text_hits={} limit={} query='{}'", results.size(), limit,
                         query);
        }

    } catch (const std::exception& e) {
        spdlog::warn("Full-text query exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryKnowledgeGraph(const std::string& query, size_t limit,
                                        const std::vector<QueryConcept>* concepts) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    if (!kgStore_) {
        return results;
    }

    try {
        std::vector<std::string> queryTerms;
        std::unordered_set<std::string> seenTerms;
        if (concepts != nullptr) {
            for (const auto& queryConcept : *concepts) {
                std::string normalized = normalizeEntityTextForKey(queryConcept.text);
                if (normalized.size() < 3 || !seenTerms.insert(normalized).second) {
                    continue;
                }
                queryTerms.push_back(queryConcept.text);
            }
        }

        // Fall back to token-level alias lookup only when the query was not grounded
        // to any explicit concepts.
        if (queryTerms.empty()) {
            std::vector<std::string> queryTokens = tokenizeKgQuery(query);
            for (const auto& token : queryTokens) {
                if (!seenTerms.insert(token).second) {
                    continue;
                }
                queryTerms.push_back(token);
            }
        }

        if (queryTerms.empty()) {
            return results;
        }

        // Collect aliases from all tokens with score tracking
        std::vector<metadata::AliasResolution> aliases;
        std::unordered_map<int64_t, float> nodeIdToScore; // Track best score per node

        // Limit aliases per token to avoid explosion
        const size_t aliasesPerToken = std::max(size_t(3), limit / queryTerms.size());

        for (const auto& tok : queryTerms) {
            auto aliasResults = kgStore_->resolveAliasExact(tok, aliasesPerToken);
            if (!aliasResults || aliasResults.value().empty()) {
                auto labelMatches = kgStore_->searchNodesByLabel(tok, aliasesPerToken, 0);
                if (labelMatches && !labelMatches.value().empty()) {
                    for (const auto& node : labelMatches.value()) {
                        const float score = tok.find(' ') != std::string::npos ? 0.90f : 0.75f;
                        auto it = nodeIdToScore.find(node.id);
                        if (it == nodeIdToScore.end()) {
                            metadata::AliasResolution alias;
                            alias.nodeId = node.id;
                            alias.score = score;
                            nodeIdToScore[node.id] = score;
                            aliases.push_back(std::move(alias));
                        } else {
                            it->second = std::min(1.0f, it->second + score * 0.5f);
                        }
                    }
                    continue;
                }

                auto fuzzyResults = kgStore_->resolveAliasFuzzy(tok, aliasesPerToken);
                if (fuzzyResults && !fuzzyResults.value().empty()) {
                    for (const auto& alias : fuzzyResults.value()) {
                        const float score = alias.score * 0.8f;
                        auto it = nodeIdToScore.find(alias.nodeId);
                        if (it == nodeIdToScore.end()) {
                            nodeIdToScore[alias.nodeId] = score;
                            aliases.push_back(alias);
                        } else {
                            it->second = std::min(1.0f, it->second + score * 0.5f);
                        }
                    }
                }
                continue;
            }

            for (const auto& alias : aliasResults.value()) {
                const float score = alias.score;
                auto it = nodeIdToScore.find(alias.nodeId);
                if (it == nodeIdToScore.end()) {
                    nodeIdToScore[alias.nodeId] = score;
                    aliases.push_back(alias);
                } else {
                    it->second = std::min(1.0f, it->second + score * 0.5f);
                }
            }
        }

        spdlog::debug("KG: {} terms -> {} unique aliases", queryTerms.size(), aliases.size());
        if (aliases.empty()) {
            return results;
        }

        // Batch fetch all nodes (single DB query instead of N)
        std::vector<std::int64_t> nodeIds;
        nodeIds.reserve(aliases.size());
        for (const auto& alias : aliases) {
            nodeIds.push_back(alias.nodeId);
        }

        auto nodesResult = kgStore_->getNodesByIds(nodeIds);
        if (!nodesResult) {
            spdlog::debug("KG batch node fetch failed: {}", nodesResult.error().message);
            return results;
        }

        // Build nodeId -> node map for quick lookup
        compat::flat_map<std::int64_t, size_t> nodeIndexMap;
        compat::reserve_if_needed(nodeIndexMap, nodesResult.value().size());
        for (size_t idx = 0; idx < nodesResult.value().size(); ++idx) {
            nodeIndexMap[nodesResult.value()[idx].id] = idx;
        }

        // Collect search terms and build term -> alias info mapping
        // This allows us to do a single batch FTS5 query instead of N queries
        const size_t maxAliasesToProcess = std::min(aliases.size(), limit);
        std::vector<std::string> searchTerms;
        searchTerms.reserve(maxAliasesToProcess);

        // Map: searchTerm -> (aliasIndex, nodeIndex) for score attribution
        std::unordered_map<std::string, std::vector<std::pair<size_t, size_t>>> termToAliasInfo;

        for (size_t i = 0; i < maxAliasesToProcess; ++i) {
            const auto& aliasRes = aliases[i];
            auto nodeIt = nodeIndexMap.find(aliasRes.nodeId);
            if (nodeIt == nodeIndexMap.end()) {
                continue;
            }

            const auto& node = nodesResult.value()[nodeIt->second];
            std::string searchTerm = node.label.value_or(node.nodeKey);

            // Track which aliases map to this term (for score attribution)
            termToAliasInfo[searchTerm].emplace_back(i, nodeIt->second);

            // Only add unique terms to the search list
            if (termToAliasInfo[searchTerm].size() == 1) {
                searchTerms.push_back(searchTerm);
            }
        }

        if (searchTerms.empty()) {
            return results;
        }

        // Build batch OR query: "term1" OR "term2" OR "term3"
        // Escape each term by wrapping in quotes (handles special chars)
        std::string batchQuery;
        for (size_t i = 0; i < searchTerms.size(); ++i) {
            if (i > 0) {
                batchQuery += " OR ";
            }
            // Quote the term to handle special characters
            batchQuery += '"';
            for (char c : searchTerms[i]) {
                if (c == '"') {
                    batchQuery += "\"\""; // Escape quotes by doubling
                } else {
                    batchQuery += c;
                }
            }
            batchQuery += '"';
        }

        // Single FTS5 query instead of N queries
        // Request more results since we're batching multiple terms
        const size_t batchLimit = std::min(limit * 3, static_cast<size_t>(200));
        auto docResults = metadataRepo_->search(batchQuery, static_cast<int>(batchLimit), 0);
        if (!docResults) {
            spdlog::debug("KG batch FTS5 search failed: {}", docResults.error().message);
            return results;
        }

        // Process results and attribute scores back to originating aliases
        std::unordered_map<std::string, size_t> docHashToResultIndex;
        docHashToResultIndex.reserve(limit);

        for (const auto& searchResult : docResults.value().results) {
            if (results.size() >= limit) {
                break;
            }

            const std::string& docHash = searchResult.document.sha256Hash;

            // Find which search term(s) this result likely matched
            // Use the snippet or fall back to first matching term
            float bestScore = 0.0f;
            std::string bestTerm;
            size_t bestNodeIdx = 0;

            for (const auto& term : searchTerms) {
                auto it = termToAliasInfo.find(term);
                if (it == termToAliasInfo.end())
                    continue;

                // Check if this term appears in snippet or path (heuristic for match attribution)
                const bool likelyMatch =
                    (!searchResult.snippet.empty() &&
                     ci_find(searchResult.snippet, term) != std::string::npos) ||
                    ci_find(searchResult.document.filePath, term) != std::string::npos;

                if (likelyMatch || bestScore == 0.0f) {
                    // Use the highest-scoring alias for this term
                    for (const auto& [aliasIdx, nodeIdx] : it->second) {
                        float aliasScore = aliases[aliasIdx].score;
                        if (aliasScore > bestScore) {
                            bestScore = aliasScore;
                            bestTerm = term;
                            bestNodeIdx = nodeIdx;
                        }
                    }
                }
            }

            // If no term matched heuristically, use first alias's score
            if (bestScore == 0.0f && !termToAliasInfo.empty()) {
                const auto& firstTerm = searchTerms[0];
                const auto& firstInfo = termToAliasInfo[firstTerm][0];
                bestScore = aliases[firstInfo.first].score;
                bestTerm = firstTerm;
                bestNodeIdx = firstInfo.second;
            }

            auto existingIt = docHashToResultIndex.find(docHash);
            if (existingIt != docHashToResultIndex.end()) {
                // Boost existing result
                results[existingIt->second].score += bestScore * 0.3f;
                continue;
            }

            const auto& node = nodesResult.value()[bestNodeIdx];

            ComponentResult result;
            result.documentHash = docHash;
            result.filePath = searchResult.document.filePath;
            result.score = bestScore * 0.8f;
            result.source = ComponentResult::Source::KnowledgeGraph;
            result.rank = results.size();
            result.snippet = searchResult.snippet.empty()
                                 ? std::optional<std::string>(bestTerm)
                                 : std::optional<std::string>(searchResult.snippet);
            result.debugInfo["node_id"] = std::to_string(node.id);
            result.debugInfo["node_key"] = node.nodeKey;
            if (node.type.has_value()) {
                result.debugInfo["node_type"] = node.type.value();
            }

            docHashToResultIndex[docHash] = results.size();
            results.push_back(std::move(result));
        }

        spdlog::debug("queryKnowledgeGraph: {} results for query '{}' (batch: {} terms)",
                      results.size(), query.substr(0, 50), searchTerms.size());

    } catch (const std::exception& e) {
        spdlog::warn("KG query exception: {}", e.what());
        return results;
    }

    return results;
}

namespace {

/// Aggregate chunk-level vector scores to document-level scores.
/// Multiple chunks from the same document are combined according to the
/// configured ChunkAggregation strategy (MAX, SUM, or TOP_K_AVG).
std::vector<vector::VectorRecord>
aggregateChunkVectorScores(const std::vector<vector::VectorRecord>& vectorRecords,
                           const SearchEngineConfig& config, size_t limit) {
    using Agg = SearchEngineConfig::ChunkAggregation;
    const auto aggStrategy = config.chunkAggregation;

    // Group chunks by document hash, keeping best record per hash
    std::unordered_map<std::string, std::vector<float>> scoresByHash;
    std::unordered_map<std::string, vector::VectorRecord> bestByHash;

    for (const auto& vr : vectorRecords) {
        if (vr.document_hash.empty()) {
            continue;
        }
        scoresByHash[vr.document_hash].push_back(vr.relevance_score);
        auto it = bestByHash.find(vr.document_hash);
        if (it == bestByHash.end()) {
            bestByHash[vr.document_hash] = vr;
        } else if (vr.relevance_score > it->second.relevance_score) {
            it->second = vr;
        }
    }

    std::vector<vector::VectorRecord> deduped;
    deduped.reserve(bestByHash.size());

    for (auto& [hash, record] : bestByHash) {
        if (aggStrategy != Agg::MAX) {
            auto& scores = scoresByHash[hash];
            if (aggStrategy == Agg::SUM) {
                double sum = 0.0;
                for (float s : scores)
                    sum += s;
                record.relevance_score = static_cast<float>(std::min(sum, 1.0));
            } else if (aggStrategy == Agg::TOP_K_AVG) {
                std::sort(scores.begin(), scores.end(), std::greater<>());
                size_t k = std::min(scores.size(), config.chunkAggregationTopK);
                double sum = 0.0;
                for (size_t i = 0; i < k; ++i)
                    sum += scores[i];
                record.relevance_score = static_cast<float>(sum / static_cast<double>(k));
            }
        }
        deduped.push_back(std::move(record));
    }

    std::sort(deduped.begin(), deduped.end(),
              [](const auto& a, const auto& b) { return a.relevance_score > b.relevance_score; });
    if (deduped.size() > limit) {
        deduped.resize(limit);
    }
    return deduped;
}

} // namespace

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryVectorIndex(const std::vector<float>& embedding,
                                     const SearchEngineConfig& config, size_t limit) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    // Vector search using VectorDatabase (sqlite-vec)
    // sqlite-vec provides efficient cosine similarity search via vec_distance_cosine()
    // Benchmarks show <30ms for 100K vectors which is acceptable for most use cases

    if (!vectorDb_) {
        return results;
    }

    try {
        vector::VectorSearchParams params;
        params.k = limit;
        params.similarity_threshold = config.similarityThreshold;

        auto vectorRecords = vectorDb_->search(embedding, params);

        // Aggregate chunk-level vector hits to document-level scores.
        // Strategy is controlled by config.chunkAggregation.
        if (!vectorRecords.empty()) {
            vectorRecords = aggregateChunkVectorScores(vectorRecords, config, limit);
        }

        if (vectorRecords.empty()) {
            return results;
        }

        // Batch fetch document info for all hashes (single DB query instead of N)
        compat::flat_map<std::string, std::string> hashToPath;
        if (metadataRepo_) {
            std::vector<std::string> hashes;
            hashes.reserve(vectorRecords.size());
            for (const auto& vr : vectorRecords) {
                hashes.push_back(vr.document_hash);
            }

            auto docMapResult = metadataRepo_->batchGetDocumentsByHash(hashes);
            if (docMapResult) {
                compat::reserve_if_needed(hashToPath, docMapResult.value().size());
                for (const auto& [hash, docInfo] : docMapResult.value()) {
                    hashToPath[hash] = docInfo.filePath;
                }
            }
        }

        for (size_t rank = 0; rank < vectorRecords.size(); ++rank) {
            const auto& vr = vectorRecords[rank];

            ComponentResult result;
            result.documentHash = vr.document_hash;
            result.score = vr.relevance_score;
            result.source = ComponentResult::Source::Vector;
            result.rank = rank;

            if (auto it = hashToPath.find(vr.document_hash); it != hashToPath.end()) {
                result.filePath = std::string(it->second);
            }
            if (!vr.content.empty()) {
                result.snippet = truncateSnippet(vr.content, 200);
            }

            results.push_back(std::move(result));
        }

        spdlog::debug("queryVectorIndex: {} results (limit={}, threshold={})", results.size(),
                      limit, config.similarityThreshold);

    } catch (const std::exception& e) {
        spdlog::warn("Vector search exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryVectorIndex(const std::vector<float>& embedding,
                                     const SearchEngineConfig& config, size_t limit,
                                     const std::unordered_set<std::string>& candidates) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    if (!vectorDb_) {
        return results;
    }

    try {
        vector::VectorSearchParams params;
        params.k = limit;
        params.similarity_threshold = config.similarityThreshold;
        params.candidate_hashes = candidates; // Narrow to Tier 1 candidates

        auto vectorRecords = vectorDb_->search(embedding, params);

        // Aggregate chunk-level vector hits to document-level scores.
        if (!vectorRecords.empty()) {
            vectorRecords = aggregateChunkVectorScores(vectorRecords, config, limit);
        }

        if (vectorRecords.empty()) {
            return results;
        }

        // Batch fetch document info for all hashes (single DB query instead of N)
        compat::flat_map<std::string, std::string> hashToPath;
        if (metadataRepo_) {
            std::vector<std::string> hashes;
            hashes.reserve(vectorRecords.size());
            for (const auto& vr : vectorRecords) {
                hashes.push_back(vr.document_hash);
            }

            auto docMapResult = metadataRepo_->batchGetDocumentsByHash(hashes);
            if (docMapResult) {
                compat::reserve_if_needed(hashToPath, docMapResult.value().size());
                for (const auto& [hash, docInfo] : docMapResult.value()) {
                    hashToPath[hash] = docInfo.filePath;
                }
            }
        }

        for (size_t rank = 0; rank < vectorRecords.size(); ++rank) {
            const auto& vr = vectorRecords[rank];

            ComponentResult result;
            result.documentHash = vr.document_hash;
            result.score = vr.relevance_score;
            result.source = ComponentResult::Source::Vector;
            result.rank = rank;

            if (auto it = hashToPath.find(vr.document_hash); it != hashToPath.end()) {
                result.filePath = std::string(it->second);
            }
            if (!vr.content.empty()) {
                result.snippet = truncateSnippet(vr.content, 200);
            }

            results.push_back(std::move(result));
        }

        spdlog::debug("Vector search (narrowed to {} candidates) returned {} results",
                      candidates.size(), results.size());

    } catch (const std::exception& e) {
        spdlog::warn("Vector search exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryEntityVectors(const std::vector<float>& embedding,
                                       const SearchEngineConfig& config, size_t limit) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    // Entity vector search finds semantically similar symbols (functions, classes, etc.)
    // This enables queries like "authentication handler" to find AuthMiddleware::handleRequest

    if (!vectorDb_) {
        return results;
    }

    try {
        vector::EntitySearchParams params;
        params.k = limit;
        params.similarity_threshold = config.similarityThreshold;
        params.include_embeddings = false; // Don't need embeddings in results

        auto entityRecords = vectorDb_->searchEntities(embedding, params);

        // Keep only the best entity hit per source document to reduce ranking noise from
        // documents with many similar symbols.
        if (!entityRecords.empty()) {
            std::unordered_map<std::string, size_t> bestByHash;
            bestByHash.reserve(entityRecords.size());

            std::vector<vector::EntityVectorRecord> deduped;
            deduped.reserve(entityRecords.size());

            for (const auto& er : entityRecords) {
                if (er.document_hash.empty()) {
                    continue;
                }
                auto it = bestByHash.find(er.document_hash);
                if (it == bestByHash.end()) {
                    bestByHash[er.document_hash] = deduped.size();
                    deduped.push_back(er);
                } else if (er.relevance_score > deduped[it->second].relevance_score) {
                    deduped[it->second] = er;
                }
            }

            std::sort(deduped.begin(), deduped.end(), [](const auto& a, const auto& b) {
                return a.relevance_score > b.relevance_score;
            });
            if (deduped.size() > limit) {
                deduped.resize(limit);
            }
            entityRecords.swap(deduped);
        }

        if (entityRecords.empty()) {
            return results;
        }

        // Batch fetch document info for all hashes (single DB query instead of N)
        compat::flat_map<std::string, std::string> hashToPath;
        if (metadataRepo_) {
            std::vector<std::string> hashes;
            hashes.reserve(entityRecords.size());
            for (const auto& er : entityRecords) {
                if (!er.document_hash.empty()) {
                    hashes.push_back(er.document_hash);
                }
            }

            if (!hashes.empty()) {
                auto docMapResult = metadataRepo_->batchGetDocumentsByHash(hashes);
                if (docMapResult) {
                    compat::reserve_if_needed(hashToPath, docMapResult.value().size());
                    for (const auto& [hash, docInfo] : docMapResult.value()) {
                        hashToPath[hash] = docInfo.filePath;
                    }
                }
            }
        }

        for (size_t rank = 0; rank < entityRecords.size(); ++rank) {
            const auto er = entityRecords[rank];

            ComponentResult result;
            result.documentHash = er.document_hash;
            result.score = er.relevance_score;
            result.source = ComponentResult::Source::EntityVector;
            result.rank = rank;

            // Use file_path from entity record, or look up from metadata
            if (!er.file_path.empty()) {
                result.filePath = er.file_path;
            } else if (auto it = hashToPath.find(er.document_hash); it != hashToPath.end()) {
                result.filePath = std::string(it->second);
            }

            // Create snippet from entity info: qualified_name or node_key
            if (!er.qualified_name.empty()) {
                result.snippet = er.qualified_name;
            } else if (!er.node_key.empty()) {
                result.snippet = er.node_key;
            }

            // Debug info for entity search results
            result.debugInfo["node_key"] = er.node_key;
            if (!er.node_type.empty()) {
                result.debugInfo["node_type"] = er.node_type;
            }
            if (!er.qualified_name.empty()) {
                result.debugInfo["qualified_name"] = er.qualified_name;
            }

            results.push_back(std::move(result));
        }

        spdlog::debug("Entity vector search returned {} results", results.size());

    } catch (const std::exception& e) {
        spdlog::warn("Entity vector search exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryCompressedANN(const std::vector<float>& embedding,
                                       const SearchEngineConfig& config, size_t limit) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    ++stats_.compressedAnnQueries;

    if (!vectorDb_) {
        ++stats_.compressedAnnFallback;
        return results;
    }

    try {
        // Lazy-build or rebuild the compressed ANN index if needed
        if (!compressedAnnIndex_ || !compressedAnnIndexReady_) {
            // Build compressed ANN index from all packed vectors in the database
            yams::vector::CompressedANNIndex::Config ann_cfg;
            ann_cfg.dimension = config.compressedAnnDim;
            ann_cfg.bits_per_channel = config.compressedAnnBits;
            ann_cfg.seed = 42;
            ann_cfg.m = 16;
            ann_cfg.ef_search = config.compressedAnnEfSearch;
            ann_cfg.max_elements = vectorDb_->getVectorCount();

            compressedAnnIndex_ = std::make_unique<yams::vector::CompressedANNIndex>(ann_cfg);
            compressedAnnDocumentHashes_.clear();

            // Collect all vectors using document-vector search with a permissive threshold.
            size_t total = vectorDb_->getVectorCount();
            if (total > 0) {
                vector::VectorSearchParams params;
                params.k = std::min(total, size_t(10000));
                params.similarity_threshold = -1.0f; // Accept all
                params.include_embeddings = true;

                auto vector_records = vectorDb_->searchSimilar(embedding, params);
                if (!vector_records.empty()) {
                    // Collect embeddings first for fitting
                    std::vector<std::vector<float>> corpus;
                    corpus.reserve(vector_records.size());
                    for (const auto& rec : vector_records) {
                        if (!rec.embedding.empty())
                            corpus.push_back(rec.embedding);
                    }

                    // Milestone 11 fix: create ONE fitted quantizer for all encodes
                    yams::vector::TurboQuantConfig tq_cfg;
                    tq_cfg.dimension = config.compressedAnnDim;
                    tq_cfg.bits_per_channel = config.compressedAnnBits;
                    tq_cfg.seed = 42;
                    yams::vector::TurboQuantMSE tq(tq_cfg);
                    if (!corpus.empty()) {
                        tq.fit(corpus, 5); // k-means fit on the full corpus
                    }

                    // Inject fitted scorer into the index
                    compressedAnnIndex_->setScorer(tq);

                    // Now encode all with the fitted quantizer
                    size_t idx = 0;
                    for (const auto& rec : vector_records) {
                        if (rec.embedding.empty())
                            continue;
                        auto packed = tq.packedEncode(rec.embedding);
                        auto addResult = compressedAnnIndex_->add(idx, packed);
                        if (!addResult) {
                            spdlog::warn("[CompressedANN] Failed to add vector {}: {}", idx,
                                         addResult.error().message);
                            continue;
                        }
                        compressedAnnDocumentHashes_.push_back(rec.document_hash);
                        ++idx;
                    }
                }
            }

            auto build_result = compressedAnnIndex_->build();
            if (!build_result) {
                spdlog::warn("[CompressedANN] Build failed: {}", build_result.error().message);
                ++stats_.compressedAnnBuildErrors;
                ++stats_.compressedAnnFallback;
                return results;
            }

            if (compressedAnnIndex_->size() == 0) {
                spdlog::warn("[CompressedANN] Built index is empty (0 vectors); will retry on next "
                             "query");
                ++stats_.compressedAnnFallback;
                return results;
            }

            compressedAnnIndexReady_ = true;
            spdlog::info("[CompressedANN] Built index: {} vectors, {} bytes",
                         compressedAnnIndex_->size(), compressedAnnIndex_->memoryBytes());
        }

        if (!compressedAnnIndexReady_ || compressedAnnIndex_->size() == 0) {
            ++stats_.compressedAnnFallback;
            return results;
        }

        // Search using compressed ANN with telemetry
        size_t candidate_count = 0;
        float decode_escapes = 0.0f;
        auto ann_search_results = compressedAnnIndex_->searchWithStats(
            embedding, limit, &candidate_count, &decode_escapes);

        stats_.compressedAnnCandidateCount += candidate_count;
        stats_.compressedAnnDecodeEscapes += static_cast<uint64_t>(decode_escapes);

        if (ann_search_results.results.empty()) {
            ++stats_.compressedAnnFallback;
            return results;
        }

        ++stats_.compressedAnnSucceeded;

        // Batch-fetch document metadata
        compat::flat_map<std::string, std::string> hashToPath;
        if (metadataRepo_) {
            std::vector<std::string> hashes;
            hashes.reserve(ann_search_results.results.size());
            for (const auto& r : ann_search_results.results) {
                if (r.id < compressedAnnDocumentHashes_.size()) {
                    hashes.push_back(compressedAnnDocumentHashes_[r.id]);
                }
            }
            if (!hashes.empty()) {
                auto docMapResult = metadataRepo_->batchGetDocumentsByHash(hashes);
                if (docMapResult) {
                    compat::reserve_if_needed(hashToPath, docMapResult.value().size());
                    for (const auto& [hash, docInfo] : docMapResult.value()) {
                        hashToPath[hash] = docInfo.filePath;
                    }
                }
            }
        }

        for (size_t rank = 0; rank < ann_search_results.results.size(); ++rank) {
            const auto& r = ann_search_results.results[rank];
            ComponentResult cr;
            cr.source = ComponentResult::Source::CompressedANN;
            if (r.id >= compressedAnnDocumentHashes_.size()) {
                continue;
            }
            cr.documentHash = compressedAnnDocumentHashes_[r.id];
            auto it = hashToPath.find(cr.documentHash);
            cr.filePath = (it != hashToPath.end()) ? std::string(it->second) : cr.documentHash;
            cr.score = r.score;
            cr.rank = rank;
            cr.debugInfo["compressed_ann"] = "true";
            cr.debugInfo["candidate_count"] = std::to_string(candidate_count);
            results.push_back(cr);
        }
    } catch (const std::exception& e) {
        spdlog::warn("Compressed ANN search exception: {}", e.what());
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryTags(const std::vector<std::string>& tags, bool matchAll, size_t limit) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    if (!metadataRepo_ || tags.empty()) {
        return results;
    }

    try {
        auto tagResults = metadataRepo_->findDocumentsByTags(tags, matchAll);
        if (!tagResults) {
            spdlog::debug("Tag search failed: {}", tagResults.error().message);
            return results;
        }

        for (size_t rank = 0; rank < tagResults.value().size() && rank < limit; ++rank) {
            const auto& doc = tagResults.value()[rank];

            ComponentResult result;
            result.documentHash = doc.sha256Hash;
            result.filePath = doc.filePath;

            // Score: matchAll=1.0, matchAny uses position-based decay
            // (avoids N database calls to fetch tags per document)
            result.score = matchAll ? 1.0f : 1.0f / (1.0f + 0.1f * static_cast<float>(rank));

            result.source = ComponentResult::Source::Tag;
            result.rank = rank;
            result.debugInfo["matched_tags"] = std::to_string(tags.size());

            results.push_back(std::move(result));
        }

        spdlog::debug("Tag query returned {} results for {} tags (matchAll={})", results.size(),
                      tags.size(), matchAll);

    } catch (const std::exception& e) {
        spdlog::warn("Tag query exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>> SearchEngine::Impl::queryMetadata(const SearchParams& params,
                                                                       size_t limit) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    if (!metadataRepo_) {
        return results;
    }

    bool hasFilters = params.mimeType.has_value() || params.extension.has_value() ||
                      params.modifiedAfter.has_value() || params.modifiedBefore.has_value();

    if (!hasFilters) {
        return results;
    }

    try {
        yams::metadata::DocumentQueryOptions options;
        options.mimeType = params.mimeType;
        options.extension = params.extension;
        options.modifiedAfter = params.modifiedAfter;
        options.modifiedBefore = params.modifiedBefore;
        options.limit = static_cast<int>(limit);

        auto docResults = metadataRepo_->queryDocuments(options);
        if (!docResults) {
            spdlog::debug("Metadata query failed: {}", docResults.error().message);
            return results;
        }

        for (size_t rank = 0; rank < docResults.value().size(); ++rank) {
            const auto& doc = docResults.value()[rank];

            ComponentResult result;
            result.documentHash = doc.sha256Hash;
            result.filePath = doc.filePath;

            // Score based on how many filters matched (all docs returned match all filters)
            int filterCount = 0;
            if (params.mimeType.has_value())
                filterCount++;
            if (params.extension.has_value())
                filterCount++;
            if (params.modifiedAfter.has_value())
                filterCount++;
            if (params.modifiedBefore.has_value())
                filterCount++;

            result.score = 1.0f; // All returned docs fully match the filters
            result.source = ComponentResult::Source::Metadata;
            result.rank = rank;
            result.debugInfo["filter_count"] = std::to_string(filterCount);
            if (params.mimeType.has_value()) {
                result.debugInfo["mime_type"] = params.mimeType.value();
            }
            if (params.extension.has_value()) {
                result.debugInfo["extension"] = params.extension.value();
            }

            results.push_back(std::move(result));
        }

        spdlog::debug("Metadata query returned {} results", results.size());

    } catch (const std::exception& e) {
        spdlog::warn("Metadata query exception: {}", e.what());
        return results;
    }

    return results;
}

Result<void> SearchEngine::Impl::healthCheck() {
    // Check metadata repository
    if (!metadataRepo_) {
        return Error{ErrorCode::InvalidState, "Metadata repository not initialized"};
    }

    // Check vector database
    if (config_.vectorWeight > 0.0f && !vectorDb_) {
        return Error{ErrorCode::InvalidState, "Vector database not initialized"};
    }

    // Check embedding generator
    if (config_.vectorWeight > 0.0f && !embeddingGen_) {
        return Error{ErrorCode::InvalidState, "Embedding generator not initialized"};
    }

    return {};
}

// ============================================================================
// SearchEngine Public API
// ============================================================================

SearchEngine::SearchEngine(std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo,
                           std::shared_ptr<vector::VectorDatabase> vectorDb,
                           std::shared_ptr<vector::EmbeddingGenerator> embeddingGen,
                           std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore,
                           const SearchEngineConfig& config)
    : pImpl_(std::make_unique<Impl>(std::move(metadataRepo), std::move(vectorDb),
                                    std::move(embeddingGen), std::move(kgStore), config)) {}

SearchEngine::~SearchEngine() = default;

SearchEngine::SearchEngine(SearchEngine&&) noexcept = default;
SearchEngine& SearchEngine::operator=(SearchEngine&&) noexcept = default;

Result<std::vector<SearchResult>> SearchEngine::search(const std::string& query,
                                                       const SearchParams& params) {
    return pImpl_->search(query, params);
}

Result<SearchResponse> SearchEngine::searchWithResponse(const std::string& query,
                                                        const SearchParams& params) {
    return pImpl_->searchWithResponse(query, params);
}

void SearchEngine::setConfig(const SearchEngineConfig& config) {
    pImpl_->setConfig(config);
}

const SearchEngineConfig& SearchEngine::getConfig() const {
    return pImpl_->getConfig();
}

const SearchEngine::Statistics& SearchEngine::getStatistics() const {
    return pImpl_->getStatistics();
}

void SearchEngine::resetStatistics() {
    pImpl_->resetStatistics();
}

Result<void> SearchEngine::healthCheck() {
    return pImpl_->healthCheck();
}

void SearchEngine::setExecutor(std::optional<boost::asio::any_io_executor> executor) {
    pImpl_->setExecutor(std::move(executor));
}

void SearchEngine::setConceptExtractor(EntityExtractionFunc extractor) {
    pImpl_->setConceptExtractor(std::move(extractor));
}

void SearchEngine::setReranker(std::shared_ptr<IReranker> reranker) {
    pImpl_->setReranker(std::move(reranker));
}

void SearchEngine::setSearchTuner(std::shared_ptr<SearchTuner> tuner) {
    pImpl_->setSearchTuner(std::move(tuner));
}

std::shared_ptr<SearchTuner> SearchEngine::getSearchTuner() const {
    return pImpl_->getSearchTuner();
}

void SearchEngine::invalidateCompressedANNIndex() {
    pImpl_->invalidateCompressedANNIndex();
}

// Factory function
std::unique_ptr<SearchEngine>
createSearchEngine(std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo,
                   std::shared_ptr<vector::VectorDatabase> vectorDb,
                   std::shared_ptr<vector::EmbeddingGenerator> embeddingGen,
                   std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore,
                   const SearchEngineConfig& config) {
    return std::make_unique<SearchEngine>(std::move(metadataRepo), std::move(vectorDb),
                                          std::move(embeddingGen), std::move(kgStore), config);
}

} // namespace yams::search
