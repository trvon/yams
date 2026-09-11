// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/daemon/components/semantic_neighbor_graph_builder.h>

#include <yams/vector/vector_database.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <unordered_set>

namespace yams::daemon {

SemanticNeighborGraphBuilder::SemanticNeighborGraphBuilder(SemanticNeighborGraphConfig config)
    : cfg_(std::move(config)) {}

std::optional<std::size_t>
SemanticNeighborGraphBuilder::checkedEdgeCapacity(std::size_t sources, std::size_t topK) noexcept {
    if (!SemanticNeighborGraphConfig::validTopK(topK))
        return std::nullopt;
    const auto edgesPerSource = topK * 2;
    const auto maximum = std::vector<metadata::KGEdge>{}.max_size();
    if (sources > maximum / edgesPerSource)
        return std::nullopt;
    return sources * edgesPerSource;
}

void SemanticNeighborGraphBuilder::setEdgeSink(EdgeSink sink) {
    edgeSink_ = std::move(sink);
}

void SemanticNeighborGraphBuilder::setPhaseTimer(PhaseTimer timer) {
    phaseTimer_ = std::move(timer);
}

void SemanticNeighborGraphBuilder::clearCaches() {
    {
        std::lock_guard<std::mutex> lock(semanticCorpusMutex_);
        semanticCorpusCache_.clear();
    }
    {
        std::lock_guard<std::mutex> lock(semanticNodeIdCacheMutex_);
        semanticNodeIdCache_.clear();
    }
}

bool SemanticNeighborGraphBuilder::emitEdges(std::vector<metadata::KGEdge> edges,
                                             std::string_view source) {
    return edgeSink_ && edgeSink_(std::move(edges), source);
}

void SemanticNeighborGraphBuilder::recordPhaseTiming(std::string_view phase,
                                                     std::chrono::steady_clock::time_point start) {
    if (phaseTimer_) {
        phaseTimer_(phase, start);
    }
}

void SemanticNeighborGraphBuilder::update(
    const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
    const std::shared_ptr<yams::vector::VectorDatabase>& vdb, const std::string& modelName,
    const std::vector<std::pair<std::string, std::string>>& sourceDocuments, bool sourceAllCorpus) {
    if (!SemanticNeighborGraphConfig::validTopK(cfg_.topK) ||
        sourceDocuments.size() > std::numeric_limits<std::size_t>::max() / 8) {
        semanticUpdateErrors_.fetch_add(1, std::memory_order_relaxed);
        spdlog::warn("Semantic graph update rejected: invalid top-K or source capacity");
        return;
    }
    if (!kgStore || !vdb || (!sourceAllCorpus && sourceDocuments.empty())) {
        return;
    }

    const std::size_t semanticTopK = cfg_.topK;
    const std::optional<float> explicitSemanticThreshold = cfg_.similarityThreshold;

    const auto inverseNorm = [](const std::vector<float>& v) {
        double norm = 0.0;
        for (float x : v) {
            const double xd = x;
            norm += xd * xd;
        }
        if (norm <= 0.0) {
            return 0.0f;
        }
        return static_cast<float>(1.0 / std::sqrt(norm));
    };

    const auto cosineSimilarity = [](const std::vector<float>& a, float invNormA,
                                     const std::vector<float>& b, float invNormB) {
        if (a.empty() || b.empty() || a.size() != b.size()) {
            return 0.0f;
        }
        if (invNormA <= 0.0f || invNormB <= 0.0f) {
            return 0.0f;
        }

        double dot = 0.0;
        for (std::size_t i = 0; i < a.size(); ++i) {
            dot += static_cast<double>(a[i]) * static_cast<double>(b[i]);
        }
        return static_cast<float>(dot * invNormA * invNormB);
    };

    struct CorpusVector {
        std::string hash;
        std::string filePath;
        std::vector<float> embedding;
        float invNorm{0.0f};
    };

    std::unordered_map<std::string, std::string> requestedSourcePaths;
    if (!sourceAllCorpus) {
        requestedSourcePaths.reserve(sourceDocuments.size());
        for (const auto& [hash, filePath] : sourceDocuments) {
            if (!hash.empty()) {
                requestedSourcePaths.emplace(hash, filePath);
            }
        }
        if (requestedSourcePaths.empty()) {
            return;
        }
    }

    if (!sourceAllCorpus) {
        struct SourceDoc {
            std::string hash;
            std::string filePath;
            std::vector<float> embedding;
            float invNorm{0.0f};
        };
        struct StreamNeighborScore {
            std::string hash;
            float similarity{0.0f};
        };
        auto isBetterStreamNeighbor = [](const StreamNeighborScore& left,
                                         const StreamNeighborScore& right) {
            if (left.similarity != right.similarity) {
                return left.similarity > right.similarity;
            }
            return left.hash < right.hash;
        };
        auto isWorseStreamNeighbor = [&](const StreamNeighborScore& left,
                                         const StreamNeighborScore& right) {
            return isBetterStreamNeighbor(right, left);
        };

        std::vector<SourceDoc> sources;
        sources.reserve(requestedSourcePaths.size());
        const auto tSourceLoad = std::chrono::steady_clock::now();
        for (const auto& [hash, requestedPath] : requestedSourcePaths) {
            auto records = vdb->getVectorsByDocument(hash);
            auto recordIt = std::find_if(
                records.begin(), records.end(), [](const yams::vector::VectorRecord& r) {
                    return r.level == yams::vector::EmbeddingLevel::DOCUMENT &&
                           !r.document_hash.empty() && !r.embedding.empty();
                });
            if (recordIt == records.end()) {
                recordIt = std::find_if(records.begin(), records.end(),
                                        [](const yams::vector::VectorRecord& r) {
                                            return !r.document_hash.empty() && !r.embedding.empty();
                                        });
            }
            if (recordIt == records.end()) {
                continue;
            }
            std::string filePath = requestedPath;
            if (filePath.empty()) {
                if (auto it = recordIt->metadata.find("path"); it != recordIt->metadata.end()) {
                    filePath = it->second;
                }
            }
            auto embedding = std::move(recordIt->embedding);
            const float inv = inverseNorm(embedding);
            if (inv <= 0.0f) {
                continue;
            }
            sources.push_back(
                SourceDoc{recordIt->document_hash, std::move(filePath), std::move(embedding), inv});
        }
        recordPhaseTiming("semantic_source_load", tSourceLoad);
        if (sources.empty()) {
            spdlog::debug("EmbeddingService: semantic graph skipped; no source document "
                          "embeddings found (requested={})",
                          requestedSourcePaths.size());
            return;
        }

        const auto edgeCapacity = checkedEdgeCapacity(sources.size(), semanticTopK);
        if (!edgeCapacity) {
            semanticUpdateErrors_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        std::vector<CorpusVector> cachedCorpus;
        {
            const auto tCorpusSnapshot = std::chrono::steady_clock::now();
            std::lock_guard<std::mutex> lock(semanticCorpusMutex_);
            for (const auto& source : sources) {
                semanticCorpusCache_[source.hash] = SemanticCorpusEntry{
                    source.hash, source.filePath, source.embedding, source.invNorm};
            }
            cachedCorpus.reserve(semanticCorpusCache_.size());
            for (const auto& [_, entry] : semanticCorpusCache_) {
                if (entry.hash.empty() || entry.embedding.empty() || entry.invNorm <= 0.0f) {
                    continue;
                }
                cachedCorpus.push_back(
                    CorpusVector{entry.hash, entry.filePath, entry.embedding, entry.invNorm});
            }
            recordPhaseTiming("semantic_corpus_snapshot", tCorpusSnapshot);
        }

        std::vector<std::vector<StreamNeighborScore>> topBySource(sources.size());
        for (auto& top : topBySource) {
            top.reserve(semanticTopK);
        }

        std::size_t candidateDocs = 0;
        std::size_t similarityPairCount = 0;
        std::size_t candidateNeighborCount = 0;

        const bool useHnswOverride = cfg_.useHnsw;
        const auto candidateCorpusSize = std::max<std::size_t>(cachedCorpus.size(), sources.size());
        constexpr std::size_t kExactPairBudget = 250'000;
        const bool exactMicroBatch =
            candidateCorpusSize == 0 || sources.size() <= kExactPairBudget / candidateCorpusSize;
        const bool useHnsw = useHnswOverride && !exactMicroBatch;

        const auto tPairScoring = std::chrono::steady_clock::now();
        if (useHnsw) {
            const std::size_t hnswK = std::max<std::size_t>(8, semanticTopK * 4);
            const float searchThreshold = explicitSemanticThreshold.value_or(0.0f);
            std::vector<std::vector<float>> queries;
            queries.reserve(sources.size());
            for (const auto& source : sources) {
                queries.push_back(source.embedding);
            }
            yams::vector::VectorSearchParams params;
            params.k = hnswK;
            params.similarity_threshold = searchThreshold;
            params.include_embeddings = false;
            auto hitBatches = vdb->searchSimilarBatch(queries, params);
            if (hitBatches.size() != sources.size()) {
                hitBatches.clear();
                hitBatches.reserve(sources.size());
                for (const auto& source : sources) {
                    hitBatches.push_back(vdb->searchSimilar(source.embedding, params));
                }
            }
            for (std::size_t i = 0; i < sources.size(); ++i) {
                const auto& source = sources[i];
                const auto& hits = hitBatches[i];
                similarityPairCount += hits.size();

                // Dedupe by document_hash; preserve the best per-doc score.
                std::unordered_map<std::string, float> bestPerDoc;
                bestPerDoc.reserve(hits.size());
                for (const auto& hit : hits) {
                    if (hit.document_hash.empty() || hit.document_hash == source.hash) {
                        continue;
                    }
                    auto [it, inserted] =
                        bestPerDoc.try_emplace(hit.document_hash, hit.relevance_score);
                    if (!inserted && hit.relevance_score > it->second) {
                        it->second = hit.relevance_score;
                    }
                }
                candidateNeighborCount += bestPerDoc.size();

                auto& topNeighbors = topBySource[i];
                topNeighbors.reserve(bestPerDoc.size());
                for (auto& [hash, sim] : bestPerDoc) {
                    topNeighbors.push_back(StreamNeighborScore{hash, sim});
                }
                std::sort(topNeighbors.begin(), topNeighbors.end(), isBetterStreamNeighbor);
                if (topNeighbors.size() > semanticTopK) {
                    topNeighbors.resize(semanticTopK);
                }
            }
            candidateDocs = candidateNeighborCount;
        } else {
            for (const auto& candidateDoc : cachedCorpus) {
                if (candidateDoc.hash.empty() || candidateDoc.embedding.empty()) {
                    continue;
                }
                ++candidateDocs;
                for (std::size_t i = 0; i < sources.size(); ++i) {
                    const auto& source = sources[i];
                    if (candidateDoc.hash == source.hash) {
                        continue;
                    }
                    ++similarityPairCount;
                    const float similarity =
                        cosineSimilarity(source.embedding, source.invNorm, candidateDoc.embedding,
                                         candidateDoc.invNorm);
                    if (explicitSemanticThreshold.has_value()) {
                        if (similarity < *explicitSemanticThreshold) {
                            continue;
                        }
                    } else if (similarity <= 0.0f) {
                        continue;
                    }
                    ++candidateNeighborCount;

                    StreamNeighborScore candidate{candidateDoc.hash, similarity};
                    auto& topNeighbors = topBySource[i];
                    if (topNeighbors.size() < semanticTopK) {
                        topNeighbors.push_back(std::move(candidate));
                        continue;
                    }
                    auto worst = std::min_element(
                        topNeighbors.begin(), topNeighbors.end(),
                        [&](const auto& a, const auto& b) { return isWorseStreamNeighbor(a, b); });
                    if (worst != topNeighbors.end() && isBetterStreamNeighbor(candidate, *worst)) {
                        *worst = std::move(candidate);
                    }
                }
            }
        }
        recordPhaseTiming("semantic_pair_scoring", tPairScoring);

        if (candidateDocs < 2) {
            return;
        }

        std::vector<std::string> nodeKeys;
        nodeKeys.reserve(sources.size() + candidateNeighborCount);
        std::unordered_set<std::string> seenNodeKeys;
        seenNodeKeys.reserve(sources.size() + candidateNeighborCount);
        auto rememberNodeKey = [&](const std::string& hash) {
            if (hash.empty()) {
                return;
            }
            std::string key = "doc:" + hash;
            if (seenNodeKeys.insert(key).second) {
                nodeKeys.push_back(std::move(key));
            }
        };
        for (const auto& source : sources) {
            rememberNodeKey(source.hash);
        }
        for (const auto& topNeighbors : topBySource) {
            for (const auto& neighbor : topNeighbors) {
                rememberNodeKey(neighbor.hash);
            }
        }

        std::unordered_map<std::string, std::optional<std::int64_t>> nodeIdCache;
        nodeIdCache.reserve(nodeKeys.size());
        std::vector<std::string> missingNodeKeys;
        missingNodeKeys.reserve(nodeKeys.size());
        const auto tNodeLookup = std::chrono::steady_clock::now();
        {
            std::lock_guard<std::mutex> lock(semanticNodeIdCacheMutex_);
            for (const auto& key : nodeKeys) {
                auto cached = semanticNodeIdCache_.find(key);
                if (cached != semanticNodeIdCache_.end()) {
                    nodeIdCache.emplace(key, cached->second);
                } else {
                    missingNodeKeys.push_back(key);
                }
            }
        }
        if (!missingNodeKeys.empty()) {
            auto nodesResult = kgStore->getNodesByKeys(missingNodeKeys);
            if (nodesResult) {
                std::unordered_set<std::string> foundKeys;
                foundKeys.reserve(nodesResult.value().size());
                {
                    std::lock_guard<std::mutex> lock(semanticNodeIdCacheMutex_);
                    for (const auto& node : nodesResult.value()) {
                        nodeIdCache.emplace(node.nodeKey, node.id);
                        semanticNodeIdCache_[node.nodeKey] = node.id;
                        foundKeys.insert(node.nodeKey);
                    }
                    for (const auto& key : missingNodeKeys) {
                        if (!foundKeys.contains(key)) {
                            nodeIdCache.emplace(key, std::nullopt);
                        }
                    }
                }
            } else {
                spdlog::warn("EmbeddingService: batch node lookup for semantic graph failed: {}",
                             nodesResult.error().message);
            }
        }
        recordPhaseTiming("semantic_node_lookup", tNodeLookup);
        auto resolveDocNodeId = [&](const std::string& hash) -> std::optional<std::int64_t> {
            const std::string key = "doc:" + hash;
            auto cached = nodeIdCache.find(key);
            if (cached != nodeIdCache.end()) {
                return cached->second;
            }
            auto node = kgStore->getNodeByKey(key);
            if (node && node.value().has_value()) {
                nodeIdCache.emplace(key, node.value()->id);
                {
                    std::lock_guard<std::mutex> lock(semanticNodeIdCacheMutex_);
                    semanticNodeIdCache_[key] = node.value()->id;
                }
                return node.value()->id;
            }
            nodeIdCache.emplace(key, std::nullopt);
            return std::nullopt;
        };

        std::size_t missingSrcNodeCount = 0;
        std::size_t missingDstNodeCount = 0;
        float minEffectiveThreshold = 1.0f;
        float maxEffectiveThreshold = 0.0f;
        std::vector<metadata::KGEdge> semanticEdges;
        semanticEdges.reserve(*edgeCapacity);
        const auto nowSecs = std::chrono::duration_cast<std::chrono::seconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();

        const auto tEdgeBuild = std::chrono::steady_clock::now();
        for (std::size_t sourceIdx = 0; sourceIdx < sources.size(); ++sourceIdx) {
            const auto& source = sources[sourceIdx];
            semanticDocsProcessed_.fetch_add(1, std::memory_order_relaxed);
            auto srcNodeId = resolveDocNodeId(source.hash);
            if (!srcNodeId.has_value()) {
                ++missingSrcNodeCount;
                continue;
            }
            auto& topNeighbors = topBySource[sourceIdx];
            if (topNeighbors.empty()) {
                continue;
            }
            std::sort(topNeighbors.begin(), topNeighbors.end(), isBetterStreamNeighbor);
            const float effectiveThreshold =
                explicitSemanticThreshold.value_or(topNeighbors.back().similarity);
            minEffectiveThreshold = std::min(minEffectiveThreshold, effectiveThreshold);
            maxEffectiveThreshold = std::max(maxEffectiveThreshold, effectiveThreshold);

            size_t kept = 0;
            for (const auto& neighbor : topNeighbors) {
                if (kept >= semanticTopK || neighbor.similarity < effectiveThreshold) {
                    break;
                }
                auto dstNodeId = resolveDocNodeId(neighbor.hash);
                if (!dstNodeId.has_value()) {
                    ++missingDstNodeCount;
                    continue;
                }

                metadata::KGEdge edge;
                edge.srcNodeId = *srcNodeId;
                edge.dstNodeId = *dstNodeId;
                edge.relation = "semantic_neighbor";
                edge.weight = std::clamp(neighbor.similarity, effectiveThreshold, 1.0f);
                edge.createdTime = nowSecs;
                nlohmann::json props;
                props["source"] = "embedding_service";
                props["source_file"] = source.filePath;
                props["document_hash"] = source.hash;
                props["neighbor_hash"] = neighbor.hash;
                props["model"] = modelName;
                props["similarity"] = neighbor.similarity;
                props["rank"] = kept + 1;
                props["layer"] = "semantic";
                edge.properties = props.dump();
                semanticEdges.push_back(std::move(edge));

                metadata::KGEdge reverseEdge;
                reverseEdge.srcNodeId = *dstNodeId;
                reverseEdge.dstNodeId = *srcNodeId;
                reverseEdge.relation = "semantic_neighbor";
                reverseEdge.weight = std::clamp(neighbor.similarity, effectiveThreshold, 1.0f);
                reverseEdge.createdTime = nowSecs;
                nlohmann::json reverseProps;
                reverseProps["source"] = "embedding_service";
                reverseProps["document_hash"] = neighbor.hash;
                reverseProps["neighbor_hash"] = source.hash;
                reverseProps["model"] = modelName;
                reverseProps["similarity"] = neighbor.similarity;
                reverseProps["rank"] = kept + 1;
                reverseProps["layer"] = "semantic";
                reverseEdge.properties = reverseProps.dump();
                semanticEdges.push_back(std::move(reverseEdge));
                ++kept;
            }
        }
        recordPhaseTiming("semantic_edge_build", tEdgeBuild);

        if (semanticEdges.empty()) {
            spdlog::debug(
                "EmbeddingService: streaming semantic neighbor graph produced no edges "
                "(processed_docs={} candidate_docs={} similarity_pairs={} candidate_neighbors={} "
                "missing_src_nodes={} missing_dst_nodes={} threshold_mode={} threshold_min={} "
                "threshold_max={} topk={})",
                sources.size(), candidateDocs, similarityPairCount, candidateNeighborCount,
                missingSrcNodeCount, missingDstNodeCount,
                explicitSemanticThreshold.has_value() ? "explicit" : "adaptive",
                minEffectiveThreshold, maxEffectiveThreshold, semanticTopK);
            return;
        }

        const auto tEdgeUpsert = std::chrono::steady_clock::now();
        const auto enqueuedCount = semanticEdges.size();
        if (emitEdges(std::move(semanticEdges), "EmbeddingService::semanticNeighborStream")) {
            recordPhaseTiming("semantic_edge_upsert", tEdgeUpsert);
            semanticEdgesCreated_.fetch_add(enqueuedCount, std::memory_order_relaxed);
            spdlog::debug(
                "EmbeddingService: streaming semantic neighbor graph added {} edges "
                "(processed_docs={} candidate_docs={} similarity_pairs={} candidate_neighbors={} "
                "missing_src_nodes={} missing_dst_nodes={} threshold_mode={} threshold_min={} "
                "threshold_max={} topk={})",
                enqueuedCount, sources.size(), candidateDocs, similarityPairCount,
                candidateNeighborCount, missingSrcNodeCount, missingDstNodeCount,
                explicitSemanticThreshold.has_value() ? "explicit" : "adaptive",
                minEffectiveThreshold, maxEffectiveThreshold, semanticTopK);
            return;
        }
        recordPhaseTiming("semantic_edge_upsert", tEdgeUpsert);
        semanticUpdateErrors_.fetch_add(1, std::memory_order_relaxed);
        spdlog::warn("EmbeddingService: WriteCoordinator unavailable; dropping {} streaming "
                     "semantic_neighbor edges",
                     enqueuedCount);
    }

    std::vector<CorpusVector> corpus;
    corpus.reserve(sourceAllCorpus ? 256u
                                   : std::max<std::size_t>(sourceDocuments.size() * 4u, 16u));
    std::unordered_set<std::string> corpusHashes;
    corpusHashes.reserve(sourceAllCorpus ? 256u
                                         : std::max<std::size_t>(sourceDocuments.size() * 8u, 32u));

    const auto tCorpusStream = std::chrono::steady_clock::now();
    auto streamResult = vdb->forEachDocumentLevelVector([&](yams::vector::VectorRecord&& record) {
        if (record.document_hash.empty() || record.embedding.empty()) {
            return true;
        }
        if (!corpusHashes.insert(record.document_hash).second) {
            return true;
        }
        std::string filePath;
        if (auto it = record.metadata.find("path"); it != record.metadata.end()) {
            filePath = it->second;
        }
        const float inv = inverseNorm(record.embedding);
        if (inv <= 0.0f) {
            return true;
        }
        corpus.push_back(CorpusVector{std::move(record.document_hash), std::move(filePath),
                                      std::move(record.embedding), inv});
        return true;
    });
    recordPhaseTiming("semantic_source_load", tCorpusStream);
    if (!streamResult) {
        spdlog::warn(
            "EmbeddingService: failed to stream document-level vectors for semantic graph: {}",
            streamResult.error().message);
    }

    if (corpus.size() < 2) {
        return;
    }

    struct SourceDocRef {
        const std::string* hash;
        const std::string* filePath;
        const std::vector<float>* embedding;
        float invNorm{0.0f};
    };
    std::vector<SourceDocRef> sources;
    sources.reserve(sourceAllCorpus ? corpus.size() : requestedSourcePaths.size());
    for (const auto& item : corpus) {
        if (sourceAllCorpus) {
            if (item.invNorm > 0.0f) {
                sources.push_back(
                    SourceDocRef{&item.hash, &item.filePath, &item.embedding, item.invNorm});
            }
            continue;
        }
        auto requested = requestedSourcePaths.find(item.hash);
        if (requested == requestedSourcePaths.end()) {
            continue;
        }
        const std::string* filePath =
            requested->second.empty() ? &item.filePath : &requested->second;
        if (item.invNorm > 0.0f) {
            sources.push_back(SourceDocRef{&item.hash, filePath, &item.embedding, item.invNorm});
        }
    }
    if (sources.empty()) {
        spdlog::debug(
            "EmbeddingService: semantic graph skipped; no source document embeddings found "
            "in streamed corpus (requested={} corpus={})",
            sourceAllCorpus ? corpus.size() : sourceDocuments.size(), corpus.size());
        return;
    }

    const auto edgeCapacity = checkedEdgeCapacity(sources.size(), semanticTopK);
    if (!edgeCapacity || corpus.size() > std::numeric_limits<std::size_t>::max() - sources.size()) {
        semanticUpdateErrors_.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    std::unordered_map<std::string, std::optional<std::int64_t>> nodeIdCache;
    nodeIdCache.reserve(corpus.size() + sources.size());
    auto resolveDocNodeId = [&](const std::string& hash) -> std::optional<std::int64_t> {
        const std::string key = "doc:" + hash;
        auto cached = nodeIdCache.find(key);
        if (cached != nodeIdCache.end()) {
            return cached->second;
        }
        auto node = kgStore->getNodeByKey(key);
        if (node && node.value().has_value()) {
            nodeIdCache.emplace(key, node.value()->id);
            return node.value()->id;
        }
        nodeIdCache.emplace(std::move(key), std::nullopt);
        return std::nullopt;
    };

    struct NeighborScore {
        const CorpusVector* doc{nullptr};
        float similarity{0.0f};
    };
    auto isBetterNeighbor = [](const NeighborScore& left, const NeighborScore& right) {
        if (left.similarity != right.similarity) {
            return left.similarity > right.similarity;
        }
        return left.doc && right.doc ? left.doc->hash < right.doc->hash : left.doc != nullptr;
    };
    auto isWorseNeighbor = [&](const NeighborScore& left, const NeighborScore& right) {
        return isBetterNeighbor(right, left);
    };

    std::size_t missingSrcNodeCount = 0;
    std::size_t missingDstNodeCount = 0;
    std::size_t similarityPairCount = 0;
    std::size_t candidateNeighborCount = 0;
    float minEffectiveThreshold = 1.0f;
    float maxEffectiveThreshold = 0.0f;

    std::vector<metadata::KGEdge> semanticEdges;
    semanticEdges.reserve(*edgeCapacity);
    const auto nowSecs = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();

    const auto tPairAndEdge = std::chrono::steady_clock::now();
    for (const auto& source : sources) {
        semanticDocsProcessed_.fetch_add(1, std::memory_order_relaxed);

        auto srcNodeId = resolveDocNodeId(*source.hash);
        if (!srcNodeId.has_value()) {
            ++missingSrcNodeCount;
            continue;
        }

        std::vector<NeighborScore> topNeighbors;
        topNeighbors.reserve(semanticTopK);
        for (const auto& neighbor : corpus) {
            if (neighbor.hash.empty() || neighbor.hash == *source.hash) {
                continue;
            }
            ++similarityPairCount;
            const float similarity = cosineSimilarity(*source.embedding, source.invNorm,
                                                      neighbor.embedding, neighbor.invNorm);
            if (explicitSemanticThreshold.has_value()) {
                if (similarity < *explicitSemanticThreshold) {
                    continue;
                }
            } else if (similarity <= 0.0f) {
                continue;
            }
            ++candidateNeighborCount;

            NeighborScore candidate{&neighbor, similarity};
            if (topNeighbors.size() < semanticTopK) {
                topNeighbors.push_back(candidate);
                continue;
            }
            auto worst = std::min_element(
                topNeighbors.begin(), topNeighbors.end(),
                [&](const auto& a, const auto& b) { return isWorseNeighbor(a, b); });
            if (worst != topNeighbors.end() && isBetterNeighbor(candidate, *worst)) {
                *worst = candidate;
            }
        }

        if (topNeighbors.empty()) {
            continue;
        }

        std::sort(topNeighbors.begin(), topNeighbors.end(), isBetterNeighbor);
        const float effectiveThreshold =
            explicitSemanticThreshold.value_or(topNeighbors.back().similarity);
        minEffectiveThreshold = std::min(minEffectiveThreshold, effectiveThreshold);
        maxEffectiveThreshold = std::max(maxEffectiveThreshold, effectiveThreshold);

        size_t kept = 0;
        for (const auto& [neighbor, similarity] : topNeighbors) {
            if (kept >= semanticTopK || neighbor == nullptr) {
                break;
            }
            if (similarity < effectiveThreshold) {
                break;
            }

            auto dstNodeId = resolveDocNodeId(neighbor->hash);
            if (!dstNodeId.has_value()) {
                ++missingDstNodeCount;
                continue;
            }

            metadata::KGEdge edge;
            edge.srcNodeId = *srcNodeId;
            edge.dstNodeId = *dstNodeId;
            edge.relation = "semantic_neighbor";
            edge.weight = std::clamp(similarity, effectiveThreshold, 1.0f);
            edge.createdTime = nowSecs;

            nlohmann::json props;
            props["source"] = "embedding_service";
            props["source_file"] = *source.filePath;
            props["document_hash"] = *source.hash;
            props["neighbor_hash"] = neighbor->hash;
            props["model"] = modelName;
            props["similarity"] = similarity;
            props["rank"] = kept + 1;
            props["layer"] = "semantic";
            edge.properties = props.dump();
            semanticEdges.push_back(std::move(edge));

            metadata::KGEdge reverseEdge;
            reverseEdge.srcNodeId = *dstNodeId;
            reverseEdge.dstNodeId = *srcNodeId;
            reverseEdge.relation = "semantic_neighbor";
            reverseEdge.weight = std::clamp(similarity, effectiveThreshold, 1.0f);
            reverseEdge.createdTime = nowSecs;

            nlohmann::json reverseProps;
            reverseProps["source"] = "embedding_service";
            reverseProps["document_hash"] = neighbor->hash;
            reverseProps["neighbor_hash"] = *source.hash;
            reverseProps["model"] = modelName;
            reverseProps["similarity"] = similarity;
            reverseProps["rank"] = kept + 1;
            reverseProps["layer"] = "semantic";
            reverseEdge.properties = reverseProps.dump();
            semanticEdges.push_back(std::move(reverseEdge));
            ++kept;
        }
    }
    recordPhaseTiming("semantic_pair_scoring", tPairAndEdge);
    recordPhaseTiming("semantic_edge_build", tPairAndEdge);

    if (semanticEdges.empty()) {
        spdlog::debug(
            "EmbeddingService: semantic neighbor graph produced no edges (processed_docs={} "
            "candidate_docs={} similarity_pairs={} candidate_neighbors={} missing_src_nodes={} "
            "missing_dst_nodes={} threshold_mode={} threshold_min={} threshold_max={} topk={})",
            sources.size(), corpus.size(), similarityPairCount, candidateNeighborCount,
            missingSrcNodeCount, missingDstNodeCount,
            explicitSemanticThreshold.has_value() ? "explicit" : "adaptive", minEffectiveThreshold,
            maxEffectiveThreshold, semanticTopK);
        return;
    }

    const auto tEdgeUpsert = std::chrono::steady_clock::now();
    const auto enqueuedCount = semanticEdges.size();
    if (emitEdges(std::move(semanticEdges), "EmbeddingService::semanticNeighborCorpus")) {
        recordPhaseTiming("semantic_edge_upsert", tEdgeUpsert);
        semanticEdgesCreated_.fetch_add(enqueuedCount, std::memory_order_relaxed);
        spdlog::debug(
            "EmbeddingService: semantic neighbor graph added {} edges (processed_docs={} "
            "candidate_docs={} similarity_pairs={} candidate_neighbors={} missing_src_nodes={} "
            "missing_dst_nodes={} threshold_mode={} threshold_min={} threshold_max={} topk={})",
            enqueuedCount, sources.size(), corpus.size(), similarityPairCount,
            candidateNeighborCount, missingSrcNodeCount, missingDstNodeCount,
            explicitSemanticThreshold.has_value() ? "explicit" : "adaptive", minEffectiveThreshold,
            maxEffectiveThreshold, semanticTopK);
        return;
    }
    recordPhaseTiming("semantic_edge_upsert", tEdgeUpsert);
    semanticUpdateErrors_.fetch_add(1, std::memory_order_relaxed);
    spdlog::warn("EmbeddingService: WriteCoordinator unavailable; dropping {} corpus "
                 "semantic_neighbor edges",
                 enqueuedCount);
}

} // namespace yams::daemon
