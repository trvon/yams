#include <yams/topology/topology_input_extractor.h>
#include <yams/vector/vector_database.h>

#include <simeon/minhash.hpp>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <numeric>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace yams::topology {

namespace {

constexpr std::string_view kDocNodePrefix = "doc:";

// Per Phase V design (and src/search/query_text_utils.cpp's filter list), drop
// entity types that are uninformative for topical clustering. Match against the
// canonicalized type string stored in kg_nodes.type.
bool isFilteredEntityType(const std::string& canonicalLabel) {
    static const std::unordered_set<std::string> filtered{"date", "time", "number", "percentage",
                                                          "ordinal"};
    return filtered.contains(canonicalLabel);
}

void l2NormalizeInPlace(std::vector<float>& v) {
    double sumSq = 0.0;
    for (float x : v) {
        sumSq += static_cast<double>(x) * x;
    }
    if (sumSq <= 0.0) {
        return;
    }
    const auto norm = static_cast<float>(std::sqrt(sumSq));
    for (float& x : v) {
        x /= norm;
    }
}

// V-B: matryoshka coarse view via per-dim variance-weighted truncation.
// Computed once over the corpus embeddings being clustered.
std::vector<float> computeVarianceWeights(const std::vector<std::vector<float>>& embeddings,
                                          std::size_t targetDim) {
    if (embeddings.empty() || targetDim == 0) {
        return {};
    }
    const std::size_t fullDim = embeddings.front().size();
    if (targetDim >= fullDim) {
        return {};
    }
    std::vector<double> mean(fullDim, 0.0);
    std::size_t n = 0;
    for (const auto& e : embeddings) {
        if (e.size() != fullDim) {
            continue;
        }
        for (std::size_t i = 0; i < fullDim; ++i) {
            mean[i] += e[i];
        }
        ++n;
    }
    if (n == 0) {
        return {};
    }
    for (auto& m : mean) {
        m /= static_cast<double>(n);
    }
    std::vector<double> var(fullDim, 0.0);
    for (const auto& e : embeddings) {
        if (e.size() != fullDim) {
            continue;
        }
        for (std::size_t i = 0; i < fullDim; ++i) {
            const double d = static_cast<double>(e[i]) - mean[i];
            var[i] += d * d;
        }
    }
    for (auto& vv : var) {
        vv /= static_cast<double>(n);
    }
    // Pick top-targetDim dimensions by variance, store (dim_index, sqrt(var)) so
    // applyMatryoshkaCoarse can rebuild the projection deterministically.
    // We return a dense fullDim-length weight vector with zeros outside top-K.
    std::vector<std::size_t> idx(fullDim);
    std::iota(idx.begin(), idx.end(), std::size_t{0});
    std::partial_sort(idx.begin(), idx.begin() + static_cast<std::ptrdiff_t>(targetDim), idx.end(),
                      [&](std::size_t a, std::size_t b) { return var[a] > var[b]; });
    std::vector<float> weights(fullDim, 0.0F);
    for (std::size_t k = 0; k < targetDim; ++k) {
        weights[idx[k]] = static_cast<float>(std::sqrt(var[idx[k]]));
    }
    return weights;
}

std::vector<float> applyMatryoshkaCoarse(const std::vector<float>& dense,
                                         const std::vector<float>& weights, std::size_t targetDim) {
    if (weights.size() != dense.size() || targetDim == 0 || targetDim >= dense.size()) {
        return dense;
    }
    // Collect indices of non-zero weights (these are the top-targetDim variance dims).
    std::vector<std::size_t> kept;
    kept.reserve(targetDim);
    for (std::size_t i = 0; i < weights.size(); ++i) {
        if (weights[i] > 0.0F) {
            kept.push_back(i);
        }
    }
    std::vector<float> coarse;
    coarse.reserve(kept.size());
    for (std::size_t i : kept) {
        coarse.push_back(dense[i] * weights[i]);
    }
    l2NormalizeInPlace(coarse);
    return coarse;
}

// V-C: bucket-count sketch over a 256-u32 MinHash signature → fixed-dim float vec.
// Each bucket sums (sig[i] mod some-prime), giving a dim-reduced MinHash-derived feature.
// Empty input → empty output (signals "no MinHash available").
std::vector<float> bucketCountSketch(const std::vector<std::uint32_t>& sig, std::size_t sketchDim) {
    if (sig.empty() || sketchDim == 0) {
        return {};
    }
    std::vector<float> sketch(sketchDim, 0.0F);
    for (std::size_t i = 0; i < sig.size(); ++i) {
        const std::size_t bucket = sig[i] % sketchDim;
        sketch[bucket] += 1.0F;
    }
    l2NormalizeInPlace(sketch);
    return sketch;
}

std::string nodeKeyForDocumentHash(std::string_view documentHash) {
    return std::string{kDocNodePrefix} + std::string{documentHash};
}

// Phase V-A: build corpus-wide histogram of canonical entity types, keep top-K.
// One bulk pass: collect all (docId, nodeId) pairs, then bulk-fetch kg_nodes once,
// then count by canonicalized node.type.
struct EntityTypeIndex {
    std::vector<std::string> topKTypes;                     // index → canonical type string
    std::unordered_map<std::string, std::size_t> typeToIdx; // canonical type → index in topKTypes
    std::unordered_map<std::int64_t, std::string> nodeIdToType; // cache for per-doc lookup
};

EntityTypeIndex buildEntityTypeIndex(metadata::KnowledgeGraphStore& kgStore,
                                     const std::vector<metadata::DocumentInfo>& documents,
                                     std::size_t K, float minConfidence) {
    EntityTypeIndex out;
    if (K == 0 || documents.empty()) {
        return out;
    }
    // Pass 1: collect all unique nodeIds referenced by docs' entities (with conf ≥ τ).
    std::unordered_set<std::int64_t> uniqueNodeIds;
    std::unordered_map<std::int64_t, std::vector<std::int64_t>> docToNodes;
    docToNodes.reserve(documents.size());
    for (const auto& doc : documents) {
        auto entitiesResult = kgStore.getDocEntitiesForDocument(doc.id);
        if (!entitiesResult) {
            continue;
        }
        std::vector<std::int64_t> nodeIds;
        nodeIds.reserve(entitiesResult.value().size());
        for (const auto& ent : entitiesResult.value()) {
            if (!ent.nodeId.has_value()) {
                continue;
            }
            if (ent.confidence.has_value() && ent.confidence.value() < minConfidence) {
                continue;
            }
            nodeIds.push_back(*ent.nodeId);
            uniqueNodeIds.insert(*ent.nodeId);
        }
        if (!nodeIds.empty()) {
            docToNodes.emplace(doc.id, std::move(nodeIds));
        }
    }
    if (uniqueNodeIds.empty()) {
        return out;
    }
    std::vector<std::int64_t> bulkIds(uniqueNodeIds.begin(), uniqueNodeIds.end());
    auto nodesResult = kgStore.getNodesByIds(bulkIds);
    if (!nodesResult) {
        return out;
    }
    out.nodeIdToType.reserve(nodesResult.value().size());
    std::unordered_map<std::string, std::size_t> typeCounts;
    for (const auto& node : nodesResult.value()) {
        if (!node.type.has_value()) {
            continue;
        }
        std::string canonical = *node.type;
        std::transform(canonical.begin(), canonical.end(), canonical.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (canonical.empty() || isFilteredEntityType(canonical)) {
            continue;
        }
        out.nodeIdToType[node.id] = canonical;
    }
    // Pass 2: count types per doc (each type contributes once per doc, not per occurrence).
    for (const auto& [docId, nodeIds] : docToNodes) {
        std::unordered_set<std::string> docTypes;
        for (auto nid : nodeIds) {
            auto it = out.nodeIdToType.find(nid);
            if (it != out.nodeIdToType.end()) {
                docTypes.insert(it->second);
            }
        }
        for (const auto& t : docTypes) {
            ++typeCounts[t];
        }
    }
    // Top-K by frequency (ties broken by lexicographic order for determinism).
    std::vector<std::pair<std::string, std::size_t>> ranked(typeCounts.begin(), typeCounts.end());
    std::sort(ranked.begin(), ranked.end(), [](const auto& a, const auto& b) {
        if (a.second != b.second)
            return a.second > b.second;
        return a.first < b.first;
    });
    const std::size_t kEffective = std::min(K, ranked.size());
    out.topKTypes.reserve(kEffective);
    for (std::size_t i = 0; i < kEffective; ++i) {
        out.typeToIdx[ranked[i].first] = i;
        out.topKTypes.push_back(ranked[i].first);
    }
    return out;
}

// Phase V-A: build per-doc entity-type histogram weighted by GLiNER confidence.
std::vector<float> buildEntityTypeSignature(metadata::KnowledgeGraphStore& kgStore,
                                            std::int64_t docId, const EntityTypeIndex& idx,
                                            float minConfidence,
                                            std::size_t* skippedLowConfidenceOut) {
    if (idx.topKTypes.empty()) {
        return {};
    }
    std::vector<float> sig(idx.topKTypes.size(), 0.0F);
    auto entitiesResult = kgStore.getDocEntitiesForDocument(docId);
    if (!entitiesResult) {
        return sig;
    }
    bool any = false;
    for (const auto& ent : entitiesResult.value()) {
        if (!ent.nodeId.has_value()) {
            continue;
        }
        if (ent.confidence.has_value() && ent.confidence.value() < minConfidence) {
            if (skippedLowConfidenceOut != nullptr) {
                ++(*skippedLowConfidenceOut);
            }
            continue;
        }
        auto typeIt = idx.nodeIdToType.find(*ent.nodeId);
        if (typeIt == idx.nodeIdToType.end()) {
            continue;
        }
        auto bucketIt = idx.typeToIdx.find(typeIt->second);
        if (bucketIt == idx.typeToIdx.end()) {
            continue;
        }
        sig[bucketIt->second] += ent.confidence.value_or(1.0F);
        any = true;
    }
    if (!any) {
        return {};
    }
    l2NormalizeInPlace(sig);
    return sig;
}

// Phase V composer: returns the final per-doc feature vector.
std::vector<float> composeFeatureVector(std::vector<float> dense,
                                        const std::vector<float>& matryoshkaWeights,
                                        const std::vector<float>& entitySignature,
                                        const std::vector<float>& minhashSketch,
                                        const FeatureComposition& cfg) {
    if (dense.empty()) {
        return dense; // nothing to compose against
    }
    // V-B: matryoshka coarse projection (no-op if disabled or target_dim >= full).
    if (cfg.enableMatryoshkaCoarseView && !matryoshkaWeights.empty() &&
        cfg.matryoshkaTargetDim > 0 && cfg.matryoshkaTargetDim < dense.size()) {
        dense = applyMatryoshkaCoarse(dense, matryoshkaWeights, cfg.matryoshkaTargetDim);
    } else {
        // Always L2-normalize the dense view to make weight tuning interpretable; embedding
        // already operates on cosine, so this is a no-op for clustering geometry but
        // makes weighted concat well-defined.
        l2NormalizeInPlace(dense);
    }
    // Determine weights. When all branches off, return dense unchanged (no-op == V0 baseline).
    const bool entityOn = cfg.enableEntityFusion && !entitySignature.empty();
    const bool minhashOn = cfg.enableMinHashSketch && !minhashSketch.empty();
    if (!entityOn && !minhashOn) {
        return dense;
    }
    const float alphaE = entityOn ? cfg.entityFusionAlpha : 0.0F;
    const float alphaM = minhashOn ? cfg.minhashAlpha : 0.0F;
    const float alphaD = std::max(0.0F, 1.0F - alphaE - alphaM);
    std::vector<float> composed;
    composed.reserve(dense.size() + (entityOn ? entitySignature.size() : 0) +
                     (minhashOn ? minhashSketch.size() : 0));
    for (float v : dense) {
        composed.push_back(v * alphaD);
    }
    if (entityOn) {
        for (float v : entitySignature) {
            composed.push_back(v * alphaE);
        }
    }
    if (minhashOn) {
        for (float v : minhashSketch) {
            composed.push_back(v * alphaM);
        }
    }
    return composed;
}

std::optional<std::string> documentHashFromNodeKey(std::string_view nodeKey) {
    if (!nodeKey.starts_with(kDocNodePrefix) || nodeKey.size() <= kDocNodePrefix.size()) {
        return std::nullopt;
    }
    return std::string{nodeKey.substr(kDocNodePrefix.size())};
}

std::vector<float> aggregateEmbedding(const std::vector<vector::VectorRecord>& records) {
    for (const auto& record : records) {
        if (record.level == vector::EmbeddingLevel::DOCUMENT && !record.embedding.empty()) {
            return record.embedding;
        }
    }

    std::vector<float> aggregate;
    std::size_t contributing = 0;
    for (const auto& record : records) {
        if (record.embedding.empty()) {
            continue;
        }
        if (aggregate.empty()) {
            aggregate.assign(record.embedding.begin(), record.embedding.end());
            contributing = 1;
            continue;
        }
        if (aggregate.size() != record.embedding.size()) {
            continue;
        }
        for (std::size_t i = 0; i < aggregate.size(); ++i) {
            aggregate[i] += record.embedding[i];
        }
        ++contributing;
    }

    if (contributing > 1) {
        for (auto& value : aggregate) {
            value /= static_cast<float>(contributing);
        }
    }
    return aggregate;
}

std::vector<TopologyNeighbor> collectNeighborsForDocument(metadata::KnowledgeGraphStore& kgStore,
                                                          std::int64_t nodeId,
                                                          std::size_t maxNeighborsPerDocument,
                                                          TopologyExtractionStats* stats) {
    auto edgeResult = kgStore.getEdgesBidirectional(
        nodeId, std::string_view{"semantic_neighbor"},
        std::max<std::size_t>(maxNeighborsPerDocument * 4, maxNeighborsPerDocument));
    if (!edgeResult) {
        return {};
    }

    if (stats != nullptr) {
        stats->neighborEdgesScanned += edgeResult.value().size();
    }

    std::unordered_map<std::int64_t, float> outgoingScores;
    std::unordered_set<std::int64_t> incomingIds;
    std::vector<std::int64_t> neighborNodeIds;
    neighborNodeIds.reserve(edgeResult.value().size());

    for (const auto& edge : edgeResult.value()) {
        if (edge.srcNodeId == nodeId) {
            auto it = outgoingScores.find(edge.dstNodeId);
            if (it == outgoingScores.end()) {
                outgoingScores.emplace(edge.dstNodeId, edge.weight);
                neighborNodeIds.push_back(edge.dstNodeId);
            } else {
                it->second = std::max(it->second, edge.weight);
            }
        }
        if (edge.dstNodeId == nodeId) {
            incomingIds.insert(edge.srcNodeId);
        }
    }

    if (neighborNodeIds.empty()) {
        return {};
    }

    auto nodesResult = kgStore.getNodesByIds(neighborNodeIds);
    if (!nodesResult) {
        return {};
    }

    std::vector<TopologyNeighbor> neighbors;
    neighbors.reserve(nodesResult.value().size());
    for (const auto& node : nodesResult.value()) {
        auto documentHash = documentHashFromNodeKey(node.nodeKey);
        if (!documentHash.has_value()) {
            continue;
        }
        const auto scoreIt = outgoingScores.find(node.id);
        if (scoreIt == outgoingScores.end()) {
            continue;
        }
        neighbors.push_back(TopologyNeighbor{.documentHash = *documentHash,
                                             .score = scoreIt->second,
                                             .reciprocal = incomingIds.contains(node.id)});
    }

    std::sort(neighbors.begin(), neighbors.end(),
              [](const TopologyNeighbor& lhs, const TopologyNeighbor& rhs) {
                  if (lhs.score != rhs.score) {
                      return lhs.score > rhs.score;
                  }
                  return lhs.documentHash < rhs.documentHash;
              });
    if (neighbors.size() > maxNeighborsPerDocument) {
        neighbors.resize(maxNeighborsPerDocument);
    }
    if (stats != nullptr) {
        stats->neighborsReturned += neighbors.size();
    }
    return neighbors;
}

} // namespace

TopologyInputExtractor::TopologyInputExtractor(
    std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
    std::shared_ptr<vector::VectorDatabase> vectorDb)
    : metadataRepo_(std::move(metadataRepo)), kgStore_(std::move(kgStore)),
      vectorDb_(std::move(vectorDb)) {}

Result<std::vector<TopologyDocumentInput>>
TopologyInputExtractor::extract(const TopologyExtractionConfig& config,
                                TopologyExtractionStats* stats) const {
    if (!metadataRepo_) {
        return Error{ErrorCode::InvalidState, "topology extractor requires metadata repository"};
    }
    if (!kgStore_) {
        return Error{ErrorCode::InvalidState, "topology extractor requires knowledge graph store"};
    }
    if (config.includeEmbeddings && !vectorDb_) {
        return Error{ErrorCode::InvalidState,
                     "topology extractor requires vector database when embeddings are enabled"};
    }

    TopologyExtractionStats localStats;
    localStats.seedDocuments = config.documentHashes.size();
    localStats.documentsRequested = config.documentHashes.empty()
                                        ? static_cast<std::size_t>(std::max(config.limit, 0))
                                        : config.documentHashes.size();

    std::vector<metadata::DocumentInfo> documents;
    if (!config.documentHashes.empty()) {
        auto batchResult = metadataRepo_->batchGetDocumentsByHash(config.documentHashes);
        if (!batchResult) {
            return batchResult.error();
        }
        documents.reserve(batchResult.value().size());
        for (const auto& hash : config.documentHashes) {
            auto it = batchResult.value().find(hash);
            if (it != batchResult.value().end()) {
                documents.push_back(it->second);
            }
        }
    } else {
        metadata::DocumentQueryOptions options;
        options.limit = config.limit;
        auto queryResult = metadataRepo_->queryDocuments(options);
        if (!queryResult) {
            return queryResult.error();
        }
        documents = std::move(queryResult.value());
    }

    localStats.documentsLoaded = documents.size();

    std::unordered_map<std::string, vector::VectorRecord> docLevelVectors;
    if (config.includeEmbeddings && vectorDb_) {
        docLevelVectors = vectorDb_->getDocumentLevelVectorsAll();
    }

    // Phase V — feature composer prep:
    //   1. (V-A) build corpus top-K canonical entity-type histogram once
    //   2. (V-B) collect raw embeddings to compute matryoshka variance weights once
    EntityTypeIndex entityIdx;
    const FeatureComposition& fc = config.featureComposition;
    if (fc.enableEntityFusion && fc.entitySignatureK > 0) {
        entityIdx =
            buildEntityTypeIndex(*kgStore_, documents, fc.entitySignatureK, fc.entityMinConfidence);
        localStats.composedTopKEntityTypeCount = entityIdx.topKTypes.size();
    }
    std::vector<float> matryoshkaWeights;
    if (fc.enableMatryoshkaCoarseView && fc.matryoshkaTargetDim > 0 && config.includeEmbeddings &&
        vectorDb_) {
        std::vector<std::vector<float>> sample;
        sample.reserve(documents.size());
        for (const auto& doc : documents) {
            auto it = docLevelVectors.find(doc.sha256Hash);
            if (it != docLevelVectors.end() && !it->second.embedding.empty()) {
                sample.push_back(it->second.embedding);
                if (sample.size() >= 4096) {
                    break;
                }
            }
        }
        matryoshkaWeights = computeVarianceWeights(sample, fc.matryoshkaTargetDim);
    }

    std::vector<TopologyDocumentInput> extracted;
    extracted.reserve(documents.size());
    for (const auto& document : documents) {
        std::vector<float> embedding;
        if (config.includeEmbeddings && vectorDb_) {
            auto it = docLevelVectors.find(document.sha256Hash);
            if (it != docLevelVectors.end() && !it->second.embedding.empty()) {
                embedding = it->second.embedding;
            } else {
                auto vectors = vectorDb_->getVectorsByDocument(document.sha256Hash);
                embedding = aggregateEmbedding(vectors);
            }
            if (embedding.empty()) {
                ++localStats.documentsMissingEmbeddings;
                if (config.requireEmbeddings) {
                    continue;
                }
            }
        }

        auto nodeResult = kgStore_->getNodeByKey(nodeKeyForDocumentHash(document.sha256Hash));
        if (!nodeResult) {
            return nodeResult.error();
        }
        if (!nodeResult.value().has_value()) {
            ++localStats.documentsMissingGraphNodes;
            if (config.requireGraphNode) {
                continue;
            }
        }

        // Phase V — compose final feature vector. When all branches off, this returns
        // the dense embedding unchanged (V0 baseline reproduces pre-Phase-V behaviour).
        std::vector<float> entitySig;
        if (fc.enableEntityFusion && !entityIdx.topKTypes.empty()) {
            entitySig =
                buildEntityTypeSignature(*kgStore_, document.id, entityIdx, fc.entityMinConfidence,
                                         &localStats.composedFilteredLowConfidenceCount);
            if (!entitySig.empty()) {
                ++localStats.composedDocsWithEntitySignature;
            }
        }
        // V-C: compute MinHash on the doc body in-extractor (no schema migration needed).
        // Skipped (and branch becomes no-op) when the body is unavailable.
        std::vector<float> minhashSketch;
        if (fc.enableMinHashSketch && fc.minhashSketchDim > 0) {
            auto contentResult = metadataRepo_->getContent(document.id);
            if (contentResult && contentResult.value().has_value()) {
                const auto& body = contentResult.value().value().contentText;
                if (!body.empty()) {
                    static thread_local simeon::MinHashEncoder kEncoder{simeon::MinHashConfig{}};
                    std::vector<std::uint32_t> sig(kEncoder.k());
                    kEncoder.encode(body, sig.data());
                    minhashSketch = bucketCountSketch(sig, fc.minhashSketchDim);
                    if (!minhashSketch.empty()) {
                        ++localStats.composedDocsWithMinHashSketch;
                    }
                }
            }
        }
        std::vector<float> composed;
        if (!embedding.empty()) {
            composed = composeFeatureVector(std::move(embedding), matryoshkaWeights, entitySig,
                                            minhashSketch, fc);
        }

        TopologyDocumentInput input;
        input.documentHash = document.sha256Hash;
        input.filePath = document.filePath;
        input.embedding = std::move(composed);

        if (config.includeMetadata) {
            auto metadataResult = metadataRepo_->getAllMetadata(document.id);
            if (!metadataResult) {
                return metadataResult.error();
            }
            input.metadata.reserve(metadataResult.value().size() + 3);
            for (const auto& [key, value] : metadataResult.value()) {
                input.metadata[key] = value.asString();
            }
            input.metadata["mime_type"] = document.mimeType;
            input.metadata["file_name"] = document.fileName;
            input.metadata["file_extension"] = document.fileExtension;
            ++localStats.metadataMapsLoaded;
        }

        if (nodeResult.value().has_value()) {
            input.neighbors = collectNeighborsForDocument(
                *kgStore_, nodeResult.value()->id, config.maxNeighborsPerDocument, &localStats);
        }

        extracted.push_back(std::move(input));
    }

    std::sort(extracted.begin(), extracted.end(),
              [](const TopologyDocumentInput& lhs, const TopologyDocumentInput& rhs) {
                  return lhs.documentHash < rhs.documentHash;
              });

    localStats.documentsReturned = extracted.size();
    localStats.regionDocuments = extracted.size();
    if (!extracted.empty()) {
        localStats.composedFeatureDim = extracted.front().embedding.size();
    }
    if (stats != nullptr) {
        *stats = localStats;
    }
    return extracted;
}

} // namespace yams::topology
