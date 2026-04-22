#include <yams/topology/topology_input_extractor.h>
#include <yams/vector/vector_database.h>

#include <algorithm>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace yams::topology {

namespace {

constexpr std::string_view kDocNodePrefix = "doc:";

std::string nodeKeyForDocumentHash(std::string_view documentHash) {
    return std::string{kDocNodePrefix} + std::string{documentHash};
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

        TopologyDocumentInput input;
        input.documentHash = document.sha256Hash;
        input.filePath = document.filePath;
        input.embedding = std::move(embedding);

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
    if (stats != nullptr) {
        *stats = localStats;
    }
    return extracted;
}

} // namespace yams::topology
