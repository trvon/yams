#include "search_vector_pipeline_internal.h"

#include <spdlog/spdlog.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/vector_database.h>

#include <algorithm>
#include <functional>
#include <unordered_map>

namespace yams::search::detail {
namespace {

std::string truncateSearchSnippet(const std::string& content, size_t maxLen) {
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

template <typename RecordRange>
std::unordered_map<std::string, std::string>
batchLoadDocumentPaths(const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                       const RecordRange& records) {
    std::unordered_map<std::string, std::string> hashToPath;
    if (!metadataRepo) {
        return hashToPath;
    }

    std::vector<std::string> hashes;
    hashes.reserve(records.size());
    for (const auto& record : records) {
        if (!record.document_hash.empty()) {
            hashes.push_back(record.document_hash);
        }
    }
    if (hashes.empty()) {
        return hashToPath;
    }

    auto docMapResult = metadataRepo->batchGetDocumentsByHash(hashes);
    if (!docMapResult) {
        return hashToPath;
    }

    hashToPath.reserve(docMapResult.value().size());
    for (const auto& [hash, docInfo] : docMapResult.value()) {
        hashToPath.emplace(hash, docInfo.filePath);
    }
    return hashToPath;
}

std::vector<vector::VectorRecord>
aggregateChunkVectorScores(const std::vector<vector::VectorRecord>& vectorRecords,
                           const SearchEngineConfig& config, size_t limit) {
    using Agg = SearchEngineConfig::ChunkAggregation;
    const auto aggStrategy = config.chunkAggregation;

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
                for (float s : scores) {
                    sum += s;
                }
                record.relevance_score = static_cast<float>(std::min(sum, 1.0));
            } else if (aggStrategy == Agg::TOP_K_AVG || aggStrategy == Agg::WEIGHTED_TOP_K_AVG) {
                std::sort(scores.begin(), scores.end(), std::greater<>());
                const size_t k = std::min(scores.size(), config.chunkAggregationTopK);
                double sum = 0.0;
                if (aggStrategy == Agg::TOP_K_AVG) {
                    for (size_t i = 0; i < k; ++i) {
                        sum += scores[i];
                    }
                    record.relevance_score = static_cast<float>(sum / static_cast<double>(k));
                } else {
                    const double decay = std::clamp(
                        static_cast<double>(config.chunkAggregationWeightDecay), 0.0, 1.0);
                    double weightedSum = 0.0;
                    double weightTotal = 0.0;
                    double weight = 1.0;
                    for (size_t i = 0; i < k; ++i) {
                        weightedSum += scores[i] * weight;
                        weightTotal += weight;
                        weight *= decay;
                    }
                    record.relevance_score = static_cast<float>(
                        weightTotal > 0.0 ? (weightedSum / weightTotal) : scores.front());
                }
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

std::vector<vector::EntityVectorRecord>
dedupeEntityVectorRecords(std::vector<vector::EntityVectorRecord> entityRecords, size_t limit) {
    if (entityRecords.empty()) {
        return entityRecords;
    }

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

    std::sort(deduped.begin(), deduped.end(),
              [](const auto& a, const auto& b) { return a.relevance_score > b.relevance_score; });
    if (deduped.size() > limit) {
        deduped.resize(limit);
    }
    return deduped;
}

Result<std::vector<ComponentResult>>
queryVectorIndexImpl(const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                     const std::shared_ptr<vector::VectorDatabase>& vectorDb,
                     const std::vector<float>& embedding, const SearchEngineConfig& config,
                     size_t limit, const std::unordered_set<std::string>* candidates) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    if (!vectorDb) {
        return results;
    }

    try {
        vector::VectorSearchParams params;
        params.k = limit;
        params.similarity_threshold = config.similarityThreshold;
        if (candidates != nullptr) {
            params.candidate_hashes = *candidates;
        }

        auto vectorRecords = vectorDb->search(embedding, params);
        if (!vectorRecords.empty()) {
            vectorRecords = aggregateChunkVectorScores(vectorRecords, config, limit);
        }
        if (vectorRecords.empty()) {
            return results;
        }

        const auto hashToPath = batchLoadDocumentPaths(metadataRepo, vectorRecords);

        for (size_t rank = 0; rank < vectorRecords.size(); ++rank) {
            const auto& vr = vectorRecords[rank];

            ComponentResult result;
            result.documentHash = vr.document_hash;
            result.score = vr.relevance_score;
            result.source = ComponentResult::Source::Vector;
            result.rank = rank;

            if (auto it = hashToPath.find(vr.document_hash); it != hashToPath.end()) {
                result.filePath = it->second;
            }
            if (!vr.content.empty()) {
                result.snippet = truncateSearchSnippet(vr.content, 200);
            }

            results.push_back(std::move(result));
        }

        if (candidates != nullptr) {
            spdlog::debug("Vector search (narrowed to {} candidates) returned {} results",
                          candidates->size(), results.size());
        } else {
            spdlog::debug("queryVectorIndex: {} results (limit={}, threshold={})", results.size(),
                          limit, config.similarityThreshold);
        }
    } catch (const std::exception& e) {
        spdlog::warn("Vector search exception: {}", e.what());
        return results;
    }

    return results;
}

} // namespace

Result<std::vector<ComponentResult>>
queryVectorIndexPipeline(const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                         const std::shared_ptr<vector::VectorDatabase>& vectorDb,
                         const std::vector<float>& embedding, const SearchEngineConfig& config,
                         size_t limit) {
    return queryVectorIndexImpl(metadataRepo, vectorDb, embedding, config, limit, nullptr);
}

Result<std::vector<ComponentResult>>
queryVectorIndexPipeline(const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                         const std::shared_ptr<vector::VectorDatabase>& vectorDb,
                         const std::vector<float>& embedding, const SearchEngineConfig& config,
                         size_t limit, const std::unordered_set<std::string>& candidates) {
    return queryVectorIndexImpl(metadataRepo, vectorDb, embedding, config, limit, &candidates);
}

Result<std::vector<ComponentResult>>
queryEntityVectorsPipeline(const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                           const std::shared_ptr<vector::VectorDatabase>& vectorDb,
                           const std::vector<float>& embedding, const SearchEngineConfig& config,
                           size_t limit) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    if (!vectorDb) {
        return results;
    }

    try {
        vector::EntitySearchParams params;
        params.k = limit;
        params.similarity_threshold = config.similarityThreshold;
        params.include_embeddings = false;

        auto entityRecords =
            dedupeEntityVectorRecords(vectorDb->searchEntities(embedding, params), limit);
        if (entityRecords.empty()) {
            return results;
        }

        const auto hashToPath = batchLoadDocumentPaths(metadataRepo, entityRecords);

        for (size_t rank = 0; rank < entityRecords.size(); ++rank) {
            const auto& er = entityRecords[rank];

            ComponentResult result;
            result.documentHash = er.document_hash;
            result.score = er.relevance_score;
            result.source = ComponentResult::Source::EntityVector;
            result.rank = rank;

            if (!er.file_path.empty()) {
                result.filePath = er.file_path;
            } else if (auto it = hashToPath.find(er.document_hash); it != hashToPath.end()) {
                result.filePath = it->second;
            }

            if (!er.qualified_name.empty()) {
                result.snippet = er.qualified_name;
            } else if (!er.node_key.empty()) {
                result.snippet = er.node_key;
            }

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

} // namespace yams::search::detail
