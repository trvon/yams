// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/search/search_tracing.h>

#include <algorithm>
#include <cmath>
#include <unordered_map>
#include <unordered_set>

namespace yams::search {

using nlohmann::json;

std::string deriveDocIdFromPath(const std::string& path) {
    if (path.empty()) {
        return {};
    }

    const auto slashPos = path.find_last_of("/\\");
    std::string fileName =
        (slashPos == std::string::npos) ? path : path.substr(static_cast<size_t>(slashPos + 1));

    constexpr std::string_view kTxtSuffix = ".txt";
    if (fileName.size() > kTxtSuffix.size() &&
        fileName.compare(fileName.size() - kTxtSuffix.size(), kTxtSuffix.size(), kTxtSuffix) == 0) {
        fileName.resize(fileName.size() - kTxtSuffix.size());
    }
    return fileName;
}

std::string documentIdForTrace(const std::string& filePath, const std::string& documentHash) {
    std::string docId = deriveDocIdFromPath(filePath);
    if (!docId.empty()) {
        return docId;
    }
    return documentHash;
}

std::vector<std::string>
collectUniqueComponentDocIds(const std::vector<ComponentResult>& componentResults) {
    std::unordered_set<std::string> uniqueIds;
    uniqueIds.reserve(componentResults.size());

    for (const auto& comp : componentResults) {
        const std::string docId = documentIdForTrace(comp.filePath, comp.documentHash);
        if (!docId.empty()) {
            uniqueIds.insert(docId);
        }
    }

    std::vector<std::string> out;
    out.reserve(uniqueIds.size());
    for (const auto& id : uniqueIds) {
        out.push_back(id);
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<std::string> collectRankedResultDocIds(const std::vector<SearchResult>& results,
                                                   size_t maxCount) {
    const size_t count = std::min(results.size(), maxCount);
    std::vector<std::string> out;
    out.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        out.push_back(
            documentIdForTrace(results[i].document.filePath, results[i].document.sha256Hash));
    }
    return out;
}

std::vector<std::string> setDifferenceIds(const std::vector<std::string>& lhs,
                                          const std::vector<std::string>& rhs) {
    std::unordered_set<std::string> rhsSet;
    rhsSet.reserve(rhs.size());
    for (const auto& id : rhs) {
        rhsSet.insert(id);
    }

    std::vector<std::string> out;
    out.reserve(lhs.size());
    for (const auto& id : lhs) {
        if (rhsSet.find(id) == rhsSet.end()) {
            out.push_back(id);
        }
    }
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

std::string joinWithTab(const std::vector<std::string>& values) {
    if (values.empty()) {
        return {};
    }
    std::string out;
    size_t totalChars = 0;
    for (const auto& value : values) {
        totalChars += value.size();
    }
    out.reserve(totalChars + values.size());

    bool first = true;
    for (const auto& value : values) {
        if (!first) {
            out.push_back('\t');
        }
        out += value;
        first = false;
    }
    return out;
}

nlohmann::json buildComponentHitSummaryJson(const std::vector<ComponentResult>& componentResults,
                                            size_t topPerComponent) {
    struct RankedHit {
        size_t rank = 0;
        std::string docId;
        double score = 0.0;
    };

    std::unordered_map<ComponentResult::Source, std::vector<RankedHit>> grouped;
    grouped.reserve(8);

    for (const auto& comp : componentResults) {
        const std::string docId = documentIdForTrace(comp.filePath, comp.documentHash);
        if (docId.empty()) {
            continue;
        }
        grouped[comp.source].push_back(RankedHit{
            .rank = comp.rank,
            .docId = docId,
            .score = static_cast<double>(comp.score),
        });
    }

    nlohmann::json out = nlohmann::json::object();
    for (auto& [source, docs] : grouped) {
        std::sort(docs.begin(), docs.end(), [](const auto& a, const auto& b) {
            if (a.rank != b.rank) {
                return a.rank < b.rank;
            }
            return a.docId < b.docId;
        });

        std::vector<std::string> topDocIds;
        nlohmann::json topHits = nlohmann::json::array();
        topDocIds.reserve(std::min(topPerComponent, docs.size()));
        std::unordered_set<std::string> seen;
        seen.reserve(docs.size());
        for (const auto& hit : docs) {
            if (!seen.insert(hit.docId).second) {
                continue;
            }
            topDocIds.push_back(hit.docId);
            topHits.push_back({
                {"doc_id", hit.docId},
                {"rank", hit.rank + 1},
                {"score", hit.score},
            });
            if (topDocIds.size() >= topPerComponent) {
                break;
            }
        }

        out[componentSourceToString(source)] = {
            {"raw_hits", docs.size()},
            {"unique_top_doc_ids", topDocIds},
            {"top_hits", topHits},
        };
    }

    return out;
}

SearchTraceCollector::SearchTraceCollector(const SearchEngineConfig& config) : config_(config) {}

void SearchTraceCollector::markStageConfigured(const std::string& name, bool enabled) {
    stages_[name].enabled = enabled;
}

void SearchTraceCollector::markStageAttempted(const std::string& name) {
    auto& stage = stages_[name];
    stage.attempted = true;
    stage.skipped = false;
    stage.skipReason.clear();
}

void SearchTraceCollector::markStageResult(const std::string& name,
                                           const std::vector<ComponentResult>& results,
                                           std::int64_t durationMicros, bool contributed) {
    auto& stage = stages_[name];
    stage.attempted = true;
    stage.failed = false;
    stage.timedOut = false;
    stage.skipped = false;
    stage.skipReason.clear();
    stage.contributed = contributed;
    stage.durationMicros = durationMicros;
    stage.rawHitCount = results.size();

    std::unordered_set<std::string> uniqueDocs;
    uniqueDocs.reserve(results.size());
    for (const auto& comp : results) {
        const std::string docId = documentIdForTrace(comp.filePath, comp.documentHash);
        if (!docId.empty()) {
            uniqueDocs.insert(docId);
        }
    }
    stage.uniqueDocCount = uniqueDocs.size();
    stage.uniqueDocIds.assign(uniqueDocs.begin(), uniqueDocs.end());
    std::sort(stage.uniqueDocIds.begin(), stage.uniqueDocIds.end());
}

void SearchTraceCollector::markStageTimeout(const std::string& name, std::int64_t durationMicros) {
    auto& stage = stages_[name];
    stage.attempted = true;
    stage.timedOut = true;
    stage.failed = false;
    stage.skipped = false;
    stage.skipReason.clear();
    stage.contributed = false;
    stage.durationMicros = durationMicros;
}

void SearchTraceCollector::markStageFailure(const std::string& name, std::int64_t durationMicros) {
    auto& stage = stages_[name];
    stage.attempted = true;
    stage.failed = true;
    stage.timedOut = false;
    stage.skipped = false;
    stage.skipReason.clear();
    stage.contributed = false;
    stage.durationMicros = durationMicros;
}

void SearchTraceCollector::markStageSkipped(const std::string& name, std::string reason) {
    auto& stage = stages_[name];
    stage.skipped = true;
    stage.attempted = false;
    stage.failed = false;
    stage.timedOut = false;
    stage.contributed = false;
    stage.rawHitCount = 0;
    stage.uniqueDocCount = 0;
    stage.uniqueDocIds.clear();
    stage.durationMicros = 0;
    stage.skipReason = std::move(reason);
}

nlohmann::json SearchTraceCollector::buildStageSummaryJson() const {
    nlohmann::json out = nlohmann::json::object();
    for (const auto& [name, stage] : stages_) {
        out[name] = {
            {"enabled", stage.enabled},
            {"attempted", stage.attempted},
            {"contributed", stage.contributed},
            {"timed_out", stage.timedOut},
            {"failed", stage.failed},
            {"skipped", stage.skipped},
            {"skip_reason", stage.skipReason},
            {"raw_hit_count", stage.rawHitCount},
            {"unique_doc_count", stage.uniqueDocCount},
            {"unique_doc_ids", stage.uniqueDocIds},
            {"duration_ms", static_cast<double>(stage.durationMicros) / 1000.0},
        };
    }
    return out;
}

nlohmann::json SearchTraceCollector::buildFusionSourceSummaryJson(
    const std::vector<ComponentResult>& componentResults,
    const std::vector<SearchResult>& finalResults, std::size_t topPerSource) const {
    struct SourceSummary {
        std::size_t rawHitCount = 0;
        std::unordered_set<std::string> uniqueDocs;
        std::unordered_set<std::string> finalDocs;
        double scoreMass = 0.0;
    };

    std::unordered_map<ComponentResult::Source, SourceSummary> summaries;
    summaries.reserve(10);

    for (const auto& comp : componentResults) {
        auto& summary = summaries[comp.source];
        summary.rawHitCount++;
        const std::string docId = documentIdForTrace(comp.filePath, comp.documentHash);
        if (!docId.empty()) {
            summary.uniqueDocs.insert(docId);
        }
    }

    const std::size_t limit = std::min(topPerSource, finalResults.size());
    for (std::size_t i = 0; i < limit; ++i) {
        const auto& result = finalResults[i];
        const std::string docId =
            documentIdForTrace(result.document.filePath, result.document.sha256Hash);
        if (docId.empty()) {
            continue;
        }

        for (int rawSource = static_cast<int>(ComponentResult::Source::Text);
             rawSource <= static_cast<int>(ComponentResult::Source::Unknown); ++rawSource) {
            const auto source = static_cast<ComponentResult::Source>(rawSource);
            const double contribution = componentSourceScoreInResult(result, source);
            if (std::abs(contribution) <= 1e-12) {
                continue;
            }
            auto& summary = summaries[source];
            summary.finalDocs.insert(docId);
            summary.scoreMass += contribution;
        }
    }

    nlohmann::json out = nlohmann::json::object();
    for (int rawSource = static_cast<int>(ComponentResult::Source::Text);
         rawSource <= static_cast<int>(ComponentResult::Source::Unknown); ++rawSource) {
        const auto source = static_cast<ComponentResult::Source>(rawSource);
        const auto sourceName = componentSourceToString(source);
        const auto weight = componentSourceWeight(config_, source);
        auto it = summaries.find(source);
        const bool present = it != summaries.end();
        const std::size_t rawHits = present ? it->second.rawHitCount : 0;
        const std::size_t uniqueDocs = present ? it->second.uniqueDocs.size() : 0;
        const std::size_t finalDocs = present ? it->second.finalDocs.size() : 0;
        const double scoreMass = present ? it->second.scoreMass : 0.0;
        out[sourceName] = {
            {"weight", weight},
            {"enabled", weight > 0.0f},
            {"raw_hit_count", rawHits},
            {"unique_doc_count", uniqueDocs},
            {"contributed_to_final", finalDocs > 0},
            {"final_top_doc_count", finalDocs},
            {"final_score_mass", scoreMass},
        };
    }
    return out;
}

nlohmann::json buildFusionTopSummaryJson(const std::vector<SearchResult>& results, size_t maxDocs) {
    nlohmann::json out = nlohmann::json::array();
    const size_t count = std::min(results.size(), maxDocs);
    for (size_t i = 0; i < count; ++i) {
        const auto& res = results[i];
        out.push_back({
            {"rank", i + 1},
            {"doc_id", documentIdForTrace(res.document.filePath, res.document.sha256Hash)},
            {"path", res.document.filePath},
            {"score", res.score},
            {"keyword_score", res.keywordScore.value_or(0.0)},
            {"graph_text_score", res.graphTextScore.value_or(0.0)},
            {"vector_score", res.vectorScore.value_or(0.0)},
            {"graph_vector_score", res.graphVectorScore.value_or(0.0)},
            {"kg_score", res.kgScore.value_or(0.0)},
            {"path_score", res.pathScore.value_or(0.0)},
            {"tag_score", res.tagScore.value_or(0.0)},
            {"symbol_score", res.symbolScore.value_or(0.0)},
        });
    }
    return out;
}

std::optional<std::int64_t>
resolveKgDocumentId(const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
                    const metadata::DocumentInfo& doc) {
    if (!kgStore) {
        return std::nullopt;
    }
    if (!doc.sha256Hash.empty()) {
        auto byHash = kgStore->getDocumentIdByHash(doc.sha256Hash);
        if (byHash && byHash.value().has_value()) {
            return byHash.value();
        }
    }
    if (!doc.filePath.empty()) {
        auto byPath = kgStore->getDocumentIdByPath(doc.filePath);
        if (byPath && byPath.value().has_value()) {
            return byPath.value();
        }
    }
    return std::nullopt;
}

nlohmann::json buildGraphDocProbeJson(const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
                                      const std::vector<SearchResult>& results, size_t limit) {
    nlohmann::json out = nlohmann::json::array();
    if (!kgStore) {
        return out;
    }
    const size_t maxDocs = std::min(limit, results.size());
    for (size_t i = 0; i < maxDocs; ++i) {
        const auto& result = results[i];
        nlohmann::json item = {
            {"doc_id", documentIdForTrace(result.document.filePath, result.document.sha256Hash)},
            {"rank", i + 1},
            {"score", result.score},
            {"keyword_score", result.keywordScore.value_or(0.0)},
            {"vector_score", result.vectorScore.value_or(0.0)},
            {"kg_score", result.kgScore.value_or(0.0)},
        };

        auto docId = resolveKgDocumentId(kgStore, result.document);
        if (!docId.has_value()) {
            item["resolved_document_id"] = nullptr;
            item["doc_entity_count"] = 0;
            item["linked_node_count"] = 0;
            item["entity_text_samples"] = nlohmann::json::array();
            item["node_label_samples"] = nlohmann::json::array();
            out.push_back(std::move(item));
            continue;
        }

        item["resolved_document_id"] = *docId;
        auto ents = kgStore->getDocEntitiesForDocument(*docId, 2000, 0);
        if (!ents) {
            item["doc_entity_count"] = -1;
            item["linked_node_count"] = -1;
            item["entity_lookup_error"] = ents.error().message;
            out.push_back(std::move(item));
            continue;
        }

        std::unordered_set<std::int64_t> uniqueNodeIds;
        nlohmann::json entitySamples = nlohmann::json::array();
        for (const auto& ent : ents.value()) {
            if (ent.nodeId.has_value()) {
                uniqueNodeIds.insert(*ent.nodeId);
            }
            if (entitySamples.size() < 5) {
                entitySamples.push_back({
                    {"text", ent.entityText},
                    {"node_id", ent.nodeId.has_value() ? nlohmann::json(*ent.nodeId)
                                                       : nlohmann::json(nullptr)},
                    {"extractor", ent.extractor.value_or("")},
                    {"confidence", ent.confidence.has_value() ? nlohmann::json(*ent.confidence)
                                                              : nlohmann::json(nullptr)},
                });
            }
        }

        nlohmann::json nodeLabelSamples = nlohmann::json::array();
        for (auto nodeIdVal : uniqueNodeIds) {
            if (nodeLabelSamples.size() >= 5) {
                break;
            }
            auto nodeRes = kgStore->getNodeById(nodeIdVal);
            if (!nodeRes || !nodeRes.value().has_value()) {
                continue;
            }
            const auto& node = *nodeRes.value();
            nodeLabelSamples.push_back({
                {"node_id", nodeIdVal},
                {"label", node.label.value_or("")},
                {"type", node.type.value_or("")},
                {"node_key", node.nodeKey},
            });
        }

        item["doc_entity_count"] = ents.value().size();
        item["linked_node_count"] = uniqueNodeIds.size();
        item["entity_text_samples"] = std::move(entitySamples);
        item["node_label_samples"] = std::move(nodeLabelSamples);
        out.push_back(std::move(item));
    }
    return out;
}

} // namespace yams::search
