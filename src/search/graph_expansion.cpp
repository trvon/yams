// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/search/graph_expansion.h>

#include <nlohmann/json.hpp>
#include <algorithm>
#include <cctype>
#include <unordered_map>
#include <unordered_set>

#include <yams/search/query_text_utils.h>

namespace yams::search {

namespace {

std::optional<std::int64_t>
resolveKgDocumentId(const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
                    const GraphExpansionSeedDoc& doc) {
    if (!kgStore) {
        return std::nullopt;
    }
    if (!doc.documentHash.empty()) {
        auto byHash = kgStore->getDocumentIdByHash(doc.documentHash);
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

std::optional<std::string> documentHashFromDocNodeKey(std::string_view nodeKey) {
    constexpr std::string_view kPrefix = "doc:";
    if (nodeKey.rfind(kPrefix, 0) != 0 || nodeKey.size() <= kPrefix.size()) {
        return std::nullopt;
    }
    return std::string(nodeKey.substr(kPrefix.size()));
}

float relationExpansionWeight(const metadata::KGEdge& edge) {
    float base = 0.35f;
    const auto& relation = edge.relation;
    if (relation == "primary_topic_of" || relation == "title_mentions") {
        base = 0.95f;
    } else if (relation == "mentioned_in_segment") {
        base = 0.90f;
    } else if (relation == "contains_segment" || relation == "segment_of") {
        base = 0.55f;
    } else if (relation == "co_occurs_biomedical" || relation == "protein_cell_association" ||
               relation == "protein_disease_association" ||
               relation == "drug_disease_association") {
        base = 0.80f;
    } else if (relation == "co_mentioned_with") {
        base = 0.50f;
    } else if (relation == "mentioned_in") {
        base = 0.10f;
    }

    if (edge.properties.has_value()) {
        try {
            auto props = nlohmann::json::parse(*edge.properties);
            const std::string region = props.value("region", "");
            const std::string scope = props.value("scope", "");
            if (region == "body_claim" || scope == "body_segment") {
                base *= 1.25f;
            } else if (region == "title" || scope == "title") {
                base *= 1.20f;
            } else if (region == "summary" || scope == "summary") {
                base *= 0.90f;
            }
        } catch (...) {
        }
    }

    return std::clamp(base, 0.0f, 1.25f);
}

float surfaceQueryMatchScore(const std::vector<std::string>& queryAliases,
                             std::string_view candidateText) {
    const std::string normalizedCandidate = normalizeGraphSurface(candidateText);
    if (normalizedCandidate.empty()) {
        return 0.0f;
    }
    const auto candidateTokens = tokenizeKgQuery(normalizedCandidate);
    const std::unordered_set<std::string> candidateSet(candidateTokens.begin(),
                                                       candidateTokens.end());

    float best = 0.0f;
    for (const auto& alias : queryAliases) {
        if (alias.empty()) {
            continue;
        }
        if (alias == normalizedCandidate) {
            return 1.0f;
        }
        const auto aliasTokens = tokenizeKgQuery(alias);
        if (alias.size() >= 4 && (aliasTokens.size() >= 2 || candidateSet.size() == 1) &&
            (normalizedCandidate.find(alias) != std::string::npos ||
             alias.find(normalizedCandidate) != std::string::npos)) {
            best = std::max(best, 0.85f);
        }

        if (aliasTokens.empty()) {
            continue;
        }
        size_t overlap = 0;
        for (const auto& tok : aliasTokens) {
            if (candidateSet.find(tok) != candidateSet.end()) {
                ++overlap;
            }
        }
        if (overlap >= 2) {
            const float coverage = static_cast<float>(overlap) /
                                   static_cast<float>(std::max<size_t>(1, aliasTokens.size()));
            best = std::max(best, coverage);
        }
    }
    return best;
}

} // namespace

std::vector<std::string> tokenizeKgQuery(std::string_view query) {
    static const std::unordered_set<std::string> kStopwords = {
        "the", "a",   "an",   "and", "or",   "not",  "to",    "of",   "in",
        "on",  "for", "with", "by",  "from", "is",   "are",   "was",  "were",
        "be",  "as",  "at",   "it",  "this", "that", "these", "those"};

    auto flushTokenVariants = [](const std::string& raw, std::vector<std::string>& dest) {
        if (raw.empty()) {
            return;
        }
        std::string lowered = toLowerCopy(raw);
        dest.push_back(lowered);

        std::vector<std::string> parts;
        std::string part;
        part.push_back(lowered[0]);
        for (size_t i = 1; i < lowered.size(); ++i) {
            const bool prevDigit = std::isdigit(static_cast<unsigned char>(lowered[i - 1])) != 0;
            const bool curDigit = std::isdigit(static_cast<unsigned char>(lowered[i])) != 0;
            if (prevDigit != curDigit && !part.empty()) {
                parts.push_back(part);
                part.clear();
            }
            part.push_back(lowered[i]);
        }
        if (!part.empty()) {
            parts.push_back(std::move(part));
        }
        if (parts.size() >= 2) {
            for (const auto& p : parts) {
                if (p.size() >= 2) {
                    dest.push_back(p);
                }
            }
            std::string joined = parts[0];
            for (size_t i = 1; i < parts.size(); ++i) {
                joined.push_back(' ');
                joined += parts[i];
            }
            dest.push_back(std::move(joined));
        }
    };

    std::vector<std::string> rawTokens;
    rawTokens.reserve(query.size() / 4 + 1);
    std::string current;
    for (unsigned char uc : query) {
        if (std::isalnum(uc)) {
            current.push_back(static_cast<char>(uc));
        } else if (uc == '-' || uc == '/' || uc == '_') {
            flushTokenVariants(current, rawTokens);
            current.clear();
        } else {
            flushTokenVariants(current, rawTokens);
            current.clear();
        }
    }
    flushTokenVariants(current, rawTokens);

    std::vector<std::string> filtered;
    filtered.reserve(rawTokens.size());
    std::unordered_set<std::string> seen;
    for (const auto& token : rawTokens) {
        if (token.size() < 2 || kStopwords.contains(token)) {
            continue;
        }
        if (seen.insert(token).second) {
            filtered.push_back(token);
        }
    }

    std::vector<std::string> out;
    out.reserve(filtered.size() * 3);
    constexpr size_t kMaxN = 4;
    constexpr size_t kMaxAliases = 96;
    for (size_t n = kMaxN; n >= 2; --n) {
        if (filtered.size() < n) {
            continue;
        }
        for (size_t i = 0; i + n <= filtered.size(); ++i) {
            std::string phrase = filtered[i];
            for (size_t j = 1; j < n; ++j) {
                phrase.push_back(' ');
                phrase += filtered[i + j];
            }
            if (seen.insert("phrase:" + phrase).second) {
                out.push_back(std::move(phrase));
            }
            if (out.size() >= kMaxAliases) {
                return out;
            }
        }
        if (n == 2) {
            break;
        }
    }

    for (const auto& token : filtered) {
        out.push_back(token);
        if (out.size() >= kMaxAliases) {
            break;
        }
    }
    return out;
}

float graphNodeExpansionWeight(const std::optional<std::string>& typeOpt,
                               std::string_view labelView) {
    std::string type;
    if (typeOpt.has_value()) {
        type.reserve(typeOpt->size());
        for (unsigned char uc : *typeOpt) {
            type.push_back(static_cast<char>(std::tolower(uc)));
        }
    }
    const std::string label = normalizeGraphSurface(labelView);
    if (type == "document" || type == "file" || type == "path" || type == "directory" ||
        type == "blob" || type == "text_segment" || type == "date" || type == "time" ||
        type == "duration" || type == "number" || type == "ordinal") {
        return 0.0f;
    }
    if (type == "protein" || type == "gene" || type == "cell" || type == "disease" ||
        type == "drug" || type == "chemical" || type == "pathway" || type == "biological_process" ||
        type == "biomarker") {
        return 1.0f;
    }
    if (type == "location" || type == "person" || type == "organization") {
        return 0.25f;
    }
    if (label == "encoded protein product" || label == "crossover products") {
        return 0.05f;
    }
    return 0.60f;
}

std::vector<GraphExpansionTerm>
generateGraphExpansionTerms(const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
                            const std::string& query, const std::vector<QueryConcept>& concepts,
                            const GraphExpansionConfig& config) {
    if (!kgStore || config.maxTerms == 0) {
        return {};
    }

    std::vector<std::pair<std::string, float>> queryTerms;
    const auto rawQueryTerms = tokenizeKgQuery(query);
    queryTerms.reserve(rawQueryTerms.size() + concepts.size() * 2);
    for (const auto& term : rawQueryTerms) {
        queryTerms.emplace_back(term, 1.0f);
    }
    for (const auto& queryConcept : concepts) {
        if (queryConcept.text.empty() || queryConcept.confidence < 0.35f) {
            continue;
        }
        const std::string normalized = normalizeGraphSurface(queryConcept.text);
        if (normalized.size() >= 3) {
            queryTerms.emplace_back(normalized,
                                    std::clamp(queryConcept.confidence + 0.15f, 0.2f, 1.0f));
        }
        for (const auto& term : tokenizeKgQuery(queryConcept.text)) {
            queryTerms.emplace_back(term, std::clamp(queryConcept.confidence, 0.2f, 1.0f));
        }
    }
    if (queryTerms.empty()) {
        return {};
    }

    struct SeedNode {
        std::int64_t nodeId = 0;
        float score = 0.0f;
    };

    std::unordered_map<std::int64_t, float> seedScores;
    std::unordered_map<std::int64_t, metadata::KGNode> nodeCache;
    nodeCache.reserve(config.maxTerms * 2 + config.maxSeeds);

    const auto getNodeCached = [&](std::int64_t nodeId) -> std::optional<metadata::KGNode> {
        auto it = nodeCache.find(nodeId);
        if (it != nodeCache.end()) {
            return it->second;
        }
        auto nodeRes = kgStore->getNodeById(nodeId);
        if (!nodeRes || !nodeRes.value().has_value()) {
            return std::nullopt;
        }
        nodeCache.emplace(nodeId, *nodeRes.value());
        return *nodeRes.value();
    };

    const size_t aliasesPerTerm = std::max<size_t>(4, config.maxTerms);
    for (const auto& [term, termWeight] : queryTerms) {
        auto exact = kgStore->resolveAliasExact(term, aliasesPerTerm);
        if (exact && !exact.value().empty()) {
            for (const auto& alias : exact.value()) {
                if (auto node = getNodeCached(alias.nodeId); node.has_value()) {
                    const float weighted =
                        termWeight * alias.score *
                        graphNodeExpansionWeight(node->type, node->label.value_or(node->nodeKey));
                    if (weighted > 0.0f) {
                        seedScores[alias.nodeId] = std::max(seedScores[alias.nodeId], weighted);
                    }
                }
            }
            continue;
        }

        auto labels = kgStore->searchNodesByLabel(term, aliasesPerTerm, 0);
        if (labels && !labels.value().empty()) {
            for (const auto& node : labels.value()) {
                const float base = term.find(' ') != std::string::npos ? 0.95f : 0.80f;
                const float weighted =
                    termWeight * base *
                    graphNodeExpansionWeight(node.type, node.label.value_or(node.nodeKey));
                if (weighted > 0.0f) {
                    seedScores[node.id] = std::max(seedScores[node.id], weighted);
                    nodeCache.emplace(node.id, node);
                }
            }
            continue;
        }

        auto fuzzy = kgStore->resolveAliasFuzzy(term, aliasesPerTerm);
        if (fuzzy && !fuzzy.value().empty()) {
            for (const auto& alias : fuzzy.value()) {
                if (auto node = getNodeCached(alias.nodeId); node.has_value()) {
                    const float weighted =
                        termWeight * alias.score * 0.8f *
                        graphNodeExpansionWeight(node->type, node->label.value_or(node->nodeKey));
                    if (weighted > 0.0f) {
                        seedScores[alias.nodeId] = std::max(seedScores[alias.nodeId], weighted);
                    }
                }
            }
        }
    }

    if (seedScores.empty()) {
        return {};
    }

    std::vector<SeedNode> seeds;
    seeds.reserve(seedScores.size());
    for (const auto& [nodeId, score] : seedScores) {
        seeds.push_back({nodeId, score});
    }
    std::stable_sort(seeds.begin(), seeds.end(),
                     [](const auto& a, const auto& b) { return a.score > b.score; });
    if (seeds.size() > config.maxSeeds) {
        seeds.resize(config.maxSeeds);
    }

    std::unordered_map<std::string, float> termScores;
    const auto addTerm = [&](std::string candidate, float score) {
        const std::string normalized = normalizeGraphSurface(candidate);
        if (normalized.size() < 3) {
            return;
        }
        termScores[normalized] = std::max(termScores[normalized], score);
    };

    for (const auto& seed : seeds) {
        if (auto node = getNodeCached(seed.nodeId); node.has_value()) {
            const float nodeWeight =
                graphNodeExpansionWeight(node->type, node->label.value_or(node->nodeKey));
            addTerm(node->label.value_or(node->nodeKey), seed.score * nodeWeight);
        }

        auto edges = kgStore->getEdgesFrom(seed.nodeId, std::nullopt, config.maxNeighbors, 0);
        if (!edges) {
            continue;
        }
        for (const auto& edge : edges.value()) {
            auto neighbor = getNodeCached(edge.dstNodeId);
            if (!neighbor.has_value()) {
                continue;
            }
            const float nodeWeight = graphNodeExpansionWeight(
                neighbor->type, neighbor->label.value_or(neighbor->nodeKey));
            if (nodeWeight <= 0.0f) {
                continue;
            }
            const float relationWeight = relationExpansionWeight(edge);
            addTerm(neighbor->label.value_or(neighbor->nodeKey),
                    seed.score * nodeWeight * relationWeight * std::clamp(edge.weight, 0.1f, 1.0f));
        }
    }

    std::vector<GraphExpansionTerm> out;
    out.reserve(termScores.size());
    for (const auto& [text, score] : termScores) {
        out.push_back({text, score});
    }
    std::stable_sort(out.begin(), out.end(),
                     [](const auto& a, const auto& b) { return a.score > b.score; });
    if (out.size() > config.maxTerms) {
        out.resize(config.maxTerms);
    }
    return out;
}

std::vector<GraphExpansionTerm> generateGraphExpansionTermsFromDocuments(
    const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore, const std::string& query,
    const std::vector<QueryConcept>& concepts, const std::vector<GraphExpansionSeedDoc>& seedDocs,
    const GraphExpansionConfig& config) {
    if (!kgStore || config.maxTerms == 0 || seedDocs.empty()) {
        return {};
    }

    std::vector<std::string> queryAliases = tokenizeKgQuery(query);
    queryAliases.reserve(queryAliases.size() + concepts.size() * 2);
    for (const auto& qc : concepts) {
        if (qc.text.empty() || qc.confidence < 0.35f) {
            continue;
        }
        const std::string normalized = normalizeGraphSurface(qc.text);
        if (normalized.size() >= 3) {
            queryAliases.push_back(normalized);
        }
        const auto conceptTokens = tokenizeKgQuery(qc.text);
        queryAliases.insert(queryAliases.end(), conceptTokens.begin(), conceptTokens.end());
    }

    std::vector<GraphExpansionSeedDoc> docs = seedDocs;
    std::stable_sort(docs.begin(), docs.end(),
                     [](const auto& a, const auto& b) { return a.score > b.score; });
    if (docs.size() > config.maxSeeds) {
        docs.resize(config.maxSeeds);
    }

    std::unordered_map<std::int64_t, metadata::KGNode> nodeCache;
    nodeCache.reserve(config.maxTerms * 2 + docs.size() * 4);

    const auto getNodeCached = [&](std::int64_t nodeId) -> std::optional<metadata::KGNode> {
        auto it = nodeCache.find(nodeId);
        if (it != nodeCache.end()) {
            return it->second;
        }
        auto nodeRes = kgStore->getNodeById(nodeId);
        if (!nodeRes || !nodeRes.value().has_value()) {
            return std::nullopt;
        }
        nodeCache.emplace(nodeId, *nodeRes.value());
        return *nodeRes.value();
    };

    std::unordered_map<std::string, float> termScores;
    const auto addTerm = [&](std::string candidate, float score) {
        const std::string normalized = normalizeGraphSurface(candidate);
        if (normalized.size() < 3) {
            return;
        }
        const float queryMatch = surfaceQueryMatchScore(queryAliases, normalized);
        if (queryMatch <= 0.0f) {
            return;
        }
        termScores[normalized] =
            std::max(termScores[normalized], score * std::clamp(queryMatch, 0.2f, 1.0f));
    };

    for (const auto& doc : docs) {
        auto docId = resolveKgDocumentId(kgStore, doc);
        if (!docId.has_value()) {
            continue;
        }
        std::optional<std::int64_t> docNodeId;
        auto docNode = kgStore->getNodeByKey("doc:" + doc.documentHash);
        if (docNode && docNode.value().has_value()) {
            docNodeId = docNode.value()->id;
        }
        auto ents = kgStore->getDocEntitiesForDocument(*docId, 128, 0);
        if (!ents) {
            continue;
        }

        for (const auto& ent : ents.value()) {
            const float confidence = ent.confidence.value_or(0.5f);
            if (confidence < 0.35f) {
                continue;
            }
            if (!ent.nodeId.has_value()) {
                continue;
            }

            auto node = getNodeCached(*ent.nodeId);
            if (!node.has_value()) {
                continue;
            }
            const float nodeWeight =
                graphNodeExpansionWeight(node->type, node->label.value_or(node->nodeKey));
            if (nodeWeight <= 0.0f) {
                continue;
            }

            auto edges = kgStore->getEdgesFrom(*ent.nodeId, std::nullopt,
                                               std::max<std::size_t>(config.maxNeighbors, 16), 0);
            if (!edges) {
                continue;
            }

            float anchorBoost = 0.0f;
            bool anchoredToTitleOrPrimary = false;
            bool anchoredToBodyClaim = false;
            for (const auto& edge : edges.value()) {
                if (docNodeId.has_value() && edge.dstNodeId == *docNodeId) {
                    if (edge.relation == "primary_topic_of") {
                        anchoredToTitleOrPrimary = true;
                        anchorBoost = std::max(anchorBoost, 1.30f);
                    } else if (edge.relation == "title_mentions") {
                        anchoredToTitleOrPrimary = true;
                        anchorBoost = std::max(anchorBoost, 1.20f);
                    }
                }
                if (edge.relation == "mentioned_in_segment" && edge.properties.has_value()) {
                    try {
                        auto props = nlohmann::json::parse(*edge.properties);
                        const std::string region = props.value("region", "");
                        if (region == "body_claim") {
                            anchoredToBodyClaim = true;
                            anchorBoost = std::max(anchorBoost, 1.15f);
                        } else if (region == "title") {
                            anchoredToTitleOrPrimary = true;
                            anchorBoost = std::max(anchorBoost, 1.20f);
                        }
                    } catch (...) {
                    }
                }
            }
            if (!(anchoredToTitleOrPrimary || anchoredToBodyClaim)) {
                continue;
            }

            const float baseScore =
                std::clamp(doc.score, 0.1f, 1.0f) * confidence * nodeWeight * anchorBoost;
            addTerm(node->label.value_or(node->nodeKey), baseScore);

            for (const auto& edge : edges.value()) {
                auto neighbor = getNodeCached(edge.dstNodeId);
                if (!neighbor.has_value()) {
                    continue;
                }
                const float neighborWeight = graphNodeExpansionWeight(
                    neighbor->type, neighbor->label.value_or(neighbor->nodeKey));
                if (neighborWeight <= 0.0f) {
                    continue;
                }
                addTerm(neighbor->label.value_or(neighbor->nodeKey),
                        baseScore * neighborWeight * relationExpansionWeight(edge) *
                            std::clamp(edge.weight, 0.1f, 1.0f));
            }
        }

        if (docNode && docNode.value().has_value()) {
            auto semanticEdges = kgStore->getEdgesFrom(
                docNode.value()->id, std::string_view("semantic_neighbor"), config.maxNeighbors, 0);
            if (semanticEdges) {
                for (const auto& edge : semanticEdges.value()) {
                    auto neighborNode = getNodeCached(edge.dstNodeId);
                    if (!neighborNode.has_value()) {
                        continue;
                    }
                    auto neighborHash = documentHashFromDocNodeKey(neighborNode->nodeKey);
                    if (!neighborHash.has_value()) {
                        continue;
                    }
                    GraphExpansionSeedDoc neighborDoc{
                        .documentHash = *neighborHash,
                        .filePath = {},
                        .score = std::clamp(doc.score * edge.weight * 0.85f, 0.1f, 1.0f)};
                    auto neighborDocId = resolveKgDocumentId(kgStore, neighborDoc);
                    if (!neighborDocId.has_value()) {
                        continue;
                    }
                    auto neighborEnts = kgStore->getDocEntitiesForDocument(*neighborDocId, 64, 0);
                    if (!neighborEnts) {
                        continue;
                    }
                    for (const auto& ent : neighborEnts.value()) {
                        if (!ent.nodeId.has_value()) {
                            continue;
                        }
                        auto node = getNodeCached(*ent.nodeId);
                        if (!node.has_value()) {
                            continue;
                        }
                        const float nodeWeight = graphNodeExpansionWeight(
                            node->type, node->label.value_or(node->nodeKey));
                        if (nodeWeight <= 0.0f) {
                            continue;
                        }
                        const float confidence = ent.confidence.value_or(0.5f);
                        auto neighborDocNode =
                            kgStore->getNodeByKey("doc:" + neighborDoc.documentHash);
                        std::optional<std::int64_t> neighborDocNodeId;
                        if (neighborDocNode && neighborDocNode.value().has_value()) {
                            neighborDocNodeId = neighborDocNode.value()->id;
                        }
                        auto entEdges = kgStore->getEdgesFrom(
                            *ent.nodeId, std::nullopt,
                            std::max<std::size_t>(config.maxNeighbors, 16), 0);
                        if (!entEdges) {
                            continue;
                        }
                        float neighborAnchorBoost = 0.0f;
                        bool neighborAnchored = false;
                        for (const auto& edge : entEdges.value()) {
                            if (neighborDocNodeId.has_value() &&
                                edge.dstNodeId == *neighborDocNodeId &&
                                (edge.relation == "primary_topic_of" ||
                                 edge.relation == "title_mentions")) {
                                neighborAnchored = true;
                                neighborAnchorBoost =
                                    std::max(neighborAnchorBoost,
                                             edge.relation == "primary_topic_of" ? 1.30f : 1.20f);
                            }
                            if (edge.relation == "mentioned_in_segment" &&
                                edge.properties.has_value()) {
                                try {
                                    auto props = nlohmann::json::parse(*edge.properties);
                                    const std::string region = props.value("region", "");
                                    if (region == "body_claim" || region == "title") {
                                        neighborAnchored = true;
                                        neighborAnchorBoost = std::max(
                                            neighborAnchorBoost, region == "title" ? 1.20f : 1.15f);
                                    }
                                } catch (...) {
                                }
                            }
                        }
                        if (!neighborAnchored) {
                            continue;
                        }
                        addTerm(node->label.value_or(node->nodeKey),
                                neighborDoc.score * nodeWeight * confidence * neighborAnchorBoost);
                    }
                }
            }
        }
    }

    std::vector<GraphExpansionTerm> out;
    out.reserve(termScores.size());
    for (const auto& [text, score] : termScores) {
        out.push_back({text, score});
    }
    std::stable_sort(out.begin(), out.end(),
                     [](const auto& a, const auto& b) { return a.score > b.score; });
    if (out.size() > config.maxTerms) {
        out.resize(config.maxTerms);
    }
    return out;
}

} // namespace yams::search
