#include "post_ingest_nl_graph_builder.h"

#include <algorithm>
#include <filesystem>
#include <string_view>
#include <unordered_set>
#include <utility>

#include <nlohmann/json.hpp>

#include <yams/common/utf8_utils.h>
#include <yams/metadata/path_utils.h>
#include <yams/search/query_text_utils.h>

namespace yams::daemon {
namespace {

std::string normalizeGraphPath(const std::string& path) {
    if (path.empty()) {
        return {};
    }
    try {
        auto derived = metadata::computePathDerivedValues(path);
        if (!derived.normalizedPath.empty()) {
            return derived.normalizedPath;
        }
    } catch (const std::exception&) {
        return path;
    }
    return path;
}

std::string normalizeEntityTextForKey(std::string_view text) {
    return search::normalizeEntityTextForKey(text);
}

std::string canonicalizeNlEntityType(std::string_view rawType, std::string_view rawText) {
    return search::canonicalizeEntityType(rawType, rawText);
}

bool isHighValueGraphType(std::string_view type) {
    return type == "protein" || type == "gene" || type == "cell" || type == "disease" ||
           type == "chemical" || type == "drug" || type == "pathway" ||
           type == "biological_process" || type == "biomarker" || type == "anatomy" ||
           type == "organism";
}

bool entityTextOverlapsTitle(std::string_view entityText, std::string_view titleText) {
    const std::string normalizedEntity = normalizeEntityTextForKey(entityText);
    const std::string normalizedTitle = normalizeEntityTextForKey(titleText);
    if (normalizedEntity.empty() || normalizedTitle.empty()) {
        return false;
    }
    return normalizedTitle.find(normalizedEntity) != std::string::npos ||
           (normalizedEntity.size() >= 4 &&
            normalizedEntity.find(normalizedTitle) != std::string::npos);
}

struct TextSegmentWindow {
    std::string text;
    std::size_t startOffset{0};
    std::size_t endOffset{0};
};

std::vector<TextSegmentWindow> buildBodyClaimSegments(std::string_view textSnippet,
                                                      std::string_view titleText,
                                                      std::size_t maxSegments = 3) {
    std::vector<TextSegmentWindow> segments;
    if (textSnippet.empty() || maxSegments == 0) {
        return segments;
    }

    std::size_t start = 0;
    if (!titleText.empty()) {
        const std::string normalizedTitle = normalizeEntityTextForKey(titleText);
        const auto newline = textSnippet.find('\n');
        if (newline != std::string_view::npos) {
            const std::string firstLine = normalizeEntityTextForKey(textSnippet.substr(0, newline));
            if (!firstLine.empty() && firstLine == normalizedTitle) {
                start = newline + 1;
            }
        }
    }

    auto flushSegment = [&](std::size_t segmentStart, std::size_t segmentEnd) {
        if (segmentEnd <= segmentStart) {
            return;
        }
        auto raw = std::string(textSnippet.substr(segmentStart, segmentEnd - segmentStart));
        auto cleaned = search::trimAndCollapseWhitespace(raw);
        if (cleaned.size() < 24) {
            return;
        }
        segments.push_back({std::move(cleaned), segmentStart, segmentEnd});
    };

    std::size_t sentenceStart = start;
    std::size_t sentenceCount = 0;
    for (std::size_t i = start; i < textSnippet.size() && segments.size() < maxSegments; ++i) {
        const char c = textSnippet[i];
        const bool boundary = c == '.' || c == '!' || c == '?' || c == '\n';
        if (!boundary) {
            continue;
        }
        ++sentenceCount;
        if (sentenceCount >= 2 || c == '\n') {
            flushSegment(sentenceStart, i + 1);
            sentenceStart = i + 1;
            sentenceCount = 0;
        }
    }
    if (segments.size() < maxSegments) {
        flushSegment(sentenceStart, textSnippet.size());
    }
    return segments;
}

std::string coOccurrenceRelation(std::string_view lhsType, std::string_view rhsType) {
    const bool lhsBio = isHighValueGraphType(lhsType);
    const bool rhsBio = isHighValueGraphType(rhsType);
    if (lhsBio && rhsBio) {
        return "co_occurs_biomedical";
    }
    if ((lhsType == "protein" && rhsType == "cell") ||
        (lhsType == "cell" && rhsType == "protein")) {
        return "protein_cell_association";
    }
    if ((lhsType == "protein" && rhsType == "disease") ||
        (lhsType == "disease" && rhsType == "protein")) {
        return "protein_disease_association";
    }
    if ((lhsType == "drug" && rhsType == "disease") ||
        (lhsType == "disease" && rhsType == "drug")) {
        return "drug_disease_association";
    }
    return "co_mentioned_with";
}

struct AliasVariant {
    std::string text;
    float confidence{1.0f};
    std::string sourceTag;
};

std::vector<AliasVariant> buildAliasVariants(const std::string& entityText,
                                             const std::string& entityType, float baseConfidence) {
    std::vector<AliasVariant> variants;
    std::unordered_set<std::string> seen;
    const auto kind = search::surfaceVariantKindForEntityType(entityType);

    auto addVariant = [&](const std::string& value, float confidenceScale, std::string sourceTag) {
        std::string normalized = normalizeEntityTextForKey(value);
        if (normalized.size() < 2 || !seen.insert(normalized).second) {
            return;
        }
        const float confidence = std::clamp(baseConfidence * confidenceScale, 0.05f, 1.0f);
        variants.push_back({std::move(normalized), confidence, std::move(sourceTag)});
    };

    auto addGeneratedVariants = [&](const std::string& value, float primaryScale,
                                    float secondaryScale, std::string primarySource,
                                    std::string secondarySource) {
        auto generated = search::generateSurfaceVariants(value, kind, 8);
        for (std::size_t i = 0; i < generated.size() && variants.size() < 8; ++i) {
            addVariant(generated[i], i == 0 ? primaryScale : secondaryScale,
                       i == 0 ? primarySource : secondarySource);
        }
    };

    addGeneratedVariants(entityText, 1.0f, 0.72f, "surface", "variant");
    if (!entityType.empty()) {
        addGeneratedVariants(entityType + " " + entityText, 0.95f, 0.68f, "type_qualified",
                             "type_qualified");
    }
    return variants;
}

struct SegmentRef {
    std::string nodeKey;
    std::size_t startOffset{0};
    std::size_t endOffset{0};
};

struct EntityRef {
    std::string nodeKey;
    std::string type;
    float confidence{0.0f};
    bool titleOverlap{false};
    bool highValue{false};
    int segmentIndex{-1};
};

} // namespace

PostIngestNlGraphBuildResult buildPostIngestNlGraph(const PostIngestNlGraphContext& context,
                                                    std::vector<search::QueryConcept> concepts) {
    PostIngestNlGraphBuildResult result;
    result.batch = std::make_unique<DeferredKGBatch>();
    auto& batch = *result.batch;
    auto& metrics = result.metrics;

    batch.nodes.reserve(concepts.size() + 6);
    batch.deferredEdges.reserve(concepts.size() * 4 + 8);
    batch.aliases.reserve(concepts.size() * 3);
    const std::string normalizedFilePath = normalizeGraphPath(context.filePath);
    batch.sourceFile = normalizedFilePath.empty() ? context.filePath : normalizedFilePath;
    metrics.entities = concepts.size();

    std::optional<std::int64_t> documentDbId;
    if (!context.hash.empty() && context.documentId >= 0) {
        documentDbId = context.documentId;
        batch.documentIdToDelete = documentDbId;
    }

    std::string docNodeKey;
    if (!context.hash.empty()) {
        docNodeKey = "doc:" + context.hash;
        metadata::KGNode docNode;
        docNode.nodeKey = docNodeKey;
        docNode.label = common::sanitizeUtf8(context.filePath);
        docNode.type = "document";
        nlohmann::json properties;
        properties["hash"] = context.hash;
        properties["path"] = common::sanitizeUtf8(batch.sourceFile);
        properties["language"] = common::sanitizeUtf8(context.language);
        docNode.properties = properties.dump();
        batch.nodes.push_back(std::move(docNode));
    }

    std::string fileNodeKey;
    if (!context.filePath.empty()) {
        fileNodeKey = "path:file:" + normalizeGraphPath(batch.sourceFile);
        metadata::KGNode fileNode;
        fileNode.nodeKey = fileNodeKey;
        fileNode.label = common::sanitizeUtf8(batch.sourceFile);
        fileNode.type = "file";
        nlohmann::json properties;
        properties["path"] = common::sanitizeUtf8(batch.sourceFile);
        properties["language"] = common::sanitizeUtf8(context.language);
        if (!batch.sourceFile.empty()) {
            properties["basename"] =
                common::sanitizeUtf8(std::filesystem::path(batch.sourceFile).filename().string());
        }
        if (!context.hash.empty()) {
            properties["current_hash"] = context.hash;
        }
        fileNode.properties = properties.dump();
        batch.nodes.push_back(std::move(fileNode));
    }

    const std::string targetNodeKey = !docNodeKey.empty() ? docNodeKey : fileNodeKey;
    const std::string effectiveTitle =
        !context.fallbackTitle.empty() ? context.fallbackTitle : context.filePath;
    std::string titleSegmentNodeKey;
    std::string summarySegmentNodeKey;
    std::vector<SegmentRef> bodySegments;

    const auto addSegmentNode = [&](const std::string& nodeKey, const std::string& label,
                                    const std::string& region, float confidence,
                                    std::size_t startOffset, std::size_t endOffset) {
        if (nodeKey.empty() || label.empty()) {
            return;
        }
        metadata::KGNode node;
        node.nodeKey = nodeKey;
        node.label = common::sanitizeUtf8(label);
        node.type = "text_segment";
        nlohmann::json properties{
            {"segment_type", "text_segment"}, {"region", region},
            {"confidence", confidence},       {"start_offset", startOffset},
            {"end_offset", endOffset},        {"path", common::sanitizeUtf8(batch.sourceFile)},
        };
        if (!context.hash.empty()) {
            properties["snapshot_id"] = context.hash;
        }
        node.properties = properties.dump();
        batch.nodes.push_back(std::move(node));
        ++metrics.segmentNodesCreated;
        if (region == "body_claim") {
            ++metrics.bodySegmentNodesCreated;
        }

        if (targetNodeKey.empty()) {
            return;
        }
        DeferredEdge contains;
        contains.srcNodeKey = targetNodeKey;
        contains.dstNodeKey = nodeKey;
        contains.relation = "contains_segment";
        contains.weight = confidence;
        contains.properties =
            nlohmann::json{{"source", "gliner"}, {"region", region}, {"confidence", confidence}}
                .dump();
        batch.deferredEdges.push_back(std::move(contains));

        DeferredEdge segmentOf;
        segmentOf.srcNodeKey = nodeKey;
        segmentOf.dstNodeKey = targetNodeKey;
        segmentOf.relation = "segment_of";
        segmentOf.weight = confidence;
        segmentOf.properties =
            nlohmann::json{{"source", "gliner"}, {"region", region}, {"confidence", confidence}}
                .dump();
        batch.deferredEdges.push_back(std::move(segmentOf));
        metrics.segmentEdgesCreated += 2;
    };

    if (!context.hash.empty() && !effectiveTitle.empty()) {
        titleSegmentNodeKey = "segment:title:" + context.hash;
        const float confidence = context.titleConfidence.has_value()
                                     ? std::clamp(*context.titleConfidence, 0.5f, 1.0f)
                                     : 0.75f;
        addSegmentNode(titleSegmentNodeKey, effectiveTitle, "title", confidence, 0,
                       effectiveTitle.size());
    }
    if (!context.hash.empty() && !context.textSnippet.empty()) {
        summarySegmentNodeKey = "segment:summary:" + context.hash;
        addSegmentNode(summarySegmentNodeKey, context.textSnippet, "summary", 0.65f, 0,
                       context.textSnippet.size());
        const auto claimSegments = buildBodyClaimSegments(context.textSnippet, effectiveTitle, 3);
        for (std::size_t i = 0; i < claimSegments.size(); ++i) {
            const std::string nodeKey =
                "segment:body:" + context.hash + ":" + std::to_string(i + 1);
            addSegmentNode(nodeKey, claimSegments[i].text, "body_claim", 0.60f,
                           claimSegments[i].startOffset, claimSegments[i].endOffset);
            bodySegments.push_back(
                {nodeKey, claimSegments[i].startOffset, claimSegments[i].endOffset});
        }
    }

    std::vector<EntityRef> entityRefs;
    entityRefs.reserve(concepts.size());
    for (const auto& candidate : concepts) {
        const std::string text = common::sanitizeUtf8(candidate.text);
        const std::string type =
            common::sanitizeUtf8(canonicalizeNlEntityType(candidate.type, candidate.text));
        const std::string nodeKey = "nl_entity:" + type + ":" + normalizeEntityTextForKey(text);

        metadata::KGNode node;
        node.nodeKey = nodeKey;
        node.label = text;
        node.type = type;
        nlohmann::json properties{{"entity_text", text},
                                  {"entity_type", type},
                                  {"confidence", candidate.confidence},
                                  {"first_seen_file", common::sanitizeUtf8(context.filePath)},
                                  {"last_seen", context.lastSeen}};
        if (!context.hash.empty()) {
            properties["first_seen_hash"] = context.hash;
        }
        node.properties = properties.dump();
        batch.nodes.push_back(std::move(node));

        const bool titleOverlap = entityTextOverlapsTitle(text, effectiveTitle);
        const bool highValue = isHighValueGraphType(type);
        int segmentIndex = -1;
        if (!titleOverlap) {
            for (std::size_t i = 0; i < bodySegments.size(); ++i) {
                if (candidate.startOffset < bodySegments[i].endOffset &&
                    candidate.endOffset > bodySegments[i].startOffset) {
                    segmentIndex = static_cast<int>(i);
                    break;
                }
            }
        }
        entityRefs.push_back(
            {nodeKey, type, candidate.confidence, titleOverlap, highValue, segmentIndex});
        metrics.highValueEntities += highValue ? 1 : 0;

        for (const auto& variant : buildAliasVariants(text, type, candidate.confidence)) {
            metadata::KGAlias alias;
            alias.alias = variant.text;
            alias.source = "gliner." + variant.sourceTag + "|" + nodeKey;
            alias.confidence = variant.confidence;
            batch.aliases.push_back(std::move(alias));
        }

        if (!targetNodeKey.empty()) {
            DeferredEdge mentioned;
            mentioned.srcNodeKey = nodeKey;
            mentioned.dstNodeKey = targetNodeKey;
            mentioned.relation = "mentioned_in";
            mentioned.weight = candidate.confidence;
            nlohmann::json edgeProperties{
                {"source", "gliner"},
                {"confidence", candidate.confidence},
                {"provenance",
                 nlohmann::json{{"source", "gliner"}, {"confidence", candidate.confidence}}},
            };
            if (!context.hash.empty()) {
                edgeProperties["snapshot_id"] = context.hash;
            }
            mentioned.properties = edgeProperties.dump();
            batch.deferredEdges.push_back(std::move(mentioned));

            if (titleOverlap) {
                DeferredEdge titleMention;
                titleMention.srcNodeKey = nodeKey;
                titleMention.dstNodeKey = targetNodeKey;
                titleMention.relation = "title_mentions";
                titleMention.weight = std::min(1.0f, candidate.confidence * 1.15f);
                nlohmann::json titleProperties{
                    {"source", "gliner"}, {"region", "title"}, {"confidence", titleMention.weight}};
                if (!context.hash.empty()) {
                    titleProperties["snapshot_id"] = context.hash;
                }
                titleMention.properties = titleProperties.dump();
                batch.deferredEdges.push_back(std::move(titleMention));
            }
        }

        DeferredEdge segmentMention;
        bool addSegmentMention = false;
        segmentMention.srcNodeKey = nodeKey;
        segmentMention.relation = "mentioned_in_segment";
        nlohmann::json segmentProperties;
        if (titleOverlap && !titleSegmentNodeKey.empty()) {
            segmentMention.dstNodeKey = titleSegmentNodeKey;
            segmentMention.weight = std::min(1.0f, candidate.confidence * 1.10f);
            segmentProperties = {
                {"source", "gliner"}, {"region", "title"}, {"confidence", segmentMention.weight}};
            addSegmentMention = true;
        } else if (segmentIndex >= 0 &&
                   static_cast<std::size_t>(segmentIndex) < bodySegments.size()) {
            segmentMention.dstNodeKey = bodySegments[segmentIndex].nodeKey;
            segmentMention.weight = std::min(1.0f, candidate.confidence * 1.05f);
            segmentProperties = {{"source", "gliner"},
                                 {"region", "body_claim"},
                                 {"confidence", segmentMention.weight},
                                 {"segment_index", segmentIndex}};
            addSegmentMention = true;
            ++metrics.bodyEntitySegmentEdgesCreated;
        } else if (!summarySegmentNodeKey.empty()) {
            segmentMention.dstNodeKey = summarySegmentNodeKey;
            segmentMention.weight = candidate.confidence;
            segmentProperties = {
                {"source", "gliner"}, {"region", "summary"}, {"confidence", candidate.confidence}};
            addSegmentMention = true;
        }
        if (addSegmentMention) {
            segmentMention.properties = segmentProperties.dump();
            batch.deferredEdges.push_back(std::move(segmentMention));
            ++metrics.entitySegmentEdgesCreated;
        }

        if (documentDbId.has_value()) {
            DeferredDocEntity entity;
            entity.documentId = *documentDbId;
            entity.entityText = text;
            entity.nodeKey = nodeKey;
            entity.startOffset = candidate.startOffset;
            entity.endOffset = candidate.endOffset;
            entity.confidence = candidate.confidence;
            entity.extractor = "gliner_title_nl";
            batch.deferredDocEntities.push_back(std::move(entity));
            ++metrics.deferredDocEntitiesQueued;
        }
    }

    std::stable_sort(entityRefs.begin(), entityRefs.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.titleOverlap != rhs.titleOverlap) {
            return lhs.titleOverlap > rhs.titleOverlap;
        }
        if (lhs.highValue != rhs.highValue) {
            return lhs.highValue > rhs.highValue;
        }
        return lhs.confidence > rhs.confidence;
    });

    constexpr std::size_t kMaxPrimaryTopicEdges = 3;
    for (const auto& ref : entityRefs) {
        if (metrics.primaryTopicEdges >= kMaxPrimaryTopicEdges) {
            break;
        }
        if (!ref.highValue || (!ref.titleOverlap && ref.confidence < 0.78f)) {
            continue;
        }
        if (targetNodeKey.empty()) {
            break;
        }
        DeferredEdge edge;
        edge.srcNodeKey = ref.nodeKey;
        edge.dstNodeKey = targetNodeKey;
        edge.relation = "primary_topic_of";
        edge.weight = std::min(1.0f, ref.confidence * (ref.titleOverlap ? 1.20f : 1.05f));
        nlohmann::json properties{{"source", "gliner"},
                                  {"confidence", edge.weight},
                                  {"title_overlap", ref.titleOverlap},
                                  {"entity_type", ref.type}};
        if (!context.hash.empty()) {
            properties["snapshot_id"] = context.hash;
        }
        edge.properties = properties.dump();
        batch.deferredEdges.push_back(std::move(edge));
        ++metrics.primaryTopicEdges;
    }

    constexpr std::size_t kMaxCoMentionEntities = 8;
    constexpr std::size_t kMaxCoMentionEdges = 16;
    const std::size_t entityLimit = std::min(entityRefs.size(), kMaxCoMentionEntities);
    for (std::size_t i = 0; i < entityLimit && metrics.coMentionEdges < kMaxCoMentionEdges; ++i) {
        for (std::size_t j = i + 1; j < entityLimit && metrics.coMentionEdges < kMaxCoMentionEdges;
             ++j) {
            if (!entityRefs[i].highValue && !entityRefs[j].highValue) {
                continue;
            }
            DeferredEdge edge;
            edge.srcNodeKey = entityRefs[i].nodeKey;
            edge.dstNodeKey = entityRefs[j].nodeKey;
            edge.relation = coOccurrenceRelation(entityRefs[i].type, entityRefs[j].type);
            const bool bothHighValue = entityRefs[i].highValue && entityRefs[j].highValue;
            const float confidenceFloor = bothHighValue ? 0.20f : 0.08f;
            edge.weight = std::max(
                confidenceFloor,
                std::min(entityRefs[i].confidence, entityRefs[j].confidence) *
                    ((entityRefs[i].titleOverlap || entityRefs[j].titleOverlap) ? 0.85f : 0.65f));
            nlohmann::json properties{
                {"source", "gliner"},
                {"confidence", edge.weight},
                {"lhs_type", entityRefs[i].type},
                {"rhs_type", entityRefs[j].type},
                {"title_overlap_pair", entityRefs[i].titleOverlap || entityRefs[j].titleOverlap},
                {"provenance", nlohmann::json{{"source", "gliner"}, {"confidence", edge.weight}}},
            };
            properties["scope"] =
                entityRefs[i].titleOverlap && entityRefs[j].titleOverlap
                    ? "title"
                    : (entityRefs[i].segmentIndex >= 0 &&
                               entityRefs[i].segmentIndex == entityRefs[j].segmentIndex
                           ? "body_segment"
                           : (entityRefs[i].titleOverlap || entityRefs[j].titleOverlap
                                  ? "mixed"
                                  : "summary"));
            if (entityRefs[i].segmentIndex >= 0 &&
                entityRefs[i].segmentIndex == entityRefs[j].segmentIndex) {
                properties["segment_index"] = entityRefs[i].segmentIndex;
            }
            if (!context.hash.empty()) {
                properties["snapshot_id"] = context.hash;
            }
            edge.properties = properties.dump();
            batch.deferredEdges.push_back(std::move(edge));
            ++metrics.coMentionEdges;
        }
    }

    return result;
}

} // namespace yams::daemon
