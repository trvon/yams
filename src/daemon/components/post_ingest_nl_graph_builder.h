#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/search/query_concept_extractor.h>

namespace yams::daemon {

struct PostIngestNlGraphContext {
    std::string hash;
    std::int64_t documentId{-1};
    std::string textSnippet;
    std::string fallbackTitle;
    std::string filePath;
    std::string language;
    std::optional<float> titleConfidence;
    std::int64_t lastSeen{0};
};

struct PostIngestNlGraphMetricDeltas {
    std::uint64_t segmentNodesCreated{0};
    std::uint64_t segmentEdgesCreated{0};
    std::uint64_t entitySegmentEdgesCreated{0};
    std::uint64_t bodySegmentNodesCreated{0};
    std::uint64_t bodyEntitySegmentEdgesCreated{0};
    std::uint64_t deferredDocEntitiesQueued{0};
    std::uint64_t entities{0};
    std::uint64_t highValueEntities{0};
    std::uint64_t coMentionEdges{0};
    std::uint64_t primaryTopicEdges{0};
};

struct PostIngestNlGraphBuildResult {
    std::unique_ptr<DeferredKGBatch> batch;
    PostIngestNlGraphMetricDeltas metrics;
};

/// Build the deferred natural-language graph operations for one immutable document snapshot.
/// Concepts are accepted by value so the builder owns their storage independently of inference.
[[nodiscard]] PostIngestNlGraphBuildResult
buildPostIngestNlGraph(const PostIngestNlGraphContext& context,
                       std::vector<search::QueryConcept> concepts);

} // namespace yams::daemon
