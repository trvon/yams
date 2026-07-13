// Copyright 2026 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <yams/core/types.h>
#include <yams/search/search_models.h>
#include <yams/search/topology_routing_session.h>

#include <cstddef>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::search {

/// Best query-local evidence retained for one source after chunk/document deduplication.
struct SourceCandidateEvidence {
    ComponentResult::Source source{ComponentResult::Source::Unknown};
    float bestRawScore{0.0F};
    std::size_t bestRank{0};
    std::size_t occurrences{0};
};

/// Query-conditioned topology evidence for an existing candidate.
///
/// The producer owns the topology math. The pipeline only applies the bounded, confidence-scaled
/// score adjustment; it never admits or removes a candidate from this record.
struct TopologyCandidateEvidence {
    float scoreAdjustment{0.0F};
    float confidence{0.0F};
    float localSupport{0.0F};
    float boundarySupport{0.0F};
    float scaleAgreement{0.0F};
    float overlapSupport{0.0F};
    float persistenceSupport{0.0F};
    float cohesionSupport{0.0F};
    float bridgeSupport{0.0F};
    float densitySupport{0.0F};
    float densityPenalty{0.0F};
};

using TopologyEvidenceByCandidate = std::unordered_map<std::string, TopologyCandidateEvidence>;

/// Convert a routed membership snapshot into bounded evidence for candidates already produced by
/// the ordinary retrieval legs. Abstained or empty routes produce no evidence.
[[nodiscard]] TopologyEvidenceByCandidate
deriveTopologyCandidateEvidence(const TopologyRoutingSessionResult& route,
                                float maxScoreAdjustment);

/// Typed evidence accumulated for one document before fusion.
struct CandidateEvidence {
    std::string candidateId;
    std::string documentHash;
    std::string filePath;
    std::string snippet;
    std::vector<SourceCandidateEvidence> sources;
    std::optional<TopologyCandidateEvidence> topology;
    bool hasLexicalAnchor{false};
    bool hasVectorEvidence{false};
};

struct EvidencePipelineTrace {
    std::size_t inputComponents{0};
    std::size_t invalidComponents{0};
    std::size_t aggregatedCandidates{0};
    std::size_t topologyAnnotatedCandidates{0};
    std::size_t fusedCandidates{0};
    std::size_t rerankedCandidates{0};
    std::size_t finalCandidates{0};
};

struct EvidencePipelineResult {
    std::vector<yams::metadata::SearchResult> results;
    std::vector<CandidateEvidence> evidence;
    EvidencePipelineTrace trace;
};

using EvidenceReranker =
    std::function<Result<void>(std::vector<yams::metadata::SearchResult>& results)>;

/// Pure candidate pipeline implementing the formulation boundary:
/// component candidates -> typed evidence -> soft fusion -> no-invent rerank -> top-k.
///
/// Candidate generation stays outside this class. This keeps the search engine a stable reference
/// while the new ranking formulation can be tested and benchmarked against identical candidate
/// pools.
class EvidenceSearchPipeline {
public:
    [[nodiscard]] Result<EvidencePipelineResult>
    execute(std::span<const ComponentResult> components,
            const TopologyEvidenceByCandidate& topologyEvidence, const SearchEngineConfig& config,
            std::size_t limit, const EvidenceReranker& reranker = {}) const;
};

} // namespace yams::search
