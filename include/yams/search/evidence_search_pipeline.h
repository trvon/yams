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
#include <unordered_set>
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
/// Optional query-conditioned ordering for an already identity-eligible topology rescue set.
/// Higher scores are preferred. An empty map preserves ordinary fused-score order.
using TopologyRescuePriorityByIdentity = std::unordered_map<std::string, float>;

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
    /// Candidates beyond the ordinary fusion window (effectiveLimit) that were additively kept
    /// because they were present in the caller-supplied topology-rescue-eligible identity set and
    /// had received actual topology evidence. Always 0 when config.topologyFusionRescueSlots is 0.
    std::size_t topologyFusionRescuedCandidates{0};
    /// Diagnostic: tail candidates (beyond effectiveLimit) present in the eligibility set,
    /// regardless of whether they carried topology evidence. Compare against
    /// topologyFusionRescuedCandidates to distinguish "nothing eligible in the tail" from
    /// "eligible but never received topology evidence". This observes the complete tail and is not
    /// bounded by topologyFusionRescueSlots.
    std::size_t topologyFusionRescueIdentityMatchesInTail{0};
    /// Diagnostic: of those identity matches, how many also carried actual topology evidence
    /// (topologyScore.has_value()) and were therefore rescued.
    std::size_t topologyFusionRescueIdentityMatchesWithEvidenceInTail{0};
    /// Complete identity-eligible tail in effective rescue-selector order. This is fused-score
    /// order for the identity baseline and query-score order when a typed priority map is present;
    /// topologyFusionRescueSlots still bounds returned candidates.
    std::vector<std::string> topologyFusionRescueEligibleIdentitiesInTail;
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
    /// topologyRescueEligibleHashes: document/candidate identities (hash, or file path when hash
    /// is empty -- matching CandidateEvidence::documentHash) permitted to additively rescue past
    /// the fusion-window boundary. Built by the caller from
    /// detail::CandidateRescueMergeResult::novelDocumentHashes; empty by default (mechanism
    /// disabled unless the caller opts in). A candidate must also have actually received topology
    /// evidence (SearchResult::topologyScore.has_value()) to be rescued.
    [[nodiscard]] Result<EvidencePipelineResult>
    execute(std::span<const ComponentResult> components,
            const TopologyEvidenceByCandidate& topologyEvidence, const SearchEngineConfig& config,
            std::size_t limit,
            const std::unordered_set<std::string>& topologyRescueEligibleHashes = {},
            const TopologyRescuePriorityByIdentity& topologyRescuePriorities = {},
            const EvidenceReranker& reranker = {}) const;
};

} // namespace yams::search
