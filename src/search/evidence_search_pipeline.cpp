// Copyright 2026 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/search/evidence_search_pipeline.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <unordered_map>

namespace yams::search {

namespace {

std::string candidateId(const ComponentResult& component) {
    if (!component.documentHash.empty()) {
        return component.documentHash;
    }
    return component.filePath;
}

std::string candidateId(const yams::metadata::SearchResult& result) {
    if (!result.document.sha256Hash.empty()) {
        return result.document.sha256Hash;
    }
    return result.document.filePath;
}

std::vector<std::string>
sortedCandidateIds(const std::vector<yams::metadata::SearchResult>& results) {
    std::vector<std::string> ids;
    ids.reserve(results.size());
    for (const auto& result : results) {
        ids.push_back(candidateId(result));
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

void sortByScore(std::vector<yams::metadata::SearchResult>& results) {
    std::stable_sort(results.begin(), results.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.score != rhs.score) {
            return lhs.score > rhs.score;
        }
        return candidateId(lhs) < candidateId(rhs);
    });
}

double sourceContribution(const SourceCandidateEvidence& source, const SearchEngineConfig& config) {
    const double weight =
        std::max(0.0, static_cast<double>(componentSourceWeight(config, source.source)));
    const double k = std::max(0.0, static_cast<double>(config.rrfK));
    const double rank = static_cast<double>(source.bestRank) + 1.0;
    const double raw = std::clamp(static_cast<double>(source.bestRawScore), 0.0, 1.0);
    return weight * (1.0 / (k + rank)) * (1.0 + 0.5 * raw);
}

const SourceCandidateEvidence* bestLexicalSource(const CandidateEvidence& evidence) {
    const SourceCandidateEvidence* best = nullptr;
    for (const auto& source : evidence.sources) {
        if (source.source != ComponentResult::Source::Text &&
            source.source != ComponentResult::Source::SimeonText) {
            continue;
        }
        if (best == nullptr || source.bestRank < best->bestRank) {
            best = &source;
        }
    }
    return best;
}

} // namespace

TopologyEvidenceByCandidate
deriveTopologyCandidateEvidence(const TopologyRoutingSessionResult& route,
                                float maxScoreAdjustment) {
    TopologyEvidenceByCandidate evidence;
    if (!route.artifactAdmitted || route.confidenceAbstained ||
        !route.certificate.hasUsefulRoute() || maxScoreAdjustment <= 0.0F) {
        return evidence;
    }

    const float best = std::clamp(route.bestRouteScore, 0.0F, 1.0F);
    const float mean = std::clamp(route.meanAcceptedRouteScore, 0.0F, 1.0F);
    const float relativeBoundary =
        best > 0.0F ? std::clamp(route.routeBoundaryScoreMargin / best, 0.0F, 1.0F) : 0.0F;
    const float seedCoverage = route.seedCount > 0
                                   ? std::clamp(static_cast<float>(route.seedsInRoutedClusters) /
                                                    static_cast<float>(route.seedCount),
                                                0.0F, 1.0F)
                                   : 0.0F;
    const float confidence = std::clamp(
        0.40F * best + 0.25F * mean + 0.20F * relativeBoundary + 0.15F * seedCoverage, 0.0F, 1.0F);
    const float boundedAdjustment = std::clamp(maxScoreAdjustment, 0.0F, 1.0F);

    evidence.reserve(route.certificate.allowedDocumentHashes.size());
    for (const auto& candidate : route.certificate.allowedDocumentHashes) {
        const float localSupport = route.routedCandidateHashes.contains(candidate) ? 1.0F : 0.0F;
        const float medoidSupport = route.medoidHashes.contains(candidate) ? 1.0F : 0.0F;
        const float baseSupport = 0.65F + 0.20F * localSupport + 0.15F * medoidSupport;
        TopologyCandidateStructureEvidence structure;
        const auto structureIt = route.candidateStructureEvidence.find(candidate);
        const bool hasStructure = structureIt != route.candidateStructureEvidence.end();
        if (hasStructure) {
            structure = structureIt->second;
        }
        const float structuralSupport = std::clamp(
            0.20F * structure.scaleAgreement + 0.15F * structure.overlapSupport +
                0.20F * structure.persistenceSupport + 0.20F * structure.cohesionSupport +
                0.10F * structure.bridgeSupport + 0.15F * structure.densitySupport,
            0.0F, 1.0F);
        const float support =
            hasStructure ? std::clamp(0.75F * baseSupport + 0.25F * structuralSupport, 0.0F, 1.0F)
                         : baseSupport;
        evidence.emplace(
            candidate, TopologyCandidateEvidence{
                           .scoreAdjustment = boundedAdjustment * support,
                           .confidence = confidence,
                           .localSupport = localSupport,
                           .boundarySupport = relativeBoundary,
                           .scaleAgreement = structure.scaleAgreement,
                           .overlapSupport = structure.overlapSupport,
                           .persistenceSupport = structure.persistenceSupport,
                           .cohesionSupport = structure.cohesionSupport,
                           .bridgeSupport = structure.bridgeSupport,
                           .densitySupport = structure.densitySupport,
                           .densityPenalty = hasStructure ? 1.0F - structure.densitySupport : 0.0F,
                       });
    }
    return evidence;
}

Result<EvidencePipelineResult>
EvidenceSearchPipeline::execute(std::span<const ComponentResult> components,
                                const TopologyEvidenceByCandidate& topologyEvidence,
                                const SearchEngineConfig& config, std::size_t limit,
                                const std::unordered_set<std::string>& topologyRescueEligibleHashes,
                                const TopologyRescuePriorityByIdentity& topologyRescuePriorities,
                                const EvidenceReranker& reranker) const {
    EvidencePipelineResult output;
    output.trace.inputComponents = components.size();
    output.evidence.reserve(components.size());

    std::unordered_map<std::string, std::size_t> candidateIndex;
    candidateIndex.reserve(components.size());
    for (const auto& component : components) {
        const std::string id = candidateId(component);
        if (id.empty()) {
            ++output.trace.invalidComponents;
            continue;
        }

        auto [indexIt, inserted] = candidateIndex.emplace(id, output.evidence.size());
        if (inserted) {
            CandidateEvidence evidence;
            evidence.candidateId = id;
            evidence.documentHash = component.documentHash;
            evidence.filePath = component.filePath;
            if (component.snippet.has_value()) {
                evidence.snippet = *component.snippet;
            }
            if (auto topologyIt = topologyEvidence.find(id); topologyIt != topologyEvidence.end()) {
                evidence.topology = topologyIt->second;
                ++output.trace.topologyAnnotatedCandidates;
            }
            output.evidence.push_back(std::move(evidence));
        }

        auto& evidence = output.evidence[indexIt->second];
        if (evidence.documentHash.empty() && !component.documentHash.empty()) {
            evidence.documentHash = component.documentHash;
        }
        if (evidence.filePath.empty() && !component.filePath.empty()) {
            evidence.filePath = component.filePath;
        }
        if (evidence.snippet.empty() && component.snippet.has_value()) {
            evidence.snippet = *component.snippet;
        }
        evidence.hasLexicalAnchor =
            evidence.hasLexicalAnchor || isTextAnchoringComponent(component.source);
        evidence.hasVectorEvidence =
            evidence.hasVectorEvidence || isVectorComponent(component.source);

        auto sourceIt = std::ranges::find_if(evidence.sources, [&](const auto& source) {
            return source.source == component.source;
        });
        if (sourceIt == evidence.sources.end()) {
            evidence.sources.push_back(SourceCandidateEvidence{.source = component.source,
                                                               .bestRawScore = component.score,
                                                               .bestRank = component.rank,
                                                               .occurrences = 1});
        } else {
            sourceIt->bestRawScore = std::max(sourceIt->bestRawScore, component.score);
            sourceIt->bestRank = std::min(sourceIt->bestRank, component.rank);
            ++sourceIt->occurrences;
        }
    }

    output.trace.aggregatedCandidates = output.evidence.size();
    output.results.reserve(output.evidence.size());
    for (const auto& evidence : output.evidence) {
        yams::metadata::SearchResult result;
        result.document.sha256Hash = evidence.documentHash;
        result.document.filePath = evidence.filePath;
        result.snippet = evidence.snippet;

        for (const auto& source : evidence.sources) {
            const double contribution = sourceContribution(source, config);
            result.score += contribution;
            accumulateComponentScore(result, source.source, contribution);
        }
        if (const auto* lexical = bestLexicalSource(evidence);
            lexical != nullptr && config.lexicalFloorBoost > 0.0F &&
            (config.lexicalFloorTopN == 0 || lexical->bestRank < config.lexicalFloorTopN)) {
            const double floor =
                std::clamp(static_cast<double>(config.lexicalFloorBoost), 0.0, 1.0) /
                (1.0 + static_cast<double>(lexical->bestRank));
            result.score += floor;
            accumulateComponentScore(result, lexical->source, floor);
        }
        if (evidence.topology.has_value()) {
            const double confidence =
                std::clamp(static_cast<double>(evidence.topology->confidence), 0.0, 1.0);
            const double adjustment =
                std::clamp(static_cast<double>(evidence.topology->scoreAdjustment), -1.0, 1.0);
            const double topologyContribution = confidence * adjustment;
            result.score += topologyContribution;
            result.topologyScore = topologyContribution;
        }
        output.results.push_back(std::move(result));
    }

    sortByScore(output.results);
    output.trace.fusedCandidates = output.results.size();

    if (reranker) {
        const auto beforeIds = sortedCandidateIds(output.results);
        auto rerankResult = reranker(output.results);
        if (!rerankResult) {
            return Error{rerankResult.error().code, rerankResult.error().message};
        }
        if (beforeIds != sortedCandidateIds(output.results)) {
            return Error{ErrorCode::ValidationError,
                         "evidence reranker must preserve the fused candidate set"};
        }
        sortByScore(output.results);
    }
    output.trace.rerankedCandidates = output.results.size();

    const std::size_t effectiveLimit = limit > 0 ? limit : config.maxResults;
    if (output.results.size() > effectiveLimit) {
        std::size_t keep = effectiveLimit;
        if (config.topologyFusionRescueSlots > 0) {
            // Observe the complete eligible tail before applying the slot budget. A bounded scan
            // can prove that the mechanism fired, but cannot measure the selector rank of a
            // relevant rescue that lost to earlier eligible candidates.
            std::vector<std::size_t> eligibleTailIndices;
            for (std::size_t i = effectiveLimit; i < output.results.size(); ++i) {
                const auto& candidate = output.results[i];
                const auto& identity = candidateId(candidate);
                if (!topologyRescueEligibleHashes.contains(identity)) {
                    continue;
                }
                ++output.trace.topologyFusionRescueIdentityMatchesInTail;
                eligibleTailIndices.push_back(i);
                if (candidate.topologyScore.has_value()) {
                    ++output.trace.topologyFusionRescueIdentityMatchesWithEvidenceInTail;
                }
            }

            // Non-empty priorities define a query-conditioned ordering only inside the already
            // identity-eligible set. Missing/invalid values sort after scored candidates and
            // retain fused order, which makes a partial scorer observable without inventing a
            // new rescue candidate.
            if (!topologyRescuePriorities.empty()) {
                std::ranges::stable_sort(
                    eligibleTailIndices, [&](std::size_t lhs, std::size_t rhs) {
                        const auto lhsIt =
                            topologyRescuePriorities.find(candidateId(output.results[lhs]));
                        const auto rhsIt =
                            topologyRescuePriorities.find(candidateId(output.results[rhs]));
                        const bool lhsScored =
                            lhsIt != topologyRescuePriorities.end() && std::isfinite(lhsIt->second);
                        const bool rhsScored =
                            rhsIt != topologyRescuePriorities.end() && std::isfinite(rhsIt->second);
                        if (lhsScored != rhsScored) {
                            return lhsScored;
                        }
                        return lhsScored && lhsIt->second != rhsIt->second
                                   ? lhsIt->second > rhsIt->second
                                   : false;
                    });
            }

            output.trace.topologyFusionRescueEligibleIdentitiesInTail.reserve(
                eligibleTailIndices.size());
            for (const std::size_t index : eligibleTailIndices) {
                output.trace.topologyFusionRescueEligibleIdentitiesInTail.push_back(
                    candidateId(output.results[index]));
            }

            for (const std::size_t i : eligibleTailIndices) {
                if (output.trace.topologyFusionRescuedCandidates >=
                    config.topologyFusionRescueSlots) {
                    break;
                }
                // Gate: the candidate must be a specific rescue-eligible identity (the
                // caller-supplied novel-document set), not merely topology-annotated -- topology
                // evidence is assigned to the whole route-admitted relation (often hundreds of
                // candidates sharing one route-level confidence), so a raw score cannot
                // discriminate the actual baseline-miss candidate from ambient route membership.
                if (!topologyRescueEligibleHashes.contains(candidateId(output.results[i]))) {
                    continue;
                }
                // Identity membership is never a bypass for having actually received topology
                // evidence: the eligibility set and the topology-evidence map are populated by
                // independent producers, and could in principle diverge if wiring drifts.
                if (!output.results[i].topologyScore.has_value()) {
                    continue;
                }
                // Additive only: swap the eligible candidate into the next open tail slot rather
                // than displacing anything inside [0, effectiveLimit). This keeps
                // PreservesBaselineRelevant trivially true for this boundary.
                if (i != keep) {
                    std::swap(output.results[keep], output.results[i]);
                }
                ++keep;
                ++output.trace.topologyFusionRescuedCandidates;
            }
        }
        output.results.resize(keep);
    }
    output.trace.finalCandidates = output.results.size();
    return output;
}

} // namespace yams::search
