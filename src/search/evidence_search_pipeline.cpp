// Copyright 2026 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/search/evidence_search_pipeline.h>
#include <yams/search/search_result_fusion.h>

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
        route.routeAllowedDocumentHashes.empty() || maxScoreAdjustment <= 0.0F) {
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

    evidence.reserve(route.routeAllowedDocumentHashes.size());
    for (const auto& candidate : route.routeAllowedDocumentHashes) {
        const float localSupport = route.routedCandidateHashes.contains(candidate) ? 1.0F : 0.0F;
        const float medoidSupport = route.medoidHashes.contains(candidate) ? 1.0F : 0.0F;
        const float support = 0.65F + 0.20F * localSupport + 0.15F * medoidSupport;
        evidence.emplace(candidate, TopologyCandidateEvidence{
                                        .scoreAdjustment = boundedAdjustment * support,
                                        .confidence = confidence,
                                        .localSupport = localSupport,
                                        .boundarySupport = relativeBoundary,
                                        .scaleAgreement = 0.0F,
                                        .densityPenalty = 0.0F,
                                    });
    }
    return evidence;
}

Result<EvidencePipelineResult>
EvidenceSearchPipeline::execute(std::span<const ComponentResult> components,
                                const TopologyEvidenceByCandidate& topologyEvidence,
                                const SearchEngineConfig& config, std::size_t limit,
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
            result.score += confidence * adjustment;
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
        output.results.resize(effectiveLimit);
    }
    output.trace.finalCandidates = output.results.size();
    return output;
}

} // namespace yams::search
