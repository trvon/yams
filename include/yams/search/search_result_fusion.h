#pragma once

#include <yams/search/search_models.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace yams::search {

class ResultFusion {
public:
    explicit ResultFusion(const SearchEngineConfig& config);

    std::vector<SearchResult> fuse(const std::vector<ComponentResult>& componentResults);

private:
    std::vector<SearchResult> fuseWeightedSum(const std::vector<ComponentResult>& results);
    std::vector<SearchResult> fuseReciprocalRank(const std::vector<ComponentResult>& results);
    std::vector<SearchResult> fuseWeightedReciprocal(const std::vector<ComponentResult>& results);
    std::vector<SearchResult> fuseCombMNZ(const std::vector<ComponentResult>& results);
    std::vector<SearchResult> fuseConvex(const std::vector<ComponentResult>& results);
    std::vector<SearchResult> fuseWeightedLinearZScore(const std::vector<ComponentResult>& results);

    template <typename ScoreFunc>
    std::vector<SearchResult> fuseSinglePass(const std::vector<ComponentResult>& results,
                                             ScoreFunc&& scoreFunc);

    float getComponentWeight(ComponentResult::Source source) const;

    const SearchEngineConfig& config_;
};

inline void accumulateComponentScore(SearchResult& r, ComponentResult::Source source,
                                     double contribution) {
    switch (source) {
        case ComponentResult::Source::Vector:
        case ComponentResult::Source::EntityVector:
            r.vectorScore = r.vectorScore.value_or(0.0) + contribution;
            break;
        case ComponentResult::Source::GraphVector:
            r.graphVectorScore = r.graphVectorScore.value_or(0.0) + contribution;
            break;
        case ComponentResult::Source::Text:
            r.keywordScore = r.keywordScore.value_or(0.0) + contribution;
            break;
        case ComponentResult::Source::GraphText:
            r.graphTextScore = r.graphTextScore.value_or(0.0) + contribution;
            break;
        case ComponentResult::Source::KnowledgeGraph:
            r.kgScore = r.kgScore.value_or(0.0) + contribution;
            break;
        case ComponentResult::Source::PathTree:
            r.pathScore = r.pathScore.value_or(0.0) + contribution;
            break;
        case ComponentResult::Source::Tag:
        case ComponentResult::Source::Metadata:
            r.tagScore = r.tagScore.value_or(0.0) + contribution;
            break;
        case ComponentResult::Source::Symbol:
            r.symbolScore = r.symbolScore.value_or(0.0) + contribution;
            break;
        default:
            break;
    }
}

inline double strongVectorOnlyReliefStrength(const SearchEngineConfig& config, double rawVector,
                                             size_t bestVectorRank) {
    if (!config.enableStrongVectorOnlyRelief) {
        return 0.0;
    }

    const double minScore =
        std::clamp(static_cast<double>(config.strongVectorOnlyMinScore), 0.0, 1.0);
    double rawStrength = 0.0;
    if (rawVector >= minScore) {
        if (minScore >= 1.0) {
            rawStrength = 1.0;
        } else {
            rawStrength = std::clamp((rawVector - minScore) / (1.0 - minScore), 0.0, 1.0);
        }
    }

    double rankStrength = 0.0;
    if (config.strongVectorOnlyTopRank > 0 &&
        bestVectorRank != std::numeric_limits<size_t>::max() &&
        bestVectorRank < config.strongVectorOnlyTopRank) {
        rankStrength =
            std::clamp(static_cast<double>(config.strongVectorOnlyTopRank - bestVectorRank) /
                           static_cast<double>(config.strongVectorOnlyTopRank),
                       0.0, 1.0);
    }

    return std::max(rawStrength, rankStrength);
}

inline bool strongVectorOnlyReliefEligible(const SearchEngineConfig& config, double rawVector,
                                           size_t bestVectorRank) {
    return strongVectorOnlyReliefStrength(config, rawVector, bestVectorRank) > 0.0;
}

inline double effectiveVectorOnlyPenalty(const SearchEngineConfig& config, double rawVector,
                                         size_t bestVectorRank) {
    const double basePenalty = std::clamp(static_cast<double>(config.vectorOnlyPenalty), 0.0, 1.0);
    if (!config.enableStrongVectorOnlyRelief) {
        return basePenalty;
    }

    const double reliefPenalty =
        std::clamp(static_cast<double>(config.strongVectorOnlyPenalty), basePenalty, 1.0);
    const double t = strongVectorOnlyReliefStrength(config, rawVector, bestVectorRank);
    return basePenalty + (reliefPenalty - basePenalty) * t;
}

inline size_t semanticRescueWindowLimit(const SearchEngineConfig& config) {
    if (config.enableReranking && config.rerankTopK > 0) {
        return std::min(config.maxResults, config.rerankTopK);
    }
    return config.maxResults;
}

template <typename ScoreFunc>
std::vector<SearchResult> ResultFusion::fuseSinglePass(const std::vector<ComponentResult>& results,
                                                       ScoreFunc&& scoreFunc) {
    std::unordered_map<std::string, SearchResult> resultMap;
    resultMap.reserve(results.size());
    std::unordered_map<std::string, double> maxVectorRawScore;
    maxVectorRawScore.reserve(results.size());
    std::unordered_set<std::string> anchoredDocs;
    anchoredDocs.reserve(results.size());
    std::unordered_map<std::string, size_t> bestTextRank;
    bestTextRank.reserve(results.size());
    std::unordered_map<std::string, size_t> bestVectorRank;
    bestVectorRank.reserve(results.size());

    for (const auto& comp : results) {
        const std::string dedupKey =
            detail::makeFusionDedupKey(comp, config_.enablePathDedupInFusion);
        auto& r = resultMap[dedupKey];
        if (r.document.sha256Hash.empty()) {
            r.document.sha256Hash = comp.documentHash;
            r.document.filePath = comp.filePath;
            r.score = 0.0;
        } else if (r.document.filePath.empty() && !comp.filePath.empty()) {
            r.document.filePath = comp.filePath;
        }

        const double contribution = scoreFunc(comp);
        r.score += contribution;
        accumulateComponentScore(r, comp.source, contribution);

        if (comp.source == ComponentResult::Source::Text) {
            auto [it, inserted] = bestTextRank.try_emplace(dedupKey, comp.rank);
            if (!inserted) {
                it->second = std::min(it->second, comp.rank);
            }
        }

        if (isVectorComponent(comp.source)) {
            const double clampedRaw = std::clamp(static_cast<double>(comp.score), 0.0, 1.0);
            auto [it, inserted] = maxVectorRawScore.try_emplace(dedupKey, clampedRaw);
            if (!inserted) {
                it->second = std::max(it->second, clampedRaw);
            }
            auto [rankIt, rankInserted] = bestVectorRank.try_emplace(dedupKey, comp.rank);
            if (!rankInserted) {
                rankIt->second = std::min(rankIt->second, comp.rank);
            }
        } else if (isTextAnchoringComponent(comp.source)) {
            anchoredDocs.insert(dedupKey);
        }

        if (r.snippet.empty() && comp.snippet.has_value()) {
            r.snippet = comp.snippet.value();
        }
    }

    std::vector<SearchResult> fusedResults;
    fusedResults.reserve(resultMap.size());
    std::vector<std::pair<double, SearchResult>> nearMissReserve;
    nearMissReserve.reserve(std::min(config_.vectorOnlyNearMissReserve, resultMap.size()));

    const double vectorOnlyThreshold =
        std::clamp(static_cast<double>(config_.vectorOnlyThreshold), 0.0, 1.0);
    const double nearMissSlack =
        std::clamp(static_cast<double>(config_.vectorOnlyNearMissSlack), 0.0, 1.0);
    const double nearMissPenalty =
        std::clamp(static_cast<double>(config_.vectorOnlyNearMissPenalty), 0.0, 1.0);

    for (auto& [hash, r] : resultMap) {
        const bool hasAnchoring = anchoredDocs.contains(hash);
        const auto vecIt = maxVectorRawScore.find(hash);
        const bool hasVector = vecIt != maxVectorRawScore.end();

        if (config_.lexicalFloorBoost > 0.0f) {
            if (auto textRankIt = bestTextRank.find(hash); textRankIt != bestTextRank.end()) {
                const bool floorEnabledForRank = (config_.lexicalFloorTopN == 0) ||
                                                 (textRankIt->second < config_.lexicalFloorTopN);
                if (floorEnabledForRank) {
                    const double floorBoost =
                        std::clamp(static_cast<double>(config_.lexicalFloorBoost), 0.0, 1.0) /
                        (1.0 + static_cast<double>(textRankIt->second));
                    r.score += floorBoost;
                }
            }
        }

        if (hasVector && !hasAnchoring) {
            const double rawVector = vecIt->second;
            const size_t bestSemanticRank = bestVectorRank.contains(hash)
                                                ? bestVectorRank.at(hash)
                                                : std::numeric_limits<size_t>::max();
            const bool strongRelief =
                strongVectorOnlyReliefEligible(config_, rawVector, bestSemanticRank);
            const double vectorOnlyPenalty =
                effectiveVectorOnlyPenalty(config_, rawVector, bestSemanticRank);
            if (rawVector < vectorOnlyThreshold) {
                const bool reserveEnabled = config_.vectorOnlyNearMissReserve > 0;
                const bool isNearMiss = reserveEnabled && vectorOnlyThreshold > 0.0 &&
                                        rawVector + nearMissSlack >= vectorOnlyThreshold;
                if (!isNearMiss && !strongRelief) {
                    continue;
                }

                if (strongRelief) {
                    r.score *= vectorOnlyPenalty;
                } else {
                    if (config_.semanticRescueSlots > 0 &&
                        rawVector >= std::max(0.0, static_cast<double>(
                                                       config_.semanticRescueMinVectorScore)) &&
                        !isNearMiss) {
                        r.score *= vectorOnlyPenalty;
                        nearMissReserve.emplace_back(rawVector, std::move(r));
                        continue;
                    }

                    const double thresholdRatio =
                        vectorOnlyThreshold > 0.0
                            ? std::clamp(rawVector / vectorOnlyThreshold, 0.0, 1.0)
                            : std::clamp(rawVector, 0.0, 1.0);
                    r.score *= (vectorOnlyPenalty * nearMissPenalty * thresholdRatio);
                    nearMissReserve.emplace_back(rawVector, std::move(r));
                    continue;
                }
            }
            r.score *= vectorOnlyPenalty;
        }

        if (hasVector && hasAnchoring && config_.vectorBoostFactor > 0.0f) {
            const double vectorContribution = r.vectorScore.value_or(0.0);
            const double anchorContribution =
                r.keywordScore.value_or(0.0) + r.pathScore.value_or(0.0) + r.kgScore.value_or(0.0) +
                r.tagScore.value_or(0.0) + r.symbolScore.value_or(0.0);

            if (vectorContribution > 0.0 && anchorContribution > 0.0) {
                const double agreement = (2.0 * std::min(vectorContribution, anchorContribution)) /
                                         (vectorContribution + anchorContribution);
                const double boostFactor =
                    std::clamp(static_cast<double>(config_.vectorBoostFactor), 0.0, 0.10);
                r.score *= (1.0 + boostFactor * agreement);
            }
        }

        fusedResults.emplace_back(std::move(r));
    }

    if (!nearMissReserve.empty() && config_.vectorOnlyNearMissReserve > 0) {
        std::sort(nearMissReserve.begin(), nearMissReserve.end(), [](const auto& a, const auto& b) {
            if (a.first != b.first) {
                return a.first > b.first;
            }
            return a.second.score > b.second.score;
        });

        const size_t reserveTake =
            std::min(config_.vectorOnlyNearMissReserve, nearMissReserve.size());
        for (size_t i = 0; i < reserveTake; ++i) {
            fusedResults.emplace_back(std::move(nearMissReserve[i].second));
        }
    }

    const auto lexicalAnchorScore = [](const SearchResult& r) {
        return r.keywordScore.value_or(0.0) + r.pathScore.value_or(0.0) + r.tagScore.value_or(0.0) +
               r.symbolScore.value_or(0.0);
    };

    const auto rawVectorScoreForResult = [&maxVectorRawScore](const std::string& dedupKey) {
        if (auto it = maxVectorRawScore.find(dedupKey); it != maxVectorRawScore.end()) {
            return it->second;
        }
        return 0.0;
    };

    const auto isVectorOnlyRescueCandidate =
        [this, &lexicalAnchorScore, &rawVectorScoreForResult](const SearchResult& r) -> bool {
        const double lexical = lexicalAnchorScore(r);
        const std::string dedupKey =
            (config_.enablePathDedupInFusion && !r.document.filePath.empty())
                ? std::string("path:") + r.document.filePath
            : (!r.document.sha256Hash.empty()) ? std::string("hash:") + r.document.sha256Hash
                                               : std::string("path:") + r.document.filePath;
        const double vector = rawVectorScoreForResult(dedupKey);
        return lexical <= 0.0 &&
               vector >= std::max(0.0, static_cast<double>(config_.semanticRescueMinVectorScore));
    };

    const auto evidenceRescueScore = [&lexicalAnchorScore](const SearchResult& r) -> double {
        const double lexical = lexicalAnchorScore(r);
        const double vector = r.vectorScore.value_or(0.0);
        const double kg = r.kgScore.value_or(0.0);
        const double bestSignal = std::max({lexical, vector, kg, r.pathScore.value_or(0.0),
                                            r.symbolScore.value_or(0.0), r.tagScore.value_or(0.0)});
        const double componentBonus = (lexical > 0.0 && vector > 0.0) ? 0.01 : 0.0;
        return bestSignal + componentBonus;
    };

    const auto isEvidenceRescueCandidate = [this, &evidenceRescueScore](const SearchResult& r) {
        return evidenceRescueScore(r) >=
               std::max(0.0, static_cast<double>(config_.fusionEvidenceRescueMinScore));
    };

    const auto lexicalAwareLess = [this, &lexicalAnchorScore](const SearchResult& a,
                                                              const SearchResult& b) {
        const double scoreDiff = a.score - b.score;
        const double tieEpsilon =
            std::max(0.0, static_cast<double>(config_.lexicalTieBreakEpsilon));

        if (!config_.enableLexicalTieBreak || std::abs(scoreDiff) > tieEpsilon) {
            if (a.score != b.score) {
                return a.score > b.score;
            }
        } else {
            const double lexicalA = lexicalAnchorScore(a);
            const double lexicalB = lexicalAnchorScore(b);
            if (lexicalA != lexicalB) {
                return lexicalA > lexicalB;
            }

            const double keywordA = a.keywordScore.value_or(0.0);
            const double keywordB = b.keywordScore.value_or(0.0);
            if (keywordA != keywordB) {
                return keywordA > keywordB;
            }

            const double vectorA = a.vectorScore.value_or(0.0);
            const double vectorB = b.vectorScore.value_or(0.0);
            if (vectorA != vectorB) {
                return vectorA < vectorB;
            }
        }

        if (a.document.filePath != b.document.filePath) {
            return a.document.filePath < b.document.filePath;
        }
        return a.document.sha256Hash < b.document.sha256Hash;
    };

    const auto applySemanticRescueWindow = [&]() {
        if (config_.semanticRescueSlots == 0 || fusedResults.empty()) {
            return;
        }

        const size_t topK = std::min(semanticRescueWindowLimit(config_), fusedResults.size());
        if (topK == 0 || topK >= fusedResults.size()) {
            return;
        }

        const size_t rescueTarget = std::min(config_.semanticRescueSlots, topK);
        size_t rescuePresent = 0;
        for (size_t i = 0; i < topK; ++i) {
            if (isVectorOnlyRescueCandidate(fusedResults[i])) {
                rescuePresent++;
            }
        }

        while (rescuePresent < rescueTarget) {
            size_t bestTailIndex = fusedResults.size();
            for (size_t i = topK; i < fusedResults.size(); ++i) {
                if (!isVectorOnlyRescueCandidate(fusedResults[i])) {
                    continue;
                }
                if (bestTailIndex >= fusedResults.size() ||
                    lexicalAwareLess(fusedResults[i], fusedResults[bestTailIndex])) {
                    bestTailIndex = i;
                }
            }
            if (bestTailIndex >= fusedResults.size()) {
                break;
            }

            size_t victimIndex = topK;
            for (size_t i = topK; i > 0; --i) {
                const size_t idx = i - 1;
                if (!isVectorOnlyRescueCandidate(fusedResults[idx])) {
                    victimIndex = idx;
                    break;
                }
            }
            if (victimIndex >= topK) {
                break;
            }

            std::swap(fusedResults[victimIndex], fusedResults[bestTailIndex]);
            rescuePresent++;
        }

        std::sort(fusedResults.begin(), fusedResults.begin() + static_cast<ptrdiff_t>(topK),
                  lexicalAwareLess);
    };

    if (fusedResults.size() > config_.maxResults) {
        std::partial_sort(fusedResults.begin(),
                          fusedResults.begin() + static_cast<ptrdiff_t>(config_.maxResults),
                          fusedResults.end(), lexicalAwareLess);
        applySemanticRescueWindow();

        if (config_.fusionEvidenceRescueSlots > 0) {
            const size_t topK = config_.maxResults;
            const size_t rescueTarget = std::min(config_.fusionEvidenceRescueSlots, topK);
            size_t rescuePresent = 0;
            for (size_t i = 0; i < topK; ++i) {
                if (isEvidenceRescueCandidate(fusedResults[i])) {
                    rescuePresent++;
                }
            }

            while (rescuePresent < rescueTarget) {
                size_t bestTailIndex = fusedResults.size();
                double bestTailEvidence = -1.0;
                for (size_t i = topK; i < fusedResults.size(); ++i) {
                    if (!isEvidenceRescueCandidate(fusedResults[i])) {
                        continue;
                    }
                    const double evidence = evidenceRescueScore(fusedResults[i]);
                    if (evidence > bestTailEvidence ||
                        (evidence == bestTailEvidence &&
                         (bestTailIndex >= fusedResults.size() ||
                          lexicalAwareLess(fusedResults[i], fusedResults[bestTailIndex])))) {
                        bestTailIndex = i;
                        bestTailEvidence = evidence;
                    }
                }
                if (bestTailIndex >= fusedResults.size()) {
                    break;
                }

                size_t victimIndex = topK;
                double victimEvidence = std::numeric_limits<double>::max();
                for (size_t i = 0; i < topK; ++i) {
                    const double evidence = evidenceRescueScore(fusedResults[i]);
                    if (evidence < victimEvidence) {
                        victimEvidence = evidence;
                        victimIndex = i;
                    }
                }
                if (victimIndex >= topK || bestTailEvidence <= victimEvidence) {
                    break;
                }

                std::swap(fusedResults[victimIndex], fusedResults[bestTailIndex]);
                rescuePresent++;
            }

            std::sort(fusedResults.begin(), fusedResults.begin() + static_cast<ptrdiff_t>(topK),
                      lexicalAwareLess);
        }

        fusedResults.resize(config_.maxResults);
    } else {
        std::sort(fusedResults.begin(), fusedResults.end(), lexicalAwareLess);
        applySemanticRescueWindow();
    }

    return fusedResults;
}

} // namespace yams::search
