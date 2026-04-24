#include <yams/search/search_result_fusion.h>

#include <spdlog/spdlog.h>

#include <exception>
#include <limits>
#include <tuple>
#include <unordered_map>

namespace yams::search {

ResultFusion::ResultFusion(const SearchEngineConfig& config) : config_(config) {}

std::vector<SearchResult> ResultFusion::fuse(const std::vector<ComponentResult>& componentResults) {
    if (componentResults.empty()) [[unlikely]] {
        return {};
    }

    switch (config_.fusionStrategy) {
        case SearchEngineConfig::FusionStrategy::WEIGHTED_SUM:
            return fuseWeightedSum(componentResults);
        case SearchEngineConfig::FusionStrategy::RECIPROCAL_RANK:
            return fuseReciprocalRank(componentResults);
        case SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL:
            return fuseWeightedReciprocal(componentResults);
        case SearchEngineConfig::FusionStrategy::COMB_MNZ:
            return fuseCombMNZ(componentResults);
        case SearchEngineConfig::FusionStrategy::CONVEX:
            try {
                return fuseConvex(componentResults);
            } catch (const std::exception& e) {
                spdlog::warn("[search] convex fusion failed ({}); falling back to RRF", e.what());
                return fuseReciprocalRank(componentResults);
            } catch (...) {
                spdlog::warn("[search] convex fusion failed (unknown); falling back to RRF");
                return fuseReciprocalRank(componentResults);
            }
        case SearchEngineConfig::FusionStrategy::WEIGHTED_LINEAR_ZSCORE:
            try {
                return fuseWeightedLinearZScore(componentResults);
            } catch (const std::exception& e) {
                spdlog::warn("[search] weighted-linear-zscore fusion failed ({}); falling back "
                             "to RRF",
                             e.what());
                return fuseReciprocalRank(componentResults);
            }
    }
    return fuseCombMNZ(componentResults);
}

std::vector<SearchResult>
ResultFusion::fuseWeightedSum(const std::vector<ComponentResult>& results) {
    return fuseSinglePass(results, [this](const ComponentResult& comp) {
        return comp.score * getComponentWeight(comp.source);
    });
}

std::vector<SearchResult>
ResultFusion::fuseReciprocalRank(const std::vector<ComponentResult>& results) {
    const float k = config_.rrfK;
    return fuseSinglePass(results, [k](const ComponentResult& comp) {
        const double rank = static_cast<double>(comp.rank) + 1.0;
        return 1.0 / (k + rank);
    });
}

std::vector<SearchResult>
ResultFusion::fuseWeightedReciprocal(const std::vector<ComponentResult>& results) {
    const float k = config_.rrfK;
    return fuseSinglePass(results, [this, k](const ComponentResult& comp) {
        float weight = getComponentWeight(comp.source);
        const double rank = static_cast<double>(comp.rank) + 1.0;
        double rrfScore = 1.0 / (k + rank);
        double scoreScale = 1.0;
        if (config_.enableFieldAwareWeightedRrf) {
            scoreScale = 0.60;
            switch (comp.source) {
                case ComponentResult::Source::Text:
                case ComponentResult::Source::GraphText:
                    scoreScale = 1.00;
                    break;
                case ComponentResult::Source::PathTree:
                    scoreScale = 0.85;
                    break;
                case ComponentResult::Source::KnowledgeGraph:
                    scoreScale = 0.80;
                    break;
                case ComponentResult::Source::Tag:
                case ComponentResult::Source::Metadata:
                    scoreScale = 0.65;
                    break;
                case ComponentResult::Source::Vector:
                    scoreScale = 0.75;
                    break;
                case ComponentResult::Source::GraphVector:
                    scoreScale = 0.45;
                    break;
                case ComponentResult::Source::EntityVector:
                    scoreScale = 0.35;
                    break;
                case ComponentResult::Source::Symbol:
                    scoreScale = 0.75;
                    break;
                case ComponentResult::Source::Anchor:
                    scoreScale = 0.70;
                    break;
                case ComponentResult::Source::Unknown:
                    scoreScale = 0.60;
                    break;
            }
        }

        double scoreBoost =
            1.0 + scoreScale * std::clamp(static_cast<double>(comp.score), 0.0, 1.0);
        return weight * rrfScore * scoreBoost;
    });
}

std::vector<SearchResult> ResultFusion::fuseCombMNZ(const std::vector<ComponentResult>& results) {
    struct Accumulator {
        double score = 0.0;
        size_t componentCount = 0;
        size_t bestTextRank = std::numeric_limits<size_t>::max();
        size_t bestVectorRank = std::numeric_limits<size_t>::max();
        bool hasAnchoring = false;
        double maxVectorRaw = 0.0;
        double keywordScore = 0.0;
        double pathScore = 0.0;
        double tagScore = 0.0;
        double symbolScore = 0.0;
        double graphTextScore = 0.0;
        double vectorScore = 0.0;
        double graphVectorScore = 0.0;
        std::string documentHash;
        std::string filePath;
        std::string snippet;
    };
    std::unordered_map<std::string, Accumulator> accumMap;
    accumMap.reserve(results.size());

    const float k = config_.rrfK;

    for (const auto& comp : results) {
        const std::string dedupKey =
            detail::makeFusionDedupKey(comp, config_.enablePathDedupInFusion);
        auto& acc = accumMap[dedupKey];

        if (acc.componentCount == 0) {
            acc.documentHash = comp.documentHash;
            acc.filePath = comp.filePath;
            if (comp.snippet.has_value()) {
                acc.snippet = comp.snippet.value();
            }
        } else if (acc.filePath.empty() && !comp.filePath.empty()) {
            acc.filePath = comp.filePath;
        }

        if (acc.snippet.empty() && comp.snippet.has_value()) {
            acc.snippet = comp.snippet.value();
        }

        if (comp.source == ComponentResult::Source::Text) {
            acc.bestTextRank = std::min(acc.bestTextRank, comp.rank);
        }
        if (isTextAnchoringComponent(comp.source)) {
            acc.hasAnchoring = true;
        }
        if (isVectorComponent(comp.source)) {
            acc.maxVectorRaw =
                std::max(acc.maxVectorRaw, std::clamp(static_cast<double>(comp.score), 0.0, 1.0));
            acc.bestVectorRank = std::min(acc.bestVectorRank, comp.rank);
        }

        float weight = getComponentWeight(comp.source);
        const double rank = static_cast<double>(comp.rank) + 1.0;
        double rrfScore = 1.0 / (k + rank);
        double contribution = weight * rrfScore;

        acc.score += contribution;
        acc.componentCount++;

        switch (comp.source) {
            case ComponentResult::Source::Text:
                acc.keywordScore += contribution;
                break;
            case ComponentResult::Source::GraphText:
                acc.graphTextScore += contribution;
                break;
            case ComponentResult::Source::PathTree:
                acc.pathScore += contribution;
                break;
            case ComponentResult::Source::Tag:
            case ComponentResult::Source::Metadata:
                acc.tagScore += contribution;
                break;
            case ComponentResult::Source::Symbol:
                acc.symbolScore += contribution;
                break;
            case ComponentResult::Source::Vector:
            case ComponentResult::Source::EntityVector:
                acc.vectorScore += contribution;
                break;
            case ComponentResult::Source::GraphVector:
                acc.graphVectorScore += contribution;
                break;
            case ComponentResult::Source::Anchor:
            case ComponentResult::Source::KnowledgeGraph:
            case ComponentResult::Source::Unknown:
                break;
        }
    }

    std::vector<SearchResult> fusedResults;
    fusedResults.reserve(accumMap.size());
    std::vector<std::pair<double, SearchResult>> semanticRescueReserve;
    semanticRescueReserve.reserve(std::min(config_.semanticRescueSlots, accumMap.size()));
    std::unordered_map<std::string, double> rawVectorScoreByDedupKey;
    rawVectorScoreByDedupKey.reserve(accumMap.size());

    for (auto& entry : accumMap) {
        rawVectorScoreByDedupKey[entry.first] = entry.second.maxVectorRaw;

        SearchResult r;
        r.document.sha256Hash = std::move(entry.second.documentHash);
        r.document.filePath = std::move(entry.second.filePath);
        r.score = static_cast<float>(entry.second.score *
                                     static_cast<double>(entry.second.componentCount));
        if (entry.second.keywordScore > 0.0) {
            r.keywordScore = entry.second.keywordScore;
        }
        if (entry.second.pathScore > 0.0) {
            r.pathScore = entry.second.pathScore;
        }
        if (entry.second.tagScore > 0.0) {
            r.tagScore = entry.second.tagScore;
        }
        if (entry.second.symbolScore > 0.0) {
            r.symbolScore = entry.second.symbolScore;
        }
        if (entry.second.graphTextScore > 0.0) {
            r.graphTextScore = entry.second.graphTextScore;
        }
        if (entry.second.vectorScore > 0.0) {
            r.vectorScore = entry.second.vectorScore;
        }
        if (entry.second.graphVectorScore > 0.0) {
            r.graphVectorScore = entry.second.graphVectorScore;
        }

        if (config_.lexicalFloorBoost > 0.0f &&
            entry.second.bestTextRank != std::numeric_limits<size_t>::max()) {
            const bool floorEnabledForRank = (config_.lexicalFloorTopN == 0) ||
                                             (entry.second.bestTextRank < config_.lexicalFloorTopN);
            if (floorEnabledForRank) {
                const double floorBoost =
                    std::clamp(static_cast<double>(config_.lexicalFloorBoost), 0.0, 1.0) /
                    (1.0 + static_cast<double>(entry.second.bestTextRank));
                r.score += floorBoost;
            }
        }

        if (entry.second.maxVectorRaw > 0.0 && !entry.second.hasAnchoring) {
            const double vectorOnlyThreshold =
                std::clamp(static_cast<double>(config_.vectorOnlyThreshold), 0.0, 1.0);
            const double nearMissSlack =
                std::clamp(static_cast<double>(config_.vectorOnlyNearMissSlack), 0.0, 1.0);
            const double nearMissPenalty =
                std::clamp(static_cast<double>(config_.vectorOnlyNearMissPenalty), 0.0, 1.0);
            const double semanticRescueMinVector =
                std::max(0.0, static_cast<double>(config_.semanticRescueMinVectorScore));
            const bool strongRelief = strongVectorOnlyReliefEligible(
                config_, entry.second.maxVectorRaw, entry.second.bestVectorRank);
            const double effectivePenalty = effectiveVectorOnlyPenalty(
                config_, entry.second.maxVectorRaw, entry.second.bestVectorRank);
            const bool semanticRescueEligible =
                config_.semanticRescueSlots > 0 &&
                entry.second.maxVectorRaw >= semanticRescueMinVector;

            if (entry.second.maxVectorRaw < vectorOnlyThreshold) {
                const bool reserveEnabled = config_.vectorOnlyNearMissReserve > 0;
                const bool isNearMiss =
                    reserveEnabled && vectorOnlyThreshold > 0.0 &&
                    entry.second.maxVectorRaw + nearMissSlack >= vectorOnlyThreshold;
                if (!isNearMiss && !strongRelief && !semanticRescueEligible) {
                    continue;
                }

                if (strongRelief) {
                    r.score = static_cast<float>(r.score * effectivePenalty);
                } else {
                    if (semanticRescueEligible && !isNearMiss) {
                        r.score = static_cast<float>(r.score * effectivePenalty);
                        semanticRescueReserve.emplace_back(entry.second.maxVectorRaw, std::move(r));
                        continue;
                    }

                    const double thresholdRatio =
                        vectorOnlyThreshold > 0.0
                            ? std::clamp(entry.second.maxVectorRaw / vectorOnlyThreshold, 0.0, 1.0)
                            : std::clamp(entry.second.maxVectorRaw, 0.0, 1.0);
                    r.score = static_cast<float>(r.score * effectivePenalty * nearMissPenalty *
                                                 thresholdRatio);
                }
            } else {
                r.score = static_cast<float>(r.score * effectivePenalty);
            }
        }

        r.snippet = std::move(entry.second.snippet);
        fusedResults.push_back(std::move(r));
    }

    if (!semanticRescueReserve.empty()) {
        std::sort(semanticRescueReserve.begin(), semanticRescueReserve.end(),
                  [](const auto& a, const auto& b) {
                      if (a.first != b.first) {
                          return a.first > b.first;
                      }
                      return a.second.score > b.second.score;
                  });

        for (auto& [_, result] : semanticRescueReserve) {
            fusedResults.push_back(std::move(result));
        }
    }

    const bool pathDedup = config_.enablePathDedupInFusion;
    const auto rawVectorScoreForResult = [&rawVectorScoreByDedupKey,
                                          pathDedup](const SearchResult& r) {
        if (auto it = rawVectorScoreByDedupKey.find(detail::makeFusionDedupKey(r, pathDedup));
            it != rawVectorScoreByDedupKey.end()) {
            return it->second;
        }
        return 0.0;
    };

    const auto lexicalAnchorScore = [](const SearchResult& r) {
        return r.keywordScore.value_or(0.0) + r.pathScore.value_or(0.0) + r.tagScore.value_or(0.0) +
               r.symbolScore.value_or(0.0);
    };

    const auto isVectorOnlyRescueCandidate =
        [this, &lexicalAnchorScore, &rawVectorScoreForResult](const SearchResult& r) -> bool {
        const double lexical = lexicalAnchorScore(r);
        const double vector = rawVectorScoreForResult(r);
        return lexical <= 0.0 &&
               vector >= std::max(0.0, static_cast<double>(config_.semanticRescueMinVectorScore));
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

    const auto semanticRescueBetter = [&rawVectorScoreForResult, &lexicalAwareLess](
                                          const SearchResult& a, const SearchResult& b) {
        const double rawVectorA = rawVectorScoreForResult(a);
        const double rawVectorB = rawVectorScoreForResult(b);
        if (rawVectorA != rawVectorB) {
            return rawVectorA > rawVectorB;
        }
        return lexicalAwareLess(a, b);
    };

    const auto applySemanticRescueWindow = [&]() {
        if (config_.semanticRescueSlots == 0 || fusedResults.empty()) {
            return;
        }

        const size_t topK = std::min((config_.enableReranking && config_.rerankTopK > 0)
                                         ? std::min(config_.maxResults, config_.rerankTopK)
                                         : config_.maxResults,
                                     fusedResults.size());
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
                    semanticRescueBetter(fusedResults[i], fusedResults[bestTailIndex])) {
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

        fusedResults.resize(config_.maxResults);
    } else {
        std::sort(fusedResults.begin(), fusedResults.end(), lexicalAwareLess);
        applySemanticRescueWindow();
    }

    return fusedResults;
}

float ResultFusion::getComponentWeight(ComponentResult::Source source) const {
    return componentSourceWeight(config_, source);
}

std::vector<SearchResult> ResultFusion::fuseConvex(const std::vector<ComponentResult>& results) {
    std::unordered_map<int, double> maxByComponent;
    maxByComponent.reserve(8);
    for (const auto& comp : results) {
        const int key = static_cast<int>(comp.source);
        auto& cur = maxByComponent[key];
        const double s = static_cast<double>(comp.score);
        if (s > cur) {
            cur = s;
        }
    }

    return fuseSinglePass(results, [this, &maxByComponent](const ComponentResult& comp) {
        const double weight = static_cast<double>(getComponentWeight(comp.source));
        if (weight <= 0.0) {
            return 0.0;
        }
        const int key = static_cast<int>(comp.source);
        auto it = maxByComponent.find(key);
        const double maxScore = (it != maxByComponent.end()) ? it->second : 0.0;
        if (maxScore <= 0.0) {
            return 0.0;
        }
        const double norm = std::clamp(static_cast<double>(comp.score) / maxScore, 0.0, 1.0);
        return weight * norm;
    });
}

std::vector<SearchResult>
ResultFusion::fuseWeightedLinearZScore(const std::vector<ComponentResult>& results) {
    auto isLexicalLeg = [](ComponentResult::Source s) noexcept {
        return s == ComponentResult::Source::Text || s == ComponentResult::Source::GraphText;
    };
    auto isSemanticLeg = [](ComponentResult::Source s) noexcept {
        return s == ComponentResult::Source::Vector || s == ComponentResult::Source::GraphVector ||
               s == ComponentResult::Source::EntityVector;
    };

    struct DocAcc {
        SearchResult result;
        double lexScore = 0.0;
        double semScore = 0.0;
        bool hasLex = false;
        bool hasSem = false;
    };
    std::unordered_map<std::string, DocAcc> byDoc;
    byDoc.reserve(results.size());

    for (const auto& comp : results) {
        const std::string key = detail::makeFusionDedupKey(comp, config_.enablePathDedupInFusion);
        auto& acc = byDoc[key];
        if (acc.result.document.sha256Hash.empty()) {
            acc.result.document.sha256Hash = comp.documentHash;
            acc.result.document.filePath = comp.filePath;
            acc.result.score = 0.0;
        }
        if (comp.snippet.has_value() && acc.result.snippet.empty()) {
            acc.result.snippet = comp.snippet.value();
        }
        const double s = static_cast<double>(comp.score);
        if (isLexicalLeg(comp.source)) {
            if (!acc.hasLex || s > acc.lexScore) {
                acc.lexScore = s;
            }
            acc.hasLex = true;
        } else if (isSemanticLeg(comp.source)) {
            if (!acc.hasSem || s > acc.semScore) {
                acc.semScore = s;
            }
            acc.hasSem = true;
        }
        accumulateComponentScore(acc.result, comp.source, s);
    }

    std::vector<std::string> poolKeys;
    poolKeys.reserve(byDoc.size());
    for (const auto& [k, acc] : byDoc) {
        if (acc.hasLex || acc.hasSem) {
            poolKeys.push_back(k);
        }
    }
    std::sort(poolKeys.begin(), poolKeys.end(), [&](const std::string& a, const std::string& b) {
        const auto& da = byDoc.at(a);
        const auto& db = byDoc.at(b);
        const double la = da.hasLex ? da.lexScore : -std::numeric_limits<double>::infinity();
        const double lb = db.hasLex ? db.lexScore : -std::numeric_limits<double>::infinity();
        if (la != lb)
            return la > lb;
        if (da.semScore != db.semScore)
            return da.semScore > db.semScore;
        return a < b;
    });
    const size_t poolSize = std::min(poolKeys.size(), config_.weightedLinearZScorePoolSize);
    poolKeys.resize(poolSize);
    if (poolKeys.empty()) {
        return {};
    }

    auto computeMoments = [&](auto extract) {
        double sum = 0.0;
        size_t n = 0;
        for (const auto& k : poolKeys) {
            auto opt = extract(byDoc.at(k));
            if (!opt)
                continue;
            sum += *opt;
            ++n;
        }
        const double mean = n > 0 ? sum / static_cast<double>(n) : 0.0;
        double sqsum = 0.0;
        for (const auto& k : poolKeys) {
            auto opt = extract(byDoc.at(k));
            if (!opt)
                continue;
            const double d = *opt - mean;
            sqsum += d * d;
        }
        const double stddev = n > 1 ? std::sqrt(sqsum / static_cast<double>(n - 1)) : 1.0;
        return std::pair{mean, stddev > 1e-9 ? stddev : 1.0};
    };
    auto extractLex = [](const DocAcc& a) -> std::optional<double> {
        return a.hasLex ? std::optional{a.lexScore} : std::nullopt;
    };
    auto extractSem = [](const DocAcc& a) -> std::optional<double> {
        return a.hasSem ? std::optional{a.semScore} : std::nullopt;
    };

    double lexMean = 0.0, lexSd = 1.0, semMean = 0.0, semSd = 1.0;
    if (config_.weightedLinearZScoreUseZScore) {
        std::tie(lexMean, lexSd) = computeMoments(extractLex);
        std::tie(semMean, semSd) = computeMoments(extractSem);
    }

    const double alpha =
        std::clamp(static_cast<double>(config_.weightedLinearZScoreAlpha), 0.0, 1.0);
    std::vector<SearchResult> out;
    out.reserve(poolKeys.size());
    for (const auto& k : poolKeys) {
        auto& acc = byDoc.at(k);
        double zLex = 0.0;
        double zSem = 0.0;
        if (acc.hasLex) {
            zLex = config_.weightedLinearZScoreUseZScore ? (acc.lexScore - lexMean) / lexSd
                                                         : acc.lexScore;
        }
        if (acc.hasSem) {
            zSem = config_.weightedLinearZScoreUseZScore ? (acc.semScore - semMean) / semSd
                                                         : acc.semScore;
        }
        acc.result.score = alpha * zLex + (1.0 - alpha) * zSem;
        out.push_back(std::move(acc.result));
    }
    std::sort(out.begin(), out.end(), [](const SearchResult& a, const SearchResult& b) {
        if (a.score != b.score)
            return a.score > b.score;
        return a.document.sha256Hash < b.document.sha256Hash;
    });
    return out;
}

} // namespace yams::search
