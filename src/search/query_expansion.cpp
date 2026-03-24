// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/search/query_expansion.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <string_view>
#include <unordered_set>

namespace yams::search {

namespace {

bool isNumericOnlyToken(std::string_view token) {
    return !token.empty() && std::all_of(token.begin(), token.end(),
                                         [](unsigned char c) { return std::isdigit(c) != 0; });
}

bool isWeakExpansionToken(std::string_view token) {
    static constexpr std::array<std::string_view, 14> kWeakTokens = {
        "show",         "shows",        "showing",  "shown",     "demonstrate",
        "demonstrates", "demonstrated", "indicate", "indicates", "indicated",
        "suggest",      "suggests",     "reveals",  "revealed",
    };
    return std::find(kWeakTokens.begin(), kWeakTokens.end(), token) != kWeakTokens.end();
}

} // namespace

float tokenFallbackSalience(const QueryToken& token) {
    const bool hasDigit = std::any_of(token.original.begin(), token.original.end(),
                                      [](unsigned char c) { return std::isdigit(c) != 0; });
    const size_t len = token.normalized.size();

    float score = 0.05f;
    if (hasDigit) {
        score += 1.25f;
    }
    if (len >= 10) {
        score += 0.75f;
    } else if (len >= 6) {
        score += 0.35f;
    } else if (len >= 3) {
        score += 0.10f;
    }
    return score;
}

static std::string joinQueryWindow(const std::vector<QueryToken>& tokens, size_t start,
                                   size_t length) {
    std::string phrase;
    for (size_t i = start; i < start + length; ++i) {
        if (!phrase.empty()) {
            phrase.push_back(' ');
        }
        phrase += tokens[i].original;
    }
    return phrase;
}

std::vector<std::string>
generateAnchoredSubPhrases(const std::string& query, size_t maxPhrases,
                           const std::unordered_map<std::string, float>* idfByToken) {
    if (maxPhrases == 0) {
        return {};
    }

    auto tokens = tokenizeQueryTokens(query);
    if (tokens.size() < 3) {
        return {};
    }

    struct AnchorCandidate {
        size_t index = 0;
        float salience = 0.0f;
    };

    std::vector<AnchorCandidate> anchors;
    anchors.reserve(tokens.size());
    for (size_t i = 0; i < tokens.size(); ++i) {
        const auto& token = tokens[i];
        if (token.normalized.size() < 2) {
            continue;
        }
        if (isNumericOnlyToken(token.normalized) || isWeakExpansionToken(token.normalized)) {
            continue;
        }

        float score = tokenFallbackSalience(token);
        if (idfByToken != nullptr) {
            auto it = idfByToken->find(token.normalized);
            if (it != idfByToken->end() && it->second > 0.0f) {
                score += it->second;
            }
        }

        anchors.push_back({i, score});
    }

    std::stable_sort(anchors.begin(), anchors.end(),
                     [](const auto& a, const auto& b) { return a.salience > b.salience; });

    std::vector<std::string> phrases;
    phrases.reserve(std::min(maxPhrases, tokens.size()));
    std::unordered_set<std::string> seen;
    seen.reserve(maxPhrases * 2);

    const auto fullNormalized = [&]() {
        std::string joined;
        for (const auto& token : tokens) {
            if (!joined.empty()) {
                joined.push_back(' ');
            }
            joined += token.normalized;
        }
        return joined;
    }();

    for (const auto& anchor : anchors) {
        if (phrases.size() >= maxPhrases) {
            break;
        }

        for (size_t length : {static_cast<size_t>(3), static_cast<size_t>(2)}) {
            if (tokens.size() < length) {
                continue;
            }

            const size_t startMin = (anchor.index + 1 >= length) ? (anchor.index + 1 - length) : 0;
            const size_t startMax = std::min(anchor.index, tokens.size() - length);

            std::vector<size_t> starts;
            starts.reserve(startMax >= startMin ? (startMax - startMin + 1) : 0);
            for (size_t start = startMin; start <= startMax; ++start) {
                starts.push_back(start);
            }

            std::stable_sort(starts.begin(), starts.end(), [&](size_t a, size_t b) {
                const size_t centerA = a + (length / 2);
                const size_t centerB = b + (length / 2);
                const auto distA =
                    (centerA > anchor.index) ? (centerA - anchor.index) : (anchor.index - centerA);
                const auto distB =
                    (centerB > anchor.index) ? (centerB - anchor.index) : (anchor.index - centerB);
                return distA < distB;
            });

            for (size_t start : starts) {
                if (phrases.size() >= maxPhrases) {
                    break;
                }

                std::string normalizedPhrase;
                for (size_t i = start; i < start + length; ++i) {
                    if (!normalizedPhrase.empty()) {
                        normalizedPhrase.push_back(' ');
                    }
                    if (isNumericOnlyToken(tokens[i].normalized) ||
                        isWeakExpansionToken(tokens[i].normalized)) {
                        normalizedPhrase.clear();
                        break;
                    }
                    normalizedPhrase += tokens[i].normalized;
                }

                if (normalizedPhrase.empty()) {
                    continue;
                }

                if (normalizedPhrase == fullNormalized || !seen.insert(normalizedPhrase).second) {
                    continue;
                }

                phrases.push_back(joinQueryWindow(tokens, start, length));
            }
        }
    }

    if (phrases.size() < maxPhrases) {
        std::string compressedNormalized;
        std::string compressedOriginal;
        size_t retained = 0;
        for (const auto& token : tokens) {
            if (token.normalized.size() < 2 || isNumericOnlyToken(token.normalized) ||
                isWeakExpansionToken(token.normalized)) {
                continue;
            }
            if (!compressedNormalized.empty()) {
                compressedNormalized.push_back(' ');
                compressedOriginal.push_back(' ');
            }
            compressedNormalized += token.normalized;
            compressedOriginal += token.original;
            retained++;
            if (retained >= 4) {
                break;
            }
        }

        if (retained >= 2 && compressedNormalized != fullNormalized &&
            seen.insert(compressedNormalized).second) {
            phrases.push_back(std::move(compressedOriginal));
        }
    }

    return phrases;
}

static std::string inferFallbackConceptType(std::string_view text) {
    const std::string normalized = normalizeGraphSurface(text);
    const auto hasDigit =
        std::any_of(text.begin(), text.end(), [](unsigned char c) { return std::isdigit(c) != 0; });
    const auto hasUpper =
        std::any_of(text.begin(), text.end(), [](unsigned char c) { return std::isupper(c) != 0; });
    if ((hasDigit && hasUpper) || normalized.starts_with("cd") || normalized.starts_with("il ") ||
        normalized.find("protein") != std::string::npos ||
        normalized.find("receptor") != std::string::npos ||
        normalized.find("kinase") != std::string::npos) {
        return "protein";
    }
    if (normalized.find("cell") != std::string::npos ||
        normalized.find("bipolar") != std::string::npos ||
        normalized.find("monocyte") != std::string::npos ||
        normalized.find("stem cell") != std::string::npos) {
        return "cell";
    }
    if (normalized.find("cancer") != std::string::npos ||
        normalized.find("disease") != std::string::npos ||
        normalized.find("tumor") != std::string::npos ||
        normalized.find("metast") != std::string::npos) {
        return "disease";
    }
    if (normalized.find("pathway") != std::string::npos ||
        normalized.find("response") != std::string::npos ||
        normalized.find("activation") != std::string::npos ||
        normalized.find("inhibition") != std::string::npos) {
        return "biological_process";
    }
    return "concept";
}

std::vector<QueryConcept>
generateFallbackQueryConcepts(const std::string& query,
                              const std::unordered_map<std::string, float>& idfByToken,
                              size_t maxConcepts) {
    if (maxConcepts == 0) {
        return {};
    }

    std::vector<QueryConcept> out;
    out.reserve(maxConcepts);
    std::unordered_set<std::string> seen;

    auto addConcept = [&](std::string text, float confidence) {
        const std::string normalized = normalizeGraphSurface(text);
        if (normalized.size() < 3 || !seen.insert(normalized).second || out.size() >= maxConcepts) {
            return;
        }
        QueryConcept qc;
        qc.text = std::move(text);
        qc.type = inferFallbackConceptType(qc.text);
        qc.confidence = std::clamp(confidence, 0.2f, 0.8f);
        qc.startOffset = 0;
        qc.endOffset = static_cast<uint32_t>(qc.text.size());
        out.push_back(std::move(qc));
    };

    for (const auto& phrase : generateAnchoredSubPhrases(query, maxConcepts, &idfByToken)) {
        addConcept(phrase, 0.62f);
    }

    auto tokens = tokenizeQueryTokens(query);
    std::vector<std::pair<std::string, float>> rankedTokens;
    rankedTokens.reserve(tokens.size());
    for (const auto& token : tokens) {
        if (token.normalized.size() < 2) {
            continue;
        }
        float score = tokenFallbackSalience(token);
        if (auto it = idfByToken.find(token.normalized); it != idfByToken.end()) {
            score += it->second;
        }
        rankedTokens.emplace_back(token.original, score);
    }
    std::stable_sort(rankedTokens.begin(), rankedTokens.end(),
                     [](const auto& a, const auto& b) { return a.second > b.second; });
    for (const auto& [text, score] : rankedTokens) {
        addConcept(text, 0.45f + std::min(0.25f, score * 0.02f));
        if (out.size() >= maxConcepts) {
            break;
        }
    }

    return out;
}

std::vector<FtsFallbackClause>
generateAggressiveFtsFallbackClauses(const std::string& query, size_t maxClauses,
                                     const std::unordered_map<std::string, float>& idfByToken) {
    if (maxClauses == 0) {
        return {};
    }

    const auto tokens = tokenizeQueryTokens(query);
    if (tokens.empty()) {
        return {};
    }

    struct RankedToken {
        QueryToken token;
        float salience = 0.0f;
    };

    std::vector<RankedToken> rankedTokens;
    rankedTokens.reserve(tokens.size());
    for (const auto& token : tokens) {
        if (token.normalized.size() < 2) {
            continue;
        }
        float score = tokenFallbackSalience(token);
        if (auto it = idfByToken.find(token.normalized);
            it != idfByToken.end() && it->second > 0.0f) {
            score += it->second;
        }
        rankedTokens.push_back({token, score});
    }
    std::stable_sort(rankedTokens.begin(), rankedTokens.end(),
                     [](const auto& a, const auto& b) { return a.salience > b.salience; });

    std::vector<FtsFallbackClause> clauses;
    clauses.reserve(maxClauses);
    std::unordered_set<std::string> seen;
    seen.reserve(maxClauses * 2);

    auto addClause = [&](std::string clause, float penalty) {
        if (clause.empty() || clauses.size() >= maxClauses) {
            return;
        }
        if (!seen.insert(clause).second) {
            return;
        }
        clauses.push_back({std::move(clause), std::clamp(penalty, 0.1f, 1.0f)});
    };

    auto addAndClauseFromTerms = [&](const std::vector<std::string>& terms, float penalty) {
        if (terms.empty()) {
            return;
        }
        std::string clause;
        if (terms.size() > 1) {
            clause.push_back('(');
        }
        for (size_t i = 0; i < terms.size(); ++i) {
            if (i > 0) {
                clause += " AND ";
            }
            clause += terms[i];
        }
        if (terms.size() > 1) {
            clause.push_back(')');
        }
        addClause(std::move(clause), penalty);
    };

    auto anchoredPhrases =
        generateAnchoredSubPhrases(query, std::min(maxClauses, size_t(8)), &idfByToken);
    for (const auto& phrase : anchoredPhrases) {
        if (clauses.size() >= maxClauses) {
            break;
        }
        std::vector<std::string> terms;
        for (const auto& token : tokenizeQueryTokens(phrase)) {
            if (token.normalized.size() >= 2) {
                terms.push_back(token.normalized);
            }
        }
        if (terms.size() >= 2) {
            addAndClauseFromTerms(terms, 0.78f);
        }
    }

    if (!rankedTokens.empty()) {
        const auto& anchor = rankedTokens.front().token.normalized;
        for (size_t i = 1; i < rankedTokens.size() && i < 5 && clauses.size() < maxClauses; ++i) {
            addAndClauseFromTerms({anchor, rankedTokens[i].token.normalized}, 0.68f);
        }
    }

    for (size_t i = 0; i < rankedTokens.size() && i < 6 && clauses.size() < maxClauses; ++i) {
        addClause(rankedTokens[i].token.normalized, 0.55f);
    }

    return clauses;
}

} // namespace yams::search
