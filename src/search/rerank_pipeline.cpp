#include <yams/search/rerank_pipeline.h>

#include <yams/metadata/metadata_repository.h>
#include <yams/search/query_text_utils.h>
#include <yams/search/search_engine.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <fstream>
#include <unordered_map>

namespace yams::search {
namespace {

std::string truncateSnippet(const std::string& content, size_t maxLen) {
    if (content.empty()) {
        return {};
    }
    if (content.size() <= maxLen) {
        return content;
    }
    std::string out = content.substr(0, maxLen);
    out.append("...");
    return out;
}

std::string makeHeadTailSnippet(std::string_view content, size_t maxLen) {
    if (content.size() <= maxLen) {
        return std::string(content);
    }
    if (maxLen <= 3) {
        return std::string(content.substr(0, maxLen));
    }

    const size_t bodyBudget = maxLen - 3;
    const size_t prefixLen = bodyBudget / 2;
    const size_t suffixLen = bodyBudget - prefixLen;

    std::string out;
    out.reserve(maxLen);
    out.append(content.substr(0, prefixLen));
    out.append("...");
    out.append(content.substr(content.size() - suffixLen, suffixLen));
    return out;
}

std::string buildRerankSnippet(const std::string& query, const std::string& content,
                               size_t maxLen) {
    if (content.empty() || maxLen == 0) {
        return {};
    }
    if (content.size() <= maxLen) {
        return content;
    }

    struct TokenHit {
        size_t pos = 0;
        size_t len = 0;
    };

    const auto queryTokens = tokenizeLower(query);
    const std::string loweredContent = toLowerCopy(content);
    std::vector<TokenHit> hits;
    hits.reserve(queryTokens.size());

    for (const auto& token : queryTokens) {
        if (token.size() < 3) {
            continue;
        }
        size_t pos = loweredContent.find(token);
        while (pos != std::string::npos) {
            hits.push_back(TokenHit{pos, token.size()});
            pos = loweredContent.find(token, pos + 1);
        }
    }

    if (hits.empty()) {
        if (content.size() <= maxLen + (maxLen / 2)) {
            return truncateSnippet(content, maxLen);
        }
        return makeHeadTailSnippet(content, maxLen);
    }

    size_t bestStart = 0;
    size_t bestHitCount = 0;
    size_t bestCovered = 0;
    for (const auto& hit : hits) {
        const size_t start = hit.pos > (maxLen / 3) ? hit.pos - (maxLen / 3) : 0;
        const size_t end = std::min(content.size(), start + maxLen);

        size_t hitCount = 0;
        size_t covered = 0;
        for (const auto& candidate : hits) {
            if (candidate.pos >= start && candidate.pos < end) {
                hitCount++;
                covered += candidate.len;
            }
        }

        if (hitCount > bestHitCount || (hitCount == bestHitCount && covered > bestCovered)) {
            bestStart = start;
            bestHitCount = hitCount;
            bestCovered = covered;
        }
    }

    const bool clippedLeft = bestStart > 0;
    const size_t ellipsisBudget = clippedLeft ? 3 : 0;
    if (ellipsisBudget >= maxLen) {
        return truncateSnippet(content, maxLen);
    }

    const size_t bodyBudget = maxLen - ellipsisBudget;
    const size_t bodyEnd = std::min(content.size(), bestStart + bodyBudget);

    std::string out;
    out.reserve(maxLen);
    if (clippedLeft) {
        out.append("...");
    }
    out.append(content.substr(bestStart, bodyEnd - bestStart));
    if (out.size() < maxLen && bodyEnd < content.size()) {
        const size_t remaining = maxLen - out.size();
        out.append(content.substr(bodyEnd, remaining));
    }
    return out;
}

bool hasQueryTokenHit(const std::string& query, const std::string& content) {
    const auto queryTokens = tokenizeLower(query);
    const std::string loweredContent = toLowerCopy(content);
    for (const auto& token : queryTokens) {
        if (token.size() < 3) {
            continue;
        }
        if (loweredContent.find(token) != std::string::npos) {
            return true;
        }
    }
    return false;
}

std::vector<std::string> buildRerankPassages(const std::string& query, const std::string& content,
                                             size_t maxLen) {
    std::vector<std::string> passages;
    if (content.empty() || maxLen == 0) {
        return passages;
    }

    const std::string primary = buildRerankSnippet(query, content, maxLen);
    if (!primary.empty()) {
        passages.push_back(primary);
    }

    if (!hasQueryTokenHit(query, content) && content.size() > maxLen * 2) {
        struct Segment {
            size_t start = 0;
            std::string text;
        };

        std::vector<Segment> segments;
        size_t segmentStart = 0;
        for (size_t i = 0; i < content.size(); ++i) {
            const char ch = content[i];
            if (ch != '.' && ch != ';' && ch != '\n') {
                continue;
            }

            size_t rawStart = segmentStart;
            size_t rawEnd = i + 1;
            while (rawStart < rawEnd &&
                   std::isspace(static_cast<unsigned char>(content[rawStart]))) {
                ++rawStart;
            }
            while (rawEnd > rawStart &&
                   std::isspace(static_cast<unsigned char>(content[rawEnd - 1]))) {
                --rawEnd;
            }

            if (rawEnd > rawStart) {
                segments.push_back(Segment{rawStart, content.substr(rawStart, rawEnd - rawStart)});
            }
            segmentStart = i + 1;
        }
        if (segmentStart < content.size()) {
            size_t rawStart = segmentStart;
            size_t rawEnd = content.size();
            while (rawStart < rawEnd &&
                   std::isspace(static_cast<unsigned char>(content[rawStart]))) {
                ++rawStart;
            }
            while (rawEnd > rawStart &&
                   std::isspace(static_cast<unsigned char>(content[rawEnd - 1]))) {
                --rawEnd;
            }
            if (rawEnd > rawStart) {
                segments.push_back(Segment{rawStart, content.substr(rawStart, rawEnd - rawStart)});
            }
        }

        const size_t prefixCoverage = std::min(content.size(), maxLen);
        size_t supplementalCount = 0;
        for (auto it = segments.rbegin(); it != segments.rend() && supplementalCount < 3; ++it) {
            if (it->start < prefixCoverage / 2) {
                continue;
            }
            std::string passage = truncateSnippet(it->text, maxLen);
            if (passage.empty() ||
                std::find(passages.begin(), passages.end(), passage) != passages.end()) {
                continue;
            }
            passages.push_back(std::move(passage));
            supplementalCount++;
        }

        if (supplementalCount == 0) {
            const size_t maxStart = content.size() > maxLen ? content.size() - maxLen : 0;
            std::vector<size_t> supplementalStarts = {
                (content.size() - maxLen) / 2,
                std::min(maxStart, (content.size() * 2) / 3 > maxLen / 2
                                       ? (content.size() * 2) / 3 - maxLen / 2
                                       : size_t(0)),
            };

            for (size_t start : supplementalStarts) {
                const std::string passage = content.substr(start, maxLen);
                if (!passage.empty() &&
                    std::find(passages.begin(), passages.end(), passage) == passages.end()) {
                    passages.push_back(passage);
                }
            }
        }
    }

    return passages;
}

std::unordered_map<size_t, std::string>
loadMetadataPreviewByIndex(const std::shared_ptr<metadata::MetadataRepository>& metadataRepo,
                           const std::vector<metadata::SearchResult>& results, size_t rerankWindow,
                           size_t snippetMaxChars) {
    std::unordered_map<size_t, std::string> metadataPreviewByIndex;
    if (!metadataRepo || rerankWindow == 0) {
        return metadataPreviewByIndex;
    }

    std::unordered_map<std::string, std::vector<size_t>> hashToIndices;
    hashToIndices.reserve(rerankWindow);
    for (size_t i = 0; i < rerankWindow; ++i) {
        const bool snippetLooksLikePath =
            !results[i].snippet.empty() && results[i].snippet == results[i].document.filePath;
        if (!results[i].snippet.empty() && !snippetLooksLikePath) {
            continue;
        }
        const std::string& hash = results[i].document.sha256Hash;
        if (!hash.empty()) {
            hashToIndices[hash].push_back(i);
        }
    }

    if (hashToIndices.empty()) {
        return metadataPreviewByIndex;
    }

    std::vector<std::string> hashes;
    hashes.reserve(hashToIndices.size());
    for (const auto& [hash, _] : hashToIndices) {
        hashes.push_back(hash);
    }

    const size_t previewLimit =
        std::max<size_t>(snippetMaxChars + 64, std::max<size_t>(snippetMaxChars * 4, 1024));

    auto combinedResult =
        metadataRepo->batchGetDocumentsWithContentPreview(hashes, static_cast<int>(previewLimit));
    if (!combinedResult) {
        return metadataPreviewByIndex;
    }

    metadataPreviewByIndex.reserve(rerankWindow);
    for (const auto& [hash, docAndPreview] : combinedResult.value()) {
        const auto& [docInfo, preview] = docAndPreview;
        (void)docInfo;
        if (preview.empty()) {
            continue;
        }
        auto it = hashToIndices.find(hash);
        if (it == hashToIndices.end()) {
            continue;
        }
        for (size_t idx : it->second) {
            metadataPreviewByIndex[idx] = preview;
        }
    }

    return metadataPreviewByIndex;
}

std::string resolveSourceText(const metadata::SearchResult& result, size_t resultIndex,
                              size_t snippetMaxChars,
                              const std::unordered_map<size_t, std::string>& previews) {
    const bool snippetLooksLikePath =
        !result.snippet.empty() && result.snippet == result.document.filePath;
    if (!result.snippet.empty() && !snippetLooksLikePath) {
        return result.snippet;
    }

    if (auto it = previews.find(resultIndex); it != previews.end()) {
        return it->second;
    }

    if (!result.snippet.empty()) {
        return result.snippet;
    }

    if (!result.document.filePath.empty()) {
        std::ifstream ifs(result.document.filePath, std::ios::in | std::ios::binary);
        if (ifs) {
            const size_t readLimit = snippetMaxChars + 64;
            std::string buf(readLimit, '\0');
            ifs.read(buf.data(), static_cast<std::streamsize>(readLimit));
            buf.resize(static_cast<size_t>(ifs.gcount()));
            if (!buf.empty()) {
                spdlog::debug("[reranker] Loaded {} bytes from file for doc {}", buf.size(),
                              resultIndex);
                return buf;
            }
        }
    }

    return {};
}

} // namespace

RerankInputCollector::RerankInputCollector(
    std::shared_ptr<metadata::MetadataRepository> metadataRepo, size_t snippetMaxChars)
    : metadataRepo_(std::move(metadataRepo)), snippetMaxChars_(snippetMaxChars) {}

RerankInputs RerankInputCollector::collect(const std::string& query,
                                           const std::vector<metadata::SearchResult>& results,
                                           size_t rerankWindow) const {
    RerankInputs out;
    out.snippets.reserve(rerankWindow);
    out.passageDocIndices.reserve(rerankWindow);

    const auto previews =
        loadMetadataPreviewByIndex(metadataRepo_, results, rerankWindow, snippetMaxChars_);
    for (size_t i = 0; i < rerankWindow; ++i) {
        auto text = resolveSourceText(results[i], i, snippetMaxChars_, previews);
        if (text.empty()) {
            spdlog::debug("[reranker] Skipping doc {} (no snippet or file content)", i);
            continue;
        }

        auto passages = buildRerankPassages(query, text, snippetMaxChars_);
        for (auto& passage : passages) {
            out.snippets.push_back(std::move(passage));
            out.passageDocIndices.push_back(i);
        }
    }

    return out;
}

RerankScoreApplier::RerankScoreApplier(const SearchEngineConfig& config) : config_(config) {}

double RerankScoreApplier::resolveEffectiveWeight(const std::vector<float>& scores) const {
    double effectiveWeight = config_.rerankWeight;
    if (!config_.rerankReplaceScores && config_.rerankAdaptiveBlend && !scores.empty()) {
        const float maxRerankScore = *std::max_element(scores.begin(), scores.end());
        effectiveWeight = std::clamp(static_cast<double>(config_.rerankWeight) * maxRerankScore,
                                     static_cast<double>(config_.rerankAdaptiveFloor),
                                     static_cast<double>(config_.rerankWeight));
        spdlog::debug("[reranker] Adaptive blend: maxScore={:.4f} effectiveWeight={:.4f} "
                      "(base={:.3f})",
                      maxRerankScore, effectiveWeight, config_.rerankWeight);
    }
    return effectiveWeight;
}

void RerankScoreApplier::apply(std::vector<metadata::SearchResult>& results, size_t rerankWindow,
                               const std::vector<float>& scores,
                               const std::vector<size_t>& rerankPassageDocIndices) const {
    const double effectiveWeight = resolveEffectiveWeight(scores);
    std::vector<double> bestRerankScoreByDoc(rerankWindow, 0.0);
    std::vector<bool> rerankScorePresentByDoc(rerankWindow, false);
    for (size_t i = 0; i < scores.size() && i < rerankPassageDocIndices.size(); ++i) {
        const size_t idx = rerankPassageDocIndices[i];
        if (idx >= rerankWindow) {
            continue;
        }
        const double rerankScore = static_cast<double>(scores[i]);
        if (!rerankScorePresentByDoc[idx] || rerankScore > bestRerankScoreByDoc[idx]) {
            bestRerankScoreByDoc[idx] = rerankScore;
            rerankScorePresentByDoc[idx] = true;
        }
    }

    for (size_t idx = 0; idx < rerankWindow; ++idx) {
        if (!rerankScorePresentByDoc[idx]) {
            continue;
        }
        const double originalScore = results[idx].score;
        const double rerankScore = bestRerankScoreByDoc[idx];

        results[idx].score =
            config_.rerankReplaceScores
                ? rerankScore
                : rerankScore * effectiveWeight + originalScore * (1.0 - effectiveWeight);
        results[idx].rerankerScore = rerankScore;
    }

    std::sort(results.begin(), results.begin() + static_cast<ptrdiff_t>(rerankWindow),
              [](const metadata::SearchResult& a, const metadata::SearchResult& b) {
                  return a.score > b.score;
              });
}

} // namespace yams::search
