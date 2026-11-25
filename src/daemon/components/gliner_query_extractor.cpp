#include <yams/daemon/components/gliner_query_extractor.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <simeon/text_rank.hpp>
#include <yams/daemon/resource/abi_entity_extractor_adapter.h>
#include <yams/plugins/entity_extractor_v2.h>
#include <yams/search/query_text_utils.h>

namespace yams::daemon {

namespace {
// Minimum confidence threshold for including concepts
constexpr float kMinConfidence = 0.4f;
constexpr std::size_t kMaxEntityTextLen = 160;

std::string trimAndCollapse(std::string_view input) {
    std::string out = yams::search::trimAndCollapseWhitespace(input);

    while (!out.empty() && std::ispunct(static_cast<unsigned char>(out.front())) &&
           out.front() != '_' && out.front() != '-') {
        out.erase(out.begin());
    }
    while (!out.empty() && std::ispunct(static_cast<unsigned char>(out.back())) &&
           out.back() != '_' && out.back() != '-') {
        out.pop_back();
    }

    return out;
}

bool isLikelyNoiseEntity(std::string_view text, std::string_view normalizedType) {
    if (text.size() < 2 || text.size() > kMaxEntityTextLen) {
        return true;
    }

    bool hasAlnum = false;
    bool hasAlpha = false;
    for (unsigned char c : text) {
        if (std::isalnum(c)) {
            hasAlnum = true;
        }
        if (std::isalpha(c)) {
            hasAlpha = true;
        }
    }

    if (!hasAlnum) {
        return true;
    }

    const std::string normalizedText = yams::search::normalizeEntityTextForKey(text);
    if (yams::search::isLowValueEntityText(normalizedText, normalizedType)) {
        return true;
    }

    return !hasAlpha && text.size() <= 2;
}

} // namespace

search::EntityExtractionFunc
createGlinerExtractionFunc(std::vector<std::shared_ptr<AbiEntityExtractorAdapter>> extractors) {
    // Find the first extractor that supports text/plain
    std::shared_ptr<AbiEntityExtractorAdapter> textExtractor;
    for (const auto& ext : extractors) {
        if (ext && ext->supportsContentType("text/plain")) {
            textExtractor = ext;
            break;
        }
    }

    if (!textExtractor) {
        spdlog::debug("createGlinerExtractionFunc: No GLiNER extractor found");
        return nullptr;
    }

    spdlog::info("createGlinerExtractionFunc: GLiNER entity extractor available");

    auto extractorMutex = std::make_shared<std::mutex>();

    // Capture extractors by value to extend plugin lifetime while search callbacks are active.
    return [extractors = std::move(extractors), textExtractor = std::move(textExtractor),
            extractorMutex = std::move(extractorMutex)](
               const std::string& content,
               const std::vector<std::string>& entityTypes) -> Result<search::QueryConceptResult> {
        (void)extractors;

        search::QueryConceptResult result;
        result.usedGliner = false;

        if (content.empty()) {
            return result;
        }

        auto startTime = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> extractorLock(*extractorMutex);

        // Build entity types array for extraction
        std::vector<const char*> types;
        std::unordered_set<std::string> requestedTypes;
        if (entityTypes.empty()) {
            for (const auto& t : yams::search::defaultQueryEntityTypes()) {
                types.push_back(t.data());
                requestedTypes.insert(yams::search::canonicalizeEntityType(t));
            }
        } else {
            for (const auto& t : entityTypes) {
                types.push_back(t.c_str());
                requestedTypes.insert(yams::search::canonicalizeEntityType(t));
            }
        }

        // Call GLiNER entity extraction
        auto* extractionResult = textExtractor->extract(content, types.data(), types.size(),
                                                        nullptr, // language
                                                        nullptr  // file_path
        );

        if (!extractionResult) {
            spdlog::warn("GLiNER extraction returned null");
            return result;
        }

        const auto extractionResultGuard =
            std::unique_ptr<yams_entity_extraction_result_v2,
                            std::function<void(yams_entity_extraction_result_v2*)>>(
                extractionResult, [textExtractor](yams_entity_extraction_result_v2* toFree) {
                    if (toFree) {
                        textExtractor->freeResult(toFree);
                    }
                });

        result.usedGliner = true;

        std::unordered_map<std::string, search::QueryConcept> bestByKey;
        bestByKey.reserve(extractionResult->entity_count);

        // Convert extraction result to QueryConcepts
        for (size_t i = 0; i < extractionResult->entity_count; ++i) {
            const auto& entity = extractionResult->entities[i];

            search::QueryConcept qc;
            qc.text = trimAndCollapse(entity.text ? entity.text : "");
            qc.type = yams::search::canonicalizeEntityType(entity.type ? entity.type : "", qc.text);
            qc.confidence = entity.confidence;
            qc.startOffset = entity.start_offset;
            qc.endOffset = entity.end_offset;

            // Only include concepts with reasonable confidence
            if (qc.confidence < kMinConfidence || qc.text.empty() ||
                isLikelyNoiseEntity(qc.text, qc.type)) {
                continue;
            }

            if (!requestedTypes.empty() && requestedTypes.find(qc.type) == requestedTypes.end()) {
                continue;
            }

            std::string dedupeKey = qc.type;
            dedupeKey.push_back('|');
            dedupeKey.append(yams::search::normalizeEntityTextForKey(qc.text));

            auto it = bestByKey.find(dedupeKey);
            if (it == bestByKey.end() || qc.confidence > it->second.confidence) {
                bestByKey[std::move(dedupeKey)] = std::move(qc);
            }
        }

        result.concepts.reserve(bestByKey.size());
        for (auto& [dedupeKey, qc] : bestByKey) {
            (void)dedupeKey;
            result.concepts.push_back(std::move(qc));
        }

        std::sort(result.concepts.begin(), result.concepts.end(),
                  [](const search::QueryConcept& a, const search::QueryConcept& b) {
                      if (a.confidence != b.confidence) {
                          return a.confidence > b.confidence;
                      }
                      return a.text < b.text;
                  });

        auto endTime = std::chrono::steady_clock::now();
        result.extractionTimeMs =
            std::chrono::duration<double, std::milli>(endTime - startTime).count();

        spdlog::debug("GLiNER query extraction: {} concepts in {:.2f}ms", result.concepts.size(),
                      result.extractionTimeMs);

        return result;
    };
}

namespace {

constexpr std::size_t kMaxSimeonScanBytes = 4096;
constexpr std::size_t kMaxSimeonTitleChars = 120;
constexpr std::size_t kMaxSimeonEntities = 12;

std::string truncateAtWordBoundary(std::string text, std::size_t maxLen) {
    if (text.size() <= maxLen) {
        return text;
    }
    std::size_t cut = text.rfind(' ', maxLen);
    if (cut == std::string::npos || cut < maxLen / 2) {
        cut = maxLen;
    }
    text.resize(cut);
    while (!text.empty() && (std::isspace(static_cast<unsigned char>(text.back())) ||
                             std::ispunct(static_cast<unsigned char>(text.back())))) {
        text.pop_back();
    }
    return text;
}

std::optional<search::QueryConcept> extractStructuralTitle(std::string_view text) {
    // 1. HTML <title>...</title> or <h1>...</h1>
    auto findHtmlTag = [&](std::string_view openTag,
                           std::string_view closeTag) -> std::optional<search::QueryConcept> {
        std::string lower;
        const std::size_t scanLen = std::min<std::size_t>(text.size(), 2048);
        lower.resize(scanLen);
        for (std::size_t i = 0; i < scanLen; ++i) {
            lower[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(text[i])));
        }
        const auto openPos = lower.find(openTag);
        if (openPos == std::string::npos) {
            return std::nullopt;
        }
        const auto gtPos = lower.find('>', openPos + openTag.size());
        if (gtPos == std::string::npos) {
            return std::nullopt;
        }
        const auto closePos = lower.find(closeTag, gtPos + 1);
        if (closePos == std::string::npos || closePos <= gtPos + 1) {
            return std::nullopt;
        }
        std::string cleaned = trimAndCollapse(text.substr(gtPos + 1, closePos - gtPos - 1));
        cleaned = truncateAtWordBoundary(std::move(cleaned), kMaxSimeonTitleChars);
        if (cleaned.size() >= 3 && !isLikelyNoiseEntity(cleaned, "title")) {
            return search::QueryConcept{.text = std::move(cleaned),
                                        .type = "title",
                                        .confidence = 0.90f,
                                        .startOffset = static_cast<uint32_t>(gtPos + 1),
                                        .endOffset = static_cast<uint32_t>(closePos)};
        }
        return std::nullopt;
    };

    if (auto htmlTitle = findHtmlTag("<title", "</title>")) {
        return htmlTitle;
    }
    if (auto htmlH1 = findHtmlTag("<h1", "</h1>")) {
        return htmlH1;
    }

    // 2. Line-based structural headings: YAML frontmatter title, Markdown ATX, Setext underline
    bool inFrontmatter = false;
    std::string_view prevLine;
    std::size_t prevLineStart = 0;
    std::size_t pos = 0;
    std::size_t lineIdx = 0;
    while (pos < text.size() && lineIdx < 24) {
        const auto nl = text.find('\n', pos);
        const std::size_t lineEnd = (nl == std::string_view::npos) ? text.size() : nl;
        std::string_view line = text.substr(pos, lineEnd - pos);
        if (!line.empty() && line.back() == '\r') {
            line.remove_suffix(1);
        }

        // Trim leading spaces
        std::size_t lead = 0;
        while (lead < line.size() && std::isspace(static_cast<unsigned char>(line[lead]))) {
            ++lead;
        }
        std::string_view trimmed = line.substr(lead);

        if (lineIdx == 0 && trimmed == "---") {
            inFrontmatter = true;
        } else if (inFrontmatter) {
            if (trimmed == "---" || trimmed == "...") {
                inFrontmatter = false;
            } else if (trimmed.size() > 6 &&
                       (trimmed.starts_with("title:") || trimmed.starts_with("Title:"))) {
                std::string_view val = trimmed.substr(6);
                std::string cleaned = trimAndCollapse(val);
                if (cleaned.size() >= 2 && ((cleaned.front() == '"' && cleaned.back() == '"') ||
                                            (cleaned.front() == '\'' && cleaned.back() == '\''))) {
                    cleaned =
                        trimAndCollapse(std::string_view(cleaned).substr(1, cleaned.size() - 2));
                }
                cleaned = truncateAtWordBoundary(std::move(cleaned), kMaxSimeonTitleChars);
                if (cleaned.size() >= 3 && !isLikelyNoiseEntity(cleaned, "title")) {
                    return search::QueryConcept{.text = std::move(cleaned),
                                                .type = "title",
                                                .confidence = 0.92f,
                                                .startOffset = static_cast<uint32_t>(pos + lead),
                                                .endOffset = static_cast<uint32_t>(lineEnd)};
                }
            }
        } else {
            // Markdown ATX heading (# Title or ## Title)
            if (!trimmed.empty() && trimmed.front() == '#') {
                std::size_t hashes = 0;
                while (hashes < trimmed.size() && trimmed[hashes] == '#') {
                    ++hashes;
                }
                if (hashes >= 1 && hashes <= 2 && hashes < trimmed.size() &&
                    std::isspace(static_cast<unsigned char>(trimmed[hashes]))) {
                    std::string cleaned = trimAndCollapse(trimmed.substr(hashes));
                    cleaned = truncateAtWordBoundary(std::move(cleaned), kMaxSimeonTitleChars);
                    if (cleaned.size() >= 3 && !isLikelyNoiseEntity(cleaned, "title")) {
                        return search::QueryConcept{.text = std::move(cleaned),
                                                    .type = "title",
                                                    .confidence = (hashes == 1) ? 0.92f : 0.85f,
                                                    .startOffset =
                                                        static_cast<uint32_t>(pos + lead),
                                                    .endOffset = static_cast<uint32_t>(lineEnd)};
                    }
                }
            }
            // Setext heading underline (=== or ---)
            if (!prevLine.empty() && trimmed.size() >= 3 &&
                (trimmed.find_first_not_of('=') == std::string_view::npos ||
                 trimmed.find_first_not_of('-') == std::string_view::npos)) {
                std::string cleaned = trimAndCollapse(prevLine);
                cleaned = truncateAtWordBoundary(std::move(cleaned), kMaxSimeonTitleChars);
                if (cleaned.size() >= 3 && !isLikelyNoiseEntity(cleaned, "title")) {
                    return search::QueryConcept{
                        .text = std::move(cleaned),
                        .type = "title",
                        .confidence = (trimmed.front() == '=') ? 0.90f : 0.84f,
                        .startOffset = static_cast<uint32_t>(prevLineStart),
                        .endOffset = static_cast<uint32_t>(prevLineStart + prevLine.size())};
                }
            }
        }

        prevLine = trimmed;
        prevLineStart = pos + lead;
        pos = (nl == std::string_view::npos) ? text.size() : (nl + 1);
        ++lineIdx;
    }

    return std::nullopt;
}

bool isTechIdentifier(std::string_view tok) {
    if (tok.size() < 2 || tok.size() > 64) {
        return false;
    }
    bool hasUpper = false;
    bool hasLower = false;
    bool hasDigit = false;
    bool hasUnderscore = false;
    bool hasInternalUpper = false;
    bool allUpperAlpha = true;
    for (std::size_t i = 0; i < tok.size(); ++i) {
        const unsigned char c = static_cast<unsigned char>(tok[i]);
        if (std::isupper(c)) {
            hasUpper = true;
            if (i > 0 && hasLower) {
                hasInternalUpper = true;
            }
        } else if (std::islower(c)) {
            hasLower = true;
            allUpperAlpha = false;
        } else if (std::isdigit(c)) {
            hasDigit = true;
            allUpperAlpha = false;
        } else if (c == '_') {
            hasUnderscore = true;
            allUpperAlpha = false;
        } else {
            allUpperAlpha = false;
        }
    }
    // Acronyms (API, HNSW, SQL, WAL, SIMD)
    if (allUpperAlpha && tok.size() >= 2 && tok.size() <= 8) {
        return true;
    }
    // camelCase / PascalCase with internal capital transition
    if (hasInternalUpper && tok.size() >= 4) {
        return true;
    }
    // snake_case identifier with at least one letter
    if (hasUnderscore && (hasLower || hasUpper) && tok.size() >= 4 && tok.front() != '_' &&
        tok.back() != '_') {
        return true;
    }
    // Alphanumeric versioned/tech tokens (e.g. FTS5, SHA256, C++23, H2O, HTTP2)
    if (hasDigit && (hasUpper || hasLower) && tok.size() >= 3 &&
        std::isalpha(static_cast<unsigned char>(tok.front()))) {
        return true;
    }
    return false;
}

bool isCapitalizedWord(std::string_view tok) {
    if (tok.size() < 2 || !std::isupper(static_cast<unsigned char>(tok.front()))) {
        return false;
    }
    bool hasLower = false;
    for (std::size_t i = 1; i < tok.size(); ++i) {
        const unsigned char c = static_cast<unsigned char>(tok[i]);
        if (std::islower(c)) {
            hasLower = true;
        } else if (!std::isalpha(c) && c != '-' && c != '\'') {
            return false;
        }
    }
    return hasLower;
}

} // namespace

search::EntityExtractionFunc createSimeonExtractionFunc() {
    spdlog::info("createSimeonExtractionFunc: Simeon training-free title & entity extractor "
                 "available");

    return [](const std::string& content,
              const std::vector<std::string>& entityTypes) -> Result<search::QueryConceptResult> {
        search::QueryConceptResult result;
        result.usedGliner = false;

        if (content.empty()) {
            return result;
        }

        const auto startTime = std::chrono::steady_clock::now();

        std::unordered_set<std::string> requestedTypes;
        if (entityTypes.empty()) {
            for (const auto& t : yams::search::defaultQueryEntityTypes()) {
                requestedTypes.insert(yams::search::canonicalizeEntityType(t));
            }
        } else {
            for (const auto& t : entityTypes) {
                requestedTypes.insert(yams::search::canonicalizeEntityType(t));
            }
        }

        const bool wantTitle = entityTypes.empty() || requestedTypes.contains("title") ||
                               requestedTypes.contains("heading");

        std::string techType;
        if (requestedTypes.contains("concept")) {
            techType = "concept";
        } else if (requestedTypes.contains("method")) {
            techType = "method";
        } else if (requestedTypes.contains("organization")) {
            techType = "organization";
        }

        std::string phraseType;
        if (requestedTypes.contains("concept")) {
            phraseType = "concept";
        } else if (requestedTypes.contains("organization")) {
            phraseType = "organization";
        } else if (requestedTypes.contains("method")) {
            phraseType = "method";
        }

        const std::string_view bounded(content.data(),
                                       std::min<std::size_t>(content.size(), kMaxSimeonScanBytes));

        // 1. Title extraction: structural heading first, then Simeon TextRank fallback
        if (wantTitle) {
            if (auto structural = extractStructuralTitle(bounded)) {
                result.concepts.push_back(std::move(*structural));
            } else {
                static const simeon::TextRank kTextRank{};
                const std::string_view top = kTextRank.top_sentence(bounded);
                if (!top.empty()) {
                    std::string cleaned = trimAndCollapse(top);
                    cleaned = truncateAtWordBoundary(std::move(cleaned), kMaxSimeonTitleChars);
                    if (cleaned.size() >= 3 && !isLikelyNoiseEntity(cleaned, "title")) {
                        const std::size_t startOff =
                            (top.data() >= bounded.data() &&
                             top.data() <= bounded.data() + bounded.size())
                                ? static_cast<std::size_t>(top.data() - bounded.data())
                                : 0u;
                        result.concepts.push_back(search::QueryConcept{
                            .text = std::move(cleaned),
                            .type = "title",
                            .confidence = 0.72f,
                            .startOffset = static_cast<uint32_t>(startOff),
                            .endOffset = static_cast<uint32_t>(startOff + top.size())});
                    }
                }
            }
        }

        // 2. Training-free entity/concept extraction (technical identifiers + capitalized phrases)
        if (!techType.empty() || !phraseType.empty()) {
            struct CandidateAgg {
                std::string text;
                std::string type;
                float baseConfidence = 0.60f;
                uint32_t startOffset = 0;
                uint32_t endOffset = 0;
                uint32_t count = 0;
            };
            std::unordered_map<std::string, CandidateAgg> aggs;

            auto recordCandidate = [&](std::string_view raw, const std::string& type, float base,
                                       std::size_t startOff, std::size_t endOff) {
                if (type.empty()) {
                    return;
                }
                std::string cleaned = trimAndCollapse(raw);
                if (cleaned.empty() || isLikelyNoiseEntity(cleaned, type)) {
                    return;
                }
                std::string key = type;
                key.push_back('|');
                key.append(yams::search::normalizeEntityTextForKey(cleaned));
                auto it = aggs.find(key);
                if (it == aggs.end()) {
                    aggs.emplace(std::move(key),
                                 CandidateAgg{.text = std::move(cleaned),
                                              .type = type,
                                              .baseConfidence = base,
                                              .startOffset = static_cast<uint32_t>(startOff),
                                              .endOffset = static_cast<uint32_t>(endOff),
                                              .count = 1});
                } else {
                    ++it->second.count;
                }
            };

            struct TokenSpan {
                std::string_view text;
                std::size_t start;
                std::size_t end;
                bool sentenceBreakBefore;
            };
            std::vector<TokenSpan> tokens;
            tokens.reserve(256);

            bool sawSentenceBreak = true;
            std::size_t i = 0;
            while (i < bounded.size()) {
                const unsigned char c = static_cast<unsigned char>(bounded[i]);
                if (std::isalnum(c) || c == '_') {
                    const std::size_t tokStart = i;
                    while (i < bounded.size()) {
                        const unsigned char t = static_cast<unsigned char>(bounded[i]);
                        if (std::isalnum(t) || t == '_' || t == '-' || t == '+') {
                            ++i;
                        } else {
                            break;
                        }
                    }
                    tokens.push_back(
                        {bounded.substr(tokStart, i - tokStart), tokStart, i, sawSentenceBreak});
                    sawSentenceBreak = false;
                } else {
                    if (c == '.' || c == '!' || c == '?' || c == '\n' || c == ';') {
                        sawSentenceBreak = true;
                    }
                    ++i;
                }
            }

            // Technical tokens
            if (!techType.empty()) {
                for (const auto& tok : tokens) {
                    if (isTechIdentifier(tok.text)) {
                        recordCandidate(tok.text, techType, 0.68f, tok.start, tok.end);
                    }
                }
            }

            // Contiguous multi-word capitalized phrases (2..4 words)
            if (!phraseType.empty()) {
                for (std::size_t idx = 0; idx < tokens.size(); ++idx) {
                    if (!isCapitalizedWord(tokens[idx].text)) {
                        continue;
                    }
                    std::size_t runEnd = idx;
                    while (runEnd + 1 < tokens.size() && (runEnd - idx) < 3 &&
                           !tokens[runEnd + 1].sentenceBreakBefore &&
                           isCapitalizedWord(tokens[runEnd + 1].text) &&
                           tokens[runEnd + 1].start <= tokens[runEnd].end + 2) {
                        ++runEnd;
                    }
                    if (runEnd > idx) {
                        const std::size_t spanStart = tokens[idx].start;
                        const std::size_t spanEnd = tokens[runEnd].end;
                        recordCandidate(bounded.substr(spanStart, spanEnd - spanStart), phraseType,
                                        0.64f, spanStart, spanEnd);
                        idx = runEnd;
                    }
                }
            }

            std::vector<search::QueryConcept> entities;
            entities.reserve(aggs.size());
            for (auto& [_, agg] : aggs) {
                const float freqBonus =
                    std::min(0.14f, static_cast<float>(agg.count > 1 ? agg.count - 1 : 0) * 0.04f);
                const float conf = std::min(0.88f, agg.baseConfidence + freqBonus);
                if (conf >= kMinConfidence) {
                    entities.push_back(search::QueryConcept{.text = std::move(agg.text),
                                                            .type = std::move(agg.type),
                                                            .confidence = conf,
                                                            .startOffset = agg.startOffset,
                                                            .endOffset = agg.endOffset});
                }
            }

            std::sort(entities.begin(), entities.end(),
                      [](const search::QueryConcept& a, const search::QueryConcept& b) {
                          if (a.confidence != b.confidence) {
                              return a.confidence > b.confidence;
                          }
                          return a.text < b.text;
                      });
            if (entities.size() > kMaxSimeonEntities) {
                entities.resize(kMaxSimeonEntities);
            }
            for (auto& e : entities) {
                result.concepts.push_back(std::move(e));
            }
        }

        result.usedGliner = true;
        const auto endTime = std::chrono::steady_clock::now();
        result.extractionTimeMs =
            std::chrono::duration<double, std::milli>(endTime - startTime).count();

        spdlog::debug("Simeon title/entity extraction: {} concepts in {:.2f}ms",
                      result.concepts.size(), result.extractionTimeMs);
        return result;
    };
}

search::EntityExtractionFunc
createEntityExtractionFunc(std::vector<std::shared_ptr<AbiEntityExtractorAdapter>> extractors) {
    if (auto gliner = createGlinerExtractionFunc(std::move(extractors))) {
        return gliner;
    }
    return createSimeonExtractionFunc();
}

} // namespace yams::daemon
