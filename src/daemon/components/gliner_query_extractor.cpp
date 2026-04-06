#include <yams/daemon/components/gliner_query_extractor.h>

#include <spdlog/spdlog.h>
#include <array>
#include <cctype>
#include <chrono>
#include <yams/daemon/resource/abi_entity_extractor_adapter.h>
#include <yams/plugins/entity_extractor_v2.h>
#include <yams/search/query_text_utils.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace yams::daemon {

namespace {
// Default entity types for query concept extraction
constexpr std::array<const char*, 19> kDefaultEntityTypes = {
    "technology", "concept", "organization", "person",
    "location",   "product", "language",     "framework",
    "protein",    "gene",    "cell",         "disease",
    "chemical",   "drug",    "pathway",      "biological_process",
    "biomarker",  "anatomy", "organism"};

// Minimum confidence threshold for including concepts
constexpr float kMinConfidence = 0.4f;
constexpr std::size_t kMaxEntityTextLen = 160;

std::string toLowerCopy(std::string_view input) {
    return yams::search::toLowerCopy(input);
}

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

std::string normalizeType(std::string_view type) {
    static const std::unordered_map<std::string, std::string> kAliases = {
        {"org", "organization"},  {"company", "organization"}, {"institution", "organization"},
        {"loc", "location"},      {"place", "location"},       {"tool", "technology"},
        {"library", "framework"},
    };

    std::string lowered = toLowerCopy(trimAndCollapse(type));
    if (lowered.empty()) {
        return "concept";
    }
    auto it = kAliases.find(lowered);
    if (it != kAliases.end()) {
        return it->second;
    }
    return lowered;
}

bool isLikelyNoiseEntity(std::string_view text) {
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

    static const std::unordered_set<std::string> kStopwords = {
        "a",  "an", "and", "are", "as", "at",  "by",   "for",  "from", "in",   "is",    "it",
        "of", "on", "or",  "the", "to", "was", "were", "with", "this", "that", "these", "those"};

    std::string lowered = toLowerCopy(text);
    if (kStopwords.find(lowered) != kStopwords.end()) {
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
            for (const auto* t : kDefaultEntityTypes) {
                types.push_back(t);
                requestedTypes.insert(normalizeType(t));
            }
        } else {
            for (const auto& t : entityTypes) {
                types.push_back(t.c_str());
                requestedTypes.insert(normalizeType(t));
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
            qc.type = normalizeType(entity.type ? entity.type : "");
            qc.confidence = entity.confidence;
            qc.startOffset = entity.start_offset;
            qc.endOffset = entity.end_offset;

            // Only include concepts with reasonable confidence
            if (qc.confidence < kMinConfidence || qc.text.empty() || isLikelyNoiseEntity(qc.text)) {
                continue;
            }

            if (!requestedTypes.empty() && requestedTypes.find(qc.type) == requestedTypes.end()) {
                continue;
            }

            std::string dedupeKey = qc.type;
            dedupeKey.push_back('|');
            dedupeKey.append(toLowerCopy(qc.text));

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

} // namespace yams::daemon
