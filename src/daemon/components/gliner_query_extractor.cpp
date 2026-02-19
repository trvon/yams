#include <yams/daemon/components/gliner_query_extractor.h>

#include <array>
#include <chrono>
#include <cctype>
#include <spdlog/spdlog.h>
#include <yams/daemon/resource/abi_entity_extractor_adapter.h>
#include <yams/plugins/entity_extractor_v2.h>

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

namespace yams::daemon {

namespace {
// Default entity types for query concept extraction
constexpr std::array<const char*, 8> kDefaultEntityTypes = {
    "technology", "concept", "organization", "person",
    "location",   "product", "language",     "framework"};

// Minimum confidence threshold for including concepts
constexpr float kMinConfidence = 0.4f;
constexpr std::size_t kMaxEntityTextLen = 160;

std::string toLowerCopy(std::string_view input) {
    std::string out;
    out.reserve(input.size());
    for (unsigned char c : input) {
        out.push_back(static_cast<char>(std::tolower(c)));
    }
    return out;
}

std::string trimAndCollapse(std::string_view input) {
    std::string out;
    out.reserve(input.size());

    bool inWs = false;
    std::size_t start = 0;
    while (start < input.size() && std::isspace(static_cast<unsigned char>(input[start]))) {
        ++start;
    }
    std::size_t end = input.size();
    while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1]))) {
        --end;
    }

    for (std::size_t i = start; i < end; ++i) {
        unsigned char c = static_cast<unsigned char>(input[i]);
        if (std::isspace(c)) {
            if (!inWs) {
                out.push_back(' ');
                inWs = true;
            }
        } else {
            out.push_back(static_cast<char>(c));
            inWs = false;
        }
    }

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
    AbiEntityExtractorAdapter* textExtractor = nullptr;
    for (const auto& ext : extractors) {
        if (ext && ext->supportsContentType("text/plain")) {
            textExtractor = ext.get();
            break;
        }
    }

    if (!textExtractor) {
        spdlog::warn("createGlinerExtractionFunc: No GLiNER extractor found");
        return nullptr;
    }

    spdlog::info("createGlinerExtractionFunc: GLiNER entity extractor available");

    // Capture extractors by value to extend lifetime
    return [extractors = std::move(extractors), textExtractor](
               const std::string& content,
               const std::vector<std::string>& entityTypes) -> Result<search::QueryConceptResult> {
        search::QueryConceptResult result;
        result.usedGliner = false;

        if (content.empty()) {
            return result;
        }

        auto startTime = std::chrono::steady_clock::now();

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

        // Free the extraction result
        textExtractor->freeResult(extractionResult);

        auto endTime = std::chrono::steady_clock::now();
        result.extractionTimeMs =
            std::chrono::duration<double, std::milli>(endTime - startTime).count();

        spdlog::debug("GLiNER query extraction: {} concepts in {:.2f}ms", result.concepts.size(),
                      result.extractionTimeMs);

        return result;
    };
}

} // namespace yams::daemon
