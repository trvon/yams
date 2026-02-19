#include <yams/daemon/components/gliner_query_extractor.h>

#include <array>
#include <chrono>
#include <spdlog/spdlog.h>
#include <yams/daemon/resource/abi_entity_extractor_adapter.h>
#include <yams/plugins/entity_extractor_v2.h>

namespace yams::daemon {

namespace {
// Default entity types for query concept extraction
constexpr std::array<const char*, 8> kDefaultEntityTypes = {
    "technology", "concept", "organization", "person",
    "location",   "product", "language",     "framework"};

// Minimum confidence threshold for including concepts
constexpr float kMinConfidence = 0.4f;

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
        if (entityTypes.empty()) {
            for (const auto* t : kDefaultEntityTypes) {
                types.push_back(t);
            }
        } else {
            for (const auto& t : entityTypes) {
                types.push_back(t.c_str());
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

        // Convert extraction result to QueryConcepts
        for (size_t i = 0; i < extractionResult->entity_count; ++i) {
            const auto& entity = extractionResult->entities[i];

            search::QueryConcept qc;
            qc.text = entity.text ? entity.text : "";
            qc.type = entity.type ? entity.type : "";
            qc.confidence = entity.confidence;
            qc.startOffset = entity.start_offset;
            qc.endOffset = entity.end_offset;

            // Only include concepts with reasonable confidence
            if (qc.confidence >= kMinConfidence && !qc.text.empty()) {
                result.concepts.push_back(std::move(qc));
            }
        }

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
