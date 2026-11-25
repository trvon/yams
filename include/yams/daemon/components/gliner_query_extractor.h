#pragma once

#include <memory>
#include <vector>
#include <yams/search/query_concept_extractor.h>

namespace yams::daemon {

class AbiEntityExtractorAdapter;

search::EntityExtractionFunc
createGlinerExtractionFunc(std::vector<std::shared_ptr<AbiEntityExtractorAdapter>> extractors);

/// Training-free, deterministic title & entity extractor backed by Simeon TextRank
/// (structural heading detection + TextRank sentence ranking + technical/phrase entity extraction).
search::EntityExtractionFunc createSimeonExtractionFunc();

/// Returns the GLiNER plugin extractor when available, falling back to Simeon's built-in
/// training-free title & entity extractor.
search::EntityExtractionFunc
createEntityExtractionFunc(std::vector<std::shared_ptr<AbiEntityExtractorAdapter>> extractors);

} // namespace yams::daemon
