#pragma once

#include <memory>
#include <vector>
#include <yams/search/query_concept_extractor.h>

namespace yams::daemon {

class AbiEntityExtractorAdapter;

/**
 * @brief Create an EntityExtractionFunc that uses GLiNER entity extractors
 *
 * This factory creates a function suitable for QueryConceptExtractor that
 * wraps the AbiEntityExtractorAdapter(s) from the PluginManager.
 *
 * @param extractors GLiNER entity extractor adapters from PluginManager
 * @return Function that performs entity extraction via GLiNER
 */
search::EntityExtractionFunc
createGlinerExtractionFunc(std::vector<std::shared_ptr<AbiEntityExtractorAdapter>> extractors);

} // namespace yams::daemon
