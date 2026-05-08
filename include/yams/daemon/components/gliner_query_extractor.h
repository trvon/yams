#pragma once

#include <memory>
#include <vector>
#include <yams/search/query_concept_extractor.h>

namespace yams::daemon {

class AbiEntityExtractorAdapter;
class ExternalEntityProviderAdapter;

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

/**
 * @brief Create an EntityExtractionFunc from an ExternalEntityProvider
 *
 * Fallback factory used when no ABI entity extractors are registered (e.g.
 * Glint/GLiNER is only available via the ExternalEntityProvider/ONNX path).
 * Converts query text → bytes → EntityResult → QueryConceptResult.
 *
 * @param providers External entity providers from PluginManager
 * @return Function that performs entity extraction via the first available
 *         provider, or nullptr if none support text extraction.
 */
search::EntityExtractionFunc createGlinerExtractionFuncFromProvider(
    const std::vector<std::shared_ptr<ExternalEntityProviderAdapter>>& providers);

} // namespace yams::daemon
