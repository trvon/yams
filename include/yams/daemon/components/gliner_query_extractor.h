#pragma once

#include <memory>
#include <vector>
#include <yams/search/query_concept_extractor.h>

namespace yams::daemon {

class AbiEntityExtractorAdapter;

search::EntityExtractionFunc
createGlinerExtractionFunc(std::vector<std::shared_ptr<AbiEntityExtractorAdapter>> extractors);

} // namespace yams::daemon
