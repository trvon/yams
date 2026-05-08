#pragma once

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>
#include <yams/search/query_concept_extractor.h>

namespace yams::search {

void enrichWithFallbackConcepts(const std::string& query, std::vector<QueryConcept>& concepts,
                                size_t maxConcepts,
                                const std::unordered_map<std::string, float>* idfMap = nullptr);

} // namespace yams::search
