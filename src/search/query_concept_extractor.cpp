#include <yams/search/query_concept_extractor.h>

#include <spdlog/spdlog.h>

namespace yams::search {

QueryConceptExtractor::QueryConceptExtractor(EntityExtractionFunc extractFunc)
    : extractFunc_(std::move(extractFunc)) {
    if (extractFunc_) {
        spdlog::info("QueryConceptExtractor: entity extraction backend available");
    }
}

QueryConceptExtractor::QueryConceptExtractor() : extractFunc_(nullptr) {
    spdlog::debug("QueryConceptExtractor: no extraction backend, concepts unavailable");
}

QueryConceptExtractor::~QueryConceptExtractor() = default;

bool QueryConceptExtractor::isAvailable() const {
    return extractFunc_ != nullptr;
}

Result<QueryConceptResult> QueryConceptExtractor::extractConcepts(const std::string& query) const {
    return extractConcepts(query, {});
}

Result<QueryConceptResult>
QueryConceptExtractor::extractConcepts(const std::string& query,
                                       const std::vector<std::string>& types) const {
    if (query.empty()) {
        return QueryConceptResult{};
    }

    if (!extractFunc_) {
        // No backend available - return empty result
        return QueryConceptResult{};
    }

    return extractFunc_(query, types);
}

} // namespace yams::search
