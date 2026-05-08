#include <yams/search/concept_resolver.h>

#include <spdlog/spdlog.h>
#include <unordered_map>
#include <unordered_set>
#include <yams/search/query_expansion.h>
#include <yams/search/query_text_utils.h>

namespace yams::search {

void enrichWithFallbackConcepts(const std::string& query, std::vector<QueryConcept>& concepts,
                                size_t maxConcepts,
                                const std::unordered_map<std::string, float>* idfMap) {
    if (maxConcepts == 0 || concepts.size() >= maxConcepts) {
        return;
    }

    // Use provided IDF map when available; fall back to empty (token salience).
    std::unordered_map<std::string, float> emptyIdf;
    const auto& idf = (idfMap && !idfMap->empty()) ? *idfMap : emptyIdf;
    auto fallbackConcepts = generateFallbackQueryConcepts(query, idf, maxConcepts);
    if (fallbackConcepts.empty()) {
        return;
    }

    std::unordered_set<std::string> seenConceptKeys;
    seenConceptKeys.reserve(concepts.size() + fallbackConcepts.size());
    for (const auto& existingConcept : concepts) {
        std::string key = normalizeEntityTextForKey(existingConcept.text);
        key.push_back('|');
        key.append(existingConcept.type);
        seenConceptKeys.insert(std::move(key));
    }

    const size_t beforeFallback = concepts.size();
    for (auto& fallbackConcept : fallbackConcepts) {
        if (concepts.size() >= maxConcepts) {
            break;
        }
        std::string key = normalizeEntityTextForKey(fallbackConcept.text);
        key.push_back('|');
        key.append(fallbackConcept.type);
        if (!seenConceptKeys.insert(std::move(key)).second) {
            continue;
        }
        concepts.push_back(std::move(fallbackConcept));
    }

    if (concepts.size() > beforeFallback) {
        spdlog::debug("concepts: added {} fallback query concepts for '{}'",
                      concepts.size() - beforeFallback, query.substr(0, 60));
    }
}

} // namespace yams::search
