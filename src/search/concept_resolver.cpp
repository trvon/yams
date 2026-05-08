#include <yams/search/concept_resolver.h>

#include <spdlog/spdlog.h>
#include <unordered_map>
#include <unordered_set>
#include <yams/search/query_expansion.h>
#include <yams/search/query_text_utils.h>

namespace yams::search {

void enrichWithFallbackConcepts(const std::string& query, std::vector<QueryConcept>& concepts,
                                size_t maxConcepts) {
    if (maxConcepts == 0 || concepts.size() >= maxConcepts) {
        return;
    }

    // IDF from metadata is optional; generateFallbackQueryConcepts handles
    // an empty map gracefully (falls back to token salience scoring).
    std::unordered_map<std::string, float> emptyIdf;
    auto fallbackConcepts = generateFallbackQueryConcepts(query, emptyIdf, maxConcepts);
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
