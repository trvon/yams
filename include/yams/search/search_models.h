#pragma once

#include <yams/search/search_engine_config.h>
#include <yams/search/search_results.h>

#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::search {

using yams::metadata::SearchResult;

struct ComponentResult {
    std::string documentHash;
    std::string filePath;
    float score = 0.0f;
    enum class Source {
        Text,
        GraphText,
        PathTree,
        KnowledgeGraph,
        Vector,
        GraphVector,
        EntityVector,
        Tag,
        Metadata,
        Symbol,
        Unknown
    } source = Source::Unknown;
    size_t rank = 0;
    std::optional<std::string> snippet;
    std::map<std::string, std::string> debugInfo;
};

inline constexpr const char* componentSourceToString(ComponentResult::Source source) noexcept {
    switch (source) {
        case ComponentResult::Source::Text:
            return "text";
        case ComponentResult::Source::GraphText:
            return "graph_text";
        case ComponentResult::Source::PathTree:
            return "path_tree";
        case ComponentResult::Source::KnowledgeGraph:
            return "kg";
        case ComponentResult::Source::Vector:
            return "vector";
        case ComponentResult::Source::GraphVector:
            return "graph_vector";
        case ComponentResult::Source::EntityVector:
            return "entity_vector";
        case ComponentResult::Source::Tag:
            return "tag";
        case ComponentResult::Source::Metadata:
            return "metadata";
        case ComponentResult::Source::Symbol:
            return "symbol";
        case ComponentResult::Source::Unknown:
            return "unknown";
    }
    return "unknown";
}

inline float componentSourceWeight(const SearchEngineConfig& config,
                                   ComponentResult::Source source) noexcept {
    switch (source) {
        case ComponentResult::Source::Text:
            return config.textWeight;
        case ComponentResult::Source::GraphText:
            return config.graphTextWeight;
        case ComponentResult::Source::PathTree:
            return config.pathTreeWeight;
        case ComponentResult::Source::KnowledgeGraph:
            return config.kgWeight;
        case ComponentResult::Source::Vector:
            return config.vectorWeight;
        case ComponentResult::Source::GraphVector:
            return config.graphVectorWeight;
        case ComponentResult::Source::EntityVector:
            return config.entityVectorWeight;
        case ComponentResult::Source::Tag:
            return config.tagWeight;
        case ComponentResult::Source::Metadata:
            return config.metadataWeight;
        case ComponentResult::Source::Symbol:
        case ComponentResult::Source::Unknown:
            return 0.0f;
    }
    return 0.0f;
}

inline constexpr bool isVectorComponent(ComponentResult::Source source) noexcept {
    return source == ComponentResult::Source::Vector ||
           source == ComponentResult::Source::GraphVector ||
           source == ComponentResult::Source::EntityVector;
}

inline constexpr bool isTextAnchoringComponent(ComponentResult::Source source) noexcept {
    return source == ComponentResult::Source::Text ||
           source == ComponentResult::Source::GraphText ||
           source == ComponentResult::Source::PathTree ||
           source == ComponentResult::Source::KnowledgeGraph ||
           source == ComponentResult::Source::Tag || source == ComponentResult::Source::Metadata ||
           source == ComponentResult::Source::Symbol;
}

inline double componentSourceScoreInResult(const SearchResult& r,
                                           ComponentResult::Source source) noexcept {
    switch (source) {
        case ComponentResult::Source::Vector:
        case ComponentResult::Source::EntityVector:
            return r.vectorScore.value_or(0.0);
        case ComponentResult::Source::GraphVector:
            return r.graphVectorScore.value_or(0.0);
        case ComponentResult::Source::Text:
            return r.keywordScore.value_or(0.0);
        case ComponentResult::Source::GraphText:
            return r.graphTextScore.value_or(0.0);
        case ComponentResult::Source::KnowledgeGraph:
            return r.kgScore.value_or(0.0);
        case ComponentResult::Source::PathTree:
            return r.pathScore.value_or(0.0);
        case ComponentResult::Source::Tag:
        case ComponentResult::Source::Metadata:
            return r.tagScore.value_or(0.0);
        case ComponentResult::Source::Symbol:
            return r.symbolScore.value_or(0.0);
        case ComponentResult::Source::Unknown:
            return 0.0;
    }
    return 0.0;
}

inline std::string documentIdFromComponent(const ComponentResult& comp) noexcept {
    if (!comp.filePath.empty()) {
        return comp.filePath;
    }
    if (!comp.documentHash.empty()) {
        return comp.documentHash;
    }
    return {};
}

struct SearchResponse {
    std::vector<SearchResult> results;
    std::vector<SearchFacet> facets;
    std::vector<std::string> timedOutComponents;
    std::vector<std::string> failedComponents;
    std::vector<std::string> contributingComponents;
    std::vector<std::string> skippedComponents;
    std::map<std::string, int64_t> componentTimingMicros;
    std::unordered_map<std::string, std::string> debugStats;
    int64_t executionTimeMs = 0;
    bool isDegraded = false;
    bool usedEarlyTermination = false;

    [[nodiscard]] bool hasResults() const { return !results.empty(); }
    [[nodiscard]] bool isComplete() const {
        return timedOutComponents.empty() && failedComponents.empty();
    }
};

namespace detail {

template <typename T> std::string makeFusionDedupKey(const T& item, bool enablePathDedup) {
    const auto& filePath = [&]() -> const std::string& {
        if constexpr (requires { item.filePath; }) {
            return item.filePath;
        } else {
            return item.document.filePath;
        }
    }();
    const auto& hash = [&]() -> const std::string& {
        if constexpr (requires { item.documentHash; }) {
            return item.documentHash;
        } else {
            return item.document.sha256Hash;
        }
    }();
    if (enablePathDedup && !filePath.empty()) {
        return "path:" + filePath;
    }
    if (!hash.empty()) {
        return "hash:" + hash;
    }
    if (!filePath.empty()) {
        return "path:" + filePath;
    }
    return "unknown:";
}

} // namespace detail

} // namespace yams::search
