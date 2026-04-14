#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace yams::search {

enum class QueryIntent { Code, Path, Prose, Mixed };

[[nodiscard]] constexpr const char* queryIntentToString(QueryIntent intent) noexcept {
    switch (intent) {
        case QueryIntent::Code:
            return "code";
        case QueryIntent::Path:
            return "path";
        case QueryIntent::Prose:
            return "prose";
        case QueryIntent::Mixed:
            return "mixed";
    }
    return "mixed";
}

enum class QueryRetrievalMode { Literal, Semantic, Hybrid, Path };

[[nodiscard]] constexpr const char* queryRetrievalModeToString(QueryRetrievalMode mode) noexcept {
    switch (mode) {
        case QueryRetrievalMode::Literal:
            return "literal";
        case QueryRetrievalMode::Semantic:
            return "semantic";
        case QueryRetrievalMode::Hybrid:
            return "hybrid";
        case QueryRetrievalMode::Path:
            return "path";
    }
    return "hybrid";
}

enum class QueryCommunity { Code, Scientific, Media };

[[nodiscard]] constexpr const char* queryCommunityToString(QueryCommunity community) noexcept {
    switch (community) {
        case QueryCommunity::Code:
            return "code";
        case QueryCommunity::Scientific:
            return "scientific";
        case QueryCommunity::Media:
            return "media";
    }
    return "unknown";
}

template <typename Label> struct RouteMatch {
    Label label;
    float confidence = 0.0f;
    std::string reason;
};

template <typename FamilyTraits> class RouteFamilyRouter {
public:
    using Label = typename FamilyTraits::Label;
    using Context = typename FamilyTraits::Context;
    using Match = RouteMatch<Label>;

    [[nodiscard]] std::optional<Match> classify(std::string_view query,
                                                const Context& context) const {
        return FamilyTraits::classify(query, context);
    }
};

struct IntentRouteFamily {
    using Label = QueryIntent;
    struct Context {};

    [[nodiscard]] static std::optional<RouteMatch<Label>> classify(std::string_view query,
                                                                   const Context& context);
};

struct QueryRouteContext {
    bool corpusUsesCodeProfile = false;
    bool corpusUsesScientificProfile = false;
    bool corpusUsesMediaProfile = false;
};

struct CommunityRouteFamily {
    using Label = QueryCommunity;

    struct Context {
        QueryIntent intent = QueryIntent::Mixed;
        QueryRouteContext routeContext{};
    };

    [[nodiscard]] static std::optional<RouteMatch<Label>> classify(std::string_view query,
                                                                   const Context& context);
};

struct QueryRouteDecision {
    RouteMatch<QueryIntent> intent{QueryIntent::Mixed, 0.0f, {}};
    RouteMatch<QueryRetrievalMode> retrievalMode{QueryRetrievalMode::Hybrid, 0.0f, {}};
    std::optional<RouteMatch<QueryCommunity>> community;
};

class QueryRouter {
public:
    [[nodiscard]] RouteMatch<QueryIntent> classifyIntent(std::string_view query) const;

    [[nodiscard]] RouteMatch<QueryRetrievalMode> classifyRetrievalMode(std::string_view query,
                                                                       QueryIntent intent) const;

    [[nodiscard]] std::optional<RouteMatch<QueryCommunity>>
    classifyCommunity(std::string_view query, const CommunityRouteFamily::Context& context) const;

    [[nodiscard]] QueryRouteDecision route(std::string_view query,
                                           const QueryRouteContext& context = {}) const;
};

} // namespace yams::search
