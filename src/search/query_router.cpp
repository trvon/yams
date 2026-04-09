#include <yams/search/query_router.h>

#include <yams/search/query_text_utils.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <sstream>

namespace yams::search {
namespace {

bool hasCamelCase(std::string_view input) {
    bool tokenHasAlpha = false;
    bool tokenHasLower = false;
    bool tokenHasUpper = false;
    bool tokenHasInteriorUpper = false;

    const auto flushToken = [&]() {
        const bool camelToken =
            tokenHasAlpha && tokenHasLower && tokenHasUpper && tokenHasInteriorUpper;
        tokenHasAlpha = false;
        tokenHasLower = false;
        tokenHasUpper = false;
        tokenHasInteriorUpper = false;
        return camelToken;
    };

    for (unsigned char c : input) {
        if (std::isalnum(c)) {
            if (std::isalpha(c)) {
                if (std::isupper(c) && tokenHasAlpha) {
                    tokenHasInteriorUpper = true;
                }
                tokenHasAlpha = true;
                tokenHasLower = tokenHasLower || std::islower(c);
                tokenHasUpper = tokenHasUpper || std::isupper(c);
            }
            continue;
        }

        if (flushToken()) {
            return true;
        }
    }

    return flushToken();
}

bool hasFileExtension(std::string_view input) {
    const auto dot = input.rfind('.');
    if (dot == std::string_view::npos || dot == 0 || dot + 1 >= input.size()) {
        return false;
    }

    const auto ext = input.substr(dot + 1);
    if (ext.size() > 5) {
        return false;
    }

    for (unsigned char c : ext) {
        if (!std::isalnum(c)) {
            return false;
        }
    }
    return true;
}

std::string scientificReason(int hitCount) {
    std::ostringstream oss;
    oss << "scientific_terms=" << hitCount;
    return oss.str();
}

} // namespace

std::optional<RouteMatch<QueryIntent>> IntentRouteFamily::classify(std::string_view query,
                                                                   const Context&) {
    if (query.empty()) {
        return RouteMatch<QueryIntent>{QueryIntent::Mixed, 0.25f, "empty_query"};
    }

    const bool hasPathSeparator =
        query.find('/') != std::string_view::npos || query.find('\\') != std::string_view::npos;
    const bool hasPathPrefix = query.rfind("./", 0) == 0 || query.rfind("../", 0) == 0;
    const bool hasCodeSig =
        query.find("::") != std::string_view::npos || query.find("->") != std::string_view::npos ||
        query.find('#') != std::string_view::npos || query.find('_') != std::string_view::npos;
    const bool hasExt = hasFileExtension(query);
    const bool hasCamel = hasCamelCase(query);

    if (hasPathSeparator || hasPathPrefix) {
        return RouteMatch<QueryIntent>{QueryIntent::Path, 0.98f, "path_separator"};
    }

    if (hasCodeSig || hasCamel || hasExt) {
        const char* reason = hasCodeSig ? "code_signature"
                             : hasCamel ? "camel_case"
                                        : "file_extension";
        return RouteMatch<QueryIntent>{QueryIntent::Code, 0.95f, reason};
    }

    const auto tokens = tokenizeLower(std::string(query));
    if (tokens.size() >= 3) {
        return RouteMatch<QueryIntent>{QueryIntent::Prose, 0.80f, "token_count>=3"};
    }

    return RouteMatch<QueryIntent>{QueryIntent::Mixed, 0.40f, "short_query_fallback"};
}

std::optional<RouteMatch<QueryCommunity>> CommunityRouteFamily::classify(std::string_view query,
                                                                         const Context& context) {
    if (context.intent == QueryIntent::Code || context.intent == QueryIntent::Path) {
        if (!context.routeContext.corpusUsesCodeProfile) {
            return RouteMatch<QueryCommunity>{QueryCommunity::Code, 0.98f, "code_or_path_intent"};
        }
        return std::nullopt;
    }

    if (context.intent != QueryIntent::Prose && context.intent != QueryIntent::Mixed) {
        return std::nullopt;
    }

    const auto tokens = tokenizeLower(std::string(query));

    static constexpr std::array<std::string_view, 18> kScientificTerms = {
        "study",      "analysis", "trial",    "effect",   "association", "mechanism",
        "inhibit",    "protein",  "gene",     "disease",  "treatment",   "cohort",
        "hypothesis", "evidence", "receptor", "exposure", "mutation",    "clinical",
    };

    int scientificHits = 0;
    for (const auto& token : tokens) {
        for (const auto& scientific : kScientificTerms) {
            if (token == scientific) {
                ++scientificHits;
                break;
            }
        }
    }

    if (scientificHits >= 2 && !context.routeContext.corpusUsesScientificProfile) {
        const float confidence = std::min(1.0f, 0.55f + static_cast<float>(scientificHits) * 0.10f);
        return RouteMatch<QueryCommunity>{QueryCommunity::Scientific, confidence,
                                          scientificReason(scientificHits)};
    }

    static constexpr std::array<std::string_view, 12> kMediaTerms = {
        "photo",  "video", "image", "audio",     "screenshot", "recording",
        "camera", "album", "clip",  "thumbnail", "podcast",    "playlist",
    };

    for (const auto& token : tokens) {
        for (const auto& media : kMediaTerms) {
            if (token == media) {
                if (!context.routeContext.corpusUsesMediaProfile) {
                    return RouteMatch<QueryCommunity>{QueryCommunity::Media, 0.92f,
                                                      std::string("media_term=") +
                                                          std::string(media)};
                }
                return std::nullopt;
            }
        }
    }

    return std::nullopt;
}

RouteMatch<QueryIntent> QueryRouter::classifyIntent(std::string_view query) const {
    RouteFamilyRouter<IntentRouteFamily> router;
    return router.classify(query, IntentRouteFamily::Context{})
        .value_or(RouteMatch<QueryIntent>{QueryIntent::Mixed, 0.25f, "fallback"});
}

std::optional<RouteMatch<QueryCommunity>>
QueryRouter::classifyCommunity(std::string_view query,
                               const CommunityRouteFamily::Context& context) const {
    RouteFamilyRouter<CommunityRouteFamily> router;
    return router.classify(query, context);
}

QueryRouteDecision QueryRouter::route(std::string_view query,
                                      const QueryRouteContext& context) const {
    QueryRouteDecision decision;
    decision.intent = classifyIntent(query);
    decision.community =
        classifyCommunity(query, CommunityRouteFamily::Context{decision.intent.label, context});
    return decision;
}

} // namespace yams::search
