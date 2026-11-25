#include <catch2/catch_test_macros.hpp>

#include <yams/search/query_router.h>

#include <cstddef>
#include <string_view>

namespace yams::search {

TEST_CASE("QueryRouter: prose sentences classify as prose intent", "[search][router][intent]") {
    QueryRouter router;

    const auto match =
        router.classifyIntent("Can statin treatment reduce inflammatory disease risk");

    CHECK(match.label == QueryIntent::Prose);
    CHECK(match.reason == "token_count>=3");
}

TEST_CASE("QueryRouter: camelCase identifiers classify as code intent",
          "[search][router][intent]") {
    QueryRouter router;

    const auto match = router.classifyIntent("resolveGraphCandidateNodeId");

    CHECK(match.label == QueryIntent::Code);
    CHECK(match.reason == "camel_case");
}

TEST_CASE("QueryRouter: code intent routes to literal retrieval mode",
          "[search][router][retrieval]") {
    QueryRouter router;

    const auto match =
        router.classifyRetrievalMode("resolveGraphCandidateNodeId", QueryIntent::Code);

    CHECK(match.label == QueryRetrievalMode::Literal);
    CHECK(match.reason == "code_intent");
}

TEST_CASE("QueryRouter: prose intent routes to semantic retrieval mode",
          "[search][router][retrieval]") {
    QueryRouter router;

    const auto match = router.classifyRetrievalMode(
        "Can statin treatment reduce inflammatory disease risk", QueryIntent::Prose);

    CHECK(match.label == QueryRetrievalMode::Semantic);
}

TEST_CASE("QueryRouter: quoted queries route to literal retrieval mode",
          "[search][router][retrieval]") {
    QueryRouter router;

    const auto decision = router.route("\"exact phrase\"");

    CHECK(decision.retrievalMode.label == QueryRetrievalMode::Literal);
    CHECK(decision.retrievalMode.reason == "quoted_literal");
}

TEST_CASE("QueryRouter: scientific community overrides prose corpora only when needed",
          "[search][router][community]") {
    QueryRouter router;
    QueryRouteContext context;

    auto match =
        router.classifyCommunity("clinical trial treatment disease evidence",
                                 CommunityRouteFamily::Context{QueryIntent::Prose, context});
    REQUIRE(match.has_value());
    CHECK(match->label == QueryCommunity::Scientific);
    CHECK(match->reason == "scientific_terms=5");

    context.corpusUsesScientificProfile = true;
    match = router.classifyCommunity("clinical trial treatment disease evidence",
                                     CommunityRouteFamily::Context{QueryIntent::Prose, context});
    CHECK_FALSE(match.has_value());
}

TEST_CASE("QueryRouter: code queries request code community in prose corpora",
          "[search][router][community]") {
    QueryRouter router;
    QueryRouteContext context;

    auto match = router.classifyCommunity(
        "src/search/query_router.cpp", CommunityRouteFamily::Context{QueryIntent::Path, context});
    REQUIRE(match.has_value());
    CHECK(match->label == QueryCommunity::Code);
    CHECK(match->reason == "code_or_path_intent");
}

TEST_CASE("QueryRouter: neutral prose queries do not force a community override",
          "[search][router][community]") {
    QueryRouter router;

    const auto match = router.classifyCommunity(
        "documents and files", CommunityRouteFamily::Context{QueryIntent::Prose, {}});

    CHECK_FALSE(match.has_value());
}

TEST_CASE("QueryRouter: route returns both intent and community labels", "[search][router]") {
    QueryRouter router;

    const auto decision =
        router.route("clinical trial treatment disease evidence", QueryRouteContext{});

    CHECK(decision.intent.label == QueryIntent::Prose);
    CHECK(decision.retrievalMode.label == QueryRetrievalMode::Semantic);
    REQUIRE(decision.community.has_value());
    CHECK(decision.community->label == QueryCommunity::Scientific);
}

TEST_CASE("QueryRouter: whitespace terms keep identifiers and paths whole",
          "[search][router][budget]") {
    CHECK(countWhitespaceTerms("") == std::size_t{0});
    CHECK(countWhitespaceTerms("   ") == std::size_t{0});
    CHECK(countWhitespaceTerms("src/search/search_engine.cpp") == std::size_t{1});
    CHECK(countWhitespaceTerms("  SearchEngine::search \t fusion\n") == std::size_t{2});
}

TEST_CASE("QueryRouter: short-query budget applies to short code and path lookups",
          "[search][router][budget]") {
    QueryRouter router;
    const auto budgeted = [&](std::string_view query) {
        return isShortQueryBudgeted(router.classifyIntent(query).label, query);
    };

    // Genuine lookups: one or two terms, whatever the intent.
    CHECK(budgeted("resolveGraphCandidateNodeId"));
    CHECK(budgeted("src/app/services/search_service.cpp"));
    CHECK(budgeted("SearchEngine::search fusion"));
    CHECK(budgeted("search_engine.cpp tuner"));
    CHECK(budgeted("statin"));
    CHECK(budgeted("statin risk"));

    // Boundary: three terms is prose length, as in the router's own prose rule.
    CHECK(router.classifyIntent("parse SearchEngine config").label == QueryIntent::Code);
    CHECK_FALSE(budgeted("parse SearchEngine config"));
    CHECK(router.classifyIntent("src/search two terms").label == QueryIntent::Path);
    CHECK_FALSE(budgeted("src/search two terms"));
}

TEST_CASE("QueryRouter: prose with one code-like token keeps the full semantic budget",
          "[search][router][budget]") {
    QueryRouter router;
    const auto check = [&](std::string_view query, QueryIntent expectedIntent) {
        const auto intent = router.classifyIntent(query).label;
        CHECK(intent == expectedIntent);
        CHECK_FALSE(isShortQueryBudgeted(intent, query));
    };

    // One CamelCase token labels the sentence Code; one slash labels it Path.
    check("The DdrB protein from Deinococcus radiodurans is an alternative SSB.",
          QueryIntent::Code);
    check("share one init-waiter body between runLoop and test helper", QueryIntent::Code);
    check("what should I pack before the 7/22 trip?", QueryIntent::Path);
    check("Can statin treatment reduce inflammatory disease risk", QueryIntent::Prose);
}

} // namespace yams::search
