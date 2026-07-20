#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>

#include "../../common/test_helpers_catch2.h"
#include "src/metadata/repository/search_query_helpers.hpp"

namespace yams::metadata {
#ifdef YAMS_TESTING
void testingResetSearchQueryHelperCaches();
#endif
} // namespace yams::metadata

namespace {
void resetSearchQueryHelperCaches() {
#ifdef YAMS_TESTING
    yams::metadata::testingResetSearchQueryHelperCaches();
#endif
}
} // namespace

TEST_CASE("Search query helpers: parseFts5ModeEnv honors supported values",
          "[unit][metadata][fts5][search-query-helpers]") {
    using yams::metadata::Fts5QueryMode;
    using yams::metadata::parseFts5ModeEnv;

    yams::test::ScopedEnvVar env("YAMS_FTS_MODE");

    SECTION("unset defaults to smart") {
        env.unset();
        CHECK((parseFts5ModeEnv() == Fts5QueryMode::Smart));
    }

    SECTION("simple is case-insensitive") {
        env.set("SIMPLE");
        CHECK((parseFts5ModeEnv() == Fts5QueryMode::Simple));
    }

    SECTION("nl shorthand selects natural mode") {
        env.set("nl");
        CHECK((parseFts5ModeEnv() == Fts5QueryMode::Natural));
    }

    SECTION("natural selects natural mode") {
        env.set("natural");
        CHECK((parseFts5ModeEnv() == Fts5QueryMode::Natural));
    }

    SECTION("unknown values fall back to smart") {
        env.set("surprising-value");
        CHECK((parseFts5ModeEnv() == Fts5QueryMode::Smart));
    }
}

TEST_CASE("Search query helpers: includeSearchSnippets caches env-derived behavior",
          "[unit][metadata][fts5][search-query-helpers]") {
    using yams::metadata::includeSearchSnippets;

    yams::test::ScopedEnvVar env("YAMS_SEARCH_INCLUDE_SNIPPET");

    SECTION("unset defaults to disabled") {
        env.unset();
        resetSearchQueryHelperCaches();
        CHECK_FALSE((includeSearchSnippets()));
    }

    SECTION("recognized falsey values disable snippets") {
        for (const char* value : {"0", "false", "off", "no"}) {
            env.set(value);
            resetSearchQueryHelperCaches();
            INFO(value);
            CHECK_FALSE((includeSearchSnippets()));
        }
    }

    SECTION("non-empty truthy values enable snippets") {
        for (const char* value : {"1", "true", "yes", "debug"}) {
            env.set(value);
            resetSearchQueryHelperCaches();
            INFO(value);
            CHECK((includeSearchSnippets()));
        }
    }
}

TEST_CASE("Search query helpers: buildDiagnosticAltOrQuery strips punctuation and limits terms",
          "[unit][metadata][fts5][search-query-helpers]") {
    using yams::metadata::buildDiagnosticAltOrQuery;

    SECTION("empty input yields empty query") {
        CHECK((buildDiagnosticAltOrQuery({}).empty()));
    }

    SECTION("filters short terms and quotes normalized tokens") {
        const std::vector<std::string> tokens = {"a",     "the",     "alpha,", "(beta)", "gamma.",
                                                 "delta", "epsilon", "zeta",   "eta"};
        CHECK((buildDiagnosticAltOrQuery(tokens) ==
               "\"alpha\" OR \"beta\" OR \"gamma\" OR \"delta\" OR \"epsilon\""));
    }
}

TEST_CASE("Search query helpers: hasAdvancedFts5Operators distinguishes power-user syntax",
          "[unit][metadata][fts5][search-query-helpers]") {
    using yams::metadata::hasAdvancedFts5Operators;

    CHECK((hasAdvancedFts5Operators("title:hello")));
    CHECK((hasAdvancedFts5Operators("hello NEAR/3 world")));
    CHECK((hasAdvancedFts5Operators("\"exact phrase\"")));
    CHECK((hasAdvancedFts5Operators("(foo OR bar) NOT baz")));

    CHECK_FALSE((hasAdvancedFts5Operators("dendritic cells (DCs)")));
    CHECK_FALSE((hasAdvancedFts5Operators("path:/tmp/file.txt")));
    CHECK_FALSE((hasAdvancedFts5Operators("hello and world")));
    CHECK_FALSE((hasAdvancedFts5Operators("plain terms only")));

    // C++ scope qualifiers are literal content, not FTS field selectors.
    CHECK_FALSE((hasAdvancedFts5Operators("SqliteVecBackend::bruteForceSearch")));
    CHECK_FALSE(
        (hasAdvancedFts5Operators("yams::vector::SqliteVecBackend::Impl::bruteForceSearch")));
}

TEST_CASE("Search query helpers: sanitizeFts5UserQuery quotes C++ qualified names",
          "[unit][metadata][fts5][search-query-helpers]") {
    using yams::metadata::sanitizeFts5UserQuery;

    // A C++ qualified name must reach SQLite as a quoted literal phrase; otherwise FTS5
    // parses the leading qualifier as a column selector and fails with "no such column".
    CHECK((sanitizeFts5UserQuery("SqliteVecBackend::bruteForceSearch") ==
           "\"SqliteVecBackend::bruteForceSearch\""));
    CHECK((sanitizeFts5UserQuery("yams::vector::SqliteVecBackend::Impl::bruteForceSearch") ==
           "\"yams::vector::SqliteVecBackend::Impl::bruteForceSearch\""));

    // Single-colon field syntax remains available for power users.
    CHECK((sanitizeFts5UserQuery("title:hello") == "title:hello"));
}
