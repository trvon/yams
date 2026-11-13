// Search scores are normalised to the top hit, so a query that matches
// nothing lexically can still return results scored 1.0. The JSON output
// must say how many results matched the query terms.
#include "embedded_cli_fixture.h"

#include <string>

using yams::test::EmbeddedCorpus;

TEST_CASE("search --json reports lexical hits", "[cli][search][no-match][contract]") {
    EmbeddedCorpus corpus("yams_search_no_match_");
    corpus.addText("ledger.txt", "quarterly ledger reconciliation notes\n");

    SECTION("a query term present in the corpus") {
        const auto r = corpus.run({"--json", "search", "ledger"});
        INFO(r.out << r.err);
        REQUIRE(r.rc == 0);
        const auto j = nlohmann::json::parse(r.out);
        REQUIRE(j.contains("lexical_hits"));
        CHECK(j.at("lexical_hits").get<int>() > 0);
    }
    SECTION("a query that matches nothing") {
        const auto r = corpus.run({"--json", "search", "zzqxqzzqxq"});
        INFO(r.out << r.err);
        REQUIRE(r.rc == 0);
        const auto j = nlohmann::json::parse(r.out);
        REQUIRE(j.contains("lexical_hits"));
        CHECK(j.at("lexical_hits").get<int>() == 0);
    }
    SECTION("grouped JSON reports it too") {
        for (const auto* query : {"ledger", "zzqxqzzqxq"}) {
            const auto r = corpus.run({"--json", "search", "--json-grouped", query});
            INFO(query << ": " << r.out << r.err);
            REQUIRE(r.rc == 0);
            const auto j = nlohmann::json::parse(r.out);
            REQUIRE(j.contains("lexical_hits"));
            CHECK((j.at("lexical_hits").get<int>() > 0) == (std::string(query) == "ledger"));
        }
    }
}
