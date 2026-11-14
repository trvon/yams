// `yams list` time filters must change what is listed.
#include "embedded_cli_fixture.h"

#include <string>

using yams::test::EmbeddedCorpus;

namespace {

std::size_t listed(const EmbeddedCorpus& corpus, std::vector<std::string> filter) {
    std::vector<std::string> args{"list", "--format", "json", "--limit", "100"};
    args.insert(args.end(), filter.begin(), filter.end());
    const auto r = corpus.run(std::move(args));
    INFO(r.out << r.err);
    REQUIRE(r.rc == 0);
    return nlohmann::json::parse(r.out).at("documents").size();
}

} // namespace

TEST_CASE("list time filters select documents", "[cli][list][time]") {
    // Every filter was accepted and ignored: `list --since 1h` printed the whole store.
    EmbeddedCorpus corpus("yams_list_time_");
    corpus.addText("one.txt", "first document\n");
    corpus.addText("two.txt", "second document\n");
    REQUIRE(listed(corpus, {}) == 2);

    CHECK(listed(corpus, {"--since", "2100-01-01T00:00:00Z"}) == 0);
    CHECK(listed(corpus, {"--since", "1h"}) == 2);
    CHECK(listed(corpus, {"--indexed-before", "2000-01-01T00:00:00Z"}) == 0);
    CHECK(listed(corpus, {"--indexed-after", "2000-01-01T00:00:00Z"}) == 2);
    CHECK(listed(corpus, {"--indexed-after", "2100-01-01T00:00:00Z"}) == 0);

    SECTION("a time that cannot be read lists nothing and reports an error") {
        const auto r = corpus.run({"list", "--format", "json", "--since", "the day before never"});
        INFO(r.out << r.err);
        const auto answer = nlohmann::json::parse(r.out);
        CHECK(answer.at("documents").empty());
        CHECK(answer.contains("error"));
    }
}
