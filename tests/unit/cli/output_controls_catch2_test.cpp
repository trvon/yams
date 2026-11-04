#include <sstream>
#include <catch2/catch_test_macros.hpp>
#include <yams/cli/content_slice_options.h>
#include <yams/cli/output_controls.h>

using namespace yams::cli;

TEST_CASE("CLI slice flags have help, validation and mutual exclusion",
          "[cli][read-slice][flags]") {
    CLI::App app;
    ContentSliceOptions opts;
    registerContentSliceOptions(app, opts);
    for (const auto* name : {"--lines", "--around", "--context"})
        REQUIRE(app.get_option(name)->get_description().size() > 0);
    const char* args[]{"test", "--lines", "10:30"};
    REQUIRE_NOTHROW(app.parse(3, args));
    CHECK(opts.lines == "10:30");
    app.clear();
    const char* bad[]{"test", "--lines", "10:30", "--around", "needle"};
    CHECK_THROWS_AS(app.parse(5, bad), CLI::ParseError);
}

TEST_CASE("CLI grep caps matches globally and paths uniquely", "[cli][limits]") {
    struct Hit {
        std::string file;
    };
    std::vector<Hit> hits{{"a"}, {"a"}, {"b"}, {"c"}};
    auto matches = hits;
    capGrepResults(matches, 2, false);
    REQUIRE(matches.size() == 2);
    CHECK(matches[1].file == "a");
    capGrepResults(hits, 2, true);
    REQUIRE(hits.size() == 2);
    CHECK(hits[0].file == "a");
    CHECK(hits[1].file == "b");
}

TEST_CASE("CLI budgets retain complete lines and count omitted output", "[cli][budget]") {
    std::ostringstream out;
    {
        OutputBudget budget(out, 8);
        out << "one\n";
        noteOutputResult(out);
        out << "two\n";
        noteOutputResult(out);
        out << "three\n";
        noteOutputResult(out);
    }
    CHECK(out.str() == "one\ntwo\n[truncated: 6 more chars, 1 more results; narrow with "
                       "--path/--limit/--lines]\n");
}

TEST_CASE("CLI budgets bound huge lines and preserve unbudgeted binary bytes", "[cli][budget]") {
    std::ostringstream out;
    {
        OutputBudget budget(out, 2);
        out << std::string(100000, 'x') << "\ny\n";
    }
    CHECK(out.str().size() < 202);
    CHECK(out.str().starts_with("[truncated: 100003 more chars"));
    std::ostringstream raw;
    {
        OutputBudget budget(raw, 0);
        raw.write("a\0b", 3);
    }
    CHECK(raw.str() == std::string("a\0b", 3));
}

TEST_CASE("CLI budgets handle unterminated lines and flushes", "[cli][budget]") {
    std::ostringstream out;
    {
        OutputBudget budget(out, 5);
        out << "ok\n" << std::flush << "long";
    }
    CHECK(out.str().starts_with("ok\n[truncated: 4 more chars"));
    std::ostringstream exact;
    {
        OutputBudget budget(exact, 3);
        exact << "abc";
    }
    CHECK(exact.str() == "abc");
}

TEST_CASE("CLI content slices use inclusive numbered lines", "[cli][read-slice]") {
    ContentSliceOptions opts;
    opts.lines = "2:3";
    CHECK(sliceContent("first\nsecond\nthird\nfourth\n", opts) == "2: second\n3: third\n");
    opts.lines = "0:2";
    CHECK_THROWS_AS(sliceContent("a\nb", opts), std::invalid_argument);
    opts.lines = "3:2";
    CHECK_THROWS_AS(sliceContent("a\nb", opts), std::invalid_argument);
    opts.lines = "1:2junk";
    CHECK_THROWS_AS(sliceContent("a\nb", opts), std::invalid_argument);
}

TEST_CASE("CLI around selects first literal match with bounded context", "[cli][read-slice]") {
    ContentSliceOptions opts;
    opts.around = "target";
    opts.context = 1;
    CHECK(sliceContent("a\ntarget\nc\ntarget\ne", opts) == "1: a\n2: target\n3: c\n");
    CHECK(sliceContent("absent", opts) == "(no results)\n");
    opts.lines = "1:2";
    CHECK_THROWS_AS(sliceContent("a", opts), std::invalid_argument);
}

TEST_CASE("CLI byte cap applies even to smart name retrieval", "[cli][read-slice]") {
    ContentSliceOptions opts;
    CHECK(sliceContent("abcdef", opts, 3) == "abc");
    CHECK(sliceContent(std::string_view("a\0b", 3), opts, 2) == std::string("a\0", 2));
}

TEST_CASE("CLI compact snippets are single bounded lines", "[cli][compact]") {
    CHECK(compactSnippet("hello\nworld\r\tthere") == "hello world there");
    CHECK(compactSnippet(std::string(200, 'x')).size() == 120);
    const auto hit = compactHit(std::string(300, 'p'), 100, std::string(500, 's'));
    CHECK(hit.size() < 180);
    CHECK(std::count(hit.begin(), hit.end(), '\n') == 1);
    CHECK(hit.size() * 5 < 900);
}

TEST_CASE("CLI ephemeral paths are component aware and allow named tagged notes",
          "[cli][ephemeral]") {
    RetrievalOutputConfig config;
    CHECK(excludeRetrievalPath("/tmp/copy.cpp", config));
    CHECK_FALSE(excludeRetrievalPath("/tmpish/real.cpp", config));
    CHECK(excludeRetrievalPath("/repo/scratchpad/copy.cpp", config));
    CHECK_FALSE(excludeRetrievalPath("/repo/src/main.cpp", config));
    CHECK_FALSE(excludeRetrievalPath("/tmp/note.md", config, "note.md", true));
    CHECK_FALSE(excludeRetrievalPath("/tmp/note.md", config, "", true));
    config.excludePaths = {"/generated/"};
    CHECK_FALSE(excludeRetrievalPath("/tmp/copy.cpp", config));
    CHECK(excludeRetrievalPath("/generated/copy.cpp", config));
}
