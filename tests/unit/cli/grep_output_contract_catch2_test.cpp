// `grep` output must not invent line numbers, hide truncation, or present a
// file from another tree as a path in the current one.
#include "embedded_cli_fixture.h"

#include <filesystem>
#include <fstream>
#include <string>

#include <yams/cli/graph_helpers.h>

using yams::test::EmbeddedCorpus;

TEST_CASE("grep prints real line numbers or none", "[cli][grep][output]") {
    EmbeddedCorpus corpus("yams_grep_lines_");
    corpus.addText("notes.txt", "first line\nsecond needle line\nthird line\n");

    const auto r = corpus.run({"grep", "needle"});
    INFO(r.out << r.err);
    REQUIRE(r.rc == 0);
    CHECK(r.out.find("second needle line") != std::string::npos);
    CHECK(r.out.find(" 0:") == std::string::npos);
    CHECK(r.out.find("2:") != std::string::npos);
}

TEST_CASE("grep says when the per-file match cap cut output", "[cli][grep][output]") {
    EmbeddedCorpus corpus("yams_grep_cap_");
    std::string body;
    for (int i = 0; i < 25; ++i)
        body += "needle " + std::to_string(i) + "\n";
    corpus.addText("many.txt", body);

    const auto r = corpus.run({"grep", "needle"});
    INFO(r.out << r.err);
    REQUIRE(r.rc == 0);
    // Default -m is 20 per file; the summary must not read as the full count.
    CHECK((r.out + r.err).find("per file") != std::string::npos);
}

TEST_CASE("grep says when the daemon result window cut output", "[cli][grep][output]") {
    EmbeddedCorpus corpus("yams_grep_window_");
    std::string body;
    for (int i = 0; i < 15; ++i)
        body += "needle " + std::to_string(i) + "\n";
    corpus.addText("first.txt", body);
    corpus.addText("second.txt", body);

    const auto r = corpus.run({"grep", "needle"});
    INFO(r.out << r.err);
    REQUIRE(r.rc == 0);
    // 30 matching lines, at most 20 returned: the summary must not read as complete.
    CHECK(r.out.find("of 30") != std::string::npos);
}

TEST_CASE("grep -l and -c report what the plain mode found", "[cli][grep][output]") {
    EmbeddedCorpus corpus("yams_grep_modes_");
    corpus.addText("two.txt", "needle one\nfiller\nneedle two\n");
    corpus.addText("none.txt", "filler only\n");

    const auto plain = corpus.run({"grep", "needle"});
    INFO(plain.out << plain.err);
    REQUIRE(plain.rc == 0);
    REQUIRE(plain.out.find("needle one") != std::string::npos);

    const auto files = corpus.run({"grep", "-l", "needle"});
    INFO(files.out << files.err);
    CHECK(files.out.find("two.txt") != std::string::npos);
    CHECK(files.out.find("none.txt") == std::string::npos);
    CHECK(files.out.find("no results") == std::string::npos);

    const auto count = corpus.run({"grep", "-c", "needle"});
    INFO(count.out << count.err);
    CHECK(count.out.find("2") != std::string::npos);
    CHECK(count.out.find("no results") == std::string::npos);
}

TEST_CASE("grep says when it stopped at its time budget", "[cli][grep][output]") {
    EmbeddedCorpus corpus("yams_grep_budget_");
    corpus.addText("notes.txt", "first line\nsecond needle line\n");
    // One millisecond is spent before the first document is read.
    yams::test::ScopedEnvVar budget{"YAMS_GREP_TIME_BUDGET_MS", std::string("1")};

    const auto r = corpus.run({"grep", "needle"});
    INFO(r.out << r.err);
    // An answer cut short by the clock must not read as "nothing matches".
    CHECK((r.out + r.err).find("time budget") != std::string::npos);
    CHECK((r.out + r.err).find("incomplete") != std::string::npos);
}

TEST_CASE("paths outside the working directory display in full", "[cli][grep][output]") {
    const auto cwd = std::filesystem::temp_directory_path() / "yams_display_cwd";
    const auto other =
        (cwd.parent_path() / "yams_display_other" / "src" / "shared.cpp").generic_string();
    const auto presentation = yams::cli::describeFileForCli(other, {}, cwd);
    // "src/shared.cpp" would name a different file under cwd.
    CHECK(presentation.displayPath == other);

    const auto inside = (cwd / "src" / "local.cpp").generic_string();
    CHECK(yams::cli::describeFileForCli(inside, {}, cwd).displayPath == "src/local.cpp");
}
