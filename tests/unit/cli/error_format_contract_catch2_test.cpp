// One error format on stderr: "error: <message>", then an optional
// "hint: ..." line. No log timestamps, no repeated message, no generic hint.
// Usage errors exit 2, runtime failures exit 1.
#include "embedded_cli_fixture.h"

#include <fstream>
#include <string>

using yams::test::CliRun;
using yams::test::EmbeddedCorpus;

namespace {

std::size_t countOf(const std::string& haystack, const std::string& needle) {
    std::size_t n = 0;
    for (auto pos = haystack.find(needle); pos != std::string::npos;
         pos = haystack.find(needle, pos + needle.size()))
        ++n;
    return n;
}

void checkFormat(const CliRun& r) {
    INFO("stderr: " << r.err);
    CHECK(r.err.rfind("error: ", 0) == 0);
    CHECK(r.err.find("Check command syntax") == std::string::npos);
    CHECK(r.err.find("\xF0\x9F\x92\xA1") == std::string::npos); // light-bulb emoji
    CHECK(r.err.find("[error]") == std::string::npos);
}

} // namespace

TEST_CASE("Usage errors use one format and exit 2", "[cli][errors][contract]") {
    EmbeddedCorpus corpus("yams_error_format_");

    SECTION("missing required selector") {
        // corpus.run puts `--data-dir <dir>` first; the hint must name `get`.
        const auto r = corpus.run({"get"});
        CHECK(r.rc == 2);
        checkFormat(r);
        CHECK(r.err.find("`yams get --help`") != std::string::npos);
    }
    SECTION("the -F hint quotes the pattern for the shell") {
        const auto r = corpus.run({"grep", "a\"(["});
        CHECK(r.rc == 2);
        CHECK(r.err.find("yams grep -F 'a\"(['") != std::string::npos);
    }
    SECTION("empty search query") {
        const auto r = corpus.run({"search", ""});
        CHECK(r.rc == 2);
        checkFormat(r);
    }
    SECTION("graph without a target prints its message once") {
        const auto r = corpus.run({"graph"});
        CHECK(r.rc == 2);
        checkFormat(r);
        CHECK(countOf(r.err, "Either hash or name is required") == 1);
    }
    SECTION("invalid regex keeps its -F tip and prints the error once") {
        const auto r = corpus.run({"grep", "(["});
        CHECK(r.rc == 2);
        checkFormat(r);
        CHECK(countOf(r.err, "nvalid regex") == 1);
        CHECK(r.err.find("-F") != std::string::npos);
    }
    SECTION("unknown command suggests the closest one") {
        const auto r = corpus.run({"serch", "foo"});
        CHECK(r.rc == 2);
        checkFormat(r);
        CHECK(r.err.find("yams search") != std::string::npos);
    }
}

TEST_CASE("Runtime failures use the same format and exit 1", "[cli][errors][contract]") {
    EmbeddedCorpus corpus("yams_error_runtime_");
    {
        std::ofstream config(corpus.root / "config.toml");
        config << "[core]\n";
    }

    const auto r = corpus.run({"config", "get", "no.such.key"});
    CHECK(r.rc == 1);
    checkFormat(r);
    CHECK(r.err.find("no.such.key") != std::string::npos);
}
