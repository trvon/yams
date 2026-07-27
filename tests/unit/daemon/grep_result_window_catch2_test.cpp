#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/components/grep_result_window.h>

// This focused executable does not link the daemon client's runtime. App service headers
// transitively instantiate the global initializer, so provide inert lifecycle definitions.
namespace yams::daemon {
GlobalIOContextInitializer::GlobalIOContextInitializer() = default;
GlobalIOContextInitializer::~GlobalIOContextInitializer() = default;
} // namespace yams::daemon

namespace {

yams::app::services::GrepMatch makeMatch(std::size_t lineNumber, std::string line) {
    yams::app::services::GrepMatch match;
    match.lineNumber = lineNumber;
    match.line = std::move(line);
    match.matchType = "regex";
    return match;
}

yams::app::services::GrepFileResult
makeFileResult(std::string file, std::vector<yams::app::services::GrepMatch> matches) {
    yams::app::services::GrepFileResult result;
    result.file = std::move(file);
    result.matches = std::move(matches);
    result.matchCount = result.matches.size();
    return result;
}

} // namespace

TEST_CASE("Grep result window deduplicates identities before applying its total cap",
          "[daemon][grep][result-window]") {
    const std::vector fileResults{
        makeFileResult("a.cpp", {makeMatch(10, "  alpha   beta "), makeMatch(10, "alpha beta"),
                                 makeMatch(11, "second")}),
        makeFileResult("b.cpp", {makeMatch(4, "third"), makeMatch(5, "fourth")}),
    };

    const auto selection = yams::daemon::grep_result_window::select(fileResults, 3);

    CHECK(selection.stats.inputMatches == 5);
    CHECK(selection.stats.uniqueMatches == 4);
    CHECK(selection.stats.emittedMatches == 3);
    REQUIRE(selection.matches.size() == 3);
    CHECK(selection.matches[0].file == "a.cpp");
    CHECK(selection.matches[0].lineNumber == 10);
    CHECK(selection.matches[0].line == "  alpha   beta ");
    CHECK(selection.matches[1].lineNumber == 11);
    CHECK(selection.matches[2].file == "b.cpp");
    CHECK(selection.matches[2].lineNumber == 4);
}

TEST_CASE("Grep result window retains equal text at distinct evidence locations",
          "[daemon][grep][result-window]") {
    const std::vector fileResults{
        makeFileResult("a.cpp", {makeMatch(10, "same"), makeMatch(11, "same")}),
        makeFileResult("b.cpp", {makeMatch(10, "same")}),
    };

    const auto selection = yams::daemon::grep_result_window::select(fileResults, 0);

    CHECK(selection.stats.inputMatches == 3);
    CHECK(selection.stats.uniqueMatches == 3);
    CHECK(selection.stats.emittedMatches == 3);
    CHECK(selection.matches.size() == 3);
}
