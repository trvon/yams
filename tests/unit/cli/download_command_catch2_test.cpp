// CLI Download Command tests
// Catch2 tests for download command option parsing and daemon job controls.

#include <catch2/catch_test_macros.hpp>

#include <CLI/CLI.hpp>

#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>

#include <memory>
#include <string>
#include <vector>

namespace yams::cli {
std::unique_ptr<ICommand> createDownloadCommand();
}

namespace {

void parseDownloadCommandWithHelp(const std::vector<const char*>& argv) {
    CLI::App app{"yams"};
    auto downloadCmd = yams::cli::createDownloadCommand();
    downloadCmd->registerCommand(app, /*cli*/ nullptr);

    bool gotHelp = false;
    bool gotParseError = false;
    std::string parseErrorMsg;

    try {
        app.parse(static_cast<int>(argv.size()), const_cast<char**>(argv.data()));
    } catch (const CLI::CallForHelp&) {
        gotHelp = true;
    } catch (const CLI::ParseError& e) {
        gotParseError = true;
        parseErrorMsg = e.what();
    }

    CHECK(gotHelp);
    if (gotParseError) {
        FAIL("Unexpected parse error: " << parseErrorMsg);
    }
}

} // namespace

TEST_CASE("DownloadCommand - help shows daemon job options", "[cli][download][catch2]") {
    auto cli = std::make_unique<yams::cli::YamsCLI>();
    const char* argv[] = {"yams", "download", "--help"};

    REQUIRE_NOTHROW((void)cli->run(3, const_cast<char**>(argv)));
}

TEST_CASE("DownloadCommand - url download flags parse with help", "[cli][download][catch2]") {
    parseDownloadCommandWithHelp({"yams", "download", "https://example.com/file.txt", "--checksum",
                                  "sha256:deadbeef", "--tag", "archive", "--meta", "owner=opencode",
                                  "--json", "--help"});
}

TEST_CASE("DownloadCommand - status action parses with help", "[cli][download][catch2]") {
    parseDownloadCommandWithHelp({"yams", "download", "--status", "job-123", "--json", "--help"});
}

TEST_CASE("DownloadCommand - list jobs action parses with help", "[cli][download][catch2]") {
    parseDownloadCommandWithHelp({"yams", "download", "--list-jobs", "--json", "--help"});
}

TEST_CASE("DownloadCommand - cancel action parses with help", "[cli][download][catch2]") {
    parseDownloadCommandWithHelp({"yams", "download", "--cancel", "job-456", "--help"});
}
