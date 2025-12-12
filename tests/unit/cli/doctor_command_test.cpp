#include <algorithm>
#include <iostream>
#include <sstream>
#include <gtest/gtest.h>
#include <yams/cli/yams_cli.h>

#include "../../common/cli_test_fixture.h"

using namespace yams;
using namespace yams::cli;
using namespace yams::test;

class DoctorCommandTest : public CliTestFixture {};

TEST_F(DoctorCommandTest, PruneHelpDoesNotRunDoctorDiagnostics) {
    // When running `yams doctor prune` without arguments (help mode),
    // it should NOT execute the default doctor diagnostics (runAll)

    std::ostringstream output;
    std::streambuf* oldCout = std::cout.rdbuf(output.rdbuf());

    // Create CLI instance via fixture
    auto cli = makeCli();

    // Parse args as if user ran: yams doctor prune
    const char* argv[] = {"yams", "doctor", "prune"};
    int argc = 3;

    // This should display prune help and NOT run doctor diagnostics
    cli->run(argc, const_cast<char**>(argv));

    std::cout.rdbuf(oldCout); // restore

    std::string outputStr = output.str();

    // Should show prune help text
    EXPECT_NE(outputStr.find("YAMS Doctor Prune"), std::string::npos);
    EXPECT_NE(outputStr.find("Composite Categories"), std::string::npos);

    // Should NOT show doctor diagnostics output
    EXPECT_EQ(outputStr.find("Collecting system information"), std::string::npos);
    EXPECT_EQ(outputStr.find("Daemon Status"), std::string::npos);
}

TEST_F(DoctorCommandTest, PruneWithCategoryDoesNotRunDoctorDiagnostics) {
    std::ostringstream output;
    std::streambuf* oldCout = std::cout.rdbuf(output.rdbuf());

    auto cli = makeCli();

    // Parse args: yams doctor prune --category build-artifacts
    const char* argv[] = {"yams", "doctor", "prune", "--category", "build-artifacts"};
    int argc = 5;

    // Note: This will fail if daemon not running, but that's OK for the test
    // We're just checking it doesn't run doctor diagnostics
    cli->run(argc, const_cast<char**>(argv));

    std::cout.rdbuf(oldCout);

    std::string outputStr = output.str();

    // Should NOT show doctor diagnostics output
    EXPECT_EQ(outputStr.find("Collecting system information"), std::string::npos);
}

// Note: We don't test bare "yams doctor" because it actually runs diagnostics
// which may hang or take a long time. The key fix is ensuring subcommands
// like "prune" don't trigger diagnostics.

TEST_F(DoctorCommandTest, PruneHelpDisplaysWithoutErrors) {
    // Test that the UI frames are built correctly
    std::ostringstream output;
    std::streambuf* oldCout = std::cout.rdbuf(output.rdbuf());

    auto cli = makeCli();
    const char* argv[] = {"yams", "doctor", "prune"};
    int argc = 3;

    // Should not throw
    EXPECT_NO_THROW(cli->run(argc, const_cast<char**>(argv)));

    std::cout.rdbuf(oldCout);

    std::string outputStr = output.str();

    // Check for proper formatting
    EXPECT_NE(outputStr.find("YAMS Doctor Prune"), std::string::npos);
    EXPECT_NE(outputStr.find("build-artifacts"), std::string::npos);
    EXPECT_NE(outputStr.find("Usage Examples"), std::string::npos);

    // Should not contain error messages (case-insensitive check)
    std::string lowerOutput = outputStr;
    std::transform(lowerOutput.begin(), lowerOutput.end(), lowerOutput.begin(), ::tolower);

    // Allow "error" in words like "stderr" but not standalone
    bool hasError = lowerOutput.find(" error") != std::string::npos ||
                    lowerOutput.find("error ") != std::string::npos ||
                    lowerOutput.find("failed") != std::string::npos;
    EXPECT_FALSE(hasError);
}
