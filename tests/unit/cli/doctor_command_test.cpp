#include <filesystem>
#include <sstream>
#include <gtest/gtest.h>
#include <yams/cli/yams_cli.h>
#include <yams/core/result.hpp>

using namespace yams;
using namespace yams::cli;

TEST(DoctorCommandTest, PruneHelpDoesNotRunDoctorDiagnostics) {
    // When running `yams doctor prune` without arguments (help mode),
    // it should NOT execute the default doctor diagnostics (runAll)

    std::ostringstream output;
    std::streambuf* oldCout = std::cout.rdbuf(output.rdbuf());

    // Create CLI instance
    YamsCLI cli;

    // Parse args as if user ran: yams doctor prune
    const char* argv[] = {"yams", "doctor", "prune"};
    int argc = 3;

    // This should display prune help and NOT run doctor diagnostics
    cli.run(argc, const_cast<char**>(argv));

    std::cout.rdbuf(oldCout); // restore

    std::string outputStr = output.str();

    // Should show prune help text
    EXPECT_NE(outputStr.find("YAMS Doctor Prune"), std::string::npos);
    EXPECT_NE(outputStr.find("Composite Categories"), std::string::npos);

    // Should NOT show doctor diagnostics output
    EXPECT_EQ(outputStr.find("Collecting system information"), std::string::npos);
    EXPECT_EQ(outputStr.find("Daemon Status"), std::string::npos);
}

TEST(DoctorCommandTest, PruneWithCategoryDoesNotRunDoctorDiagnostics) {
    std::ostringstream output;
    std::streambuf* oldCout = std::cout.rdbuf(output.rdbuf());

    YamsCLI cli;

    // Parse args: yams doctor prune --category build-artifacts
    const char* argv[] = {"yams", "doctor", "prune", "--category", "build-artifacts"};
    int argc = 5;

    // Note: This will fail if daemon not running, but that's OK for the test
    // We're just checking it doesn't run doctor diagnostics
    cli.run(argc, const_cast<char**>(argv));

    std::cout.rdbuf(oldCout);

    std::string outputStr = output.str();

    // Should NOT show doctor diagnostics output
    EXPECT_EQ(outputStr.find("Collecting system information"), std::string::npos);
}

TEST(DoctorCommandTest, BareDoctorCommandRunsDiagnostics) {
    std::ostringstream output;
    std::streambuf* oldCout = std::cout.rdbuf(output.rdbuf());

    YamsCLI cli;

    // Parse args: yams doctor
    const char* argv[] = {"yams", "doctor"};
    int argc = 2;

    cli.run(argc, const_cast<char**>(argv));

    std::cout.rdbuf(oldCout);

    std::string outputStr = output.str();

    // SHOULD show doctor diagnostics output
    EXPECT_NE(outputStr.find("Collecting system information"), std::string::npos);
}

TEST(DoctorCommandTest, PruneHelpDisplaysWithoutErrors) {
    // Test that the UI frames are built correctly
    std::ostringstream output;
    std::streambuf* oldCout = std::cout.rdbuf(output.rdbuf());

    YamsCLI cli;
    const char* argv[] = {"yams", "doctor", "prune"};
    int argc = 3;

    // Should not throw
    EXPECT_NO_THROW(cli.run(argc, const_cast<char**>(argv)));

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
