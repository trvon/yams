#include <gtest/gtest.h>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>

#include "../../common/cli_test_fixture.h"

using namespace yams::cli;
using namespace yams::test;

class ModelCommandCLITest : public CliTestFixture {};

TEST_F(ModelCommandCLITest, ListsAndShowsInfo) {
    // Test --list flag
    EXPECT_EQ(runCommand({"yams", "model", "--list"}), 0);

    // Test --info flag (may return non-zero if model not installed, but should not crash)
    (void)runCommand({"yams", "model", "--info", "all-MiniLM-L6-v2"});
    SUCCEED();
}
