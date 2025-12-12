#include <gtest/gtest.h>
#include <yams/cli/yams_cli.h>

#include "../../common/cli_test_fixture.h"

using namespace yams::cli;
using namespace yams::test;

class SessionCommandCLITest : public CliTestFixture {};

TEST_F(SessionCommandCLITest, DiffSubcommandParses) {
    // Parsing should succeed (command may fail if no daemon, but should not crash)
    (void)runCommand({"yams", "session", "diff", "--json"});
    SUCCEED();
}

TEST_F(SessionCommandCLITest, ParsesWarmWithBudgets) {
    // Ensure CLI constructs and registers commands without throwing
    // Return code may be non-zero due to environment (no daemon)
    (void)runCommand({"yams", "session", "warm", "--limit", "10",
                      "--cores", "2", "--snippet-len", "80"});
    SUCCEED();
}
