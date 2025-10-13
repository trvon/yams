#include <gtest/gtest.h>
#include <yams/cli/yams_cli.h>

using namespace yams::cli;

TEST(SessionCommandCLI, DiffSubcommandParses) {
    YamsCLI cli;
    const char* argv[] = {"yams", "session", "diff", "--json"};
    (void)cli.run(static_cast<int>(std::size(argv)), const_cast<char**>(argv));
    SUCCEED();
}

#include <yams/cli/yams_cli.h>

using namespace yams::cli;

TEST(SessionCommandCLI, ParsesWarmWithBudgets) {
    // Ensure CLI constructs and registers commands without throwing
    YamsCLI cli;
    const char* argv[] = {"yams",    "session", "warm",          "--limit", "10",
                          "--cores", "2",       "--snippet-len", "80"};
    // We only check that parsing runs to completion (return code may be non-zero due to
    // environment)
    (void)cli.run(static_cast<int>(std::size(argv)), const_cast<char**>(argv));
    SUCCEED();
}
