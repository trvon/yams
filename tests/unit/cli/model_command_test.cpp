#include <gtest/gtest.h>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>

using namespace yams::cli;

TEST(ModelCommandCLI, ListsAndShowsInfo) {
    {
        YamsCLI cli;
        const char* argv1[] = {"yams", "model", "--list"};
        (void)cli.run(static_cast<int>(std::size(argv1)), const_cast<char**>(argv1));
    }
    {
        YamsCLI cli;
        const char* argv2[] = {"yams", "model", "--info", "all-MiniLM-L6-v2"};
        (void)cli.run(static_cast<int>(std::size(argv2)), const_cast<char**>(argv2));
    }
    SUCCEED();
}
