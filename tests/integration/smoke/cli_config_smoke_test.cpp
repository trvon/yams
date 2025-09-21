#include <CLI/CLI.hpp>
#include <gtest/gtest.h>

TEST(IntegrationSmoke, CLI11ConfigParse) {
    CLI::App app{"yams smoke"};
    std::string config_path;
    int threads = 0;
    bool verbose = false;
    app.add_option("--config", config_path, "Path to config file");
    app.add_option("-j,--threads", threads, "Worker threads");
    app.add_flag("-v,--verbose", verbose, "Verbose output");

    const char* argv[] = {"yams-smoke", "--config", "/tmp/yams.cfg", "-j", "4", "-v"};
    int argc = static_cast<int>(sizeof(argv) / sizeof(argv[0]));
    EXPECT_NO_THROW(app.parse(argc, const_cast<char**>(argv)));
    EXPECT_EQ(config_path, "/tmp/yams.cfg");
    EXPECT_EQ(threads, 4);
    EXPECT_TRUE(verbose);
}
