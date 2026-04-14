#include <CLI/CLI.hpp>
#include <catch2/catch_test_macros.hpp>

TEST_CASE("IntegrationSmoke.CLI11ConfigParse", "[smoke][integration][cli]") {
    CLI::App app{"yams smoke"};
    std::string config_path;
    int threads = 0;
    bool verbose = false;
    app.add_option("--config", config_path, "Path to config file");
    app.add_option("-j,--threads", threads, "Worker threads");
    app.add_flag("-v,--verbose", verbose, "Verbose output");

    const char* argv[] = {"yams-smoke", "--config", "/tmp/yams.cfg", "-j", "4", "-v"};
    int argc = static_cast<int>(sizeof(argv) / sizeof(argv[0]));
    CHECK_NOTHROW(app.parse(argc, const_cast<char**>(argv)));
    CHECK(config_path == "/tmp/yams.cfg");
    CHECK(threads == 4);
    CHECK(verbose);
}
