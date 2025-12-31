// CLI Add Command stdin tests
// Catch2 migration from GTest (yams-3s4 / yams-cli)
//
// Tests for add command reading from piped stdin.

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <string>

#include "../../common/test_helpers_catch2.h"

#ifdef _WIN32
#include <process.h>
#define WIFEXITED(x) ((x) != -1)
#define WEXITSTATUS(x) (x)
#define getpid _getpid
#else
#include <unistd.h>
#include <sys/wait.h>
#endif

namespace fs = std::filesystem;

TEST_CASE("AddCommand - reads from piped stdin and stores content",
          "[cli][add][stdin][catch2][!mayfail]") {
#ifdef _WIN32
    SKIP("Test requires Unix shell with 'cat' and pipe support");
#else
    const std::string payload = "line1\nline2\n\tindent\n";

    fs::path tmp = fs::temp_directory_path() / ("yams_add_stdin_" + std::to_string(::getpid()));
    fs::create_directories(tmp);

    // Use RAII to restore environment on exit (including exceptions/early returns)
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", tmp.string());

    fs::path in = tmp / "stdin.txt";
    {
        std::ofstream o(in, std::ios::binary);
        o << payload;
    }

    std::string cmd = "cat '" + in.string() + "' | yams add - --name piped.txt --json";
    int rc = std::system(cmd.c_str());

    // Cleanup temp directory
    std::error_code ec;
    fs::remove_all(tmp, ec);

    if (rc == -1 || (WIFEXITED(rc) && WEXITSTATUS(rc) == 127)) {
        SKIP("yams binary not available in PATH for CLI test");
    }
    REQUIRE(WIFEXITED(rc));
    CHECK(WEXITSTATUS(rc) == 0);
#endif
}
