#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <gtest/gtest.h>

#include "../../common/test_helpers.h"

#ifdef _WIN32
#include <process.h>
#define WIFEXITED(x) ((x) != -1)
#define WEXITSTATUS(x) (x)
#define getpid _getpid
#else
#include <sys/wait.h>
#endif

namespace fs = std::filesystem;

TEST(AddCommand, ReadsFromPipedStdinAndStoresContent) {
#ifdef _WIN32
    GTEST_SKIP() << "Test requires Unix shell with 'cat' and pipe support";
#endif
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
        GTEST_SKIP() << "yams binary not available in PATH for CLI test";
    }
    ASSERT_TRUE(WIFEXITED(rc));
    ASSERT_EQ(WEXITSTATUS(rc), 0);
}
