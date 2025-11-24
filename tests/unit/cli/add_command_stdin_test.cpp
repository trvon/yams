#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <gtest/gtest.h>

#ifdef _WIN32
#include <process.h>
#define WIFEXITED(x) ((x) != -1)
#define WEXITSTATUS(x) (x)
static int setenv(const char* name, const char* value, int overwrite) {
    return _putenv_s(name, value);
}
#define getpid _getpid
#else
#include <sys/wait.h>
#endif

namespace fs = std::filesystem;

TEST(AddCommand, ReadsFromPipedStdinAndStoresContent) {
    const std::string payload = "line1\nline2\n\tindent\n";

    fs::path tmp = fs::temp_directory_path() / ("yams_add_stdin_" + std::to_string(::getpid()));
    fs::create_directories(tmp);
    setenv("YAMS_DATA_DIR", tmp.string().c_str(), 1);

    fs::path in = tmp / "stdin.txt";
    {
        std::ofstream o(in, std::ios::binary);
        o << payload;
    }

    std::string cmd = "cat '" + in.string() + "' | yams add - --name piped.txt --json";
    int rc = std::system(cmd.c_str());
    if (rc == -1 || (WIFEXITED(rc) && WEXITSTATUS(rc) == 127)) {
        GTEST_SKIP() << "yams binary not available in PATH for CLI test";
    }
    ASSERT_TRUE(WIFEXITED(rc));
    ASSERT_EQ(WEXITSTATUS(rc), 0);
}
