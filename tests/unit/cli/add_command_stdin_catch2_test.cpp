// CLI Add Command stdin tests
// Catch2 migration from GTest (yams-3s4 / yams-cli)
//
// Tests for add command reading from piped stdin.

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <string>

#include <nlohmann/json.hpp>

#include "../../common/test_helpers_catch2.h"
#include <yams/compat/unistd.h>

#ifdef _WIN32
#define WIFEXITED(x) ((x) != -1)
#define WEXITSTATUS(x) (x)
#else
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

    // Prefer the freshly built yams-cli when available; fall back to PATH.
    fs::path out = tmp / "out.json";
    const fs::path builtYams =
        fs::current_path() / "build" / "release" / "tools" / "yams-cli" / "yams-cli";
    const std::string yamsBin = fs::exists(builtYams) ? builtYams.string() : std::string("yams");
    std::string cmd = "cat '" + in.string() + "' | '" + yamsBin +
                      "' --json add - --name piped.txt > '" + out.string() + "'";
    int rc = std::system(cmd.c_str());

    if (rc == -1 || (WIFEXITED(rc) && WEXITSTATUS(rc) == 127)) {
        // Cleanup temp directory
        std::error_code ec;
        fs::remove_all(tmp, ec);
        SKIP("yams binary not available in PATH for CLI test");
    }
    REQUIRE(WIFEXITED(rc));
    CHECK(WEXITSTATUS(rc) == 0);

    // Validate JSON output
    {
        std::ifstream i(out);
        REQUIRE(i.good());
        std::string content((std::istreambuf_iterator<char>(i)), std::istreambuf_iterator<char>());
        REQUIRE(!content.empty());

        // Some invocations might print non-JSON lines; ensure we parse from the first '{'.
        auto pos = content.find('{');
        REQUIRE(pos != std::string::npos);
        content = content.substr(pos);

        nlohmann::json j;
        REQUIRE_NOTHROW(j = nlohmann::json::parse(content));
        REQUIRE(j.is_object());

        REQUIRE(j.contains("results"));
        REQUIRE(j["results"].is_array());
        REQUIRE(!j["results"].empty());

        REQUIRE(j.contains("summary"));
        REQUIRE(j["summary"].is_object());
        REQUIRE(j["summary"].contains("added"));
        REQUIRE(j["summary"].contains("updated"));
        REQUIRE(j["summary"].contains("skipped"));
        REQUIRE(j["summary"].contains("failed"));

        const auto& r0 = j["results"][0];
        REQUIRE(r0.is_object());
        REQUIRE(r0.contains("path"));
        REQUIRE(r0.contains("success"));
    }

    // Cleanup temp directory
    std::error_code ec;
    fs::remove_all(tmp, ec);
#endif
}
