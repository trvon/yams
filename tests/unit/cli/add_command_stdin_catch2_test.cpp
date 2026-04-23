// CLI Add Command stdin tests
// Catch2 migration from GTest (yams-3s4 / yams-cli)
//
// Tests for add command reading from piped stdin.

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <string>
#include <chrono>

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

namespace {

auto findBuiltYamsCli() -> fs::path {
    std::vector<fs::path> bases;
    if (const char* sourceRoot = std::getenv("MESON_SOURCE_ROOT")) {
        bases.emplace_back(sourceRoot);
    }

    std::error_code ec;
    auto cwd = fs::current_path(ec);
    if (!ec) {
        bases.push_back(cwd);
        if (cwd.parent_path().filename() == "builddir-tsan" ||
            cwd.parent_path().filename() == "builddir-asan" ||
            cwd.parent_path().filename() == "builddir-ubsan" ||
            cwd.parent_path().filename() == "builddir-nosan" ||
            cwd.parent_path().filename() == "builddir") {
            bases.push_back(cwd.parent_path().parent_path());
            bases.push_back(cwd.parent_path());
        }
        if (cwd.filename() == "asan" || cwd.filename() == "debug" || cwd.filename() == "release" ||
            cwd.filename() == "coverage") {
            bases.push_back(cwd.parent_path().parent_path());
        } else if (cwd.filename() == "builddir" || cwd.filename() == "builddir-nosan" ||
                   cwd.filename() == "builddir-asan" || cwd.filename() == "builddir-tsan" ||
                   cwd.filename() == "builddir-ubsan") {
            bases.push_back(cwd.parent_path());
        }
    }

    const std::vector<fs::path> suffixes = {
        fs::path("builddir-tsan") / "tools" / "yams-cli" / "yams-cli",
        fs::path("builddir-asan") / "tools" / "yams-cli" / "yams-cli",
        fs::path("builddir") / "tools" / "yams-cli" / "yams-cli",
        fs::path("builddir-nosan") / "tools" / "yams-cli" / "yams-cli",
        fs::path("build") / "asan" / "tools" / "yams-cli" / "yams-cli",
        fs::path("build") / "coverage" / "tools" / "yams-cli" / "yams-cli",
        fs::path("build") / "release" / "tools" / "yams-cli" / "yams-cli",
        fs::path("build") / "debug" / "tools" / "yams-cli" / "yams-cli",
        fs::path("tools") / "yams-cli" / "yams-cli",
    };

    for (const auto& base : bases) {
        for (const auto& suffix : suffixes) {
            const auto candidate = base / suffix;
            if (fs::exists(candidate)) {
                return candidate;
            }
        }
    }
    return {};
}

} // namespace

TEST_CASE("AddCommand - reads from piped stdin and stores content", "[cli][add][stdin][catch2]") {
#ifdef _WIN32
    SKIP("Test requires Unix shell with 'cat' and pipe support");
#else
    const fs::path yamsCli = findBuiltYamsCli();
    REQUIRE_FALSE(yamsCli.empty());

    const std::string payload = "line1\nline2\n\tindent\n";

    const auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    fs::path tmp = fs::temp_directory_path() / ("yams_add_stdin_" + unique);
    fs::create_directories(tmp);

    // Use RAII to restore environment on exit (including exceptions/early returns)
    yams::test::ScopedEnvVar configEnv("YAMS_CONFIG", (tmp / "config.toml").string());
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", tmp.string());
    yams::test::ScopedEnvVar nonInteractiveEnv("YAMS_NON_INTERACTIVE",
                                               std::optional<std::string>{"1"});
    yams::test::ScopedEnvVar disableDaemonEnv("YAMS_CLI_DISABLE_DAEMON_AUTOSTART",
                                              std::optional<std::string>{"1"});

    fs::path in = tmp / "stdin.txt";
    {
        std::ofstream o(in, std::ios::binary);
        o << payload;
    }

    fs::path out = tmp / "out.json";
    std::string cmd = "cat '" + in.string() + "' | '" + yamsCli.string() +
                      "' --json add - --name piped.txt > '" + out.string() + "'";
    int rc = std::system(cmd.c_str());
    REQUIRE(WIFEXITED(rc));
    CHECK(WEXITSTATUS(rc) == 0);

    // Validate JSON output
    {
        std::ifstream i(out);
        REQUIRE(i.good());
        std::string content((std::istreambuf_iterator<char>(i)), std::istreambuf_iterator<char>());
        REQUIRE(!content.empty());

        // Some invocations may prepend or append non-JSON lines; isolate the JSON object.
        const auto first = content.find('{');
        const auto last = content.rfind('}');
        REQUIRE(first != std::string::npos);
        REQUIRE(last != std::string::npos);
        REQUIRE(last >= first);
        content = content.substr(first, last - first + 1);

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
