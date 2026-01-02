// CLI Repair Command tests
// Catch2 tests for repair command option parsing and behavior
//
// Tests for repair command flag handling, verbose mode defaults,
// and CLI option isolation.

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/yams_cli.h>

#include <filesystem>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

// Include test helpers for ScopedEnvVar
#include "../../common/test_helpers_catch2.h"

namespace fs = std::filesystem;
using namespace yams::cli;

namespace {

/**
 * RAII helper for repair command tests with proper isolation and cleanup.
 */
struct RepairTestHelper {
    fs::path tempDir;
    std::optional<yams::test::ScopedEnvVar> configEnv;
    std::optional<yams::test::ScopedEnvVar> dataEnv;
    std::optional<yams::test::ScopedEnvVar> nonInteractiveEnv;
    std::optional<yams::test::ScopedEnvVar> disableDaemonEnv;

    RepairTestHelper() {
        tempDir = yams::test::make_temp_dir("yams_repair_catch2_test_");
        fs::create_directories(tempDir);

        configEnv.emplace("YAMS_CONFIG", (tempDir / "config.toml").string());
        dataEnv.emplace("YAMS_DATA_DIR", (tempDir / "data").string());
        nonInteractiveEnv.emplace(std::string("YAMS_NON_INTERACTIVE"),
                                  std::optional<std::string>("1"));
        // Disable daemon autostart to prevent cleanup crashes
        disableDaemonEnv.emplace(std::string("YAMS_CLI_DISABLE_DAEMON_AUTOSTART"),
                                 std::optional<std::string>("1"));
    }

    ~RepairTestHelper() {
        configEnv.reset();
        dataEnv.reset();
        nonInteractiveEnv.reset();
        disableDaemonEnv.reset();

        std::error_code ec;
        fs::remove_all(tempDir, ec);
    }

    std::unique_ptr<YamsCLI> makeCli() const { return std::make_unique<YamsCLI>(); }

    int runCommand(const std::vector<std::string>& args) {
        auto cli = makeCli();
        std::vector<char*> argv;
        argv.reserve(args.size());
        for (const auto& arg : args) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        return cli->run(static_cast<int>(argv.size()), argv.data());
    }
};

/**
 * RAII helper to capture and restore cout.
 */
class CaptureStdout {
public:
    CaptureStdout() : oldCout_(std::cout.rdbuf(buffer_.rdbuf())) {}
    ~CaptureStdout() { std::cout.rdbuf(oldCout_); }

    std::string str() const { return buffer_.str(); }

    CaptureStdout(const CaptureStdout&) = delete;
    CaptureStdout& operator=(const CaptureStdout&) = delete;

private:
    std::ostringstream buffer_;
    std::streambuf* oldCout_;
};

} // namespace

TEST_CASE("RepairCommand - help displays without errors", "[cli][repair][catch2]") {
    RepairTestHelper helper;

    CaptureStdout capture;

    auto cli = helper.makeCli();
    const char* argv[] = {"yams", "repair", "--help"};

    // Should not throw - --help exits early
    (void)cli->run(3, const_cast<char**>(argv));

    std::string output = capture.str();

    // Should show repair help text
    CHECK(output.find("repair") != std::string::npos);
    CHECK(output.find("--orphans") != std::string::npos);
    CHECK(output.find("--fts5") != std::string::npos);
    CHECK(output.find("--embeddings") != std::string::npos);
}

TEST_CASE("RepairCommand - verbose flag is documented correctly", "[cli][repair][catch2]") {
    RepairTestHelper helper;

    CaptureStdout capture;

    auto cli = helper.makeCli();
    const char* argv[] = {"yams", "repair", "--help"};
    (void)cli->run(3, const_cast<char**>(argv));

    std::string output = capture.str();

    // Verbose flag should mention transient errors (the key change from the refactor)
    CHECK(output.find("--verbose") != std::string::npos);
    CHECK(output.find("-v") != std::string::npos);
}

TEST_CASE("RepairCommand - option flags parse correctly", "[cli][repair][catch2][.slow]") {
    // SKIP: This test actually runs repair operations which require database/daemon
    // and can hang in test environment. Tagged with [.slow] to exclude from default runs.
    SKIP("Repair operations require database access - use integration tests");
}

// Note: Tests that actually run repair operations (--fts5, --embeddings, --all, etc.)
// are skipped because they require database access and can hang in unit test environment.
// Integration tests should cover these scenarios with proper daemon/database setup.

TEST_CASE("RepairCommand - fts5 flag parses correctly", "[cli][repair][catch2][.slow]") {
    SKIP("Repair operations require database access - use integration tests");
}

TEST_CASE("RepairCommand - embeddings flag with verbose parses correctly",
          "[cli][repair][catch2][.slow]") {
    SKIP("Repair operations require database access - use integration tests");
}

TEST_CASE("RepairCommand - all flag enables all repair operations",
          "[cli][repair][catch2][.slow]") {
    SKIP("Repair operations require database access - use integration tests");
}

TEST_CASE("RepairCommand - model option accepts value", "[cli][repair][catch2][.slow]") {
    SKIP("Repair operations require database access - use integration tests");
}

TEST_CASE("RepairCommand - timeout option accepts value", "[cli][repair][catch2][.slow]") {
    SKIP("Repair operations require database access - use integration tests");
}
