#include <gtest/gtest.h>

#include <exception>
#include <filesystem>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "common/test_helpers.h"

#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/global_io_context.h>

namespace fs = std::filesystem;

namespace {

class CaptureStdout {
public:
    CaptureStdout() : old_(std::cout.rdbuf(buffer_.rdbuf())) {}
    ~CaptureStdout() { std::cout.rdbuf(old_); }

    std::string str() const { return buffer_.str(); }

private:
    std::ostringstream buffer_;
    std::streambuf* old_{nullptr};
};

int run_cli(const std::vector<std::string>& args, std::string* output = nullptr,
            std::optional<std::string> stdinData = std::nullopt) {
    std::vector<std::string> effectiveArgs = args;
    const bool hasDataDirFlag =
        std::find(effectiveArgs.begin(), effectiveArgs.end(), "--data-dir") !=
            effectiveArgs.end() ||
        std::find(effectiveArgs.begin(), effectiveArgs.end(), "--storage") != effectiveArgs.end();
    if (!hasDataDirFlag) {
        if (const char* dataDir = std::getenv("YAMS_DATA_DIR"); dataDir && *dataDir) {
            effectiveArgs.insert(effectiveArgs.begin() + 1, std::string(dataDir));
            effectiveArgs.insert(effectiveArgs.begin() + 1, "--data-dir");
        }
    }
    int rc = 0;
    std::string captured;
    try {
        yams::cli::YamsCLI cli;
        std::vector<char*> argv;
        argv.reserve(effectiveArgs.size());
        for (const auto& arg : effectiveArgs) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }

        CaptureStdout capture;

        std::istringstream in;
        std::streambuf* oldIn = nullptr;
        if (stdinData.has_value()) {
            in.str(*stdinData);
            oldIn = std::cin.rdbuf(in.rdbuf());
        }

        {
            rc = cli.run(static_cast<int>(argv.size()), argv.data());
        }

        if (oldIn) {
            std::cin.rdbuf(oldIn);
        }
        if (captured.empty()) {
            captured = capture.str();
        }
    } catch (const std::exception& e) {
        rc = -1;
        captured = std::string("EXCEPTION: ") + e.what();
    } catch (...) {
        rc = -1;
        captured = "EXCEPTION: unknown";
    }
    if (output) {
        *output = std::move(captured);
    }
    return rc;
}

} // namespace

TEST(IntegrationSmoke, CliCommandsPreferInProcessFallbackOverLocalServices) {
    if (std::getenv("TSAN_OPTIONS") != nullptr) {
        GTEST_SKIP() << "TSAN-only teardown flake in repeated CLI fallback path";
    }

    const fs::path root = yams::test::make_temp_dir("yams_cli_transport_fallback_");
    const fs::path dataDir = root / "data";
    const fs::path blockedSocketDir = root / "blocked-socket";
    fs::create_directories(dataDir);
    fs::create_directories(blockedSocketDir);

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::nullopt);
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);
    yams::test::ScopedEnvVar dataEnv("YAMS_DATA_DIR", dataDir.string());
    yams::test::ScopedEnvVar storageEnv("YAMS_STORAGE", dataDir.string());
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS", std::string("1"));
    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", std::string("1"));
    yams::test::ScopedEnvVar disableWatcher("YAMS_DISABLE_SESSION_WATCHER", std::string("1"));
    yams::test::ScopedEnvVar daemonSocket("YAMS_DAEMON_SOCKET",
                                          (blockedSocketDir / "daemon.sock").string());

    std::error_code ec;
    fs::permissions(blockedSocketDir, fs::perms::none, fs::perm_options::replace, ec);

    std::string out;

    int rc = run_cli({"yams", "add", "-", "--name", "transport-smoke.txt", "--sync"}, &out,
                     std::string("transport fallback smoke\n"));
    EXPECT_EQ(rc, 0) << out;

    out.clear();
    rc = run_cli({"yams", "list", "--limit", "10"}, &out);
    EXPECT_EQ(rc, 0) << out;
    EXPECT_EQ(out.find("Using local list path"), std::string::npos) << out;

    out.clear();
    rc = run_cli({"yams", "search", "transport", "--limit", "5"}, &out);
    EXPECT_EQ(rc, 0) << out;
    EXPECT_EQ(out.find("falling back to local"), std::string::npos) << out;

    out.clear();
    rc = run_cli({"yams", "grep", "transport", "--max-matches", "10"}, &out);
    EXPECT_EQ(rc, 0) << out;
    EXPECT_EQ(out.find("falling back to local"), std::string::npos) << out;

    out.clear();
    rc = run_cli({"yams", "get", "--name", "transport-smoke.txt", "--raw"}, &out);
    EXPECT_EQ(rc, 0) << out;
    EXPECT_NE(out.find("transport fallback smoke"), std::string::npos) << out;

    fs::permissions(blockedSocketDir, fs::perms::owner_all, fs::perm_options::replace, ec);
}
