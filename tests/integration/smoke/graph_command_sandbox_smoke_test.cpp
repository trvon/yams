#include <gtest/gtest.h>

#include <filesystem>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "common/test_helpers.h"

#include <yams/cli/yams_cli.h>

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
    yams::cli::YamsCLI cli;
    std::vector<char*> argv;
    argv.reserve(args.size());
    for (const auto& arg : args) {
        argv.push_back(const_cast<char*>(arg.c_str()));
    }

    CaptureStdout capture;

    std::istringstream in;
    std::streambuf* oldIn = nullptr;
    if (stdinData.has_value()) {
        in.str(*stdinData);
        oldIn = std::cin.rdbuf(in.rdbuf());
    }

    const int rc = cli.run(static_cast<int>(argv.size()), argv.data());

    if (oldIn) {
        std::cin.rdbuf(oldIn);
    }
    if (output) {
        *output = capture.str();
    }
    return rc;
}

} // namespace

TEST(IntegrationSmoke, GraphCommandFallsBackToInProcessWhenDaemonUnavailable) {
    const fs::path root = yams::test::make_temp_dir("yams_graph_fallback_");
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
    const int rc = run_cli({"yams", "graph", "--list-types", "--json"}, &out);

    fs::permissions(blockedSocketDir, fs::perms::owner_all, fs::perm_options::replace, ec);

    EXPECT_EQ(rc, 0) << out;
    EXPECT_EQ(out.find("Connection failed"), std::string::npos) << out;
    EXPECT_EQ(out.find("Operation not permitted"), std::string::npos) << out;
}

TEST(IntegrationSmoke, GraphCommandRespectsForcedSocketMode) {
    const fs::path root = yams::test::make_temp_dir("yams_graph_socket_forced_");
    const fs::path dataDir = root / "data";
    const fs::path blockedSocketDir = root / "blocked-socket";
    fs::create_directories(dataDir);
    fs::create_directories(blockedSocketDir);

    yams::test::ScopedEnvVar embedded("YAMS_EMBEDDED", std::string("0"));
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
    const int rc = run_cli({"yams", "graph", "--list-types", "--json"}, &out);

    fs::permissions(blockedSocketDir, fs::perms::owner_all, fs::perm_options::replace, ec);

    EXPECT_NE(rc, 0) << out;
}
