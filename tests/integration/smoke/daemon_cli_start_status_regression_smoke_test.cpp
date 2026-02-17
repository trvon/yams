#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <sys/wait.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

struct CommandResult {
    int exitCode = -1;
    std::string output;
};

class ScopedEnvVar {
public:
    ScopedEnvVar(const std::string& key, const std::string& value) : key_(key) {
        if (const char* cur = std::getenv(key.c_str())) {
            hadOld_ = true;
            oldValue_ = cur;
        }
        setenv(key.c_str(), value.c_str(), 1);
    }

    ~ScopedEnvVar() {
        if (hadOld_) {
            setenv(key_.c_str(), oldValue_.c_str(), 1);
        } else {
            unsetenv(key_.c_str());
        }
    }

    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;

private:
    std::string key_;
    bool hadOld_ = false;
    std::string oldValue_;
};

CommandResult runCommandCapture(const std::string& cmd) {
    CommandResult result;
    std::array<char, 4096> buffer{};
    FILE* pipe = popen((cmd + " 2>&1").c_str(), "r");
    if (!pipe) {
        result.exitCode = -1;
        result.output = "popen failed";
        return result;
    }

    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
        result.output.append(buffer.data());
    }

    int status = pclose(pipe);
    if (status == -1) {
        result.exitCode = -1;
    } else {
        result.exitCode = WEXITSTATUS(status);
    }

    return result;
}

std::string shellQuote(const std::string& value) {
    std::string quoted = "'";
    for (char c : value) {
        if (c == '\'') {
            quoted += "'\\''";
        } else {
            quoted.push_back(c);
        }
    }
    quoted += "'";
    return quoted;
}

std::optional<fs::path> findYamsBinary() {
    if (const char* buildRoot = std::getenv("MESON_BUILD_ROOT")) {
        fs::path candidate = fs::path(buildRoot) / "src/cli/yams";
        if (fs::exists(candidate)) {
            return candidate;
        }
    }
    return std::nullopt;
}

} // namespace

TEST(DaemonCliStartStatusRegression, StatusWorksAfterListBecomesResponsive) {
    auto yamsBinary = findYamsBinary();
    if (!yamsBinary.has_value()) {
        GTEST_SKIP() << "Skipping: build yams binary not found via MESON_BUILD_ROOT";
    }

    const auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const fs::path root = fs::temp_directory_path() / ("yams_cli_status_regression_" + unique);
    const fs::path dataDir = root / "data";
    const fs::path runtimeDir = root / "runtime";
    const fs::path configPath = root / "config.toml";

    fs::create_directories(dataDir);
    fs::create_directories(runtimeDir);
    {
        std::ofstream cfg(configPath);
        cfg << "[storage]\n";
        cfg << "path = \"" << dataDir.string() << "\"\n";
    }

    ScopedEnvVar cfgEnv("YAMS_CONFIG", configPath.string());
    ScopedEnvVar dataEnv("YAMS_DATA_DIR", dataDir.string());
    ScopedEnvVar runtimeEnv("XDG_RUNTIME_DIR", runtimeDir.string());
    ScopedEnvVar noAutoStart("YAMS_CLI_DISABLE_DAEMON_AUTOSTART", "0");

    const std::string yams = shellQuote(yamsBinary->string());

    auto stopRes = runCommandCapture(yams + " daemon stop --force");
    (void)stopRes;

    auto startRes = runCommandCapture(yams + " daemon start");
    ASSERT_EQ(startRes.exitCode, 0) << "daemon start failed:\n" << startRes.output;

    bool listSucceeded = false;
    CommandResult lastList;
    for (int attempt = 0; attempt < 30; ++attempt) {
        lastList = runCommandCapture(yams + " list --limit 5");
        if (lastList.exitCode == 0) {
            listSucceeded = true;
            break;
        }
        std::this_thread::sleep_for(250ms);
    }
    ASSERT_TRUE(listSucceeded) << "list never became responsive after daemon start. Last output:\n"
                               << lastList.output;

    auto statusRes = runCommandCapture(yams + " daemon status -d");
    EXPECT_EQ(statusRes.exitCode, 0) << "daemon status -d failed after list succeeded:\n"
                                     << statusRes.output;
    EXPECT_EQ(statusRes.output.find("YAMS daemon status unavailable (IPC error)"),
              std::string::npos)
        << "status command reported IPC unavailable after daemon was already serving list:\n"
        << statusRes.output;

    auto finalStop = runCommandCapture(yams + " daemon stop --force");
    (void)finalStop;

    std::error_code ec;
    fs::remove_all(root, ec);
}