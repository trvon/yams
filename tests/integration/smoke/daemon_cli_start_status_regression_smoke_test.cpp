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

#include <yams/compat/unistd.h>

#ifndef _WIN32
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/wait.h>
#endif

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

bool setEnvValue(const std::string& key, const std::string& value) {
    return setenv(key.c_str(), value.c_str(), 1) == 0;
}

bool unsetEnvValue(const std::string& key) {
    return unsetenv(key.c_str()) == 0;
}

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
        setEnvValue(key, value);
    }

    ~ScopedEnvVar() {
        if (hadOld_) {
            setEnvValue(key_, oldValue_);
        } else {
            unsetEnvValue(key_);
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
#ifdef _WIN32
    FILE* pipe = _popen((cmd + " 2>&1").c_str(), "r");
#else
    FILE* pipe = popen((cmd + " 2>&1").c_str(), "r");
#endif
    if (!pipe) {
        result.exitCode = -1;
        result.output = "popen failed";
        return result;
    }

    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
        result.output.append(buffer.data());
    }

    int status = 0;
#ifdef _WIN32
    status = _pclose(pipe);
#else
    status = pclose(pipe);
#endif
    if (status == -1) {
        result.exitCode = -1;
    } else {
        result.exitCode = status;
    }

    return result;
}

std::string shellQuote(const std::string& value) {
#ifdef _WIN32
    std::string quoted = "\"";
    for (char c : value) {
        if (c == '"') {
            quoted += "\\\"";
        } else {
            quoted.push_back(c);
        }
    }
    quoted += '"';
    return quoted;
#else
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
#endif
}

std::optional<fs::path> findYamsBinary() {
    if (const char* buildRoot = std::getenv("MESON_BUILD_ROOT")) {
#ifdef _WIN32
        fs::path candidate = fs::path(buildRoot) / "src/cli/yams.exe";
#else
        fs::path candidate = fs::path(buildRoot) / "src/cli/yams";
#endif
        if (fs::exists(candidate)) {
            return candidate;
        }
    }
    return std::nullopt;
}

#ifndef _WIN32
std::optional<pid_t> spawnLockHolder(const fs::path& lockFile, const fs::path& fakeSocket) {
    pid_t pid = fork();
    if (pid < 0) {
        return std::nullopt;
    }
    if (pid == 0) {
        int fd = ::open(lockFile.c_str(), O_CREAT | O_RDWR, 0644);
        if (fd < 0) {
            _exit(2);
        }
        if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
            ::close(fd);
            _exit(3);
        }
        std::ostringstream payload;
        payload << "{\"pid\":" << static_cast<long>(::getpid()) << ",\"socket\":\""
                << fakeSocket.string() << "\",\"timestamp\":0}";
        const auto text = payload.str();
        (void)::ftruncate(fd, 0);
        (void)::lseek(fd, 0, SEEK_SET);
        (void)::write(fd, text.data(), text.size());
        ::fsync(fd);
        std::this_thread::sleep_for(30s);
        ::close(fd);
        _exit(0);
    }
    return pid;
}

bool isPidAlive(pid_t pid) {
    if (pid <= 0) {
        return false;
    }
    return ::kill(pid, 0) == 0;
}

void cleanupChild(pid_t pid) {
    if (pid <= 0) {
        return;
    }
    if (isPidAlive(pid)) {
        (void)::kill(pid, SIGKILL);
    }
    int status = 0;
    (void)::waitpid(pid, &status, WNOHANG);
}
#endif

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

#ifndef _WIN32
TEST(DaemonCliStartStatusRegression, StartRecoversFromStaleDataDirLockHolder) {
    auto yamsBinary = findYamsBinary();
    if (!yamsBinary.has_value()) {
        GTEST_SKIP() << "Skipping: build yams binary not found via MESON_BUILD_ROOT";
    }

    const auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const fs::path root = fs::temp_directory_path() / ("yams_lock_recovery_" + unique);
    const fs::path dataDir = root / "data";
    const fs::path runtimeDir = root / "runtime";
    const fs::path configPath = root / "config.toml";
    const fs::path fakeSocket = runtimeDir / "missing.sock";
    const fs::path lockFile = dataDir / ".yams-lock";

    fs::create_directories(dataDir);
    fs::create_directories(runtimeDir);
    {
        std::ofstream cfg(configPath);
        cfg << "[core]\n";
        cfg << "data_dir = \"" << dataDir.string() << "\"\n";
        cfg << "[daemon]\n";
        cfg << "socket_path = \"" << (runtimeDir / "yams-daemon.sock").string() << "\"\n";
    }

    ScopedEnvVar cfgEnv("YAMS_CONFIG", configPath.string());
    ScopedEnvVar dataEnv("YAMS_DATA_DIR", dataDir.string());
    ScopedEnvVar storageEnv("YAMS_STORAGE", dataDir.string());
    ScopedEnvVar runtimeEnv("XDG_RUNTIME_DIR", runtimeDir.string());

    auto childPid = spawnLockHolder(lockFile, fakeSocket);
    ASSERT_TRUE(childPid.has_value()) << "Failed to spawn stale lock holder process";
    std::this_thread::sleep_for(150ms);
    ASSERT_TRUE(isPidAlive(*childPid));

    const std::string yams = shellQuote(yamsBinary->string());
    auto startRes = runCommandCapture(yams + " daemon start");
    EXPECT_EQ(startRes.exitCode, 0) << "daemon start failed:\n" << startRes.output;

    EXPECT_FALSE(isPidAlive(*childPid))
        << "stale lock holder PID still alive after daemon start recovery";

    auto stopRes = runCommandCapture(yams + " daemon stop --force");
    (void)stopRes;

    cleanupChild(*childPid);
    std::error_code ec;
    fs::remove_all(root, ec);
}
#endif
