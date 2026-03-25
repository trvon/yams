#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

#ifndef _WIN32
#include <cerrno>
#include <csignal>
#include <fcntl.h>
#include <spawn.h>
#include <unistd.h>
#include <sys/wait.h>

extern char** environ;
#endif

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

struct ScopedPathCleanup {
    fs::path path;

    ~ScopedPathCleanup() {
        if (path.empty()) {
            return;
        }

        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

std::optional<fs::path> findYamsBinary() {
    std::vector<fs::path> candidates;
    auto addRelativeCandidates = [&candidates](const fs::path& base) {
#ifdef _WIN32
        candidates.emplace_back(base / "tools/yams-cli/yams-cli.exe");
        candidates.emplace_back(base / "src/cli/yams.exe");
#else
        candidates.emplace_back(base / "tools/yams-cli/yams-cli");
        candidates.emplace_back(base / "src/cli/yams");
#endif
    };

    if (const char* buildRoot = std::getenv("MESON_BUILD_ROOT")) {
        addRelativeCandidates(fs::path(buildRoot));
    }

#ifdef __APPLE__
    uint32_t execPathSize = 0;
    (void)_NSGetExecutablePath(nullptr, &execPathSize);
    if (execPathSize > 0) {
        std::string execPath(execPathSize, '\0');
        if (_NSGetExecutablePath(execPath.data(), &execPathSize) == 0) {
            fs::path resolved = fs::weakly_canonical(fs::path(execPath.c_str()));
            if (!resolved.empty()) {
                fs::path cur = resolved.parent_path();
                for (int depth = 0; depth < 6 && !cur.empty(); ++depth) {
                    addRelativeCandidates(cur);
                    cur = cur.parent_path();
                }
            }
        }
    }
#endif

    fs::path cur = fs::current_path();
    for (int depth = 0; depth < 6 && !cur.empty(); ++depth) {
        addRelativeCandidates(cur);
        cur = cur.parent_path();
    }

#ifdef _WIN32
    candidates.emplace_back("build/release/tools/yams-cli/yams-cli.exe");
    candidates.emplace_back("builddir/tools/yams-cli/yams-cli.exe");
#else
    candidates.emplace_back("build/release/tools/yams-cli/yams-cli");
    candidates.emplace_back("builddir/tools/yams-cli/yams-cli");
#endif

    for (const auto& candidate : candidates) {
        std::error_code ec;
        if (fs::exists(candidate, ec) && fs::is_regular_file(candidate, ec)) {
            return fs::absolute(candidate);
        }
    }

    return std::nullopt;
}

std::optional<fs::path> findYamsDaemonBinary() {
    std::vector<fs::path> candidates;
    auto addRelativeCandidates = [&candidates](const fs::path& base) {
#ifdef _WIN32
        candidates.emplace_back(base / "src/daemon/yams-daemon.exe");
#else
        candidates.emplace_back(base / "src/daemon/yams-daemon");
#endif
    };

    if (const char* buildRoot = std::getenv("MESON_BUILD_ROOT")) {
        addRelativeCandidates(fs::path(buildRoot));
    }

#ifdef __APPLE__
    uint32_t execPathSize = 0;
    (void)_NSGetExecutablePath(nullptr, &execPathSize);
    if (execPathSize > 0) {
        std::string execPath(execPathSize, '\0');
        if (_NSGetExecutablePath(execPath.data(), &execPathSize) == 0) {
            fs::path resolved = fs::weakly_canonical(fs::path(execPath.c_str()));
            if (!resolved.empty()) {
                fs::path cur = resolved.parent_path();
                for (int depth = 0; depth < 6 && !cur.empty(); ++depth) {
                    addRelativeCandidates(cur);
                    cur = cur.parent_path();
                }
            }
        }
    }
#endif

    fs::path cur = fs::current_path();
    for (int depth = 0; depth < 6 && !cur.empty(); ++depth) {
        addRelativeCandidates(cur);
        cur = cur.parent_path();
    }

#ifdef _WIN32
    candidates.emplace_back("build/release/src/daemon/yams-daemon.exe");
    candidates.emplace_back("builddir/src/daemon/yams-daemon.exe");
#else
    candidates.emplace_back("build/release/src/daemon/yams-daemon");
    candidates.emplace_back("builddir/src/daemon/yams-daemon");
#endif

    for (const auto& candidate : candidates) {
        std::error_code ec;
        if (fs::exists(candidate, ec) && fs::is_regular_file(candidate, ec)) {
            return fs::absolute(candidate);
        }
    }

    return std::nullopt;
}

#ifndef _WIN32

struct SubprocessResult {
    int exitCode = -1;
    int termSignal = 0;
    bool signaled = false;
    bool timedOut = false;
    std::string output;
};

struct BackgroundProcess {
    pid_t pid = -1;
};

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

void cleanupBackgroundProcess(const BackgroundProcess& process) {
    if (process.pid <= 0) {
        return;
    }
    (void)::kill(process.pid, SIGTERM);
    int status = 0;
    for (int attempt = 0; attempt < 100; ++attempt) {
        pid_t waited = ::waitpid(process.pid, &status, WNOHANG);
        if (waited == process.pid) {
            return;
        }
        if (waited < 0 && errno != EINTR) {
            return;
        }
        std::this_thread::sleep_for(50ms);
    }
    (void)::kill(process.pid, SIGKILL);
    while (::waitpid(process.pid, &status, 0) < 0 && errno == EINTR) {
    }
}

std::string readTextFile(const fs::path& path) {
    std::ifstream in(path);
    if (!in) {
        return {};
    }
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

void drainPipe(int fd, std::string& output) {
    if (fd < 0) {
        return;
    }

    std::array<char, 4096> buffer{};
    for (;;) {
        const ssize_t n = ::read(fd, buffer.data(), buffer.size());
        if (n > 0) {
            output.append(buffer.data(), static_cast<size_t>(n));
            continue;
        }
        if (n == 0) {
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }
        break;
    }
}

SubprocessResult runSubprocess(const fs::path& binary, const std::vector<std::string>& args,
                               const std::map<std::string, std::string>& env,
                               std::chrono::milliseconds timeout) {
    SubprocessResult result;

    std::vector<std::string> ownedArgs;
    ownedArgs.reserve(args.size() + 1);
    ownedArgs.push_back(binary.string());
    ownedArgs.insert(ownedArgs.end(), args.begin(), args.end());

    std::vector<char*> argv;
    argv.reserve(ownedArgs.size() + 1);
    for (auto& arg : ownedArgs) {
        argv.push_back(arg.data());
    }
    argv.push_back(nullptr);

    std::map<std::string, std::string> mergedEnv;
    for (char** it = environ; it != nullptr && *it != nullptr; ++it) {
        std::string entry(*it);
        const auto eq = entry.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        mergedEnv.emplace(entry.substr(0, eq), entry.substr(eq + 1));
    }
    for (const auto& [key, value] : env) {
        mergedEnv[key] = value;
    }

    std::vector<std::string> ownedEnv;
    ownedEnv.reserve(mergedEnv.size());
    for (const auto& [key, value] : mergedEnv) {
        ownedEnv.push_back(key + "=" + value);
    }

    std::vector<char*> envp;
    envp.reserve(ownedEnv.size() + 1);
    for (auto& entry : ownedEnv) {
        envp.push_back(entry.data());
    }
    envp.push_back(nullptr);

    int outputPipe[2] = {-1, -1};
    if (::pipe(outputPipe) != 0) {
        result.output = "pipe() failed";
        return result;
    }

    (void)::fcntl(outputPipe[0], F_SETFD, FD_CLOEXEC);
    (void)::fcntl(outputPipe[1], F_SETFD, FD_CLOEXEC);

    const int flags = ::fcntl(outputPipe[0], F_GETFL, 0);
    if (flags >= 0) {
        (void)::fcntl(outputPipe[0], F_SETFL, flags | O_NONBLOCK);
    }

    posix_spawn_file_actions_t fileActions;
    ::posix_spawn_file_actions_init(&fileActions);
    ::posix_spawn_file_actions_adddup2(&fileActions, outputPipe[1], STDOUT_FILENO);
    ::posix_spawn_file_actions_adddup2(&fileActions, outputPipe[1], STDERR_FILENO);
    ::posix_spawn_file_actions_addclose(&fileActions, outputPipe[0]);
    ::posix_spawn_file_actions_addclose(&fileActions, outputPipe[1]);

    posix_spawnattr_t attr;
    ::posix_spawnattr_init(&attr);
#ifdef POSIX_SPAWN_CLOEXEC_DEFAULT
    short spawnFlags = POSIX_SPAWN_CLOEXEC_DEFAULT;
    ::posix_spawnattr_setflags(&attr, spawnFlags);
#endif

    const auto start = std::chrono::steady_clock::now();
    pid_t pid = -1;
    const int spawnErr =
        ::posix_spawn(&pid, binary.c_str(), &fileActions, &attr, argv.data(), envp.data());
    ::posix_spawn_file_actions_destroy(&fileActions);
    ::posix_spawnattr_destroy(&attr);

    if (spawnErr != 0) {
        ::close(outputPipe[0]);
        ::close(outputPipe[1]);
        result.output = std::string("posix_spawn() failed: ") + std::strerror(spawnErr);
        return result;
    }

    ::close(outputPipe[1]);

    int status = 0;
    const auto deadline = start + timeout;
    for (;;) {
        drainPipe(outputPipe[0], result.output);

        pid_t waited = ::waitpid(pid, &status, WNOHANG);
        if (waited == pid) {
            break;
        }
        if (waited < 0) {
            if (errno == EINTR) {
                continue;
            }
            result.output += "\nwaitpid() failed";
            break;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            result.timedOut = true;
            (void)::kill(pid, SIGKILL);
            while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {
            }
            break;
        }
        std::this_thread::sleep_for(20ms);
    }

    for (;;) {
        drainPipe(outputPipe[0], result.output);
        std::array<char, 1> peek{};
        const ssize_t n = ::read(outputPipe[0], peek.data(), peek.size());
        if (n > 0) {
            result.output.push_back(peek[0]);
            continue;
        }
        if (n == 0) {
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }
        break;
    }
    ::close(outputPipe[0]);

    if (result.timedOut) {
        result.exitCode = 124;
        return result;
    }

    if (WIFEXITED(status)) {
        result.exitCode = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        result.signaled = true;
        result.termSignal = WTERMSIG(status);
    }

    return result;
}

std::optional<BackgroundProcess>
startBackgroundProcess(const fs::path& binary, const std::vector<std::string>& args,
                       const std::map<std::string, std::string>& env,
                       std::string* error = nullptr) {
    std::ostringstream shell;
    for (const auto& [key, value] : env) {
        shell << key << "=" << shellQuote(value) << " ";
    }
    shell << "exec " << shellQuote(binary.string());
    for (const auto& arg : args) {
        shell << " " << shellQuote(arg);
    }
    shell << " >/dev/null 2>&1";

    std::vector<std::string> ownedArgs = {"/bin/sh", "-c", shell.str()};
    std::vector<char*> argv;
    argv.reserve(ownedArgs.size() + 1);
    for (auto& arg : ownedArgs) {
        argv.push_back(arg.data());
    }
    argv.push_back(nullptr);

    posix_spawn_file_actions_t fileActions;
    ::posix_spawn_file_actions_init(&fileActions);
    ::posix_spawn_file_actions_addopen(&fileActions, STDIN_FILENO, "/dev/null", O_RDONLY, 0);

    pid_t pid = -1;
    const int spawnErr =
        ::posix_spawn(&pid, "/bin/sh", &fileActions, nullptr, argv.data(), environ);
    ::posix_spawn_file_actions_destroy(&fileActions);

    if (spawnErr != 0) {
        if (error != nullptr) {
            *error = std::string("posix_spawn(/bin/sh) failed: ") + std::strerror(spawnErr);
        }
        return std::nullopt;
    }

    return BackgroundProcess{pid};
}

std::string describeFailure(const std::vector<std::string>& args, const SubprocessResult& result) {
    std::string command;
    for (const auto& arg : args) {
        if (!command.empty()) {
            command += ' ';
        }
        command += arg;
    }

    std::string summary = "command: yams-cli ";
    summary += command;
    if (result.timedOut) {
        summary += "\nresult: timed out";
    } else if (result.signaled) {
        summary += "\nresult: signal ";
        summary += std::to_string(result.termSignal);
    } else {
        summary += "\nresult: exit ";
        summary += std::to_string(result.exitCode);
    }
    if (!result.output.empty()) {
        summary += "\noutput:\n";
        summary += result.output;
    }
    return summary;
}

bool waitForCommandSuccess(const fs::path& binary, const std::vector<std::string>& args,
                           const std::map<std::string, std::string>& env,
                           std::chrono::milliseconds overallTimeout,
                           SubprocessResult* lastResult = nullptr) {
    const auto deadline = std::chrono::steady_clock::now() + overallTimeout;
    SubprocessResult current;
    while (std::chrono::steady_clock::now() < deadline) {
        current = runSubprocess(binary, args, env, 10s);
        if (!current.timedOut && !current.signaled && current.exitCode == 0) {
            if (lastResult != nullptr) {
                *lastResult = std::move(current);
            }
            return true;
        }
        std::this_thread::sleep_for(250ms);
    }

    if (lastResult != nullptr) {
        *lastResult = std::move(current);
    }
    return false;
}

#endif

} // namespace

#ifndef _WIN32
TEST(CliSubprocessSegfaultRegressionSmoke, ShortLivedCommandsExitCleanly) {
    auto yamsBinary = findYamsBinary();
    if (!yamsBinary.has_value()) {
        GTEST_SKIP() << "Skipping: built yams-cli binary not found";
    }
    auto daemonBinary = findYamsDaemonBinary();
    if (!daemonBinary.has_value()) {
        GTEST_SKIP() << "Skipping: built yams-daemon binary not found";
    }
    const auto unique =
        std::to_string(static_cast<unsigned long long>(::getpid())) + "_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() % 1000000000ULL);
    const fs::path root = fs::temp_directory_path() / ("yams_cli_sigsegv_smoke_" + unique);
    ScopedPathCleanup rootCleanup{root};
    const fs::path dataDir = root / "data";
    const fs::path runtimeDir = root / "runtime";
    const fs::path configPath = root / "config.toml";
    const fs::path socketPath = fs::path("/tmp") / ("yams_cli_" + unique + ".sock");
    const fs::path pidFile = root / "yams-daemon.pid";
    const fs::path daemonLogPath = root / "yams-daemon.log";
    const fs::path docPath = root / "turboquant_cli_smoke.txt";

    fs::create_directories(dataDir);
    fs::create_directories(runtimeDir);

    {
        std::ofstream cfg(configPath);
        cfg << "[core]\n";
        cfg << "data_dir = \"" << dataDir.string() << "\"\n";
        cfg << "\n[daemon]\n";
        cfg << "data_dir = \"" << dataDir.string() << "\"\n";
        cfg << "socket_path = \"" << socketPath.string() << "\"\n";
        cfg << "pid_file = \"" << pidFile.string() << "\"\n";
        cfg << "log_file = \"" << daemonLogPath.string() << "\"\n";
        cfg << "log_level = \"info\"\n";
    }

    {
        std::ofstream doc(docPath);
        doc << "turboquant hnsw teardown regression smoke token\n";
        doc << "short lived cli subprocess search list daemon status\n";
    }

    std::map<std::string, std::string> baseEnv = {
        {"YAMS_CONFIG", configPath.string()},
        {"YAMS_DATA_DIR", dataDir.string()},
        {"YAMS_STORAGE", dataDir.string()},
        {"YAMS_DAEMON_SOCKET_PATH", socketPath.string()},
        {"YAMS_DAEMON_SOCKET", socketPath.string()},
        {"YAMS_DISABLE_VECTORS", "1"},
        {"YAMS_NON_INTERACTIVE", "1"},
        {"XDG_RUNTIME_DIR", runtimeDir.string()},
        {"YAMS_TEST_SAFE_SINGLE_INSTANCE", "1"},
    };

    std::string daemonStartError;
    auto daemonProc = startBackgroundProcess(
        *daemonBinary,
        {"--foreground", "--config", configPath.string(), "--socket", socketPath.string(),
         "--data-dir", dataDir.string(), "--pid-file", pidFile.string(), "--log-file",
         daemonLogPath.string(), "--log-level", "info", "--no-plugins"},
        baseEnv, &daemonStartError);
    ASSERT_TRUE(daemonProc.has_value()) << daemonStartError;
    BackgroundProcess daemon = std::move(*daemonProc);

    std::map<std::string, std::string> probeEnv = baseEnv;
    probeEnv["YAMS_CLI_DISABLE_DAEMON_AUTOSTART"] = "1";

    SubprocessResult warmStatus;
    ASSERT_TRUE(
        waitForCommandSuccess(*yamsBinary, {"daemon", "status", "-d"}, probeEnv, 30s, &warmStatus))
        << describeFailure({"daemon", "status", "-d"}, warmStatus) << "\ndaemon log:\n"
        << readTextFile(daemonLogPath);

    SubprocessResult warmList;
    ASSERT_TRUE(
        waitForCommandSuccess(*yamsBinary, {"list", "--limit", "5"}, probeEnv, 15s, &warmList))
        << describeFailure({"list", "--limit", "5"}, warmList) << "\ndaemon log:\n"
        << readTextFile(daemonLogPath);

    SubprocessResult addResult;
    ASSERT_TRUE(
        waitForCommandSuccess(*yamsBinary, {"add", docPath.string()}, baseEnv, 20s, &addResult))
        << describeFailure({"add", docPath.string()}, addResult) << "\ndaemon log:\n"
        << readTextFile(daemonLogPath);

    SubprocessResult warmSearch;
    ASSERT_TRUE(waitForCommandSuccess(*yamsBinary, {"search", "turboquant hnsw", "--limit", "5"},
                                      probeEnv, 20s, &warmSearch))
        << describeFailure({"search", "turboquant hnsw", "--limit", "5"}, warmSearch)
        << "\ndaemon log:\n"
        << readTextFile(daemonLogPath);

    const std::vector<std::vector<std::string>> commands = {
        {"list", "--limit", "5"},
        {"search", "turboquant hnsw", "--limit", "5"},
        {"daemon", "status", "-d"},
    };

    for (int iteration = 0; iteration < 20; ++iteration) {
        for (const auto& command : commands) {
            auto result = runSubprocess(*yamsBinary, command, probeEnv, 10s);
            EXPECT_FALSE(result.timedOut) << "iteration " << iteration << "\n"
                                          << describeFailure(command, result);
            EXPECT_FALSE(result.signaled) << "iteration " << iteration << "\n"
                                          << describeFailure(command, result);
            EXPECT_EQ(result.exitCode, 0) << "iteration " << iteration << "\n"
                                          << describeFailure(command, result);
            if (result.timedOut || result.signaled || result.exitCode != 0) {
                break;
            }
        }
    }

    cleanupBackgroundProcess(daemon);
    daemon.pid = -1;
}
#else
TEST(CliSubprocessSegfaultRegressionSmoke, ShortLivedCommandsExitCleanly) {
    GTEST_SKIP() << "Unix-only subprocess signal smoke test";
}
#endif
