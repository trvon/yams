#pragma once

// Isolates every path a daemon or the CLI could resolve to a shared location: HOME, XDG_*, and
// the YAMS_* path variables all point into (or are cleared under) one temp directory. Tests
// that resolve or launch daemons use it so nothing touches the user's daemon, store or log.

#include <chrono>
#include <filesystem>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "test_helpers_catch2.h"

#if defined(__linux__)
#include <csignal>
#include <unistd.h>
#include <sys/wait.h>
#endif

namespace yams::test {

class IsolatedDaemonEnv {
public:
    explicit IsolatedDaemonEnv(std::string_view prefix = "yams_iso_")
        : root_(make_temp_dir(prefix)) {
        namespace fs = std::filesystem;
        for (const char* dir : {"home", "config", "data", "state", "runtime", "cache"}) {
            fs::create_directories(root_ / dir);
        }
        set("HOME", root_ / "home");
        set("XDG_CONFIG_HOME", root_ / "config");
        set("XDG_DATA_HOME", root_ / "data");
        set("XDG_STATE_HOME", root_ / "state");
        set("XDG_RUNTIME_DIR", root_ / "runtime");
        set("XDG_CACHE_HOME", root_ / "cache");
        for (const char* key : {"YAMS_CONFIG", "YAMS_CONFIG_PATH", "YAMS_DATA_DIR", "YAMS_STORAGE",
                                "YAMS_DAEMON_SOCKET", "YAMS_DAEMON_SOCKET_PATH", "YAMS_DAEMON_BIN",
                                "YAMS_SOCKET_PATH", "YAMS_PID_FILE", "YAMS_RUNTIME_DIR"}) {
            vars_.emplace_back(std::string(key), std::nullopt);
        }
        vars_.emplace_back(std::string("YAMS_NON_INTERACTIVE"), std::string("1"));
        vars_.emplace_back(std::string("YAMS_CLI_DISABLE_DAEMON_AUTOSTART"), std::string("1"));
    }

    ~IsolatedDaemonEnv() {
        while (!vars_.empty()) {
            vars_.pop_back();
        }
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }

    IsolatedDaemonEnv(const IsolatedDaemonEnv&) = delete;
    IsolatedDaemonEnv& operator=(const IsolatedDaemonEnv&) = delete;

    const std::filesystem::path& root() const { return root_; }

    /// The user's default config file ($XDG_CONFIG_HOME/yams/config.toml).
    std::filesystem::path userConfigPath() const {
        return root_ / "config" / "yams" / "config.toml";
    }

    /// Writes the user's default config with a shared daemon socket and PID file, like a
    /// config that sets socket_path = "/tmp/yams-daemon.sock".
    void writeUserConfig(const std::filesystem::path& socket, const std::filesystem::path& pid) {
        writeConfig(userConfigPath(), socket, pid);
    }

    static void writeConfig(const std::filesystem::path& file, const std::filesystem::path& socket,
                            const std::filesystem::path& pid) {
        std::filesystem::create_directories(file.parent_path());
        std::ofstream cfg(file);
        cfg << "[version]\nconfig_version = 3\n[daemon]\n";
        cfg << "socket_path = \"" << socket.string() << "\"\n";
        if (!pid.empty()) {
            cfg << "pid_file = \"" << pid.string() << "\"\n";
        }
    }

    void set(const std::string& key, const std::filesystem::path& value) {
        vars_.emplace_back(key, value.string());
    }

private:
    std::filesystem::path root_;
    std::vector<ScopedEnvVar> vars_;
};

#if defined(__linux__)
/// A child process that discovery and PID-file identity checks take for a live yams-daemon on
/// `socket` (a copy of cat named yams-daemon, blocked on a pipe), without listening anywhere.
/// It stands in for "the user's live daemon"; the test owns it and kills it on destruction.
class FakeLiveDaemon {
public:
    FakeLiveDaemon(const std::filesystem::path& dir, const std::filesystem::path& socket) {
        namespace fs = std::filesystem;
        fs::create_directories(dir);
        exe_ = dir / "yams-daemon";
        fs::copy_file("/bin/cat", exe_, fs::copy_options::overwrite_existing);
        fs::permissions(exe_, fs::perms::owner_all, fs::perm_options::add);
        int fds[2];
        if (::pipe(fds) != 0) {
            throw std::runtime_error("pipe failed");
        }
        const std::string socketArg = socket.string();
        pid_ = ::fork();
        if (pid_ < 0) {
            throw std::runtime_error("fork failed");
        }
        if (pid_ == 0) {
            ::dup2(fds[0], STDIN_FILENO);
            ::close(fds[0]);
            ::close(fds[1]);
            ::execl(exe_.c_str(), "yams-daemon", "--", "-", "--socket", socketArg.c_str(),
                    static_cast<char*>(nullptr));
            ::_exit(127);
        }
        ::close(fds[0]);
        writeEnd_ = fds[1];
        // Wait until the child runs the fake executable (exec done).
        for (int i = 0; i < 300; ++i) {
            std::error_code ec;
            const auto live = fs::read_symlink("/proc/" + std::to_string(pid_) + "/exe", ec);
            if (!ec && live.filename() == "yams-daemon") {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        throw std::runtime_error("fake daemon did not start");
    }

    ~FakeLiveDaemon() {
        if (pid_ > 0 && !reaped_) {
            (void)::kill(pid_, SIGKILL);
            int status = 0;
            (void)::waitpid(pid_, &status, 0);
        }
        if (writeEnd_ >= 0) {
            ::close(writeEnd_);
        }
    }

    FakeLiveDaemon(const FakeLiveDaemon&) = delete;
    FakeLiveDaemon& operator=(const FakeLiveDaemon&) = delete;

    pid_t pid() const { return pid_; }
    const std::filesystem::path& exe() const { return exe_; }

    /// False once the process exited or was killed (reaps it).
    bool alive() {
        if (reaped_) {
            return false;
        }
        int status = 0;
        if (::waitpid(pid_, &status, WNOHANG) == pid_) {
            reaped_ = true;
            return false;
        }
        return ::kill(pid_, 0) == 0;
    }

private:
    std::filesystem::path exe_;
    pid_t pid_{-1};
    int writeEnd_{-1};
    bool reaped_{false};
};
#endif

} // namespace yams::test
