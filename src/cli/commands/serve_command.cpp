#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <system_error>
#include <vector>

#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>

#ifdef _WIN32
#if defined(__has_include)
#if __has_include(<process.h>)
#include <process.h>
#endif
#else
#include <process.h>
#endif
#include <windows.h>
#else
#include <cerrno>
#include <cstring>
#include <unistd.h>
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif
#endif

namespace yams::cli {
namespace {

std::filesystem::path currentExecutablePath() {
#ifdef _WIN32
    std::wstring buffer(MAX_PATH, L'\0');
    DWORD len = 0;
    while (true) {
        len = GetModuleFileNameW(nullptr, buffer.data(), static_cast<DWORD>(buffer.size()));
        if (len == 0) {
            return {};
        }
        if (len < buffer.size() - 1) {
            buffer.resize(len);
            return std::filesystem::path(buffer);
        }
        buffer.resize(buffer.size() * 2);
    }
#elif defined(__APPLE__)
    uint32_t size = 0;
    _NSGetExecutablePath(nullptr, &size);
    std::string buffer(size, '\0');
    if (_NSGetExecutablePath(buffer.data(), &size) != 0) {
        return {};
    }
    buffer.resize(std::strlen(buffer.c_str()));
    std::error_code ec;
    return std::filesystem::weakly_canonical(buffer, ec);
#elif defined(__linux__)
    std::error_code ec;
    return std::filesystem::read_symlink("/proc/self/exe", ec);
#else
    return {};
#endif
}

std::filesystem::path resolveMcpServerPath() {
    const auto self = currentExecutablePath();
    if (!self.empty()) {
#ifdef _WIN32
        const auto sibling = self.parent_path() / "yams-mcp-server.exe";
#else
        const auto sibling = self.parent_path() / "yams-mcp-server";
#endif
        std::error_code ec;
        if (std::filesystem::exists(sibling, ec)) {
            return sibling;
        }

#ifdef _WIN32
        const auto buildTreeSibling =
            self.parent_path().parent_path() / "yams-mcp" / "yams-mcp-server.exe";
#else
        const auto buildTreeSibling =
            self.parent_path().parent_path() / "yams-mcp" / "yams-mcp-server";
#endif
        if (std::filesystem::exists(buildTreeSibling, ec)) {
            return buildTreeSibling;
        }
    }

#ifdef _WIN32
    return "yams-mcp-server.exe";
#else
    return "yams-mcp-server";
#endif
}

std::string effectiveLogLevel(const bool verbose) {
    return verbose ? "info" : "warn";
}

#ifndef _WIN32
[[noreturn]] void execMcpServer(const std::filesystem::path& serverPath,
                                const std::vector<std::string>& args) {
    std::vector<char*> argv;
    argv.reserve(args.size() + 1);
    for (const auto& arg : args) {
        argv.push_back(const_cast<char*>(arg.c_str()));
    }
    argv.push_back(nullptr);

    execv(serverPath.c_str(), argv.data());
    execvp(args.front().c_str(), argv.data());
    const int err = errno;
    std::cerr << "Failed to exec " << serverPath << ": "
              << std::error_code(err, std::generic_category()).message() << '\n';
    std::_Exit(127);
}
#endif

} // namespace

class ServeCommand : public ICommand {
public:
    std::string getName() const override { return "serve"; }

    std::string getDescription() const override { return "Start MCP server"; }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("serve", getDescription());
        cmd->add_option("--daemon-socket", daemonSocket_, "Override daemon socket path")
            ->envname("YAMS_DAEMON_SOCKET");
        cmd->add_flag(
            "--quiet", quietFlag_,
            "Deprecated / no-op: server runs quiet by default (use --verbose to show banner)");
        cmd->add_flag(
            "--verbose", verbose_,
            "Show startup banner and enable info-level logging (overrides default quiet mode)");

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                std::cerr << "Command failed: " << result.error().message << '\n';
                throw CLI::RuntimeError(1);
            }
        });
    }

    Result<void> execute() override {
        (void)cli_;
        const auto serverPath = resolveMcpServerPath();
        std::vector<std::string> args;
#ifdef _WIN32
        args.push_back(serverPath.string());
#else
        args.push_back(serverPath.filename().empty() ? std::string("yams-mcp-server")
                                                     : serverPath.filename().string());
#endif
        args.emplace_back("--log-level");
        args.push_back(effectiveLogLevel(verbose_));
        if (!daemonSocket_.empty()) {
            args.emplace_back("--daemon-socket");
            args.push_back(daemonSocket_);
        }

        if (verbose_) {
            std::cerr << "Delegating to " << serverPath << '\n';
        }

#ifdef _WIN32
        std::vector<const char*> argv;
        argv.reserve(args.size() + 1);
        for (const auto& arg : args) {
            argv.push_back(arg.c_str());
        }
        argv.push_back(nullptr);
        const int rc = _spawnv(_P_WAIT, serverPath.string().c_str(), argv.data());
        if (rc != 0) {
            return Error{ErrorCode::Unknown,
                         "yams-mcp-server exited with code " + std::to_string(rc)};
        }
        return {};
#else
        execMcpServer(serverPath, args);
#endif
    }

private:
    YamsCLI* cli_ = nullptr;
    bool quietFlag_ = true;
    bool verbose_ = false;
    std::string daemonSocket_;
};

std::unique_ptr<ICommand> createServeCommand() {
    return std::make_unique<ServeCommand>();
}

} // namespace yams::cli
