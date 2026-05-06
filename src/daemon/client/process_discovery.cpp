#include <yams/daemon/client/process_discovery.h>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <optional>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#ifndef _WIN32
#include <spawn.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
extern char** environ;
#endif

namespace yams::daemon::client {
namespace {

std::optional<int> readPidFromFile(const std::filesystem::path& pidFilePath) {
    if (pidFilePath.empty()) {
        return std::nullopt;
    }

    std::ifstream input(pidFilePath);
    if (!input.is_open()) {
        return std::nullopt;
    }

    int pid = -1;
    input >> pid;
    if (!input.good() || pid <= 0) {
        return std::nullopt;
    }

    return pid;
}

#ifndef _WIN32
std::string escapeRegexLiteral(const std::string& value) {
    std::string out;
    out.reserve(value.size() * 2);
    for (char ch : value) {
        switch (ch) {
            case '.':
            case '^':
            case '$':
            case '*':
            case '+':
            case '?':
            case '(':
            case ')':
            case '[':
            case ']':
            case '{':
            case '}':
            case '|':
            case '\\':
                out.push_back('\\');
                break;
            default:
                break;
        }
        out.push_back(ch);
    }
    return out;
}

std::string runProcessCapture(const std::vector<std::string>& args) {
    std::string output;
    if (args.empty()) {
        return output;
    }

    int pipeFds[2] = {-1, -1};
    if (pipe(pipeFds) != 0) {
        return output;
    }

    posix_spawn_file_actions_t actions;
    if (posix_spawn_file_actions_init(&actions) != 0) {
        close(pipeFds[0]);
        close(pipeFds[1]);
        return output;
    }

    posix_spawn_file_actions_adddup2(&actions, pipeFds[1], STDOUT_FILENO);
    posix_spawn_file_actions_addclose(&actions, pipeFds[0]);
    posix_spawn_file_actions_addclose(&actions, pipeFds[1]);

    std::vector<char*> argv;
    argv.reserve(args.size() + 1);
    for (const auto& arg : args) {
        argv.push_back(const_cast<char*>(arg.c_str()));
    }
    argv.push_back(nullptr);

    pid_t childPid = -1;
    const int spawnResult =
        posix_spawnp(&childPid, argv[0], &actions, nullptr, argv.data(), environ);
    posix_spawn_file_actions_destroy(&actions);
    close(pipeFds[1]);
    if (spawnResult != 0) {
        close(pipeFds[0]);
        return output;
    }

    char buffer[512];
    ssize_t nread = 0;
    while ((nread = read(pipeFds[0], buffer, sizeof(buffer))) > 0) {
        output.append(buffer, static_cast<size_t>(nread));
    }

    close(pipeFds[0]);
    int status = 0;
    (void)waitpid(childPid, &status, 0);
    return output;
}

std::optional<std::string> readProcCommandLine(int pid) {
#if defined(__linux__)
    std::ifstream input("/proc/" + std::to_string(pid) + "/cmdline", std::ios::binary);
    if (!input.is_open()) {
        return std::nullopt;
    }

    std::ostringstream buffer;
    buffer << input.rdbuf();
    std::string commandLine = buffer.str();
    if (commandLine.empty()) {
        return std::nullopt;
    }

    for (char& ch : commandLine) {
        if (ch == '\0') {
            ch = ' ';
        }
    }
    while (!commandLine.empty() && commandLine.back() == ' ') {
        commandLine.pop_back();
    }

    if (commandLine.empty()) {
        return std::nullopt;
    }

    return commandLine;
#else
    (void)pid;
    return std::nullopt;
#endif
}

std::string describeProcess(int pid) {
    if (pid <= 0) {
        return {};
    }

    if (auto commandLine = readProcCommandLine(pid); commandLine && !commandLine->empty()) {
        return std::to_string(pid) + " " + *commandLine;
    }

    auto output =
        runProcessCapture({"ps", "-o", "pid=,ppid=,stat=,command=", "-p", std::to_string(pid)});
    while (!output.empty() &&
           (output.back() == '\n' || output.back() == '\r' || output.back() == ' ')) {
        output.pop_back();
    }
    return output;
}

std::optional<std::filesystem::path>
extractSocketPathFromProcessDescription(const std::string& description) {
    if (description.empty()) {
        return std::nullopt;
    }

    static const std::regex socketRegex(R"((?:^|\s)--socket(?:=|\s+)("?)([^\s"]+)\1)");
    std::smatch match;
    if (std::regex_search(description, match, socketRegex) && match.size() >= 3) {
        return std::filesystem::path(match[2].str());
    }

    return std::nullopt;
}

std::vector<int> collectDaemonPidsForPattern(const std::string& pattern) {
    std::vector<int> pids;
    std::set<int> seen;
    const std::regex daemonRegex(pattern);

#if defined(__linux__)
    std::error_code procEc;
    for (const auto& entry : std::filesystem::directory_iterator("/proc", procEc)) {
        if (procEc) {
            break;
        }

        std::error_code entryEc;
        if (!entry.is_directory(entryEc) || entryEc) {
            continue;
        }

        const std::string name = entry.path().filename().string();
        if (name.empty() || !std::all_of(name.begin(), name.end(),
                                         [](unsigned char ch) { return std::isdigit(ch) != 0; })) {
            continue;
        }

        int pid = -1;
        try {
            pid = std::stoi(name);
        } catch (...) {
            continue;
        }

        auto commandLine = readProcCommandLine(pid);
        if (!commandLine || !std::regex_search(*commandLine, daemonRegex)) {
            continue;
        }

        if (seen.insert(pid).second) {
            pids.push_back(pid);
        }
    }

    if (!pids.empty()) {
        return pids;
    }
#endif

    std::istringstream lines(runProcessCapture({"ps", "-ax", "-o", "pid=,command="}));
    std::string line;
    while (std::getline(lines, line)) {
        const auto first = line.find_first_not_of(" \t");
        if (first == std::string::npos) {
            continue;
        }

        const auto pidEnd = line.find_first_of(" \t", first);
        const std::string pidToken = line.substr(first, pidEnd - first);

        int pid = -1;
        try {
            pid = std::stoi(pidToken);
        } catch (...) {
            continue;
        }

        const std::string command =
            pidEnd == std::string::npos ? std::string{} : line.substr(pidEnd + 1);
        if (command.empty() || !std::regex_search(command, daemonRegex)) {
            continue;
        }

        if (seen.insert(pid).second) {
            pids.push_back(pid);
        }
    }

    return pids;
}
#endif

} // namespace

std::optional<std::filesystem::path>
discoverLiveDaemonSocket(const std::filesystem::path& preferredSocket,
                         const std::filesystem::path& pidFilePath, bool allowAnyDaemonFallback) {
#ifdef _WIN32
    (void)preferredSocket;
    (void)pidFilePath;
    (void)allowAnyDaemonFallback;
    return std::nullopt;
#else
    std::vector<int> candidatePids;
    std::set<int> seen;

    if (auto pidFromFile = readPidFromFile(pidFilePath);
        pidFromFile && kill(*pidFromFile, 0) ==
                           0) { // nosemgrep: yams.cpp.kill-zero-one-shot -- snapshot candidate
                                // filter; callers perform bounded connection attempts.
        candidatePids.push_back(*pidFromFile);
        seen.insert(*pidFromFile);
    }

    if (!preferredSocket.empty()) {
        for (auto pid : collectDaemonPidsForPattern(std::string("yams-daemon.*") +
                                                    escapeRegexLiteral(preferredSocket.string()))) {
            if (seen.insert(pid).second) {
                candidatePids.push_back(pid);
            }
        }
    }

    if (allowAnyDaemonFallback) {
        for (auto pid : collectDaemonPidsForPattern("yams-daemon")) {
            if (seen.insert(pid).second) {
                candidatePids.push_back(pid);
            }
        }
    }

    std::optional<std::filesystem::path> fallbackSocket;
    for (auto pid : candidatePids) {
        const auto desc = describeProcess(pid);
        if (auto parsed = extractSocketPathFromProcessDescription(desc);
            parsed && !parsed->empty()) {
            if (*parsed == preferredSocket) {
                return *parsed;
            }
            if (!fallbackSocket) {
                fallbackSocket = *parsed;
            }
        }
    }

    return fallbackSocket;
#endif
}

} // namespace yams::daemon::client
