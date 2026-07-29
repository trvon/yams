#include <yams/daemon/client/process_discovery.h>

#include <nlohmann/json.hpp>
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#else
#include <signal.h>
#include <spawn.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#ifdef __APPLE__
#include <TargetConditionals.h>
#if TARGET_OS_OSX
#include <libproc.h>
#include <sys/sysctl.h>
#endif
#endif
extern char** environ;
#endif

namespace yams::daemon::client {
namespace {

struct PidFileRecord {
    int pid{-1};
    std::uint64_t startTimeNs{0};
    std::filesystem::path executable;
    bool structured{false};
};

std::optional<PidFileRecord> readPidFileRecord(const std::filesystem::path& pidFilePath) {
    if (pidFilePath.empty()) {
        return std::nullopt;
    }

    std::ifstream input(pidFilePath);
    if (!input.is_open()) {
        return std::nullopt;
    }

    std::string content;
    std::getline(input, content, '\0');
    const auto first = std::find_if_not(content.begin(), content.end(),
                                        [](unsigned char ch) { return std::isspace(ch) != 0; });
    const auto last = std::find_if_not(content.rbegin(), content.rend(), [](unsigned char ch) {
                          return std::isspace(ch) != 0;
                      }).base();
    if (first >= last) {
        return std::nullopt;
    }
    content = std::string(first, last);

    if (content.front() == '{') {
        auto parsed = nlohmann::json::parse(content, nullptr, false);
        if (parsed.is_discarded() || !parsed.is_object() || !parsed.contains("pid") ||
            !parsed["pid"].is_number_integer()) {
            return std::nullopt;
        }
        const auto pid = parsed["pid"].get<std::int64_t>();
        if (pid <= 0 || pid > std::numeric_limits<int>::max()) {
            return std::nullopt;
        }
        PidFileRecord record;
        record.pid = static_cast<int>(pid);
        if (parsed.contains("start_ns")) {
            if (!parsed["start_ns"].is_number_unsigned()) {
                return std::nullopt;
            }
            record.startTimeNs = parsed["start_ns"].get<std::uint64_t>();
        }
        if (parsed.contains("exe")) {
            if (!parsed["exe"].is_string()) {
                return std::nullopt;
            }
            record.executable = parsed["exe"].get<std::string>();
        }
        record.structured = true;
        return record.pid > 0 ? std::optional<PidFileRecord>{std::move(record)} : std::nullopt;
    }

    std::istringstream parser(content);
    PidFileRecord record;
    parser >> record.pid;
    parser >> std::ws;
    return parser && parser.eof() && record.pid > 0
               ? std::optional<PidFileRecord>{std::move(record)}
               : std::nullopt;
}

std::optional<int> readPidFromFile(const std::filesystem::path& pidFilePath) {
    auto record = readPidFileRecord(pidFilePath);
    return record ? std::optional<int>{record->pid} : std::nullopt;
}

bool isProcessAlive(int pid) {
    if (pid <= 0) {
        return false;
    }
#ifdef _WIN32
    HANDLE process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, pid);
    if (!process) {
        return false;
    }
    DWORD exitCode = 0;
    const bool alive = GetExitCodeProcess(process, &exitCode) && exitCode == STILL_ACTIVE;
    CloseHandle(process);
    return alive;
#else
    const bool querySucceeded =
        kill(pid, 0) == 0; // nosemgrep: yams.cpp.kill-zero-one-shot -- identity snapshot
    return querySucceeded || errno == EPERM;
#endif
}

} // namespace

std::filesystem::path processExecutablePath(int pid) {
#ifdef _WIN32
    HANDLE process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, pid);
    if (!process) {
        return {};
    }
    char buffer[MAX_PATH];
    DWORD size = static_cast<DWORD>(sizeof(buffer));
    const bool ok = QueryFullProcessImageNameA(process, 0, buffer, &size) != 0;
    CloseHandle(process);
    return ok ? std::filesystem::path(std::string(buffer, size)) : std::filesystem::path{};
#elif defined(__APPLE__) && TARGET_OS_OSX
    char buffer[PROC_PIDPATHINFO_MAXSIZE] = {};
    const int size = proc_pidpath(pid, buffer, sizeof(buffer));
    return size > 0 ? std::filesystem::path(std::string(buffer, static_cast<std::size_t>(size)))
                    : std::filesystem::path{};
#elif defined(__APPLE__)
    (void)pid;
    return {};
#else
    std::error_code ec;
    auto path = std::filesystem::read_symlink(
        std::filesystem::path("/proc") / std::to_string(pid) / "exe", ec);
    return ec ? std::filesystem::path{} : path;
#endif
}

std::uint64_t processStartTimeNs(int pid) {
#ifdef _WIN32
    HANDLE process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, pid);
    if (!process) {
        return 0;
    }
    FILETIME createTime{}, exitTime{}, kernelTime{}, userTime{};
    const bool ok = GetProcessTimes(process, &createTime, &exitTime, &kernelTime, &userTime) != 0;
    CloseHandle(process);
    if (!ok) {
        return 0;
    }
    ULARGE_INTEGER value;
    value.LowPart = createTime.dwLowDateTime;
    value.HighPart = createTime.dwHighDateTime;
    return static_cast<std::uint64_t>(value.QuadPart) * 100ull;
#elif defined(__APPLE__) && TARGET_OS_OSX
    struct kinfo_proc processInfo;
    std::memset(&processInfo, 0, sizeof(processInfo));
    int mib[4] = {CTL_KERN, KERN_PROC, KERN_PROC_PID, pid};
    std::size_t length = sizeof(processInfo);
    if (sysctl(mib, 4, &processInfo, &length, nullptr, 0) != 0 || length == 0) {
        return 0;
    }
    const auto start = processInfo.kp_proc.p_starttime;
    return static_cast<std::uint64_t>(start.tv_sec) * 1'000'000'000ull +
           static_cast<std::uint64_t>(start.tv_usec) * 1'000ull;
#elif defined(__APPLE__)
    (void)pid;
    return 0;
#else
    std::ifstream statFile("/proc/" + std::to_string(pid) + "/stat");
    std::string statLine;
    std::getline(statFile, statLine);
    const auto rightParen = statLine.rfind(')');
    if (rightParen == std::string::npos) {
        return 0;
    }
    std::istringstream fields(statLine.substr(rightParen + 1));
    std::string ignored;
    for (int field = 0; field < 19; ++field) {
        if (!(fields >> ignored)) {
            return 0;
        }
    }
    unsigned long long startTicks = 0;
    const long ticksPerSecond = sysconf(_SC_CLK_TCK);
    if (!(fields >> startTicks) || ticksPerSecond <= 0) {
        return 0;
    }
    const double seconds = static_cast<double>(startTicks) / static_cast<double>(ticksPerSecond);
    return static_cast<std::uint64_t>(seconds * 1'000'000'000.0);
#endif
}

namespace {

bool sameExecutable(const std::filesystem::path& recorded, const std::filesystem::path& live) {
    if (recorded.empty() || live.empty()) {
        return false;
    }
    std::error_code recordedError;
    const auto canonicalRecorded = std::filesystem::weakly_canonical(recorded, recordedError);
    std::error_code liveError;
    const auto canonicalLive = std::filesystem::weakly_canonical(live, liveError);
    return !recordedError && !liveError && canonicalRecorded == canonicalLive;
}

bool executableLooksLikeDaemon(const std::filesystem::path& executable) {
    auto name = executable.filename().string();
#ifdef _WIN32
    std::transform(name.begin(), name.end(), name.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
#endif
    return name == "yams-daemon" || name == "yams-daemon.exe";
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

bool isLiveDaemonProcess(int pid) {
    return isProcessAlive(pid) && executableLooksLikeDaemon(processExecutablePath(pid));
}

bool pidFileIdentifiesLiveDaemon(const std::filesystem::path& pidFilePath, int expectedPid) {
    const auto record = readPidFileRecord(pidFilePath);
    if (!record || record->pid != expectedPid || !isLiveDaemonProcess(expectedPid)) {
        return false;
    }

    const auto liveExecutable = processExecutablePath(expectedPid);
    if (!record->structured) {
        return true;
    }
    if (record->startTimeNs == 0 && record->executable.empty()) {
        return false;
    }
    if (record->startTimeNs != 0) {
        const auto liveStartTime = processStartTimeNs(expectedPid);
        if (liveStartTime == 0 || liveStartTime != record->startTimeNs) {
            return false;
        }
    }
    if (!record->executable.empty() && !sameExecutable(record->executable, liveExecutable)) {
        return false;
    }
    return true;
}

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
        pidFromFile && pidFileIdentifiesLiveDaemon(pidFilePath, *pidFromFile)) {
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
        if (!isLiveDaemonProcess(pid)) {
            continue;
        }
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
