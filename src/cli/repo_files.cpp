#include <yams/cli/repo_files.h>

#include <algorithm>
#include <cerrno>
#include <set>
#include <stdexcept>
#include <system_error>
#ifndef _WIN32
#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <unistd.h>
#include <sys/wait.h>
extern char** environ;
#endif

namespace yams::cli {

CliProcessResult runCliProcess(const std::vector<std::string>& arguments, std::size_t maxBytes,
                               const std::function<bool(std::string_view)>& onLine) {
    if (arguments.empty())
        throw std::invalid_argument("Missing process arguments");
#ifdef _WIN32
    (void)maxBytes;
    (void)onLine;
    throw std::runtime_error("Git-aware CLI reads currently require a POSIX host");
#else
    int descriptors[2];
    if (pipe(descriptors) != 0)
        throw std::runtime_error("Cannot create CLI subprocess pipe");
    struct PipeGuard {
        int* fds;
        ~PipeGuard() {
            for (int i = 0; i < 2; ++i)
                if (fds[i] >= 0)
                    close(fds[i]);
        }
    } pipeGuard{descriptors};
    if (fcntl(descriptors[0], F_SETFD, FD_CLOEXEC) == -1 ||
        fcntl(descriptors[1], F_SETFD, FD_CLOEXEC) == -1)
        throw std::runtime_error("Cannot protect CLI subprocess pipe");
    std::vector<char*> argv;
    for (const auto& arg : arguments) {
        if (arg.find('\0') != std::string::npos)
            throw std::invalid_argument("NUL byte in process argument");
        argv.push_back(const_cast<char*>(arg.c_str()));
    }
    argv.push_back(nullptr);
    posix_spawn_file_actions_t actions;
    if (posix_spawn_file_actions_init(&actions) != 0)
        throw std::runtime_error("Cannot initialize CLI subprocess actions");
    struct ActionGuard {
        posix_spawn_file_actions_t* actions;
        ~ActionGuard() { posix_spawn_file_actions_destroy(actions); }
    } actionGuard{&actions};
    if (posix_spawn_file_actions_adddup2(&actions, descriptors[1], STDOUT_FILENO) != 0 ||
        posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, "/dev/null", O_WRONLY, 0) != 0 ||
        posix_spawn_file_actions_addclose(&actions, descriptors[0]) != 0 ||
        posix_spawn_file_actions_addclose(&actions, descriptors[1]) != 0)
        throw std::runtime_error("Cannot configure CLI subprocess output");
    pid_t child = 0;
    const auto spawned = posix_spawnp(&child, argv[0], &actions, nullptr, argv.data(), environ);
    if (spawned != 0)
        throw std::system_error(spawned, std::generic_category(), "Cannot start " + arguments[0]);
    close(descriptors[1]);
    descriptors[1] = -1;
    struct ChildGuard {
        pid_t pid;
        bool waited{false};
        ~ChildGuard() {
            if (!waited) {
                kill(pid, SIGKILL);
                int status = 0;
                while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {
                }
            }
        }
    } childGuard{child};
    CliProcessResult result;
    std::string pending;
    bool stopped = false;
    char buffer[16384];
    while (true) {
        const auto count = read(descriptors[0], buffer, sizeof(buffer));
        if (count < 0 && errno == EINTR)
            continue;
        if (count < 0)
            throw std::runtime_error("Cannot read CLI subprocess output");
        if (count == 0)
            break;
        auto& destination = onLine ? pending : result.output;
        if (static_cast<std::size_t>(count) > maxBytes - destination.size())
            throw std::runtime_error("CLI subprocess output exceeds capture limit");
        destination.append(buffer, static_cast<std::size_t>(count));
        if (onLine) {
            std::size_t consumed = 0;
            while (true) {
                const auto end = pending.find('\n', consumed);
                if (end == std::string::npos)
                    break;
                if (!onLine(std::string_view(pending).substr(consumed, end - consumed))) {
                    stopped = true;
                    break;
                }
                consumed = end + 1;
            }
            pending.erase(0, consumed);
            if (stopped) {
                kill(child,
                     SIGKILL); // read-only child; stop immediately once the caller's cap is met
                break;
            }
        }
    }
    if (onLine && !stopped && !pending.empty())
        (void)onLine(pending);
    int status = 0;
    pid_t waited;
    do {
        waited = waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    if (waited < 0)
        throw std::runtime_error("Cannot reap CLI subprocess");
    childGuard.waited = true;
    result.exitCode =
        stopped ? 0 : (WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status));
    return result;
#endif
}

namespace {
std::string gitText(const RepoPath& directory, std::vector<std::string> args) {
    std::vector<std::string> command{"git", "-C", directory.string()};
    command.insert(command.end(), args.begin(), args.end());
    auto result = runCliProcess(command);
    if (result.exitCode != 0)
        throw std::runtime_error("Cannot inspect Git repository at " + directory.string());
    if (!result.output.empty() && result.output.back() == '\n')
        result.output.pop_back();
    return result.output;
}

bool insideRoot(const RepoPath& path, const RepoPath& root) {
    const auto relative = path.lexically_normal().lexically_relative(root);
    return !relative.empty() && !relative.is_absolute() && *relative.begin() != "..";
}
} // namespace

RepoFiles readRepoFiles(const RepoPath& directory, bool includeUntracked) {
    const auto absolute = std::filesystem::canonical(directory);
    RepoFiles files;
    files.root = std::filesystem::canonical(gitText(absolute, {"rev-parse", "--show-toplevel"}));
    files.worktree = files.root.filename().string();
    auto branch = runCliProcess(
        {"git", "-C", files.root.string(), "symbolic-ref", "--quiet", "--short", "HEAD"});
    if (branch.exitCode == 0) {
        files.branch = std::move(branch.output);
        if (!files.branch.empty() && files.branch.back() == '\n')
            files.branch.pop_back();
    } else {
        files.branch = gitText(files.root, {"rev-parse", "--short", "HEAD"});
    }
    std::vector<std::string> command{"git", "-C",      files.root.string(), "ls-files",
                                     "-z",  "--stage", "--cached"};
    auto result = runCliProcess(command);
    if (result.exitCode != 0)
        throw std::runtime_error("Cannot enumerate Git files");
    std::set<RepoPath> unique;
    const auto appendFiles = [&](const std::string& output, bool staged) {
        for (std::size_t start = 0; start < output.size();) {
            const auto end = output.find('\0', start);
            if (end == std::string::npos)
                throw std::runtime_error("Invalid Git file enumeration");
            auto entry = output.substr(start, end - start);
            start = end + 1;
            if (staged) {
                if (entry.starts_with("160000 "))
                    continue; // a gitlink is a separate repository, not a file in this worktree
                const auto tab = entry.find('\t');
                if (tab == std::string::npos)
                    throw std::runtime_error("Invalid staged Git file entry");
                entry.erase(0, tab + 1);
            }
            const auto path = (files.root / entry).lexically_normal();
            if (!insideRoot(path, files.root))
                throw std::runtime_error("Git path escapes worktree");
            unique.insert(path);
        }
    };
    appendFiles(result.output, true);
    if (includeUntracked) {
        auto other = runCliProcess(
            {"git", "-C", files.root.string(), "ls-files", "-z", "--others", "--exclude-standard"});
        if (other.exitCode != 0)
            throw std::runtime_error("Cannot enumerate untracked Git files");
        appendFiles(other.output, false);
    }
    files.tracked.assign(unique.begin(), unique.end());
    return files;
}

RepoPath resolveWorktree(const RepoPath& directory, std::string_view name) {
    const auto result =
        runCliProcess({"git", "-C", directory.string(), "worktree", "list", "--porcelain", "-z"});
    if (result.exitCode != 0)
        throw std::runtime_error("Cannot list worktrees");
    std::vector<RepoPath> matches;
    for (std::size_t start = 0; start < result.output.size();) {
        const auto end = result.output.find('\0', start);
        if (end == std::string::npos)
            break;
        const std::string_view field(result.output.data() + start, end - start);
        if (field.starts_with("worktree ")) {
            RepoPath path(field.substr(9));
            if (path.filename() == name || path.generic_string() == name)
                matches.push_back(std::move(path));
        }
        start = end + 1;
    }
    if (matches.size() != 1)
        throw std::runtime_error("Worktree name must identify exactly one registered worktree");
    return matches.front();
}

RepoDiff compareRepoIndex(const RepoFiles& files, const RepoIndex& index,
                          const std::function<std::string(const RepoPath&)>& hashFile) {
    RepoDiff result;
    result.tracked = files.tracked.size();
    std::set<RepoPath> missing;
    for (const auto& path : files.tracked) {
        std::error_code error;
        const auto status = std::filesystem::symlink_status(path, error);
        if (error && error != std::errc::no_such_file_or_directory)
            throw std::runtime_error("Cannot stat tracked file: " + path.string());
        if (!std::filesystem::exists(status)) {
            missing.insert(path);
            continue;
        }
        if (!std::filesystem::is_regular_file(status))
            throw std::runtime_error("Repo freshness requires regular tracked files: " +
                                     path.string());
        const auto found = index.find(path);
        if (found == index.end() || found->second.hash != hashFile(path))
            result.changed.push_back(path);
    }
    for (const auto& [path, entry] : index) {
        (void)entry;
        if (insideRoot(path, files.root) && !std::filesystem::exists(path))
            missing.insert(path);
    }
    result.missing.assign(missing.begin(), missing.end());
    return result;
}

} // namespace yams::cli
