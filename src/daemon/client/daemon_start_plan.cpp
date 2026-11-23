#include <yams/daemon/client/daemon_start_plan.h>

#include <nlohmann/json.hpp>

#include <cstring>
#include <fstream>
#include <iterator>
#include <system_error>

#ifndef _WIN32
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#endif

namespace yams::daemon::client {

namespace fs = std::filesystem;
using yams::config::RuntimePathSource;

namespace {

fs::path siblingWithExtension(const fs::path& socketPath, const char* extension) {
    auto stem = socketPath.stem().string();
    if (stem.empty()) {
        stem = "yams-daemon";
    }
    return socketPath.parent_path() / (stem + extension);
}

} // namespace

bool sameRuntimePath(const fs::path& lhs, const fs::path& rhs) {
    if (lhs.empty() || rhs.empty()) {
        return lhs.empty() && rhs.empty();
    }
    const auto normal = [](const fs::path& p) {
        auto n = p.lexically_normal();
        return n.has_filename() || !n.has_relative_path() ? n : n.parent_path(); // drop trailing /
    };
    if (normal(lhs) == normal(rhs)) {
        return true;
    }
    std::error_code ec;
    const bool equivalent = fs::equivalent(lhs, rhs, ec);
    return !ec && equivalent;
}

DaemonStartPlan planDaemonStart(const yams::config::ResolvedRuntimePaths& effective,
                                const yams::config::ResolvedRuntimePaths& shared,
                                const std::optional<fs::path>& pidFileFlag,
                                const std::optional<fs::path>& logFileFlag) {
    DaemonStartPlan plan;
    plan.socketPath = effective.socketPath.value;
    plan.dataDir = effective.dataDir.value;
    plan.configFile = effective.configFile.value;
    plan.sharedSocket = sameRuntimePath(plan.socketPath, shared.socketPath.value);
    plan.sharedDataDir = sameRuntimePath(plan.dataDir, shared.dataDir.value);

    if (pidFileFlag && !pidFileFlag->empty()) {
        plan.pidFile = *pidFileFlag;
    } else if (plan.sharedSocket) {
        plan.pidFile = effective.pidFile.value;
    } else if (effective.pidFile.source != RuntimePathSource::PlatformDefault &&
               !sameRuntimePath(effective.pidFile.value, shared.pidFile.value)) {
        plan.pidFile = effective.pidFile.value;
    } else {
        plan.pidFile = siblingWithExtension(plan.socketPath, ".pid");
    }

    if (logFileFlag && !logFileFlag->empty()) {
        plan.logFile = *logFileFlag;
    } else if (!plan.sharedDataDir) {
        plan.logFile = plan.dataDir / "daemon.log";
    } else if (!plan.sharedSocket) {
        plan.logFile = siblingWithExtension(plan.socketPath, ".log");
    }
    return plan;
}

LiveDaemonStore classifyLiveDaemonStore(const std::optional<fs::path>& liveDataDir,
                                        const fs::path& targetDataDir) {
    if (!liveDataDir || liveDataDir->empty()) {
        return LiveDaemonStore::Unknown;
    }
    return sameRuntimePath(*liveDataDir, targetDataDir) ? LiveDaemonStore::Same
                                                        : LiveDaemonStore::Other;
}

std::string otherStoreRefusal(const fs::path& socketPath, const fs::path& liveDataDir,
                              const fs::path& targetDataDir) {
    const std::string live = liveDataDir.empty() ? std::string("a data dir it did not report")
                                                 : "data dir '" + liveDataDir.string() + "'";
    return "The daemon on socket '" + socketPath.string() + "' serves " + live +
           "; not stopping it to start a daemon for data dir '" + targetDataDir.string() +
           "'. Pass a different --socket for this data dir.";
}

bool mayReplaceDaemonHoldingPidFile(int existingPid, const std::optional<fs::path>& recordedDataDir,
                                    const std::optional<int>& targetDataDirLockPid,
                                    const fs::path& targetDataDir) {
    if (existingPid <= 0) {
        return false;
    }
    if (recordedDataDir && !recordedDataDir->empty()) {
        return sameRuntimePath(*recordedDataDir, targetDataDir);
    }
    return targetDataDirLockPid && *targetDataDirLockPid == existingPid;
}

std::optional<int> readDataDirLockPid(const fs::path& dataDir) {
    if (dataDir.empty()) {
        return std::nullopt;
    }
    std::ifstream in(dataDir / ".yams-lock");
    if (!in) {
        return std::nullopt;
    }
    const std::string content((std::istreambuf_iterator<char>(in)),
                              std::istreambuf_iterator<char>());
    const auto parsed = nlohmann::json::parse(content, nullptr, false);
    if (parsed.is_discarded() || !parsed.is_object() || !parsed.contains("pid") ||
        !parsed["pid"].is_number_integer()) {
        return std::nullopt;
    }
    const auto pid = parsed["pid"].get<long long>();
    if (pid <= 0 || pid > 0x7fffffff) {
        return std::nullopt;
    }
    return static_cast<int>(pid);
}

bool unixSocketAcceptsConnections(const fs::path& socketPath) {
#ifdef _WIN32
    (void)socketPath;
    return false;
#else
    const auto native = socketPath.string();
    sockaddr_un addr{};
    if (native.empty() || native.size() >= sizeof(addr.sun_path)) {
        return false;
    }
    const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return false;
    }
    addr.sun_family = AF_UNIX;
    std::memcpy(addr.sun_path, native.c_str(), native.size() + 1);
    const bool connected =
        ::connect(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) == 0;
    ::close(fd);
    return connected;
#endif
}

} // namespace yams::daemon::client
