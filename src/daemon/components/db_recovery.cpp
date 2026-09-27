#include <yams/daemon/components/db_recovery.h>

#include <spdlog/spdlog.h>

#include <chrono>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <system_error>

namespace yams::daemon {

namespace fs = std::filesystem;

namespace {

std::string formatUtcTimestamp(std::chrono::system_clock::time_point tp) {
    auto t = std::chrono::system_clock::to_time_t(tp);
    std::tm tm{};
#ifdef _WIN32
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    std::ostringstream out;
    out << std::put_time(&tm, "%Y%m%dT%H%M%SZ");
    return out.str();
}

// Returns `base` when free, else `base.N` for the first free N. Recovery artifacts must never be
// renamed over an earlier one: POSIX rename silently replaces the destination.
fs::path uniqueSiblingPath(const fs::path& base) {
    std::error_code ec;
    if (!fs::exists(base, ec) && !ec) {
        return base;
    }
    for (int i = 1; i < 1000; ++i) {
        fs::path candidate(base.string() + "." + std::to_string(i));
        if (!fs::exists(candidate, ec) && !ec) {
            return candidate;
        }
    }
    return {};
}

void renameIfExists(const fs::path& src, const fs::path& dst) {
    std::error_code ec;
    if (fs::exists(src, ec)) {
        fs::rename(src, dst, ec);
        if (ec) {
            spdlog::warn("[db_recovery] could not rename {} -> {}: {}", src.string(), dst.string(),
                         ec.message());
        }
    }
}

} // namespace

Result<DbRecoveryResult> quarantineAndRecreate(const fs::path& dbPath) {
    if (dbPath.empty()) {
        return Error{ErrorCode::InvalidArgument, "db path is empty"};
    }

    const auto timestamp = formatUtcTimestamp(std::chrono::system_clock::now());

    std::error_code ec;
    if (!fs::exists(dbPath, ec)) {
        return Error{ErrorCode::FileNotFound,
                     "db file does not exist for quarantine: " + dbPath.string()};
    }

    // Never rename over an earlier quarantine from the same second.
    const fs::path quarantined =
        uniqueSiblingPath(dbPath.string() + std::string(kCorruptDbMarker) + timestamp);
    if (quarantined.empty()) {
        return Error{ErrorCode::IOError, "no free quarantine name for " + dbPath.string()};
    }

    fs::rename(dbPath, quarantined, ec);
    if (ec) {
        return Error{ErrorCode::IOError, "rename " + dbPath.string() + " -> " +
                                             quarantined.string() + " failed: " + ec.message()};
    }

    renameIfExists(fs::path(dbPath.string() + "-wal"), fs::path(quarantined.string() + "-wal"));
    renameIfExists(fs::path(dbPath.string() + "-shm"), fs::path(quarantined.string() + "-shm"));

    const fs::path sentinel = dbPath.string() + ".recovered-" + timestamp;
    {
        std::ofstream out(sentinel);
        if (out) {
            out << "quarantined_path=" << quarantined.string() << '\n';
            out << "timestamp=" << timestamp << '\n';
            out << "note=Original metadata DB failed integrity check on startup. Run\n";
            out << "note=`yams repair --orphans` to rebuild metadata from CAS.\n";
        }
    }

    spdlog::warn("[db_recovery] metadata DB quarantined: {} (run 'yams repair --orphans' "
                 "to rebuild metadata)",
                 quarantined.string());

    DbRecoveryResult res{quarantined, sentinel, timestamp};
    return res;
}

std::vector<fs::path> listCorruptDbs(const fs::path& dataDir, std::string_view dbFileName) {
    std::vector<fs::path> out;
    std::error_code ec;
    for (fs::directory_iterator it(dataDir, ec), end; !ec && it != end; it.increment(ec)) {
        if (isCorruptDbFileName(it->path().filename().string(), dbFileName)) {
            out.push_back(it->path());
        }
    }
    return out;
}

Result<std::vector<fs::path>> quarantineSqliteSidecars(const fs::path& dbPath) {
    if (dbPath.empty()) {
        return Error{ErrorCode::InvalidArgument, "db path is empty"};
    }

    const auto timestamp = formatUtcTimestamp(std::chrono::system_clock::now());
    std::vector<fs::path> moved;
    // WAL first: it is the file that can carry uncheckpointed data. If it cannot be moved, leave
    // the SHM alone too so SQLite still sees a consistent pair on the next open.
    for (const auto* suffix : {"-wal", "-shm"}) {
        const fs::path sidecar(dbPath.string() + suffix);
        std::error_code ec;
        if (!fs::exists(sidecar, ec)) {
            if (ec) {
                return Error{ErrorCode::IOError,
                             "cannot inspect " + sidecar.string() + ": " + ec.message()};
            }
            continue;
        }
        const auto target = uniqueSiblingPath(
            sidecar.string() + std::string(kSqliteSidecarQuarantineMarker) + timestamp);
        if (target.empty()) {
            return Error{ErrorCode::IOError, "no free quarantine name for " + sidecar.string() +
                                                 " (moved so far: " + std::to_string(moved.size()) +
                                                 ")"};
        }
        fs::rename(sidecar, target, ec);
        if (ec) {
            return Error{ErrorCode::IOError, "rename " + sidecar.string() + " -> " +
                                                 target.string() + " failed: " + ec.message() +
                                                 " (moved so far: " + std::to_string(moved.size()) +
                                                 ")"};
        }
        moved.push_back(target);
    }
    return moved;
}

std::optional<DbRecoverySentinel> readLatestRecoverySentinel(const fs::path& dbPath) {
    if (dbPath.empty()) {
        return std::nullopt;
    }
    const fs::path parent = dbPath.has_parent_path() ? dbPath.parent_path() : fs::path(".");
    const std::string prefix = dbPath.filename().string() + ".recovered-";

    std::error_code ec;
    if (!fs::exists(parent, ec) || !fs::is_directory(parent, ec)) {
        return std::nullopt;
    }

    fs::path latest;
    std::string latestStem;
    for (fs::directory_iterator it(parent, ec), end; it != end; it.increment(ec)) {
        if (ec) {
            ec.clear();
            continue;
        }
        const auto& p = it->path();
        const auto name = p.filename().string();
        if (name.rfind(prefix, 0) != 0) {
            continue;
        }
        if (latest.empty() || name > latestStem) {
            latest = p;
            latestStem = name;
        }
    }
    if (latest.empty()) {
        return std::nullopt;
    }

    DbRecoverySentinel out{};
    out.timestamp = latestStem.substr(prefix.size());
    std::ifstream in(latest);
    std::string line;
    while (std::getline(in, line)) {
        const auto eq = line.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        const auto key = line.substr(0, eq);
        const auto val = line.substr(eq + 1);
        if (key == "quarantined_path") {
            out.quarantinedPath = val;
        }
    }
    return out;
}

} // namespace yams::daemon
