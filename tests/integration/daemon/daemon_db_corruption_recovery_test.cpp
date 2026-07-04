// Integration smoke: daemon recovers from a corrupt metadata DB on startup.
//
// Plants a corrupt yams.db into a fresh data dir, starts the daemon harness,
// asserts the daemon reaches Ready (no hang) and that the corrupt file was
// quarantined to yams.db.corrupt-<timestamp> with a sentinel left behind.

#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <thread>

#include "test_daemon_harness.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/metadata/database.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {
constexpr std::uintmax_t kOversizedDbBytes = 600ULL * 1024 * 1024;
constexpr std::uintmax_t kCompactedDbMaxBytes = 1024ULL * 1024;

void writeCorruptDb(const fs::path& path) {
    std::ofstream out(path, std::ios::binary);
    std::string header = "SQLite format 3";
    header.push_back('\0');
    out.write(header.data(), static_cast<std::streamsize>(header.size()));
    const std::string garbage(4096, '\xff');
    out.write(garbage.data(), static_cast<std::streamsize>(garbage.size()));
}

void writeDummyWal(const fs::path& dbPath, const std::string& payload = "stale-wal") {
    std::ofstream out(dbPath.string() + "-wal", std::ios::binary | std::ios::trunc);
    REQUIRE(out.good());
    out.write(payload.data(), static_cast<std::streamsize>(payload.size()));
}

void seedValidDb(const fs::path& dbPath, const std::string& value = "seed") {
    yams::metadata::Database db;
    REQUIRE(db.open(dbPath.string(), yams::metadata::ConnectionMode::Create));
    REQUIRE(
        db.execute("CREATE TABLE IF NOT EXISTS startup_probe(id INTEGER PRIMARY KEY, value TEXT)"));
    REQUIRE(db.execute("DELETE FROM startup_probe"));
    REQUIRE(db.execute("INSERT INTO startup_probe(value) VALUES('" + value + "')"));
    db.close();
}

std::size_t startupProbeRowCount(const fs::path& dbPath) {
    yams::metadata::Database db;
    REQUIRE(db.open(dbPath.string(), yams::metadata::ConnectionMode::ReadWrite));
    auto stmtR = db.prepare("SELECT COUNT(*) FROM startup_probe");
    REQUIRE(stmtR);
    auto& stmt = stmtR.value();
    REQUIRE(stmt.step());
    auto count = static_cast<std::size_t>(stmt.getInt(0));
    db.close();
    return count;
}

bool walSidecarCleared(const fs::path& dbPath) {
    std::error_code ec;
    const fs::path walPath(dbPath.string() + "-wal");
    if (!fs::exists(walPath, ec)) {
        return true;
    }
    return fs::file_size(walPath, ec) == 0;
}

bool walSidecarStillMatchesPayload(const fs::path& dbPath, const std::string& payload) {
    const fs::path walPath(dbPath.string() + "-wal");
    std::error_code ec;
    if (!fs::exists(walPath, ec) || fs::file_size(walPath, ec) != payload.size()) {
        return false;
    }
    std::ifstream in(walPath, std::ios::binary);
    std::string actual(payload.size(), '\0');
    in.read(actual.data(), static_cast<std::streamsize>(actual.size()));
    return in.good() && actual == payload;
}

std::optional<fs::path> findQuarantinedFile(const fs::path& dataDir) {
    std::error_code ec;
    if (!fs::exists(dataDir, ec)) {
        return std::nullopt;
    }
    const std::string prefix = "yams.db.corrupt-";
    for (fs::directory_iterator it(dataDir, ec), end; it != end; it.increment(ec)) {
        if (ec) {
            ec.clear();
            continue;
        }
        if (it->path().filename().string().rfind(prefix, 0) == 0 &&
            it->path().extension() != ".sentinel") {
            return it->path();
        }
    }
    return std::nullopt;
}

bool waitForDbBelowSize(const fs::path& dbPath, std::uintmax_t maxBytes,
                        std::chrono::steady_clock::duration timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::error_code ec;
    while (std::chrono::steady_clock::now() < deadline) {
        const auto size = fs::file_size(dbPath, ec);
        if (!ec && size < maxBytes) {
            return true;
        }
        ec.clear();
        std::this_thread::sleep_for(100ms);
    }
    const auto size = fs::file_size(dbPath, ec);
    return !ec && size < maxBytes;
}

} // namespace

TEST_CASE("Daemon auto-recovers from corrupt metadata DB on startup",
          "[integration][daemon][catch2][db_recovery]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    auto dataDir = fs::temp_directory_path() /
                   ("yams_db_recovery_it_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::create_directories(dataDir);
    const auto dbPath = dataDir / "yams.db";
    writeCorruptDb(dbPath);
    writeDummyWal(dbPath, "corrupt-sidecar");

    yams::test::DaemonHarnessOptions opts;
    opts.dataDir = dataDir;
    opts.enableModelProvider = false;
    opts.useMockModelProvider = false;
    opts.autoLoadPlugins = false;
    opts.enableAutoRepair = false;

    yams::test::DaemonHarness harness(opts);
    bool started = harness.start(60s);
    REQUIRE(started);

    auto quarantinedPath = findQuarantinedFile(dataDir);
    REQUIRE(quarantinedPath.has_value());
    REQUIRE(fs::exists(*quarantinedPath));

    yams::daemon::ClientConfig cfg;
    cfg.socketPath = harness.socketPath();
    cfg.requestTimeout = 5s;
    yams::daemon::DaemonClient client(cfg);
    auto connect = yams::cli::run_sync(client.connect(), 5s);
    REQUIRE(connect.has_value());

    auto status = yams::cli::run_sync(client.status(), 5s);
    REQUIRE(status.has_value());
    // After a corruption-triggered recovery the daemon should report the
    // ready phase and remember which file was quarantined so the CLI can
    // surface "recovered from …" in `yams daemon status`.
    REQUIRE((status.value().databasePhase == "ready"));
    REQUIRE_FALSE(status.value().databaseRecoveredFrom.empty());
    CHECK((status.value().databaseRecoveredFrom == quarantinedPath->string()));
    CHECK((status.value().databaseRecoveredFrom.find(dataDir.string()) == 0));

    harness.stop();

    std::error_code ec;
    fs::remove_all(dataDir, ec);
}

TEST_CASE("Daemon startup checkpoints stale WAL and auto-vacuums oversized metadata DB",
          "[integration][daemon][catch2][db_recovery][vacuum][wal]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    auto dataDir = fs::temp_directory_path() /
                   ("yams_db_vacuum_it_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::create_directories(dataDir);
    const auto dbPath = dataDir / "yams.db";
    seedValidDb(dbPath, "vacuum_seed");
    fs::resize_file(dbPath, kOversizedDbBytes);
    REQUIRE((fs::file_size(dbPath) == kOversizedDbBytes));
    std::error_code spaceEc;
    const auto spaceBeforeStart = fs::space(dataDir, spaceEc);
    const bool expectAutoVacuum = !spaceEc && spaceBeforeStart.available > kOversizedDbBytes;
    const std::string staleWalPayload = "oversized-sidecar";
    writeDummyWal(dbPath, staleWalPayload);

    yams::test::DaemonHarnessOptions opts;
    opts.dataDir = dataDir;
    opts.enableModelProvider = false;
    opts.useMockModelProvider = false;
    opts.autoLoadPlugins = false;
    opts.enableAutoRepair = false;

    yams::test::DaemonHarness harness(opts);
    bool started = harness.start(60s);
    REQUIRE(started);

    yams::daemon::ClientConfig cfg;
    cfg.socketPath = harness.socketPath();
    cfg.requestTimeout = 5s;
    yams::daemon::DaemonClient client(cfg);
    auto connect = yams::cli::run_sync(client.connect(), 5s);
    REQUIRE(connect.has_value());

    auto status = yams::cli::run_sync(client.status(), 5s);
    REQUIRE(status.has_value());
    CHECK((status.value().databasePhase == "ready"));
    CHECK(status.value().databaseRecoveredFrom.empty());
    CHECK_FALSE(walSidecarStillMatchesPayload(dbPath, staleWalPayload));
    CHECK((startupProbeRowCount(dbPath) == 1U));

    if (expectAutoVacuum) {
        CHECK(waitForDbBelowSize(dbPath, kCompactedDbMaxBytes, 30s));
    } else {
        WARN("Skipping DB shrink assertion because this runner has insufficient free space for "
             "startup auto-VACUUM");
    }

    harness.stop();
    CHECK(walSidecarCleared(dbPath));
    if (expectAutoVacuum) {
        CHECK((fs::file_size(dbPath) < kCompactedDbMaxBytes));
    }
    CHECK((startupProbeRowCount(dbPath) == 1U));

    std::error_code ec;
    fs::remove_all(dataDir, ec);
}
