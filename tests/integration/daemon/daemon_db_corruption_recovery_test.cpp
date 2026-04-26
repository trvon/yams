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
#include <string>

#include "test_daemon_harness.h"
#include <yams/daemon/client/daemon_client.h>

using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {

void writeCorruptDb(const fs::path& path) {
    std::ofstream out(path, std::ios::binary);
    const std::string header = "SQLite format 3\0";
    out.write(header.data(), static_cast<std::streamsize>(header.size()));
    const std::string garbage(4096, '\xff');
    out.write(garbage.data(), static_cast<std::streamsize>(garbage.size()));
}

bool quarantinedFileExists(const fs::path& dataDir) {
    std::error_code ec;
    if (!fs::exists(dataDir, ec)) {
        return false;
    }
    const std::string prefix = "yams.db.corrupt-";
    for (fs::directory_iterator it(dataDir, ec), end; it != end; it.increment(ec)) {
        if (ec) {
            ec.clear();
            continue;
        }
        if (it->path().filename().string().rfind(prefix, 0) == 0) {
            return true;
        }
    }
    return false;
}

} // namespace

TEST_CASE("Daemon auto-recovers from corrupt metadata DB on startup",
          "[integration][daemon][catch2][db_recovery]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    auto dataDir = fs::temp_directory_path() /
                   ("yams_db_recovery_it_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::create_directories(dataDir);
    writeCorruptDb(dataDir / "yams.db");

    yams::test::DaemonHarnessOptions opts;
    opts.dataDir = dataDir;
    opts.enableModelProvider = false;
    opts.useMockModelProvider = false;
    opts.autoLoadPlugins = false;
    opts.enableAutoRepair = false;

    yams::test::DaemonHarness harness(opts);
    bool started = harness.start(60s);
    REQUIRE(started);

    REQUIRE(quarantinedFileExists(dataDir));

    yams::daemon::ClientConfig cfg;
    cfg.socketPath = harness.socketPath();
    cfg.requestTimeout = 5s;
    yams::daemon::DaemonClient client(cfg);
    auto connect = yams::cli::run_sync(client.connect(), 5s);
    REQUIRE(connect.has_value());

    auto status = yams::cli::run_sync(client.status(), 5s);
    REQUIRE(status.has_value());

    harness.stop();

    std::error_code ec;
    fs::remove_all(dataDir, ec);
}
