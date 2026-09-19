// WalMetricsProvider optics seam: WALManager counters must reach a queryable
// surface. The provider is the smallest testable unit — verify no-manager
// zeros and real-manager pass-through so the DaemonMetrics enrichment has a
// trusted input (metrics/data-system optics, yams status --detailed).
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>

#include <yams/daemon/components/WalMetricsProvider.h>
#include <yams/wal/wal_manager.h>

using namespace yams;
using namespace yams::daemon;
using namespace yams::wal;

namespace {

struct TempDir {
    TempDir() {
        auto stamp = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        path = std::filesystem::temp_directory_path() / ("yams_wal_metrics_test_" + stamp);
        std::filesystem::create_directories(path);
    }

    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }

    std::filesystem::path path;
};

} // namespace

TEST_CASE("WalMetricsProvider reports zeros without a manager", "[unit][daemon][metrics][wal]") {
    WalMetricsProvider provider;
    const auto stats = provider.getStats();
    CHECK(stats.activeTransactions == 0);
    CHECK(stats.pendingEntries == 0);
    CHECK(stats.totalEntries == 0);
    CHECK(stats.totalBytes == 0);
    CHECK(stats.logFileCount == 0);
}

TEST_CASE("WalMetricsProvider stays noexcept when WAL stats throw",
          "[unit][daemon][metrics][wal]") {
    // WALManager::getStats() performs filesystem queries over walDirectory. Pointing the config at
    // a regular file makes std::filesystem::directory_iterator throw, which is the cheapest
    // deterministic way to reach that path. The provider is noexcept, so an escaping exception
    // would terminate the daemon instead of degrading to zeroed metrics.
    TempDir temp;
    const auto notADirectory = temp.path / "wal-is-a-file";
    {
        std::ofstream file(notADirectory);
        REQUIRE(file.good());
        file << "not a directory";
    }

    WALManager::Config cfg;
    cfg.walDirectory = notADirectory;
    cfg.compressOldLogs = false;
    cfg.enableGroupCommit = false;

    auto manager = std::make_shared<WALManager>(cfg);

    WalMetricsProvider provider;
    provider.setManager(manager);

    // Must not throw and must not terminate: the optional filesystem statistics are missing, so
    // the best-effort contract is zeros.
    CHECK_NOTHROW((void)provider.getStats());
    const auto stats = provider.getStats();
    CHECK(stats.logFileCount == 0);
}

TEST_CASE("WalMetricsProvider passes through WALManager stats", "[unit][daemon][metrics][wal]") {
    TempDir temp;
    const auto walDir = temp.path / "wal";
    std::filesystem::create_directories(walDir);

    WALManager::Config cfg;
    cfg.walDirectory = walDir;
    cfg.compressOldLogs = false;
    cfg.enableGroupCommit = false;
    cfg.syncInterval = 1;

    auto manager = std::make_shared<WALManager>(cfg);
    REQUIRE(manager->initialize());

    WalMetricsProvider provider;
    provider.setManager(manager);

    // A written entry must be visible in totalEntries / totalBytes and the
    // provider must forward it unchanged (the optics surface depends on it).
    const std::string hash(64, 'a');
    auto payload = WALEntry::DeleteBlockData::encode(hash);
    WALEntry entry(WALEntry::OpType::DeleteBlock, 0, 0, payload);
    REQUIRE(manager->writeEntry(entry));

    const auto stats = provider.getStats();
    CHECK(stats.totalEntries >= 1);
    CHECK(stats.totalBytes > 0);
    CHECK(stats.logFileCount >= 1);
    // activeTransactions/pendingEntries are transient; only assert the stable ones.

    // Nulling the manager returns to the zero baseline (shutdown path).
    provider.setManager(nullptr);
    const auto reset = provider.getStats();
    CHECK(reset.totalEntries == 0);

    REQUIRE(manager->shutdown());
}
