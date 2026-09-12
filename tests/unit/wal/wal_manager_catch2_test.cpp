#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

#include <yams/core/types.h>
#include <yams/wal/wal_manager.h>

using namespace yams;
using namespace yams::wal;

namespace {

struct TempDir {
    TempDir() {
        auto stamp = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        path = std::filesystem::temp_directory_path() / ("yams_wal_manager_test_" + stamp);
        std::filesystem::create_directories(path);
    }

    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }

    std::filesystem::path path;
};

std::string makeHash(char fill) {
    return std::string(HASH_SIZE, fill);
}

} // namespace

TEST_CASE("WALManager initialize picks max sequence from existing log names",
          "[unit][wal][manager]") {
    TempDir temp;
    auto walDir = temp.path / "wal";
    std::filesystem::create_directories(walDir);

    {
        std::ofstream plain((walDir / "wal_20240101_010101_42.log").string(), std::ios::binary);
        REQUIRE(plain.good());
    }
    {
        std::ofstream compressed((walDir / "wal_20240101_010102_123.log.zst").string(),
                                 std::ios::binary);
        REQUIRE(compressed.good());
    }
    {
        std::ofstream ignored((walDir / "wal_invalid.log").string(), std::ios::binary);
        REQUIRE(ignored.good());
    }

    WALManager::Config cfg;
    cfg.walDirectory = walDir;
    cfg.compressOldLogs = false;
    cfg.enableGroupCommit = false;
    cfg.syncInterval = 1;

    WALManager manager(cfg);
    REQUIRE(manager.initialize());

    CHECK(manager.getCurrentSequence() == 123);

    auto payload = WALEntry::DeleteBlockData::encode(makeHash('s'));
    WALEntry entry(WALEntry::OpType::DeleteBlock, 0, 0, payload);
    auto writeResult = manager.writeEntry(entry);
    REQUIRE(writeResult);
    CHECK(writeResult.value() == 124);
    CHECK(manager.getCurrentSequence() == 124);

    REQUIRE(manager.shutdown());
}

TEST_CASE("WALManager pruneLogs removes old inactive logs and keeps active log",
          "[unit][wal][manager]") {
    TempDir temp;
    auto walDir = temp.path / "wal";

    WALManager::Config cfg;
    cfg.walDirectory = walDir;
    cfg.compressOldLogs = false;
    cfg.enableGroupCommit = false;
    cfg.syncInterval = 1;

    WALManager manager(cfg);
    REQUIRE(manager.initialize());

    auto activeLogPath = manager.getCurrentLogPath();
    REQUIRE(!activeLogPath.empty());
    REQUIRE(std::filesystem::exists(activeLogPath));

    auto oldInactiveLog = walDir / "wal_19990101_000000_1.log";
    {
        std::ofstream oldLog(oldInactiveLog.string(), std::ios::binary);
        REQUIRE(oldLog.good());
    }
    auto oldTimestamp = std::filesystem::file_time_type::clock::now() - std::chrono::hours(24);
    std::filesystem::last_write_time(oldInactiveLog, oldTimestamp);

    REQUIRE(manager.pruneLogs(std::chrono::hours(0)));

    CHECK(std::filesystem::exists(activeLogPath));
    CHECK_FALSE(std::filesystem::exists(oldInactiveLog));

    REQUIRE(manager.shutdown());
}

TEST_CASE("WALManager transaction lifecycle updates stats and enforces state",
          "[unit][wal][manager][transaction]") {
    TempDir temp;
    auto walDir = temp.path / "wal";

    WALManager::Config cfg;
    cfg.walDirectory = walDir;
    cfg.compressOldLogs = false;
    cfg.enableGroupCommit = false;
    cfg.syncInterval = 1;

    WALManager manager(cfg);
    REQUIRE(manager.initialize());

    auto txn = manager.beginTransaction();
    REQUIRE(txn);
    CHECK(txn->getState() == Transaction::State::Active);
    CHECK(manager.getStats().activeTransactions == 1);

    REQUIRE(txn->storeBlock(makeHash('a'), 123, 1));
    REQUIRE(txn->commit());
    CHECK(txn->getState() == Transaction::State::Committed);
    CHECK(manager.getStats().activeTransactions == 0);

    auto secondCommit = txn->commit();
    REQUIRE_FALSE(secondCommit);
    CHECK(secondCommit.error().code == ErrorCode::InvalidOperation);

    auto addAfterCommit = txn->deleteBlock(makeHash('b'));
    REQUIRE_FALSE(addAfterCommit);
    CHECK(addAfterCommit.error().code == ErrorCode::InvalidOperation);

    {
        auto autoRollbackTxn = manager.beginTransaction();
        REQUIRE(autoRollbackTxn);
        REQUIRE(autoRollbackTxn->updateReference(makeHash('c'), 2));
        CHECK(manager.getStats().activeTransactions == 1);
    }

    CHECK(manager.getStats().activeTransactions == 0);
    REQUIRE(manager.shutdown());
}
