#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <yams/core/types.h>
#include <yams/wal/wal_manager.h>
#include <yams/wal/wal_recovery.h>

using namespace yams;
using namespace yams::wal;

namespace {

struct TempDir {
    TempDir() {
        auto stamp = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        path = std::filesystem::temp_directory_path() / ("yams_wal_recovery_test_" + stamp);
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

TEST_CASE("recoverFromLogs validates callback and directory arguments", "[unit][wal][recovery]") {
    RecoveryOptions emptyOptions;
    auto noCallback = recoverFromLogs(emptyOptions, nullptr);
    REQUIRE_FALSE(noCallback);
    CHECK(noCallback.error().code == ErrorCode::InvalidArgument);

    TempDir temp;
    auto notDirectory = temp.path / "wal.log";
    {
        std::ofstream out(notDirectory.string(), std::ios::binary);
        REQUIRE(out.good());
        out << "x";
    }

    RecoveryOptions badPath;
    badPath.walDirectory = notDirectory;
    auto badPathResult = recoverFromLogs(badPath, [](const WALEntry&) { return Result<void>(); });
    REQUIRE_FALSE(badPathResult);
    CHECK(badPathResult.error().code == ErrorCode::InvalidPath);
}

TEST_CASE("recoverFromLogs can create missing directory and return empty stats",
          "[unit][wal][recovery]") {
    TempDir temp;
    auto missingDir = temp.path / "missing" / "wal";

    RecoveryOptions options;
    options.walDirectory = missingDir;
    options.createDirectoryIfMissing = true;
    options.managerConfig.compressOldLogs = false;

    auto result = recoverFromLogs(options, [](const WALEntry&) { return Result<void>(); });
    REQUIRE(result);
    CHECK(std::filesystem::exists(missingDir));
    CHECK(result.value().entriesProcessed == 0);
    CHECK(result.value().transactionsRecovered == 0);
    CHECK(result.value().transactionsRolledBack == 0);
    CHECK(result.value().errorsEncountered == 0);
}

TEST_CASE("recoverFromLogs replays committed operations and skips rolled back transaction",
          "[unit][wal][recovery]") {
    TempDir temp;
    auto walDir = temp.path / "wal";

    WALManager::Config cfg;
    cfg.walDirectory = walDir;
    cfg.compressOldLogs = false;
    cfg.enableGroupCommit = false;
    cfg.syncInterval = 1;

    WALManager manager(cfg);
    REQUIRE(manager.initialize());

    auto directPayload = WALEntry::DeleteBlockData::encode(makeHash('d'));
    WALEntry directEntry(WALEntry::OpType::DeleteBlock, 0, 0, directPayload);
    auto directWrite = manager.writeEntry(directEntry);
    REQUIRE(directWrite);

    auto committedTxn = manager.beginTransaction();
    REQUIRE(committedTxn);
    REQUIRE(committedTxn->storeBlock(makeHash('a'), 100, 1));
    REQUIRE(committedTxn->commit());

    auto rolledBackTxn = manager.beginTransaction();
    REQUIRE(rolledBackTxn);
    REQUIRE(rolledBackTxn->updateReference(makeHash('b'), 5));
    REQUIRE(rolledBackTxn->rollback());

    auto danglingBeginPayload = WALEntry::TransactionData::encode(777, 1);
    WALEntry danglingBegin(WALEntry::OpType::BeginTransaction, 0, 777, danglingBeginPayload);
    REQUIRE(manager.writeEntry(danglingBegin));

    auto danglingStorePayload = WALEntry::StoreBlockData::encode(makeHash('c'), 55, 1);
    WALEntry danglingStore(WALEntry::OpType::StoreBlock, 0, 777, danglingStorePayload);
    REQUIRE(manager.writeEntry(danglingStore));

    REQUIRE(manager.shutdown());

    std::vector<WALEntry::OpType> appliedOps;
    RecoveryOptions options;
    options.walDirectory = walDir;
    options.managerConfig = cfg;

    auto recovered = recoverFromLogs(options, [&](const WALEntry& entry) {
        appliedOps.push_back(entry.header.operation);
        return Result<void>();
    });

    REQUIRE(recovered);
    CHECK(recovered.value().entriesProcessed >= 7);
    CHECK(recovered.value().transactionsRecovered == 1);
    CHECK(recovered.value().transactionsRolledBack == 1);
    CHECK(recovered.value().errorsEncountered == 0);

    REQUIRE(appliedOps.size() == 2);
    CHECK(appliedOps[0] == WALEntry::OpType::DeleteBlock);
    CHECK(appliedOps[1] == WALEntry::OpType::StoreBlock);
}