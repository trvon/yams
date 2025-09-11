#include <filesystem>
#include <fstream>
#include <random>
#include <gtest/gtest.h>
#include <yams/wal/wal_entry.h>
#include <yams/wal/wal_manager.h>

using namespace yams;
using namespace yams::wal;
namespace fs = std::filesystem;

namespace {
fs::path make_temp_dir(const std::string& prefix) {
    auto base = fs::temp_directory_path();
    auto dir = base / (prefix + std::to_string(std::random_device{}()));
    fs::create_directories(dir);
    return dir;
}
} // namespace

TEST(WALDualReadTest, MixedV1V2LogsRecover) {
    auto walDir = make_temp_dir("yams_wal_dual_");

    WALManager wal({.walDirectory = walDir});
    ASSERT_TRUE(wal.initialize().has_value());

    // Create a simple v1 log by normal transaction
    {
        auto tx = wal.beginTransaction();
        ASSERT_NE(tx, nullptr);
        ASSERT_TRUE(tx->storeBlock("dual_v1_hash", 1234, 1).has_value());
        ASSERT_TRUE(tx->commit().has_value());
    }

    // Create a separate v2 log file manually with a small transaction
    fs::path v2log = walDir / "manual_v2.log";
    std::ofstream ofs(v2log, std::ios::binary);
    ASSERT_TRUE(ofs.good());

    // Build entries with version=2
    uint64_t txnId = 9999;
    {
        auto data = WALEntry::TransactionData::encode(txnId, 1);
        WALEntry e(WALEntry::OpType::BeginTransaction, 1, txnId, data);
        e.header.version = 2;
        e.updateChecksum();
        auto bytes = e.serialize();
        ofs.write(reinterpret_cast<const char*>(bytes.data()),
                  static_cast<std::streamsize>(bytes.size()));
    }
    {
        auto data = WALEntry::StoreBlockData::encode("dual_v2_hash", 5678, 1);
        WALEntry e(WALEntry::OpType::StoreBlock, 2, txnId, data);
        e.header.version = 2;
        e.updateChecksum();
        auto bytes = e.serialize();
        ofs.write(reinterpret_cast<const char*>(bytes.data()),
                  static_cast<std::streamsize>(bytes.size()));
    }
    {
        auto data = WALEntry::TransactionData::encode(txnId, 0);
        WALEntry e(WALEntry::OpType::CommitTransaction, 3, txnId, data);
        e.header.version = 2;
        e.updateChecksum();
        auto bytes = e.serialize();
        ofs.write(reinterpret_cast<const char*>(bytes.data()),
                  static_cast<std::streamsize>(bytes.size()));
    }
    ofs.close();

    // Shutdown and recover to exercise read path
    ASSERT_TRUE(wal.shutdown().has_value());

    WALManager wal2({.walDirectory = walDir});
    ASSERT_TRUE(wal2.initialize().has_value());

    size_t recovered = 0;
    auto rec = wal2.recover([&](const WALEntry& entry) -> Result<void> {
        (void)entry;
        recovered++;
        return {};
    });

    ASSERT_TRUE(rec.has_value());
    EXPECT_GT(recovered, 0u);

    ASSERT_TRUE(wal2.shutdown().has_value());

    fs::remove_all(walDir);
}

// Scaffolding for sequence-based ordering test.
// Note: Currently WALManager orders logs by mtime; once sequence-based
// ordering is implemented, enable and assert correct ordering.
TEST(WALDualReadTest, DISABLED_RecoverySequenceOrdering) {
    SUCCEED();
}
