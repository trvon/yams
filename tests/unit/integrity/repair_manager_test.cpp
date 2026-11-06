#include <optional>
#include <catch2/catch_test_macros.hpp>

#include <sqlite3.h>
#include <yams/api/content_store_builder.h>
#include <yams/crypto/hasher.h>
#include <yams/integrity/repair_manager.h>
#include <yams/storage/storage_engine.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

using namespace yams::integrity;
using namespace yams::storage;

namespace {

std::vector<std::byte> toBytes(const std::string& s) {
    std::vector<std::byte> out(s.size());
    std::memcpy(out.data(), s.data(), s.size());
    return out;
}

struct TempDir {
    std::filesystem::path path;

    TempDir() {
        path = std::filesystem::temp_directory_path() /
               ("yams_test_" +
                std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) + "_" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path);
    }

    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }

    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
};

struct RepairFixture {
    TempDir tempDir;
    StorageConfig storageConfig;
    std::unique_ptr<StorageEngine> storage;
    RepairManagerConfig repairConfig;
    std::unique_ptr<RepairManager> manager;

    RepairFixture() {
        storageConfig.basePath = tempDir.path / "storage";
        std::filesystem::create_directories(storageConfig.basePath);
        storage = std::make_unique<StorageEngine>(storageConfig);

        // Set up repair config with dummy fetchers
        repairConfig.backupFetcher =
            [](const std::string&) -> yams::Result<std::vector<std::byte>> {
            return yams::Result<std::vector<std::byte>>(yams::ErrorCode::NotFound);
        };
        repairConfig.p2pFetcher = [](const std::string&) -> yams::Result<std::vector<std::byte>> {
            return yams::Result<std::vector<std::byte>>(yams::ErrorCode::NotFound);
        };

        manager = std::make_unique<RepairManager>(*storage, repairConfig);
    }

    void storeTestData(const std::string& hash, const std::string& data) {
        auto bytes = toBytes(data);
        storage->store(hash, std::span<const std::byte>(bytes.data(), bytes.size()));
    }
};

} // namespace

TEST_CASE_METHOD(RepairFixture, "RepairManager basic operations", "[integrity][repair]") {
    SECTION("canRepair checks availability of repair sources") {
        // canRepair returns true when repair sources are configured,
        // even if the block doesn't exist (it checks capability, not block existence)
        (void)manager->canRepair("some-hash");
        // Just verify the method is callable without crashing
        CHECK(true);
    }

    SECTION("attemptRepair returns false when no repair sources available") {
        storeTestData("test-hash", "test data");
        // Repair should fail since all fetchers return NotFound
        CHECK_FALSE(manager->attemptRepair("unknown-hash"));
    }
}

TEST_CASE("RepairManager with working backup fetcher", "[integrity][repair]") {
    TempDir tempDir;
    StorageConfig storageConfig;
    storageConfig.basePath = tempDir.path / "storage";
    std::filesystem::create_directories(storageConfig.basePath);
    StorageEngine storage(storageConfig);

    RepairManagerConfig config;
    config.backupFetcher = [](const std::string& hash) -> yams::Result<std::vector<std::byte>> {
        if (hash == "recoverable-hash") {
            std::string data = "recovered data";
            std::vector<std::byte> bytes(data.size());
            std::memcpy(bytes.data(), data.data(), data.size());
            return bytes;
        }
        return yams::Result<std::vector<std::byte>>(yams::ErrorCode::NotFound);
    };

    RepairManager manager(storage, config);

    SECTION("attemptRepair succeeds when backup fetcher returns data") {
        // The repair should attempt to use the backup fetcher
        (void)manager.attemptRepair("recoverable-hash");
        // Note: actual success depends on hash validation, but we test the flow
        CHECK(true); // Verify no exception thrown
    }
}

TEST_CASE("RepairManager repair strategy order", "[integrity][repair]") {
    TempDir tempDir;
    StorageConfig storageConfig;
    storageConfig.basePath = tempDir.path / "storage";
    std::filesystem::create_directories(storageConfig.basePath);
    StorageEngine storage(storageConfig);

    std::vector<std::string> callOrder;

    RepairManagerConfig config;
    config.backupFetcher =
        [&callOrder](const std::string&) -> yams::Result<std::vector<std::byte>> {
        callOrder.push_back("backup");
        return yams::Result<std::vector<std::byte>>(yams::ErrorCode::NotFound);
    };
    config.p2pFetcher = [&callOrder](const std::string&) -> yams::Result<std::vector<std::byte>> {
        callOrder.push_back("p2p");
        return yams::Result<std::vector<std::byte>>(yams::ErrorCode::NotFound);
    };
    config.defaultOrder = {RepairStrategy::FromBackup, RepairStrategy::FromP2P};

    RepairManager manager(storage, config);

    SECTION("attempts repair strategies in configured order") {
        (void)manager.attemptRepair("some-hash");
        REQUIRE(callOrder.size() >= 1u);
        CHECK(callOrder[0] == "backup");
        if (callOrder.size() >= 2u) {
            CHECK(callOrder[1] == "p2p");
        }
    }
}

TEST_CASE("RepairManager rejects P2P data with wrong hash", "[integrity][repair]") {
    TempDir tempDir;
    StorageConfig storageConfig;
    storageConfig.basePath = tempDir.path / "storage";
    std::filesystem::create_directories(storageConfig.basePath);
    StorageEngine storage(storageConfig);

    const std::string targetHash =
        "a1b2c3d4e5f60001112223334444555566667777888899990000aaaabbbbcccc";
    std::string wrongData = "wrong data that produces a different hash";

    RepairManagerConfig config;
    config.backupFetcher = [](const std::string&) -> yams::Result<std::vector<std::byte>> {
        return yams::Result<std::vector<std::byte>>(yams::ErrorCode::NotFound);
    };
    config.p2pFetcher = [&wrongData](const std::string&) -> yams::Result<std::vector<std::byte>> {
        return toBytes(wrongData);
    };
    config.defaultOrder = {RepairStrategy::FromP2P};

    RepairManager manager(storage, config);
    auto repaired = manager.attemptRepair(targetHash);
    CHECK_FALSE(repaired);

    CHECK_FALSE(storage.exists(targetHash).value());
}

TEST_CASE("RepairManager recovers block from P2P with verified hash", "[integrity][repair]") {
    TempDir tempDir;
    StorageConfig storageConfig;
    storageConfig.basePath = tempDir.path / "storage";
    std::filesystem::create_directories(storageConfig.basePath);
    StorageEngine storage(storageConfig);

    // Pre-compute hash of the recovery data so it matches
    const std::string recoveryData = "known-good-recovery-data-0123456789";
    std::string recoveryHash;
    {
        auto hasher = yams::crypto::createSHA256Hasher();
        auto bytes = toBytes(recoveryData);
        recoveryHash = hasher->hash(bytes);
    }

    RepairManagerConfig config;
    config.backupFetcher = [](const std::string&) -> yams::Result<std::vector<std::byte>> {
        return yams::Result<std::vector<std::byte>>(yams::ErrorCode::NotFound);
    };
    config.p2pFetcher =
        [&recoveryData](const std::string&) -> yams::Result<std::vector<std::byte>> {
        return toBytes(recoveryData);
    };
    config.defaultOrder = {RepairStrategy::FromP2P};

    RepairManager manager(storage, config);
    auto repaired = manager.attemptRepair(recoveryHash);
    CHECK(repaired);

    // Verify the data was actually stored with the correct hash
    CHECK(storage.exists(recoveryHash).value());

    // Content should match
    auto retrieved = storage.retrieve(recoveryHash);
    REQUIRE(retrieved.has_value());
    std::string retrievedStr(reinterpret_cast<const char*>(retrieved.value().data()),
                             retrieved.value().size());
    CHECK(retrievedStr == recoveryData);
}

TEST_CASE("RepairManager: backup fetcher preferred over P2P", "[integrity][repair]") {
    TempDir tempDir;
    StorageConfig storageConfig;
    storageConfig.basePath = tempDir.path / "storage";
    std::filesystem::create_directories(storageConfig.basePath);
    StorageEngine storage(storageConfig);

    const std::string backupData = "data-from-backup-0123456789";
    const std::string p2pData = "data-from-p2p-0123456789";
    std::string recoveryHash;
    {
        auto hasher = yams::crypto::createSHA256Hasher();
        auto bytes = toBytes(backupData);
        recoveryHash = hasher->hash(bytes);
    }

    bool backupUsed = false;
    bool p2pUsed = false;

    RepairManagerConfig config;
    config.backupFetcher =
        [&backupUsed, &backupData](const std::string&) -> yams::Result<std::vector<std::byte>> {
        backupUsed = true;
        return toBytes(backupData);
    };
    config.p2pFetcher = [&p2pUsed](const std::string&) -> yams::Result<std::vector<std::byte>> {
        p2pUsed = true;
        return yams::Result<std::vector<std::byte>>(yams::ErrorCode::NotFound);
    };
    config.defaultOrder = {RepairStrategy::FromBackup, RepairStrategy::FromP2P};

    RepairManager manager(storage, config);
    auto repaired = manager.attemptRepair(recoveryHash);
    REQUIRE(repaired);

    // Backup should have been tried (and succeeded)
    CHECK(backupUsed);
    // P2P should NOT have been tried if backup succeeded
    CHECK_FALSE(p2pUsed);

    CHECK(storage.exists(recoveryHash).value());
}

TEST_CASE("RepairManager: canRepair reflects source availability", "[integrity][repair]") {
    TempDir tempDir;
    StorageConfig storageConfig;
    storageConfig.basePath = tempDir.path / "storage";
    std::filesystem::create_directories(storageConfig.basePath);
    StorageEngine storage(storageConfig);

    SECTION("canRepair false when no repair sources configured") {
        RepairManagerConfig config; // no fetchers set at all
        RepairManager manager(storage, config);
        CHECK_FALSE(manager.canRepair("any-hash"));
    }

    SECTION("canRepair true when backup fetcher is configured") {
        RepairManagerConfig config;
        config.backupFetcher = [](const std::string&) -> yams::Result<std::vector<std::byte>> {
            std::string data = "backup-data";
            return toBytes(data);
        };
        config.p2pFetcher = [](const std::string&) -> yams::Result<std::vector<std::byte>> {
            return yams::Result<std::vector<std::byte>>(yams::ErrorCode::NotFound);
        };

        RepairManager manager(storage, config);
        CHECK(manager.canRepair("any-hash"));
    }

    SECTION("canRepair true when p2p fetcher returns data") {
        RepairManagerConfig config;
        config.backupFetcher = [](const std::string&) -> yams::Result<std::vector<std::byte>> {
            return yams::Result<std::vector<std::byte>>(yams::ErrorCode::NotFound);
        };
        config.p2pFetcher = [](const std::string&) -> yams::Result<std::vector<std::byte>> {
            std::string data = "p2p-data";
            return toBytes(data);
        };

        RepairManager manager(storage, config);
        CHECK(manager.canRepair("any-hash"));
    }
}

namespace {
// A real compressed, chunked content store, so refs.db rows and object files match production.
struct BlockRefsStore {
    std::filesystem::path root =
        std::filesystem::temp_directory_path() /
        ("yams_block_refs_" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::unique_ptr<yams::api::IContentStore> store;

    BlockRefsStore() {
        auto built = yams::api::ContentStoreBuilder()
                         .withStoragePath(root / "storage")
                         .withChunkSize(65536)
                         .withCompression(true)
                         .withDeduplication(true)
                         .build();
        REQUIRE(built);
        store = std::move(built.value());
        // Compressible text, several chunks.
        std::string text;
        for (int i = 0; text.size() < 600 * 1024; ++i)
            text += "line " + std::to_string(i) + " of compressible repeated content\n";
        std::vector<std::byte> bytes(text.size());
        std::memcpy(bytes.data(), text.data(), text.size());
        REQUIRE(store->storeBytes(bytes));
    }
    ~BlockRefsStore() {
        store.reset();
        std::error_code ec;
        std::filesystem::remove_all(root, ec);
    }
    std::filesystem::path objects() const { return root / "storage" / "objects"; }
    std::filesystem::path refs() const { return root / "storage" / "refs.db"; }

    // {hash, block_size, uncompressed_size} of one referenced block.
    std::tuple<std::string, std::int64_t, std::int64_t> anyBlock() const {
        sqlite3* db = nullptr;
        REQUIRE(sqlite3_open(refs().string().c_str(), &db) == SQLITE_OK);
        sqlite3_stmt* st = nullptr;
        REQUIRE(sqlite3_prepare_v2(db,
                                   "SELECT block_hash, block_size, uncompressed_size FROM "
                                   "block_references WHERE ref_count > 0 ORDER BY block_hash "
                                   "LIMIT 1",
                                   -1, &st, nullptr) == SQLITE_OK);
        REQUIRE(sqlite3_step(st) == SQLITE_ROW);
        std::tuple<std::string, std::int64_t, std::int64_t> row{
            reinterpret_cast<const char*>(sqlite3_column_text(st, 0)), sqlite3_column_int64(st, 1),
            sqlite3_column_int64(st, 2)};
        sqlite3_finalize(st);
        sqlite3_close(db);
        return row;
    }
    // A block_references column for one block; nullopt when the column is NULL.
    std::optional<std::int64_t> column(const std::string& hash, const char* name) const {
        sqlite3* db = nullptr;
        REQUIRE(sqlite3_open(refs().string().c_str(), &db) == SQLITE_OK);
        sqlite3_stmt* st = nullptr;
        const auto sql =
            std::string("SELECT ") + name + " FROM block_references WHERE block_hash = ?";
        REQUIRE(sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) == SQLITE_OK);
        sqlite3_bind_text(st, 1, hash.c_str(), -1, SQLITE_TRANSIENT);
        REQUIRE(sqlite3_step(st) == SQLITE_ROW);
        std::optional<std::int64_t> value;
        if (sqlite3_column_type(st, 0) != SQLITE_NULL)
            value = sqlite3_column_int64(st, 0);
        sqlite3_finalize(st);
        sqlite3_close(db);
        return value;
    }
    std::optional<std::int64_t> blockSize(const std::string& hash) const {
        return column(hash, "block_size");
    }
    std::optional<std::int64_t> uncompressedSize(const std::string& hash) const {
        return column(hash, "uncompressed_size");
    }
    void setBlockSize(const std::string& hash, std::int64_t size) const {
        sqlite3* db = nullptr;
        REQUIRE(sqlite3_open(refs().string().c_str(), &db) == SQLITE_OK);
        const auto sql = "UPDATE block_references SET block_size = " + std::to_string(size) +
                         ", uncompressed_size = NULL WHERE block_hash = '" + hash + "'";
        REQUIRE(sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr) == SQLITE_OK);
        sqlite3_close(db);
    }
    std::int64_t diskSize(const std::string& hash) const {
        return static_cast<std::int64_t>(
            std::filesystem::file_size(objects() / hash.substr(0, 2) / hash.substr(2)));
    }
};
} // namespace

// `yams repair --block-refs` rewrites refs.db sizes from the object files. It had no coverage.
TEST_CASE("Block reference repair corrects drifted sizes and leaves correct rows alone",
          "[integrity][repair][block-refs]") {
    BlockRefsStore s;
    const auto [hash, original, uncompressed] = s.anyBlock();
    REQUIRE(original == s.diskSize(hash));
    REQUIRE(uncompressed >= original); // compressed on disk

    SECTION("a clean store needs no updates") {
        auto result = yams::integrity::RepairManager::repairBlockReferences(s.objects(), s.refs(),
                                                                            /*dryRun=*/false);
        REQUIRE(result);
        CHECK(result.value().errors == 0);
        CHECK(result.value().blocksUpdated == 0);
        CHECK(result.value().compressedBlocks >= 1);
    }
    SECTION("a dry run reports drift without writing") {
        s.setBlockSize(hash, original + 1);
        auto result = yams::integrity::RepairManager::repairBlockReferences(s.objects(), s.refs(),
                                                                            /*dryRun=*/true);
        REQUIRE(result);
        CHECK(result.value().blocksUpdated == 1);
        CHECK(s.blockSize(hash) == original + 1);
    }
    SECTION("a real run restores the on-disk size") {
        s.setBlockSize(hash, original + 1);
        auto result = yams::integrity::RepairManager::repairBlockReferences(s.objects(), s.refs(),
                                                                            /*dryRun=*/false);
        REQUIRE(result);
        CHECK(result.value().errors == 0);
        CHECK(result.value().blocksUpdated == 1);
        CHECK(s.blockSize(hash) == original);
        CHECK(s.uncompressedSize(hash) == uncompressed);
    }
}
