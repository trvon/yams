#include <catch2/catch_test_macros.hpp>

#include <sqlite3.h>
#include <yams/api/content_store.h>
#include <yams/api/content_store_builder.h>
#include <yams/storage/block_reference_repair.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

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

    // {hash, block_size, uncompressed_size} of the n-th referenced block by hash.
    std::tuple<std::string, std::int64_t, std::int64_t> block(int n = 0) const {
        sqlite3* db = nullptr;
        REQUIRE(sqlite3_open(refs().string().c_str(), &db) == SQLITE_OK);
        sqlite3_stmt* st = nullptr;
        REQUIRE(sqlite3_prepare_v2(db,
                                   "SELECT block_hash, block_size, uncompressed_size FROM "
                                   "block_references WHERE ref_count > 0 ORDER BY block_hash "
                                   "LIMIT 1 OFFSET ?",
                                   -1, &st, nullptr) == SQLITE_OK);
        sqlite3_bind_int(st, 1, n);
        REQUIRE(sqlite3_step(st) == SQLITE_ROW);
        std::tuple<std::string, std::int64_t, std::int64_t> row{
            reinterpret_cast<const char*>(sqlite3_column_text(st, 0)), sqlite3_column_int64(st, 1),
            sqlite3_column_int64(st, 2)};
        sqlite3_finalize(st);
        sqlite3_close(db);
        return row;
    }
    // First column of a single-row query; nullopt when the value is NULL.
    std::optional<std::int64_t> scalar(const std::string& sql,
                                       const std::string& bind = std::string()) const {
        sqlite3* db = nullptr;
        REQUIRE(sqlite3_open(refs().string().c_str(), &db) == SQLITE_OK);
        sqlite3_stmt* st = nullptr;
        REQUIRE(sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) == SQLITE_OK);
        if (!bind.empty())
            sqlite3_bind_text(st, 1, bind.c_str(), -1, SQLITE_TRANSIENT);
        REQUIRE(sqlite3_step(st) == SQLITE_ROW);
        std::optional<std::int64_t> value;
        if (sqlite3_column_type(st, 0) != SQLITE_NULL)
            value = sqlite3_column_int64(st, 0);
        sqlite3_finalize(st);
        sqlite3_close(db);
        return value;
    }
    void exec(const std::string& sql) const {
        sqlite3* db = nullptr;
        REQUIRE(sqlite3_open(refs().string().c_str(), &db) == SQLITE_OK);
        REQUIRE(sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr) == SQLITE_OK);
        sqlite3_close(db);
    }
    // A block_references column for one block; nullopt when the column is NULL.
    std::optional<std::int64_t> column(const std::string& hash, const char* name) const {
        return scalar(std::string("SELECT ") + name + " FROM block_references WHERE block_hash = ?",
                      hash);
    }
    std::optional<std::int64_t> blockSize(const std::string& hash) const {
        return column(hash, "block_size");
    }
    std::optional<std::int64_t> uncompressedSize(const std::string& hash) const {
        return column(hash, "uncompressed_size");
    }
    std::optional<std::int64_t> statistic(const std::string& name) const {
        return scalar("SELECT stat_value FROM ref_statistics WHERE stat_name = ?", name);
    }
    std::int64_t rowsWithoutUncompressedSize() const {
        return scalar("SELECT COUNT(*) FROM block_references WHERE uncompressed_size IS NULL")
            .value_or(-1);
    }
    void setBlockSize(const std::string& hash, std::int64_t size) const {
        exec("UPDATE block_references SET block_size = " + std::to_string(size) +
             ", uncompressed_size = NULL WHERE block_hash = '" + hash + "'");
    }
    std::int64_t diskSize(const std::string& hash) const {
        return static_cast<std::int64_t>(
            std::filesystem::file_size(objects() / hash.substr(0, 2) / hash.substr(2)));
    }

    // ref_statistics must equal the refs.db schema backfill / trigger definitions
    // (reference_counter.cpp): totals over every row, unreferenced counters over ref_count = 0.
    void checkStatisticsMatchTriggers() const {
        CHECK(statistic("total_blocks") == scalar("SELECT COUNT(*) FROM block_references"));
        CHECK(statistic("total_references") ==
              scalar("SELECT IFNULL(SUM(ref_count),0) FROM block_references"));
        CHECK(statistic("total_bytes") ==
              scalar("SELECT IFNULL(SUM(block_size),0) FROM block_references"));
        CHECK(statistic("total_uncompressed_bytes") ==
              scalar("SELECT IFNULL(SUM(COALESCE(uncompressed_size, block_size)),0) FROM "
                     "block_references"));
        CHECK(statistic("unreferenced_blocks") ==
              scalar("SELECT COUNT(*) FROM block_references WHERE ref_count=0"));
        CHECK(statistic("unreferenced_bytes") ==
              scalar("SELECT IFNULL(SUM(block_size),0) FROM block_references WHERE ref_count=0"));
    }
};

// Every scanned object lands in exactly one outcome bucket and one compression bucket.
void checkAccounting(const yams::storage::BlockRefsRepairResult& r) {
    CHECK(r.blocksScanned >= 1);
    CHECK(r.blocksUpdated + r.blocksSkipped + r.errors == r.blocksScanned);
    CHECK(r.compressedBlocks + r.uncompressedBlocks == r.blocksScanned);
    CHECK(r.totalDiskBytes > 0);
    CHECK(r.totalUncompressedBytes > 0);
}

void writeFile(const std::filesystem::path& path) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream(path, std::ios::binary) << "not an object";
}
} // namespace

// `yams repair --block-refs` rewrites refs.db sizes from the object files.
TEST_CASE("Block reference repair corrects drifted sizes and leaves correct rows alone",
          "[storage][repair][block-refs]") {
    BlockRefsStore s;
    const auto [hash, original, uncompressed] = s.block();
    REQUIRE(original == s.diskSize(hash));
    REQUIRE(uncompressed >= original); // compressed on disk

    SECTION("a clean store needs no updates") {
        auto result = yams::storage::repairBlockReferences(s.objects(), s.refs(),
                                                           /*dryRun=*/false);
        REQUIRE(result);
        CHECK(result.value().errors == 0);
        CHECK(result.value().blocksUpdated == 0);
        CHECK(result.value().blocksSkipped == result.value().blocksScanned);
        CHECK(result.value().entriesIgnored == 0);
        CHECK(result.value().compressedBlocks >= 1);
        checkAccounting(result.value());
        CHECK(s.blockSize(hash) == original);
        CHECK(s.uncompressedSize(hash) == uncompressed);
    }
    SECTION("a dry run reports drift without writing") {
        s.setBlockSize(hash, original + 1);
        auto result = yams::storage::repairBlockReferences(s.objects(), s.refs(),
                                                           /*dryRun=*/true);
        REQUIRE(result);
        CHECK(result.value().errors == 0);
        CHECK(result.value().blocksUpdated == 1);
        checkAccounting(result.value());
        CHECK(s.blockSize(hash) == original + 1);
        CHECK_FALSE(s.uncompressedSize(hash).has_value());
    }
    SECTION("a real run restores the on-disk size") {
        s.setBlockSize(hash, original + 1);
        auto result = yams::storage::repairBlockReferences(s.objects(), s.refs(),
                                                           /*dryRun=*/false);
        REQUIRE(result);
        CHECK(result.value().errors == 0);
        CHECK(result.value().blocksUpdated == 1);
        checkAccounting(result.value());
        CHECK(s.blockSize(hash) == original);
        CHECK(s.uncompressedSize(hash) == uncompressed);
        s.checkStatisticsMatchTriggers();

        SECTION("a second run finds nothing left to repair") {
            auto again = yams::storage::repairBlockReferences(s.objects(), s.refs(),
                                                              /*dryRun=*/false);
            REQUIRE(again);
            CHECK(again.value().blocksUpdated == 0);
            CHECK(again.value().errors == 0);
        }
    }
    SECTION("missing paths are reported as NotFound") {
        auto noObjects =
            yams::storage::repairBlockReferences(s.root / "absent", s.refs(), /*dryRun=*/true);
        REQUIRE_FALSE(noObjects);
        CHECK(noObjects.error().code == yams::ErrorCode::NotFound);
        auto noRefs = yams::storage::repairBlockReferences(s.objects(), s.root / "absent.db", true);
        REQUIRE_FALSE(noRefs);
        CHECK(noRefs.error().code == yams::ErrorCode::NotFound);
    }
}

// ref_statistics totals cover unreferenced rows too; a repair must not narrow them to
// ref_count > 0 the way it once did.
TEST_CASE("Block reference repair keeps ref_statistics consistent with the schema triggers",
          "[storage][repair][block-refs]") {
    BlockRefsStore s;
    const auto [referenced, referencedSize, referencedUncompressed] = s.block(0);
    const auto [unreferenced, unreferencedSize, unreferencedUncompressed] = s.block(1);
    REQUIRE(referenced != unreferenced);

    // The ref_count update and the insert go through the triggers, so the counters are exact
    // before the drift below, which (like the real-world drift) bypasses them.
    s.exec("UPDATE block_references SET ref_count = 0 WHERE block_hash = '" + unreferenced + "'");
    s.exec("INSERT INTO block_references (block_hash, ref_count, block_size, uncompressed_size, "
           "created_at, last_accessed) VALUES ('" +
           std::string(64, 'f') + "', 0, 123, 456, 0, 0)");
    s.setBlockSize(referenced, referencedSize + 7);
    s.setBlockSize(unreferenced, unreferencedSize + 11);

    auto result = yams::storage::repairBlockReferences(s.objects(), s.refs(), /*dryRun=*/false);
    REQUIRE(result);
    CHECK(result.value().errors == 0);
    CHECK(result.value().blocksUpdated == 2);
    CHECK(s.blockSize(referenced) == referencedSize);
    CHECK(s.blockSize(unreferenced) == unreferencedSize);
    CHECK(s.uncompressedSize(unreferenced) == unreferencedUncompressed);
    // The row with no object file is left as it is, but still counted.
    CHECK(s.blockSize(std::string(64, 'f')) == 123);
    CHECK(s.statistic("unreferenced_blocks") == 2);
    s.checkStatisticsMatchTriggers();
}

TEST_CASE("Block reference repair aborts cleanly when refs.db stays locked",
          "[storage][repair][block-refs]") {
    BlockRefsStore s;
    const auto [hash, original, uncompressed] = s.block();
    s.setBlockSize(hash, original + 1);

    sqlite3* holder = nullptr;
    REQUIRE(sqlite3_open(s.refs().string().c_str(), &holder) == SQLITE_OK);
    REQUIRE(sqlite3_exec(holder, "BEGIN IMMEDIATE;", nullptr, nullptr, nullptr) == SQLITE_OK);

    yams::storage::BlockRefsRepairOptions options;
    options.busyTimeout = std::chrono::milliseconds{50};
    auto result = yams::storage::repairBlockReferences(s.objects(), s.refs(), options);

    sqlite3_exec(holder, "ROLLBACK;", nullptr, nullptr, nullptr);
    sqlite3_close(holder);

    REQUIRE_FALSE(result);
    CHECK(result.error().code == yams::ErrorCode::DatabaseError);
    // Nothing was written: the drift is still there and a later run can fix it.
    CHECK(s.blockSize(hash) == original + 1);
    CHECK_FALSE(s.uncompressedSize(hash).has_value());
}

TEST_CASE("Block reference repair stops between batches when asked to",
          "[storage][repair][block-refs]") {
    BlockRefsStore s;
    // Drift every row so each scanned object needs an update.
    s.exec("UPDATE block_references SET block_size = block_size + 1, uncompressed_size = NULL");
    const auto driftedBefore = s.rowsWithoutUncompressedSize();
    REQUIRE(driftedBefore >= 2);

    int checks = 0;
    yams::storage::BlockRefsRepairOptions options;
    options.batchSize = 1;
    options.shouldContinue = [&checks]() {
        ++checks;
        return false;
    };
    auto result = yams::storage::repairBlockReferences(s.objects(), s.refs(), options);

    REQUIRE_FALSE(result);
    CHECK(result.error().code == yams::ErrorCode::OperationCancelled);
    CHECK(checks == 1);
    // The first batch (one object) was committed before stopping; the rest are untouched.
    CHECK(s.rowsWithoutUncompressedSize() == driftedBefore - 1);
    s.checkStatisticsMatchTriggers();
}

TEST_CASE("Block reference repair only scans storage-key object files",
          "[storage][repair][block-refs]") {
    BlockRefsStore s;
    auto baseline = yams::storage::repairBlockReferences(s.objects(), s.refs(), /*dryRun=*/true);
    REQUIRE(baseline);

    const auto [hash, original, uncompressed] = s.block();
    writeFile(s.objects() / hash.substr(0, 2) / "notahash.tmp");          // stray name in a shard
    writeFile(s.objects() / "temp" / std::string(62, 'a'));               // temp-style directory
    writeFile(s.objects() / "stray.txt");                                 // file at the shard level
    writeFile(s.objects() / "ab" / (std::string(62, 'c') + ".manifest")); // not a block

    auto result = yams::storage::repairBlockReferences(s.objects(), s.refs(), /*dryRun=*/true);
    REQUIRE(result);
    CHECK(result.value().errors == 0);
    CHECK(result.value().blocksScanned == baseline.value().blocksScanned);
    CHECK(result.value().entriesIgnored == baseline.value().entriesIgnored + 4);
}
