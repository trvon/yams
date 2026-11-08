#include <catch2/catch_test_macros.hpp>

#include <sqlite3.h>
#include <yams/api/content_store.h>
#include <yams/api/content_store_builder.h>
#include <yams/storage/block_reference_repair.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
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
    std::optional<std::int64_t> referencedBytes() const {
        return scalar("SELECT COALESCE(SUM(block_size), 0) FROM block_references "
                      "WHERE ref_count > 0");
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

// Every scanned object lands in exactly one outcome bucket and one compression bucket.
void checkAccounting(const yams::storage::BlockRefsRepairResult& r) {
    CHECK(r.blocksScanned >= 1);
    CHECK(r.blocksUpdated + r.blocksSkipped + r.errors == r.blocksScanned);
    CHECK(r.compressedBlocks + r.uncompressedBlocks == r.blocksScanned);
    CHECK(r.totalDiskBytes > 0);
    CHECK(r.totalUncompressedBytes > 0);
}
} // namespace

// `yams repair --block-refs` rewrites refs.db sizes from the object files.
TEST_CASE("Block reference repair corrects drifted sizes and leaves correct rows alone",
          "[storage][repair][block-refs]") {
    BlockRefsStore s;
    const auto [hash, original, uncompressed] = s.anyBlock();
    REQUIRE(original == s.diskSize(hash));
    REQUIRE(uncompressed >= original); // compressed on disk

    SECTION("a clean store needs no updates") {
        auto result = yams::storage::repairBlockReferences(s.objects(), s.refs(),
                                                           /*dryRun=*/false);
        REQUIRE(result);
        CHECK(result.value().errors == 0);
        CHECK(result.value().blocksUpdated == 0);
        CHECK(result.value().blocksSkipped == result.value().blocksScanned);
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
        // ref_statistics totals are recomputed from the repaired rows.
        CHECK(s.statistic("total_bytes") == s.referencedBytes());

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
