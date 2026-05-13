// Unit tests for RefCountCheck — reference count consistency validation
#include <catch2/catch_test_macros.hpp>

#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/doctor/checks/ref_count_check.h>
#include <yams/cli/yams_cli.h>

#include <sqlite3.h>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>

#include "../../../common/test_helpers_catch2.h"

namespace fs = std::filesystem;
using namespace yams::cli::doctor;

namespace {

struct TestEnv {
    fs::path dataDir;
    yams::test::ScopedEnvVar dataEnv;
    yams::test::ScopedEnvVar homeEnv;

    explicit TestEnv()
        : dataDir(yams::test::make_temp_dir("yams_refcount_test_")),
          dataEnv("YAMS_DATA_DIR", dataDir.string()), homeEnv("HOME", dataDir.string()) {
        fs::create_directories(dataDir);
    }

    std::unique_ptr<yams::cli::YamsCLI> makeCli() { return std::make_unique<yams::cli::YamsCLI>(); }

    void createRefsDb() {
        auto storageDir = dataDir / "storage";
        fs::create_directories(storageDir);
        sqlite3* db = nullptr;
        sqlite3_open((storageDir / "refs.db").string().c_str(), &db);
        if (!db)
            return;

        auto exec = [&](const char* sql) { sqlite3_exec(db, sql, nullptr, nullptr, nullptr); };

        exec("CREATE TABLE block_references ("
             "block_hash TEXT PRIMARY KEY,"
             "ref_count INTEGER NOT NULL DEFAULT 0,"
             "block_size INTEGER NOT NULL,"
             "uncompressed_size INTEGER,"
             "created_at INTEGER NOT NULL,"
             "last_accessed INTEGER NOT NULL,"
             "metadata TEXT,"
             "CHECK (ref_count >= 0),"
             "CHECK (block_size > 0))");

        exec("CREATE TABLE ref_statistics ("
             "stat_name TEXT PRIMARY KEY,"
             "stat_value INTEGER NOT NULL,"
             "updated_at INTEGER NOT NULL)");

        sqlite3_close(db);
    }

    void insertBlock(sqlite3* db, const char* hash, int refCount, int blockSize) {
        sqlite3_stmt* stmt = nullptr;
        sqlite3_prepare_v2(db,
                           "INSERT INTO block_references (block_hash, ref_count, block_size, "
                           "created_at, last_accessed) VALUES (?, ?, ?, 0, 0)",
                           -1, &stmt, nullptr);
        if (!stmt)
            return;
        sqlite3_bind_text(stmt, 1, hash, -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt, 2, refCount);
        sqlite3_bind_int(stmt, 3, blockSize);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

    void upsertStat(sqlite3* db, const char* name, int64_t value) {
        sqlite3_stmt* stmt = nullptr;
        sqlite3_prepare_v2(db,
                           "INSERT OR REPLACE INTO ref_statistics (stat_name, stat_value, "
                           "updated_at) VALUES (?, ?, 0)",
                           -1, &stmt, nullptr);
        if (!stmt)
            return;
        sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
        sqlite3_bind_int64(stmt, 2, value);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
};

} // namespace

TEST_CASE("RefCountCheck - no refs DB has no issues", "[doctor][refcount]") {
    TestEnv env;
    auto cli = env.makeCli();
    DoctorContext ctx(cli.get());
    RefCountCheck check;
    auto result = check.execute(ctx);

    CHECK(result.totalBlocks == 0);
    CHECK(result.ok == true);
}

TEST_CASE("RefCountCheck - empty refs DB is clean", "[doctor][refcount]") {
    TestEnv env;
    env.createRefsDb();

    auto cli = env.makeCli();
    DoctorContext ctx(cli.get());
    RefCountCheck check;
    auto result = check.execute(ctx);

    CHECK(result.totalBlocks == 0);
    CHECK(result.totalReferences == 0);
    CHECK(result.unreferencedBlocks == 0);
    CHECK(result.statsDrift == false);
    CHECK(result.ok == true);
}

TEST_CASE("RefCountCheck - reports stats and ref counts", "[doctor][refcount]") {
    TestEnv env;
    env.createRefsDb();

    auto storageDir = env.dataDir / "storage";
    sqlite3* db = nullptr;
    sqlite3_open((storageDir / "refs.db").string().c_str(), &db);
    REQUIRE(db != nullptr);

    env.insertBlock(db, "aaa", 3, 1000);
    env.insertBlock(db, "bbb", 1, 2000);
    env.insertBlock(db, "ccc", 0, 500);
    env.upsertStat(db, "total_blocks", 3);
    env.upsertStat(db, "total_references", 4);
    env.upsertStat(db, "total_bytes", 3500);
    // total_uncompressed_bytes absent (blocks have NULL uncompressed_size)
    sqlite3_close(db);

    auto cli = env.makeCli();
    DoctorContext ctx(cli.get());
    RefCountCheck check;
    auto result = check.execute(ctx);

    CHECK(result.totalBlocks == 3);
    CHECK(result.totalReferences == 4);
    CHECK(result.totalBytes == 3500);
    CHECK(result.unreferencedBlocks == 1);
    CHECK(result.unreferencedBytes == 500);
    CHECK(result.negativeRefCounts == 0);
    CHECK(result.statsDrift == false);
    CHECK(result.ok == true);
}

TEST_CASE("RefCountCheck - detects stats drift", "[doctor][refcount]") {
    TestEnv env;
    env.createRefsDb();

    auto storageDir = env.dataDir / "storage";
    sqlite3* db = nullptr;
    sqlite3_open((storageDir / "refs.db").string().c_str(), &db);
    REQUIRE(db != nullptr);

    env.insertBlock(db, "aaa", 2, 500);
    env.insertBlock(db, "bbb", 1, 750);
    // Stats say 3 blocks but actually only 2
    env.upsertStat(db, "total_blocks", 3);
    env.upsertStat(db, "total_references", 5); // says 5 refs, actually 3
    env.upsertStat(db, "total_bytes", 1250);
    env.upsertStat(db, "total_uncompressed_bytes", 1250);
    sqlite3_close(db);

    auto cli = env.makeCli();
    DoctorContext ctx(cli.get());
    RefCountCheck check;
    auto result = check.execute(ctx);

    CHECK(result.totalBlocks == 2);
    CHECK(result.totalReferences == 3);
    CHECK(result.statsDrift == true);
    CHECK(result.driftDetails.size() ==
          3); // total_blocks + total_references + total_uncompressed_bytes
    CHECK(result.ok == false);
}

TEST_CASE("RefCountCheck - render produces output", "[doctor][refcount]") {
    TestEnv env;
    auto cli = env.makeCli();
    DoctorContext ctx(cli.get());
    RefCountCheck check;
    auto result = check.execute(ctx);

    std::ostringstream os;
    RefCountCheck::render(os, result);
    auto output = os.str();

    CHECK_FALSE(output.empty());
    CHECK(output.find("empty") != std::string::npos);
}

TEST_CASE("RefCountCheck - render with data", "[doctor][refcount]") {
    TestEnv env;
    env.createRefsDb();

    auto storageDir = env.dataDir / "storage";
    sqlite3* db = nullptr;
    sqlite3_open((storageDir / "refs.db").string().c_str(), &db);
    REQUIRE(db != nullptr);
    env.insertBlock(db, "aaa", 1, 1000);
    env.upsertStat(db, "total_blocks", 1);
    env.upsertStat(db, "total_references", 1);
    env.upsertStat(db, "total_bytes", 1000);
    sqlite3_close(db);

    auto cli = env.makeCli();
    DoctorContext ctx(cli.get());
    RefCountCheck check;
    auto result = check.execute(ctx);

    std::ostringstream os;
    RefCountCheck::render(os, result);
    auto output = os.str();

    CHECK_FALSE(output.empty());
    CHECK(output.find("Total blocks") != std::string::npos);
    CHECK(output.find("OK") != std::string::npos);
}
