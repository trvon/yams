#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/db_salvage.h>
#include <yams/metadata/database.h>

#include <sqlite3.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

namespace fs = std::filesystem;

namespace {

fs::path makeScratchDir(const std::string& prefix) {
    auto base = fs::temp_directory_path() /
                (prefix + "_" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::create_directories(base);
    return base;
}

const char* kDocumentsTableDdl = R"(
    CREATE TABLE documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_path TEXT NOT NULL,
        file_name TEXT NOT NULL,
        file_extension TEXT,
        file_size INTEGER NOT NULL,
        sha256_hash TEXT UNIQUE NOT NULL,
        mime_type TEXT,
        created_time INTEGER,
        modified_time INTEGER,
        indexed_time INTEGER,
        content_extracted BOOLEAN DEFAULT 0,
        extraction_status TEXT DEFAULT 'pending',
        extraction_error TEXT,
        path_prefix TEXT,
        reverse_path TEXT,
        path_hash TEXT,
        parent_hash TEXT,
        path_depth INTEGER DEFAULT 0,
        repair_status TEXT DEFAULT 'pending',
        repair_attempted_at INTEGER,
        repair_attempts INTEGER DEFAULT 0
    )
)";

const char* kDocumentsInsertSql = R"(
    INSERT INTO documents (
        file_path, file_name, file_extension, file_size, sha256_hash,
        mime_type, created_time, modified_time, indexed_time, content_extracted,
        extraction_status, extraction_error, path_prefix, reverse_path, path_hash,
        parent_hash, path_depth, repair_status, repair_attempted_at, repair_attempts
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
)";

void createAndPopulateDb(const fs::path& dbPath, int docCount,
                         const std::vector<std::string>& hashes) {
    yams::metadata::Database db;
    auto open = db.open(dbPath.string(), yams::metadata::ConnectionMode::Create);
    REQUIRE(open);
    REQUIRE(db.execute(kDocumentsTableDdl));

    for (int i = 0; i < docCount; ++i) {
        const auto& hash = hashes[static_cast<size_t>(i)];
        auto stmtResult = db.prepare(kDocumentsInsertSql);
        REQUIRE(stmtResult);
        auto stmt = std::move(stmtResult).value();

        std::string filePath = "/test/file_" + std::to_string(i) + ".txt";
        std::string fileName = "file_" + std::to_string(i) + ".txt";
        std::string ext = "txt";
        int64_t fileSize = static_cast<int64_t>(1024) * (i + 1);

        REQUIRE(stmt.bind(1, filePath));
        REQUIRE(stmt.bind(2, fileName));
        REQUIRE(stmt.bind(3, ext));
        REQUIRE(stmt.bind(4, fileSize));
        REQUIRE(stmt.bind(5, hash));
        REQUIRE(stmt.bind(6, "text/plain"));
        REQUIRE(stmt.bind(7, static_cast<int64_t>(1000 + i)));
        REQUIRE(stmt.bind(8, static_cast<int64_t>(2000 + i)));
        REQUIRE(stmt.bind(9, static_cast<int64_t>(3000 + i)));
        REQUIRE(stmt.bind(10, 0));
        REQUIRE(stmt.bind(11, std::string("pending")));
        REQUIRE(stmt.bind(12, std::string("")));
        REQUIRE(stmt.bind(13, std::string("/test")));
        REQUIRE(stmt.bind(14, std::string("txt.0_elif/tset/")));
        REQUIRE(stmt.bind(15, std::string("path_hash_") + std::to_string(i)));
        REQUIRE(stmt.bind(16, std::string("parent_hash_") + std::to_string(i)));
        REQUIRE(stmt.bind(17, static_cast<int64_t>(1)));
        REQUIRE(stmt.bind(18, std::string("pending")));
        REQUIRE(stmt.bind(19, static_cast<int64_t>(0)));
        REQUIRE(stmt.bind(20, static_cast<int64_t>(0)));

        auto execResult = stmt.execute();
        REQUIRE(execResult);
    }

    db.close();
}

std::string getJournalMode(const fs::path& dbPath) {
    sqlite3* raw = nullptr;
    int rc = sqlite3_open_v2(dbPath.string().c_str(), &raw, SQLITE_OPEN_READWRITE, nullptr);
    REQUIRE((rc == SQLITE_OK));

    sqlite3_stmt* stmt = nullptr;
    rc = sqlite3_prepare_v2(raw, "PRAGMA journal_mode", -1, &stmt, nullptr);
    REQUIRE((rc == SQLITE_OK));

    std::string mode;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const auto* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        if (text != nullptr) {
            mode = text;
        }
    }
    sqlite3_finalize(stmt);
    sqlite3_close(raw);
    return mode;
}

int countDocuments(const fs::path& dbPath) {
    sqlite3* raw = nullptr;
    int rc = sqlite3_open_v2(dbPath.string().c_str(), &raw, SQLITE_OPEN_READONLY, nullptr);
    REQUIRE((rc == SQLITE_OK));

    sqlite3_stmt* stmt = nullptr;
    rc = sqlite3_prepare_v2(raw, "SELECT COUNT(*) FROM documents", -1, &stmt, nullptr);
    REQUIRE((rc == SQLITE_OK));

    int count = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    sqlite3_close(raw);
    return count;
}

std::vector<std::string> makeTestHashes(int count) {
    std::vector<std::string> hashes;
    hashes.reserve(static_cast<size_t>(count));
    for (int i = 0; i < count; ++i) {
        std::ostringstream os;
        os << std::hex << std::setfill('0') << std::setw(64) << (1000000 + i);
        hashes.push_back(os.str());
    }
    return hashes;
}

} // namespace

TEST_CASE("salvageFromCorruptDb returns empty result when corrupt DB is missing",
          "[unit][daemon][db_salvage]") {
    auto dir = makeScratchDir("yams_salvage_missing");
    auto freshPath = dir / "fresh.db";

    {
        yams::metadata::Database db;
        REQUIRE(db.open(freshPath.string(), yams::metadata::ConnectionMode::Create));
        REQUIRE(db.execute(kDocumentsTableDdl));
        db.close();
    }

    auto corruptPath = dir / "nonexistent.db";
    auto res = yams::daemon::salvageFromCorruptDb(corruptPath, freshPath);
    REQUIRE(res);
    REQUIRE((res.value().documentsSalvaged == 0));
    REQUIRE_FALSE(res.value().diagnostics.empty());

    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_CASE("salvageFromCorruptDb copies documents via ATTACH", "[unit][daemon][db_salvage]") {
    auto dir = makeScratchDir("yams_salvage_attach");
    auto corruptPath = dir / "corrupt.db";
    auto freshPath = dir / "fresh.db";
    auto hashes = makeTestHashes(5);

    createAndPopulateDb(corruptPath, 5, hashes);
    {
        yams::metadata::Database db;
        REQUIRE(db.open(freshPath.string(), yams::metadata::ConnectionMode::Create));
        REQUIRE(db.execute(kDocumentsTableDdl));
        db.close();
    }

    auto res = yams::daemon::salvageFromCorruptDb(corruptPath, freshPath);
    REQUIRE(res);
    REQUIRE((res.value().documentsSalvaged == 5));
    REQUIRE((res.value().documentsFailed == 0));

    REQUIRE((countDocuments(freshPath) == 5));

    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_CASE("salvageFromCorruptDb preserves fresh DB journaling mode", "[unit][daemon][db_salvage]") {
    auto dir = makeScratchDir("yams_salvage_journal");
    auto corruptPath = dir / "corrupt.db";
    auto freshPath = dir / "fresh.db";
    auto hashes = makeTestHashes(2);

    createAndPopulateDb(corruptPath, 2, hashes);
    {
        yams::metadata::Database db;
        REQUIRE(db.open(freshPath.string(), yams::metadata::ConnectionMode::Create));
        REQUIRE(db.execute(kDocumentsTableDdl));
        REQUIRE(db.execute("PRAGMA journal_mode = WAL"));
        db.close();
    }

    REQUIRE((getJournalMode(freshPath) == "wal"));
    auto res = yams::daemon::salvageFromCorruptDb(corruptPath, freshPath);
    REQUIRE(res);
    CHECK((res.value().documentsSalvaged == 2));
    CHECK((getJournalMode(freshPath) == "wal"));

    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_CASE("salvageFromCorruptDb handles large document counts", "[unit][daemon][db_salvage]") {
    auto dir = makeScratchDir("yams_salvage_large");
    auto corruptPath = dir / "corrupt.db";
    auto freshPath = dir / "fresh.db";
    auto hashes = makeTestHashes(100);

    createAndPopulateDb(corruptPath, 100, hashes);

    {
        yams::metadata::Database db;
        REQUIRE(db.open(freshPath.string(), yams::metadata::ConnectionMode::Create));
        REQUIRE(db.execute(kDocumentsTableDdl));
        db.close();
    }

    auto res = yams::daemon::salvageFromCorruptDb(corruptPath, freshPath);
    REQUIRE(res);
    REQUIRE((res.value().documentsSalvaged == 100));
    REQUIRE((res.value().documentsFailed == 0));

    REQUIRE((countDocuments(freshPath) == 100));

    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_CASE("salvageFromCorruptDb skips duplicate hashes", "[unit][daemon][db_salvage]") {
    auto dir = makeScratchDir("yams_salvage_dupes");
    auto corruptPath = dir / "corrupt.db";
    auto freshPath = dir / "fresh.db";
    auto hashes = makeTestHashes(3);

    createAndPopulateDb(corruptPath, 3, hashes);

    // Pre-populate fresh DB with one of the same hashes
    {
        yams::metadata::Database db;
        REQUIRE(db.open(freshPath.string(), yams::metadata::ConnectionMode::Create));
        REQUIRE(db.execute(kDocumentsTableDdl));

        auto stmtResult = db.prepare(kDocumentsInsertSql);
        REQUIRE(stmtResult);
        auto stmt = std::move(stmtResult).value();
        REQUIRE(stmt.bind(1, std::string("/test/dup.txt")));
        REQUIRE(stmt.bind(2, std::string("dup.txt")));
        REQUIRE(stmt.bind(3, std::string("txt")));
        REQUIRE(stmt.bind(4, static_cast<int64_t>(100)));
        REQUIRE(stmt.bind(5, hashes[0]));
        REQUIRE(stmt.bind(6, std::string("text/plain")));
        REQUIRE(stmt.bind(7, static_cast<int64_t>(1)));
        REQUIRE(stmt.bind(8, static_cast<int64_t>(2)));
        REQUIRE(stmt.bind(9, static_cast<int64_t>(3)));
        REQUIRE(stmt.bind(10, 0));
        REQUIRE(stmt.bind(11, std::string("pending")));
        REQUIRE(stmt.bind(12, std::string("")));
        REQUIRE(stmt.bind(13, std::string("/")));
        REQUIRE(stmt.bind(14, std::string("")));
        REQUIRE(stmt.bind(15, std::string("")));
        REQUIRE(stmt.bind(16, std::string("")));
        REQUIRE(stmt.bind(17, static_cast<int64_t>(0)));
        REQUIRE(stmt.bind(18, std::string("pending")));
        REQUIRE(stmt.bind(19, static_cast<int64_t>(0)));
        REQUIRE(stmt.bind(20, static_cast<int64_t>(0)));
        REQUIRE(stmt.execute());
        db.close();
    }

    // The duplicate hash should be skipped (INSERT OR IGNORE)
    // So 2 new docs + 1 existing = 3 total
    auto res = yams::daemon::salvageFromCorruptDb(corruptPath, freshPath);
    REQUIRE(res);

    REQUIRE((countDocuments(freshPath) == 3));

    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_CASE("salvageFromCorruptDb fails cleanly when fresh DB is missing",
          "[unit][daemon][db_salvage]") {
    auto dir = makeScratchDir("yams_salvage_fresh_missing");
    auto corruptPath = dir / "corrupt.db";
    auto freshPath = dir / "fresh.db";
    auto hashes = makeTestHashes(1);

    createAndPopulateDb(corruptPath, 1, hashes);

    auto res = yams::daemon::salvageFromCorruptDb(corruptPath, freshPath);
    REQUIRE_FALSE(res);

    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_CASE("recovery sentinel cleanup is independent of salvage need",
          "[unit][daemon][db_salvage][startup_refactor]") {
    auto dir = makeScratchDir("yams_recovery_sentinel_cleanup");
    auto dbPath = dir / "yams.db";
    auto hashes = makeTestHashes(2);
    createAndPopulateDb(dbPath, 2, hashes);

    const auto sentinelA = dir / "yams.db.recovered-20260101T000000Z";
    const auto sentinelB = dir / "yams.db.recovered-20260101T000001Z";
    const auto unrelated = dir / "other.db.recovered-20260101T000000Z";
    {
        std::ofstream(sentinelA) << "recovered";
        std::ofstream(sentinelB) << "recovered";
        std::ofstream(unrelated) << "keep";
    }

    const auto qc = yams::daemon::quickCheckSalvageNeeded(dir, dbPath);
    REQUIRE_FALSE(qc.needsSalvage);
    REQUIRE((qc.corruptDbCount == 0));

    const auto cleanup = yams::daemon::removeRecoverySentinels(dbPath);
    REQUIRE(cleanup.errors.empty());
    REQUIRE((cleanup.removed.size() == 2));
    REQUIRE_FALSE(fs::exists(sentinelA));
    REQUIRE_FALSE(fs::exists(sentinelB));
    REQUIRE(fs::exists(unrelated));

    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_CASE("quick salvage check tracks unreadable corrupt DBs without requesting cleanup",
          "[unit][daemon][db_salvage][startup_refactor]") {
    auto dir = makeScratchDir("yams_salvage_unreadable_corrupt");
    auto dbPath = dir / "yams.db";
    auto hashes = makeTestHashes(1);
    createAndPopulateDb(dbPath, 1, hashes);

    const auto unreadableCorrupt = dir / "yams.db.corrupt-bad";
    {
        std::ofstream(unreadableCorrupt, std::ios::binary) << "not a sqlite database";
    }

    const auto qc = yams::daemon::quickCheckSalvageNeeded(dir, dbPath);
    REQUIRE((qc.corruptDbCount == 1));
    REQUIRE((qc.unreadableCorruptDbCount == 1));
    REQUIRE_FALSE(qc.needsSalvage);
    REQUIRE((qc.maxCorruptCount == 0));
    REQUIRE(fs::exists(unreadableCorrupt));

    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST_CASE("corrupt DB cleanup removes readable artifacts but keeps unreadable evidence",
          "[unit][daemon][db_salvage][startup_refactor]") {
    auto dir = makeScratchDir("yams_corrupt_cleanup_safe");
    auto readableCorrupt = dir / "yams.db.corrupt-readable";
    auto unreadableCorrupt = dir / "yams.db.corrupt-unreadable";
    auto readableWal = fs::path(readableCorrupt.string() + "-wal");
    auto readableShm = fs::path(readableCorrupt.string() + "-shm");
    auto hashes = makeTestHashes(1);

    createAndPopulateDb(readableCorrupt, 1, hashes);
    {
        std::ofstream(readableWal) << "wal";
        std::ofstream(readableShm) << "shm";
        std::ofstream(unreadableCorrupt, std::ios::binary) << "not sqlite";
    }

    const auto cleanup = yams::daemon::removeCorruptDbFiles(dir);
    REQUIRE(cleanup.errors.empty());
    REQUIRE((cleanup.removed.size() == 1));
    REQUIRE((cleanup.removed.front().filename() == readableCorrupt.filename()));
    REQUIRE_FALSE(fs::exists(readableCorrupt));
    REQUIRE_FALSE(fs::exists(readableWal));
    REQUIRE_FALSE(fs::exists(readableShm));
    REQUIRE(fs::exists(unreadableCorrupt));

    std::error_code ec;
    fs::remove_all(dir, ec);
}
