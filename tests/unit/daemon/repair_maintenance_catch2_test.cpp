// Idle maintenance run by RepairService: vectors.db VACUUM.

#include <catch2/catch_test_macros.hpp>

#include "../../common/test_helpers_catch2.h"

#include <yams/daemon/components/RepairService.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/sqlite_vacuum.h>
#include <yams/vector/vector_database.h>

#include <sqlite3.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>

using namespace yams::daemon;
namespace fs = std::filesystem;

namespace {

constexpr std::uint64_t kPageSize = 4096;
constexpr std::uint64_t kGiB = 1024ULL * 1024 * 1024;

// A policy small enough for a test file: any file with >= 64 KiB of free pages and a 10%
// free ratio qualifies.
SqliteVacuumPolicy smallFilePolicy() {
    SqliteVacuumPolicy policy;
    policy.minDatabaseBytes = 0;
    policy.minReclaimableBytes = 64 * 1024;
    policy.minReclaimableRatio = 0.10;
    return policy;
}

void execOrFail(sqlite3* db, const char* sql) {
    char* err = nullptr;
    const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);
    INFO(sql << " -> " << (err ? err : "ok"));
    sqlite3_free(err);
    REQUIRE(rc == SQLITE_OK);
}

// Keep a few thousand live rows (so VACUUM has real copying to do) and add ~4 MiB of blobs
// that are then deleted, leaving most pages on the freelist.
void addReclaimableFreePages(const fs::path& path) {
    sqlite3* db = nullptr;
    REQUIRE(sqlite3_open_v2(path.string().c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                            nullptr) == SQLITE_OK);
    execOrFail(db, "PRAGMA journal_mode=WAL");
    execOrFail(db, "CREATE TABLE IF NOT EXISTS filler(id INTEGER PRIMARY KEY, payload BLOB)");
    execOrFail(db, "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < "
                   "1024) INSERT INTO filler(payload) SELECT zeroblob(4096) FROM n");
    execOrFail(db, "CREATE TABLE IF NOT EXISTS live_rows(id INTEGER PRIMARY KEY, label TEXT)");
    execOrFail(db, "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < "
                   "4000) INSERT INTO live_rows(label) SELECT 'row-' || i FROM n");
    execOrFail(db, "DELETE FROM filler");
    execOrFail(db, "PRAGMA wal_checkpoint(TRUNCATE)");
    sqlite3_close_v2(db);
}

std::uint64_t fileBytes(const fs::path& path) {
    return static_cast<std::uint64_t>(fs::file_size(path));
}

} // namespace

TEST_CASE("shouldVacuumSqlite keeps the yams.db thresholds",
          "[daemon][repair][maintenance][vacuum][catch2]") {
    const SqliteVacuumPolicy policy;

    SECTION("large file with mostly free pages is worth it") {
        // The audited install: vectors.db 39.7 GB with 48% free pages.
        const std::uint64_t dbBytes = 397ULL * kGiB / 10;
        const std::uint64_t pages = dbBytes / kPageSize;
        CHECK(shouldVacuumSqlite(dbBytes, pages, pages * 48 / 100, kPageSize, policy));
    }
    SECTION("small files are never vacuumed") {
        CHECK_FALSE(shouldVacuumSqlite(256ULL * 1024 * 1024, 65'536, 60'000, kPageSize, policy));
    }
    SECTION("a low free-page ratio is not worth a rewrite") {
        const std::uint64_t pages = 8ULL * kGiB / kPageSize;
        CHECK_FALSE(shouldVacuumSqlite(8ULL * kGiB, pages, pages / 20, kPageSize, policy));
    }
    SECTION("trailing bytes past the logical end count as reclaimable") {
        CHECK(shouldVacuumSqlite(600ULL * 1024 * 1024, 8, 0, kPageSize, policy));
    }
    SECTION("inconsistent statistics are rejected") {
        CHECK_FALSE(shouldVacuumSqlite(2ULL * kGiB, 10, 11, kPageSize, policy));
        CHECK_FALSE(shouldVacuumSqlite(2ULL * kGiB, 0, 0, kPageSize, policy));
        CHECK_FALSE(shouldVacuumSqlite(2ULL * kGiB, 10, 5, 0, policy));
    }
    SECTION("free disk must exceed the current file size") {
        CHECK(hasSpaceForSqliteVacuum(10 * kGiB, 10 * kGiB + 1));
        CHECK_FALSE(hasSpaceForSqliteVacuum(10 * kGiB, 10 * kGiB));
        CHECK_FALSE(hasSpaceForSqliteVacuum(10 * kGiB, kGiB));
    }
}

TEST_CASE("vacuumSqliteFileIfUseful reclaims free pages and yields to work",
          "[daemon][repair][maintenance][vacuum][catch2]") {
    yams::test::TempDirGuard tmp("yams_sqlite_vacuum_");
    const auto dbPath = tmp.path() / "vectors.db";
    addReclaimableFreePages(dbPath);
    const auto before = fileBytes(dbPath);
    REQUIRE(before > 2ULL * 1024 * 1024);

    SECTION("the default policy leaves a small file alone") {
        const auto outcome = vacuumSqliteFileIfUseful(dbPath, SqliteVacuumPolicy{}, {});
        CHECK(outcome.status == SqliteVacuumStatus::NotNeeded);
        CHECK(fileBytes(dbPath) == before);
    }
    SECTION("a qualifying file is rewritten and shrinks") {
        const auto outcome = vacuumSqliteFileIfUseful(dbPath, smallFilePolicy(), {});
        CHECK(outcome.status == SqliteVacuumStatus::Vacuumed);
        CHECK(outcome.bytesBefore == before);
        CHECK(outcome.bytesAfter < before / 4);
        CHECK(fileBytes(dbPath) == outcome.bytesAfter);
    }
    SECTION("an abort request rolls the VACUUM back") {
        const auto outcome =
            vacuumSqliteFileIfUseful(dbPath, smallFilePolicy(), [] { return true; });
        CHECK(outcome.status == SqliteVacuumStatus::Interrupted);
        CHECK(fileBytes(dbPath) == before);
    }
    SECTION("a live writer makes it back off instead of waiting") {
        sqlite3* writer = nullptr;
        REQUIRE(sqlite3_open_v2(dbPath.string().c_str(), &writer, SQLITE_OPEN_READWRITE, nullptr) ==
                SQLITE_OK);
        execOrFail(writer, "BEGIN IMMEDIATE");
        const auto outcome = vacuumSqliteFileIfUseful(dbPath, smallFilePolicy(), {});
        execOrFail(writer, "ROLLBACK");
        sqlite3_close_v2(writer);
        CHECK(outcome.status == SqliteVacuumStatus::Busy);
        CHECK(fileBytes(dbPath) == before);
    }
    SECTION("a missing file is never created") {
        const auto missing = tmp.path() / "absent.db";
        const auto outcome = vacuumSqliteFileIfUseful(missing, smallFilePolicy(), {});
        CHECK(outcome.status == SqliteVacuumStatus::OpenFailed);
        CHECK_FALSE(fs::exists(missing));
    }
}

TEST_CASE("vectors.db VACUUM is admitted only while the daemon is idle",
          "[daemon][repair][maintenance][vacuum][catch2]") {
    RepairService::VectorVacuumSignals idle;
    idle.maintenanceAllowed = true;
    idle.pressure = ResourcePressureLevel::Normal;
    CHECK(RepairService::vectorVacuumAdmitted(idle));

    auto busy = idle;
    busy.shuttingDown = true;
    CHECK_FALSE(RepairService::vectorVacuumAdmitted(busy));

    busy = idle;
    busy.maintenanceAllowed = false; // a client is connected
    CHECK_FALSE(RepairService::vectorVacuumAdmitted(busy));

    busy = idle;
    busy.pressure = ResourcePressureLevel::Warning;
    CHECK_FALSE(RepairService::vectorVacuumAdmitted(busy));

    busy = idle;
    busy.embeddingQueued = 1;
    CHECK_FALSE(RepairService::vectorVacuumAdmitted(busy));

    busy = idle;
    busy.embeddingInFlight = 1;
    CHECK_FALSE(RepairService::vectorVacuumAdmitted(busy));

    busy = idle;
    busy.indexMutating = true; // rebuild or bulk load owns the vector connection
    CHECK_FALSE(RepairService::vectorVacuumAdmitted(busy));

    busy = idle;
    busy.repairInProgress = true;
    CHECK_FALSE(RepairService::vectorVacuumAdmitted(busy));
}

TEST_CASE("RepairService maintenance vacuums the live vectors.db when idle",
          "[daemon][repair][maintenance][vacuum][catch2]") {
    yams::test::ScopedEnvVar vectorsOn("YAMS_DISABLE_VECTORS", std::nullopt);
    yams::test::ScopedEnvVar vectorsOnAlias("YAMS_DISABLE_VECTOR_DB", std::nullopt);
    yams::test::ScopedEnvVar vectorsOnSingular("YAMS_DISABLE_VECTOR", std::nullopt);
    yams::test::ScopedEnvVar onDisk("YAMS_VDB_IN_MEMORY", std::nullopt);
    ResourceGovernor::instance().testing_setPressureState(ResourcePressureLevel::Normal,
                                                          std::chrono::steady_clock::now());

    yams::test::TempDirGuard tmp("yams_repair_vacuum_");
    const auto dbPath = tmp.path() / "vectors.db";

    yams::vector::VectorDatabaseConfig vcfg;
    vcfg.database_path = dbPath.string();
    vcfg.embedding_dim = 8;
    auto vectorDb = std::make_shared<yams::vector::VectorDatabase>(vcfg);
    REQUIRE(vectorDb->initialize());
    addReclaimableFreePages(dbPath);
    const auto before = fileBytes(dbPath);

    std::size_t activeConnections = 0;
    std::size_t queuedEmbeddings = 0;
    RepairServiceContext ctx;
    ctx.getVectorDatabase = [vectorDb] { return vectorDb; };
    ctx.getEmbeddingQueuedJobs = [&queuedEmbeddings] { return queuedEmbeddings; };
    ctx.getEmbeddingInFlightJobs = [] { return std::size_t{0}; };

    RepairService::Config cfg;
    cfg.enable = true;
    cfg.dataDir = tmp.path();
    RepairService service(ctx, nullptr, [&activeConnections] { return activeConnections; }, cfg);
    service.testing_setVectorVacuumPolicy(smallFilePolicy());

    SECTION("a connected client defers it") {
        activeConnections = 1;
        CHECK_FALSE(service.testing_runVectorVacuumMaintenance().has_value());
        CHECK(fileBytes(dbPath) == before);
    }
    SECTION("queued embedding writes defer it") {
        queuedEmbeddings = 3;
        CHECK_FALSE(service.testing_runVectorVacuumMaintenance().has_value());
        CHECK(fileBytes(dbPath) == before);
    }
    SECTION("an idle daemon reclaims the free pages and the database stays usable") {
        const auto outcome = service.testing_runVectorVacuumMaintenance();
        REQUIRE(outcome.has_value());
        CHECK(outcome->status == SqliteVacuumStatus::Vacuumed);
        CHECK(fileBytes(dbPath) < before / 4);

        yams::vector::VectorRecord record;
        record.chunk_id = "chunk-after-vacuum";
        record.document_hash = std::string(64, 'a');
        record.embedding = std::vector<float>(8, 0.5f);
        record.content = "still writable";
        CHECK(vectorDb->insertVector(record));
        CHECK(vectorDb->getVectorCount() == 1);
    }
    SECTION("vectors disabled by configuration leaves the file alone") {
        yams::test::ScopedEnvVar disabled("YAMS_DISABLE_VECTORS", std::string("1"));
        CHECK_FALSE(service.testing_runVectorVacuumMaintenance().has_value());
        CHECK(fileBytes(dbPath) == before);
    }
    vectorDb->close();
}
