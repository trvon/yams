// Idle maintenance run by RepairService: vectors.db VACUUM and session expiry.

#include <catch2/catch_test_macros.hpp>

#include "../../common/test_helpers_catch2.h"

#include <yams/app/services/session_service.hpp>
#include <yams/daemon/components/RepairService.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/sqlite_vacuum.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/vector_database.h>

#include <nlohmann/json.hpp>

#include <sqlite3.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

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

using namespace std::chrono_literals;

std::int64_t epochSeconds(std::chrono::system_clock::time_point when) {
    return std::chrono::duration_cast<std::chrono::seconds>(when.time_since_epoch()).count();
}

// Write a session file the way SessionService does, last touched `idle` ago.
void writeSession(const fs::path& dir, const std::string& name, std::chrono::hours idle,
                  nlohmann::json extra = nlohmann::json::object()) {
    const auto touched = std::chrono::system_clock::now() - idle;
    nlohmann::json j = {{"name", name},
                        {"state", "closed"},
                        {"createdTime", epochSeconds(touched)},
                        {"lastOpenedTime", epochSeconds(touched)},
                        {"lastClosedTime", 0},
                        {"selectors", nlohmann::json::array()},
                        {"materialized", nlohmann::json::array()}};
    j.update(extra);
    const auto path = dir / (name + ".json");
    std::ofstream(path) << j.dump(2);
    fs::last_write_time(path, fs::file_time_type::clock::now() - idle);
}

bool contains(const std::vector<std::string>& names, const std::string& name) {
    return std::find(names.begin(), names.end(), name) != names.end();
}

// A session store shaped like the audited machine: per-run tool sessions plus a few that must
// survive expiry.
struct SessionStore {
    yams::test::TempDirGuard tmp{"yams_session_expiry_"};
    fs::path dir{tmp.path() / "sessions"};
    fs::path watchedDir{tmp.path() / "project"};

    SessionStore() {
        fs::create_directories(dir);
        fs::create_directories(watchedDir);
        std::ofstream(dir / "index.json") << R"({"current": "active-now"})";
        fs::last_write_time(dir / "index.json", fs::file_time_type::clock::now() - 24h * 90);

        writeSession(dir, "opencode-1a2b", 24h * 40);
        writeSession(dir, "mcp-grep-hot-7f", 24h * 45);
        writeSession(dir, "active-now", 24h * 60); // current, however old
        writeSession(dir, "used-yesterday", 24h);
        writeSession(dir, "watching-project", 24h * 60,
                     {{"watch", {{"enabled", true}, {"interval_ms", 2000}}},
                      {"selectors", {{{"path", watchedDir.string()}}}}});
        writeSession(dir, "watching-deleted-dir", 24h * 60,
                     {{"watch", {{"enabled", true}, {"interval_ms", 2000}}},
                      {"selectors", {{{"path", (tmp.path() / "gone").string()}}}}});
        writeSession(dir, "owns-documents", 24h * 60);
        std::ofstream(dir / "corrupt.json") << "{ not json";
        fs::last_write_time(dir / "corrupt.json", fs::file_time_type::clock::now() - 24h * 90);
    }
};

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

TEST_CASE("expireIdleSessions deletes only idle, unowned, unwatched sessions",
          "[daemon][repair][maintenance][sessions][catch2]") {
    SessionStore store;
    yams::app::services::SessionExpiryOptions options;
    options.maxIdle = 24h * 30;
    options.hasSessionDocuments = [](const std::string& name) { return name == "owns-documents"; };

    const auto result = yams::app::services::expireIdleSessions(store.dir, options);

    CHECK(contains(result.expired, "opencode-1a2b"));
    CHECK(contains(result.expired, "mcp-grep-hot-7f"));
    CHECK(contains(result.expired, "watching-deleted-dir"));
    CHECK(result.expired.size() == 3);
    CHECK_FALSE(fs::exists(store.dir / "opencode-1a2b.json"));
    CHECK_FALSE(fs::exists(store.dir / "mcp-grep-hot-7f.json"));

    for (const char* kept : {"active-now", "used-yesterday", "watching-project", "owns-documents",
                             "corrupt", "index"}) {
        INFO(kept);
        CHECK(fs::exists(store.dir / (std::string(kept) + ".json")));
    }
    CHECK(result.kept == 5);
}

TEST_CASE("expireIdleSessions keeps everything when ownership cannot be checked",
          "[daemon][repair][maintenance][sessions][catch2]") {
    SessionStore store;
    yams::app::services::SessionExpiryOptions options;
    options.maxIdle = 24h * 30;
    CHECK(yams::app::services::expireIdleSessions(store.dir, options).expired.empty());

    options.hasSessionDocuments = [](const std::string&) { return false; };
    options.maxIdle = std::chrono::system_clock::duration::zero(); // expiry disabled
    CHECK(yams::app::services::expireIdleSessions(store.dir, options).expired.empty());
    CHECK(fs::exists(store.dir / "opencode-1a2b.json"));

    CHECK(yams::app::services::expireIdleSessions(store.tmp.path() / "missing", options)
              .expired.empty());
}

TEST_CASE("RepairService maintenance expires idle sessions using document ownership",
          "[daemon][repair][maintenance][sessions][catch2]") {
    SessionStore store;
    const auto dbPath = store.tmp.path() / "yams.db";
    yams::metadata::ConnectionPoolConfig poolConfig;
    poolConfig.minConnections = 1;
    poolConfig.maxConnections = 2;
    auto pool = std::make_unique<yams::metadata::ConnectionPool>(dbPath.string(), poolConfig);
    REQUIRE(pool->initialize().has_value());
    auto repo = std::make_shared<yams::metadata::MetadataRepository>(*pool);

    yams::metadata::DocumentInfo doc;
    doc.filePath = (store.tmp.path() / "note.md").string();
    doc.fileName = "note.md";
    doc.fileExtension = ".md";
    doc.fileSize = 4;
    doc.sha256Hash = std::string(64, 'b');
    doc.mimeType = "text/markdown";
    auto docId = repo->insertDocument(doc);
    REQUIRE(docId.has_value());
    REQUIRE(repo->setMetadata(docId.value(), "session_id",
                              yams::metadata::MetadataValue(std::string("owns-documents")))
                .has_value());

    std::size_t activeConnections = 0;
    RepairServiceContext ctx;
    ctx.getMetadataRepo = [repo] { return repo; };
    RepairService::Config cfg;
    cfg.enable = true;
    cfg.dataDir = store.tmp.path();
    cfg.sessionsDir = store.dir;
    RepairService service(ctx, nullptr, [&activeConnections] { return activeConnections; }, cfg);

    SECTION("a connected client defers it") {
        activeConnections = 1;
        CHECK_FALSE(service.testing_runSessionExpiryMaintenance().has_value());
        CHECK(fs::exists(store.dir / "opencode-1a2b.json"));
    }
    SECTION("an idle daemon expires the tool sessions but not the one owning documents") {
        const auto result = service.testing_runSessionExpiryMaintenance();
        REQUIRE(result.has_value());
        CHECK(contains(result->expired, "opencode-1a2b"));
        CHECK(contains(result->expired, "mcp-grep-hot-7f"));
        CHECK_FALSE(contains(result->expired, "owns-documents"));
        CHECK(fs::exists(store.dir / "owns-documents.json"));
        CHECK(fs::exists(store.dir / "active-now.json"));
    }

    repo.reset();
    pool->shutdown();
}
