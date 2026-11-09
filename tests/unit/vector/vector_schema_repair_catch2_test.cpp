// Characterization tests for yams::vector::ensureVectorSchemaAligned and recreateVectorTables.
//
// The stored embedding dimension is probed from rows in `vectors`, so an empty table reports no
// dimension. Cases seed a row at a known dimension and assert on the probed dimension and the row
// count after repair, not only on the returned Result.
//
// No environment-based skip: neither the repair functions nor SqliteVecBackend read
// YAMS_DISABLE_VECTORS / YAMS_SQLITE_VEC_SKIP_INIT, and sqlite-vec-cpp is statically linked
// (ensureVecLoaded is a no-op), so these cases run as-is in the CI unit lane, which exports both.

#include <catch2/catch_test_macros.hpp>

#include "../../common/test_helpers_catch2.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_schema_repair.h>

using namespace yams::vector;

namespace {

struct VectorSchemaRepairFixture {
    // Unique per run (make_temp_dir), removed on scope exit. The data dir itself is a
    // not-yet-created child so the "missing" case can observe directory creation.
    yams::test::TempDirGuard tempRoot{"yams_vector_schema_repair_"};
    std::filesystem::path dataDir = tempRoot.path() / "data";

    std::string dbPath() const { return (dataDir / "vectors.db").string(); }

    // Create the tables at `dim` and insert one row so the dimension becomes probeable.
    void seed(std::size_t dim) const {
        std::filesystem::create_directories(dataDir);
        SqliteVecBackend backend;
        REQUIRE(backend.initialize(dbPath()).has_value());
        REQUIRE(backend.createTables(dim).has_value());
        VectorRecord rec;
        rec.document_hash = "doc-seed";
        rec.chunk_id = "doc-seed_chunk_0";
        rec.embedding.assign(dim, 0.5f);
        rec.content = "seed";
        rec.start_offset = 0;
        rec.end_offset = rec.content.size();
        REQUIRE(backend.insertVector(rec).has_value());
        backend.close();
    }

    // Create the tables at `dim` with an empty `vectors` and one `entity_vectors` row at `dim`.
    void seedEntityOnly(std::size_t dim) const {
        std::filesystem::create_directories(dataDir);
        SqliteVecBackend backend;
        REQUIRE(backend.initialize(dbPath()).has_value());
        REQUIRE(backend.createTables(dim).has_value());
        EntityVectorRecord rec;
        rec.node_key = "node-seed";
        rec.embedding.assign(dim, 0.25f);
        rec.content = "seed";
        REQUIRE(backend.insertEntityVector(rec).has_value());
        backend.close();
    }

    // One `vectors` row at `vectorsDim` and one `entity_vectors` row at `entityDim`.
    void seedMixed(std::size_t vectorsDim, std::size_t entityDim) const {
        std::filesystem::create_directories(dataDir);
        SqliteVecBackend backend;
        REQUIRE(backend.initialize(dbPath()).has_value());
        REQUIRE(backend.createTables(vectorsDim).has_value());
        VectorRecord rec;
        rec.document_hash = "doc-mixed";
        rec.chunk_id = "doc-mixed_chunk_0";
        rec.embedding.assign(vectorsDim, 0.5f);
        rec.content = "mixed";
        rec.end_offset = rec.content.size();
        REQUIRE(backend.insertVector(rec).has_value());
        EntityVectorRecord entity;
        entity.node_key = "node-mixed";
        entity.embedding.assign(entityDim, 0.25f);
        entity.content = "mixed";
        REQUIRE(backend.insertEntityVector(entity).has_value());
        backend.close();
    }

    struct Snapshot {
        bool vectorsTable = false;
        bool entityVectorsTable = false;
        std::optional<std::size_t> storedDim;
        std::size_t rows = 0;
        std::size_t entityRows = 0;
    };

    Snapshot snapshot() const {
        Snapshot s;
        SqliteVecBackend backend;
        REQUIRE(backend.initialize(dbPath()).has_value());
        sqlite3* db = backend.getDbHandle();
        REQUIRE(db != nullptr);
        s.vectorsTable = tableExists(db, "vectors");
        s.entityVectorsTable = tableExists(db, "entity_vectors");
        s.storedDim = backend.getStoredEmbeddingDimension();
        if (s.vectorsTable) {
            s.rows = countRows(db, "SELECT COUNT(*) FROM vectors");
        }
        if (s.entityVectorsTable) {
            s.entityRows = countRows(db, "SELECT COUNT(*) FROM entity_vectors");
        }
        backend.close();
        return s;
    }

    // Plain SQLite connection, so setup and checks bypass SqliteVecBackend's own schema healing.
    struct RawDb {
        explicit RawDb(const std::string& path) {
            REQUIRE(sqlite3_open_v2(path.data(), &db, SQLITE_OPEN_READWRITE, nullptr) == SQLITE_OK);
        }
        ~RawDb() { sqlite3_close(db); }
        RawDb(const RawDb&) = delete;
        RawDb& operator=(const RawDb&) = delete;

        void exec(const char* sql) const {
            char* err = nullptr;
            const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);
            const std::string message = err ? err : "";
            sqlite3_free(err);
            INFO(sql << ": " << message);
            REQUIRE(rc == SQLITE_OK);
        }

        std::int64_t integer(const char* sql) const {
            sqlite3_stmt* stmt = nullptr;
            REQUIRE(sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK);
            REQUIRE(sqlite3_step(stmt) == SQLITE_ROW);
            const std::int64_t value = sqlite3_column_int64(stmt, 0);
            sqlite3_finalize(stmt);
            return value;
        }

        sqlite3* db = nullptr;
    };

    // One simeon_pq_meta row and one simeon_pq_codes row at `dim` (tables must already exist).
    void seedPq(std::size_t dim) const {
        RawDb raw(dbPath());
        const auto d = std::to_string(dim);
        const std::string meta =
            "INSERT OR REPLACE INTO simeon_pq_meta (dim, m, k, seed, rerank_factor, vector_count, "
            "codebooks_blob) VALUES (" +
            d + ", 4, 16, 1, 2, 1, x'00')";
        const std::string codes =
            "INSERT OR REPLACE INTO simeon_pq_codes (dim, rowid, code) VALUES (" + d +
            ", 1, x'00')";
        raw.exec(meta.c_str());
        raw.exec(codes.c_str());
    }

    struct PqSnapshot {
        bool metaTable = false;
        bool codesTable = false;
        std::int64_t metaRows = 0;
        std::int64_t codesRows = 0;
    };

    PqSnapshot pqSnapshot() const {
        RawDb raw(dbPath());
        PqSnapshot s;
        s.metaTable = raw.integer("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND "
                                  "name='simeon_pq_meta'") == 1;
        s.codesTable = raw.integer("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND "
                                   "name='simeon_pq_codes'") == 1;
        if (s.metaTable)
            s.metaRows = raw.integer("SELECT COUNT(*) FROM simeon_pq_meta");
        if (s.codesTable)
            s.codesRows = raw.integer("SELECT COUNT(*) FROM simeon_pq_codes");
        return s;
    }

    std::int64_t structuralGeneration() const {
        RawDb raw(dbPath());
        return raw.integer(
            "SELECT structural_generation FROM vector_index_generation WHERE id = 1");
    }

    static std::size_t countRows(sqlite3* db, const char* sql) {
        sqlite3_stmt* stmt = nullptr;
        REQUIRE(sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK);
        std::size_t rows = 0;
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            rows = static_cast<std::size_t>(sqlite3_column_int64(stmt, 0));
        }
        sqlite3_finalize(stmt);
        return rows;
    }

    static bool tableExists(sqlite3* db, const char* name) {
        sqlite3_stmt* stmt = nullptr;
        const char* sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?";
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
            return false;
        }
        sqlite3_bind_text(stmt, 1, name, -1, SQLITE_TRANSIENT);
        const bool present = (sqlite3_step(stmt) == SQLITE_ROW);
        sqlite3_finalize(stmt);
        return present;
    }
};

} // namespace

TEST_CASE_METHOD(VectorSchemaRepairFixture,
                 "ensureVectorSchemaAligned creates the data dir and tables when missing",
                 "[unit][vector][schema-repair][catch2]") {
    REQUIRE_FALSE(std::filesystem::exists(dataDir));

    REQUIRE(ensureVectorSchemaAligned(dataDir, 16, /*recreateIfMismatch=*/false).has_value());

    REQUIRE(std::filesystem::exists(dataDir / "vectors.db"));
    const auto s = snapshot();
    CHECK(s.vectorsTable);
    CHECK(s.entityVectorsTable);
    CHECK(s.rows == 0);
    // Empty tables carry no probeable dimension.
    CHECK_FALSE(s.storedDim.has_value());

    // Re-running on the empty tables is idempotent.
    REQUIRE(ensureVectorSchemaAligned(dataDir, 16, /*recreateIfMismatch=*/true).has_value());
    CHECK(snapshot().vectorsTable);
}

TEST_CASE_METHOD(VectorSchemaRepairFixture,
                 "ensureVectorSchemaAligned recreates mismatched tables when allowed",
                 "[unit][vector][schema-repair][catch2]") {
    seed(8);
    REQUIRE(snapshot().storedDim == std::optional<std::size_t>{8});

    REQUIRE(ensureVectorSchemaAligned(dataDir, 16, /*recreateIfMismatch=*/true).has_value());

    const auto s = snapshot();
    CHECK(s.vectorsTable);
    CHECK(s.entityVectorsTable);
    CHECK(s.rows == 0);
    CHECK_FALSE(s.storedDim.has_value());
}

TEST_CASE_METHOD(VectorSchemaRepairFixture,
                 "ensureVectorSchemaAligned leaves mismatched tables alone when recreate is off",
                 "[unit][vector][schema-repair][catch2]") {
    seed(8);

    // Documented contract: success, database untouched.
    REQUIRE(ensureVectorSchemaAligned(dataDir, 16, /*recreateIfMismatch=*/false).has_value());

    const auto s = snapshot();
    CHECK(s.rows == 1);
    CHECK(s.storedDim == std::optional<std::size_t>{8});
}

TEST_CASE_METHOD(VectorSchemaRepairFixture,
                 "ensureVectorSchemaAligned is a no-op when the dimension already matches",
                 "[unit][vector][schema-repair][catch2]") {
    seed(8);

    REQUIRE(ensureVectorSchemaAligned(dataDir, 8, /*recreateIfMismatch=*/true).has_value());

    const auto s = snapshot();
    CHECK(s.rows == 1);
    CHECK(s.storedDim == std::optional<std::size_t>{8});
}

TEST_CASE_METHOD(VectorSchemaRepairFixture,
                 "ensureVectorSchemaAligned treats stale entity_vectors as a mismatch",
                 "[unit][vector][schema-repair][catch2]") {
    seedEntityOnly(8);
    {
        const auto before = snapshot();
        REQUIRE(before.rows == 0);
        REQUIRE(before.entityRows == 1);
    }

    SECTION("recreate allowed: entity_vectors is dropped and recreated empty") {
        REQUIRE(ensureVectorSchemaAligned(dataDir, 16, /*recreateIfMismatch=*/true).has_value());
        const auto s = snapshot();
        CHECK(s.vectorsTable);
        CHECK(s.entityVectorsTable);
        CHECK(s.rows == 0);
        CHECK(s.entityRows == 0);
    }
    SECTION("recreate off: success, stale entity rows are left untouched") {
        REQUIRE(ensureVectorSchemaAligned(dataDir, 16, /*recreateIfMismatch=*/false).has_value());
        const auto s = snapshot();
        CHECK(s.entityVectorsTable);
        CHECK(s.entityRows == 1);
    }
    SECTION("matching dimension: no-op, entity rows kept") {
        REQUIRE(ensureVectorSchemaAligned(dataDir, 8, /*recreateIfMismatch=*/true).has_value());
        CHECK(snapshot().entityRows == 1);
    }
}

TEST_CASE_METHOD(VectorSchemaRepairFixture,
                 "ensureVectorSchemaAligned recreates only the stale table group",
                 "[unit][vector][schema-repair][catch2]") {
    SECTION("vectors at target, entity_vectors stale: document vectors are kept") {
        seedMixed(/*vectorsDim=*/16, /*entityDim=*/8);
        REQUIRE(ensureVectorSchemaAligned(dataDir, 16, /*recreateIfMismatch=*/true).has_value());
        const auto s = snapshot();
        CHECK(s.rows == 1);
        CHECK(s.storedDim == std::optional<std::size_t>{16});
        CHECK(s.entityVectorsTable);
        CHECK(s.entityRows == 0);
    }
    SECTION("vectors stale, entity_vectors at target: entity vectors are kept") {
        seedMixed(/*vectorsDim=*/8, /*entityDim=*/16);
        REQUIRE(ensureVectorSchemaAligned(dataDir, 16, /*recreateIfMismatch=*/true).has_value());
        const auto s = snapshot();
        CHECK(s.vectorsTable);
        CHECK(s.rows == 0);
        CHECK_FALSE(s.storedDim.has_value());
        CHECK(s.entityRows == 1);
    }
}

TEST_CASE_METHOD(VectorSchemaRepairFixture,
                 "ensureVectorSchemaAligned bumps the index generation only when vectors go",
                 "[unit][vector][schema-repair][catch2]") {
    SECTION("vectors group dropped") {
        seed(8);
        const auto before = structuralGeneration();
        REQUIRE(ensureVectorSchemaAligned(dataDir, 16, /*recreateIfMismatch=*/true).has_value());
        CHECK(structuralGeneration() > before);
    }
    SECTION("only entity_vectors dropped") {
        seedMixed(/*vectorsDim=*/16, /*entityDim=*/8);
        const auto before = structuralGeneration();
        REQUIRE(ensureVectorSchemaAligned(dataDir, 16, /*recreateIfMismatch=*/true).has_value());
        CHECK(structuralGeneration() == before);
        CHECK(snapshot().rows == 1);
    }
}

TEST_CASE_METHOD(VectorSchemaRepairFixture,
                 "ensureVectorSchemaAligned creates missing tables next to a stale group",
                 "[unit][vector][schema-repair][catch2]") {
    // Empty `vectors`, PQ codes at an old dimension, entity_vectors missing.
    std::filesystem::create_directories(dataDir);
    {
        SqliteVecBackend backend;
        REQUIRE(backend.initialize(dbPath()).has_value());
        REQUIRE(backend.createTables(8).has_value());
        backend.close();
    }
    {
        RawDb raw(dbPath());
        raw.exec("INSERT INTO simeon_pq_codes (dim, rowid, code) VALUES (8, 1, x'00')");
        raw.exec("DROP TABLE entity_vectors");
    }

    REQUIRE(ensureVectorSchemaAligned(dataDir, 16, /*recreateIfMismatch=*/false).has_value());

    RawDb raw(dbPath());
    CHECK(raw.integer("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND "
                      "name='entity_vectors'") == 1);
    // recreate is off: the stale PQ rows are kept.
    CHECK(raw.integer("SELECT COUNT(*) FROM simeon_pq_codes WHERE dim = 8") == 1);
}

TEST_CASE_METHOD(VectorSchemaRepairFixture,
                 "ensureVectorSchemaAligned returns probe failures instead of succeeding",
                 "[unit][vector][schema-repair][catch2]") {
    // A prepare failure stands in for a mid-scan step failure (BUSY/IOERR/CORRUPT), which is not
    // injectable here: both go through the same error return. simeon_pq_codes without its `dim`
    // column makes the PQ probe fail to prepare.
    seed(16);
    {
        RawDb raw(dbPath());
        raw.exec("DROP TABLE simeon_pq_codes");
        raw.exec("CREATE TABLE simeon_pq_codes (unexpected INTEGER)");
    }

    auto aligned = ensureVectorSchemaAligned(dataDir, 16, /*recreateIfMismatch=*/true);
    REQUIRE_FALSE(aligned.has_value());
    CHECK(aligned.error().code == yams::ErrorCode::DatabaseError);

    RawDb raw(dbPath());
    CHECK(raw.integer("SELECT COUNT(*) FROM vectors") == 1);
}

TEST_CASE_METHOD(VectorSchemaRepairFixture,
                 "ensureVectorSchemaAligned treats the Simeon PQ tables as one group",
                 "[unit][vector][schema-repair][simeon-pq][catch2]") {
    SECTION("stale vectors take the PQ group with them") {
        seed(8);
        seedPq(8);
        REQUIRE(ensureVectorSchemaAligned(dataDir, 16, /*recreateIfMismatch=*/true).has_value());
        const auto pq = pqSnapshot();
        CHECK(pq.metaTable);
        CHECK(pq.codesTable);
        CHECK(pq.metaRows == 0);
        CHECK(pq.codesRows == 0);
        CHECK(snapshot().rows == 0);
    }
    SECTION("stale entity_vectors leave a matching PQ group alone") {
        seedMixed(/*vectorsDim=*/16, /*entityDim=*/8);
        seedPq(16);
        REQUIRE(ensureVectorSchemaAligned(dataDir, 16, /*recreateIfMismatch=*/true).has_value());
        const auto pq = pqSnapshot();
        CHECK(pq.metaRows == 1);
        CHECK(pq.codesRows == 1);
        const auto s = snapshot();
        CHECK(s.rows == 1);
        CHECK(s.entityRows == 0);
    }
    SECTION("a stale PQ group alone is dropped without touching vectors") {
        seed(16);
        seedPq(8);
        const auto generation = structuralGeneration();
        REQUIRE(ensureVectorSchemaAligned(dataDir, 16, /*recreateIfMismatch=*/true).has_value());
        const auto pq = pqSnapshot();
        CHECK(pq.metaTable);
        CHECK(pq.codesTable);
        CHECK(pq.metaRows == 0);
        CHECK(pq.codesRows == 0);
        const auto s = snapshot();
        CHECK(s.rows == 1);
        CHECK(s.storedDim == std::optional<std::size_t>{16});
        CHECK(structuralGeneration() == generation);
    }
    SECTION("recreate off leaves a stale PQ group untouched") {
        seed(16);
        seedPq(8);
        REQUIRE(ensureVectorSchemaAligned(dataDir, 16, /*recreateIfMismatch=*/false).has_value());
        const auto pq = pqSnapshot();
        CHECK(pq.metaRows == 1);
        CHECK(pq.codesRows == 1);
        CHECK(snapshot().rows == 1);
    }
}

TEST_CASE_METHOD(VectorSchemaRepairFixture,
                 "recreateVectorTables discards vectors and is idempotent",
                 "[unit][vector][schema-repair][catch2]") {
    seed(8);

    SECTION("at a new dimension") {
        REQUIRE(recreateVectorTables(dataDir, 16).has_value());
    }
    SECTION("at the same dimension (still unconditional)") {
        REQUIRE(recreateVectorTables(dataDir, 8).has_value());
    }

    auto s = snapshot();
    CHECK(s.vectorsTable);
    CHECK(s.entityVectorsTable);
    CHECK(s.rows == 0);
    CHECK_FALSE(s.storedDim.has_value());

    // A second recreate on the now-empty tables succeeds and leaves the same schema.
    REQUIRE(recreateVectorTables(dataDir, 16).has_value());
    s = snapshot();
    CHECK(s.vectorsTable);
    CHECK(s.entityVectorsTable);
    CHECK(s.rows == 0);
}
