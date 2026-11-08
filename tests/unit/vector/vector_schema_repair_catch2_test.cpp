// Characterization tests for yams::vector::ensureVectorSchemaAligned and recreateVectorTables.
//
// The stored embedding dimension is probed from rows in `vectors`, so an empty table reports no
// dimension. Cases seed a row at a known dimension and assert on the probed dimension and the row
// count after repair, not only on the returned Result.

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstddef>
#include <filesystem>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

#include <sqlite3.h>
#include <yams/config/config_helpers.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_schema_repair.h>

using namespace yams::vector;

namespace {

bool envFlagSet(const char* name) {
    auto value = yams::config::getenv_optional(name);
    return value && (*value == "1" || *value == "true");
}

struct VectorSchemaRepairFixture {
    VectorSchemaRepairFixture() {
        if (envFlagSet("YAMS_SQLITE_VEC_SKIP_INIT")) {
            skipReason = "Skipping (YAMS_SQLITE_VEC_SKIP_INIT=1)";
        } else if (envFlagSet("YAMS_DISABLE_VECTORS")) {
            skipReason = "Skipping (YAMS_DISABLE_VECTORS=1)";
        }
        static std::atomic<int> counter{0};
        dataDir = std::filesystem::temp_directory_path() /
                  ("yams_vector_schema_repair_" + std::to_string(++counter));
        std::error_code ec;
        std::filesystem::remove_all(dataDir, ec);
    }

    ~VectorSchemaRepairFixture() {
        std::error_code ec;
        std::filesystem::remove_all(dataDir, ec);
    }

    void skipIfNeeded() const {
        if (!skipReason.empty()) {
            SKIP(skipReason);
        }
    }

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

    struct Snapshot {
        bool vectorsTable = false;
        bool entityVectorsTable = false;
        std::optional<std::size_t> storedDim;
        std::size_t rows = 0;
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
        backend.close();
        return s;
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

    std::string skipReason;
    std::filesystem::path dataDir;
};

} // namespace

TEST_CASE_METHOD(VectorSchemaRepairFixture,
                 "ensureVectorSchemaAligned creates the data dir and tables when missing",
                 "[unit][vector][schema-repair][catch2]") {
    skipIfNeeded();
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
    skipIfNeeded();
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
    skipIfNeeded();
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
    skipIfNeeded();
    seed(8);

    REQUIRE(ensureVectorSchemaAligned(dataDir, 8, /*recreateIfMismatch=*/true).has_value());

    const auto s = snapshot();
    CHECK(s.rows == 1);
    CHECK(s.storedDim == std::optional<std::size_t>{8});
}

TEST_CASE_METHOD(VectorSchemaRepairFixture,
                 "recreateVectorTables discards vectors and is idempotent",
                 "[unit][vector][schema-repair][catch2]") {
    skipIfNeeded();
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
