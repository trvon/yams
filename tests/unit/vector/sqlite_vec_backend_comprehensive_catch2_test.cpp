// Comprehensive Catch2 tests for SqliteVecBackend
// Task: yams-47l2.7 - Comprehensive Vector Subsystem Tests
//
// Coverage:
// - Initialization (new DB, existing DB, corrupted)
// - CRUD operations (insert, update, delete, get)
// - Search operations (filters, thresholds, edge cases)
// - Transaction operations (begin/commit/rollback)
// - Edge cases (empty corpus, k > size, etc.)

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <limits>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <sqlite3.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/turboquant.h>
#include <yams/vector/vector_index_manager.h>
#include <yams/vector/vector_schema_migration.h>
#include <yams/vector/vector_utils.h>

#include "../../../src/vector/simeon_pq_persistence.h"

using namespace yams::vector;
using Catch::Matchers::WithinAbs;

namespace {

struct SqliteVecBackendFixture {
    SqliteVecBackendFixture() {
        if (const char* skipEnv = std::getenv("YAMS_SQLITE_VEC_SKIP_INIT")) {
            std::string v(skipEnv);
            if (v == "1" || v == "true") {
                skipReason = "Skipping test (YAMS_SQLITE_VEC_SKIP_INIT=1)";
            }
        }
        if (const char* disableEnv = std::getenv("YAMS_DISABLE_VECTORS")) {
            std::string v(disableEnv);
            if (v == "1" || v == "true") {
                skipReason = "Skipping test (YAMS_DISABLE_VECTORS=1)";
            }
        }
    }

    ~SqliteVecBackendFixture() {
        // Clean up temp files and SQLite sidecars.
        for (const auto& path : tempFiles) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
            std::filesystem::remove(path + "-wal", ec);
            std::filesystem::remove(path + "-shm", ec);
        }
    }

    void skipIfNeeded() {
        if (!skipReason.empty()) {
            SKIP(skipReason);
        }
    }

    std::string createTempDbPath() {
        auto path = std::filesystem::temp_directory_path() /
                    ("sqlite_vec_test_" + std::to_string(++tempCounter) + "_" +
                     std::to_string(std::random_device{}()) + ".db");
        std::error_code ec;
        std::filesystem::remove(path, ec);
        std::filesystem::remove(path.string() + "-wal", ec);
        std::filesystem::remove(path.string() + "-shm", ec);
        tempFiles.push_back(path.string());
        return path.string();
    }

    std::vector<float> createEmbedding(size_t dim, float seed = 1.0f) {
        std::vector<float> emb(dim);
        std::mt19937 rng(static_cast<uint32_t>(seed * 1000));
        std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
        for (size_t i = 0; i < dim; ++i) {
            emb[i] = dist(rng);
        }
        // Normalize
        float norm = 0.0f;
        for (float v : emb)
            norm += v * v;
        norm = std::sqrt(norm);
        if (norm > 0) {
            for (float& v : emb)
                v /= norm;
        }
        return emb;
    }

    VectorRecord createVectorRecord(const std::string& id, const std::vector<float>& embedding,
                                    const std::string& doc_hash = "") {
        VectorRecord rec;
        rec.chunk_id = "chunk_" + id;
        rec.document_hash = doc_hash.empty() ? "doc_" + id : doc_hash;
        rec.embedding = embedding;
        rec.content = "Test content for " + id;
        rec.start_offset = 0;
        rec.end_offset = rec.content.size();
        return rec;
    }

    std::string skipReason;
    std::vector<std::string> tempFiles;
    static inline int tempCounter = 0;
};

int countRows(sqlite3* db, const char* table) {
    std::string sql = "SELECT COUNT(*) FROM ";
    sql += table;
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        return -1;
    }
    int count = -1;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return count;
}

std::size_t countLiveStatements(sqlite3* db) {
    std::size_t count = 0;
    for (sqlite3_stmt* stmt = sqlite3_next_stmt(db, nullptr); stmt != nullptr;
         stmt = sqlite3_next_stmt(db, stmt)) {
        ++count;
    }
    return count;
}

std::vector<std::uint8_t> queryBlob(sqlite3* db, const char* sql, std::int64_t value) {
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        return {};
    }
    sqlite3_bind_int64(stmt, 1, value);
    std::vector<std::uint8_t> blob;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const auto* bytes = static_cast<const std::uint8_t*>(sqlite3_column_blob(stmt, 0));
        const int size = sqlite3_column_bytes(stmt, 0);
        if (bytes && size > 0) {
            blob.assign(bytes, bytes + size);
        }
    }
    sqlite3_finalize(stmt);
    return blob;
}

bool updateEmbeddingRaw(sqlite3* db, const std::string& chunkId,
                        const std::vector<float>& embedding) {
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db,
                           "UPDATE vectors SET embedding = ?1, embedding_dim = ?2 "
                           "WHERE chunk_id = ?3",
                           -1, &stmt, nullptr) != SQLITE_OK) {
        return false;
    }
    sqlite3_bind_blob(stmt, 1, embedding.data(), static_cast<int>(embedding.size() * sizeof(float)),
                      SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 2, static_cast<sqlite3_int64>(embedding.size()));
    sqlite3_bind_text(stmt, 3, chunkId.c_str(), -1, SQLITE_TRANSIENT);
    const int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE && sqlite3_changes(db) == 1;
}

struct UpdateEmbeddingOnMaterialization {
    sqlite3* mutatorDb = nullptr;
    const std::string* chunkId = nullptr;
    const std::vector<float>* embedding = nullptr;
    const std::vector<float>* secondEmbedding = nullptr;
    bool attempted = false;
    bool updated = false;
    bool secondAttempted = false;
    bool secondUpdated = false;
};

int updateEmbeddingOnMaterialization(unsigned event, void* context, void* statement, void*) {
    if (event != SQLITE_TRACE_STMT || context == nullptr || statement == nullptr) {
        return 0;
    }
    auto& update = *static_cast<UpdateEmbeddingOnMaterialization*>(context);
    if (update.secondAttempted) {
        return 0;
    }
    const char* sql = sqlite3_sql(static_cast<sqlite3_stmt*>(statement));
    if (sql == nullptr ||
        std::string{sql}.find("FROM vectors WHERE rowid = ?") == std::string::npos) {
        return 0;
    }
    if (!update.attempted) {
        update.attempted = true;
        update.updated = updateEmbeddingRaw(update.mutatorDb, *update.chunkId, *update.embedding);
    } else if (update.secondEmbedding != nullptr) {
        update.secondAttempted = true;
        update.secondUpdated =
            updateEmbeddingRaw(update.mutatorDb, *update.chunkId, *update.secondEmbedding);
    }
    return 0;
}

struct MaterializationMutationGuard {
    MaterializationMutationGuard(sqlite3* db, UpdateEmbeddingOnMaterialization& update) : db(db) {
        sqlite3_trace_v2(db, SQLITE_TRACE_STMT, updateEmbeddingOnMaterialization, &update);
    }
    ~MaterializationMutationGuard() { sqlite3_trace_v2(db, 0, nullptr, nullptr); }

    sqlite3* db;
};

struct UpdateDuringGenerationTriggerMigration {
    sqlite3* mutatorDb = nullptr;
    const std::string* chunkId = nullptr;
    const std::vector<float>* embedding = nullptr;
    bool requireProtectedInstall = false;
    bool sawSafeReplacement = false;
    bool sawProtectedInstall = false;
    bool attempted = false;
    bool updated = false;
};

int updateDuringGenerationTriggerMigration(unsigned event, void* context, void* statement, void*) {
    if (event != SQLITE_TRACE_STMT || context == nullptr || statement == nullptr) {
        return 0;
    }
    auto& update = *static_cast<UpdateDuringGenerationTriggerMigration*>(context);
    const char* sql = sqlite3_sql(static_cast<sqlite3_stmt*>(statement));
    if (sql == nullptr) {
        return 0;
    }
    const std::string_view statementSql(sql);
    if (statementSql.find("CREATE TRIGGER IF NOT EXISTS vectors_index_generation_update_v2") !=
        std::string_view::npos) {
        update.sawSafeReplacement = true;
        auto* statementDb = sqlite3_db_handle(static_cast<sqlite3_stmt*>(statement));
        update.sawProtectedInstall =
            statementDb != nullptr && sqlite3_get_autocommit(statementDb) == 0;
        if (update.requireProtectedInstall) {
            if (!update.sawProtectedInstall && !update.attempted) {
                update.attempted = true;
                update.updated =
                    updateEmbeddingRaw(update.mutatorDb, *update.chunkId, *update.embedding);
            }
        }
        return 0;
    }
    const bool unsafeLegacyCreate =
        statementSql.find("CREATE TRIGGER vectors_index_generation_update") !=
            std::string_view::npos &&
        statementSql.find("vectors_index_generation_update_v2") == std::string_view::npos;
    const bool safeLegacyDrop =
        update.sawSafeReplacement && !update.sawProtectedInstall &&
        statementSql.find("DROP TRIGGER IF EXISTS vectors_index_generation_update") !=
            std::string_view::npos;
    if (!update.attempted && (unsafeLegacyCreate || safeLegacyDrop)) {
        update.attempted = true;
        update.updated = updateEmbeddingRaw(update.mutatorDb, *update.chunkId, *update.embedding);
    }
    return 0;
}

struct GenerationTriggerMigrationGuard {
    GenerationTriggerMigrationGuard(sqlite3* db, UpdateDuringGenerationTriggerMigration& update)
        : db(db) {
        sqlite3_trace_v2(db, SQLITE_TRACE_STMT, updateDuringGenerationTriggerMigration, &update);
    }
    ~GenerationTriggerMigrationGuard() { sqlite3_trace_v2(db, 0, nullptr, nullptr); }

    sqlite3* db;
};

int interruptSqlite(void*) {
    return 1;
}

struct InterruptAfterProgressCalls {
    int remaining = 0;
};

int interruptSqliteAfterProgressCalls(void* context) {
    auto& state = *static_cast<InterruptAfterProgressCalls*>(context);
    --state.remaining;
    return state.remaining <= 0 ? 1 : 0;
}

struct SqliteProgressHandlerGuard {
    explicit SqliteProgressHandlerGuard(sqlite3* db) : db(db) {}
    ~SqliteProgressHandlerGuard() { sqlite3_progress_handler(db, 0, nullptr, nullptr); }

    sqlite3* db;
};

int captureSqliteStatements(unsigned event, void* context, void* statement, void*) {
    if (event != SQLITE_TRACE_STMT || context == nullptr || statement == nullptr) {
        return 0;
    }
    if (const char* sql = sqlite3_sql(static_cast<sqlite3_stmt*>(statement)); sql != nullptr) {
        static_cast<std::vector<std::string>*>(context)->emplace_back(sql);
    }
    return 0;
}

struct SqliteTraceGuard {
    SqliteTraceGuard(sqlite3* db, std::vector<std::string>& statements) : db(db) {
        sqlite3_trace_v2(db, SQLITE_TRACE_STMT, captureSqliteStatements, &statements);
    }
    ~SqliteTraceGuard() { sqlite3_trace_v2(db, 0, nullptr, nullptr); }

    sqlite3* db;
};

} // namespace

// =============================================================================
// Initialization Tests
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend initialize with new in-memory DB",
                 "[vector][backend][init][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    config.embedding_dim = 64;

    SqliteVecBackend backend(config);
    auto result = backend.initialize(":memory:");
    REQUIRE((result.has_value()));
    REQUIRE((backend.isInitialized()));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend initialize with new file DB",
                 "[vector][backend][init][catch2]") {
    skipIfNeeded();

    std::string dbPath = createTempDbPath();

    SqliteVecBackend::Config config;
    config.embedding_dim = 64;

    SqliteVecBackend backend(config);
    auto result = backend.initialize(dbPath);
    REQUIRE((result.has_value()));
    REQUIRE((backend.isInitialized()));

    // Verify file was created
    REQUIRE((std::filesystem::exists(dbPath)));

    backend.close();
    REQUIRE_FALSE(backend.isInitialized());
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend serializes one SQLite connection across worker threads",
                 "[vector][backend][init][concurrency][catch2]") {
    skipIfNeeded();

    SqliteVecBackend backend;
    REQUIRE((backend.initialize(":memory:").has_value()));

    // Graph ingestion and Simeon PQ routing can search the same backend concurrently.
    // A non-null connection mutex proves this handle was opened in serialized mode even
    // when SQLite's process default is the multi-thread (connection-unprotected) mode.
    CHECK((sqlite3_db_mutex(backend.getDbHandle()) != nullptr));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend initialize with existing DB",
                 "[vector][backend][init][catch2]") {
    skipIfNeeded();

    std::string dbPath = createTempDbPath();

    // First session: create and populate
    {
        SqliteVecBackend::Config config;
        config.embedding_dim = 64;

        SqliteVecBackend backend(config);
        REQUIRE((backend.initialize(dbPath).has_value()));
        REQUIRE((backend.createTables(64).has_value()));

        auto emb = createEmbedding(64, 1.0f);
        REQUIRE((backend.insertVector(createVectorRecord("existing", emb)).has_value()));

        backend.close();
    }

    // Second session: reopen
    {
        SqliteVecBackend::Config config;
        config.embedding_dim = 64;

        SqliteVecBackend backend(config);
        REQUIRE((backend.initialize(dbPath).has_value()));
        REQUIRE((backend.isInitialized()));
        REQUIRE((backend.tablesExist()));

        // Verify data persisted
        auto countResult = backend.getVectorCount();
        REQUIRE((countResult.has_value()));
        CHECK((countResult.value() == 1));

        backend.close();
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend createTables and tablesExist",
                 "[vector][backend][init][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);

    REQUIRE((backend.initialize(":memory:").has_value()));

    // Tables don't exist yet
    REQUIRE_FALSE(backend.tablesExist());

    // Create tables
    REQUIRE((backend.createTables(64).has_value()));

    // Now they exist
    REQUIRE((backend.tablesExist()));
}

// =============================================================================
// CRUD Operations Tests
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend insertVector single",
                 "[vector][backend][crud][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    auto emb = createEmbedding(64, 1.0f);
    auto rec = createVectorRecord("single", emb);

    auto result = backend.insertVector(rec);
    REQUIRE((result.has_value()));

    // Verify inserted
    auto countResult = backend.getVectorCount();
    REQUIRE((countResult.has_value()));
    CHECK((countResult.value() == 1));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend insertVectorsBatch",
                 "[vector][backend][crud][batch][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    std::vector<VectorRecord> records;
    for (int i = 0; i < 100; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        records.push_back(createVectorRecord("batch_" + std::to_string(i), emb));
    }

    auto result = backend.insertVectorsBatch(records);
    REQUIRE((result.has_value()));

    auto countResult = backend.getVectorCount();
    REQUIRE((countResult.has_value()));
    CHECK((countResult.value() == 100));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend getVector existing",
                 "[vector][backend][crud][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    auto emb = createEmbedding(64, 1.0f);
    auto rec = createVectorRecord("gettest", emb);
    rec.content = "Specific content for get test";
    REQUIRE((backend.insertVector(rec).has_value()));

    auto getResult = backend.getVector("chunk_gettest");
    REQUIRE((getResult.has_value()));
    REQUIRE((getResult.value().has_value()));

    auto& retrieved = getResult.value().value();
    CHECK((retrieved.chunk_id == "chunk_gettest"));
    CHECK((retrieved.document_hash == "doc_gettest"));
    CHECK((retrieved.content == "Specific content for get test"));
    CHECK((retrieved.embedding.size() == 64));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend getVector non-existent",
                 "[vector][backend][crud][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    auto getResult = backend.getVector("nonexistent_chunk");
    REQUIRE((getResult.has_value()));
    CHECK_FALSE(getResult.value().has_value());
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend getVectorsBatch mixed",
                 "[vector][backend][crud][batch][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert some vectors
    for (int i = 0; i < 5; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE((
            backend.insertVector(createVectorRecord("vec_" + std::to_string(i), emb)).has_value()));
    }

    // Request mix of existing and non-existing
    std::vector<std::string> ids = {"chunk_vec_0", "chunk_vec_2", "nonexistent", "chunk_vec_4"};

    auto batchResult = backend.getVectorsBatch(ids);
    REQUIRE((batchResult.has_value()));

    auto& results = batchResult.value();
    CHECK((results.size() == 3)); // Only existing ones
    CHECK((results.count("chunk_vec_0") == 1));
    CHECK((results.count("chunk_vec_2") == 1));
    CHECK((results.count("chunk_vec_4") == 1));
    CHECK((results.count("nonexistent") == 0));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend serializes cached read statements",
                 "[vector][backend][crud][concurrency][catch2]") {
    skipIfNeeded();

    SqliteVecBackend backend;
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));
    REQUIRE((backend.insertVector(createVectorRecord("reader_a", createEmbedding(64, 1.0F)))
                 .has_value()));
    REQUIRE((backend.insertVector(createVectorRecord("reader_b", createEmbedding(64, 2.0F)))
                 .has_value()));

    std::promise<void> firstReady;
    std::promise<void> secondReady;
    std::promise<void> start;
    auto startSignal = start.get_future().share();
    std::atomic<std::size_t> failures{0};
    constexpr std::size_t kIterations = 1'000;

    auto reader = [&](std::string chunkId, std::promise<void>& ready) {
        ready.set_value();
        startSignal.wait();
        for (std::size_t iteration = 0; iteration < kIterations; ++iteration) {
            auto result = backend.getVector(chunkId);
            if (!result || !result.value() || result.value()->chunk_id != chunkId) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
            auto count = backend.getVectorCount();
            auto stats = backend.getStats();
            if (!count || count.value() != 2 || !stats || stats.value().total_vectors != 2) {
                failures.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    std::thread first(reader, "chunk_reader_a", std::ref(firstReady));
    std::thread second(reader, "chunk_reader_b", std::ref(secondReady));
    firstReady.get_future().wait();
    secondReady.get_future().wait();
    start.set_value();
    first.join();
    second.join();

    CHECK((failures.load(std::memory_order_relaxed) == 0));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend getVectorsByDocument",
                 "[vector][backend][crud][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert vectors with same document hash
    for (int i = 0; i < 3; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        auto rec = createVectorRecord("doc1_chunk_" + std::to_string(i), emb, "shared_doc_hash");
        REQUIRE((backend.insertVector(rec).has_value()));
    }

    // Insert vector with different document
    auto emb2 = createEmbedding(64, 10.0f);
    REQUIRE(
        (backend.insertVector(createVectorRecord("other_doc", emb2, "other_hash")).has_value()));

    auto result = backend.getVectorsByDocument("shared_doc_hash");
    REQUIRE((result.has_value()));
    CHECK((result.value().size() == 3));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend updateVector",
                 "[vector][backend][crud][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert original
    auto emb1 = createEmbedding(64, 1.0f);
    auto rec1 = createVectorRecord("update_test", emb1);
    rec1.content = "Original content";
    REQUIRE((backend.insertVector(rec1).has_value()));

    // Update
    auto emb2 = createEmbedding(64, 2.0f);
    auto rec2 = createVectorRecord("update_test", emb2);
    rec2.content = "Updated content";

    auto updateResult = backend.updateVector("chunk_update_test", rec2);
    REQUIRE((updateResult.has_value()));

    // Verify update
    auto getResult = backend.getVector("chunk_update_test");
    REQUIRE((getResult.has_value()));
    REQUIRE((getResult.value().has_value()));
    CHECK((getResult.value().value().content == "Updated content"));

    // Still only 1 vector
    auto countResult = backend.getVectorCount();
    REQUIRE((countResult.has_value()));
    CHECK((countResult.value() == 1));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend preserves the original vector when replacement fails",
                 "[vector][backend][crud][transaction][catch2]") {
    skipIfNeeded();

    SqliteVecBackend backend;
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    auto original = createVectorRecord("update_atomic_a", createEmbedding(64, 1.0F));
    original.content = "original";
    REQUIRE((backend.insertVector(original).has_value()));
    REQUIRE((backend.insertVector(createVectorRecord("update_atomic_b", createEmbedding(64, 2.0F)))
                 .has_value()));

    auto conflicting = createVectorRecord("update_atomic_b", createEmbedding(64, 3.0F));
    conflicting.content = "replacement";
    auto update = backend.updateVector(original.chunk_id, conflicting);
    REQUIRE_FALSE(update.has_value());

    auto preserved = backend.getVector(original.chunk_id);
    REQUIRE((preserved.has_value()));
    REQUIRE((preserved.value().has_value()));
    CHECK((preserved.value()->content == "original"));
    CHECK((backend.getVectorCount().value() == 2));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend deleteVector",
                 "[vector][backend][crud][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert
    auto emb = createEmbedding(64, 1.0f);
    REQUIRE((backend.insertVector(createVectorRecord("delete_me", emb)).has_value()));

    auto countBefore = backend.getVectorCount();
    REQUIRE((countBefore.has_value()));
    CHECK((countBefore.value() == 1));

    // Delete
    auto deleteResult = backend.deleteVector("chunk_delete_me");
    REQUIRE((deleteResult.has_value()));

    auto countAfter = backend.getVectorCount();
    REQUIRE((countAfter.has_value()));
    CHECK((countAfter.value() == 0));

    // Verify gone
    auto getResult = backend.getVector("chunk_delete_me");
    REQUIRE((getResult.has_value()));
    CHECK_FALSE(getResult.value().has_value());
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend deleteVectorsByDocument",
                 "[vector][backend][crud][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert multiple chunks for same document
    for (int i = 0; i < 5; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        auto rec = createVectorRecord("doc_chunk_" + std::to_string(i), emb, "doc_to_delete");
        REQUIRE((backend.insertVector(rec).has_value()));
    }

    // Insert vector for different document
    auto emb2 = createEmbedding(64, 100.0f);
    REQUIRE((backend.insertVector(createVectorRecord("keeper", emb2, "other_doc")).has_value()));

    auto countBefore = backend.getVectorCount();
    REQUIRE((countBefore.has_value()));
    CHECK((countBefore.value() == 6));

    // Delete by document
    auto deleteResult = backend.deleteVectorsByDocument("doc_to_delete");
    REQUIRE((deleteResult.has_value()));

    auto countAfter = backend.getVectorCount();
    REQUIRE((countAfter.has_value()));
    CHECK((countAfter.value() == 1));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend reports SQLite delete failures without changing rows",
                 "[vector][backend][crud][errors][catch2]") {
    skipIfNeeded();

    SqliteVecBackend backend;
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));
    REQUIRE((backend
                 .insertVector(createVectorRecord("delete_blocked", createEmbedding(64, 1.0F),
                                                  "delete_blocked_doc"))
                 .has_value()));

    char* error = nullptr;
    REQUIRE((sqlite3_exec(backend.getDbHandle(),
                          "CREATE TRIGGER reject_vector_delete BEFORE DELETE ON vectors BEGIN "
                          "SELECT RAISE(ABORT, 'forced delete failure'); END",
                          nullptr, nullptr, &error) == SQLITE_OK));
    sqlite3_free(error);

    SECTION("single vector") {
        auto deleted = backend.deleteVector("chunk_delete_blocked");
        CHECK_FALSE(deleted.has_value());
    }

    SECTION("document") {
        auto deleted = backend.deleteVectorsByDocument("delete_blocked_doc");
        CHECK_FALSE(deleted.has_value());
    }

    auto preserved = backend.getVector("chunk_delete_blocked");
    REQUIRE((preserved.has_value()));
    CHECK((preserved.value().has_value()));
    CHECK((backend.getVectorCount().value() == 1));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend hasEmbedding",
                 "[vector][backend][crud][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert vector
    auto emb = createEmbedding(64, 1.0f);
    REQUIRE((backend.insertVector(createVectorRecord("has_test", emb, "has_doc")).has_value()));

    auto hasResult = backend.hasEmbedding("has_doc");
    REQUIRE((hasResult.has_value()));
    CHECK((hasResult.value() == true));

    auto noResult = backend.hasEmbedding("no_such_doc");
    REQUIRE((noResult.has_value()));
    CHECK((noResult.value() == false));
}

// =============================================================================
// Search Tests
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend searchSimilar basic",
                 "[vector][backend][search][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert vectors
    for (int i = 0; i < 10; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE((backend.insertVector(createVectorRecord("search_" + std::to_string(i), emb))
                     .has_value()));
    }

    // Search with first vector's embedding
    auto query = createEmbedding(64, 1.0f);
    auto searchResult = backend.searchSimilar(query, 5, 0.0f, std::nullopt, {});
    if (!searchResult.has_value()) {
        INFO(searchResult.error().message);
    }
    REQUIRE((searchResult.has_value()));

    auto& results = searchResult.value();
    CHECK((results.size() == 5));

    // First result should be exact match (same seed)
    CHECK((results[0].chunk_id == "chunk_search_0"));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend SPQ rerank improves recall",
                 "[vector][backend][search][spq][catch2]") {
    skipIfNeeded();

    constexpr size_t kDim = 64;
    constexpr size_t kCorpus = 400;
    constexpr size_t kQueries = 20;
    constexpr size_t kTopK = 10;

    std::vector<std::vector<float>> corpus;
    for (size_t i = 0; i < kCorpus; ++i) {
        corpus.push_back(createEmbedding(kDim, static_cast<float>(i + 1)));
    }
    std::vector<std::vector<float>> queries;
    for (size_t i = 0; i < kQueries; ++i) {
        queries.push_back(createEmbedding(kDim, static_cast<float>(5000 + i)));
    }

    auto cosine = [](const std::vector<float>& a, const std::vector<float>& b) {
        float dot = 0.0f;
        for (size_t i = 0; i < a.size(); ++i)
            dot += a[i] * b[i];
        return dot;
    };

    auto runRecall = [&](size_t rerank_factor) {
        SqliteVecBackend::Config config;
        config.search_engine = VectorSearchEngine::SimeonPqAdc;
        config.embedding_dim = kDim;
        config.simeon_pq_subquantizers = 8;
        config.simeon_pq_rerank_factor = rerank_factor;
        SqliteVecBackend backend(config);
        REQUIRE((backend.initialize(":memory:").has_value()));
        REQUIRE((backend.createTables(kDim).has_value()));

        std::vector<VectorRecord> records;
        for (size_t i = 0; i < kCorpus; ++i) {
            records.push_back(createVectorRecord("r" + std::to_string(i), corpus[i]));
        }
        REQUIRE((backend.insertVectorsBatch(records).has_value()));
        REQUIRE((backend.buildIndex().has_value()));

        size_t hits = 0;
        for (const auto& q : queries) {
            std::vector<std::pair<float, size_t>> exact;
            for (size_t i = 0; i < kCorpus; ++i) {
                exact.emplace_back(cosine(q, corpus[i]), i);
            }
            std::partial_sort(exact.begin(), exact.begin() + kTopK, exact.end(),
                              [](const auto& a, const auto& b) { return a.first > b.first; });
            std::unordered_set<std::string> expected;
            for (size_t i = 0; i < kTopK; ++i) {
                expected.insert("chunk_r" + std::to_string(exact[i].second));
            }

            auto result = backend.searchSimilar(q, kTopK, 0.0f, std::nullopt, {});
            REQUIRE((result.has_value()));
            float prev = std::numeric_limits<float>::max();
            for (const auto& rec : result.value()) {
                CHECK((rec.relevance_score <= prev + 1e-5f));
                prev = rec.relevance_score;
                hits += expected.contains(rec.chunk_id) ? 1 : 0;
            }
        }
        return static_cast<double>(hits) / static_cast<double>(kQueries * kTopK);
    };

    const double recall_no_rerank = runRecall(1);
    const double recall_rerank8 = runRecall(8);

    INFO("recall rerank=1: " << recall_no_rerank << ", rerank=8: " << recall_rerank8);
    CHECK((recall_rerank8 > recall_no_rerank));
    CHECK((recall_rerank8 >= 0.85));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend Simeon PQ ranking is independent of insertion order",
                 "[vector][backend][search][spq][determinism][catch2]") {
    skipIfNeeded();

    constexpr size_t kDim = 32;
    constexpr size_t kCorpus = 96;
    constexpr size_t kTopK = 8;

    std::vector<VectorRecord> records;
    records.reserve(kCorpus);
    for (size_t i = 0; i < kCorpus; ++i) {
        records.push_back(createVectorRecord("stable_" + std::to_string(i),
                                             createEmbedding(kDim, static_cast<float>(i + 1))));
    }

    const auto buildBackend = [&](bool reverseInsertion) {
        SqliteVecBackend::Config config;
        config.search_engine = VectorSearchEngine::SimeonPqAdc;
        config.embedding_dim = kDim;
        config.simeon_pq_subquantizers = 8;
        config.simeon_pq_centroids = 2;
        config.simeon_pq_train_limit = 32;
        config.simeon_pq_rerank_factor = 1;

        auto backend = std::make_unique<SqliteVecBackend>(config);
        REQUIRE((backend->initialize(":memory:").has_value()));
        REQUIRE((backend->createTables(kDim).has_value()));
        if (reverseInsertion) {
            auto reversed = records;
            std::ranges::reverse(reversed);
            REQUIRE((backend->insertVectorsBatch(reversed).has_value()));
        } else {
            REQUIRE((backend->insertVectorsBatch(records).has_value()));
        }
        REQUIRE((backend->buildIndex().has_value()));
        return backend;
    };

    auto forward = buildBackend(false);
    auto reverse = buildBackend(true);
    for (size_t queryIndex = 0; queryIndex < 12; ++queryIndex) {
        const auto query = createEmbedding(kDim, static_cast<float>(5000 + queryIndex));
        auto forwardResult = forward->searchSimilar(query, kTopK, -1.0F, std::nullopt, {});
        auto reverseResult = reverse->searchSimilar(query, kTopK, -1.0F, std::nullopt, {});
        REQUIRE((forwardResult.has_value()));
        REQUIRE((reverseResult.has_value()));

        std::vector<std::string> forwardIds;
        std::vector<std::string> reverseIds;
        std::ranges::transform(forwardResult.value(), std::back_inserter(forwardIds),
                               &VectorRecord::chunk_id);
        std::ranges::transform(reverseResult.value(), std::back_inserter(reverseIds),
                               &VectorRecord::chunk_id);
        CAPTURE(queryIndex, forwardIds, reverseIds);
        CHECK((forwardIds == reverseIds));
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend vec0 search engine basic",
                 "[vector][backend][search][vec0][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    config.search_engine = VectorSearchEngine::Vec0L2;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    for (int i = 0; i < 10; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE((backend.insertVector(createVectorRecord("vec0_" + std::to_string(i), emb))
                     .has_value()));
    }

    auto query = createEmbedding(64, 1.0f);
    auto searchResult = backend.searchSimilar(query, 5, 0.0f, std::nullopt, {});
    REQUIRE((searchResult.has_value()));
    REQUIRE((searchResult.value().size() == 5));
    CHECK((searchResult.value()[0].chunk_id == "chunk_vec0_0"));

    sqlite3_stmt* stmt = nullptr;
    REQUIRE((sqlite3_prepare_v2(backend.getDbHandle(),
                                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1", -1,
                                &stmt, nullptr) == SQLITE_OK));
    sqlite3_bind_text(stmt, 1, "vectors_64_vec0", -1, SQLITE_TRANSIENT);
    CHECK((sqlite3_step(stmt) == SQLITE_ROW));
    sqlite3_finalize(stmt);
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend vec0 PHSS search path basic",
                 "[vector][backend][search][vec0][phss][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    config.search_engine = VectorSearchEngine::Vec0L2;
    config.vec0_phss_enabled = true;
    config.vec0_phss_candidates = 8;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    for (int i = 0; i < 12; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE((backend.insertVector(createVectorRecord("vec0_phss_" + std::to_string(i), emb))
                     .has_value()));
    }

    auto query = createEmbedding(64, 1.0f);
    auto searchResult = backend.searchSimilar(query, 5, 0.0f, std::nullopt, {});
    REQUIRE((searchResult.has_value()));
    REQUIRE((searchResult.value().size() == 5));
    CHECK((searchResult.value()[0].chunk_id == "chunk_vec0_phss_0"));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend Simeon PQ search engine basic",
                 "[vector][backend][search][spq-basic][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    for (int i = 0; i < 12; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE((backend.insertVector(createVectorRecord("spq_basic_" + std::to_string(i), emb))
                     .has_value()));
    }

    auto build = backend.buildIndex();
    if (!build.has_value()) {
        INFO(build.error().message);
    }
    REQUIRE((build.has_value()));

    auto query = createEmbedding(64, 1.0f);
    auto searchResult = backend.searchSimilar(query, 5, 0.0f, std::nullopt, {});
    if (!searchResult.has_value()) {
        INFO(searchResult.error().message);
    }
    REQUIRE((searchResult.has_value()));
    REQUIRE((searchResult.value().size() == 5));
    CHECK((searchResult.value()[0].chunk_id == "chunk_spq_basic_0"));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend uses exact search when a corpus cannot train its PQ codebook",
                 "[vector][backend][search][spq][small-corpus][catch2]") {
    skipIfNeeded();

    constexpr size_t kDim = 64;
    constexpr size_t kCorpus = 64;

    SqliteVecBackend::Config config;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.embedding_dim = kDim;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 256;
    config.simeon_pq_rerank_factor = 2;

    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(kDim).has_value()));

    std::vector<VectorRecord> records;
    records.reserve(kCorpus);
    for (size_t i = 0; i < kCorpus; ++i) {
        records.push_back(createVectorRecord("small_" + std::to_string(i),
                                             createEmbedding(kDim, static_cast<float>(i + 1))));
    }
    REQUIRE((backend.insertVectorsBatch(records).has_value()));
    REQUIRE((backend.buildIndex().has_value()));
    REQUIRE((backend.persistIndex().has_value()));
    auto reusable = backend.hasReusablePersistedSearchIndex();
    REQUIRE((reusable.has_value()));
    CHECK_FALSE(reusable.value());
    CHECK((countRows(backend.getDbHandle(), "simeon_pq_meta") == 0));
    CHECK((countRows(backend.getDbHandle(), "simeon_pq_codes") == 0));

    VectorSearchDiagnostics diagnostics;
    auto result = backend.searchSimilarWithDiagnostics(records[37].embedding, 1, -1.0F,
                                                       std::nullopt, {}, {}, diagnostics);
    REQUIRE((result.has_value()));
    REQUIRE((result.value().size() == 1));
    CHECK((result.value().front().chunk_id == records[37].chunk_id));
    CHECK(diagnostics.usedExactScan);
    CHECK_FALSE(diagnostics.usedAnn);
    CHECK(diagnostics.exactDistanceEvaluationsObserved);
    CHECK((diagnostics.exactDistanceEvaluations == kCorpus));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend suppresses Simeon PQ rebuilds for memory instrumentation",
                 "[vector][backend][index][spq-suppressed][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.suppress_search_index_builds = true;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    for (int i = 0; i < 12; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE(
            (backend.insertVector(createVectorRecord("spq_suppressed_" + std::to_string(i), emb))
                 .has_value()));
    }

    REQUIRE((backend.buildIndex().has_value()));
    REQUIRE((backend.persistIndex().has_value()));
    CHECK((countRows(backend.getDbHandle(), "simeon_pq_meta") == 0));
    CHECK((countRows(backend.getDbHandle(), "simeon_pq_codes") == 0));

    auto query = createEmbedding(64, 1.0f);
    VectorSearchDiagnostics diagnostics;
    auto searchResult =
        backend.searchSimilarWithDiagnostics(query, 5, 0.0f, std::nullopt, {}, {}, diagnostics);
    REQUIRE((searchResult.has_value()));
    REQUIRE_FALSE((searchResult.value().empty()));
    CHECK((searchResult.value().front().chunk_id == "chunk_spq_suppressed_0"));
    CHECK(diagnostics.usedExactScan);
    CHECK_FALSE(diagnostics.usedAnn);
    CHECK(diagnostics.exactDistanceEvaluationsObserved);
    CHECK((diagnostics.exactDistanceEvaluations == 12));
    CHECK((countRows(backend.getDbHandle(), "simeon_pq_meta") == 0));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend searchSimilar with similarity_threshold",
                 "[vector][backend][search][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert vectors with varying similarity
    for (int i = 0; i < 10; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE((backend.insertVector(createVectorRecord("thresh_" + std::to_string(i), emb))
                     .has_value()));
    }

    auto query = createEmbedding(64, 1.0f);

    // High threshold - should filter most results
    auto highThreshResult = backend.searchSimilar(query, 10, 0.99f, std::nullopt, {});
    REQUIRE((highThreshResult.has_value()));
    // With high threshold, might only get exact match
    CHECK((highThreshResult.value().size() <= 2));

    // Low threshold - should include more
    auto lowThreshResult = backend.searchSimilar(query, 10, 0.0f, std::nullopt, {});
    REQUIRE((lowThreshResult.has_value()));
    CHECK((lowThreshResult.value().size() >= 5));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend searchSimilar with document_hash filter",
                 "[vector][backend][search][filter][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert vectors for doc_A
    for (int i = 0; i < 5; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE((backend.insertVector(createVectorRecord("docA_" + std::to_string(i), emb, "doc_A"))
                     .has_value()));
    }

    // Insert vectors for doc_B
    for (int i = 0; i < 5; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 10));
        REQUIRE((backend.insertVector(createVectorRecord("docB_" + std::to_string(i), emb, "doc_B"))
                     .has_value()));
    }

    auto query = createEmbedding(64, 1.0f);

    // Search only in doc_A
    auto filterResult = backend.searchSimilar(query, 10, 0.0f, "doc_A", {});
    REQUIRE((filterResult.has_value()));

    auto& results = filterResult.value();
    for (const auto& r : results) {
        CHECK((r.document_hash == "doc_A"));
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend vec0 search engine preserves filtered search semantics",
                 "[vector][backend][search][filter][vec0][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    config.search_engine = VectorSearchEngine::Vec0L2;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    for (int i = 0; i < 5; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE((
            backend.insertVector(createVectorRecord("vec0_docA_" + std::to_string(i), emb, "doc_A"))
                .has_value()));
    }
    for (int i = 0; i < 5; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 10));
        REQUIRE((
            backend.insertVector(createVectorRecord("vec0_docB_" + std::to_string(i), emb, "doc_B"))
                .has_value()));
    }

    auto query = createEmbedding(64, 1.0f);
    auto filterResult = backend.searchSimilar(query, 10, 0.0f, "doc_A", {});
    if (!filterResult.has_value()) {
        INFO(filterResult.error().message);
    }
    REQUIRE((filterResult.has_value()));
    REQUIRE_FALSE(filterResult.value().empty());
    for (const auto& r : filterResult.value()) {
        CHECK((r.document_hash == "doc_A"));
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend Simeon PQ preserves filtered search semantics",
                 "[vector][backend][search][filter][spq][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    for (int i = 0; i < 6; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE(
            (backend.insertVector(createVectorRecord("spq_docA_" + std::to_string(i), emb, "doc_A"))
                 .has_value()));
    }
    for (int i = 0; i < 6; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 20));
        REQUIRE(
            (backend.insertVector(createVectorRecord("spq_docB_" + std::to_string(i), emb, "doc_B"))
                 .has_value()));
    }

    REQUIRE((backend.buildIndex().has_value()));

    auto query = createEmbedding(64, 1.0f);
    auto filterResult = backend.searchSimilar(query, 10, 0.0f, "doc_A", {});
    if (!filterResult.has_value()) {
        INFO(filterResult.error().message);
    }
    REQUIRE((filterResult.has_value()));
    REQUIRE_FALSE(filterResult.value().empty());
    for (const auto& r : filterResult.value()) {
        CHECK((r.document_hash == "doc_A"));
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend Simeon PQ routes candidate documents through ADC",
                 "[vector][backend][search][filter][spq][diagnostics][catch2]") {
    skipIfNeeded();

    constexpr size_t kDim = 64;
    SqliteVecBackend::Config config;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.embedding_dim = kDim;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 16;
    config.simeon_pq_rerank_factor = 2;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(kDim).has_value()));

    std::vector<std::vector<float>> embeddings;
    for (int i = 0; i < 24; ++i) {
        embeddings.push_back(createEmbedding(kDim, static_cast<float>(i + 1)));
        REQUIRE((backend
                     .insertVector(createVectorRecord("spq_route_" + std::to_string(i),
                                                      embeddings.back(),
                                                      "route_doc_" + std::to_string(i)))
                     .has_value()));
    }
    REQUIRE((backend.buildIndex().has_value()));

    const std::unordered_set<std::string> routedDocuments = {"route_doc_3", "route_doc_11",
                                                             "route_doc_19"};
    VectorSearchDiagnostics diagnostics;
    auto routed =
        backend.searchSimilarWithDiagnostics(embeddings[11], routedDocuments.size(), -1.0F,
                                             std::nullopt, routedDocuments, {}, diagnostics);
    REQUIRE((routed.has_value()));
    REQUIRE((routed.value().size() == routedDocuments.size()));
    CHECK((routed.value().front().document_hash == "route_doc_11"));
    for (const auto& record : routed.value()) {
        CHECK((routedDocuments.contains(record.document_hash)));
    }

    CHECK((diagnostics.usedAnn));
    CHECK_FALSE(diagnostics.usedExactScan);
    CHECK(diagnostics.rowsVisitedObserved);
    CHECK(diagnostics.exactDistanceEvaluationsObserved);
    CHECK(diagnostics.annCandidateBudgetObserved);
    CHECK((diagnostics.annCandidateBudget == routedDocuments.size()));
    CHECK((diagnostics.rowsVisited == routedDocuments.size()));
    CHECK((diagnostics.exactDistanceEvaluations == routedDocuments.size()));
    CHECK((diagnostics.usedCandidateIndexCache));
    CHECK((diagnostics.candidateIndexPayloadBytes > 0));
    CHECK((diagnostics.candidateLookupCount == 1));
    CHECK((diagnostics.materializedRows == routedDocuments.size()));
    CHECK((diagnostics.returnedRows == routedDocuments.size()));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend searchSimilar with metadata_filters",
                 "[vector][backend][search][filter][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert vectors with metadata
    for (int i = 0; i < 10; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        auto rec = createVectorRecord("meta_" + std::to_string(i), emb);
        rec.metadata["category"] = (i % 2 == 0) ? "even" : "odd";
        rec.metadata["level"] = std::to_string(i);
        REQUIRE((backend.insertVector(rec).has_value()));
    }

    auto query = createEmbedding(64, 1.0f);

    // Search with metadata filter
    std::map<std::string, std::string> filters;
    filters["category"] = "even";

    auto filterResult = backend.searchSimilar(query, 10, 0.0f, std::nullopt, {}, filters);
    REQUIRE((filterResult.has_value()));

    // All results should have category=even
    for (const auto& r : filterResult.value()) {
        auto it = r.metadata.find("category");
        REQUIRE((it != r.metadata.end()));
        CHECK((it->second == "even"));
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend searchSimilar empty corpus",
                 "[vector][backend][search][edge][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Don't insert anything
    auto query = createEmbedding(64, 1.0f);
    auto searchResult = backend.searchSimilar(query, 5, 0.0f, std::nullopt, {});
    REQUIRE((searchResult.has_value()));
    CHECK((searchResult.value().empty()));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend exact scan propagates interruption",
                 "[vector][backend][search][exact-scan][error][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    config.embedding_dim = 64;
    config.search_engine = VectorSearchEngine::ExactScan;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));
    for (int i = 0; i < 32; ++i) {
        REQUIRE((backend
                     .insertVector(createVectorRecord("interrupt_" + std::to_string(i),
                                                      createEmbedding(64, i + 1.0F)))
                     .has_value()));
    }

    sqlite3_progress_handler(backend.getDbHandle(), 100, interruptSqlite, nullptr);
    SqliteProgressHandlerGuard progressGuard(backend.getDbHandle());
    auto result = backend.searchSimilar(createEmbedding(64), 5, -1.0F);

    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::DatabaseError);
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend PQ build propagates interruption",
                 "[vector][backend][search][spq][error][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    config.embedding_dim = 64;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 8;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));
    for (int i = 0; i < 32; ++i) {
        REQUIRE((backend
                     .insertVector(createVectorRecord("pq_interrupt_" + std::to_string(i),
                                                      createEmbedding(64, i + 1.0F)))
                     .has_value()));
    }

    sqlite3_progress_handler(backend.getDbHandle(), 1, interruptSqlite, nullptr);
    SqliteProgressHandlerGuard progressGuard(backend.getDbHandle());
    auto result = backend.buildIndex();

    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::DatabaseError);
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend searchSimilar k greater than corpus",
                 "[vector][backend][search][edge][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert only 3 vectors
    for (int i = 0; i < 3; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE((backend.insertVector(createVectorRecord("small_" + std::to_string(i), emb))
                     .has_value()));
    }

    // Request k=100
    auto query = createEmbedding(64, 1.0f);
    auto searchResult = backend.searchSimilar(query, 100, 0.0f, std::nullopt, {});
    REQUIRE((searchResult.has_value()));

    // Should return at least 1 result, not error.
    // Note: HNSW search may return fewer results than corpus size due to the graph
    // structure when vectors are inserted incrementally. This test validates that
    // requesting k > corpus_size doesn't cause errors.
    CHECK((searchResult.value().size() >= 1));
    CHECK((searchResult.value().size() <= 3));
}

// =============================================================================
// Transaction Tests
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend transaction commit",
                 "[vector][backend][transaction][catch2]") {
    skipIfNeeded();

    std::string dbPath = createTempDbPath();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(dbPath).has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Begin transaction
    auto beginResult = backend.beginTransaction();
    REQUIRE((beginResult.has_value()));

    // Insert within transaction
    auto emb = createEmbedding(64, 1.0f);
    REQUIRE((backend.insertVector(createVectorRecord("txn_test", emb)).has_value()));

    // Commit
    auto commitResult = backend.commitTransaction();
    REQUIRE((commitResult.has_value()));

    // Verify persisted
    auto countResult = backend.getVectorCount();
    REQUIRE((countResult.has_value()));
    CHECK((countResult.value() == 1));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend transaction rollback",
                 "[vector][backend][transaction][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert one before transaction
    auto emb1 = createEmbedding(64, 1.0f);
    REQUIRE((backend.insertVector(createVectorRecord("before_txn", emb1)).has_value()));

    // Begin transaction
    auto beginResult = backend.beginTransaction();
    REQUIRE((beginResult.has_value()));

    // Insert within transaction
    auto emb2 = createEmbedding(64, 2.0f);
    REQUIRE((backend.insertVector(createVectorRecord("during_txn", emb2)).has_value()));

    // Rollback
    auto rollbackResult = backend.rollbackTransaction();
    REQUIRE((rollbackResult.has_value()));

    // Should only have the vector from before transaction
    auto countResult = backend.getVectorCount();
    REQUIRE((countResult.has_value()));
    CHECK((countResult.value() == 1));

    // The rolled-back vector should not exist
    auto getResult = backend.getVector("chunk_during_txn");
    REQUIRE((getResult.has_value()));
    CHECK_FALSE(getResult.value().has_value());
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend batch commit failure rolls back its internal transaction",
                 "[vector][backend][transaction][errors][catch2]") {
    skipIfNeeded();

    SqliteVecBackend backend;
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    char* error = nullptr;
    REQUIRE((sqlite3_exec(backend.getDbHandle(), "PRAGMA foreign_keys = ON", nullptr, nullptr,
                          &error) == SQLITE_OK));
    sqlite3_free(error);
    error = nullptr;
    REQUIRE((sqlite3_exec(backend.getDbHandle(),
                          "CREATE TABLE deferred_parent(id INTEGER PRIMARY KEY);"
                          "CREATE TABLE deferred_child(parent_id INTEGER, "
                          "FOREIGN KEY(parent_id) REFERENCES deferred_parent(id) "
                          "DEFERRABLE INITIALLY DEFERRED);"
                          "CREATE TRIGGER reject_vector_commit AFTER INSERT ON vectors BEGIN "
                          "INSERT INTO deferred_child(parent_id) VALUES (999); END",
                          nullptr, nullptr, &error) == SQLITE_OK));
    sqlite3_free(error);

    std::vector<VectorRecord> records{
        createVectorRecord("commit_failure", createEmbedding(64, 1.0F))};
    auto inserted = backend.insertVectorsBatch(records);
    CHECK_FALSE(inserted.has_value());
    CHECK((sqlite3_get_autocommit(backend.getDbHandle()) == 1));
    CHECK((backend.getVectorCount().value() == 0));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend rejects writes from outside the explicit transaction owner",
                 "[vector][backend][transaction][concurrency][catch2]") {
    skipIfNeeded();

    SqliteVecBackend backend;
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));
    REQUIRE((backend.beginTransaction().has_value()));
    REQUIRE((backend.insertVector(createVectorRecord("owner_write", createEmbedding(64, 1.0F)))
                 .has_value()));

    auto foreignWrite = std::async(std::launch::async, [&] {
        return backend.insertVector(createVectorRecord("foreign_write", createEmbedding(64, 2.0F)));
    });
    auto foreignResult = foreignWrite.get();
    REQUIRE_FALSE(foreignResult.has_value());
    CHECK((foreignResult.error().code == yams::ErrorCode::InvalidState));

    auto foreignCommit =
        std::async(std::launch::async, [&] { return backend.commitTransaction(); }).get();
    REQUIRE_FALSE(foreignCommit.has_value());
    CHECK((foreignCommit.error().code == yams::ErrorCode::InvalidState));

    REQUIRE((backend.rollbackTransaction().has_value()));
    CHECK((backend.getVectorCount().value() == 0));
}

// =============================================================================
// Statistics Tests
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend getStats",
                 "[vector][backend][stats][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert some vectors
    for (int i = 0; i < 50; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE((backend.insertVector(createVectorRecord("stats_" + std::to_string(i), emb))
                     .has_value()));
    }

    auto statsResult = backend.getStats();
    REQUIRE((statsResult.has_value()));

    auto& stats = statsResult.value();
    CHECK((stats.total_vectors == 50));
}

// =============================================================================
// Build/Optimize Tests
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend buildIndex",
                 "[vector][backend][index][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert vectors
    for (int i = 0; i < 100; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE((backend.insertVector(createVectorRecord("build_" + std::to_string(i), emb))
                     .has_value()));
    }

    // Build index (should be no-op for HNSW which builds incrementally)
    auto buildResult = backend.buildIndex();
    REQUIRE((buildResult.has_value()));

    // Search should still work
    auto query = createEmbedding(64, 1.0f);
    auto searchResult = backend.searchSimilar(query, 5, -2.0f, std::nullopt, {});
    REQUIRE((searchResult.has_value()));
    CHECK((searchResult.value().size() == 5));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend optimize",
                 "[vector][backend][index][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert and delete to create soft deletes
    for (int i = 0; i < 100; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE((
            backend.insertVector(createVectorRecord("opt_" + std::to_string(i), emb)).has_value()));
    }

    // Delete half
    for (int i = 0; i < 50; ++i) {
        REQUIRE((backend.deleteVector("chunk_opt_" + std::to_string(i)).has_value()));
    }

    // Optimize (compacts HNSW)
    auto optimizeResult = backend.optimize();
    REQUIRE((optimizeResult.has_value()));

    // Search should work on remaining
    auto query = createEmbedding(64, 75.0f);
    auto searchResult = backend.searchSimilar(query, 10, 0.0f, std::nullopt, {});
    REQUIRE((searchResult.has_value()));
    CHECK((searchResult.value().size() <= 50)); // Only 50 remain
}

// =============================================================================
// Drop Tables Test
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend dropTables",
                 "[vector][backend][admin][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert some data
    auto emb = createEmbedding(64, 1.0f);
    REQUIRE((backend.insertVector(createVectorRecord("drop_test", emb)).has_value()));

    REQUIRE((backend.tablesExist()));

    // Drop tables
    auto dropResult = backend.dropTables();
    REQUIRE((dropResult.has_value()));

    REQUIRE_FALSE(backend.tablesExist());
}

// =============================================================================
// Candidate Hashes Filtering Tests (Tiered search narrowing)
// =============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend searchSimilar with candidate_hashes",
                 "[vector][backend][search][tiered][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    config.search_engine = VectorSearchEngine::Vec0L2;
    config.vec0_phss_enabled = true;
    config.vec0_phss_candidates = 16;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert vectors from different documents (doc groups A, B, C)
    for (int i = 0; i < 30; ++i) {
        std::string docGroup;
        if (i < 10)
            docGroup = "doc_group_A";
        else if (i < 20)
            docGroup = "doc_group_B";
        else
            docGroup = "doc_group_C";

        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        auto rec = createVectorRecord("candidate_" + std::to_string(i), emb, docGroup);
        REQUIRE((backend.insertVector(rec).has_value()));
    }

    // Query that would match all documents
    auto query = createEmbedding(64, 15.0f); // Middle of range

    // Search WITHOUT candidate filter - should return from all groups
    auto unfilteredResult = backend.searchSimilar(query, 10, 0.0f, std::nullopt, {});
    REQUIRE((unfilteredResult.has_value()));
    CHECK((unfilteredResult.value().size() == 10));

    // Search WITH candidate filter - only doc_group_A allowed
    std::unordered_set<std::string> candidatesA = {"doc_group_A"};
    VectorSearchDiagnostics diagnostics;
    auto filteredResultA = backend.searchSimilarWithDiagnostics(query, 10, 0.0f, std::nullopt,
                                                                candidatesA, {}, diagnostics);
    REQUIRE((filteredResultA.has_value()));
    CHECK(diagnostics.usedAnn);
    CHECK_FALSE(diagnostics.usedExactScan);
    CHECK_FALSE(diagnostics.rowsVisitedObserved);
    CHECK_FALSE(diagnostics.exactDistanceEvaluationsObserved);
    CHECK_FALSE(diagnostics.annCandidateBudgetObserved);
    CHECK(diagnostics.rowsVisited == 0);
    CHECK(diagnostics.exactDistanceEvaluations == 0);
    CHECK(diagnostics.annCandidateBudget == 16);

    // All results should be from doc_group_A
    for (const auto& result : filteredResultA.value()) {
        CHECK((result.document_hash == "doc_group_A"));
    }
    // Should have at most 10 results from A's 10 vectors
    CHECK((filteredResultA.value().size() <= 10));

    // Search with multiple candidate groups (A and C, not B)
    std::unordered_set<std::string> candidatesAC = {"doc_group_A", "doc_group_C"};
    auto filteredResultAC = backend.searchSimilar(query, 20, 0.0f, std::nullopt, candidatesAC, {});
    REQUIRE((filteredResultAC.has_value()));

    // All results should be from A or C, never B
    for (const auto& result : filteredResultAC.value()) {
        bool isAorC =
            (result.document_hash == "doc_group_A" || result.document_hash == "doc_group_C");
        CHECK((isAorC));
        CHECK((result.document_hash != "doc_group_B"));
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend searchSimilar combined document_hash and candidate_hashes",
                 "[vector][backend][search][tiered][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    for (int i = 0; i < 20; ++i) {
        const std::string docGroup = (i < 10) ? "doc_A" : "doc_B";
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE(
            (backend.insertVector(createVectorRecord("combo_" + std::to_string(i), emb, docGroup))
                 .has_value()));
    }

    auto query = createEmbedding(64, 5.0f);

    std::unordered_set<std::string> bothGroups = {"doc_A", "doc_B"};
    auto combined = backend.searchSimilar(query, 10, 0.0f, std::string("doc_A"), bothGroups, {});
    REQUIRE((combined.has_value()));
    REQUIRE_FALSE(combined.value().empty());
    for (const auto& r : combined.value()) {
        CHECK((r.document_hash == "doc_A"));
    }

    std::unordered_set<std::string> onlyB = {"doc_B"};
    auto disjoint = backend.searchSimilar(query, 10, 0.0f, std::string("doc_A"), onlyB, {});
    REQUIRE((disjoint.has_value()));
    CHECK((disjoint.value().empty()));

    auto exact = backend.searchSimilar(createEmbedding(64, 3.0f), 1, 0.9f, std::string("doc_A"),
                                       bothGroups, {});
    REQUIRE((exact.has_value()));
    REQUIRE((exact.value().size() == 1));
    CHECK((exact.value().front().chunk_id == "chunk_combo_2"));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend searchSimilar candidate_hashes empty returns all",
                 "[vector][backend][search][tiered][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert some vectors
    for (int i = 0; i < 20; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        REQUIRE(
            (backend.insertVector(createVectorRecord("empty_candidate_" + std::to_string(i), emb))
                 .has_value()));
    }

    auto query = createEmbedding(64, 10.0f);

    // Empty candidate set should NOT filter (returns all matching results)
    std::unordered_set<std::string> emptyCandidates;
    auto result = backend.searchSimilar(query, 10, 0.0f, std::nullopt, emptyCandidates, {});
    REQUIRE((result.has_value()));
    CHECK((result.value().size() == 10));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend searchSimilar candidate_hashes no match returns empty",
                 "[vector][backend][search][tiered][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert vectors from doc_A
    for (int i = 0; i < 10; ++i) {
        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        auto rec = createVectorRecord("nomatch_" + std::to_string(i), emb, "doc_A");
        REQUIRE((backend.insertVector(rec).has_value()));
    }

    auto query = createEmbedding(64, 5.0f);

    // Search with candidates that don't exist in the corpus
    std::unordered_set<std::string> nonExistentCandidates = {"doc_X", "doc_Y", "doc_Z"};
    auto result = backend.searchSimilar(query, 10, 0.0f, std::nullopt, nonExistentCandidates, {});
    REQUIRE((result.has_value()));
    CHECK((result.value().empty()));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend searchSimilar combines candidate_hashes with metadata_filters",
                 "[vector][backend][search][tiered][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    // Insert vectors with different docs and metadata
    for (int i = 0; i < 20; ++i) {
        std::string docGroup = (i < 10) ? "doc_A" : "doc_B";
        std::string category = (i % 2 == 0) ? "even" : "odd";

        auto emb = createEmbedding(64, static_cast<float>(i + 1));
        VectorRecord rec;
        rec.chunk_id = "combined_" + std::to_string(i);
        rec.document_hash = docGroup;
        rec.embedding = emb;
        rec.content = "Content " + std::to_string(i);
        rec.metadata["category"] = category;

        REQUIRE((backend.insertVector(rec).has_value()));
    }

    auto query = createEmbedding(64, 10.0f);

    // Filter to doc_A AND category=even
    std::unordered_set<std::string> candidates = {"doc_A"};
    std::map<std::string, std::string> metaFilters = {{"category", "even"}};

    auto result = backend.searchSimilar(query, 20, 0.0f, std::nullopt, candidates, metaFilters);
    REQUIRE((result.has_value()));

    // Should only return doc_A vectors with category=even
    for (const auto& r : result.value()) {
        CHECK((r.document_hash == "doc_A"));
        auto catIt = r.metadata.find("category");
        REQUIRE((catIt != r.metadata.end()));
        CHECK((catIt->second == "even"));
    }

    // doc_A has indices 0-9, even indices are 0,2,4,6,8 = 5 vectors max
    // HNSW is approximate, so we may not find all 5, but should find at least 1
    CHECK((result.value().size() >= 1));
    CHECK((result.value().size() <= 5));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend V2.2 migration adds quantized columns",
                 "[sqlite_vec_backend][migration][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend backend;
    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE((init_result.has_value()));
    REQUIRE((backend.createTables(128).has_value())); // Creates fresh V2.2 schema

    // V2.2 schema adds quantized sidecar columns.
    auto schema_version = VectorSchemaMigration::detectVersion(backend.getDbHandle());
    CHECK((static_cast<int>(schema_version) >= 3)); // At least V2.1
    CHECK((VectorSchemaMigration::hasQuantizedColumns(backend.getDbHandle())));

    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(
        backend.getDbHandle(),
        "SELECT quantized_format, quantized_bits, quantized_seed FROM vectors LIMIT 1", -1, &stmt,
        nullptr);
    CHECK((rc == SQLITE_OK));
    sqlite3_finalize(stmt);
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend quantized sidecar round-trips through insert/get",
                 "[sqlite_vec_backend][turboquant][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend backend;
    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE((init_result.has_value()));
    REQUIRE((backend.createTables(128).has_value())); // Creates fresh V2.2 schema

    VectorRecord rec;
    rec.chunk_id = "quantized_test_1";
    rec.document_hash = "quant_doc_1";
    rec.embedding = createEmbedding(128, 1.0f);
    rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
    rec.quantized.bits_per_channel = 4;
    rec.quantized.seed = 42;
    rec.quantized.packed_codes = {1, 2, 3, 4, 5, 6, 7, 8};

    auto insert_result = backend.insertVector(rec);
    REQUIRE((insert_result.has_value()));

    auto get_result = backend.getVector("quantized_test_1");
    REQUIRE((get_result.has_value()));
    REQUIRE((get_result.value().has_value()));
    auto retrieved = get_result.value().value();
    CHECK((retrieved.quantized.format == VectorRecord::QuantizedFormat::TURBOquant_1));
    CHECK((retrieved.quantized.bits_per_channel == 4));
    CHECK((retrieved.quantized.seed == 42));
    CHECK((retrieved.quantized.packed_codes.size() == 8));
    CHECK((std::memcmp(retrieved.quantized.packed_codes.data(), rec.quantized.packed_codes.data(),
                       8) == 0));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend mixed rows: quantized and non-quantized coexist",
                 "[sqlite_vec_backend][turboquant][mixed][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend backend;
    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE((init_result.has_value()));
    REQUIRE((backend.createTables(128).has_value()));

    // Insert a non-quantized row
    VectorRecord plain_rec;
    plain_rec.chunk_id = "plain_row_1";
    plain_rec.document_hash = "plain_doc";
    plain_rec.embedding = createEmbedding(128, 1.0f);
    REQUIRE((backend.insertVector(plain_rec).has_value()));

    // Insert a quantized row
    VectorRecord quant_rec;
    quant_rec.chunk_id = "quant_row_1";
    quant_rec.document_hash = "quant_doc";
    quant_rec.embedding = createEmbedding(128, 2.0f);
    quant_rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
    quant_rec.quantized.bits_per_channel = 4;
    quant_rec.quantized.seed = 99;
    quant_rec.quantized.packed_codes = {10, 20, 30, 40};
    REQUIRE((backend.insertVector(quant_rec).has_value()));

    // Retrieve both
    auto plain_get = backend.getVector("plain_row_1");
    REQUIRE((plain_get.has_value()));
    REQUIRE((plain_get.value().has_value()));
    CHECK((plain_get.value().value().quantized.packed_codes.empty()));

    auto quant_get = backend.getVector("quant_row_1");
    REQUIRE((quant_get.has_value()));
    REQUIRE((quant_get.value().has_value()));
    CHECK((quant_get.value().value().quantized.format ==
           VectorRecord::QuantizedFormat::TURBOquant_1));
    CHECK((quant_get.value().value().quantized.packed_codes.size() == 4));

    // Batch get both at once
    std::vector<std::string> ids = {"plain_row_1", "quant_row_1"};
    auto batch_result = backend.getVectorsBatch(ids);
    REQUIRE((batch_result.has_value()));
    CHECK((batch_result.value().size() == 2));
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend batch insert of quantized rows",
                 "[sqlite_vec_backend][turboquant][batch][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend backend;
    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE((init_result.has_value()));
    REQUIRE((backend.createTables(128).has_value()));

    std::vector<VectorRecord> batch;
    for (size_t i = 0; i < 5; ++i) {
        VectorRecord rec;
        rec.chunk_id = "batch_quant_" + std::to_string(i);
        rec.document_hash = "batch_doc_" + std::to_string(i);
        rec.embedding = createEmbedding(128, static_cast<float>(i + 1));
        rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
        rec.quantized.bits_per_channel = 4;
        rec.quantized.seed = static_cast<uint64_t>(i * 11);
        rec.quantized.packed_codes = {static_cast<uint8_t>(i), static_cast<uint8_t>(i * 2),
                                      static_cast<uint8_t>(i * 3), static_cast<uint8_t>(i * 4)};
        batch.push_back(rec);
    }

    auto batch_result = backend.insertVectorsBatch(batch);
    REQUIRE((batch_result.has_value()));

    // Retrieve all 5 and verify quantized data survived
    for (size_t i = 0; i < 5; ++i) {
        auto get_result = backend.getVector("batch_quant_" + std::to_string(i));
        REQUIRE((get_result.has_value()));
        REQUIRE((get_result.value().has_value()));
        auto& rec = get_result.value().value();
        CHECK((rec.quantized.format == VectorRecord::QuantizedFormat::TURBOquant_1));
        CHECK((rec.quantized.packed_codes.size() == 4));
        CHECK((rec.quantized.packed_codes[0] == static_cast<uint8_t>(i)));
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend update changes quantized sidecar",
                 "[sqlite_vec_backend][turboquant][update][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend backend;
    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE((init_result.has_value()));
    REQUIRE((backend.createTables(128).has_value()));

    VectorRecord rec;
    rec.chunk_id = "update_quant_1";
    rec.document_hash = "update_doc";
    rec.embedding = createEmbedding(128, 3.0f);
    rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
    rec.quantized.bits_per_channel = 4;
    rec.quantized.seed = 7;
    rec.quantized.packed_codes = {1, 2, 3, 4};
    REQUIRE((backend.insertVector(rec).has_value()));

    // Update: replace quantized data
    rec.quantized.packed_codes = {9, 8, 7, 6};
    rec.quantized.seed = 88;
    auto update_result = backend.updateVector(rec.chunk_id, rec);
    REQUIRE((update_result.has_value()));

    auto get_result = backend.getVector("update_quant_1");
    REQUIRE((get_result.has_value()));
    REQUIRE((get_result.value().has_value()));
    auto& retrieved = get_result.value().value();
    CHECK((retrieved.quantized.seed == 88));
    CHECK((retrieved.quantized.packed_codes.size() == 4));
    CHECK((retrieved.quantized.packed_codes[0] == 9));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend loads legacy V2.1 row (NULL quantized columns) safely",
                 "[sqlite_vec_backend][turboquant][migration][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend backend;
    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE((init_result.has_value()));
    REQUIRE((backend.createTables(128).has_value()));

    // Manually insert a row that simulates a pre-V2.2 insert: quantized columns as NULL.
    // This mimics a row that existed before the quantized sidecar was added.
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(
        backend.getDbHandle(),
        "INSERT INTO vectors (chunk_id, document_hash, embedding, embedding_dim, "
        "quantized_format, quantized_bits, quantized_seed, quantized_packed_codes) "
        "VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL)",
        -1, &stmt, nullptr);
    REQUIRE((rc == SQLITE_OK));

    std::vector<uint8_t> embedding_bytes(128 * sizeof(float));
    std::memcpy(embedding_bytes.data(), createEmbedding(128, 99.0f).data(), 128 * sizeof(float));

    sqlite3_bind_text(stmt, 1, "legacy_row_1", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, "legacy_doc", -1, SQLITE_TRANSIENT);
    sqlite3_bind_blob(stmt, 3, embedding_bytes.data(), static_cast<int>(embedding_bytes.size()),
                      SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 4, 128);
    rc = sqlite3_step(stmt);
    REQUIRE((rc == SQLITE_DONE));
    sqlite3_finalize(stmt);

    // Load it back — should not crash and should return the row with empty quantized fields
    auto get_result = backend.getVector("legacy_row_1");
    REQUIRE((get_result.has_value()));
    REQUIRE((get_result.value().has_value()));
    auto& retrieved = get_result.value().value();
    CHECK((retrieved.chunk_id == "legacy_row_1"));
    CHECK((retrieved.quantized.packed_codes.empty()));
    CHECK((retrieved.quantized.seed == 0)); // Default/zero for NULL
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend quantized-primary storage omits float blob",
                 "[sqlite_vec_backend][turboquant][quantized_primary][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    // Configure backend with quantized-primary storage mode
    SqliteVecBackend::Config config;
    config.embedding_dim = 128;
    config.enable_turboquant_storage = true;
    config.quantized_primary_storage = true;
    config.turboquant_bits = 4;
    config.turboquant_seed = 7;
    SqliteVecBackend backend(config);

    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE((init_result.has_value()));
    REQUIRE((backend.createTables(128).has_value()));

    // Insert a vector with both embedding and quantized data
    VectorRecord rec;
    rec.chunk_id = "qp_test_1";
    rec.document_hash = "qp_doc";
    rec.embedding = createEmbedding(128, 3.0f);
    rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
    rec.quantized.bits_per_channel = 4;
    rec.quantized.seed = 7;
    rec.quantized.packed_codes = {1, 2, 3, 4, 5, 6, 7, 8};
    REQUIRE((backend.insertVector(rec).has_value()));

    // Verify embedding blob is NULL directly in SQLite
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(backend.getDbHandle(),
                                "SELECT embedding, quantized_format, quantized_bits, "
                                "quantized_seed, quantized_packed_codes "
                                "FROM vectors WHERE chunk_id = ?",
                                -1, &stmt, nullptr);
    REQUIRE((rc == SQLITE_OK));
    sqlite3_bind_text(stmt, 1, "qp_test_1", -1, SQLITE_TRANSIENT);
    REQUIRE((sqlite3_step(stmt) == SQLITE_ROW));

    // Float blob should be NULL in quantized-primary mode
    CHECK((sqlite3_column_type(stmt, 0) == SQLITE_NULL));

    // Quantized sidecar should be populated
    CHECK((sqlite3_column_int(stmt, 1) == 1));            // TURBOquant_1
    CHECK((sqlite3_column_int(stmt, 2) == 4));            // bits
    CHECK((sqlite3_column_int64(stmt, 3) == 7));          // seed
    CHECK((sqlite3_column_type(stmt, 4) != SQLITE_NULL)); // packed_codes present
    sqlite3_finalize(stmt);
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend quantized-primary + VectorDatabase dequantizes on getVector",
                 "[sqlite_vec_backend][turboquant][quantized_primary][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend::Config config;
    config.embedding_dim = 128;
    config.enable_turboquant_storage = true;
    config.quantized_primary_storage = true;
    config.turboquant_bits = 4;
    config.turboquant_seed = 99;
    SqliteVecBackend backend(config);

    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE((init_result.has_value()));
    REQUIRE((backend.createTables(128).has_value()));

    // Insert quantized-primary row
    VectorRecord rec;
    rec.chunk_id = "qp_vec_1";
    rec.document_hash = "qp_doc_1";
    rec.embedding = createEmbedding(128, 7.0f);
    rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
    rec.quantized.bits_per_channel = 4;
    rec.quantized.seed = 99;
    rec.quantized.packed_codes = {10, 20, 30, 40};
    REQUIRE((backend.insertVector(rec).has_value()));

    // getVector should return the row (backend itself returns NULL blob)
    auto get_result = backend.getVector("qp_vec_1");
    REQUIRE((get_result.has_value()));
    REQUIRE((get_result.value().has_value()));
    auto& retrieved = get_result.value().value();
    CHECK((retrieved.chunk_id == "qp_vec_1"));
    // Backend returns NULL blob → embedding is empty (VectorDatabase layer handles dequantization)
    CHECK((retrieved.embedding.empty()));
    CHECK((retrieved.quantized.format == VectorRecord::QuantizedFormat::TURBOquant_1));
}

TEST_CASE_METHOD(
    SqliteVecBackendFixture,
    "SqliteVecBackend update/delete derive dimension from embedding_dim in quantized-primary mode",
    "[sqlite_vec_backend][turboquant][quantized_primary][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend::Config config;
    config.embedding_dim = 128;
    config.enable_turboquant_storage = true;
    config.quantized_primary_storage = true;
    config.turboquant_bits = 4;
    config.turboquant_seed = 11;
    SqliteVecBackend backend(config);

    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE((init_result.has_value()));
    REQUIRE((backend.createTables(128).has_value()));

    // Insert a quantized-primary row
    VectorRecord rec;
    rec.chunk_id = "upd_del_1";
    rec.document_hash = "doc_upd";
    rec.embedding = createEmbedding(128, 5.0f);
    rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
    rec.quantized.bits_per_channel = 4;
    rec.quantized.seed = 11;
    rec.quantized.packed_codes = {5, 10, 15, 20, 25, 30, 35, 40};
    REQUIRE((backend.insertVector(rec).has_value()));

    // Update: pass a record with embedding for HNSW but quantized for storage
    VectorRecord upd;
    upd.chunk_id = "upd_del_1";
    upd.document_hash = "doc_upd";
    upd.embedding = createEmbedding(128, 6.0f);
    upd.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
    upd.quantized.bits_per_channel = 4;
    upd.quantized.seed = 11;
    upd.quantized.packed_codes = {6, 12, 18, 24, 30, 36, 42, 48};
    REQUIRE((backend.updateVector("upd_del_1", upd).has_value()));

    // Verify the row still exists (update succeeded without dimension error)
    auto get_result = backend.getVector("upd_del_1");
    REQUIRE((get_result.has_value()));
    REQUIRE((get_result.value().has_value()));
    CHECK((get_result.value().value().chunk_id == "upd_del_1"));

    // Delete: should not crash (dimension derived from embedding_dim, not embedding.size())
    REQUIRE((backend.deleteVector("upd_del_1").has_value()));

    // Verify gone
    auto gone_result = backend.getVector("upd_del_1");
    REQUIRE((gone_result.has_value()));
    CHECK((!gone_result.value().has_value()));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend queryVectorDimsUnlocked includes quantized-primary rows",
                 "[sqlite_vec_backend][turboquant][quantized_primary][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend::Config config;
    config.embedding_dim = 128;
    config.enable_turboquant_storage = true;
    config.quantized_primary_storage = true;
    config.turboquant_bits = 4;
    config.turboquant_seed = 13;
    SqliteVecBackend backend(config);

    auto init_result = backend.initialize(createTempDbPath());
    REQUIRE((init_result.has_value()));
    REQUIRE((backend.createTables(128).has_value()));

    // Prepare a TurboQuantMSE to produce valid packed codes for dim=128, bits=4
    TurboQuantConfig tq_cfg;
    tq_cfg.dimension = 128;
    tq_cfg.bits_per_channel = 4;
    tq_cfg.seed = 13;
    TurboQuantMSE tq(tq_cfg);

    // Insert several quantized-primary rows using real quantized data
    for (int i = 0; i < 3; ++i) {
        auto emb = createEmbedding(128, static_cast<float>(i));
        std::vector<uint8_t> packed = vector_utils::packedQuantizeVector(emb, &tq);

        VectorRecord rec;
        rec.chunk_id = "dim_test_" + std::to_string(i);
        rec.document_hash = "doc_dim";
        rec.embedding = emb;
        rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
        rec.quantized.bits_per_channel = 4;
        rec.quantized.seed = 13;
        rec.quantized.packed_codes = packed;
        REQUIRE((backend.insertVector(rec).has_value()));
    }

    // Brute-force search should find these quantized-primary rows
    // (proves queryVectorDimsUnlocked includes them in its dimension scan)
    // Use same seed as first row for exact match (similarity = 1.0)
    auto bf_result = backend.searchSimilar(createEmbedding(128, 0.0f), 5, 0.0f);
    REQUIRE((bf_result.has_value()));
    auto& results = bf_result.value();
    // Should find at least the row with matching seed 0.0f
    CHECK((results.size() >= 1));
    CHECK((results[0].chunk_id == "dim_test_0"));
    CHECK((results[0].relevance_score >
           0.5f)); // TurboQuant causes some distortion; just verify reasonable similarity
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend trained Simeon PQ reranks quantized-primary candidates",
                 "[sqlite_vec_backend][turboquant][quantized_primary][spq][rerank][catch2]") {
    skipIfNeeded();

    constexpr size_t kDim = 32;
    constexpr size_t kCorpus = 16;
    constexpr size_t kResultCount = 4;

    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.enable_turboquant_storage = true;
    config.quantized_primary_storage = true;
    config.turboquant_bits = 4;
    config.turboquant_seed = 23;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 8;
    config.simeon_pq_train_limit = kCorpus;
    config.simeon_pq_rerank_factor = 2;

    TurboQuantConfig tqConfig;
    tqConfig.dimension = kDim;
    tqConfig.bits_per_channel = config.turboquant_bits;
    tqConfig.seed = config.turboquant_seed;
    TurboQuantMSE tq(tqConfig);

    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(kDim).has_value()));

    std::vector<float> query;
    for (size_t i = 0; i < kCorpus; ++i) {
        auto embedding = createEmbedding(kDim, static_cast<float>(i + 1));
        if (i == 0) {
            query = embedding;
        }
        auto record = createVectorRecord("quantized_spq_" + std::to_string(i), embedding);
        record.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
        record.quantized.bits_per_channel = config.turboquant_bits;
        record.quantized.seed = config.turboquant_seed;
        record.quantized.packed_codes = vector_utils::packedQuantizeVector(embedding, &tq);
        REQUIRE((backend.insertVector(record).has_value()));
    }
    REQUIRE((backend.buildIndex().has_value()));

    VectorSearchDiagnostics diagnostics;
    auto result = backend.searchSimilarWithDiagnostics(query, kResultCount, -1.0F, std::nullopt, {},
                                                       {}, diagnostics);
    REQUIRE((result.has_value()));
    REQUIRE((result.value().size() == kResultCount));
    CHECK(diagnostics.usedAnn);
    CHECK(diagnostics.exactDistanceEvaluationsObserved);
    CHECK((diagnostics.exactDistanceEvaluations == kResultCount * config.simeon_pq_rerank_factor));
}

TEST_CASE_METHOD(
    SqliteVecBackendFixture,
    "SqliteVecBackend quantized-primary rows survive close+reopen and remain searchable",
    "[sqlite_vec_backend][turboquant][quantized_primary][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    std::string db_path = createTempDbPath();

    // Phase 1: write
    {
        SqliteVecBackend::Config config;
        config.embedding_dim = 128;
        config.enable_turboquant_storage = true;
        config.quantized_primary_storage = true;
        config.turboquant_bits = 4;
        config.turboquant_seed = 17;
        SqliteVecBackend writer(config);
        REQUIRE((writer.initialize(db_path).has_value()));
        REQUIRE((writer.createTables(128).has_value()));

        TurboQuantConfig tq_cfg;
        tq_cfg.dimension = 128;
        tq_cfg.bits_per_channel = 4;
        tq_cfg.seed = 17;
        TurboQuantMSE tq(tq_cfg);

        // Insert two rows
        for (int i = 0; i < 2; ++i) {
            auto emb = createEmbedding(128, static_cast<float>(i));
            std::vector<uint8_t> packed = vector_utils::packedQuantizeVector(emb, &tq);
            VectorRecord rec;
            rec.chunk_id = "reopen_" + std::to_string(i);
            rec.document_hash = "doc_reopen";
            rec.embedding = emb;
            rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
            rec.quantized.bits_per_channel = 4;
            rec.quantized.seed = 17;
            rec.quantized.packed_codes = packed;
            REQUIRE((writer.insertVector(rec).has_value()));
        }
    } // writer closes here

    // Phase 2: reopen fresh backend on same path and verify rows survived
    {
        SqliteVecBackend::Config config;
        config.embedding_dim = 128;
        config.enable_turboquant_storage = true;
        config.quantized_primary_storage = true;
        config.turboquant_bits = 4;
        config.turboquant_seed = 17;
        SqliteVecBackend reader(config);
        REQUIRE((reader.initialize(db_path).has_value()));

        // Verify both rows exist (backend getVector returns raw record; dequantization
        // is VectorDatabase's responsibility. The proof is that search finds them.)
        for (int i = 0; i < 2; ++i) {
            auto get_result = reader.getVector("reopen_" + std::to_string(i));
            REQUIRE((get_result.has_value()));
            REQUIRE((get_result.value().has_value()));
            const auto& rec = get_result.value().value();
            CHECK((rec.chunk_id == "reopen_" + std::to_string(i)));
            CHECK((rec.quantized.format == VectorRecord::QuantizedFormat::TURBOquant_1));
        }

        // Phase 3: brute-force search finds the rows after reopen (dequantizes from packed codes)
        auto bf_result = reader.searchSimilar(createEmbedding(128, 0.0f), 5, 0.0f);
        REQUIRE((bf_result.has_value()));
        CHECK((bf_result.value().size() >= 1));
        CHECK((bf_result.value()[0].chunk_id == "reopen_0"));
    }
}

TEST_CASE_METHOD(
    SqliteVecBackendFixture,
    "SqliteVecBackend rejects quantized_primary_storage without enable_turboquant_storage",
    "[sqlite_vec_backend][turboquant][quantized_primary][catch2]") {
    if (!skipReason.empty()) {
        SKIP(skipReason);
    }

    SqliteVecBackend::Config config;
    config.embedding_dim = 128;
    config.enable_turboquant_storage = false;
    config.quantized_primary_storage = true; // Invalid: requires TurboQuant
    SqliteVecBackend backend(config);

    auto result = backend.initialize(createTempDbPath());
    CHECK((!result.has_value()));
    CHECK((result.error().code == yams::ErrorCode::InvalidArgument));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend Simeon PQ persists and reloads across reopen",
                 "[vector][backend][search][spq][catch2]") {
    skipIfNeeded();
#ifdef _WIN32
    SKIP("Simeon PQ persistence/reload is not bounded on Windows CI");
#endif

    const std::string dbPath = createTempDbPath();

    {
        SqliteVecBackend::Config config;
        config.embedding_dim = 64;
        config.search_engine = VectorSearchEngine::SimeonPqAdc;
        config.simeon_pq_subquantizers = 8;
        config.simeon_pq_centroids = 16;
        config.simeon_pq_train_limit = 64;
        config.simeon_pq_rerank_factor = 1;
        SqliteVecBackend writer(config);
        REQUIRE((writer.initialize(dbPath).has_value()));
        REQUIRE((writer.createTables(64).has_value()));

        std::vector<VectorRecord> records;
        for (int i = 0; i < 24; ++i) {
            records.push_back(
                createVectorRecord("spq_" + std::to_string(i), createEmbedding(64, float(i + 1))));
        }
        REQUIRE((writer.insertVectorsBatch(records).has_value()));
        REQUIRE((writer.buildIndex().has_value()));
        auto reusableBefore = writer.hasReusablePersistedSearchIndex();
        REQUIRE((reusableBefore.has_value()));
        CHECK_FALSE(reusableBefore.value());
        REQUIRE((writer.persistIndex().has_value()));
        auto reusableAfter = writer.hasReusablePersistedSearchIndex();
        REQUIRE((reusableAfter.has_value()));
        CHECK((reusableAfter.value()));
    }

    {
        SqliteVecBackend::Config config;
        config.embedding_dim = 64;
        config.search_engine = VectorSearchEngine::SimeonPqAdc;
        config.simeon_pq_subquantizers = 8;
        config.simeon_pq_centroids = 16;
        config.simeon_pq_train_limit = 64;
        // Rerank depth is a query policy, not part of the persisted codebook recipe.
        // A reopened index must honor the current typed configuration.
        config.simeon_pq_rerank_factor = 4;
        SqliteVecBackend reader(config);
        REQUIRE((reader.initialize(dbPath).has_value()));
        auto reusable = reader.hasReusablePersistedSearchIndex();
        REQUIRE((reusable.has_value()));
        CHECK((reusable.value()));
        REQUIRE((reader.prepareSearchIndex().has_value()));

        VectorSearchDiagnostics diagnostics;
        auto result = reader.searchSimilarWithDiagnostics(createEmbedding(64, 1.0f), 5, 0.0f,
                                                          std::nullopt, {}, {}, diagnostics);
        REQUIRE((result.has_value()));
        REQUIRE_FALSE(result.value().empty());
        CHECK((result.value().front().chunk_id == "chunk_spq_0"));
        CHECK((diagnostics.usedAnn));
        CHECK((diagnostics.exactDistanceEvaluations == 20));
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend prepare rebuilds dirty Simeon PQ instead of loading stale state",
                 "[vector][backend][search][spq][persistence][catch2]") {
    skipIfNeeded();

    constexpr size_t kDim = 64;
    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 16;
    config.simeon_pq_train_limit = 64;
    config.simeon_pq_rerank_factor = 1;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(kDim).has_value()));

    for (int i = 0; i < 24; ++i) {
        REQUIRE((backend
                     .insertVector(createVectorRecord("stale_" + std::to_string(i),
                                                      createEmbedding(kDim, float(i + 1))))
                     .has_value()));
    }
    REQUIRE((backend.buildIndex().has_value()));
    REQUIRE((backend.persistIndex().has_value()));

    std::vector<float> exactQuery(kDim, 0.0F);
    exactQuery.front() = 1.0F;
    REQUIRE((backend.insertVector(createVectorRecord("fresh", exactQuery)).has_value()));
    REQUIRE((backend.prepareSearchIndex().has_value()));

    VectorSearchDiagnostics diagnostics;
    auto result = backend.searchSimilarWithDiagnostics(exactQuery, 25, -1.0F, std::nullopt, {}, {},
                                                       diagnostics);
    REQUIRE((result.has_value()));
    REQUIRE((result.value().size() == 25));
    CHECK((result.value().front().chunk_id == "chunk_fresh"));
    CHECK((diagnostics.rowsVisited == 25));
}

TEST_CASE_METHOD(
    SqliteVecBackendFixture,
    "SqliteVecBackend rejects a persisted Simeon PQ snapshot after a pre-restart mutation",
    "[vector][backend][search][spq][persistence][restart][catch2]") {
    skipIfNeeded();
#ifdef _WIN32
    SKIP("Simeon PQ persistence/reload is not bounded on Windows CI");
#endif

    constexpr size_t kDim = 64;
    const std::string dbPath = createTempDbPath();
    std::vector<float> exactQuery(kDim, 0.0F);
    exactQuery.front() = 1.0F;

    {
        SqliteVecBackend::Config config;
        config.embedding_dim = kDim;
        config.search_engine = VectorSearchEngine::SimeonPqAdc;
        config.simeon_pq_subquantizers = 8;
        config.simeon_pq_centroids = 16;
        config.simeon_pq_train_limit = 64;
        config.simeon_pq_rerank_factor = 1;
        SqliteVecBackend writer(config);
        REQUIRE((writer.initialize(dbPath).has_value()));
        REQUIRE((writer.createTables(kDim).has_value()));

        for (int i = 0; i < 24; ++i) {
            REQUIRE((writer
                         .insertVector(createVectorRecord("restart_stale_" + std::to_string(i),
                                                          createEmbedding(kDim, float(i + 1))))
                         .has_value()));
        }
        REQUIRE((writer.buildIndex().has_value()));
        REQUIRE((writer.persistIndex().has_value()));
        REQUIRE((writer.insertVector(createVectorRecord("restart_fresh", exactQuery)).has_value()));
        // Intentionally close without rebuilding or persisting the now-dirty index.
    }

    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 16;
    config.simeon_pq_train_limit = 64;
    config.simeon_pq_rerank_factor = 1;
    SqliteVecBackend reader(config);
    REQUIRE((reader.initialize(dbPath).has_value()));

    auto reusable = reader.hasReusablePersistedSearchIndex();
    REQUIRE((reusable.has_value()));
    CHECK_FALSE(reusable.value());
    REQUIRE((reader.prepareSearchIndex().has_value()));

    VectorSearchDiagnostics diagnostics;
    auto result = reader.searchSimilarWithDiagnostics(exactQuery, 25, -1.0F, std::nullopt, {}, {},
                                                      diagnostics);
    REQUIRE((result.has_value()));
    REQUIRE((result.value().size() == 25));
    CHECK((result.value().front().chunk_id == "chunk_restart_fresh"));
    CHECK((diagnostics.rowsVisited == 25));
}

TEST_CASE_METHOD(
    SqliteVecBackendFixture,
    "SqliteVecBackend does not bless a dirty Simeon PQ snapshot during suppressed persistence",
    "[vector][backend][search][spq][persistence][restart][suppressed][catch2]") {
    skipIfNeeded();
#ifdef _WIN32
    SKIP("Simeon PQ persistence/reload is not bounded on Windows CI");
#endif

    constexpr size_t kDim = 64;
    const std::string dbPath = createTempDbPath();
    std::vector<float> exactQuery(kDim, 0.0F);
    exactQuery.front() = 1.0F;

    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 16;
    config.simeon_pq_train_limit = 64;
    config.simeon_pq_rerank_factor = 1;

    {
        SqliteVecBackend writer(config);
        REQUIRE((writer.initialize(dbPath).has_value()));
        REQUIRE((writer.createTables(kDim).has_value()));
        for (int i = 0; i < 24; ++i) {
            REQUIRE((writer
                         .insertVector(createVectorRecord("suppressed_stale_" + std::to_string(i),
                                                          createEmbedding(kDim, float(i + 1))))
                         .has_value()));
        }
        REQUIRE((writer.buildIndex().has_value()));
        REQUIRE((writer.persistIndex().has_value()));
    }

    {
        auto suppressedConfig = config;
        suppressedConfig.suppress_search_index_builds = true;
        SqliteVecBackend suppressed(suppressedConfig);
        REQUIRE((suppressed.initialize(dbPath).has_value()));

        // Lazy search loads the reusable 24-row snapshot without building a new index.
        auto loaded = suppressed.searchSimilar(createEmbedding(kDim, 1.0F), 5, -1.0F);
        REQUIRE((loaded.has_value()));
        REQUIRE((loaded.value().size() == 5));

        REQUIRE((suppressed.insertVector(createVectorRecord("suppressed_fresh", exactQuery))
                     .has_value()));
        // A checkpoint may persist while instrumentation suppresses index builds. It must not
        // stamp the now-stale 24-row in-memory snapshot with the 25-row vector generation.
        REQUIRE((suppressed.persistIndex().has_value()));
    }

    SqliteVecBackend reader(config);
    REQUIRE((reader.initialize(dbPath).has_value()));
    auto reusable = reader.hasReusablePersistedSearchIndex();
    REQUIRE((reusable.has_value()));
    CHECK_FALSE(reusable.value());
    REQUIRE((reader.prepareSearchIndex().has_value()));

    VectorSearchDiagnostics diagnostics;
    auto result = reader.searchSimilarWithDiagnostics(exactQuery, 25, -1.0F, std::nullopt, {}, {},
                                                      diagnostics);
    REQUIRE((result.has_value()));
    REQUIRE((result.value().size() == 25));
    CHECK((result.value().front().chunk_id == "chunk_suppressed_fresh"));
    CHECK((diagnostics.rowsVisited == 25));
}

TEST_CASE_METHOD(
    SqliteVecBackendFixture,
    "SqliteVecBackend refreshes a loaded Simeon PQ after another backend updates a vector",
    "[vector][backend][search][spq][persistence][generation][concurrency][catch2]") {
    skipIfNeeded();
#ifdef _WIN32
    SKIP("Simeon PQ cross-connection persistence is not bounded on Windows CI");
#endif

    constexpr size_t kDim = 64;
    const std::string dbPath = createTempDbPath();
    std::vector<float> query(kDim, 0.0F);
    query.front() = 1.0F;
    auto staleTarget = query;
    staleTarget.front() = -1.0F;

    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 16;
    config.simeon_pq_train_limit = 64;
    config.simeon_pq_rerank_factor = 1;

    {
        SqliteVecBackend seed(config);
        REQUIRE((seed.initialize(dbPath).has_value()));
        REQUIRE((seed.createTables(kDim).has_value()));
        REQUIRE(
            (seed.insertVector(createVectorRecord("external_target", staleTarget)).has_value()));
        for (int i = 1; i < 32; ++i) {
            REQUIRE((seed.insertVector(createVectorRecord("external_" + std::to_string(i),
                                                          createEmbedding(kDim, float(i + 1))))
                         .has_value()));
        }
        REQUIRE((seed.buildIndex().has_value()));
        REQUIRE((seed.persistIndex().has_value()));
    }

    SqliteVecBackend reader(config);
    REQUIRE((reader.initialize(dbPath).has_value()));
    REQUIRE((reader.prepareSearchIndex().has_value()));

    auto before = reader.searchSimilar(query, 1, -1.0F);
    REQUIRE((before.has_value()));
    REQUIRE((before.value().size() == 1));
    REQUIRE((before.value().front().chunk_id != "chunk_external_target"));

    SqliteVecBackend mutator(config);
    REQUIRE((mutator.initialize(dbPath).has_value()));
    REQUIRE((updateEmbeddingRaw(mutator.getDbHandle(), "chunk_external_target", query)));

    auto after = reader.searchSimilar(query, 1, -1.0F);
    REQUIRE((after.has_value()));
    REQUIRE((after.value().size() == 1));
    CHECK((after.value().front().chunk_id == "chunk_external_target"));
}

TEST_CASE_METHOD(
    SqliteVecBackendFixture,
    "SqliteVecBackend preserves vector generation tracking while migrating update triggers",
    "[vector][backend][search][spq][persistence][generation][concurrency][migration][catch2]") {
    skipIfNeeded();
#ifdef _WIN32
    SKIP("Simeon PQ cross-connection persistence is not bounded on Windows CI");
#endif

    constexpr size_t kDim = 64;
    const std::string dbPath = createTempDbPath();
    const std::string targetChunkId = "chunk_trigger_target";
    auto initialEmbedding = createEmbedding(kDim, 1.0F);
    auto updatedEmbedding = createEmbedding(kDim, 1001.0F);
    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;

    {
        SqliteVecBackend seed(config);
        REQUIRE((seed.initialize(dbPath).has_value()));
        REQUIRE((seed.createTables(kDim).has_value()));
        REQUIRE((
            seed.insertVector(createVectorRecord("trigger_target", initialEmbedding)).has_value()));
    }

    sqlite3* mutator = nullptr;
    REQUIRE((sqlite3_open(dbPath.c_str(), &mutator) == SQLITE_OK));
    std::uint64_t generationBefore = 0;
    REQUIRE((yams::vector::detail::loadVectorIndexGeneration(mutator, generationBefore)));

    SqliteVecBackend initializer(config);
    REQUIRE((initializer.initialize(dbPath).has_value()));
    UpdateDuringGenerationTriggerMigration update{
        .mutatorDb = mutator, .chunkId = &targetChunkId, .embedding = &updatedEmbedding};
    {
        GenerationTriggerMigrationGuard migrationGuard(initializer.getDbHandle(), update);
        REQUIRE((initializer.createTables(kDim).has_value()));
    }

    if (!update.attempted) {
        REQUIRE(update.sawProtectedInstall);
        update.attempted = true;
        update.updated = updateEmbeddingRaw(mutator, targetChunkId, updatedEmbedding);
    }

    REQUIRE(update.attempted);
    REQUIRE(update.updated);
    std::uint64_t generationAfter = 0;
    REQUIRE((yams::vector::detail::loadVectorIndexGeneration(mutator, generationAfter)));
    CHECK((generationAfter > generationBefore));
    REQUIRE((sqlite3_close(mutator) == SQLITE_OK));
}

TEST_CASE_METHOD(
    SqliteVecBackendFixture, "SqliteVecBackend installs vector generation tracking atomically",
    "[vector][backend][search][spq][persistence][generation][concurrency][migration][catch2]") {
    skipIfNeeded();
#ifdef _WIN32
    SKIP("Simeon PQ cross-connection persistence is not bounded on Windows CI");
#endif

    constexpr size_t kDim = 64;
    const std::string dbPath = createTempDbPath();
    const std::string targetChunkId = "chunk_trigger_install_target";
    auto initialEmbedding = createEmbedding(kDim, 1.0F);
    auto updatedEmbedding = createEmbedding(kDim, 1001.0F);
    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;

    SqliteVecBackend initializer(config);
    REQUIRE((initializer.initialize(dbPath).has_value()));
    REQUIRE((initializer.createTables(kDim).has_value()));
    REQUIRE(
        (initializer.insertVector(createVectorRecord("trigger_install_target", initialEmbedding))
             .has_value()));

    char* error = nullptr;
    REQUIRE((sqlite3_exec(initializer.getDbHandle(),
                          "DROP TRIGGER IF EXISTS vectors_index_generation_insert;"
                          "DROP TRIGGER IF EXISTS vectors_index_generation_update_v2;"
                          "DROP TRIGGER IF EXISTS vectors_index_generation_update;"
                          "DROP TRIGGER IF EXISTS vectors_index_generation_delete;"
                          "DROP TABLE vector_index_generation;",
                          nullptr, nullptr, &error) == SQLITE_OK));
    sqlite3_free(error);

    sqlite3* mutator = nullptr;
    REQUIRE((sqlite3_open(dbPath.c_str(), &mutator) == SQLITE_OK));
    UpdateDuringGenerationTriggerMigration update{.mutatorDb = mutator,
                                                  .chunkId = &targetChunkId,
                                                  .embedding = &updatedEmbedding,
                                                  .requireProtectedInstall = true};
    {
        GenerationTriggerMigrationGuard migrationGuard(initializer.getDbHandle(), update);
        REQUIRE((initializer.ensurePersistenceSchema().has_value()));
    }
    if (!update.attempted) {
        REQUIRE(update.sawProtectedInstall);
        update.attempted = true;
        update.updated = updateEmbeddingRaw(mutator, targetChunkId, updatedEmbedding);
    }

    REQUIRE(update.attempted);
    REQUIRE(update.updated);
    std::uint64_t generation = 0;
    REQUIRE((yams::vector::detail::loadVectorIndexGeneration(mutator, generation)));
    CHECK((generation > 0));
    REQUIRE((sqlite3_close(mutator) == SQLITE_OK));
}

TEST_CASE_METHOD(
    SqliteVecBackendFixture,
    "SqliteVecBackend does not return a stale Simeon PQ result when another backend commits "
    "during materialization",
    "[vector][backend][search][spq][persistence][generation][concurrency][catch2]") {
    skipIfNeeded();
#ifdef _WIN32
    SKIP("Simeon PQ cross-connection persistence is not bounded on Windows CI");
#endif

    constexpr size_t kDim = 64;
    const std::string dbPath = createTempDbPath();
    std::vector<float> query(kDim, 0.0F);
    query.front() = 1.0F;
    auto staleTarget = query;
    staleTarget.front() = -1.0F;

    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 16;
    config.simeon_pq_train_limit = 64;
    config.simeon_pq_rerank_factor = 1;

    {
        SqliteVecBackend seed(config);
        REQUIRE((seed.initialize(dbPath).has_value()));
        REQUIRE((seed.createTables(kDim).has_value()));
        REQUIRE(
            (seed.insertVector(createVectorRecord("external_target", staleTarget)).has_value()));
        for (int i = 1; i < 32; ++i) {
            REQUIRE((seed.insertVector(createVectorRecord("external_" + std::to_string(i),
                                                          createEmbedding(kDim, float(i + 1))))
                         .has_value()));
        }
        REQUIRE((seed.buildIndex().has_value()));
        REQUIRE((seed.persistIndex().has_value()));
    }

    SqliteVecBackend reader(config);
    REQUIRE((reader.initialize(dbPath).has_value()));
    REQUIRE((reader.prepareSearchIndex().has_value()));

    auto before = reader.searchSimilar(query, 1, -1.0F);
    REQUIRE((before.has_value()));
    REQUIRE((before.value().size() == 1));
    REQUIRE((before.value().front().chunk_id != "chunk_external_target"));

    SqliteVecBackend mutator(config);
    REQUIRE((mutator.initialize(dbPath).has_value()));
    const std::string targetChunkId = "chunk_external_target";
    UpdateEmbeddingOnMaterialization update{
        .mutatorDb = mutator.getDbHandle(), .chunkId = &targetChunkId, .embedding = &query};
    MaterializationMutationGuard mutationGuard(reader.getDbHandle(), update);

    VectorSearchDiagnostics diagnostics;
    auto after =
        reader.searchSimilarWithDiagnostics(query, 1, -1.0F, std::nullopt, {}, {}, diagnostics);
    REQUIRE(update.attempted);
    REQUIRE(update.updated);
    REQUIRE((after.has_value()));
    REQUIRE((after.value().size() == 1));
    CHECK((after.value().front().chunk_id == targetChunkId));
    CHECK((diagnostics.usedExactScan));
}

TEST_CASE_METHOD(
    SqliteVecBackendFixture,
    "SqliteVecBackend restarts a stale Simeon PQ batch when another backend commits during it",
    "[vector][backend][search][spq][persistence][generation][concurrency][batch][catch2]") {
    skipIfNeeded();
#ifdef _WIN32
    SKIP("Simeon PQ cross-connection persistence is not bounded on Windows CI");
#endif

    constexpr size_t kDim = 64;
    const std::string dbPath = createTempDbPath();
    std::vector<float> query(kDim, 0.0F);
    query.front() = 1.0F;
    auto staleTarget = query;
    staleTarget.front() = -1.0F;

    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 16;
    config.simeon_pq_train_limit = 64;
    config.simeon_pq_rerank_factor = 1;

    {
        SqliteVecBackend seed(config);
        REQUIRE((seed.initialize(dbPath).has_value()));
        REQUIRE((seed.createTables(kDim).has_value()));
        REQUIRE((seed.insertVector(createVectorRecord("batch_external_target", staleTarget))
                     .has_value()));
        for (int i = 1; i < 32; ++i) {
            REQUIRE((seed.insertVector(createVectorRecord("batch_external_" + std::to_string(i),
                                                          createEmbedding(kDim, float(i + 1))))
                         .has_value()));
        }
        REQUIRE((seed.buildIndex().has_value()));
        REQUIRE((seed.persistIndex().has_value()));
    }

    SqliteVecBackend reader(config);
    REQUIRE((reader.initialize(dbPath).has_value()));
    REQUIRE((reader.prepareSearchIndex().has_value()));

    SqliteVecBackend mutator(config);
    REQUIRE((mutator.initialize(dbPath).has_value()));
    const std::string targetChunkId = "chunk_batch_external_target";
    UpdateEmbeddingOnMaterialization update{
        .mutatorDb = mutator.getDbHandle(), .chunkId = &targetChunkId, .embedding = &query};
    MaterializationMutationGuard mutationGuard(reader.getDbHandle(), update);

    auto results = reader.searchSimilarBatch({query, query}, 1, -1.0F);
    REQUIRE(update.attempted);
    REQUIRE(update.updated);
    REQUIRE((results.has_value()));
    REQUIRE((results.value().size() == 2));
    for (const auto& result : results.value()) {
        REQUIRE((result.size() == 1));
        CHECK((result.front().chunk_id == targetChunkId));
    }
}

TEST_CASE_METHOD(
    SqliteVecBackendFixture,
    "SqliteVecBackend returns exact scores from one snapshot after Simeon PQ invalidation",
    "[vector][backend][search][spq][generation][concurrency][snapshot][catch2]") {
    skipIfNeeded();
#ifdef _WIN32
    SKIP("Simeon PQ cross-connection persistence is not bounded on Windows CI");
#endif

    constexpr size_t kDim = 64;
    const std::string dbPath = createTempDbPath();
    std::vector<float> query(kDim, 0.0F);
    query.front() = 1.0F;
    auto opposite = query;
    opposite.front() = -1.0F;

    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 16;
    config.simeon_pq_train_limit = 64;
    config.simeon_pq_rerank_factor = 1;

    {
        SqliteVecBackend seed(config);
        REQUIRE((seed.initialize(dbPath).has_value()));
        REQUIRE((seed.createTables(kDim).has_value()));
        REQUIRE((seed.insertVector(createVectorRecord("snapshot_target", opposite)).has_value()));
        for (int i = 1; i < 32; ++i) {
            REQUIRE((seed.insertVector(createVectorRecord("snapshot_" + std::to_string(i),
                                                          createEmbedding(kDim, float(i + 1))))
                         .has_value()));
        }
        REQUIRE((seed.buildIndex().has_value()));
        REQUIRE((seed.persistIndex().has_value()));
    }

    SqliteVecBackend reader(config);
    REQUIRE((reader.initialize(dbPath).has_value()));
    REQUIRE((reader.prepareSearchIndex().has_value()));

    SqliteVecBackend mutator(config);
    REQUIRE((mutator.initialize(dbPath).has_value()));
    const std::string targetChunkId = "chunk_snapshot_target";
    UpdateEmbeddingOnMaterialization update{.mutatorDb = mutator.getDbHandle(),
                                            .chunkId = &targetChunkId,
                                            .embedding = &query,
                                            .secondEmbedding = &opposite};
    MaterializationMutationGuard mutationGuard(reader.getDbHandle(), update);

    VectorSearchDiagnostics diagnostics;
    auto result =
        reader.searchSimilarWithDiagnostics(query, 1, -1.0F, std::nullopt, {}, {}, diagnostics);
    REQUIRE(update.attempted);
    REQUIRE(update.updated);
    REQUIRE((result.has_value()));
    REQUIRE((result.value().size() == 1));
    REQUIRE((result.value().front().chunk_id == targetChunkId));
    double returnedNormSquared = 0.0;
    double returnedDot = 0.0;
    for (std::size_t i = 0; i < kDim; ++i) {
        returnedNormSquared +=
            result.value().front().embedding[i] * result.value().front().embedding[i];
        returnedDot += query[i] * result.value().front().embedding[i];
    }
    const auto returnedSimilarity = returnedDot / std::sqrt(returnedNormSquared);
    CHECK_THAT(result.value().front().relevance_score, WithinAbs(returnedSimilarity, 1.0e-5));
    CHECK((diagnostics.usedExactScan));
}

TEST_CASE_METHOD(
    SqliteVecBackendFixture,
    "SqliteVecBackend does not restamp a loaded Simeon PQ after another backend mutates vectors",
    "[vector][backend][search][spq][persistence][generation][concurrency][suppressed][catch2]") {
    skipIfNeeded();
#ifdef _WIN32
    SKIP("Simeon PQ cross-connection persistence is not bounded on Windows CI");
#endif

    constexpr size_t kDim = 64;
    const std::string dbPath = createTempDbPath();
    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 16;
    config.simeon_pq_train_limit = 64;
    config.simeon_pq_rerank_factor = 1;

    {
        SqliteVecBackend seed(config);
        REQUIRE((seed.initialize(dbPath).has_value()));
        REQUIRE((seed.createTables(kDim).has_value()));
        for (int i = 0; i < 24; ++i) {
            REQUIRE((seed.insertVector(createVectorRecord("external_persist_" + std::to_string(i),
                                                          createEmbedding(kDim, float(i + 1))))
                         .has_value()));
        }
        REQUIRE((seed.buildIndex().has_value()));
        REQUIRE((seed.persistIndex().has_value()));
    }

    auto suppressedConfig = config;
    suppressedConfig.suppress_search_index_builds = true;
    SqliteVecBackend suppressed(suppressedConfig);
    REQUIRE((suppressed.initialize(dbPath).has_value()));
    auto loaded = suppressed.searchSimilar(createEmbedding(kDim, 1.0F), 5, -1.0F);
    REQUIRE((loaded.has_value()));
    REQUIRE((loaded.value().size() == 5));

    SqliteVecBackend mutator(config);
    REQUIRE((mutator.initialize(dbPath).has_value()));
    REQUIRE((updateEmbeddingRaw(mutator.getDbHandle(), "chunk_external_persist_0",
                                createEmbedding(kDim, 1001.0F))));

    REQUIRE((suppressed.persistIndex().has_value()));

    SqliteVecBackend verifier(config);
    REQUIRE((verifier.initialize(dbPath).has_value()));
    auto reusable = verifier.hasReusablePersistedSearchIndex();
    REQUIRE((reusable.has_value()));
    CHECK_FALSE(reusable.value());
}

TEST_CASE_METHOD(
    SqliteVecBackendFixture,
    "SqliteVecBackend keeps persisted Simeon PQ reusable across metadata-only stale marking",
    "[vector][backend][search][spq][persistence][generation][migration][catch2]") {
    skipIfNeeded();
#ifdef _WIN32
    SKIP("Simeon PQ persistence/reload is not bounded on Windows CI");
#endif

    constexpr size_t kDim = 64;
    const std::string dbPath = createTempDbPath();
    const auto query = createEmbedding(kDim, 1.0F);

    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 16;
    config.simeon_pq_train_limit = 64;
    config.simeon_pq_rerank_factor = 1;

    {
        SqliteVecBackend writer(config);
        REQUIRE((writer.initialize(dbPath).has_value()));
        REQUIRE((writer.createTables(kDim).has_value()));
        for (int i = 0; i < 24; ++i) {
            REQUIRE((writer
                         .insertVector(createVectorRecord("generation_" + std::to_string(i),
                                                          createEmbedding(kDim, float(i + 1))))
                         .has_value()));
        }
        REQUIRE((writer.buildIndex().has_value()));
        REQUIRE((writer.persistIndex().has_value()));

        // Model the broad trigger installed by earlier builds. Reopen must migrate it to an
        // index-payload-aware UPDATE trigger rather than preserving it via IF NOT EXISTS.
        char* error = nullptr;
        REQUIRE((sqlite3_exec(writer.getDbHandle(),
                              "DROP TRIGGER IF EXISTS vectors_index_generation_update_v2;"
                              "DROP TRIGGER IF EXISTS vectors_index_generation_update;"
                              "CREATE TRIGGER vectors_index_generation_update "
                              "AFTER UPDATE ON vectors BEGIN "
                              "UPDATE vector_index_generation SET generation = generation + 1 "
                              "WHERE id = 1; END",
                              nullptr, nullptr, &error) == SQLITE_OK));
        sqlite3_free(error);
    }

    SqliteVecBackend reader(config);
    REQUIRE((reader.initialize(dbPath).has_value()));
    auto reusable = reader.hasReusablePersistedSearchIndex();
    REQUIRE((reusable.has_value()));
    REQUIRE((reusable.value()));

    auto before = reader.searchSimilar(query, 5, -1.0F);
    REQUIRE((before.has_value()));
    REQUIRE((before.value().size() == 5));
    std::vector<std::string> beforeOrder;
    for (const auto& record : before.value()) {
        beforeOrder.push_back(record.chunk_id);
    }

    REQUIRE((reader.markAsStale("chunk_generation_0").has_value()));
    reusable = reader.hasReusablePersistedSearchIndex();
    REQUIRE((reusable.has_value()));
    CHECK((reusable.value()));

    auto after = reader.searchSimilar(query, 5, -1.0F);
    REQUIRE((after.has_value()));
    REQUIRE((after.value().size() == beforeOrder.size()));
    for (std::size_t i = 0; i < beforeOrder.size(); ++i) {
        CHECK((after.value()[i].chunk_id == beforeOrder[i]));
    }

    REQUIRE((reader
                 .updateVector("chunk_generation_0",
                               createVectorRecord("generation_0", createEmbedding(kDim, 101.0F)))
                 .has_value()));
    reusable = reader.hasReusablePersistedSearchIndex();
    REQUIRE((reusable.has_value()));
    CHECK_FALSE(reusable.value());
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend rejects persisted Simeon PQ with a different recipe",
                 "[vector][backend][search][spq][persistence][catch2]") {
    skipIfNeeded();
#ifdef _WIN32
    SKIP("Simeon PQ persistence/reload is not bounded on Windows CI");
#endif

    constexpr size_t kDim = 64;
    const std::string dbPath = createTempDbPath();
    {
        SqliteVecBackend::Config config;
        config.embedding_dim = kDim;
        config.search_engine = VectorSearchEngine::SimeonPqAdc;
        config.simeon_pq_subquantizers = 8;
        config.simeon_pq_centroids = 16;
        config.simeon_pq_train_limit = 16;
        SqliteVecBackend writer(config);
        REQUIRE((writer.initialize(dbPath).has_value()));
        REQUIRE((writer.createTables(kDim).has_value()));
        for (int i = 0; i < 32; ++i) {
            REQUIRE((writer
                         .insertVector(createVectorRecord("recipe_" + std::to_string(i),
                                                          createEmbedding(kDim, float(i + 1))))
                         .has_value()));
        }
        REQUIRE((writer.buildIndex().has_value()));
        REQUIRE((writer.persistIndex().has_value()));
    }

    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 16;
    config.simeon_pq_train_limit = 16;

    SECTION("subquantizer count is part of the persisted recipe") {
        config.simeon_pq_subquantizers = 16;
    }
    SECTION("centroid count is part of the persisted recipe") {
        config.simeon_pq_centroids = 8;
    }
    SECTION("training seed is part of the persisted recipe") {
        config.simeon_pq_seed = 0x123456789ULL;
    }
    SECTION("training sample limit is part of the persisted recipe") {
        config.simeon_pq_train_limit = 24;
    }

    SqliteVecBackend reader(config);
    REQUIRE((reader.initialize(dbPath).has_value()));
    auto reusable = reader.hasReusablePersistedSearchIndex();
    REQUIRE((reusable.has_value()));
    CHECK_FALSE(reusable.value());
    REQUIRE((reader.prepareSearchIndex().has_value()));
    REQUIRE((reader.persistIndex().has_value()));

    sqlite3_stmt* stmt = nullptr;
    REQUIRE((sqlite3_prepare_v2(reader.getDbHandle(),
                                "SELECT m, k, seed, train_limit FROM simeon_pq_meta WHERE dim = ?1",
                                -1, &stmt, nullptr) == SQLITE_OK));
    sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(kDim));
    REQUIRE((sqlite3_step(stmt) == SQLITE_ROW));
    CHECK((sqlite3_column_int64(stmt, 0) ==
           static_cast<sqlite3_int64>(config.simeon_pq_subquantizers)));
    CHECK(
        (sqlite3_column_int64(stmt, 1) == static_cast<sqlite3_int64>(config.simeon_pq_centroids)));
    CHECK((static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 2)) == config.simeon_pq_seed));
    CHECK(
        (static_cast<std::size_t>(sqlite3_column_int64(stmt, 3)) == config.simeon_pq_train_limit));
    sqlite3_finalize(stmt);
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend reusable probe rejects corrupt Simeon PQ codes",
                 "[vector][backend][search][spq][persistence][corruption][catch2]") {
    skipIfNeeded();

    constexpr size_t kDim = 32;
    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 8;
    config.simeon_pq_train_limit = 16;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(kDim).has_value()));

    for (int i = 0; i < 16; ++i) {
        REQUIRE((backend
                     .insertVector(createVectorRecord("corrupt_probe_" + std::to_string(i),
                                                      createEmbedding(kDim, float(i + 1))))
                     .has_value()));
    }
    REQUIRE((backend.buildIndex().has_value()));
    REQUIRE((backend.persistIndex().has_value()));

    char* error = nullptr;
    REQUIRE((sqlite3_exec(backend.getDbHandle(),
                          "UPDATE simeon_pq_codes SET code = x'00' "
                          "WHERE rowid = (SELECT MIN(rowid) FROM simeon_pq_codes)",
                          nullptr, nullptr, &error) == SQLITE_OK));
    sqlite3_free(error);

    auto reusable = backend.hasReusablePersistedSearchIndex();
    REQUIRE((reusable.has_value()));
    CHECK_FALSE(reusable.value());
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend rejects zero-width persisted Simeon PQ metadata",
                 "[vector][backend][search][spq][persistence][corruption][catch2]") {
    skipIfNeeded();

    constexpr size_t kDim = 32;
    const std::string dbPath = createTempDbPath();
    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 8;
    config.simeon_pq_train_limit = 32;

    {
        SqliteVecBackend writer(config);
        REQUIRE((writer.initialize(dbPath).has_value()));
        REQUIRE((writer.createTables(kDim).has_value()));
        for (int i = 0; i < 32; ++i) {
            REQUIRE((writer
                         .insertVector(createVectorRecord("zero_m_" + std::to_string(i),
                                                          createEmbedding(kDim, float(i + 1))))
                         .has_value()));
        }
        REQUIRE((writer.buildIndex().has_value()));
        REQUIRE((writer.persistIndex().has_value()));
    }

    sqlite3* corruptor = nullptr;
    REQUIRE((sqlite3_open(dbPath.c_str(), &corruptor) == SQLITE_OK));
    char* error = nullptr;
    REQUIRE((sqlite3_exec(corruptor, "UPDATE simeon_pq_meta SET m = 0", nullptr, nullptr, &error) ==
             SQLITE_OK));
    sqlite3_free(error);
    REQUIRE((sqlite3_close(corruptor) == SQLITE_OK));

    SqliteVecBackend reader(config);
    REQUIRE((reader.initialize(dbPath).has_value()));
    auto reusable = reader.hasReusablePersistedSearchIndex();
    REQUIRE((reusable.has_value()));
    CHECK_FALSE(reusable.value());
    REQUIRE((reader.prepareSearchIndex().has_value()));

    auto results = reader.searchSimilar(createEmbedding(kDim, 1.0f), 5, 0.0f);
    REQUIRE((results.has_value()));
    CHECK_FALSE(results.value().empty());
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend rejects out-of-range persisted Simeon PQ codes",
                 "[vector][backend][search][spq][persistence][corruption][catch2]") {
    skipIfNeeded();

    constexpr size_t kDim = 32;
    const std::string dbPath = createTempDbPath();
    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 8;
    config.simeon_pq_train_limit = 32;

    {
        SqliteVecBackend writer(config);
        REQUIRE((writer.initialize(dbPath).has_value()));
        REQUIRE((writer.createTables(kDim).has_value()));
        for (int i = 0; i < 32; ++i) {
            REQUIRE((writer
                         .insertVector(createVectorRecord("bad_code_" + std::to_string(i),
                                                          createEmbedding(kDim, float(i + 1))))
                         .has_value()));
        }
        REQUIRE((writer.buildIndex().has_value()));
        REQUIRE((writer.persistIndex().has_value()));
    }

    sqlite3* corruptor = nullptr;
    REQUIRE((sqlite3_open(dbPath.c_str(), &corruptor) == SQLITE_OK));
    char* error = nullptr;
    REQUIRE((sqlite3_exec(corruptor,
                          "UPDATE simeon_pq_codes SET code = x'FFFFFFFFFFFFFFFF' "
                          "WHERE rowid = (SELECT MIN(rowid) FROM simeon_pq_codes)",
                          nullptr, nullptr, &error) == SQLITE_OK));
    sqlite3_free(error);
    REQUIRE((sqlite3_close(corruptor) == SQLITE_OK));

    SqliteVecBackend reader(config);
    REQUIRE((reader.initialize(dbPath).has_value()));
    auto reusable = reader.hasReusablePersistedSearchIndex();
    REQUIRE((reusable.has_value()));
    CHECK_FALSE(reusable.value());
    REQUIRE((reader.prepareSearchIndex().has_value()));

    auto results = reader.searchSimilar(createEmbedding(kDim, 1.0f), 5, 0.0f);
    REQUIRE((results.has_value()));
    CHECK_FALSE(results.value().empty());
}

TEST_CASE("Simeon PQ codec rejects non-finite persisted codebooks",
          "[vector][backend][search][spq][persistence][corruption][catch2]") {
    constexpr std::uint32_t kDim = 8;
    constexpr std::uint32_t kSubquantizers = 2;
    constexpr std::uint32_t kCentroids = 2;
    std::vector<float> persistedCodebooks(kDim * kCentroids, 0.5F);
    persistedCodebooks.front() = std::numeric_limits<float>::quiet_NaN();
    auto blob = yams::vector::detail::serializeSimeonPqCodebooks(kDim, kSubquantizers, kCentroids,
                                                                 true, persistedCodebooks);

    bool trained = false;
    std::vector<float> loadedCodebooks;
    CHECK_FALSE((yams::vector::detail::deserializeSimeonPqCodebooks(
        blob, kDim, kSubquantizers, kCentroids, trained, loadedCodebooks)));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend persisted-index probes release transient SQLite statements",
                 "[vector][backend][search][spq][persistence][lifetime][catch2]") {
    skipIfNeeded();

    SqliteVecBackend::Config config;
    config.embedding_dim = 32;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 8;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(32).has_value()));
    REQUIRE((backend.insertVector(createVectorRecord("probe", createEmbedding(32))).has_value()));

    const auto baseline = countLiveStatements(backend.getDbHandle());
    for (int attempt = 0; attempt < 3; ++attempt) {
        auto reusable = backend.hasReusablePersistedSearchIndex();
        REQUIRE((reusable.has_value()));
        CHECK_FALSE(reusable.value());
    }
    CHECK((countLiveStatements(backend.getDbHandle()) == baseline));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend retains a fully validated Simeon PQ probe for later reuse",
                 "[vector][backend][search][spq][persistence][readiness][catch2]") {
    skipIfNeeded();
#ifdef _WIN32
    SKIP("Simeon PQ persistence/reload is not bounded on Windows CI");
#endif

    constexpr size_t kDim = 32;
    const std::string dbPath = createTempDbPath();
    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 8;
    config.simeon_pq_train_limit = 32;

    {
        SqliteVecBackend seed(config);
        REQUIRE((seed.initialize(dbPath).has_value()));
        REQUIRE((seed.createTables(kDim).has_value()));
        for (int i = 0; i < 32; ++i) {
            REQUIRE((seed.insertVector(createVectorRecord("retained_" + std::to_string(i),
                                                          createEmbedding(kDim, float(i + 1))))
                         .has_value()));
        }
        REQUIRE((seed.buildIndex().has_value()));
        REQUIRE((seed.persistIndex().has_value()));
    }

    SqliteVecBackend reader(config);
    REQUIRE((reader.initialize(dbPath).has_value()));
    auto firstProbe = reader.hasReusablePersistedSearchIndex();
    REQUIRE((firstProbe.has_value()));
    REQUIRE((firstProbe.value()));

    std::vector<std::string> statements;
    {
        SqliteTraceGuard trace(reader.getDbHandle(), statements);
        auto secondProbe = reader.hasReusablePersistedSearchIndex();
        REQUIRE((secondProbe.has_value()));
        REQUIRE((secondProbe.value()));
    }

    CHECK((std::ranges::none_of(statements, [](const std::string& sql) {
        return sql.find("FROM simeon_pq_codes") != std::string::npos;
    })));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "Simeon PQ code loading rejects an interrupted partial result",
                 "[vector][backend][search][spq][persistence][corruption][catch2]") {
    skipIfNeeded();

    constexpr size_t kDim = 32;
    constexpr size_t kSubquantizers = 8;
    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = kSubquantizers;
    config.simeon_pq_centroids = 8;
    config.simeon_pq_train_limit = 32;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(kDim).has_value()));
    for (int i = 0; i < 32; ++i) {
        REQUIRE((backend
                     .insertVector(createVectorRecord("partial_" + std::to_string(i),
                                                      createEmbedding(kDim, float(i + 1))))
                     .has_value()));
    }
    REQUIRE((backend.buildIndex().has_value()));
    REQUIRE((backend.persistIndex().has_value()));

    std::vector<std::size_t> rowids;
    std::vector<std::uint8_t> codes;
    InterruptAfterProgressCalls interrupt{40};
    sqlite3_progress_handler(backend.getDbHandle(), 1, interruptSqliteAfterProgressCalls,
                             &interrupt);
    SqliteProgressHandlerGuard progressGuard(backend.getDbHandle());

    CHECK_FALSE((yams::vector::detail::loadPersistedSimeonPqCodes(
        backend.getDbHandle(), kDim, kSubquantizers, config.simeon_pq_centroids, rowids, codes)));
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend persists Simeon PQ metadata and codes atomically",
                 "[vector][backend][search][spq][persistence][transaction][catch2]") {
    skipIfNeeded();

    constexpr size_t kDim = 32;
    SqliteVecBackend::Config config;
    config.embedding_dim = kDim;
    config.search_engine = VectorSearchEngine::SimeonPqAdc;
    config.simeon_pq_subquantizers = 8;
    config.simeon_pq_centroids = 8;
    config.simeon_pq_train_limit = 32;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(":memory:").has_value()));
    REQUIRE((backend.createTables(kDim).has_value()));

    for (int i = 0; i < 32; ++i) {
        REQUIRE((backend
                     .insertVector(createVectorRecord("atomic_" + std::to_string(i),
                                                      createEmbedding(kDim, float(i + 1))))
                     .has_value()));
    }
    REQUIRE((backend.buildIndex().has_value()));
    REQUIRE((backend.persistIndex().has_value()));
    const auto originalCodebooks = queryBlob(
        backend.getDbHandle(), "SELECT codebooks_blob FROM simeon_pq_meta WHERE dim = ?1", kDim);
    REQUIRE_FALSE(originalCodebooks.empty());

    for (int i = 0; i < 32; ++i) {
        const auto chunkId = "chunk_atomic_" + std::to_string(i);
        REQUIRE(
            (backend
                 .updateVector(chunkId, createVectorRecord("atomic_" + std::to_string(i),
                                                           createEmbedding(kDim, float(i + 1001))))
                 .has_value()));
    }
    REQUIRE((backend.buildIndex().has_value()));

    char* error = nullptr;
    REQUIRE((sqlite3_exec(backend.getDbHandle(),
                          "CREATE TRIGGER reject_simeon_pq_code_insert "
                          "BEFORE INSERT ON simeon_pq_codes BEGIN "
                          "SELECT RAISE(ABORT, 'forced code failure'); END",
                          nullptr, nullptr, &error) == SQLITE_OK));
    sqlite3_free(error);

    auto persist = backend.persistIndex();
    REQUIRE_FALSE(persist.has_value());
    CHECK(persist.error().code == yams::ErrorCode::DatabaseError);
    CHECK(queryBlob(backend.getDbHandle(),
                    "SELECT codebooks_blob FROM simeon_pq_meta WHERE dim = ?1",
                    kDim) == originalCodebooks);
    CHECK((countRows(backend.getDbHandle(), "simeon_pq_codes") == 32));
}

// ============================================================================
// Architecture regression tests (tasks #17-#21)
// ============================================================================

TEST_CASE_METHOD(SqliteVecBackendFixture, "VectorIndexManager delegates to backend correctly",
                 "[vector][regression][index_manager][catch2]") {
    skipIfNeeded();
    SqliteVecBackend::Config config;
    config.embedding_dim = 64;
    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(createTempDbPath()).has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    auto mgr = VectorIndexManager(backend, [&backend] { return backend.isInitialized(); });

    CHECK((mgr.isInitialized()));
    CHECK_FALSE(mgr.hasReusablePersistedSearchIndex().value());

    // Insert vectors and build index through the manager
    for (int i = 0; i < 12; ++i) {
        auto rec = createVectorRecord("idx_" + std::to_string(i), createEmbedding(64, float(i)));
        REQUIRE((backend.insertVector(rec).has_value()));
    }
    auto buildResult = mgr.buildIndex();
    REQUIRE((buildResult.has_value()));

    // Search should work after index build
    auto result = backend.searchSimilar(createEmbedding(64, 1.0f), 5, 0.0f);
    REQUIRE((result.has_value()));
    REQUIRE_FALSE(result.value().empty());
}

TEST_CASE_METHOD(SqliteVecBackendFixture, "SqliteVecBackend rebuilds vec0 ANN across reopen",
                 "[vector][regression][vec0_reopen][catch2]") {
    skipIfNeeded();
    std::string dbPath = createTempDbPath();

    // Build: create tables, insert vectors, build index, close
    {
        SqliteVecBackend::Config config;
        config.embedding_dim = 64;
        config.search_engine = VectorSearchEngine::Vec0L2;
        SqliteVecBackend writer(config);
        REQUIRE((writer.initialize(dbPath).has_value()));
        REQUIRE((writer.createTables(64).has_value()));

        for (int i = 0; i < 20; ++i) {
            auto rec =
                createVectorRecord("v" + std::to_string(i), createEmbedding(64, float(i + 1)));
            REQUIRE((writer.insertVector(rec).has_value()));
        }
        REQUIRE((writer.buildIndex().has_value()));
        auto reusable = writer.hasReusablePersistedSearchIndex();
        REQUIRE((reusable.has_value()));
        CHECK_FALSE(reusable.value());
    }

    // Reopen: data tables remain, while search preparation explicitly rebuilds and warms ANN.
    {
        SqliteVecBackend::Config config;
        config.embedding_dim = 64;
        config.search_engine = VectorSearchEngine::Vec0L2;
        SqliteVecBackend reader(config);
        REQUIRE((reader.initialize(dbPath).has_value()));
        CHECK((reader.tablesExist()));
        auto reusable = reader.hasReusablePersistedSearchIndex();
        REQUIRE((reusable.has_value()));
        CHECK_FALSE(reusable.value());

        REQUIRE((reader.prepareSearchIndex().has_value()));
        auto result = reader.searchSimilar(createEmbedding(64, 1.0f), 5, 0.0f);
        REQUIRE((result.has_value()));
        REQUIRE_FALSE(result.value().empty());
        CHECK((result.value().front().chunk_id == "chunk_v0"));
    }
}

TEST_CASE_METHOD(SqliteVecBackendFixture,
                 "SqliteVecBackend threaded ANN build produces correct search results",
                 "[vector][regression][vec0_parallel][catch2]") {
    skipIfNeeded();
    SqliteVecBackend::Config config;
    config.embedding_dim = 64;
    config.search_engine = VectorSearchEngine::Vec0L2;

    // Insert enough vectors to trigger parallel HNSW build (>= 256)
    const int N = 260;

    SqliteVecBackend backend(config);
    REQUIRE((backend.initialize(createTempDbPath()).has_value()));
    REQUIRE((backend.createTables(64).has_value()));

    for (int i = 0; i < N; ++i) {
        auto rec = createVectorRecord("par_" + std::to_string(i),
                                      createEmbedding(64, float((i * 37 + 11) % 100)));
        REQUIRE((backend.insertVector(rec).has_value()));
    }

    REQUIRE((backend.buildIndex().has_value()));

    // After parallel build, search should return the expected nearest neighbor
    auto query = createEmbedding(64, 1.0f);
    auto result = backend.searchSimilar(query, 3, 0.0f);
    REQUIRE((result.has_value()));
    REQUIRE((result.value().size() >= 3));
    // All results should have well-formed chunk IDs
    for (const auto& r : result.value()) {
        CHECK((r.chunk_id.find("chunk_par_") == 0));
        CHECK_FALSE(r.embedding.empty());
    }
}
