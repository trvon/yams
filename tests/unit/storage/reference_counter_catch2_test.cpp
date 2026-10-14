// Catch2 tests for reference counter
// Migrated from GTest: reference_counter_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <sqlite3.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <exception>
#include <filesystem>
#include <format>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <string_view>
#include <thread>
#include <vector>

#include <yams/storage/reference_counter.h>
#include <yams/storage/reference_counter_writer.h>
#include <yams/storage/storage_engine.h>

#include "../../common/test_helpers_catch2.h"

using namespace yams::storage;

namespace {

struct ReferenceCounterFixture {
    ReferenceCounterFixture() {
        testDbPath = std::filesystem::temp_directory_path() /
                     std::format("yams_refcount_catch2_{}.db",
                                 std::chrono::system_clock::now().time_since_epoch().count());

        ReferenceCounter::Config config{.databasePath = testDbPath,
                                        .enableWAL = true,
                                        .enableStatistics = true,
                                        .cacheSize = 1000,
                                        .busyTimeout = 1000,
                                        .enableAuditLog = false};

        refCounter = std::make_unique<ReferenceCounter>(std::move(config));
    }

    ~ReferenceCounterFixture() {
        refCounter.reset();
        std::filesystem::remove(testDbPath);
        std::filesystem::remove(testDbPath.string() + "-wal");
        std::filesystem::remove(testDbPath.string() + "-shm");
    }

    std::string generateHash(int i) { return std::format("hash_{:064}", i); }

    std::filesystem::path testDbPath;
    std::unique_ptr<ReferenceCounter> refCounter;
};

// Regression: a lock held by another connection during initialization must be retried. The retry
// loop classifies transient locks from the error text, and the schema-migration path used to
// collapse every failure into a bare DatabaseError, so a locked database aborted startup instead
// of retrying. The tiny busyTimeout forces the first attempt to fail fast rather than block, which
// makes the failure deterministic instead of timing-dependent.
TEST_CASE("ReferenceCounter initialize retries while another connection holds the write lock",
          "[storage][reference][contention]") {
    const auto dbPath = std::filesystem::temp_directory_path() /
                        std::format("yams_refcount_contention_{}.db",
                                    std::chrono::system_clock::now().time_since_epoch().count());

    sqlite3* holder = nullptr;
    REQUIRE(sqlite3_open(dbPath.string().c_str(), &holder) == SQLITE_OK);
    REQUIRE(sqlite3_exec(holder, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr) == SQLITE_OK);

    std::thread releaser([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        sqlite3_exec(holder, "COMMIT", nullptr, nullptr, nullptr);
    });

    ReferenceCounter::Config config{.databasePath = dbPath,
                                    .enableWAL = true,
                                    .enableStatistics = false,
                                    .cacheSize = 1000,
                                    .busyTimeout = 1,
                                    .enableAuditLog = false};

    // The constructor initializes the database and throws if initialization fails, which is the
    // behaviour under test: it must retry until the other connection releases the lock.
    std::optional<std::string> initError;
    try {
        ReferenceCounter counter(std::move(config));
    } catch (const std::exception& e) {
        initError = e.what();
    }

    releaser.join();
    sqlite3_close(holder);
    std::filesystem::remove(dbPath);
    std::filesystem::remove(dbPath.string() + "-wal");
    std::filesystem::remove(dbPath.string() + "-shm");

    REQUIRE_FALSE(initError.has_value());
    if (initError) {
        INFO("constructor failed: " << *initError);
    }
}

struct GarbageCollectorFixture {
    GarbageCollectorFixture() {
        auto tempDir = std::filesystem::temp_directory_path();
        testDbPath =
            tempDir / std::format("yams_gc_catch2_{}.db",
                                  std::chrono::system_clock::now().time_since_epoch().count());
        testStoragePath =
            tempDir / std::format("yams_gc_storage_catch2_{}",
                                  std::chrono::system_clock::now().time_since_epoch().count());

        ReferenceCounter::Config config{
            .databasePath = testDbPath, .enableWAL = true, .enableStatistics = true};
        refCounter = std::make_unique<ReferenceCounter>(std::move(config));

        StorageConfig storageConfig{
            .basePath = testStoragePath, .shardDepth = 2, .mutexPoolSize = 100};
        storageEngine = std::make_unique<StorageEngine>(std::move(storageConfig));

        gc = std::make_unique<GarbageCollector>(*refCounter, *storageEngine);
    }

    ~GarbageCollectorFixture() {
        gc.reset();
        storageEngine.reset();
        refCounter.reset();

        std::filesystem::remove_all(testStoragePath);
        std::filesystem::remove(testDbPath);
        std::filesystem::remove(testDbPath.string() + "-wal");
        std::filesystem::remove(testDbPath.string() + "-shm");
    }

    std::string generateHash(int i) { return std::format("{:064x}", i); }

    std::filesystem::path testDbPath;
    std::filesystem::path testStoragePath;
    std::unique_ptr<ReferenceCounter> refCounter;
    std::unique_ptr<StorageEngine> storageEngine;
    std::unique_ptr<GarbageCollector> gc;
};

struct SqliteHandle {
    explicit SqliteHandle(const std::filesystem::path& path) {
        REQUIRE(sqlite3_open(path.string().c_str(), &db) == SQLITE_OK);
    }

    ~SqliteHandle() {
        if (db != nullptr) {
            sqlite3_close(db);
        }
    }

    SqliteHandle(const SqliteHandle&) = delete;
    SqliteHandle& operator=(const SqliteHandle&) = delete;

    sqlite3* db = nullptr;
};

// Observe the next real counter connection, not a parallel SQL implementation. Declare this
// guard before the fixture so its callback context outlives the database, including restore.
struct SqliteStatsObserver {
    SqliteStatsObserver() {
        REQUIRE(active_ == nullptr);
        std::cout << "REF_STATS_SQLITE_IDENTITY version=" << sqlite3_libversion()
                  << " sourceid=" << sqlite3_sourceid() << '\n';
        active_ = this;
        // An unsupported provider is a failed observer control, never behavioral cache RED.
        const int rc = sqlite3_auto_extension(reinterpret_cast<void (*)()>(&initialize));
        if (rc != SQLITE_OK) {
            active_ = nullptr;
        }
        REQUIRE(rc == SQLITE_OK);
    }

    ~SqliteStatsObserver() {
        sqlite3_cancel_auto_extension(reinterpret_cast<void (*)()>(&initialize));
        active_ = nullptr;
    }

    SqliteStatsObserver(const SqliteStatsObserver&) = delete;
    SqliteStatsObserver& operator=(const SqliteStatsObserver&) = delete;

    static int initialize(sqlite3* db, char**, const sqlite3_api_routines*) {
        auto* observer = active_;
        if (observer == nullptr || !observer->observeNextConnection_.exchange(false)) {
            return SQLITE_OK;
        }
        const int rc = sqlite3_set_authorizer(db, &authorize, observer);
        if (rc != SQLITE_OK) {
            return rc;
        }
        const int traceRc =
            sqlite3_trace_v2(db, SQLITE_TRACE_STMT | SQLITE_TRACE_PROFILE, &trace, observer);
        if (traceRc == SQLITE_OK) {
            observer->observedDb_ = db;
            observer->attachments_.fetch_add(1);
        }
        return traceRc;
    }

    static int authorize(void* context, int action, const char* argument, const char*, const char*,
                         const char*) {
        auto& observer = *static_cast<SqliteStatsObserver*>(context);
        if (action == SQLITE_PRAGMA && observer.denyDataVersion_.load() && argument != nullptr &&
            sqlite3_stricmp(argument, "data_version") == 0) {
            return SQLITE_DENY;
        }
        if (action == SQLITE_READ && observer.denyBlockReads_.load() && argument != nullptr &&
            sqlite3_stricmp(argument, "block_references") == 0) {
            observer.deniedBlockReads_.fetch_add(1);
            return SQLITE_DENY;
        }
        return SQLITE_OK;
    }

    static int trace(unsigned event, void* context, void* statement, void*) {
        const char* text = sqlite3_sql(static_cast<sqlite3_stmt*>(statement));
        if (text == nullptr) {
            return 0;
        }
        // Counts only the preserved getStats aggregate SQL. The independent SQLITE_READ
        // authorizer also detects reads through views/CTEs without a COUNT expression.
        const std::string_view sql(text);
        if (sql.find("FROM block_references") == std::string_view::npos ||
            sql.find("COUNT(") == std::string_view::npos) {
            return 0;
        }
        auto& observer = *static_cast<SqliteStatsObserver*>(context);
        if (event == SQLITE_TRACE_STMT) {
            observer.aggregateQueries_.fetch_add(1);
        } else if (event == SQLITE_TRACE_PROFILE &&
                   observer.injectExternalCommit_.exchange(false)) {
            // No Catch2 assertions or exceptions across SQLite's C callback. In WAL mode the
            // separate writer can commit after the aggregate result, before a cache is stamped.
            observer.externalCommitResult_.store(sqlite3_exec(observer.externalWriter_,
                                                              observer.externalWrite_.c_str(),
                                                              nullptr, nullptr, nullptr));
        }
        return 0;
    }

    // Constructor/rearm opens on this thread. Unrelated-thread opens cannot select the
    // context, and this thread's destructor cannot race an in-flight initializer.
    static inline thread_local SqliteStatsObserver* active_ = nullptr;
    std::atomic<bool> observeNextConnection_{true};
    std::atomic<size_t> attachments_{0};
    std::atomic<bool> denyBlockReads_{false};
    std::atomic<size_t> deniedBlockReads_{0};
    std::atomic<bool> denyDataVersion_{false};
    std::atomic<size_t> aggregateQueries_{0};
    // Borrowed only while the observed fixture owns this connection; no production handle API.
    sqlite3* observedDb_ = nullptr;
    sqlite3* externalWriter_ = nullptr;
    std::string externalWrite_;
    std::atomic<bool> injectExternalCommit_{false};
    std::atomic<int> externalCommitResult_{SQLITE_MISUSE};
};

void execSql(sqlite3* db, std::string_view sql) {
    char* err = nullptr;
    const int rc = sqlite3_exec(db, std::string(sql).c_str(), nullptr, nullptr, &err);
    INFO(std::string(sql));
    INFO(std::string(err != nullptr ? err : ""));
    REQUIRE(rc == SQLITE_OK);
    if (err != nullptr) {
        sqlite3_free(err);
    }
}

int64_t queryInt64(const std::filesystem::path& dbPath, std::string_view sql) {
    SqliteHandle db(dbPath);
    sqlite3_stmt* rawStmt = nullptr;
    REQUIRE(sqlite3_prepare_v2(db.db, std::string(sql).c_str(), -1, &rawStmt, nullptr) ==
            SQLITE_OK);
    auto stmt =
        std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>(rawStmt, &sqlite3_finalize);
    REQUIRE(sqlite3_step(stmt.get()) == SQLITE_ROW);
    return sqlite3_column_int64(stmt.get(), 0);
}

bool columnExists(const std::filesystem::path& dbPath, std::string_view table,
                  std::string_view column) {
    SqliteHandle db(dbPath);
    sqlite3_stmt* rawStmt = nullptr;
    const auto sql = std::format("PRAGMA table_info({})", table);
    REQUIRE(sqlite3_prepare_v2(db.db, sql.c_str(), -1, &rawStmt, nullptr) == SQLITE_OK);
    auto stmt =
        std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>(rawStmt, &sqlite3_finalize);

    while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
        const auto* name = sqlite3_column_text(stmt.get(), 1);
        if (name != nullptr && column == reinterpret_cast<const char*>(name)) {
            return true;
        }
    }
    return false;
}

} // namespace

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter basic increment decrement",
                 "[storage][refcount][catch2]") {
    const std::string hash = generateHash(1);
    const size_t blockSize = 4096;

    // Initially, reference count should be 0
    auto count = refCounter->getRefCount(hash);
    REQUIRE(count.has_value());
    CHECK(count.value() == 0u);

    // Increment
    auto result = refCounter->increment(hash, blockSize);
    REQUIRE(result.has_value());

    count = refCounter->getRefCount(hash);
    REQUIRE(count.has_value());
    CHECK(count.value() == 1u);

    // Increment again
    result = refCounter->increment(hash, blockSize);
    REQUIRE(result.has_value());

    count = refCounter->getRefCount(hash);
    REQUIRE(count.has_value());
    CHECK(count.value() == 2u);

    // Decrement
    result = refCounter->decrement(hash);
    REQUIRE(result.has_value());

    count = refCounter->getRefCount(hash);
    REQUIRE(count.has_value());
    CHECK(count.value() == 1u);

    // Decrement to zero
    result = refCounter->decrement(hash);
    REQUIRE(result.has_value());

    count = refCounter->getRefCount(hash);
    REQUIRE(count.has_value());
    CHECK(count.value() == 0u);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter hasReferences",
                 "[storage][refcount][catch2]") {
    const std::string hash = generateHash(2);

    // Initially no references
    auto hasRefs = refCounter->hasReferences(hash);
    REQUIRE(hasRefs.has_value());
    CHECK_FALSE(hasRefs.value());

    // After increment, has references
    refCounter->increment(hash, 1024);
    hasRefs = refCounter->hasReferences(hash);
    REQUIRE(hasRefs.has_value());
    CHECK(hasRefs.value());

    // After decrement to zero, no references
    refCounter->decrement(hash);
    hasRefs = refCounter->hasReferences(hash);
    REQUIRE(hasRefs.has_value());
    CHECK_FALSE(hasRefs.value());
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter batch operations",
                 "[storage][refcount][batch][catch2]") {
    const size_t batchSize = 100;
    std::vector<std::string> hashes;

    for (size_t i = 0; i < batchSize; ++i) {
        hashes.push_back(generateHash(i));
    }

    // Batch increment
    auto result = refCounter->incrementBatch(hashes, 2048);
    REQUIRE(result.has_value());

    // Verify all incremented
    for (const auto& hash : hashes) {
        auto count = refCounter->getRefCount(hash);
        REQUIRE(count.has_value());
        CHECK(count.value() == 1u);
    }

    // Batch decrement
    result = refCounter->decrementBatch(hashes);
    REQUIRE(result.has_value());

    // Verify all decremented
    for (const auto& hash : hashes) {
        auto count = refCounter->getRefCount(hash);
        REQUIRE(count.has_value());
        CHECK(count.value() == 0u);
    }
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter batch with sizes",
                 "[storage][refcount][batch][catch2]") {
    struct BlockInfo {
        std::string hash;
        size_t size;
    };

    std::vector<BlockInfo> blocks = {{generateHash(100), 1024},
                                     {generateHash(101), 2048},
                                     {generateHash(102), 4096},
                                     {generateHash(103), 8192}};

    auto result = refCounter->incrementBatchWithSizes(blocks);
    REQUIRE(result.has_value());

    // Verify all incremented
    for (const auto& block : blocks) {
        auto count = refCounter->getRefCount(block.hash);
        REQUIRE(count.has_value());
        CHECK(count.value() == 1u);
    }
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter transaction commit",
                 "[storage][refcount][transaction][catch2]") {
    const std::string hash1 = generateHash(200);
    const std::string hash2 = generateHash(201);

    auto txn = refCounter->beginTransaction();
    REQUIRE(txn != nullptr);
    REQUIRE(txn->isActive());

    // Add operations to transaction
    txn->increment(hash1, 1024);
    txn->increment(hash2, 2048);

    // Counts should still be zero (not committed)
    auto count1 = refCounter->getRefCount(hash1);
    REQUIRE(count1.has_value());
    CHECK(count1.value() == 0u);

    // Commit transaction
    auto result = txn->commit();
    REQUIRE(result.has_value());
    CHECK_FALSE(txn->isActive());

    // Now counts should be updated
    count1 = refCounter->getRefCount(hash1);
    REQUIRE(count1.has_value());
    CHECK(count1.value() == 1u);

    auto count2 = refCounter->getRefCount(hash2);
    REQUIRE(count2.has_value());
    CHECK(count2.value() == 1u);

    auto stats = refCounter->getStats();
    REQUIRE(stats.has_value());
    CHECK(stats.value().transactions == 1u);
    CHECK(stats.value().rollbacks == 0u);
}

TEST_CASE_METHOD(ReferenceCounterFixture,
                 "ReferenceCounter commitTransactionBatches commits logical transactions",
                 "[storage][refcount][batch][catch2]") {
    const std::string hash1 = generateHash(230);
    const std::string hash2 = generateHash(231);
    const std::string hash3 = generateHash(232);

    RefTransactionBatch batch1;
    batch1.operations.push_back({.type = RefDelta::Type::Increment,
                                 .blockHash = hash1,
                                 .compressedSize = 1024,
                                 .uncompressedSize = 2048});
    batch1.operations.push_back({.type = RefDelta::Type::Increment,
                                 .blockHash = hash2,
                                 .compressedSize = 512,
                                 .uncompressedSize = 1024});

    RefTransactionBatch batch2;
    batch2.operations.push_back({.type = RefDelta::Type::Increment,
                                 .blockHash = hash1,
                                 .compressedSize = 1024,
                                 .uncompressedSize = 2048});
    batch2.operations.push_back({.type = RefDelta::Type::Increment,
                                 .blockHash = hash3,
                                 .compressedSize = 256,
                                 .uncompressedSize = 256});

    std::array<RefTransactionBatch, 2> batches{batch1, batch2};
    auto result = refCounter->commitTransactionBatches(batches);
    REQUIRE(result.has_value());

    auto count1 = refCounter->getRefCount(hash1);
    auto count2 = refCounter->getRefCount(hash2);
    auto count3 = refCounter->getRefCount(hash3);
    REQUIRE(count1.has_value());
    REQUIRE(count2.has_value());
    REQUIRE(count3.has_value());
    CHECK(count1.value() == 2u);
    CHECK(count2.value() == 1u);
    CHECK(count3.value() == 1u);

    auto stats = refCounter->getStats();
    REQUIRE(stats.has_value());
    CHECK(stats.value().transactions == 2u);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "RefCounterWriter flush and shutdown drain work",
                 "[storage][refcount][writer][catch2]") {
    resetRefCounterWriterMetrics();
    auto sharedCounter =
        std::shared_ptr<ReferenceCounter>(refCounter.get(), [](ReferenceCounter*) {});
    RefCounterWriter::Options options;
    options.maxBatchCount = 16;
    options.maxDelay = std::chrono::microseconds{50};
    RefCounterWriter writer(sharedCounter, options);

    std::vector<std::future<yams::Result<void>>> futures;
    for (int i = 0; i < 8; ++i) {
        RefTransactionBatch batch;
        batch.operations.push_back({.type = RefDelta::Type::Increment,
                                    .blockHash = generateHash(240 + i),
                                    .compressedSize = 100 + static_cast<size_t>(i),
                                    .uncompressedSize = 200 + static_cast<size_t>(i)});
        futures.push_back(writer.submit(std::move(batch)));
    }

    auto flushResult = writer.flush();
    REQUIRE(flushResult.has_value());

    for (auto& future : futures) {
        auto result = future.get();
        REQUIRE(result.has_value());
    }

    for (int i = 0; i < 8; ++i) {
        auto count = refCounter->getRefCount(generateHash(240 + i));
        REQUIRE(count.has_value());
        CHECK(count.value() == 1u);
    }

    const auto writerTimings = getRefCounterWriterTimingsSnapshot();
    REQUIRE(writerTimings.at("queue_wait").calls == 8u);
    CHECK(writerTimings.at("commit_batches_total").calls >= 1u);
    CHECK(writerTimings.at("batch_apply_ops").calls >= 1u);
    CHECK(writerTimings.at("batch_sqlite_commit").calls >= 1u);

    const auto writerValues = getRefCounterWriterValueMetricsSnapshot();
    CHECK(writerValues.at("submitted_batches").total == 8u);
    CHECK(writerValues.at("submitted_ops").total == 8u);
    CHECK(writerValues.at("writer_batch_size").total == 8u);
    CHECK(writerValues.at("writer_batch_ops").total == 8u);
    CHECK(writerValues.at("committed_ops").total == 8u);

    writer.shutdown();
    RefTransactionBatch lateBatch;
    lateBatch.operations.push_back({.type = RefDelta::Type::Increment,
                                    .blockHash = generateHash(260),
                                    .compressedSize = 1,
                                    .uncompressedSize = 1});
    auto late = writer.submit(std::move(lateBatch)).get();
    CHECK_FALSE(late.has_value());
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter transaction increment without size",
                 "[storage][refcount][transaction][catch2]") {
    const std::string hash = generateHash(250);

    auto txn = refCounter->beginTransaction();
    REQUIRE(txn != nullptr);
    txn->increment(hash);

    auto result = txn->commit();
    REQUIRE(result.has_value());

    auto count = refCounter->getRefCount(hash);
    REQUIRE(count.has_value());
    CHECK(count.value() == 1u);

    auto stats = refCounter->getStats();
    REQUIRE(stats.has_value());
    CHECK(stats.value().totalBytes == 1u);
    CHECK(stats.value().totalUncompressedBytes == 1u);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter transaction rollback",
                 "[storage][refcount][transaction][catch2]") {
    const std::string incrementedHash = generateHash(300);
    const std::string decrementedHash = generateHash(301);
    const std::string newHash = generateHash(302);

    REQUIRE(refCounter->increment(incrementedHash, 1024).has_value());
    REQUIRE(refCounter->increment(decrementedHash, 2048).has_value());
    REQUIRE(refCounter->increment(decrementedHash, 2048).has_value());

    auto beforeStats = refCounter->getStats();
    REQUIRE(beforeStats.has_value());

    auto txn = refCounter->beginTransaction();
    REQUIRE(txn != nullptr);

    txn->increment(incrementedHash, 1024);
    txn->decrement(decrementedHash);
    txn->increment(newHash, 4096);

    // Rollback transaction
    txn->rollback();
    CHECK_FALSE(txn->isActive());

    auto incrementedCount = refCounter->getRefCount(incrementedHash);
    REQUIRE(incrementedCount.has_value());
    CHECK(incrementedCount.value() == 1u);

    auto decrementedCount = refCounter->getRefCount(decrementedHash);
    REQUIRE(decrementedCount.has_value());
    CHECK(decrementedCount.value() == 2u);

    auto newCount = refCounter->getRefCount(newHash);
    REQUIRE(newCount.has_value());
    CHECK(newCount.value() == 0u);

    auto afterStats = refCounter->getStats();
    REQUIRE(afterStats.has_value());
    CHECK(afterStats.value().totalBlocks == beforeStats.value().totalBlocks);
    CHECK(afterStats.value().totalReferences == beforeStats.value().totalReferences);
    CHECK(afterStats.value().transactions == beforeStats.value().transactions);
    CHECK(afterStats.value().rollbacks == beforeStats.value().rollbacks + 1);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter transaction auto rollback",
                 "[storage][refcount][transaction][catch2]") {
    const std::string hash = generateHash(400);

    {
        auto txn = refCounter->beginTransaction();
        txn->increment(hash, 1024);
        // Transaction destroyed without commit - should rollback
    }

    // Count should still be zero
    auto count = refCounter->getRefCount(hash);
    REQUIRE(count.has_value());
    CHECK(count.value() == 0u);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter transaction closed-state behavior",
                 "[storage][refcount][transaction][edge][catch2]") {
    const std::string hash = generateHash(450);

    auto txn = refCounter->beginTransaction();
    REQUIRE(txn != nullptr);
    txn->increment(hash, 1024);

    auto firstCommit = txn->commit();
    REQUIRE(firstCommit.has_value());
    CHECK_FALSE(txn->isActive());

    auto secondCommit = txn->commit();
    CHECK_FALSE(secondCommit.has_value());
    CHECK(secondCommit.error().code == yams::ErrorCode::TransactionFailed);

    // After commit, increment/decrement must fail.
    // In dev/CI (DCHECK enabled): YAMS_DCHECK aborts — no catchable exception.
    // In release (NDEBUG): DCHECK compiles out, throw fires.
#if defined(YAMS_DISABLE_DCHECK) || (defined(NDEBUG) && !defined(YAMS_ENABLE_DCHECK))
    REQUIRE_THROWS_AS(txn->increment(hash, 1024), std::runtime_error);
    REQUIRE_THROWS_AS(txn->decrement(hash), std::runtime_error);
#else
    // DCHECK abort path covered by fork()-based assertion tests.
    SUCCEED("YAMS_DCHECK abort path is covered by fork()-based assertion tests");
#endif
}

TEST_CASE_METHOD(ReferenceCounterFixture,
                 "ReferenceCounter failed transaction commit leaves no partial writes",
                 "[storage][refcount][transaction][edge][catch2]") {
    const std::string existingHash = generateHash(460);
    const std::string newHash = generateHash(461);

    REQUIRE(refCounter->increment(existingHash, 1024).has_value());

    auto backupPath = std::filesystem::temp_directory_path() /
                      std::format("yams_refcount_stale_txn_backup_{}.db",
                                  std::chrono::system_clock::now().time_since_epoch().count());
    std::error_code ec;
    std::filesystem::remove(backupPath, ec);
    std::filesystem::remove(backupPath.string() + "-wal", ec);
    std::filesystem::remove(backupPath.string() + "-shm", ec);

    auto backupResult = refCounter->backup(backupPath);
    REQUIRE(backupResult.has_value());

    auto txn = refCounter->beginTransaction();
    REQUIRE(txn != nullptr);
    txn->increment(existingHash, 1024);
    txn->increment(newHash, 2048);

    auto restoreResult = refCounter->restore(backupPath);
    REQUIRE(restoreResult.has_value());

    auto commitResult = txn->commit();
    CHECK_FALSE(commitResult.has_value());
    CHECK(commitResult.error().code == yams::ErrorCode::TransactionFailed);
    CHECK_FALSE(txn->isActive());

    auto existingCount = refCounter->getRefCount(existingHash);
    REQUIRE(existingCount.has_value());
    CHECK(existingCount.value() == 1u);

    auto newCount = refCounter->getRefCount(newHash);
    REQUIRE(newCount.has_value());
    CHECK(newCount.value() == 0u);

    std::filesystem::remove(backupPath, ec);
    std::filesystem::remove(backupPath.string() + "-wal", ec);
    std::filesystem::remove(backupPath.string() + "-shm", ec);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter get unreferenced blocks",
                 "[storage][refcount][catch2]") {
    std::vector<std::string> allHashes;
    std::vector<std::string> unreferencedHashes;

    // Create some blocks with references
    for (int i = 0; i < 5; ++i) {
        auto hash = generateHash(500 + i);
        allHashes.push_back(hash);
        refCounter->increment(hash, 1024);
    }

    // Create some unreferenced blocks
    for (int i = 5; i < 10; ++i) {
        auto hash = generateHash(500 + i);
        allHashes.push_back(hash);
        unreferencedHashes.push_back(hash);

        // Increment then decrement to create unreferenced entry
        refCounter->increment(hash, 1024);
        refCounter->decrement(hash);
    }

    // Get unreferenced blocks (with 0 seconds min age)
    auto result = refCounter->getUnreferencedBlocks(100, std::chrono::seconds(0));
    REQUIRE(result.has_value());

    const auto& blocks = result.value();
    CHECK(blocks.size() == unreferencedHashes.size());

    // Verify we got the right blocks
    std::set<std::string> foundBlocks(blocks.begin(), blocks.end());
    std::set<std::string> expectedBlocks(unreferencedHashes.begin(), unreferencedHashes.end());
    CHECK(foundBlocks == expectedBlocks);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter statistics",
                 "[storage][refcount][stats][catch2]") {
    // Add some blocks
    const size_t numBlocks = 10;
    const size_t blockSize = 1024;

    for (size_t i = 0; i < numBlocks; ++i) {
        refCounter->increment(generateHash(600 + i), blockSize);
    }

    // Decrement some to make them unreferenced
    for (size_t i = 0; i < 3; ++i) {
        refCounter->decrement(generateHash(600 + i));
    }

    // Get statistics
    auto statsResult = refCounter->getStats();
    REQUIRE(statsResult.has_value());

    const auto& stats = statsResult.value();
    CHECK(stats.totalBlocks == numBlocks);
    CHECK(stats.totalReferences == numBlocks - 3); // 3 were decremented
    CHECK(stats.totalBytes == numBlocks * blockSize);
    CHECK(stats.unreferencedBlocks == 3u);
    CHECK(stats.unreferencedBytes == 3u * blockSize);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter statistics reconcile corrupt counters",
                 "[storage][refcount][stats][edge][catch2]") {
    const std::string referencedHash = generateHash(650);
    const std::string unreferencedHash = generateHash(651);

    REQUIRE(refCounter->increment(referencedHash, 512, 1024).has_value());
    REQUIRE(refCounter->increment(unreferencedHash, 2048, 4096).has_value());
    REQUIRE(refCounter->decrement(unreferencedHash).has_value());

    REQUIRE(refCounter->updateStatistics("total_blocks", -100).has_value());
    REQUIRE(refCounter->updateStatistics("total_references", 50).has_value());

    auto statsResult = refCounter->getStats();
    REQUIRE(statsResult.has_value());

    const auto& stats = statsResult.value();
    CHECK(stats.totalBlocks == 2u);
    CHECK(stats.totalReferences == 1u);
    CHECK(stats.totalBytes == 2560u);
    CHECK(stats.totalUncompressedBytes == 5120u);
    CHECK(stats.unreferencedBlocks == 1u);
    CHECK(stats.unreferencedBytes == 2048u);
}

// Run this control successfully before accepting any cache test as behavioral RED. It checks
// provider attachment, the preserved cold-query trace, and real-table authorizer enforcement.
TEST_CASE("ReferenceCounter statistics observer control",
          "[storage][refcount][stats][observer-control]") {
    SqliteStatsObserver observer;
    int unrelatedOpen = SQLITE_MISUSE;
    std::thread unrelated([&] {
        sqlite3* db = nullptr;
        unrelatedOpen = sqlite3_open(":memory:", &db);
        if (db != nullptr) {
            sqlite3_close(db);
        }
    });
    unrelated.join();
    REQUIRE(unrelatedOpen == SQLITE_OK);
    REQUIRE(observer.attachments_.load() == 0u);
    ReferenceCounterFixture fixture;
    REQUIRE(observer.attachments_.load() == 1u);
    const auto hash = fixture.generateHash(659);
    REQUIRE(fixture.refCounter->increment(hash, 512, 1024).has_value());
    observer.aggregateQueries_.store(0);
    const auto first = fixture.refCounter->getStats();
    REQUIRE(first.has_value());
    CHECK(first.value().totalReferences == 1u);
    REQUIRE(observer.aggregateQueries_.load() == 1u);

    REQUIRE(fixture.refCounter->increment(hash, 512, 1024).has_value());
    observer.denyBlockReads_.store(true);
    const auto denied = fixture.refCounter->getStats();
    REQUIRE_FALSE(denied.has_value());
    CHECK(denied.error().code == yams::ErrorCode::DatabaseError);
    REQUIRE(observer.deniedBlockReads_.load() > 0u);
    observer.denyBlockReads_.store(false);
    const auto recovered = fixture.refCounter->getStats();
    REQUIRE(recovered.has_value());
    CHECK(recovered.value().totalReferences == 2u);
}

TEST_CASE("ReferenceCounter unchanged statistics polls perform only one aggregate",
          "[storage][refcount][stats][cache][noscan]") {
    SqliteStatsObserver observer;
    ReferenceCounterFixture fixture;
    REQUIRE(observer.attachments_.load() == 1u);
    REQUIRE(fixture.refCounter->increment(fixture.generateHash(660), 512, 1024).has_value());
    observer.aggregateQueries_.store(0);

    for (int poll = 0; poll < 6; ++poll) {
        const auto stats = fixture.refCounter->getStats();
        REQUIRE(stats.has_value());
        CHECK(stats.value().totalBlocks == 1u);
        CHECK(stats.value().totalReferences == 1u);
        CHECK(stats.value().totalBytes == 512u);
        CHECK(stats.value().totalUncompressedBytes == 1024u);
        CHECK(stats.value().unreferencedBlocks == 0u);
        CHECK(stats.value().unreferencedBytes == 0u);
        CHECK(stats.value().transactions == 0u);
        CHECK(stats.value().rollbacks == 0u);
    }
    CHECK(observer.aggregateQueries_.load() == 1u);
}

TEST_CASE("ReferenceCounter cached statistics do not read block_references",
          "[storage][refcount][stats][cache][noscan]") {
    SqliteStatsObserver observer;
    ReferenceCounterFixture fixture;
    REQUIRE(observer.attachments_.load() == 1u);
    const auto hash = fixture.generateHash(661);
    REQUIRE(fixture.refCounter->increment(hash, 512, 1024).has_value());
    REQUIRE(fixture.refCounter->getStats().has_value());

    observer.denyBlockReads_.store(true);
    const auto cached = fixture.refCounter->getStats();
    REQUIRE(cached.has_value());
    CHECK(cached.value().totalReferences == 1u);

    // Writes themselves read this table; disable the test policy for the increment only.
    // The next denied refresh must report an error, not return an old cached success.
    observer.denyBlockReads_.store(false);
    REQUIRE(fixture.refCounter->increment(hash, 512, 1024).has_value());
    observer.denyBlockReads_.store(true);
    const auto denied = fixture.refCounter->getStats();
    REQUIRE_FALSE(denied.has_value());
    CHECK(denied.error().code == yams::ErrorCode::DatabaseError);
    CHECK(observer.deniedBlockReads_.load() > 0u);
    observer.denyBlockReads_.store(false);
    const auto fresh = fixture.refCounter->getStats();
    REQUIRE(fresh.has_value());
    CHECK(fresh.value().totalReferences == 2u);
}

TEST_CASE("ReferenceCounter uncertain generation falls back without caching success",
          "[storage][refcount][stats][cache][generation-error]") {
    SqliteStatsObserver observer;
    ReferenceCounterFixture fixture;
    REQUIRE(observer.attachments_.load() == 1u);
    REQUIRE(fixture.refCounter->increment(fixture.generateHash(669), 512, 1024).has_value());
    REQUIRE(fixture.refCounter->getStats().has_value());
    observer.aggregateQueries_.store(0);
    observer.denyDataVersion_.store(true);
    for (int poll = 0; poll < 2; ++poll) {
        const auto fallback = fixture.refCounter->getStats();
        REQUIRE(fallback.has_value());
        CHECK(fallback.value().totalBlocks == 1u);
        CHECK(fallback.value().totalReferences == 1u);
        CHECK(fallback.value().totalBytes == 512u);
        CHECK(fallback.value().totalUncompressedBytes == 1024u);
        CHECK(fallback.value().unreferencedBlocks == 0u);
        CHECK(fallback.value().unreferencedBytes == 0u);
        CHECK(fallback.value().transactions == 0u);
        CHECK(fallback.value().rollbacks == 0u);
    }
    CHECK(observer.aggregateQueries_.load() == 2u);
    // Probe failure alone is not an aggregate error. If fallback itself fails, no cached
    // success may hide it, even though this connection previously returned valid statistics.
    observer.denyBlockReads_.store(true);
    const auto denied = fixture.refCounter->getStats();
    REQUIRE_FALSE(denied.has_value());
    CHECK(denied.error().code == yams::ErrorCode::DatabaseError);
    CHECK(observer.deniedBlockReads_.load() > 0u);
    observer.denyBlockReads_.store(false);
    observer.denyDataVersion_.store(false);
    const auto recovered = fixture.refCounter->getStats();
    REQUIRE(recovered.has_value());
    CHECK(recovered.value().totalReferences == 1u);
}

TEST_CASE("ReferenceCounter null local generation requires an authoritative fallback",
          "[storage][refcount][stats][cache][generation-null]") {
    SqliteStatsObserver observer;
    ReferenceCounterFixture fixture;
    REQUIRE(observer.attachments_.load() == 1u);
    REQUIRE(observer.observedDb_ != nullptr);
    REQUIRE(fixture.refCounter->increment(fixture.generateHash(674), 512, 1024).has_value());
    REQUIRE(fixture.refCounter->getStats().has_value());
    REQUIRE(
        sqlite3_create_function_v2(
            observer.observedDb_, "total_changes", 0, SQLITE_UTF8, nullptr,
            [](sqlite3_context* context, int, sqlite3_value**) { sqlite3_result_null(context); },
            nullptr, nullptr, nullptr) == SQLITE_OK);
    // Execute a provider control first. Unsupported registration/execution is not cache RED.
    {
        sqlite3_stmt* raw = nullptr;
        REQUIRE(sqlite3_prepare_v2(observer.observedDb_, "SELECT total_changes()", -1, &raw,
                                   nullptr) == SQLITE_OK);
        auto probe =
            std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>(raw, &sqlite3_finalize);
        REQUIRE(sqlite3_step(probe.get()) == SQLITE_ROW);
        REQUIRE(sqlite3_column_type(probe.get(), 0) == SQLITE_NULL);
    }
    observer.aggregateQueries_.store(0);
    for (int poll = 0; poll < 2; ++poll) {
        const auto fallback = fixture.refCounter->getStats();
        REQUIRE(fallback.has_value());
        CHECK(fallback.value().totalBlocks == 1u);
        CHECK(fallback.value().totalReferences == 1u);
        CHECK(fallback.value().totalBytes == 512u);
        CHECK(fallback.value().totalUncompressedBytes == 1024u);
        CHECK(fallback.value().unreferencedBlocks == 0u);
        CHECK(fallback.value().unreferencedBytes == 0u);
        CHECK(fallback.value().transactions == 0u);
        CHECK(fallback.value().rollbacks == 0u);
    }
    CHECK(observer.aggregateQueries_.load() == 2u);
    observer.denyBlockReads_.store(true);
    const auto denied = fixture.refCounter->getStats();
    REQUIRE_FALSE(denied.has_value());
    CHECK(denied.error().code == yams::ErrorCode::DatabaseError);
    CHECK(observer.deniedBlockReads_.load() > 0u);
}

TEST_CASE("ReferenceCounter restored statistics discard the previous connection snapshot",
          "[storage][refcount][stats][cache][restore]") {
    SqliteStatsObserver observer;
    ReferenceCounterFixture fixture;
    REQUIRE(observer.attachments_.load() == 1u);
    const auto backupPath = std::filesystem::path(fixture.testDbPath.string() + ".stats-backup");
    REQUIRE(fixture.refCounter->increment(fixture.generateHash(670), 512, 1024).has_value());
    REQUIRE(fixture.refCounter->backup(backupPath).has_value());
    REQUIRE(fixture.refCounter->increment(fixture.generateHash(671), 256, 512).has_value());
    const auto changed = fixture.refCounter->getStats();
    REQUIRE(changed.has_value());
    CHECK(changed.value().totalBlocks == 2u);

    observer.observeNextConnection_.store(true);
    REQUIRE(fixture.refCounter->restore(backupPath).has_value());
    REQUIRE(observer.attachments_.load() == 2u);
    observer.aggregateQueries_.store(0);
    for (int poll = 0; poll < 2; ++poll) {
        const auto restored = fixture.refCounter->getStats();
        REQUIRE(restored.has_value());
        CHECK(restored.value().totalBlocks == 1u);
        CHECK(restored.value().totalReferences == 1u);
        CHECK(restored.value().totalBytes == 512u);
        CHECK(restored.value().totalUncompressedBytes == 1024u);
    }
    CHECK(observer.aggregateQueries_.load() == 1u);
    std::filesystem::remove(backupPath);
}

// Run this baseline RED separately: legacy restore closes the handle before copy_file throws,
// and the current getStats dereferences that null handle. Do not lose other baseline artifacts
// by combining this known crash characterization with the polling benchmark process.
TEST_CASE_METHOD(ReferenceCounterFixture,
                 "ReferenceCounter statistics fail safely after failed restore",
                 "[storage][refcount][stats][cache][restore-failure]") {
    REQUIRE(refCounter->increment(generateHash(672), 512, 1024).has_value());
    REQUIRE(refCounter->getStats().has_value());
    const auto missingBackup = std::filesystem::path(testDbPath.string() + ".does-not-exist");
    REQUIRE_FALSE(refCounter->restore(missingBackup).has_value());
    const auto stats = refCounter->getStats();
    REQUIRE_FALSE(stats.has_value());
    CHECK(stats.error().code == yams::ErrorCode::DatabaseError);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter statistics track local mutations",
                 "[storage][refcount][stats][cache]") {
    REQUIRE(refCounter->getStats().has_value());
    const auto checkReferences = [&](uint64_t expected) {
        const auto stats = refCounter->getStats();
        REQUIRE(stats.has_value());
        CHECK(stats.value().totalReferences == expected);
    };
    const auto firstHash = generateHash(662);
    const auto secondHash = generateHash(663);
    REQUIRE(refCounter->increment(firstHash, 512, 1024).has_value());
    checkReferences(1);
    REQUIRE(refCounter->increment(firstHash, 512, 1024).has_value());
    checkReferences(2);
    REQUIRE(refCounter->decrement(firstHash).has_value());
    checkReferences(1);

    auto batch = refCounter->beginTransaction();
    batch->increment(secondHash, 256, 512);
    batch->decrement(firstHash);
    REQUIRE(batch->commit().has_value());
    auto stats = refCounter->getStats();
    REQUIRE(stats.has_value());
    CHECK(stats.value().totalBlocks == 2u);
    CHECK(stats.value().totalReferences == 1u);
    CHECK(stats.value().totalBytes == 768u);
    CHECK(stats.value().totalUncompressedBytes == 1536u);
    CHECK(stats.value().unreferencedBlocks == 1u);
    CHECK(stats.value().unreferencedBytes == 512u);
    CHECK(stats.value().transactions == 1u);

    auto rolledBack = refCounter->beginTransaction();
    rolledBack->increment(generateHash(664), 128, 256);
    rolledBack->rollback();
    stats = refCounter->getStats();
    REQUIRE(stats.has_value());
    CHECK(stats.value().totalBlocks == 2u);
    CHECK(stats.value().rollbacks == 1u);

    auto prune = refCounter->beginTransaction();
    prune->pruneReference(firstHash);
    REQUIRE(prune->commit().has_value());
    stats = refCounter->getStats();
    REQUIRE(stats.has_value());
    CHECK(stats.value().totalBlocks == 1u);
    CHECK(stats.value().totalReferences == 1u);
    CHECK(stats.value().totalBytes == 256u);
    CHECK(stats.value().totalUncompressedBytes == 512u);
    CHECK(stats.value().unreferencedBlocks == 0u);
    CHECK(stats.value().unreferencedBytes == 0u);
    CHECK(stats.value().transactions == 2u);
    CHECK(stats.value().rollbacks == 1u);

    // The first operation writes, but the zero-size insert violates the schema. SQLite must
    // roll it back; total_changes still changes, so invalidation must remain conservative.
    auto failed = refCounter->beginTransaction();
    failed->increment(secondHash, 256, 512);
    failed->increment(generateHash(665), 0, 0);
    REQUIRE_FALSE(failed->commit().has_value());
    stats = refCounter->getStats();
    REQUIRE(stats.has_value());
    CHECK(stats.value().totalBlocks == 1u);
    CHECK(stats.value().totalReferences == 1u);
    CHECK(stats.value().totalBytes == 256u);
}

TEST_CASE_METHOD(
    ReferenceCounterFixture,
    "ReferenceCounter statistics overlap supported writes without stale completed rounds",
    "[storage][refcount][stats][cache][concurrent]") {
    constexpr size_t kRounds = 20;
    const auto hash = generateHash(673);
    REQUIRE(refCounter->getStats().has_value());
    std::barrier<> roundSync(2);
    std::array<bool, kRounds> writes{};
    std::array<std::optional<RefCountStats>, kRounds * 2> reads{};
    std::exception_ptr writerException;
    std::exception_ptr readerException;

    std::thread writer([&] {
        for (size_t round = 0; round < kRounds; ++round) {
            roundSync.arrive_and_wait();
            try {
                if (round % 2 == 0) {
                    writes[round] = refCounter->increment(hash, 512, 1024).has_value();
                } else {
                    auto transaction = refCounter->beginTransaction();
                    transaction->increment(hash, 512, 1024);
                    writes[round] = transaction->commit().has_value();
                }
            } catch (...) {
                writerException = std::current_exception();
            }
            roundSync.arrive_and_wait();
        }
    });
    // This thread is the reader, so creating a second worker cannot strand the first at a
    // two-party barrier on a thread-creation failure. Record failures, then join before asserting.
    const auto record = [&](size_t index) {
        try {
            const auto stats = refCounter->getStats();
            if (stats.has_value()) {
                reads[index] = stats.value();
            }
        } catch (...) {
            readerException = std::current_exception();
        }
    };
    for (size_t round = 0; round < kRounds; ++round) {
        roundSync.arrive_and_wait();
        record(round * 2); // Can precede or follow this round's commit.
        roundSync.arrive_and_wait();
        record(round * 2 + 1); // Writer completed; next write waits at the next start barrier.
    }
    writer.join();

    // No Catch2 assertions/exceptions cross the worker entry points. Joined results are stable.
    if (writerException) {
        std::rethrow_exception(writerException);
    }
    if (readerException) {
        std::rethrow_exception(readerException);
    }
    for (size_t round = 0; round < kRounds; ++round) {
        INFO("round=" << round);
        REQUIRE(writes[round]);
        for (size_t phase = 0; phase < 2; ++phase) {
            const auto& observation = reads[round * 2 + phase];
            REQUIRE(observation.has_value());
            const auto references = observation->totalReferences;
            if (phase == 0) {
                CHECK((references == round || references == round + 1));
            } else {
                CHECK(references == round + 1);
            }
            CHECK(observation->totalBlocks == (references == 0 ? 0u : 1u));
            CHECK(observation->totalBytes == (references == 0 ? 0u : 512u));
            CHECK(observation->totalUncompressedBytes == (references == 0 ? 0u : 1024u));
            CHECK(observation->unreferencedBlocks == 0u);
            CHECK(observation->unreferencedBytes == 0u);
            CHECK(observation->transactions == references / 2);
            CHECK(observation->rollbacks == 0u);
        }
    }
}

TEST_CASE_METHOD(ReferenceCounterFixture,
                 "ReferenceCounter statistics detect external repair and preserve reconciliation",
                 "[storage][refcount][stats][cache][external]") {
    const auto hash = generateHash(666);
    REQUIRE(refCounter->increment(hash, 512, 1024).has_value());
    REQUIRE(refCounter->getStats().has_value());
    SqliteHandle writer(testDbPath);
    execSql(writer.db, "BEGIN IMMEDIATE");
    execSql(writer.db, std::format("UPDATE block_references SET ref_count=0, block_size=2048, "
                                   "uncompressed_size=4096 WHERE block_hash='{}'",
                                   hash));
    // An uncommitted external write does not change the committed statistics snapshot.
    auto stats = refCounter->getStats();
    REQUIRE(stats.has_value());
    CHECK(stats.value().totalReferences == 1u);
    CHECK(stats.value().totalBytes == 512u);
    execSql(writer.db, "COMMIT");
    stats = refCounter->getStats();
    REQUIRE(stats.has_value());
    CHECK(stats.value().totalBlocks == 1u);
    CHECK(stats.value().totalReferences == 0u);
    CHECK(stats.value().totalBytes == 2048u);
    CHECK(stats.value().totalUncompressedBytes == 4096u);
    CHECK(stats.value().unreferencedBlocks == 1u);
    CHECK(stats.value().unreferencedBytes == 2048u);

    execSql(writer.db, "UPDATE ref_statistics SET stat_value=-321 WHERE stat_name='total_blocks'");
    for (int poll = 0; poll < 3; ++poll) {
        stats = refCounter->getStats();
        REQUIRE(stats.has_value());
        CHECK(stats.value().totalBlocks == 1u);
        CHECK(stats.value().totalReferences == 0u);
        CHECK(stats.value().totalBytes == 2048u);
        CHECK(stats.value().totalUncompressedBytes == 4096u);
        CHECK(stats.value().unreferencedBlocks == 1u);
        CHECK(stats.value().unreferencedBytes == 2048u);
        CHECK(stats.value().transactions == 0u);
        CHECK(stats.value().rollbacks == 0u);
    }
    CHECK(queryInt64(testDbPath,
                     "SELECT stat_value FROM ref_statistics WHERE stat_name='total_blocks'") ==
          -321);
}

TEST_CASE("ReferenceCounter does not cache an aggregate under a later external commit",
          "[storage][refcount][stats][cache][external][race]") {
    SqliteStatsObserver observer;
    ReferenceCounterFixture fixture;
    REQUIRE(observer.attachments_.load() == 1u);
    const auto hash = fixture.generateHash(667);
    REQUIRE(fixture.refCounter->increment(hash, 512, 1024).has_value());
    SqliteHandle writer(fixture.testDbPath);
    observer.aggregateQueries_.store(0);
    observer.externalWriter_ = writer.db;
    observer.externalWrite_ = std::format(
        "UPDATE block_references SET ref_count=ref_count+1 WHERE block_hash='{}'", hash);
    observer.injectExternalCommit_.store(true);

    const auto beforeCommit = fixture.refCounter->getStats();
    REQUIRE(beforeCommit.has_value());
    CHECK(beforeCommit.value().totalReferences == 1u);
    REQUIRE(observer.externalCommitResult_.load() == SQLITE_OK);
    const auto afterCommit = fixture.refCounter->getStats();
    REQUIRE(afterCommit.has_value());
    CHECK(afterCommit.value().totalReferences == 2u);
    CHECK(observer.aggregateQueries_.load() == 2u);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter cached statistics expose schema errors",
                 "[storage][refcount][stats][cache][external][edge]") {
    REQUIRE(refCounter->increment(generateHash(668), 512, 1024).has_value());
    REQUIRE(refCounter->getStats().has_value());
    SqliteHandle writer(testDbPath);
    execSql(writer.db, "DROP TABLE ref_statistics");
    const auto stats = refCounter->getStats();
    REQUIRE_FALSE(stats.has_value());
    CHECK(stats.error().code == yams::ErrorCode::DatabaseError);
}

TEST_CASE("ReferenceCounter statistics polling benchmark", "[.stats-benchmark]") {
    // Component-only, unchanged-database polls: not an ingest-throughput or daemon CPU claim.
    // The first read is a cold stats snapshot, not cold disk/cache: setup already touches pages.
    // Execute this same case three times per frozen binary and retain each REF_STATS_BENCH line.
    constexpr size_t kPolls = 50;
    for (const size_t rows : {1000u, 100000u}) {
        SqliteStatsObserver observer;
        ReferenceCounterFixture fixture;
        REQUIRE(observer.attachments_.load() == 1u);
        SqliteHandle writer(fixture.testDbPath);
        execSql(writer.db, std::format(R"(
            WITH RECURSIVE blocks(i) AS (
                VALUES(1) UNION ALL SELECT i+1 FROM blocks WHERE i < {}
            )
            INSERT INTO block_references(block_hash, ref_count, block_size, uncompressed_size,
                                         created_at, last_accessed)
            SELECT printf('%064x', i), CASE WHEN i % 3 = 0 THEN 0 ELSE 2 END, 4096, 8192,
                   1700000000, 1700000000
            FROM blocks
        )",
                                       rows));
        uint64_t expectedReferences = 2 * (rows - rows / 3);
        observer.aggregateQueries_.store(0);
        const auto poll = [&] {
            const auto start = std::chrono::steady_clock::now();
            const auto stats = fixture.refCounter->getStats();
            const double elapsedMs =
                std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start)
                    .count();
            REQUIRE(stats.has_value());
            CHECK(stats.value().totalBlocks == rows);
            CHECK(stats.value().totalReferences == expectedReferences);
            CHECK(stats.value().totalBytes == rows * 4096u);
            CHECK(stats.value().totalUncompressedBytes == rows * 8192u);
            CHECK(stats.value().unreferencedBlocks == rows / 3);
            CHECK(stats.value().unreferencedBytes == (rows / 3) * 4096u);
            CHECK(stats.value().transactions == 0u);
            CHECK(stats.value().rollbacks == 0u);
            return elapsedMs;
        };
        const double coldMs = poll();
        REQUIRE(observer.aggregateQueries_.load() == 1u);
        std::vector<double> warmMs;
        warmMs.reserve(kPolls);
        for (size_t i = 0; i < kPolls; ++i) {
            warmMs.push_back(poll());
        }
        const size_t warmPinnedQueries = observer.aggregateQueries_.load() - 1;
        std::sort(warmMs.begin(), warmMs.end());
        execSql(writer.db, "UPDATE block_references SET ref_count=ref_count+1 "
                           "WHERE block_hash=printf('%064x', 1)");
        ++expectedReferences;
        const double invalidationMs = poll();
        std::cout << std::format(
            "REF_STATS_BENCH {{\"rows\":{},\"polls\":{},\"cold_snapshot_ms\":{:.6f},"
            "\"warm_p50_ms\":{:.6f},\"warm_p95_ms\":{:.6f},\"warm_pinned_aggregate_queries\":{},"
            "\"external_invalidation_ms\":{:.6f},\"total_pinned_aggregate_queries\":{}}}\n",
            rows, kPolls, coldMs, (warmMs[(kPolls - 1) / 2] + warmMs[kPolls / 2]) / 2,
            warmMs[(kPolls * 95 + 99) / 100 - 1], warmPinnedQueries, invalidationMs,
            observer.aggregateQueries_.load());
        std::cout.flush();
    }
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter concurrent operations",
                 "[storage][refcount][concurrent][catch2]") {
    const size_t numThreads = 10;
    const size_t opsPerThread = 100;
    const std::string sharedHash = generateHash(700);

    std::vector<std::thread> threads;
    std::atomic<size_t> successCount{0};

    // Each thread increments the same hash
    for (size_t t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, &successCount, &sharedHash]() {
            for (size_t i = 0; i < opsPerThread; ++i) {
                auto result = refCounter->increment(sharedHash, 1024);
                if (result.has_value()) {
                    successCount++;
                }
            }
        });
    }

    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }

    // Verify final count
    auto count = refCounter->getRefCount(sharedHash);
    REQUIRE(count.has_value());
    CHECK(count.value() == successCount.load());
    CHECK(count.value() == numThreads * opsPerThread);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter concurrent transactions",
                 "[storage][refcount][concurrent][transaction][catch2]") {
    const size_t numThreads = 5;
    const size_t hashesPerThread = 20;

    std::vector<std::thread> threads;
    std::atomic<size_t> commitCount{0};
    std::atomic<size_t> errorCount{0};

    for (size_t t = 0; t < numThreads; ++t) {
        threads.emplace_back([this, &commitCount, &errorCount, t]() {
            try {
                auto txn = refCounter->beginTransaction();
                if (!txn) {
                    errorCount++;
                    return;
                }

                // Each thread works on its own set of hashes
                for (size_t i = 0; i < hashesPerThread; ++i) {
                    auto hash = generateHash(800 + t * 100 + i);
                    txn->increment(hash, 2048);
                }

                auto result = txn->commit();
                if (result.has_value()) {
                    commitCount++;
                } else {
                    errorCount++;
                }
            } catch (const std::exception&) {
                errorCount++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    CHECK(errorCount.load() == 0u);
    CHECK(commitCount.load() == numThreads);

    // Verify all hashes were incremented correctly
    for (size_t t = 0; t < numThreads; ++t) {
        for (size_t i = 0; i < hashesPerThread; ++i) {
            auto hash = generateHash(800 + t * 100 + i);
            auto count = refCounter->getRefCount(hash);
            REQUIRE(count.has_value());
            CHECK(count.value() == 1u);
        }
    }
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter maintenance operations",
                 "[storage][refcount][maintenance][catch2]") {
    // Add some data
    for (int i = 0; i < 100; ++i) {
        refCounter->increment(generateHash(900 + i), 1024);
    }

    // Test vacuum
    auto result = refCounter->vacuum();
    CHECK(result.has_value());

    // Test checkpoint
    result = refCounter->checkpoint();
    CHECK(result.has_value());

    // Test analyze
    result = refCounter->analyze();
    CHECK(result.has_value());

    // Test integrity check
    auto integrityResult = refCounter->verifyIntegrity();
    REQUIRE(integrityResult.has_value());
    CHECK(integrityResult.value());
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter backup restore",
                 "[storage][refcount][backup][catch2]") {
    // Add some data
    const std::string hash1 = generateHash(1000);
    const std::string hash2 = generateHash(1001);

    refCounter->increment(hash1, 1024);
    refCounter->increment(hash2, 2048);
    refCounter->increment(hash2, 2048); // Count = 2

    // Create backup
    auto backupPath = std::filesystem::temp_directory_path() / "yams_backup_catch2.db";
    auto result = refCounter->backup(backupPath);
    REQUIRE(result.has_value());

    // Modify data
    refCounter->decrement(hash1);
    refCounter->increment(generateHash(1002), 4096);

    // Restore from backup
    result = refCounter->restore(backupPath);
    REQUIRE(result.has_value());

    // Verify restored state
    auto count1 = refCounter->getRefCount(hash1);
    REQUIRE(count1.has_value());
    CHECK(count1.value() == 1u); // Original value

    auto count2 = refCounter->getRefCount(hash2);
    REQUIRE(count2.has_value());
    CHECK(count2.value() == 2u); // Original value

    auto count3 = refCounter->getRefCount(generateHash(1002));
    REQUIRE(count3.has_value());
    CHECK(count3.value() == 0u); // Not in backup

    // Cleanup
    std::filesystem::remove(backupPath);
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter backup restore error paths",
                 "[storage][refcount][backup][edge][catch2]") {
    auto missingParent = std::filesystem::temp_directory_path() /
                         std::format("yams_refcount_missing_parent_{}",
                                     std::chrono::system_clock::now().time_since_epoch().count()) /
                         "backup.db";

    auto backupResult = refCounter->backup(missingParent);
    CHECK_FALSE(backupResult.has_value());
    CHECK(backupResult.error().code == yams::ErrorCode::DatabaseError);

    auto missingBackup = std::filesystem::temp_directory_path() /
                         std::format("yams_refcount_missing_backup_{}.db",
                                     std::chrono::system_clock::now().time_since_epoch().count());
    auto restoreResult = refCounter->restore(missingBackup);
    CHECK_FALSE(restoreResult.has_value());
    CHECK(restoreResult.error().code == yams::ErrorCode::DatabaseError);
}

TEST_CASE("ReferenceCounter restore rejects corrupt database image",
          "[storage][refcount][backup][edge][catch2]") {
    auto tempDir = std::filesystem::temp_directory_path();
    auto stamp = std::chrono::system_clock::now().time_since_epoch().count();
    auto dbPath = tempDir / std::format("yams_refcount_corrupt_restore_{}.db", stamp);
    auto corruptPath = tempDir / std::format("yams_refcount_corrupt_restore_src_{}.db", stamp);

    auto cleanup = [&] {
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
        std::filesystem::remove(dbPath.string() + "-wal", ec);
        std::filesystem::remove(dbPath.string() + "-shm", ec);
        std::filesystem::remove(corruptPath, ec);
    };
    cleanup();

    {
        ReferenceCounter::Config config{
            .databasePath = dbPath, .enableWAL = true, .enableStatistics = true};
        ReferenceCounter counter(std::move(config));

        {
            std::ofstream corrupt(corruptPath.string(), std::ios::binary | std::ios::trunc);
            REQUIRE(corrupt.good());
            corrupt << "not a sqlite database";
        }

        auto restoreResult = counter.restore(corruptPath);
        CHECK_FALSE(restoreResult.has_value());
        CHECK(restoreResult.error().code == yams::ErrorCode::DatabaseError);
    }

    cleanup();
}

TEST_CASE("ReferenceCounter factory returns null for invalid database path",
          "[storage][refcount][factory][edge][catch2]") {
    auto tempRoot = std::filesystem::temp_directory_path();
    auto blockerPath =
        tempRoot / std::format("yams_refcount_blocker_{}",
                               std::chrono::system_clock::now().time_since_epoch().count());
    {
        std::ofstream blocker(blockerPath.string(), std::ios::binary);
        REQUIRE(blocker.good());
        blocker << "x";
    }

    ReferenceCounter::Config config{.databasePath = blockerPath / "nested" / "refcount.db",
                                    .enableWAL = true,
                                    .enableStatistics = true,
                                    .cacheSize = 1000,
                                    .busyTimeout = 1000,
                                    .enableAuditLog = false};

    auto counter = createReferenceCounter(std::move(config));
    CHECK(counter == nullptr);

    std::error_code ec;
    std::filesystem::remove(blockerPath, ec);
}

TEST_CASE("ReferenceCounter upgrades schema discovered via YAMS_DATA_DIR",
          "[storage][refcount][bootstrap][catch2]") {
    yams::test::TempDirGuard tempDir("yams_refcount_schema_");
    const auto dbPath = tempDir.path() / "refs.db";
    const auto schemaPath = tempDir.path() / "reference_schema.sql";
    yams::test::write_file(schemaPath, R"(
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS block_references (
            block_hash TEXT PRIMARY KEY,
            ref_count INTEGER NOT NULL DEFAULT 0,
            block_size INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            last_accessed INTEGER NOT NULL,
            metadata TEXT,
            CHECK (ref_count >= 0),
            CHECK (block_size > 0)
        );

        CREATE TABLE IF NOT EXISTS ref_transactions (
            transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_timestamp INTEGER NOT NULL,
            commit_timestamp INTEGER,
            state TEXT NOT NULL DEFAULT 'PENDING'
        );

        CREATE TABLE IF NOT EXISTS ref_transaction_ops (
            op_id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id INTEGER NOT NULL,
            block_hash TEXT NOT NULL,
            operation TEXT NOT NULL,
            delta INTEGER NOT NULL DEFAULT 1,
            block_size INTEGER,
            timestamp INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ref_statistics (
            stat_name TEXT PRIMARY KEY,
            stat_value INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        INSERT OR IGNORE INTO ref_statistics(stat_name, stat_value, updated_at) VALUES
            ('total_blocks', 0, strftime('%s', 'now')),
            ('total_references', 0, strftime('%s', 'now')),
            ('total_bytes', 0, strftime('%s', 'now')),
            ('transactions_completed', 0, strftime('%s', 'now')),
            ('transactions_rolled_back', 0, strftime('%s', 'now'));
    )");
    yams::test::ScopedEnvVar dataDirEnv("YAMS_DATA_DIR",
                                        std::optional<std::string>{tempDir.path().string()});

    {
        ReferenceCounter::Config config{.databasePath = dbPath,
                                        .enableWAL = true,
                                        .enableStatistics = true,
                                        .cacheSize = 1000,
                                        .busyTimeout = 1000,
                                        .enableAuditLog = false};
        ReferenceCounter counter(std::move(config));

        CHECK(columnExists(dbPath, "block_references", "uncompressed_size"));
        CHECK(
            queryInt64(
                dbPath,
                "SELECT COUNT(*) FROM ref_statistics WHERE stat_name='total_uncompressed_bytes'") ==
            1);

        const auto hash = std::format("{:064x}", 0xABC);
        REQUIRE(counter.increment(hash, 512, 2048).has_value());
        REQUIRE(counter.decrement(hash).has_value());

        auto stats = counter.getStats();
        REQUIRE(stats.has_value());
        CHECK(stats.value().totalBlocks == 1u);
        CHECK(stats.value().totalReferences == 0u);
        CHECK(stats.value().totalBytes == 512u);
        CHECK(stats.value().totalUncompressedBytes == 2048u);
        CHECK(stats.value().unreferencedBlocks == 1u);
        CHECK(stats.value().unreferencedBytes == 512u);
    }
}

TEST_CASE("ReferenceCounter upgrades existing legacy database state on open",
          "[storage][refcount][migration][catch2]") {
    yams::test::TempDirGuard tempDir("yams_refcount_legacy_");
    const auto dbPath = tempDir.path() / "refs.db";
    const auto legacyHash = std::format("{:064x}", 0x1234);

    {
        SqliteHandle db(dbPath);
        execSql(db.db, R"(
            CREATE TABLE block_references (
                block_hash TEXT PRIMARY KEY,
                ref_count INTEGER NOT NULL DEFAULT 0,
                block_size INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                last_accessed INTEGER NOT NULL,
                metadata TEXT
            );
        )");
        execSql(db.db, R"(
            CREATE TABLE ref_statistics (
                stat_name TEXT PRIMARY KEY,
                stat_value INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
        )");
        execSql(db.db, std::format(R"(
            INSERT INTO block_references
                (block_hash, ref_count, block_size, created_at, last_accessed, metadata)
            VALUES ('{}', 1, 111, strftime('%s', 'now'), strftime('%s', 'now'), NULL);
        )",
                                   legacyHash));
        execSql(db.db, R"(
            INSERT INTO ref_statistics(stat_name, stat_value, updated_at) VALUES
                ('total_blocks', 0, strftime('%s', 'now')),
                ('total_references', 0, strftime('%s', 'now')),
                ('total_bytes', 0, strftime('%s', 'now')),
                ('transactions_completed', 0, strftime('%s', 'now')),
                ('transactions_rolled_back', 0, strftime('%s', 'now'));
        )");
    }

    {
        ReferenceCounter::Config config{.databasePath = dbPath,
                                        .enableWAL = true,
                                        .enableStatistics = true,
                                        .cacheSize = 1000,
                                        .busyTimeout = 1000,
                                        .enableAuditLog = false};
        ReferenceCounter counter(std::move(config));

        CHECK(columnExists(dbPath, "block_references", "uncompressed_size"));
        CHECK(
            queryInt64(
                dbPath,
                "SELECT COUNT(*) FROM ref_statistics WHERE stat_name='total_uncompressed_bytes'") ==
            1);

        auto legacyCount = counter.getRefCount(legacyHash);
        REQUIRE(legacyCount.has_value());
        CHECK(legacyCount.value() == 1u);

        auto stats = counter.getStats();
        REQUIRE(stats.has_value());
        CHECK(stats.value().totalBlocks == 1u);
        CHECK(stats.value().totalReferences == 1u);
        CHECK(stats.value().totalBytes == 111u);
        CHECK(stats.value().totalUncompressedBytes == 111u);

        const auto newHash = std::format("{:064x}", 0x5678);
        REQUIRE(counter.increment(newHash, 50, 75).has_value());

        stats = counter.getStats();
        REQUIRE(stats.has_value());
        CHECK(stats.value().totalBlocks == 2u);
        CHECK(stats.value().totalReferences == 2u);
        CHECK(stats.value().totalBytes == 161u);
        CHECK(stats.value().totalUncompressedBytes == 186u);
    }
}

TEST_CASE_METHOD(ReferenceCounterFixture,
                 "ReferenceCounter pruneReference removes multiple rows in one commit",
                 "[storage][refcount][transaction][prune][catch2]") {
    std::vector<std::string> hashes;
    for (int i = 0; i < 3; ++i) {
        hashes.push_back(generateHash(950 + i));
        REQUIRE(refCounter->increment(hashes.back(), 256, 512).has_value());
        REQUIRE(refCounter->decrement(hashes.back()).has_value());
    }

    auto before = refCounter->getStats();
    REQUIRE(before.has_value());
    CHECK(before.value().totalBlocks == 3u);
    CHECK(before.value().unreferencedBlocks == 3u);

    auto txn = refCounter->beginTransaction();
    REQUIRE(txn != nullptr);
    for (const auto& hash : hashes) {
        txn->pruneReference(hash);
    }
    REQUIRE(txn->commit().has_value());

    auto after = refCounter->getStats();
    REQUIRE(after.has_value());
    CHECK(after.value().totalBlocks == 0u);
    CHECK(after.value().totalReferences == 0u);
    CHECK(after.value().totalBytes == 0u);
    CHECK(after.value().totalUncompressedBytes == 0u);
    CHECK(after.value().unreferencedBlocks == 0u);
    CHECK(after.value().unreferencedBytes == 0u);

    auto unreferenced = refCounter->getUnreferencedBlocks(10, std::chrono::seconds(0));
    REQUIRE(unreferenced.has_value());
    CHECK(unreferenced.value().empty());
}

TEST_CASE_METHOD(ReferenceCounterFixture, "ReferenceCounter async batch operations",
                 "[storage][refcount][async][catch2]") {
    const size_t batchSize = 50;
    std::vector<std::string> hashes;

    for (size_t i = 0; i < batchSize; ++i) {
        hashes.push_back(generateHash(1100 + i));
    }

    // Async batch increment
    auto future = refCounter->incrementBatchAsync(hashes, 1024);
    auto result = future.get();
    REQUIRE(result.has_value());

    // Verify all incremented
    for (const auto& hash : hashes) {
        auto count = refCounter->getRefCount(hash);
        REQUIRE(count.has_value());
        CHECK(count.value() == 1u);
    }
}

// Garbage Collector tests

TEST_CASE_METHOD(GarbageCollectorFixture, "GarbageCollector basic collection",
                 "[storage][gc][catch2]") {
    // Store some blocks
    std::vector<std::byte> data(1024, std::byte{42});
    std::vector<std::string> hashes;

    for (int i = 0; i < 5; ++i) {
        auto hash = generateHash(i);
        hashes.push_back(hash);

        // Store in storage engine
        auto storeResult = storageEngine->store(hash, data);
        REQUIRE(storeResult.has_value());

        // Add reference
        auto refResult = refCounter->increment(hash, data.size());
        REQUIRE(refResult.has_value());
    }

    // Remove references from some blocks
    for (int i = 0; i < 3; ++i) {
        auto refResult = refCounter->decrement(hashes[i]);
        REQUIRE(refResult.has_value());
    }

    // Run garbage collection (with 0 second min age for testing)
    GCOptions options{
        .maxBlocksPerRun = 10, .minAgeSeconds = 0, .dryRun = false, .progressCallback = nullptr};

    auto result = gc->collect(options);
    REQUIRE(result.has_value());

    const auto& stats = result.value();
    CHECK(stats.blocksScanned == 3u); // 3 unreferenced blocks
    CHECK(stats.blocksDeleted == 3u);
    CHECK(stats.bytesReclaimed == 3u * data.size());
    CHECK(stats.errors.empty());

    // Verify unreferenced blocks were deleted
    for (int i = 0; i < 3; ++i) {
        auto exists = storageEngine->exists(hashes[i]);
        REQUIRE(exists.has_value());
        CHECK_FALSE(exists.value());
    }

    // Verify referenced blocks still exist
    for (int i = 3; i < 5; ++i) {
        auto exists = storageEngine->exists(hashes[i]);
        REQUIRE(exists.has_value());
        CHECK(exists.value());
    }
}

TEST_CASE_METHOD(GarbageCollectorFixture, "GarbageCollector dry run collection",
                 "[storage][gc][catch2]") {
    // Store some unreferenced blocks
    std::vector<std::byte> data(1024, std::byte{42});
    std::vector<std::string> hashes;

    for (int i = 0; i < 3; ++i) {
        auto hash = generateHash(100 + i);
        hashes.push_back(hash);

        // Store in storage engine
        storageEngine->store(hash, data);

        // Add and remove reference
        refCounter->increment(hash, data.size());
        refCounter->decrement(hash);
    }

    // Run dry-run collection
    std::vector<std::string> progressHashes;
    GCOptions options{
        .maxBlocksPerRun = 10,
        .minAgeSeconds = 0,
        .dryRun = true,
        .progressCallback = [&progressHashes](const std::string& hash, size_t /*count*/) {
            progressHashes.push_back(hash);
        }};

    auto result = gc->collect(options);
    REQUIRE(result.has_value());

    const auto& stats = result.value();
    CHECK(stats.blocksScanned == 3u);
    CHECK(stats.blocksDeleted == 0u);  // dry-run: counted as scanned, not deleted
    CHECK(stats.bytesReclaimed == 0u); // dry-run: no bytes actually reclaimed
    CHECK(stats.errors.empty());
    CHECK(progressHashes.size() == hashes.size());

    // Verify blocks still exist (dry run)
    for (const auto& hash : hashes) {
        auto exists = storageEngine->exists(hash);
        REQUIRE(exists.has_value());
        CHECK(exists.value());

        auto count = refCounter->getRefCount(hash);
        REQUIRE(count.has_value());
        CHECK(count.value() == 0u);
    }

    auto lastStats = gc->getLastStats();
    CHECK(lastStats.blocksScanned == stats.blocksScanned);
    CHECK(lastStats.blocksDeleted == stats.blocksDeleted);
    CHECK(lastStats.bytesReclaimed == stats.bytesReclaimed);
}

TEST_CASE_METHOD(GarbageCollectorFixture,
                 "GarbageCollector updates gc statistics only for committed deletions",
                 "[storage][gc][stats][catch2]") {
    std::vector<std::byte> data(512, std::byte{7});
    const auto hashA = generateHash(140);
    const auto hashB = generateHash(141);

    for (const auto& hash : {hashA, hashB}) {
        REQUIRE(storageEngine->store(hash, data).has_value());
        REQUIRE(refCounter->increment(hash, data.size()).has_value());
        REQUIRE(refCounter->decrement(hash).has_value());
    }

    GCOptions dryRun{
        .maxBlocksPerRun = 10, .minAgeSeconds = 0, .dryRun = true, .progressCallback = nullptr};
    auto dryRunResult = gc->collect(dryRun);
    REQUIRE(dryRunResult.has_value());
    CHECK(queryInt64(testDbPath,
                     "SELECT stat_value FROM ref_statistics WHERE stat_name='gc_runs'") == 0);
    CHECK(queryInt64(
              testDbPath,
              "SELECT stat_value FROM ref_statistics WHERE stat_name='gc_blocks_collected'") == 0);
    CHECK(queryInt64(
              testDbPath,
              "SELECT stat_value FROM ref_statistics WHERE stat_name='gc_bytes_reclaimed'") == 0);

    GCOptions realRun{
        .maxBlocksPerRun = 10, .minAgeSeconds = 0, .dryRun = false, .progressCallback = nullptr};
    auto realRunResult = gc->collect(realRun);
    REQUIRE(realRunResult.has_value());
    CHECK(realRunResult.value().blocksDeleted == 2u);
    CHECK(queryInt64(testDbPath,
                     "SELECT stat_value FROM ref_statistics WHERE stat_name='gc_runs'") == 1);
    CHECK(queryInt64(
              testDbPath,
              "SELECT stat_value FROM ref_statistics WHERE stat_name='gc_blocks_collected'") == 2);
    CHECK(
        queryInt64(testDbPath,
                   "SELECT stat_value FROM ref_statistics WHERE stat_name='gc_bytes_reclaimed'") ==
        static_cast<int64_t>(2 * data.size()));
}

TEST_CASE_METHOD(GarbageCollectorFixture, "GarbageCollector respects minimum age horizon",
                 "[storage][gc][edge][catch2]") {
    std::vector<std::byte> data(1024, std::byte{42});
    auto hash = generateHash(150);

    REQUIRE(storageEngine->store(hash, data).has_value());
    REQUIRE(refCounter->increment(hash, data.size()).has_value());
    REQUIRE(refCounter->decrement(hash).has_value());

    GCOptions options{
        .maxBlocksPerRun = 10, .minAgeSeconds = 3600, .dryRun = false, .progressCallback = nullptr};

    auto result = gc->collect(options);
    REQUIRE(result.has_value());

    const auto& stats = result.value();
    CHECK(stats.blocksScanned == 0u);
    CHECK(stats.blocksDeleted == 0u);
    CHECK(stats.bytesReclaimed == 0u);
    CHECK(stats.errors.empty());

    auto exists = storageEngine->exists(hash);
    REQUIRE(exists.has_value());
    CHECK(exists.value());

    auto count = refCounter->getRefCount(hash);
    REQUIRE(count.has_value());
    CHECK(count.value() == 0u);
}

TEST_CASE_METHOD(GarbageCollectorFixture,
                 "GarbageCollector preserves failure evidence and continues after missing block",
                 "[storage][gc][edge][catch2]") {
    std::vector<std::byte> data(1024, std::byte{42});
    auto presentHash = generateHash(175);
    auto missingHash = generateHash(176);

    REQUIRE(storageEngine->store(presentHash, data).has_value());

    REQUIRE(refCounter->increment(presentHash, data.size()).has_value());
    REQUIRE(refCounter->decrement(presentHash).has_value());

    REQUIRE(refCounter->increment(missingHash, data.size()).has_value());
    REQUIRE(refCounter->decrement(missingHash).has_value());

    GCOptions options{
        .maxBlocksPerRun = 10, .minAgeSeconds = 0, .dryRun = false, .progressCallback = nullptr};

    auto result = gc->collect(options);
    REQUIRE(result.has_value());

    const auto& stats = result.value();
    CHECK(stats.blocksScanned == 2u);
    CHECK(stats.blocksDeleted == 1u);
    CHECK(stats.bytesReclaimed == data.size());
    REQUIRE(stats.errors.size() == 1u);
    CHECK(stats.errors.front().find(missingHash) != std::string::npos);

    auto presentExists = storageEngine->exists(presentHash);
    REQUIRE(presentExists.has_value());
    CHECK_FALSE(presentExists.value());

    auto missingExists = storageEngine->exists(missingHash);
    REQUIRE(missingExists.has_value());
    CHECK_FALSE(missingExists.value());

    auto presentCount = refCounter->getRefCount(presentHash);
    REQUIRE(presentCount.has_value());
    CHECK(presentCount.value() == 0u);

    auto missingCount = refCounter->getRefCount(missingHash);
    REQUIRE(missingCount.has_value());
    CHECK(missingCount.value() == 0u);

    auto lastStats = gc->getLastStats();
    CHECK(lastStats.blocksScanned == stats.blocksScanned);
    CHECK(lastStats.blocksDeleted == stats.blocksDeleted);
    CHECK(lastStats.errors == stats.errors);
}

TEST_CASE_METHOD(GarbageCollectorFixture, "GarbageCollector async collection",
                 "[storage][gc][async][catch2]") {
    // Create unreferenced block
    std::vector<std::byte> data(1024, std::byte{42});
    auto hash = generateHash(200);

    storageEngine->store(hash, data);
    refCounter->increment(hash, data.size());
    refCounter->decrement(hash);

    // Run async collection
    GCOptions options{
        .maxBlocksPerRun = 10, .minAgeSeconds = 0, .dryRun = false, .progressCallback = nullptr};

    auto future = gc->collectAsync(options);
    auto result = future.get();

    REQUIRE(result.has_value());
    const auto& stats = result.value();
    CHECK(stats.blocksDeleted == 1u);
}

TEST_CASE_METHOD(GarbageCollectorFixture, "GarbageCollector concurrent collection prevented",
                 "[storage][gc][concurrent][catch2]") {
    // Create an unreferenced block so the garbage collector has work to do
    std::vector<std::byte> data(1024, std::byte{42});
    auto hash = generateHash(300);
    storageEngine->store(hash, data);
    refCounter->increment(hash, data.size());
    refCounter->decrement(hash);

    // Start a long-running collection
    std::promise<void> collectionStartedPromise;
    auto collectionStartedFuture = collectionStartedPromise.get_future();
    std::atomic<bool> allowCompletion{false};
    std::atomic<int> callbackCount{0};

    GCOptions options{.maxBlocksPerRun = 10,
                      .minAgeSeconds = 0,
                      .dryRun = false,
                      .progressCallback = [&collectionStartedPromise, &allowCompletion,
                                           &callbackCount](const std::string&, size_t) {
                          int count = callbackCount.fetch_add(1);
                          if (count == 0) {
                              // First call - signal that collection has started
                              collectionStartedPromise.set_value();
                          }
                          // Block until allowed to complete to ensure concurrent attempt fails
                          while (!allowCompletion) {
                              std::this_thread::sleep_for(std::chrono::milliseconds(10));
                          }
                      }};

    // Start first collection
    auto future1 = gc->collectAsync(options);

    // Wait for it to start with timeout
    auto status = collectionStartedFuture.wait_for(std::chrono::seconds(2));
    REQUIRE(status != std::future_status::timeout);

    // Small delay to ensure the async collection is actually running
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Try to start second collection - should fail immediately
    auto result2 = gc->collect(options);
    CHECK_FALSE(result2.has_value());
    CHECK(result2.error().code == yams::ErrorCode::OperationInProgress);

    // Allow first collection to complete
    allowCompletion = true;

    // Wait for completion with timeout
    auto completeStatus = future1.wait_for(std::chrono::seconds(5));
    REQUIRE(completeStatus != std::future_status::timeout);

    auto result1 = future1.get();
    CHECK(result1.has_value());
}

TEST_CASE_METHOD(GarbageCollectorFixture, "GarbageCollector get last stats",
                 "[storage][gc][stats][catch2]") {
    // Initially empty stats
    auto stats = gc->getLastStats();
    CHECK(stats.blocksScanned == 0u);
    CHECK(stats.blocksDeleted == 0u);

    // Run collection
    GCOptions options{
        .maxBlocksPerRun = 10, .minAgeSeconds = 0, .dryRun = true, .progressCallback = nullptr};

    gc->collect(options);

    // Get updated stats
    stats = gc->getLastStats();
    static_cast<void>(stats.blocksScanned);
    CHECK(stats.duration.count() >= 0); // Duration can be 0 for very fast operations
}

TEST_CASE_METHOD(GarbageCollectorFixture, "GarbageCollector isCollecting reflects active state",
                 "[storage][gc][isCollecting][catch2]") {
    CHECK_FALSE(gc->isCollecting());

    GCOptions options{
        .maxBlocksPerRun = 10, .minAgeSeconds = 0, .dryRun = true, .progressCallback = nullptr};

    auto future = gc->collectAsync(options);

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    bool wasCollecting = gc->isCollecting();

    future.wait();
    CHECK_FALSE(gc->isCollecting());
    (void)wasCollecting;
}

TEST_CASE_METHOD(GarbageCollectorFixture, "GarbageCollector scheduleCollection starts and stops",
                 "[storage][gc][schedule][catch2]") {
    GCOptions options{
        .maxBlocksPerRun = 5, .minAgeSeconds = 0, .dryRun = true, .progressCallback = nullptr};

    CHECK_FALSE(gc->isCollecting());

    gc->scheduleCollection(std::chrono::seconds(1), options);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    gc->stopScheduledCollection();

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    CHECK_FALSE(gc->isCollecting());
}

TEST_CASE("rebuildReferenceDatabase scans storage and populates refs.db",
          "[storage][gc][rebuild][catch2]") {
    auto tempDir = std::filesystem::temp_directory_path();
    auto dbPath =
        tempDir / std::format("yams_rebuild_refs_{}.db",
                              std::chrono::system_clock::now().time_since_epoch().count());
    auto storagePath =
        tempDir / std::format("yams_rebuild_storage_{}",
                              std::chrono::system_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(storagePath);

    // Create a shard with test blocks
    auto shardDir = storagePath / "aa";
    std::filesystem::create_directories(shardDir);

    std::string testHash = std::string(62, 'a') + "bb";
    std::ofstream block(shardDir / (std::string(62, 'a') + "bb"));
    block.write("test_block_content", 18);
    block.close();

    auto cleanup = [&] {
        std::filesystem::remove_all(storagePath);
        std::filesystem::remove(dbPath);
        std::filesystem::remove(dbPath.string() + "-wal");
        std::filesystem::remove(dbPath.string() + "-shm");
    };

    auto result = rebuildReferenceDatabase(dbPath, storagePath);
    REQUIRE(result.has_value());

    // Verify refs.db was created and has the expected entry
    ReferenceCounter::Config config{
        .databasePath = dbPath, .enableWAL = true, .enableStatistics = true};
    auto refCounter = createReferenceCounter(config);

    auto count = refCounter->getRefCount(testHash);
    REQUIRE(count.has_value());
    CHECK(count.value() >= 1u);

    refCounter.reset();
    cleanup();
}

TEST_CASE("rebuildReferenceDatabase handles empty storage gracefully",
          "[storage][gc][rebuild][catch2]") {
    auto tempDir = std::filesystem::temp_directory_path();
    auto dbPath =
        tempDir / std::format("yams_rebuild_empty_{}.db",
                              std::chrono::system_clock::now().time_since_epoch().count());
    auto storagePath =
        tempDir / std::format("yams_rebuild_empty_storage_{}",
                              std::chrono::system_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(storagePath);

    auto cleanup = [&] {
        std::filesystem::remove_all(storagePath);
        std::filesystem::remove(dbPath);
        std::filesystem::remove(dbPath.string() + "-wal");
        std::filesystem::remove(dbPath.string() + "-shm");
    };

    auto result = rebuildReferenceDatabase(dbPath, storagePath);
    REQUIRE(result.has_value());

    cleanup();
}

TEST_CASE("ReferenceCounter recovery after forced close preserves committed data",
          "[storage][refcount][crash][catch2]") {
    auto tempDir = std::filesystem::temp_directory_path();
    auto stamp = std::chrono::system_clock::now().time_since_epoch().count();
    auto dbPath = tempDir / std::format("yams_refcount_crash_{}.db", stamp);

    auto cleanup = [&] {
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
        std::filesystem::remove(dbPath.string() + "-wal", ec);
        std::filesystem::remove(dbPath.string() + "-shm", ec);
    };
    cleanup();

    std::string committedHash;
    {
        ReferenceCounter::Config config{
            .databasePath = dbPath, .enableWAL = true, .enableStatistics = true};
        ReferenceCounter counter(std::move(config));

        committedHash = std::format("{:064x}", 1);
        REQUIRE(counter.increment(committedHash, 1024, 1024).has_value());
    }

    REQUIRE(std::filesystem::exists(dbPath));

    {
        ReferenceCounter::Config config{
            .databasePath = dbPath, .enableWAL = true, .enableStatistics = true};
        ReferenceCounter counter(std::move(config));

        auto count = counter.getRefCount(committedHash);
        REQUIRE(count.has_value());
        CHECK(count.value() >= 1u);

        auto integrity = counter.verifyIntegrity();
        REQUIRE(integrity.has_value());
        CHECK(integrity.value());
    }

    cleanup();
}

TEST_CASE("ReferenceCounter recovers from uncommitted transaction after forced close",
          "[storage][refcount][crash][catch2]") {
    auto tempDir = std::filesystem::temp_directory_path();
    auto stamp = std::chrono::system_clock::now().time_since_epoch().count();
    auto dbPath = tempDir / std::format("yams_refcount_crash2_{}.db", stamp);

    auto cleanup = [&] {
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
        std::filesystem::remove(dbPath.string() + "-wal", ec);
        std::filesystem::remove(dbPath.string() + "-shm", ec);
    };
    cleanup();

    std::string committedHash;
    std::string uncommittedHash;
    {
        ReferenceCounter::Config config{
            .databasePath = dbPath, .enableWAL = true, .enableStatistics = true};
        ReferenceCounter counter(std::move(config));

        committedHash = std::format("{:064x}", 10);
        REQUIRE(counter.increment(committedHash, 512, 512).has_value());

        uncommittedHash = std::format("{:064x}", 11);
        auto txn = counter.beginTransaction();
        REQUIRE(txn != nullptr);
        txn->increment(uncommittedHash, 256, 256);
    }

    {
        ReferenceCounter::Config config{
            .databasePath = dbPath, .enableWAL = true, .enableStatistics = true};
        ReferenceCounter counter(std::move(config));

        auto count = counter.getRefCount(committedHash);
        REQUIRE(count.has_value());
        CHECK(count.value() >= 1u);

        auto orphanCount = counter.getRefCount(uncommittedHash);
        REQUIRE(orphanCount.has_value());
        CHECK(orphanCount.value() == 0u);

        auto integrity = counter.verifyIntegrity();
        REQUIRE(integrity.has_value());
        CHECK(integrity.value());
    }

    cleanup();
}

TEST_CASE("ReferenceCounter rejects corrupted database file on open",
          "[storage][refcount][crash][catch2]") {
    auto tempDir = std::filesystem::temp_directory_path();
    auto stamp = std::chrono::system_clock::now().time_since_epoch().count();
    auto dbPath = tempDir / std::format("yams_refcount_corrupt_open_{}_invalid.db", stamp);

    auto cleanup = [&] {
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
        std::filesystem::remove(dbPath.string() + "-wal", ec);
        std::filesystem::remove(dbPath.string() + "-shm", ec);
    };
    cleanup();

    {
        std::ofstream corrupt(dbPath.string(), std::ios::binary | std::ios::trunc);
        REQUIRE(corrupt.good());
        corrupt << "not a valid sqlite3 database file";
        corrupt.close();
    }

    ReferenceCounter::Config config{
        .databasePath = dbPath, .enableWAL = true, .enableStatistics = true};
    REQUIRE_THROWS_AS(ReferenceCounter(std::move(config)), std::exception);

    cleanup();
}

TEST_CASE("ReferenceCounter returns consistent stats after interrupted commit recovery",
          "[storage][refcount][crash][stats][catch2]") {
    auto tempDir = std::filesystem::temp_directory_path();
    auto stamp = std::chrono::system_clock::now().time_since_epoch().count();
    auto dbPath = tempDir / std::format("yams_refcount_crash3_{}.db", stamp);

    auto cleanup = [&] {
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
        std::filesystem::remove(dbPath.string() + "-wal", ec);
        std::filesystem::remove(dbPath.string() + "-shm", ec);
    };
    cleanup();

    {
        ReferenceCounter::Config config{
            .databasePath = dbPath, .enableWAL = true, .enableStatistics = true};
        ReferenceCounter counter(std::move(config));

        for (int i = 0; i < 10; ++i) {
            REQUIRE(counter.increment(std::format("{:064x}", 100 + i), 1024, 1024).has_value());
        }

        auto txn = counter.beginTransaction();
        REQUIRE(txn != nullptr);
        txn->increment(std::format("{:064x}", 200), 512, 512);
        txn->decrement(std::format("{:064x}", 100));
    }

    {
        ReferenceCounter::Config config{
            .databasePath = dbPath, .enableWAL = true, .enableStatistics = true};
        ReferenceCounter counter(std::move(config));

        auto stats = counter.getStats();
        REQUIRE(stats.has_value());
        CHECK(stats.value().totalBlocks == 10u);

        auto block0 = counter.hasReferences(std::format("{:064x}", 100));
        REQUIRE(block0.has_value());
        CHECK(block0.value());

        auto block200 = counter.hasReferences(std::format("{:064x}", 200));
        REQUIRE(block200.has_value());
        CHECK_FALSE(block200.value());
    }

    cleanup();
}

TEST_CASE("ReferenceCounter multiple PENDING transactions survive force close",
          "[storage][refcount][crash][catch2]") {
    auto tempDir = std::filesystem::temp_directory_path();
    auto stamp = std::chrono::system_clock::now().time_since_epoch().count();
    auto dbPath = tempDir / std::format("yams_refcount_crash4_{}.db", stamp);

    auto cleanup = [&] {
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
        std::filesystem::remove(dbPath.string() + "-wal", ec);
        std::filesystem::remove(dbPath.string() + "-shm", ec);
    };
    cleanup();

    const std::string baseline = std::format("{:064x}", 500);
    {
        ReferenceCounter::Config config{
            .databasePath = dbPath, .enableWAL = true, .enableStatistics = true};
        ReferenceCounter counter(std::move(config));

        REQUIRE(counter.increment(baseline, 8192, 8192).has_value());

        std::vector<std::unique_ptr<IReferenceCounter::ITransaction>> pending;
        for (int i = 0; i < 5; ++i) {
            auto txn = counter.beginTransaction();
            REQUIRE(txn != nullptr);
            txn->increment(std::format("{:064x}", 600 + i), 256, 256);
            pending.push_back(std::move(txn));
        }
    }

    {
        ReferenceCounter::Config config{
            .databasePath = dbPath, .enableWAL = true, .enableStatistics = true};
        ReferenceCounter counter(std::move(config));

        auto count = counter.getRefCount(baseline);
        REQUIRE(count.has_value());
        CHECK(count.value() >= 1u);

        for (int i = 0; i < 5; ++i) {
            auto orphan = counter.getRefCount(std::format("{:064x}", 600 + i));
            REQUIRE(orphan.has_value());
            CHECK(orphan.value() == 0u);
        }

        auto stats = counter.getStats();
        REQUIRE(stats.has_value());
        CHECK(stats.value().totalBlocks == 1u);
    }

    cleanup();
}

TEST_CASE("ReferenceCounter survives WAL-only checkpoint after forced close",
          "[storage][refcount][crash][wal][catch2]") {
    auto tempDir = std::filesystem::temp_directory_path();
    auto stamp = std::chrono::system_clock::now().time_since_epoch().count();
    auto dbPath = tempDir / std::format("yams_refcount_crash5_{}.db", stamp);

    auto cleanup = [&] {
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
        std::filesystem::remove(dbPath.string() + "-wal", ec);
        std::filesystem::remove(dbPath.string() + "-shm", ec);
    };
    cleanup();

    {
        ReferenceCounter::Config config{
            .databasePath = dbPath, .enableWAL = true, .enableStatistics = true};
        ReferenceCounter counter(std::move(config));

        for (int i = 0; i < 20; ++i) {
            REQUIRE(counter.increment(std::format("{:064x}", 700 + i), 512, 512).has_value());
        }

        REQUIRE(counter.checkpoint().has_value());
    }

    REQUIRE(std::filesystem::exists(dbPath));

    {
        ReferenceCounter::Config config{
            .databasePath = dbPath, .enableWAL = true, .enableStatistics = true};
        ReferenceCounter counter(std::move(config));

        for (int i = 0; i < 20; ++i) {
            auto count = counter.getRefCount(std::format("{:064x}", 700 + i));
            REQUIRE(count.has_value());
            CHECK(count.value() >= 1u);
        }

        auto integrity = counter.verifyIntegrity();
        REQUIRE(integrity.has_value());
        CHECK(integrity.value());
    }

    cleanup();
}

TEST_CASE("ReferenceCounter init retries when the database is transiently locked",
          "[storage][reference][lock]") {
    const auto dbPath = std::filesystem::temp_directory_path() /
                        std::format("yams_refcount_lock_catch2_{}.db",
                                    std::chrono::system_clock::now().time_since_epoch().count());
    struct Cleanup {
        std::filesystem::path p;
        ~Cleanup() {
            std::filesystem::remove(p);
            std::filesystem::remove(p.string() + "-wal");
            std::filesystem::remove(p.string() + "-shm");
        }
    } cleanup{dbPath};

    // Hold a write lock on the database so the first init attempt must wait.
    sqlite3* blocker = nullptr;
    REQUIRE(sqlite3_open(dbPath.string().c_str(), &blocker) == SQLITE_OK);
    execSql(blocker, "BEGIN IMMEDIATE");

    // Release the lock shortly after init starts so the retry path is exercised.
    std::thread releaser([blocker] {
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        char* err = nullptr;
        sqlite3_exec(blocker, "ROLLBACK", nullptr, nullptr, &err);
        sqlite3_free(err);
        sqlite3_close(blocker);
    });

    // Small busy timeout keeps the inner retries fast; the outer init retry
    // must succeed once the lock is released.
    ReferenceCounter::Config config{.databasePath = dbPath, .enableWAL = true, .busyTimeout = 10};
    std::unique_ptr<ReferenceCounter> refCounter;
    REQUIRE_NOTHROW(refCounter = std::make_unique<ReferenceCounter>(std::move(config)));

    releaser.join();
}
