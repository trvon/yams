// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <yams/memory_sync/memory_sync_service.h>
#include <yams/storage/storage_backend.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_sync_adapter.h>

using namespace yams::vector;
using namespace yams::memory_sync;

namespace {

std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> result(text.size());
    std::memcpy(result.data(), text.data(), text.size());
    return result;
}

std::string skipReasonIfAny() {
    if (const char* skipEnv = std::getenv("YAMS_SQLITE_VEC_SKIP_INIT")) {
        std::string v(skipEnv);
        if (v == "1" || v == "true") {
            return "Skipping (YAMS_SQLITE_VEC_SKIP_INIT=1)";
        }
    }
    if (const char* disableEnv = std::getenv("YAMS_DISABLE_VECTORS")) {
        std::string v(disableEnv);
        if (v == "1" || v == "true") {
            return "Skipping (YAMS_DISABLE_VECTORS=1)";
        }
    }
    return {};
}

std::unique_ptr<VectorDatabase> makeVectorDb(std::size_t dim) {
    VectorDatabaseConfig config;
    config.database_path = ":memory:";
    config.embedding_dim = dim;
    config.create_if_missing = true;
    config.use_in_memory = true;
    config.search_engine = VectorSearchEngine::ExactScan;
    auto db = std::make_unique<VectorDatabase>(config);
    REQUIRE(db->initializeChecked().has_value());
    return db;
}

std::unique_ptr<yams::storage::FilesystemBackend> makeBackend(const std::filesystem::path& dir) {
    yams::storage::BackendConfig config;
    config.type = "filesystem";
    config.localPath = dir;
    auto backend = std::make_unique<yams::storage::FilesystemBackend>();
    REQUIRE(backend->initialize(config).has_value());
    return backend;
}

struct TempDirGuard {
    TempDirGuard() {
        path = std::filesystem::temp_directory_path() /
               ("yams-vector-sync-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path);
    }
    ~TempDirGuard() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
    std::filesystem::path path;
};

} // namespace

TEST_CASE("vector sync adapter converges an embedding from A to B",
          "[vector][memory-sync][exact-scan]") {
    const auto skip = skipReasonIfAny();
    if (!skip.empty()) {
        SKIP(skip);
    }

    TempDirGuard temp;
    auto dbA = makeVectorDb(4);
    auto dbB = makeVectorDb(4);
    MemorySyncService syncA{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService syncB{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    VectorSyncAdapter adapterA{*dbA, syncA};
    VectorSyncAdapter adapterB{*dbB, syncB};

    VectorRecord record;
    record.chunk_id = "chunk-1";
    record.document_hash = "doc-1";
    record.model_id = "model-v1";
    record.model_version = "1.0";
    record.embedding = {1.0F, 0.0F, 0.0F, 0.0F};
    record.content = "embedding content";
    record.embedding_dim = 4;

    // Commit on A through the real vector write seam, then mirror.
    REQUIRE(dbA->insertVectorChecked(record).has_value());
    REQUIRE(adapterA.publish(record).has_value());

    // B has no such embedding before apply.
    CHECK_FALSE(dbB->getVector("chunk-1").has_value());

    auto applied = adapterB.apply();
    REQUIRE(applied.has_value());
    CHECK(applied.value() == 1);

    // The replicated embedding is returned by a vector query on B.
    VectorSearchParams params;
    params.k = 3;
    params.similarity_threshold = -1.0F;
    auto results = dbB->searchSimilarChecked({1.0F, 0.0F, 0.0F, 0.0F}, params);
    REQUIRE(results.has_value());
    REQUIRE_FALSE(results.value().empty());
    CHECK(results.value().front().chunk_id == "chunk-1");
    CHECK(results.value().front().document_hash == "doc-1");
    CHECK(results.value().front().model_id == "model-v1");

    // Idempotent: a second apply does not duplicate the chunk.
    auto again = adapterB.apply();
    REQUIRE(again.has_value());
    CHECK(again.value() == 0);
    auto count = dbB->getVectorCount();
    CHECK(count == 1);

    REQUIRE(adapterA.publishDelete(record.model_id, record.chunk_id).has_value());
    auto deleted = adapterB.apply();
    REQUIRE(deleted.has_value());
    CHECK(deleted.value() == 1);
    CHECK_FALSE(dbB->getVector(record.chunk_id).has_value());
    CHECK(dbB->getVectorCount() == 0);
}

TEST_CASE("vector sync adapter rejects payload identity mismatches before mutation",
          "[vector][memory-sync][identity][exact-scan]") {
    const auto skip = skipReasonIfAny();
    if (!skip.empty()) {
        SKIP(skip);
    }
    TempDirGuard temp;
    auto target = makeVectorDb(4);
    MemorySyncService writer{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService reader{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    VectorSyncAdapter consumer{*target, reader};

    SECTION("value") {
        EmbeddingRecord record;
        record.model = "payload-model";
        record.chunkId = "payload-chunk";
        record.dimensions = 4;
        record.values = {1.0F, 0.0F, 0.0F, 0.0F};
        REQUIRE(writer
                    .publish("embedding/envelope-model/envelope-chunk",
                             bytes(nlohmann::json(record).dump()))
                    .has_value());

        const auto applied = consumer.apply();
        REQUIRE_FALSE(applied.has_value());
        CHECK(applied.error().code == yams::ErrorCode::InvalidData);
        CHECK(target->getVectorCount() == 0);
    }

    SECTION("tombstone chunk") {
        VectorRecord record;
        record.chunk_id = "chunk-b";
        record.document_hash = "doc";
        record.model_id = "model";
        record.embedding = {1.0F, 0.0F, 0.0F, 0.0F};
        record.embedding_dim = 4;
        REQUIRE(target->insertVectorChecked(record).has_value());
        REQUIRE(writer.erase("embedding/model/chunk-a", "chunk-b").has_value());

        const auto applied = consumer.apply();
        REQUIRE_FALSE(applied.has_value());
        CHECK(applied.error().code == yams::ErrorCode::InvalidData);
        CHECK(target->getVector("chunk-b").has_value());
    }

    SECTION("tombstone model") {
        VectorRecord record;
        record.chunk_id = "shared-chunk";
        record.document_hash = "doc";
        record.model_id = "retained-model";
        record.embedding = {1.0F, 0.0F, 0.0F, 0.0F};
        record.embedding_dim = 4;
        REQUIRE(target->insertVectorChecked(record).has_value());
        REQUIRE(writer.erase("embedding/other-model/shared-chunk", "shared-chunk").has_value());

        const auto applied = consumer.apply();
        REQUIRE_FALSE(applied.has_value());
        CHECK(applied.error().code == yams::ErrorCode::InvalidData);
        const auto retained = target->getVector("shared-chunk");
        REQUIRE(retained.has_value());
        CHECK(retained->model_id == "retained-model");
    }
}

TEST_CASE("vector sync adapter requires replicated content before mutation",
          "[vector][memory-sync][prerequisite]") {
    const auto skip = skipReasonIfAny();
    if (!skip.empty()) {
        SKIP(skip);
    }
    TempDirGuard temp;
    auto target = makeVectorDb(4);
    MemorySyncService writer{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService reader{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};

    EmbeddingRecord record;
    record.model = "model";
    record.chunkId = "missing-content";
    record.documentId = std::string(64, 'a');
    record.dimensions = 4;
    record.values = {1.0F, 0.0F, 0.0F, 0.0F};
    REQUIRE(writer.publish("embedding/model/missing-content", bytes(nlohmann::json(record).dump()))
                .has_value());

    VectorSyncAdapter consumer{
        *target, reader, {}, nullptr, [](std::string_view) -> yams::Result<bool> { return false; }};
    const auto applied = consumer.apply();
    REQUIRE_FALSE(applied.has_value());
    CHECK(applied.error().code == yams::ErrorCode::NotFound);
    CHECK_FALSE(target->getVector("missing-content").has_value());
}

TEST_CASE("vector sync adapter preserves rebuild dirty state after partial mutation",
          "[vector][memory-sync][rebuild][exact-scan]") {
    const auto skip = skipReasonIfAny();
    if (!skip.empty()) {
        SKIP(skip);
    }
    TempDirGuard temp;
    auto target = makeVectorDb(4);
    MemorySyncService writer{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService reader{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    bool rebuildDirty = false;
    std::size_t rebuildCalls = 0;
    auto rebuild = [&]() -> yams::Result<void> {
        ++rebuildCalls;
        return {};
    };
    VectorSyncAdapter consumer{*target, reader, rebuild, &rebuildDirty};

    EmbeddingRecord valid;
    valid.model = "model";
    valid.chunkId = "a-valid";
    valid.documentId = "doc";
    valid.dimensions = 4;
    valid.values = {1.0F, 0.0F, 0.0F, 0.0F};
    EmbeddingRecord invalid = valid;
    invalid.chunkId = "z-invalid";
    invalid.dimensions = 3;
    invalid.values = {1.0F, 0.0F, 0.0F};
    REQUIRE(
        writer.publish("embedding/model/a-valid", bytes(nlohmann::json(valid).dump())).has_value());
    REQUIRE(writer.publish("embedding/model/z-invalid", bytes(nlohmann::json(invalid).dump()))
                .has_value());

    const auto failed = consumer.apply();
    REQUIRE_FALSE(failed.has_value());
    CHECK(target->getVector("a-valid").has_value());
    CHECK(rebuildDirty);
    CHECK(rebuildCalls == 0);

    REQUIRE(writer.erase("embedding/model/z-invalid", "z-invalid").has_value());
    VectorSyncAdapter retry{*target, reader, rebuild, &rebuildDirty};
    const auto recovered = retry.apply();
    REQUIRE(recovered.has_value());
    CHECK(recovered.value() == 0);
    CHECK(rebuildCalls == 1);
    CHECK_FALSE(rebuildDirty);
}

TEST_CASE("vector sync adapter rejects unsupported multi-model chunk identity before mutation",
          "[vector][memory-sync][exact-scan]") {
    const auto skip = skipReasonIfAny();
    if (!skip.empty()) {
        SKIP(skip);
    }
    TempDirGuard temp;
    auto source = makeVectorDb(4);
    auto target = makeVectorDb(4);
    MemorySyncService syncA{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService syncB{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    VectorSyncAdapter publisher{*source, syncA};
    VectorSyncAdapter consumer{*target, syncB};

    VectorRecord first;
    first.chunk_id = "shared-chunk";
    first.document_hash = "doc";
    first.model_id = "model-a";
    first.embedding = {1.0F, 0.0F, 0.0F, 0.0F};
    first.embedding_dim = 4;
    auto second = first;
    second.model_id = "model-b";
    second.embedding = {0.0F, 1.0F, 0.0F, 0.0F};
    REQUIRE(publisher.publish(first).has_value());
    REQUIRE(publisher.publish(second).has_value());

    const auto applied = consumer.apply();
    REQUIRE_FALSE(applied.has_value());
    CHECK(applied.error().code == yams::ErrorCode::NotSupported);
    CHECK(target->getVectorCount() == 0);
}

TEST_CASE("vector sync adapter validates malformed winner batches before mutation",
          "[vector][memory-sync][exact-scan]") {
    const auto skip = skipReasonIfAny();
    if (!skip.empty()) {
        SKIP(skip);
    }
    TempDirGuard temp;
    auto source = makeVectorDb(4);
    auto target = makeVectorDb(4);
    MemorySyncService syncA{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService syncB{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    VectorSyncAdapter publisher{*source, syncA};
    VectorSyncAdapter consumer{*target, syncB};
    VectorRecord valid;
    valid.chunk_id = "valid-chunk";
    valid.document_hash = "doc";
    valid.model_id = "model";
    valid.embedding = {1.0F, 0.0F, 0.0F, 0.0F};
    valid.embedding_dim = 4;
    REQUIRE(publisher.publish(valid).has_value());
    const std::string malformed = R"({"chunk_id":"broken"})";
    const auto malformedBytes = std::span<const std::byte>(
        reinterpret_cast<const std::byte*>(malformed.data()), malformed.size());
    REQUIRE(syncA.publish("embedding/model/broken", malformedBytes).has_value());

    const auto applied = consumer.apply();
    REQUIRE_FALSE(applied.has_value());
    CHECK(applied.error().code == yams::ErrorCode::InvalidData);
    CHECK(target->getVectorCount() == 0);
}

TEST_CASE("vector sync adapter propagates index rebuild failure and retries winner",
          "[vector][memory-sync][exact-scan]") {
    const auto skip = skipReasonIfAny();
    if (!skip.empty()) {
        SKIP(skip);
    }
    TempDirGuard temp;
    auto source = makeVectorDb(4);
    auto target = makeVectorDb(4);
    MemorySyncService syncA{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService syncB{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    VectorSyncAdapter publisher{*source, syncA};
    bool failRebuild = true;
    VectorSyncAdapter consumer{*target, syncB};
    consumer.setRebuildCallback([&]() -> yams::Result<void> {
        if (failRebuild) {
            return yams::Error{yams::ErrorCode::InternalError, "injected rebuild failure"};
        }
        return {};
    });
    VectorRecord record;
    record.chunk_id = "retry-chunk";
    record.document_hash = "doc";
    record.model_id = "model";
    record.embedding = {1.0F, 0.0F, 0.0F, 0.0F};
    record.embedding_dim = 4;
    REQUIRE(publisher.publish(record).has_value());
    const auto failed = consumer.apply();
    REQUIRE_FALSE(failed.has_value());
    failRebuild = false;
    const auto retried = consumer.apply();
    REQUIRE(retried.has_value());
    CHECK(retried.value() == 0);
}

TEST_CASE("vector sync adapter rejects publishing without identity",
          "[vector][memory-sync][exact-scan]") {
    const auto skip = skipReasonIfAny();
    if (!skip.empty()) {
        SKIP(skip);
    }

    TempDirGuard temp;
    auto db = makeVectorDb(4);
    MemorySyncService sync{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    VectorSyncAdapter adapter{*db, sync};

    VectorRecord record;
    record.chunk_id = "chunk-no-model";
    record.embedding = {1.0F, 0.0F, 0.0F, 0.0F};

    const auto published = adapter.publish(record);
    REQUIRE_FALSE(published.has_value());
    CHECK(published.error().code == yams::ErrorCode::InvalidArgument);
}
