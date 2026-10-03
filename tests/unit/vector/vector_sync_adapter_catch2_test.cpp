// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <set>
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

TEST_CASE("vector sync adapter suite supports disabled-vector CI lanes",
          "[vector][memory-sync][registration]") {
    SUCCEED("The binary remains valid when behavior cases are intentionally skipped");
}

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

TEST_CASE("vector sync adapter skips and reports payload identity mismatches",
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
        REQUIRE(applied.has_value());
        CHECK(applied.value() == 0);
        REQUIRE(consumer.failure().has_value());
        CHECK(consumer.failure()->code == yams::ErrorCode::InvalidData);
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
        REQUIRE(applied.has_value());
        CHECK(applied.value() == 0);
        REQUIRE(consumer.failure().has_value());
        CHECK(consumer.failure()->code == yams::ErrorCode::InvalidData);
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
        REQUIRE(applied.has_value());
        CHECK(applied.value() == 0);
        REQUIRE(consumer.failure().has_value());
        CHECK(consumer.failure()->code == yams::ErrorCode::InvalidData);
        const auto retained = target->getVector("shared-chunk");
        REQUIRE(retained.has_value());
        CHECK(retained->model_id == "retained-model");
    }
}

TEST_CASE("vector sync adapter defers a record whose content has not replicated yet",
          "[vector][memory-sync][prerequisite][defer]") {
    const auto skip = skipReasonIfAny();
    if (!skip.empty()) {
        SKIP(skip);
    }
    TempDirGuard temp;
    auto target = makeVectorDb(4);
    MemorySyncService writer{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService reader{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};

    const std::string readyHash(64, 'a');
    const std::string waitingHash(64, 'b');
    EmbeddingRecord ready;
    ready.model = "model";
    ready.chunkId = "ready-chunk";
    ready.documentId = readyHash;
    ready.dimensions = 4;
    ready.values = {1.0F, 0.0F, 0.0F, 0.0F};
    EmbeddingRecord waiting = ready;
    waiting.chunkId = "waiting-chunk";
    waiting.documentId = waitingHash;
    waiting.values = {0.0F, 1.0F, 0.0F, 0.0F};
    REQUIRE(writer.publish("embedding/model/ready-chunk", bytes(nlohmann::json(ready).dump()))
                .has_value());
    REQUIRE(writer.publish("embedding/model/waiting-chunk", bytes(nlohmann::json(waiting).dump()))
                .has_value());

    // Only the ready record's content blob has landed locally.
    std::set<std::string> localContent{readyHash};
    const auto contentExists = [&](std::string_view hash) -> yams::Result<bool> {
        return localContent.contains(std::string(hash));
    };

    VectorSyncAdapter consumer{*target, reader, {}, nullptr, contentExists};
    const auto first = consumer.apply();
    REQUIRE(first.has_value());
    CHECK(first.value() == 1);
    CHECK(target->getVector("ready-chunk").has_value());
    CHECK_FALSE(target->getVector("waiting-chunk").has_value());
    CHECK(consumer.deferredKeys() == std::vector<std::string>{"embedding/model/waiting-chunk"});

    // The deferred winner stays pending in the replicated index and applies once its
    // prerequisite arrives on a later cycle.
    localContent.insert(waitingHash);
    VectorSyncAdapter retry{*target, reader, {}, nullptr, contentExists};
    const auto second = retry.apply();
    REQUIRE(second.has_value());
    CHECK(second.value() == 1);
    CHECK(target->getVector("waiting-chunk").has_value());
    CHECK(retry.deferredKeys().empty());
}

TEST_CASE("vector sync adapter reports a content probe error as a record failure",
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
    record.chunkId = "probe-error";
    record.documentId = std::string(64, 'c');
    record.dimensions = 4;
    record.values = {1.0F, 0.0F, 0.0F, 0.0F};
    REQUIRE(writer.publish("embedding/model/probe-error", bytes(nlohmann::json(record).dump()))
                .has_value());

    VectorSyncAdapter consumer{
        *target, reader, {}, nullptr, [](std::string_view) -> yams::Result<bool> {
            return yams::Error{yams::ErrorCode::IOError, "probe failed"};
        }};
    const auto applied = consumer.apply();
    REQUIRE(applied.has_value());
    CHECK(applied.value() == 0);
    // Not a deferral: the probe did not say the content is missing, it failed.
    CHECK(consumer.deferredKeys().empty());
    REQUIRE(consumer.failure().has_value());
    CHECK(consumer.failure()->code == yams::ErrorCode::IOError);
    CHECK(consumer.failure()->message.find("embedding/model/probe-error") != std::string::npos);
    CHECK_FALSE(target->getVector("probe-error").has_value());
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
    bool failRebuild = true;
    std::size_t rebuildCalls = 0;
    auto rebuild = [&]() -> yams::Result<void> {
        ++rebuildCalls;
        if (failRebuild) {
            return yams::Error{yams::ErrorCode::InternalError, "injected rebuild failure"};
        }
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

    // The valid row lands despite the invalid one, then the index rebuild fails.
    const auto failed = consumer.apply();
    REQUIRE_FALSE(failed.has_value());
    CHECK(target->getVector("a-valid").has_value());
    CHECK(rebuildDirty);
    CHECK(rebuildCalls == 1);

    // Nothing new applies on the retry, but the dirty index from the earlier pass is rebuilt.
    failRebuild = false;
    VectorSyncAdapter retry{*target, reader, rebuild, &rebuildDirty};
    const auto recovered = retry.apply();
    REQUIRE(recovered.has_value());
    CHECK(recovered.value() == 0);
    CHECK(rebuildCalls == 2);
    CHECK_FALSE(rebuildDirty);
    // The 3-dimensional row is well formed but the 4-dimensional store rejects it, every pass.
    REQUIRE(retry.failure().has_value());
    CHECK(retry.failure()->message.starts_with("embedding/model/z-invalid: "));
}

TEST_CASE("vector sync adapter skips every winner of a chunk claimed by two models",
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
    REQUIRE(applied.has_value());
    CHECK(applied.value() == 0);
    REQUIRE(consumer.failure().has_value());
    CHECK(consumer.failure()->code == yams::ErrorCode::NotSupported);
    CHECK(target->getVectorCount() == 0);
}

TEST_CASE("vector sync adapter applies the valid winner of a batch with a malformed one",
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
    REQUIRE(applied.has_value());
    CHECK(applied.value() == 1);
    REQUIRE(consumer.failure().has_value());
    CHECK(consumer.failure()->code == yams::ErrorCode::InvalidData);
    CHECK(consumer.failure()->message.find("embedding/model/broken") != std::string::npos);
    CHECK(target->getVector("valid-chunk").has_value());
    CHECK(target->getVectorCount() == 1);
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

TEST_CASE("vector sync adapter applies good records past a bad one",
          "[vector][memory-sync][bad-record][exact-scan]") {
    const auto skip = skipReasonIfAny();
    if (!skip.empty()) {
        SKIP(skip);
    }
    TempDirGuard temp;
    auto target = makeVectorDb(4);
    MemorySyncService writer{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService reader{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    const std::string documentHash(64, 'd');
    const std::set<std::string> localContent{documentHash};
    const auto contentExists = [&](std::string_view hash) -> yams::Result<bool> {
        return localContent.contains(std::string(hash));
    };

    // Winners are scanned in key order. The bad record sorts between two good ones, so it must
    // hide neither the record before it nor the record after it.
    EmbeddingRecord good;
    good.model = "model";
    good.documentId = documentHash;
    good.dimensions = 4;
    good.chunkId = "a-good";
    good.values = {1.0F, 0.0F, 0.0F, 0.0F};
    EmbeddingRecord later = good;
    later.chunkId = "z-good";
    later.values = {0.0F, 0.0F, 0.0F, 1.0F};
    REQUIRE(
        writer.publish("embedding/model/a-good", bytes(nlohmann::json(good).dump())).has_value());
    REQUIRE(
        writer.publish("embedding/model/z-good", bytes(nlohmann::json(later).dump())).has_value());

    const std::string badKey = "embedding/model/m-bad";
    EmbeddingRecord bad = good;
    bad.chunkId = "m-bad";
    std::string badPayload;
    SECTION("corrupt payload") {
        badPayload = "{not json";
    }
    SECTION("dimension disagrees with its values") {
        bad.values = {1.0F, 0.0F, 0.0F};
        badPayload = nlohmann::json(bad).dump();
    }
    SECTION("dimension the local store cannot hold") {
        bad.dimensions = 3;
        bad.values = {1.0F, 0.0F, 0.0F};
        badPayload = nlohmann::json(bad).dump();
    }
    SECTION("document identity is not a content hash") {
        bad.documentId = "missing-doc";
        badPayload = nlohmann::json(bad).dump();
    }
    SECTION("identity disagrees with its key") {
        bad.chunkId = "other-chunk";
        badPayload = nlohmann::json(bad).dump();
    }
    REQUIRE(writer.publish(badKey, bytes(badPayload)).has_value());

    std::size_t rebuildCalls = 0;
    VectorSyncAdapter consumer{*target, reader,
                               [&]() -> yams::Result<void> {
                                   ++rebuildCalls;
                                   return {};
                               },
                               nullptr, contentExists};
    const auto applied = consumer.apply();
    REQUIRE(applied.has_value());
    CHECK(applied.value() == 2);
    CHECK(target->getVector("a-good").has_value());
    CHECK(target->getVector("z-good").has_value());
    CHECK_FALSE(target->getVector("m-bad").has_value());
    CHECK_FALSE(target->getVector("other-chunk").has_value());
    CHECK(rebuildCalls == 1);
    // A bad record is a failure, not a deferral: no replicated prerequisite will fix it.
    CHECK(consumer.deferredKeys().empty());
    REQUIRE(consumer.failure().has_value());
    CHECK(consumer.failure()->message.starts_with(badKey + ": "));

    // The bad record stays a winner and is retried; the good ones are not applied twice.
    VectorSyncAdapter retry{*target, reader, {}, nullptr, contentExists};
    const auto again = retry.apply();
    REQUIRE(again.has_value());
    CHECK(again.value() == 0);
    REQUIRE(retry.failure().has_value());
    CHECK(retry.failure()->message.starts_with(badKey + ": "));
}

TEST_CASE("vector sync adapter replaces a chunk's model when its old model is tombstoned",
          "[vector][memory-sync][exact-scan]") {
    const auto skip = skipReasonIfAny();
    if (!skip.empty()) {
        SKIP(skip);
    }
    TempDirGuard temp;
    auto target = makeVectorDb(4);
    MemorySyncService writer{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService reader{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};

    VectorRecord local;
    local.chunk_id = "chunk";
    local.document_hash = "doc";
    local.model_id = "old-model";
    local.embedding = {1.0F, 0.0F, 0.0F, 0.0F};
    local.embedding_dim = 4;
    REQUIRE(target->insertVectorChecked(local).has_value());

    EmbeddingRecord replacement;
    replacement.model = "new-model";
    replacement.chunkId = "chunk";
    replacement.documentId = "doc";
    replacement.dimensions = 4;
    replacement.values = {0.0F, 1.0F, 0.0F, 0.0F};
    REQUIRE(writer.erase("embedding/old-model/chunk", "chunk").has_value());
    REQUIRE(writer.publish("embedding/new-model/chunk", bytes(nlohmann::json(replacement).dump()))
                .has_value());

    VectorSyncAdapter consumer{*target, reader};
    const auto applied = consumer.apply();
    REQUIRE(applied.has_value());
    CHECK(applied.value() == 2);
    CHECK_FALSE(consumer.failure().has_value());
    const auto current = target->getVector("chunk");
    REQUIRE(current.has_value());
    CHECK(current->model_id == "new-model");
}
