// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <yams/memory_sync/memory_sync_service.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_sync_adapter.h>
#include <yams/metadata/path_utils.h>
#include <yams/storage/storage_backend.h>

#include "../../common/metadata_test_db.h"

using namespace yams::metadata;
using namespace yams::memory_sync;

namespace {

constexpr std::string_view kDocHash =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
constexpr std::string_view kOtherDocHash =
    "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";

std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> result(text.size());
    std::memcpy(result.data(), text.data(), text.size());
    return result;
}

std::filesystem::path tempDbPath(const char* prefix) {
    return yams::test::migrated_metadata_db_template().clone(prefix);
}

struct RepoFixture {
    explicit RepoFixture(const char* prefix) {
        dbPath_ = tempDbPath(prefix);
        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        REQUIRE(pool_->initialize().has_value());
        repository_ = std::make_unique<MetadataRepository>(
            *pool_, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);
    }
    RepoFixture(const RepoFixture&) = delete;
    RepoFixture& operator=(const RepoFixture&) = delete;
    RepoFixture(RepoFixture&&) = delete;
    RepoFixture& operator=(RepoFixture&&) = delete;

    ~RepoFixture() {
        repository_.reset();
        pool_->shutdown();
        pool_.reset();
        yams::test::remove_sqlite_artifacts(dbPath_);
    }

    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repository_;
};

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
               ("yams-metadata-sync-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path);
    }
    TempDirGuard(const TempDirGuard&) = delete;
    TempDirGuard& operator=(const TempDirGuard&) = delete;
    TempDirGuard(TempDirGuard&&) = delete;
    TempDirGuard& operator=(TempDirGuard&&) = delete;

    ~TempDirGuard() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
    std::filesystem::path path;
};

DocumentInfo makeDocument(const std::string& path, const std::string& hash) {
    DocumentInfo info;
    info.filePath = path;
    info.fileName = std::filesystem::path(path).filename().string();
    info.fileExtension = std::filesystem::path(path).extension().string();
    info.fileSize = 2048;
    info.sha256Hash = hash;
    info.mimeType = "text/markdown";
    info.createdTime = std::chrono::sys_seconds{std::chrono::seconds{1000}};
    info.modifiedTime = std::chrono::sys_seconds{std::chrono::seconds{1001}};
    info.indexedTime = std::chrono::sys_seconds{std::chrono::seconds{1002}};
    populatePathDerivedFields(info);
    info.contentExtracted = true;
    info.extractionStatus = ExtractionStatus::Success;
    info.repairStatus = RepairStatus::Completed;
    info.repairAttempts = 1;
    return info;
}

} // namespace

TEST_CASE("metadata sync payload is independent of local derivation readiness",
          "[metadata][memory-sync][freshness]") {
    TempDirGuard temp;
    RepoFixture repo("meta_sync_local_readiness_");
    MemorySyncService writer{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MetadataSyncAdapter adapter{*repo.repository_, writer};
    auto info = makeDocument("/corpus/source.md", std::string(kDocHash));
    REQUIRE(adapter.publish(info, {}).has_value());
    REQUIRE(writer.syncOnce().has_value());
    const auto completedPayload = writer.readCached("document/" + std::string(kDocHash));
    REQUIRE(completedPayload.has_value());

    info.contentExtracted = false;
    info.extractionStatus = ExtractionStatus::Failed;
    info.extractionError = "receiver-specific extractor failure";
    info.repairStatus = RepairStatus::Failed;
    info.repairAttempts = 12;
    info.repairAttemptedAt = std::chrono::sys_seconds{std::chrono::seconds{2000}};
    REQUIRE(adapter.publish(info, {}).has_value());
    REQUIRE(writer.syncOnce().has_value());
    const auto failedPayload = writer.readCached("document/" + std::string(kDocHash));
    REQUIRE(failedPayload.has_value());
    CHECK(failedPayload.value() == completedPayload.value());
}

TEST_CASE("legacy sender completion does not certify receiver indexes",
          "[metadata][memory-sync][freshness]") {
    TempDirGuard temp;
    RepoFixture repo("meta_sync_legacy_readiness_");
    MemorySyncService writer{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService reader{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    MetadataSyncAdapter adapter{*repo.repository_, reader};
    MetadataDocumentRecord record;
    record.documentId = kDocHash;
    record.contentHash = kDocHash;
    record.filePath = "/corpus/legacy.md";
    record.contentExtracted = true;
    record.extractionStatus = static_cast<int>(ExtractionStatus::Success);
    record.repairStatus = static_cast<int>(RepairStatus::Completed);
    record.repairAttempts = 8;
    record.repairAttemptedAt = 2000;
    REQUIRE(
        writer.publish("document/" + std::string(kDocHash), bytes(nlohmann::json(record).dump()))
            .has_value());
    REQUIRE(adapter.apply().has_value());
    auto imported = repo.repository_->getDocumentByHash(std::string(kDocHash));
    REQUIRE(imported.has_value());
    REQUIRE(imported.value().has_value());
    CHECK_FALSE(imported.value()->contentExtracted);
    CHECK(imported.value()->extractionStatus == ExtractionStatus::Pending);
    CHECK(imported.value()->repairStatus == RepairStatus::Pending);
    CHECK(imported.value()->repairAttempts == 0);
    CHECK(imported.value()->repairAttemptedAt.time_since_epoch().count() == 0);
}

TEST_CASE("metadata sync adapter rejects payload identity mismatches before mutation",
          "[metadata][memory-sync][identity]") {
    TempDirGuard temp;
    RepoFixture repo("meta_sync_identity_");
    MemorySyncService writer{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService reader{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    MetadataSyncAdapter adapter{*repo.repository_, reader};
    const std::string logicalKey = "document/" + std::string(kDocHash);

    SECTION("value") {
        MetadataDocumentRecord record;
        record.documentId = kOtherDocHash;
        record.contentHash = kOtherDocHash;
        record.filePath = "/corpus/mismatch.md";
        REQUIRE(writer.publish(logicalKey, bytes(nlohmann::json(record).dump())).has_value());

        const auto applied = adapter.apply();
        REQUIRE_FALSE(applied.has_value());
        CHECK(applied.error().code == yams::ErrorCode::InvalidData);
        const auto unexpected = repo.repository_->getDocumentByHash(std::string(kOtherDocHash));
        REQUIRE(unexpected.has_value());
        CHECK_FALSE(unexpected.value().has_value());
    }

    SECTION("tombstone") {
        BatchDocumentInsert item;
        item.info = makeDocument("/corpus/other.md", std::string(kOtherDocHash));
        std::vector<BatchDocumentInsert> items{item};
        REQUIRE(repo.repository_->batchInsertDocumentsWithMetadata(items).has_value());
        REQUIRE(writer.erase(logicalKey, std::string(kOtherDocHash)).has_value());

        const auto applied = adapter.apply();
        REQUIRE_FALSE(applied.has_value());
        CHECK(applied.error().code == yams::ErrorCode::InvalidData);
        const auto retained = repo.repository_->getDocumentByHash(std::string(kOtherDocHash));
        REQUIRE(retained.has_value());
        CHECK(retained.value().has_value());
    }
}

TEST_CASE("metadata sync adapter rejects non-SHA document identities before mutation",
          "[metadata][memory-sync][identity]") {
    TempDirGuard temp;
    RepoFixture repo("meta_sync_digest_identity_");
    MemorySyncService writer{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService reader{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    MetadataSyncAdapter adapter{*repo.repository_, reader};
    constexpr std::string_view kInvalidHash = "not-a-sha256-digest";
    const std::string logicalKey = "document/" + std::string(kInvalidHash);

    SECTION("value") {
        MetadataDocumentRecord record;
        record.documentId = kInvalidHash;
        record.contentHash = kInvalidHash;
        record.filePath = "/corpus/invalid-digest.md";
        REQUIRE(writer.publish(logicalKey, bytes(nlohmann::json(record).dump())).has_value());

        const auto applied = adapter.apply();
        REQUIRE_FALSE(applied.has_value());
        CHECK(applied.error().code == yams::ErrorCode::InvalidData);
        const auto unexpected = repo.repository_->getDocumentByHash(std::string(kInvalidHash));
        REQUIRE(unexpected.has_value());
        CHECK_FALSE(unexpected.value().has_value());
    }

    SECTION("tombstone") {
        BatchDocumentInsert item;
        item.info = makeDocument("/corpus/invalid-digest.md", std::string(kInvalidHash));
        std::vector<BatchDocumentInsert> items{item};
        REQUIRE(repo.repository_->batchInsertDocumentsWithMetadata(items).has_value());
        REQUIRE(writer.erase(logicalKey, std::string(kInvalidHash)).has_value());

        const auto applied = adapter.apply();
        REQUIRE_FALSE(applied.has_value());
        CHECK(applied.error().code == yams::ErrorCode::InvalidData);
        const auto retained = repo.repository_->getDocumentByHash(std::string(kInvalidHash));
        REQUIRE(retained.has_value());
        CHECK(retained.value().has_value());
    }
}

TEST_CASE("metadata sync adapter converges a committed document from A to B",
          "[metadata][memory-sync]") {
    TempDirGuard temp;
    RepoFixture repoA("meta_sync_a_");
    RepoFixture repoB("meta_sync_b_");
    MemorySyncService syncA{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService syncB{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    MetadataSyncAdapter adapterA{*repoA.repository_, syncA};
    std::vector<std::pair<std::string, std::string>> postIngest;
    MetadataSyncAdapter adapterB{*repoB.repository_, syncB, {}, [&](const DocumentInfo& applied) {
                                     postIngest.emplace_back(applied.sha256Hash, applied.mimeType);
                                 }};

    const auto info = makeDocument("/corpus/note.md", std::string(kDocHash));
    const std::vector<std::pair<std::string, MetadataValue>> tags = {
        {"stage", MetadataValue("alpha")},
        {"priority", MetadataValue(static_cast<std::int64_t>(7))},
    };

    // Commit on A through the real insert seam, then mirror through the adapter.
    std::vector<BatchDocumentInsert> inserts;
    BatchDocumentInsert item;
    item.info = info;
    item.tags = tags;
    inserts.push_back(item);
    auto inserted = repoA.repository_->batchInsertDocumentsWithMetadata(inserts);
    REQUIRE(inserted.has_value());

    REQUIRE(adapterA.publish(info, tags).has_value());

    // B does not have the document before apply.
    auto before = repoB.repository_->getDocumentByHash(info.sha256Hash);
    REQUIRE(before.has_value());
    CHECK_FALSE(before.value().has_value());

    // apply() reconciles the sync backend and writes the winning record.
    auto applied = adapterB.apply();
    REQUIRE(applied.has_value());
    CHECK(applied.value() == 1);
    REQUIRE(postIngest.size() == 1);
    CHECK(postIngest.front().first == info.sha256Hash);
    CHECK(postIngest.front().second == info.mimeType);

    auto after = repoB.repository_->getDocumentByHash(info.sha256Hash);
    REQUIRE(after.has_value());
    REQUIRE(after.value().has_value());
    CHECK(after.value()->id > 0);
    CHECK(after.value()->filePath == info.filePath);
    CHECK(after.value()->mimeType == "text/markdown");
    CHECK_FALSE(after.value()->contentExtracted);
    CHECK(after.value()->extractionStatus == ExtractionStatus::Pending);
    CHECK(after.value()->repairStatus == RepairStatus::Pending);
    CHECK(after.value()->repairAttempts == 0);

    // Local derivation completion is not portable metadata. A remote winner must
    // neither certify absent indexes nor overwrite successful local work.
    auto locallyDerived = *after.value();
    locallyDerived.contentExtracted = true;
    locallyDerived.extractionStatus = ExtractionStatus::Success;
    locallyDerived.repairStatus = RepairStatus::Completed;
    locallyDerived.repairAttempts = 2;
    REQUIRE(repoB.repository_->updateDocument(locallyDerived).has_value());

    auto stage = repoB.repository_->getMetadata(after.value()->id, "stage");
    REQUIRE(stage.has_value());
    REQUIRE(stage.value().has_value());
    CHECK(stage.value()->value == "alpha");

    auto priority = repoB.repository_->getMetadata(after.value()->id, "priority");
    REQUIRE(priority.has_value());
    REQUIRE(priority.value().has_value());
    CHECK(priority.value()->asInteger() == 7);

    // A later winner must replace mutable document fields and the complete metadata map.
    auto updatedInfo = info;
    updatedInfo.filePath = "/moved/final.md";
    updatedInfo.fileName = "final.md";
    updatedInfo.extractionStatus = ExtractionStatus::Failed;
    updatedInfo.extractionError = "remote failure";
    populatePathDerivedFields(updatedInfo);
    const std::vector<std::pair<std::string, MetadataValue>> updatedTags = {
        {"stage", MetadataValue("final")},
    };
    REQUIRE(adapterA.publish(updatedInfo, updatedTags).has_value());
    const auto updated = adapterB.apply();
    REQUIRE(updated.has_value());
    CHECK(updated.value() == 1);
    CHECK(postIngest.size() == 1);

    auto afterAgain = repoB.repository_->getDocumentByHash(info.sha256Hash);
    REQUIRE(afterAgain.has_value());
    REQUIRE(afterAgain.value().has_value());
    CHECK(afterAgain.value()->id == after.value()->id);
    CHECK(afterAgain.value()->filePath == updatedInfo.filePath);
    CHECK(afterAgain.value()->contentExtracted);
    CHECK(afterAgain.value()->extractionStatus == ExtractionStatus::Success);
    CHECK(afterAgain.value()->extractionError.empty());
    CHECK(afterAgain.value()->repairStatus == RepairStatus::Completed);
    CHECK(afterAgain.value()->repairAttempts == 2);
    const auto finalStage = repoB.repository_->getMetadata(afterAgain.value()->id, "stage");
    REQUIRE(finalStage.has_value());
    REQUIRE(finalStage.value().has_value());
    CHECK(finalStage.value()->value == "final");
    const auto removedPriority = repoB.repository_->getMetadata(afterAgain.value()->id, "priority");
    REQUIRE(removedPriority.has_value());
    CHECK_FALSE(removedPriority.value().has_value());

    // Reapplying the unchanged winner performs no writes.
    const auto unchanged = adapterB.apply();
    REQUIRE(unchanged.has_value());
    CHECK(unchanged.value() == 0);
    CHECK(postIngest.size() == 1);

    REQUIRE(adapterA.publishDelete(info.sha256Hash).has_value());
    auto deleted = adapterB.apply();
    REQUIRE(deleted.has_value());
    CHECK(deleted.value() == 1);
    const auto afterDelete = repoB.repository_->getDocumentByHash(info.sha256Hash);
    REQUIRE(afterDelete.has_value());
    CHECK_FALSE(afterDelete.value().has_value());

    // A fresh adapter instance (restart/resume) observes the retained tombstone.
    MemorySyncService restartedSync{makeBackend(temp.path / "sync"),
                                    MemorySyncConfig{"restarted", 50}};
    MetadataSyncAdapter restarted{*repoB.repository_, restartedSync};
    REQUIRE(restarted.apply().has_value());
    const auto afterRestart = repoB.repository_->getDocumentByHash(info.sha256Hash);
    REQUIRE(afterRestart.has_value());
    CHECK_FALSE(afterRestart.value().has_value());
}

TEST_CASE("local committed tombstone preserves and republishes a later local re-add",
          "[metadata][memory-sync][tombstone][ordering]") {
    TempDirGuard temp;
    RepoFixture repo("meta_sync_local_readd_");
    MemorySyncService sync{makeBackend(temp.path / "sync"), MemorySyncConfig{"local", 50}};
    MetadataSyncAdapter adapter{*repo.repository_, sync};
    const auto info = makeDocument("/corpus/readded.md", std::string(kDocHash));
    const std::vector<std::pair<std::string, MetadataValue>> tags = {
        {"state", MetadataValue("readded")},
    };
    BatchDocumentInsert item;
    item.info = info;
    item.tags = tags;
    std::vector<BatchDocumentInsert> inserts{item};
    REQUIRE(repo.repository_->batchInsertDocumentsWithMetadata(inserts).has_value());
    REQUIRE(adapter.publish(info, tags).has_value());
    REQUIRE(adapter.publishDelete(info.sha256Hash).has_value());

    auto applied = adapter.apply();
    REQUIRE(applied.has_value());
    CHECK(applied.value() == 0);
    auto retained = repo.repository_->getDocumentByHash(info.sha256Hash);
    REQUIRE(retained.has_value());
    REQUIRE(retained.value().has_value());
    auto merged = sync.syncOnce();
    REQUIRE(merged.has_value());
    REQUIRE(merged.value().contains("document/" + info.sha256Hash));
    CHECK_FALSE(merged.value().at("document/" + info.sha256Hash).isTombstone());
    CHECK(sync.replicationState().commitments.at("local").counter == 3);
}

TEST_CASE("metadata sync replacement rolls back document and tags together",
          "[metadata][memory-sync][transaction]") {
    TempDirGuard temp;
    RepoFixture repoA("meta_sync_atomic_a_");
    RepoFixture repoB("meta_sync_atomic_b_");
    MemorySyncService syncA{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MemorySyncService syncB{makeBackend(temp.path / "sync"), MemorySyncConfig{"B", 50}};
    MetadataSyncAdapter adapterA{*repoA.repository_, syncA};
    MetadataSyncAdapter adapterB{*repoB.repository_, syncB};

    const auto original = makeDocument("/corpus/original.md", std::string(kDocHash));
    const std::vector<std::pair<std::string, MetadataValue>> originalTags = {
        {"stage", MetadataValue("original")},
    };
    REQUIRE(adapterA.publish(original, originalTags).has_value());
    REQUIRE(adapterB.apply().has_value());

    auto replacement = original;
    replacement.filePath = "/corpus/replacement.md";
    populatePathDerivedFields(replacement);
    const std::vector<std::pair<std::string, MetadataValue>> replacementTags = {
        {"explode", MetadataValue("reject")},
    };
    REQUIRE(adapterA.publish(replacement, replacementTags).has_value());

    {
        auto connection = repoB.pool_->acquire();
        REQUIRE(connection.has_value());
        REQUIRE((*connection.value())
                    ->execute(R"(
                        CREATE TRIGGER reject_sync_metadata
                        BEFORE INSERT ON metadata
                        WHEN NEW.key = 'explode'
                        BEGIN
                            SELECT RAISE(ABORT, 'injected sync metadata failure');
                        END
                    )")
                    .has_value());
    }

    const auto failed = adapterB.apply();
    REQUIRE_FALSE(failed.has_value());
    const auto afterFailure = repoB.repository_->getDocumentByHash(original.sha256Hash);
    REQUIRE(afterFailure.has_value());
    REQUIRE(afterFailure.value().has_value());
    CHECK(afterFailure.value()->filePath == original.filePath);
    const auto originalStage = repoB.repository_->getMetadata(afterFailure.value()->id, "stage");
    REQUIRE(originalStage.has_value());
    REQUIRE(originalStage.value().has_value());
    CHECK(originalStage.value()->value == "original");
    const auto rejectedTag = repoB.repository_->getMetadata(afterFailure.value()->id, "explode");
    REQUIRE(rejectedTag.has_value());
    CHECK_FALSE(rejectedTag.value().has_value());

    {
        auto connection = repoB.pool_->acquire();
        REQUIRE(connection.has_value());
        REQUIRE((*connection.value())->execute("DROP TRIGGER reject_sync_metadata").has_value());
    }
    REQUIRE(adapterB.apply().has_value());
    const auto afterRetry = repoB.repository_->getDocumentByHash(original.sha256Hash);
    REQUIRE(afterRetry.has_value());
    REQUIRE(afterRetry.value().has_value());
    CHECK(afterRetry.value()->filePath == replacement.filePath);
}

TEST_CASE("metadata sync adapter rejects publishing without a content-hash identity",
          "[metadata][memory-sync]") {
    TempDirGuard temp;
    RepoFixture repo("meta_sync_identity_");
    MemorySyncService sync{makeBackend(temp.path / "sync"), MemorySyncConfig{"A", 50}};
    MetadataSyncAdapter adapter{*repo.repository_, sync};

    const auto info = makeDocument("/corpus/unnamed.md", "");
    const auto published = adapter.publish(info, {});
    REQUIRE_FALSE(published.has_value());
    CHECK(published.error().code == yams::ErrorCode::InvalidArgument);
}
