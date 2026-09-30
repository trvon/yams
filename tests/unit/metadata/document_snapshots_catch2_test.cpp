// Document snapshot membership: which snapshots a document was stored in.
//
// Membership used to be two metadata rows per (document, snapshot): snapshot_id:<id> and
// snapshot_time:<id>. They were never pruned; on one 93 GB database they were 43.1M of the
// 44.0M metadata rows, and `yams list` hydrated every one of them.

#include <catch2/catch_test_macros.hpp>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "../../common/metadata_test_db.h"
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>

using namespace yams;
using namespace yams::metadata;

namespace {

struct SnapshotFixture {
    SnapshotFixture() {
        spdlog::set_level(spdlog::level::warn);
        dbPath = yams::test::migrated_metadata_db_template().clone("document_snapshots_");
        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;
        pool = std::make_unique<ConnectionPool>(dbPath.string(), config);
        REQUIRE(pool->initialize().has_value());
        repo = std::make_unique<MetadataRepository>(
            *pool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);
    }
    ~SnapshotFixture() {
        repo.reset();
        pool->shutdown();
        pool.reset();
        yams::test::remove_sqlite_artifacts(dbPath);
    }

    DocumentInfo document(const std::string& path, const std::string& hash) const {
        DocumentInfo info;
        info.filePath = path;
        info.fileName = std::filesystem::path(path).filename().string();
        info.fileExtension = std::filesystem::path(path).extension().string();
        info.fileSize = 42;
        info.sha256Hash = hash;
        info.mimeType = "text/plain";
        info.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        info.modifiedTime = info.createdTime;
        info.indexedTime = info.createdTime;
        return info;
    }

    // Mirrors DocumentService::store: latest snapshot_id/snapshot_time tags plus a snapshot record.
    int64_t store(const DocumentInfo& info, const std::string& snapshotId, int64_t timeMicros) {
        std::vector<std::pair<std::string, MetadataValue>> tags{
            {"snapshot_id", MetadataValue(snapshotId)},
            {"snapshot_time", MetadataValue(std::to_string(timeMicros))},
        };
        TreeSnapshotRecord snapshot;
        snapshot.snapshotId = snapshotId;
        snapshot.createdTime = timeMicros / 1'000'000;
        snapshot.fileCount = 1;
        snapshot.metadata["directory_path"] = info.filePath;
        auto id = repo->insertDocumentWithMetadata(info, tags, &snapshot, false, true);
        REQUIRE(id.has_value());
        return id.value();
    }

    static bool contains(const std::vector<DocumentInfo>& docs, int64_t id) {
        return std::any_of(docs.begin(), docs.end(),
                           [&](const DocumentInfo& d) { return d.id == id; });
    }

    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::unique_ptr<MetadataRepository> repo;
};

std::vector<std::string> snapshotIds(const std::vector<DocumentSnapshotEntry>& entries) {
    std::vector<std::string> ids;
    for (const auto& e : entries) {
        ids.push_back(e.snapshotId);
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

} // namespace

TEST_CASE("A re-stored document stays a member of every snapshot it was stored in",
          "[metadata][snapshots]") {
    SnapshotFixture f;
    const auto info = f.document("/repo/src/main.cpp", std::string(64, 'a'));
    const auto first = f.store(info, "S1", 1'000'000);
    const auto second = f.store(info, "S2", 2'000'000);
    REQUIRE((first == second)); // same content, same document

    // Before membership, S1 lost the document as soon as snapshot_id was overwritten by S2.
    auto inS1 = f.repo->findDocumentsBySnapshot("S1");
    REQUIRE(inS1.has_value());
    CHECK(SnapshotFixture::contains(inS1.value(), first));
    auto inS2 = f.repo->findDocumentsBySnapshot("S2");
    REQUIRE(inS2.has_value());
    CHECK(SnapshotFixture::contains(inS2.value(), first));

    auto history = f.repo->getDocumentSnapshots(first);
    REQUIRE(history.has_value());
    CHECK((snapshotIds(history.value()) == std::vector<std::string>{"S1", "S2"}));
    for (const auto& entry : history.value()) {
        CHECK((entry.snapshotTimeMicros == (entry.snapshotId == "S1" ? 1'000'000 : 2'000'000)));
    }
}

TEST_CASE("Storing into a snapshot adds no per-snapshot metadata keys", "[metadata][snapshots]") {
    SnapshotFixture f;
    const auto id = f.store(f.document("/repo/a.txt", std::string(64, 'b')), "S1", 5'000'000);
    f.store(f.document("/repo/a.txt", std::string(64, 'b')), "S2", 6'000'000);

    auto all = f.repo->getAllMetadata(id);
    REQUIRE(all.has_value());
    for (const auto& [key, value] : all.value()) {
        (void)value;
        CHECK_FALSE(key.rfind("snapshot_id:", 0) == 0);
        CHECK_FALSE(key.rfind("snapshot_time:", 0) == 0);
    }
    CHECK((all.value().at("snapshot_id").asString() == "S2"));
}

TEST_CASE("Legacy per-snapshot keys move to membership in bounded batches",
          "[metadata][snapshots]") {
    SnapshotFixture f;
    auto inserted = f.repo->insertDocument(f.document("/repo/legacy.txt", std::string(64, 'c')));
    REQUIRE(inserted.has_value());
    const auto id = inserted.value();
    for (const auto& [snap, micros] :
         std::vector<std::pair<std::string, int64_t>>{{"L1", 100}, {"L2", 200}, {"L3", 300}}) {
        REQUIRE(f.repo->setMetadata(id, "snapshot_id:" + snap, MetadataValue(snap)).has_value());
        REQUIRE(
            f.repo->setMetadata(id, "snapshot_time:" + snap, MetadataValue(std::to_string(micros)))
                .has_value());
    }

    // History is complete before, during and after the move.
    auto before = f.repo->getDocumentSnapshots(id);
    REQUIRE(before.has_value());
    CHECK((snapshotIds(before.value()) == std::vector<std::string>{"L1", "L2", "L3"}));

    // Listing never pays for legacy keys, even before they are moved.
    const std::vector<int64_t> ids{id};
    auto hydrated = f.repo->getMetadataForDocuments(ids);
    REQUIRE(hydrated.has_value());
    for (const auto& [key, value] : hydrated.value()[id]) {
        (void)value;
        CHECK_FALSE(key.rfind("snapshot_", 0) == 0);
    }

    auto moved = f.repo->migrateLegacySnapshotKeys(2);
    REQUIRE(moved.has_value());
    CHECK((moved.value() == 2));
    auto during = f.repo->getDocumentSnapshots(id);
    REQUIRE(during.has_value());
    CHECK((snapshotIds(during.value()) == std::vector<std::string>{"L1", "L2", "L3"}));

    moved = f.repo->migrateLegacySnapshotKeys(2);
    REQUIRE(moved.has_value());
    CHECK((moved.value() == 1));
    moved = f.repo->migrateLegacySnapshotKeys(2);
    REQUIRE(moved.has_value());
    CHECK((moved.value() == 0));

    auto all = f.repo->getAllMetadata(id);
    REQUIRE(all.has_value());
    for (const auto& [key, value] : all.value()) {
        (void)value;
        CHECK_FALSE(key.rfind("snapshot_id:", 0) == 0);
        CHECK_FALSE(key.rfind("snapshot_time:", 0) == 0);
    }
    auto after = f.repo->getDocumentSnapshots(id);
    REQUIRE(after.has_value());
    REQUIRE((after.value().size() == 3));
    for (const auto& entry : after.value()) {
        CHECK((entry.snapshotTimeMicros == (entry.snapshotId == "L1"   ? 100
                                            : entry.snapshotId == "L2" ? 200
                                                                       : 300)));
    }
    auto inL2 = f.repo->findDocumentsBySnapshot("L2");
    REQUIRE(inL2.has_value());
    CHECK(SnapshotFixture::contains(inL2.value(), id));
}

TEST_CASE("Snapshot summaries count every member, not only latest snapshot_id holders",
          "[metadata][snapshots]") {
    SnapshotFixture f;
    const auto shared =
        f.store(f.document("/repo/shared.txt", std::string(64, 'd')), "S1", 1'000'000);
    f.store(f.document("/repo/only-s1.txt", std::string(64, 'e')), "S1", 1'000'000);
    f.store(f.document("/repo/shared.txt", std::string(64, 'd')), "S2", 2'000'000); // re-store

    auto snapshots = f.repo->getSnapshots();
    REQUIRE(snapshots.has_value());
    CHECK(std::find(snapshots.value().begin(), snapshots.value().end(), "S1") !=
          snapshots.value().end());
    CHECK(std::find(snapshots.value().begin(), snapshots.value().end(), "S2") !=
          snapshots.value().end());

    // Before membership, S1 lost the re-stored document and reported 1 file.
    auto s1 = f.repo->getSnapshotInfo("S1");
    REQUIRE(s1.has_value());
    CHECK((s1.value().fileCount == 2));
    auto s2 = f.repo->getSnapshotInfo("S2");
    REQUIRE(s2.has_value());
    CHECK((s2.value().fileCount == 1));

    auto batch = f.repo->batchGetSnapshotInfo({"S1", "S2", "missing"});
    REQUIRE(batch.has_value());
    CHECK((batch.value().at("S1").fileCount == 2));
    CHECK((batch.value().at("S2").fileCount == 1));
    CHECK_FALSE(batch.value().contains("missing"));
    (void)shared;
}

TEST_CASE("Snapshot summaries include legacy per-snapshot keys until they are moved",
          "[metadata][snapshots]") {
    SnapshotFixture f;
    auto inserted = f.repo->insertDocument(f.document("/repo/legacy2.txt", std::string(64, 'f')));
    REQUIRE(inserted.has_value());
    REQUIRE(
        f.repo->setMetadata(inserted.value(), "snapshot_id:L9", MetadataValue(std::string("L9")))
            .has_value());
    auto info = f.repo->getSnapshotInfo("L9");
    REQUIRE(info.has_value());
    CHECK((info.value().fileCount == 1));
}
