// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// repairMissingPathTree: batched, resumable path-tree repair over MetadataRepository.

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_tree_repair.h>
#include <yams/metadata/path_utils.h>

#include "../../common/metadata_test_db.h"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

using namespace yams::metadata;

namespace {

struct PathTreeRepairFixture {
    PathTreeRepairFixture() {
        dbPath = yams::test::migrated_metadata_db_template().clone("yams_path_tree_repair_db_");
        ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        pool = std::make_unique<ConnectionPool>(dbPath.string(), poolConfig);
        REQUIRE(pool->initialize().has_value());
        repo = std::make_unique<MetadataRepository>(
            *pool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);
    }

    ~PathTreeRepairFixture() {
        repo.reset();
        if (pool)
            pool->shutdown();
        pool.reset();
        yams::test::remove_sqlite_artifacts(dbPath);
    }

    // insertDocument does not touch the path tree, so a plain insert models a document whose
    // path-tree upsert was lost.
    DocumentInfo insertWithoutTree(const std::string& path) {
        DocumentInfo info;
        info.filePath = path;
        info.fileName = std::filesystem::path(path).filename().string();
        info.fileExtension = std::filesystem::path(path).extension().string();
        info.fileSize = 16;
        info.sha256Hash = "hash-" + std::to_string(nextHash++);
        info.mimeType = "text/plain";
        info.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        info.modifiedTime = info.createdTime;
        info.indexedTime = info.createdTime;
        populatePathDerivedFields(info);
        auto id = repo->insertDocument(info);
        REQUIRE(id.has_value());
        info.id = id.value();
        return info;
    }

    DocumentInfo insertWithTree(const std::string& path) {
        auto info = insertWithoutTree(path);
        REQUIRE(repo->upsertPathTreeForDocument(info, info.id, true, {}).has_value());
        return info;
    }

    bool hasNode(const std::string& fullPath) {
        auto node = repo->findPathTreeNodeByFullPath(fullPath);
        REQUIRE(node.has_value());
        return node.value().has_value();
    }

    std::int64_t docCount(const std::string& fullPath) {
        auto node = repo->findPathTreeNodeByFullPath(fullPath);
        REQUIRE(node.has_value());
        REQUIRE(node.value().has_value());
        return node.value()->docCount;
    }

    std::uint64_t missing() {
        auto count = repo->countDocsMissingPathTree();
        REQUIRE(count.has_value());
        return count.value();
    }

    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::unique_ptr<MetadataRepository> repo;
    int nextHash = 0;
};

} // namespace

TEST_CASE("repairMissingPathTree creates nodes for documents missing from the tree",
          "[unit][metadata][path-tree][repair]") {
    PathTreeRepairFixture fix;
    fix.insertWithoutTree("/repair/a.txt");
    fix.insertWithoutTree("/repair/sub/b.txt");
    REQUIRE(fix.missing() == 2);

    auto result = repairMissingPathTree(*fix.repo);
    REQUIRE_FALSE(result.error.has_value());
    const auto& stats = result;
    CHECK(stats.finished);
    CHECK(stats.documentsScanned == 2);
    CHECK(stats.documentsMissing == 2);
    CHECK(stats.nodesCreated == 2);
    CHECK(stats.errors == 0);

    CHECK(fix.missing() == 0);
    CHECK(fix.hasNode("/repair/a.txt"));
    CHECK(fix.hasNode("/repair/sub/b.txt"));
    CHECK(fix.docCount("/repair") == 2);

    // A second pass finds nothing to do and changes nothing.
    auto again = repairMissingPathTree(*fix.repo);
    REQUIRE_FALSE(again.error.has_value());
    CHECK(again.finished);
    CHECK(again.documentsMissing == 0);
    CHECK(again.nodesCreated == 0);
    CHECK(fix.docCount("/repair") == 2);
}

TEST_CASE("repairMissingPathTree leaves documents that already have nodes untouched",
          "[unit][metadata][path-tree][repair]") {
    PathTreeRepairFixture fix;
    fix.insertWithTree("/kept/tree/a.txt");
    fix.insertWithTree("/kept/tree/b.txt");
    fix.insertWithoutTree("/kept/lost.txt");
    REQUIRE(fix.docCount("/kept/tree") == 2);
    REQUIRE(fix.docCount("/kept/tree/a.txt") == 1);

    auto result = repairMissingPathTree(*fix.repo);
    REQUIRE_FALSE(result.error.has_value());
    CHECK(result.documentsScanned == 3);
    CHECK(result.documentsMissing == 1);
    CHECK(result.nodesCreated == 1);

    // Only the lost document was upserted; the indexed ones kept their nodes and counts.
    CHECK(fix.docCount("/kept/tree") == 2);
    CHECK(fix.docCount("/kept/tree/a.txt") == 1);
    CHECK(fix.docCount("/kept/tree/b.txt") == 1);
    CHECK(fix.docCount("/kept") == 3);
    CHECK(fix.hasNode("/kept/lost.txt"));
}

TEST_CASE("repairMissingPathTree respects the batch size and resumes from its cursor",
          "[unit][metadata][path-tree][repair]") {
    PathTreeRepairFixture fix;
    std::vector<DocumentInfo> docs;
    for (int i = 0; i < 5; ++i) {
        docs.push_back(fix.insertWithoutTree("/batch/doc-" + std::to_string(i) + ".txt"));
    }

    PathTreeRepairOptions options;
    options.batchSize = 2;
    options.maxBatches = 1;

    auto first = repairMissingPathTree(*fix.repo, options);
    REQUIRE_FALSE(first.error.has_value());
    CHECK_FALSE(first.finished);
    CHECK(first.batches == 1);
    CHECK(first.documentsScanned == 2);
    CHECK(first.nodesCreated == 2);
    CHECK(first.lastDocumentId == docs[1].id);
    CHECK(fix.hasNode("/batch/doc-0.txt"));
    CHECK(fix.hasNode("/batch/doc-1.txt"));
    CHECK_FALSE(fix.hasNode("/batch/doc-2.txt"));
    CHECK(fix.missing() == 3);

    options.startAfterId = first.lastDocumentId;
    auto second = repairMissingPathTree(*fix.repo, options);
    REQUIRE_FALSE(second.error.has_value());
    CHECK_FALSE(second.finished);
    CHECK(second.documentsScanned == 2);
    CHECK(second.lastDocumentId == docs[3].id);
    CHECK(fix.hasNode("/batch/doc-3.txt"));

    options.startAfterId = second.lastDocumentId;
    auto third = repairMissingPathTree(*fix.repo, options);
    REQUIRE_FALSE(third.error.has_value());
    CHECK(third.finished); // short window: the table ended inside it
    CHECK(third.documentsScanned == 1);
    CHECK(third.lastDocumentId == docs[4].id);
    CHECK(fix.missing() == 0);

    // Resuming past the end examines nothing and reports the pass finished.
    options.startAfterId = third.lastDocumentId;
    auto past = repairMissingPathTree(*fix.repo, options);
    REQUIRE_FALSE(past.error.has_value());
    CHECK(past.finished);
    CHECK(past.documentsScanned == 0);
    CHECK(past.lastDocumentId == docs[4].id);

    SECTION("progress is reported once per batch") {
        for (int i = 5; i < 9; ++i) {
            fix.insertWithoutTree("/batch/doc-" + std::to_string(i) + ".txt");
        }
        PathTreeRepairOptions all;
        all.batchSize = 2;
        all.startAfterId = docs[4].id;
        std::vector<std::uint64_t> created;
        auto run = repairMissingPathTree(*fix.repo, all, [&](const PathTreeRepairStats& s) {
            created.push_back(s.nodesCreated);
        });
        REQUIRE_FALSE(run.error.has_value());
        CHECK(run.finished);
        // Two full windows, then an empty one that ends the scan without reporting.
        CHECK(created == std::vector<std::uint64_t>{2, 4});
    }
}

TEST_CASE("repairMissingPathTree stops between batches when told to yield",
          "[unit][metadata][path-tree][repair]") {
    PathTreeRepairFixture fix;
    for (int i = 0; i < 6; ++i) {
        fix.insertWithoutTree("/yield/doc-" + std::to_string(i) + ".txt");
    }

    int checks = 0;
    PathTreeRepairOptions options;
    options.batchSize = 2;
    options.shouldContinue = [&checks] { return ++checks <= 2; };

    auto result = repairMissingPathTree(*fix.repo, options);
    REQUIRE_FALSE(result.error.has_value());
    CHECK_FALSE(result.finished);
    CHECK(result.batches == 2);
    CHECK(result.nodesCreated == 4);
    CHECK(checks == 3); // checked before each batch, including the one it refused
    CHECK(fix.missing() == 2);

    SECTION("a refusal before the first batch does no work") {
        PathTreeRepairOptions refused;
        refused.shouldContinue = [] { return false; };
        auto none = repairMissingPathTree(*fix.repo, refused);
        REQUIRE_FALSE(none.error.has_value());
        CHECK_FALSE(none.finished);
        CHECK(none.batches == 0);
        CHECK(fix.missing() == 2);
    }
    SECTION("resuming from the cursor finishes the pass") {
        PathTreeRepairOptions resume;
        resume.batchSize = 2;
        resume.startAfterId = result.lastDocumentId;
        auto rest = repairMissingPathTree(*fix.repo, resume);
        REQUIRE_FALSE(rest.error.has_value());
        CHECK(rest.finished);
        CHECK(rest.nodesCreated == 2);
        CHECK(fix.missing() == 0);
    }
}

TEST_CASE("repairMissingPathTree rejects a zero batch size",
          "[unit][metadata][path-tree][repair]") {
    PathTreeRepairFixture fix;
    PathTreeRepairOptions options;
    options.batchSize = 0;
    const auto stats = repairMissingPathTree(*fix.repo, options);
    REQUIRE(stats.error.has_value());
    CHECK(stats.error->code == yams::ErrorCode::InvalidArgument);
    CHECK(stats.batches == 0);
}

TEST_CASE("scanPathTreeWindow pages by a document window, not by misses",
          "[unit][metadata][path-tree][repair]") {
    PathTreeRepairFixture fix;
    fix.insertWithTree("/scan/a.txt");
    auto b = fix.insertWithoutTree("/scan/b.txt");
    auto c = fix.insertWithTree("/scan/c.txt");
    auto d = fix.insertWithoutTree("/scan/d.txt");

    auto first = fix.repo->scanPathTreeWindow(0, 3);
    REQUIRE(first.has_value());
    CHECK(first.value().scanned == 3);
    CHECK(first.value().lastScannedId == c.id);
    CHECK(first.value().missingIds == std::vector<std::int64_t>{b.id});

    auto second = fix.repo->scanPathTreeWindow(c.id, 3);
    REQUIRE(second.has_value());
    CHECK(second.value().scanned == 1);
    CHECK(second.value().lastScannedId == d.id);
    CHECK(second.value().missingIds == std::vector<std::int64_t>{d.id});

    auto end = fix.repo->scanPathTreeWindow(d.id, 3);
    REQUIRE(end.has_value());
    CHECK(end.value().scanned == 0);
    CHECK(end.value().lastScannedId == d.id);
    CHECK(end.value().missingIds.empty());

    // Read-only: scanning repaired nothing.
    CHECK(fix.missing() == 2);
    CHECK_FALSE(fix.repo->scanPathTreeWindow(0, 0).has_value());
    CHECK_FALSE(fix.repo->repairPathTreeWindow(0, 0).has_value());
}

TEST_CASE("repairMissingPathTree matches the normalized leaf the upsert writes",
          "[unit][metadata][path-tree][repair]") {
    PathTreeRepairFixture fix;
    auto doc = fix.insertWithoutTree("/notes//a.md/");
    REQUIRE(fix.missing() == 1);

    auto first = repairMissingPathTree(*fix.repo);
    REQUIRE_FALSE(first.error.has_value());
    CHECK(first.documentsMissing == 1);
    CHECK(first.nodesCreated == 1);
    CHECK(fix.hasNode("/notes/a.md"));
    CHECK(fix.docCount("/notes/a.md") == 1);

    // The raw file_path never matches a node, but the document is linked to its leaf.
    CHECK(fix.missing() == 0);
    auto second = repairMissingPathTree(*fix.repo);
    REQUIRE_FALSE(second.error.has_value());
    CHECK(second.documentsMissing == 0);
    CHECK(second.nodesCreated == 0);
    CHECK(fix.docCount("/notes/a.md") == 1);
    (void)doc;
}

TEST_CASE("repairMissingPathTree links a document whose path already has a node",
          "[unit][metadata][path-tree][repair]") {
    PathTreeRepairFixture fix;
    // An older version of the file owns the node; the re-ingested document's upsert was lost.
    fix.insertWithTree("/reingest/file.txt");
    auto current = fix.insertWithoutTree("/reingest/staging.txt");
    // Point the new row at the same path directly, independent of insert-time dedupe rules.
    auto moved = fix.pool->withConnection([&](Database& db) -> yams::Result<void> {
        return db.execute("UPDATE documents SET file_path = '/reingest/file.txt' WHERE id = " +
                          std::to_string(current.id));
    });
    REQUIRE(moved.has_value());
    REQUIRE(fix.hasNode("/reingest/file.txt"));
    REQUIRE(fix.docCount("/reingest/file.txt") == 1);
    REQUIRE(fix.missing() == 1);

    auto window = fix.repo->scanPathTreeWindow(0, 10);
    REQUIRE(window.has_value());
    CHECK(window.value().missingIds == std::vector<std::int64_t>{current.id});

    auto stats = repairMissingPathTree(*fix.repo);
    REQUIRE_FALSE(stats.error.has_value());
    CHECK(stats.documentsMissing == 1);
    CHECK(stats.nodesCreated == 1);
    CHECK(fix.docCount("/reingest/file.txt") == 2);
    CHECK(fix.missing() == 0);
}

TEST_CASE("repairMissingPathTree reports committed batches when a later batch fails",
          "[unit][metadata][path-tree][repair]") {
    PathTreeRepairFixture fix;
    for (int i = 0; i < 4; ++i) {
        fix.insertWithoutTree("/partial/doc-" + std::to_string(i) + ".txt");
    }

    PathTreeRepairOptions options;
    options.batchSize = 2;
    int checks = 0;
    // Break the schema after the first batch commits, so the second batch cannot run.
    options.shouldContinue = [&] {
        if (++checks == 2) {
            auto dropped = fix.pool->withConnection([](Database& db) -> yams::Result<void> {
                return db.execute("DROP TABLE path_tree_node_documents");
            });
            REQUIRE(dropped.has_value());
        }
        return true;
    };

    const auto stats = repairMissingPathTree(*fix.repo, options);
    REQUIRE(stats.error.has_value());
    CHECK_FALSE(stats.finished);
    CHECK(stats.batches == 1);
    CHECK(stats.documentsScanned == 2);
    CHECK(stats.nodesCreated == 2);
    CHECK(fix.hasNode("/partial/doc-1.txt"));
    CHECK_FALSE(fix.hasNode("/partial/doc-2.txt"));
}

TEST_CASE("repairMissingPathTree commits the documents done when aborted mid-batch",
          "[unit][metadata][path-tree][repair]") {
    PathTreeRepairFixture fix;
    std::vector<DocumentInfo> docs;
    docs.push_back(fix.insertWithTree("/abort/kept.txt"));
    for (int i = 0; i < 5; ++i) {
        docs.push_back(fix.insertWithoutTree("/abort/doc-" + std::to_string(i) + ".txt"));
    }

    int upserts = 0;
    PathTreeRepairOptions options;
    options.batchSize = 100; // one window holds every document
    options.shouldAbort = [&upserts] { return upserts++ >= 2; };

    const auto stopped = repairMissingPathTree(*fix.repo, options);
    REQUIRE_FALSE(stopped.error.has_value());
    CHECK_FALSE(stopped.finished);
    CHECK(stopped.batches == 1);
    CHECK(stopped.nodesCreated == 2);
    CHECK(stopped.documentsMissing == 2);
    CHECK(stopped.documentsScanned == 3); // the linked document and the two repaired ones
    CHECK(stopped.lastDocumentId == docs[3].id - 1);
    CHECK(fix.hasNode("/abort/doc-1.txt"));
    CHECK_FALSE(fix.hasNode("/abort/doc-2.txt"));
    CHECK(fix.missing() == 3);

    PathTreeRepairOptions resume;
    resume.batchSize = 100;
    resume.startAfterId = stopped.lastDocumentId;
    const auto rest = repairMissingPathTree(*fix.repo, resume);
    REQUIRE_FALSE(rest.error.has_value());
    CHECK(rest.finished);
    CHECK(rest.nodesCreated == 3);
    CHECK(fix.missing() == 0);
}
