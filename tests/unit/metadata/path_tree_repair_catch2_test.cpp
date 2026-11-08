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
    REQUIRE(result.has_value());
    const auto& stats = result.value();
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
    REQUIRE(again.has_value());
    CHECK(again.value().finished);
    CHECK(again.value().documentsMissing == 0);
    CHECK(again.value().nodesCreated == 0);
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
    REQUIRE(result.has_value());
    CHECK(result.value().documentsScanned == 3);
    CHECK(result.value().documentsMissing == 1);
    CHECK(result.value().nodesCreated == 1);

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
    REQUIRE(first.has_value());
    CHECK_FALSE(first.value().finished);
    CHECK(first.value().batches == 1);
    CHECK(first.value().documentsScanned == 2);
    CHECK(first.value().nodesCreated == 2);
    CHECK(first.value().lastDocumentId == docs[1].id);
    CHECK(fix.hasNode("/batch/doc-0.txt"));
    CHECK(fix.hasNode("/batch/doc-1.txt"));
    CHECK_FALSE(fix.hasNode("/batch/doc-2.txt"));
    CHECK(fix.missing() == 3);

    options.startAfterId = first.value().lastDocumentId;
    auto second = repairMissingPathTree(*fix.repo, options);
    REQUIRE(second.has_value());
    CHECK_FALSE(second.value().finished);
    CHECK(second.value().documentsScanned == 2);
    CHECK(second.value().lastDocumentId == docs[3].id);
    CHECK(fix.hasNode("/batch/doc-3.txt"));

    options.startAfterId = second.value().lastDocumentId;
    auto third = repairMissingPathTree(*fix.repo, options);
    REQUIRE(third.has_value());
    CHECK(third.value().finished); // short window: the table ended inside it
    CHECK(third.value().documentsScanned == 1);
    CHECK(third.value().lastDocumentId == docs[4].id);
    CHECK(fix.missing() == 0);

    // Resuming past the end examines nothing and reports the pass finished.
    options.startAfterId = third.value().lastDocumentId;
    auto past = repairMissingPathTree(*fix.repo, options);
    REQUIRE(past.has_value());
    CHECK(past.value().finished);
    CHECK(past.value().documentsScanned == 0);
    CHECK(past.value().lastDocumentId == docs[4].id);

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
        REQUIRE(run.has_value());
        CHECK(run.value().finished);
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
    REQUIRE(result.has_value());
    CHECK_FALSE(result.value().finished);
    CHECK(result.value().batches == 2);
    CHECK(result.value().nodesCreated == 4);
    CHECK(checks == 3); // checked before each batch, including the one it refused
    CHECK(fix.missing() == 2);

    SECTION("a refusal before the first batch does no work") {
        PathTreeRepairOptions refused;
        refused.shouldContinue = [] { return false; };
        auto none = repairMissingPathTree(*fix.repo, refused);
        REQUIRE(none.has_value());
        CHECK_FALSE(none.value().finished);
        CHECK(none.value().batches == 0);
        CHECK(fix.missing() == 2);
    }
    SECTION("resuming from the cursor finishes the pass") {
        PathTreeRepairOptions resume;
        resume.batchSize = 2;
        resume.startAfterId = result.value().lastDocumentId;
        auto rest = repairMissingPathTree(*fix.repo, resume);
        REQUIRE(rest.has_value());
        CHECK(rest.value().finished);
        CHECK(rest.value().nodesCreated == 2);
        CHECK(fix.missing() == 0);
    }
}

TEST_CASE("repairMissingPathTree rejects a zero batch size",
          "[unit][metadata][path-tree][repair]") {
    PathTreeRepairFixture fix;
    PathTreeRepairOptions options;
    options.batchSize = 0;
    CHECK_FALSE(repairMissingPathTree(*fix.repo, options).has_value());
}

TEST_CASE("scanDocsMissingPathTree pages by a document window, not by misses",
          "[unit][metadata][path-tree][repair]") {
    PathTreeRepairFixture fix;
    fix.insertWithTree("/scan/a.txt");
    auto b = fix.insertWithoutTree("/scan/b.txt");
    auto c = fix.insertWithTree("/scan/c.txt");
    auto d = fix.insertWithoutTree("/scan/d.txt");

    auto first = fix.repo->scanDocsMissingPathTree(0, 3);
    REQUIRE(first.has_value());
    CHECK(first.value().scanned == 3);
    CHECK(first.value().lastScannedId == c.id);
    REQUIRE(first.value().missing.size() == 1);
    CHECK(first.value().missing.front().id == b.id);

    auto second = fix.repo->scanDocsMissingPathTree(c.id, 3);
    REQUIRE(second.has_value());
    CHECK(second.value().scanned == 1);
    CHECK(second.value().lastScannedId == d.id);
    REQUIRE(second.value().missing.size() == 1);
    CHECK(second.value().missing.front().filePath == "/scan/d.txt");

    auto end = fix.repo->scanDocsMissingPathTree(d.id, 3);
    REQUIRE(end.has_value());
    CHECK(end.value().scanned == 0);
    CHECK(end.value().lastScannedId == d.id);
    CHECK(end.value().missing.empty());

    CHECK_FALSE(fix.repo->scanDocsMissingPathTree(0, 0).has_value());
}
