// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/storage/corpus_stats.h>

using namespace yams;
using namespace yams::metadata;

namespace {
std::filesystem::path tempDbPath(const char* prefix) {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (std::string(prefix) + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}

DocumentInfo makeDocumentWithPath(const std::string& path, const std::string& hash) {
    DocumentInfo info;
    info.filePath = path;
    info.fileName = std::filesystem::path(path).filename().string();
    info.fileExtension = std::filesystem::path(path).extension().string();
    info.fileSize = 1234;
    info.sha256Hash = hash;
    info.mimeType = "text/plain";
    info.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    info.modifiedTime = info.createdTime;
    info.indexedTime = info.createdTime;
    populatePathDerivedFields(info);
    info.contentExtracted = true;
    info.extractionStatus = ExtractionStatus::Success;
    return info;
}

struct MetadataRepositoryFixture {
    MetadataRepositoryFixture() {
        dbPath = tempDbPath("metadata_repo_tag_stats_");

        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;

        pool = std::make_unique<ConnectionPool>(dbPath.string(), config);
        REQUIRE(pool->initialize().has_value());
        repository = std::make_unique<MetadataRepository>(*pool);
    }

    ~MetadataRepositoryFixture() {
        repository.reset();
        pool->shutdown();
        pool.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
    }

    Result<void> installMetadataInsertAbortTrigger(const std::string& triggerName) {
        return pool->withConnection([&](Database& db) -> Result<void> {
            return db.execute("CREATE TRIGGER " + triggerName +
                              " BEFORE INSERT ON metadata "
                              "BEGIN "
                              "SELECT RAISE(ABORT, 'injected metadata insert failure'); "
                              "END");
        });
    }

    Result<void> installDocumentDeleteAbortTrigger(int64_t documentId,
                                                   const std::string& triggerName) {
        return pool->withConnection([&](Database& db) -> Result<void> {
            return db.execute("CREATE TRIGGER " + triggerName +
                              " BEFORE DELETE ON documents "
                              "WHEN OLD.id = " +
                              std::to_string(documentId) +
                              " BEGIN "
                              "SELECT RAISE(ABORT, 'injected document delete failure'); "
                              "END");
        });
    }

    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::unique_ptr<MetadataRepository> repository;
};
} // namespace

TEST_CASE("MetadataRepository tag writes refresh cached corpus stats",
          "[unit][metadata][repository][metadata][corpus-stats]") {
    MetadataRepositoryFixture fix;

    auto doc = makeDocumentWithPath("/tmp/tag-stats.txt", "tag-stats-hash");
    auto insert = fix.repository->insertDocument(doc);
    REQUIRE(insert.has_value());
    const int64_t docId = insert.value();

    auto warmStats = fix.repository->getCorpusStats();
    REQUIRE(warmStats.has_value());
    CHECK((warmStats.value().tagCount == 0));
    CHECK((warmStats.value().docsWithTags == 0));

    REQUIRE(fix.repository->setMetadata(docId, "tag:genre", MetadataValue("jazz")).has_value());
    auto taggedStats = fix.repository->getCorpusStats();
    REQUIRE(taggedStats.has_value());
    CHECK((taggedStats.value().tagCount == 1));
    CHECK((taggedStats.value().docsWithTags == 1));

    REQUIRE(fix.repository->removeMetadata(docId, "tag:genre").has_value());
    auto removedStats = fix.repository->getCorpusStats();
    REQUIRE(removedStats.has_value());
    CHECK((removedStats.value().tagCount == 0));
    CHECK((removedStats.value().docsWithTags == 0));
}

TEST_CASE(
    "MetadataRepository insertDocumentWithMetadata refreshes cached tag stats for existing doc",
    "[unit][metadata][repository][metadata][corpus-stats]") {
    MetadataRepositoryFixture fix;

    auto doc =
        makeDocumentWithPath("/tmp/insert-with-metadata-tag.txt", "insert-with-metadata-tag-hash");
    auto insert = fix.repository->insertDocument(doc);
    REQUIRE(insert.has_value());

    auto warmStats = fix.repository->getCorpusStats();
    REQUIRE(warmStats.has_value());
    CHECK((warmStats.value().tagCount == 0));
    CHECK((warmStats.value().docsWithTags == 0));

    std::vector<std::pair<std::string, MetadataValue>> metadata = {
        {"tag:source", MetadataValue("import")}, {"title", MetadataValue("Cached tag stats")}};
    auto upsert = fix.repository->insertDocumentWithMetadata(doc, metadata, nullptr);
    REQUIRE(upsert.has_value());
    CHECK((upsert.value() == insert.value()));

    auto stats = fix.repository->getCorpusStats();
    REQUIRE(stats.has_value());
    CHECK((stats.value().tagCount == 1));
    CHECK((stats.value().docsWithTags == 1));

    auto stored = fix.repository->getMetadata(insert.value(), "tag:source");
    REQUIRE(stored.has_value());
    REQUIRE(stored.value().has_value());
    CHECK((stored.value()->asString() == "import"));
}

TEST_CASE("MetadataRepository insertDocumentWithMetadata keeps corpus stats coherent for new docs",
          "[unit][metadata][repository][metadata][corpus-stats][crud]") {
    MetadataRepositoryFixture fix;
    fix.repository->initializeCounters();

    auto warmStats = fix.repository->getCorpusStats();
    REQUIRE(warmStats.has_value());
    CHECK((warmStats.value().docCount == 0));

    auto doc = makeDocumentWithPath("/tmp/corpus/new-note.md", "insert-with-metadata-new-hash");
    doc.fileSize = 777;

    std::vector<std::pair<std::string, MetadataValue>> metadata = {
        {"tag:topic", MetadataValue("notes")}, {"title", MetadataValue("Fresh document")}};
    auto insert = fix.repository->insertDocumentWithMetadata(doc, metadata, nullptr);
    REQUIRE(insert.has_value());

    auto stats = fix.repository->getCorpusStats();
    REQUIRE(stats.has_value());
    CHECK((stats.value().docCount == 1));
    CHECK((stats.value().totalSizeBytes == 777));
    CHECK((stats.value().contentExtractedCount == 1));
    CHECK((stats.value().tagCount == 1));
    CHECK((stats.value().docsWithTags == 1));
    auto extIt = stats.value().extensionCounts.find(".md");
    REQUIRE((extIt != stats.value().extensionCounts.end()));
    CHECK((extIt->second == 1));
}

TEST_CASE("MetadataRepository updateDocument refreshes cached corpus stats and extracted counters",
          "[unit][metadata][repository][metadata][corpus-stats][crud]") {
    MetadataRepositoryFixture fix;
    fix.repository->initializeCounters();

    auto doc = makeDocumentWithPath("/tmp/corpus/original.md", "update-corpus-stats-hash");
    doc.fileSize = 100;
    doc.contentExtracted = false;
    doc.extractionStatus = ExtractionStatus::Pending;

    auto insert = fix.repository->insertDocument(doc);
    REQUIRE(insert.has_value());
    doc.id = insert.value();

    auto warmStats = fix.repository->getCorpusStats();
    REQUIRE(warmStats.has_value());
    CHECK((warmStats.value().totalSizeBytes == 100));
    CHECK((warmStats.value().contentExtractedCount == 0));
    auto warmExtIt = warmStats.value().extensionCounts.find(".md");
    REQUIRE((warmExtIt != warmStats.value().extensionCounts.end()));
    CHECK((warmExtIt->second == 1));

    doc.filePath = "/tmp/corpus/nested/code/original.cpp";
    doc.fileName = std::filesystem::path(doc.filePath).filename().string();
    doc.fileExtension = std::filesystem::path(doc.filePath).extension().string();
    doc.fileSize = 900;
    doc.modifiedTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.indexedTime = doc.modifiedTime;
    doc.contentExtracted = true;
    doc.extractionStatus = ExtractionStatus::Success;
    populatePathDerivedFields(doc);

    REQUIRE(fix.repository->updateDocument(doc).has_value());
    CHECK((fix.repository->getCachedExtractedCount() == 1));

    auto updated = fix.repository->getDocument(doc.id);
    REQUIRE(updated.has_value());
    REQUIRE(updated.value().has_value());
    CHECK((updated.value()->fileExtension == ".cpp"));
    CHECK((updated.value()->fileSize == 900));
    CHECK((updated.value()->contentExtracted));

    auto stats = fix.repository->getCorpusStats();
    REQUIRE(stats.has_value());
    CHECK((stats.value().docCount == 1));
    CHECK((stats.value().totalSizeBytes == 900));
    CHECK((stats.value().contentExtractedCount == 1));
    auto cppIt = stats.value().extensionCounts.find(".cpp");
    REQUIRE((cppIt != stats.value().extensionCounts.end()));
    CHECK((cppIt->second == 1));
    CHECK((stats.value().extensionCounts.find(".md") == stats.value().extensionCounts.end()));
}

TEST_CASE("MetadataRepository insertDocumentWithMetadata preserves cached counters on rollback",
          "[unit][metadata][repository][metadata][corpus-stats][rollback]") {
    MetadataRepositoryFixture fix;
    fix.repository->initializeCounters();

    auto doc = makeDocumentWithPath("/tmp/corpus/rollback-note.md", "rollback-insert-metadata");
    doc.fileSize = 321;

    REQUIRE(fix.installMetadataInsertAbortTrigger("abort_metadata_insert_for_counter_rollback")
                .has_value());

    std::vector<std::pair<std::string, MetadataValue>> metadata = {
        {"tag:topic", MetadataValue("rollback")}, {"title", MetadataValue("Should fail")}};
    auto insert = fix.repository->insertDocumentWithMetadata(doc, metadata, nullptr);
    REQUIRE_FALSE(insert.has_value());
    CHECK((insert.error().message.find("constraint failed") != std::string::npos));
    CHECK((fix.repository->getCachedDocumentCount() == 0));
    CHECK((fix.repository->getCachedExtractedCount() == 0));

    auto persisted = fix.repository->getDocumentByHash("rollback-insert-metadata");
    REQUIRE(persisted.has_value());
    CHECK_FALSE(persisted.value().has_value());
}

TEST_CASE("MetadataRepository deleteDocumentsBatch keeps cached counters and tag stats aligned",
          "[unit][metadata][repository][metadata][corpus-stats][crud]") {
    MetadataRepositoryFixture fix;
    fix.repository->initializeCounters();

    auto doomed = makeDocumentWithPath("/tmp/corpus/doomed.md", "delete-corpus-doomed-hash");
    doomed.fileSize = 150;
    auto survivor = makeDocumentWithPath("/tmp/corpus/survivor.txt", "delete-corpus-survivor-hash");
    survivor.fileSize = 250;
    survivor.contentExtracted = false;
    survivor.extractionStatus = ExtractionStatus::Pending;

    auto doomedInsert = fix.repository->insertDocument(doomed);
    auto survivorInsert = fix.repository->insertDocument(survivor);
    REQUIRE(doomedInsert.has_value());
    REQUIRE(survivorInsert.has_value());
    const auto doomedId = doomedInsert.value();

    REQUIRE(
        fix.repository->setMetadata(doomedId, "tag:kind", MetadataValue("temporary")).has_value());
    REQUIRE(
        fix.repository->insertContent(DocumentContent{doomedId, "doomed content", 14, "test", "en"})
            .has_value());
    REQUIRE(fix.repository
                ->indexDocumentContent(doomedId, doomed.fileName, "doomed content", doomed.mimeType)
                .has_value());
    REQUIRE(
        fix.repository->updateDocumentEmbeddingStatusByHash(doomed.sha256Hash, true, "embed-model")
            .has_value());

    auto warmStats = fix.repository->getCorpusStats();
    REQUIRE(warmStats.has_value());
    CHECK((warmStats.value().docCount == 2));
    CHECK((warmStats.value().tagCount == 1));
    CHECK((warmStats.value().docsWithTags == 1));
    CHECK((warmStats.value().embeddingCount == 1));
    CHECK((warmStats.value().ftsIndexedCount == 1));
    CHECK((warmStats.value().contentExtractedCount == 1));

    auto deleted = fix.repository->deleteDocumentsBatch({doomedId, 999999});
    REQUIRE(deleted.has_value());
    CHECK((deleted.value() == 1));
    CHECK((fix.repository->getCachedDocumentCount() == 1));
    CHECK((fix.repository->getCachedExtractedCount() == 0));
    CHECK((fix.repository->getCachedIndexedCount() == 0));
    CHECK((fix.repository->getCachedEmbeddedCount() == 0));

    auto stats = fix.repository->getCorpusStats();
    REQUIRE(stats.has_value());
    CHECK((stats.value().docCount == 1));
    CHECK((stats.value().totalSizeBytes == 250));
    CHECK((stats.value().contentExtractedCount == 0));
    CHECK((stats.value().embeddingCount == 0));
    CHECK((stats.value().ftsIndexedCount == 0));
    CHECK((stats.value().tagCount == 0));
    CHECK((stats.value().docsWithTags == 0));
    auto txtIt = stats.value().extensionCounts.find(".txt");
    REQUIRE((txtIt != stats.value().extensionCounts.end()));
    CHECK((txtIt->second == 1));
}

TEST_CASE("MetadataRepository deleteDocumentsBatch preserves cached counters on rollback",
          "[unit][metadata][repository][metadata][corpus-stats][rollback]") {
    MetadataRepositoryFixture fix;
    fix.repository->initializeCounters();

    auto doomed = makeDocumentWithPath("/tmp/corpus/rollback-doomed.md", "rollback-doomed-hash");
    auto blocker =
        makeDocumentWithPath("/tmp/corpus/rollback-blocker.txt", "rollback-blocker-hash");
    blocker.contentExtracted = false;
    blocker.extractionStatus = ExtractionStatus::Pending;

    auto doomedInsert = fix.repository->insertDocument(doomed);
    auto blockerInsert = fix.repository->insertDocument(blocker);
    REQUIRE(doomedInsert.has_value());
    REQUIRE(blockerInsert.has_value());
    const auto doomedId = doomedInsert.value();
    const auto blockerId = blockerInsert.value();

    REQUIRE(
        fix.repository->setMetadata(doomedId, "tag:rollback", MetadataValue("yes")).has_value());
    REQUIRE(fix.repository
                ->insertContent(DocumentContent{doomedId, "rollback content", 16, "test", "en"})
                .has_value());
    REQUIRE(
        fix.repository
            ->indexDocumentContent(doomedId, doomed.fileName, "rollback content", doomed.mimeType)
            .has_value());
    REQUIRE(
        fix.repository->updateDocumentEmbeddingStatusByHash(doomed.sha256Hash, true, "embed-model")
            .has_value());

    REQUIRE(
        fix.installDocumentDeleteAbortTrigger(blockerId, "abort_second_delete_for_counter_rollback")
            .has_value());

    auto deleted = fix.repository->deleteDocumentsBatch({doomedId, blockerId});
    REQUIRE_FALSE(deleted.has_value());
    CHECK((deleted.error().message.find("constraint failed") != std::string::npos));
    CHECK((fix.repository->getCachedDocumentCount() == 2));
    CHECK((fix.repository->getCachedExtractedCount() == 1));
    CHECK((fix.repository->getCachedIndexedCount() == 1));
    CHECK((fix.repository->getCachedEmbeddedCount() == 1));

    auto doomedAfter = fix.repository->getDocument(doomedId);
    auto blockerAfter = fix.repository->getDocument(blockerId);
    REQUIRE(doomedAfter.has_value());
    REQUIRE(blockerAfter.has_value());
    REQUIRE((doomedAfter.value().has_value()));
    REQUIRE((blockerAfter.value().has_value()));
}
