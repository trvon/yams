// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 YAMS Contributors

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <thread>
#include <vector>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <yams/common/utf8_utils.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/metadata/path_utils.h>

using namespace yams;
using namespace yams::metadata;
using namespace yams::search;

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

DocumentInfo makeDocumentWithPath(const std::string& path, const std::string& hash,
                                  const std::string& mime = "text/plain") {
    DocumentInfo info;
    info.filePath = path;
    info.fileName = std::filesystem::path(path).filename().string();
    info.fileExtension = std::filesystem::path(path).extension().string();
    info.fileSize = 1234;
    info.sha256Hash = hash;
    info.mimeType = mime;
    info.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    info.modifiedTime = info.createdTime;
    info.indexedTime = info.createdTime;
    auto derived = computePathDerivedValues(path);
    info.filePath = derived.normalizedPath;
    info.pathPrefix = derived.pathPrefix;
    info.reversePath = derived.reversePath;
    info.pathHash = derived.pathHash;
    info.parentHash = derived.parentHash;
    info.pathDepth = derived.pathDepth;
    info.contentExtracted = true;
    info.extractionStatus = ExtractionStatus::Success;
    return info;
}

struct MetadataRepositoryFixture {
    MetadataRepositoryFixture() {
        dbPath_ = tempDbPath("metadata_repo_catch2_test_");

        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;

        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        auto initResult = pool_->initialize();
        REQUIRE(initResult.has_value());

        repository_ = std::make_unique<MetadataRepository>(*pool_);
    }

    ~MetadataRepositoryFixture() {
        repository_.reset();
        pool_->shutdown();
        pool_.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath_, ec);
    }

    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repository_;
};

} // namespace

TEST_CASE("MetadataRepository: insert and get document", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "hash123";
    docInfo.fileName = "test.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto result = fix.repository_->insertDocument(docInfo);
    REQUIRE(result.has_value());

    auto docId = result.value();
    CHECK(docId > 0);

    auto getResult = fix.repository_->getDocument(docId);
    REQUIRE(getResult.has_value());
    REQUIRE(getResult.value().has_value());

    auto retrievedDoc = getResult.value().value();
    CHECK(retrievedDoc.id == docId);
    CHECK(retrievedDoc.sha256Hash == docInfo.sha256Hash);
    CHECK(retrievedDoc.fileName == docInfo.fileName);
    CHECK(retrievedDoc.fileSize == docInfo.fileSize);
    CHECK(retrievedDoc.mimeType == docInfo.mimeType);
}

TEST_CASE("MetadataRepository: get document not found", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto result = fix.repository_->getDocument(999999);
    REQUIRE(result.has_value());
    CHECK_FALSE(result.value().has_value());
}

TEST_CASE("MetadataRepository: get document by hash", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "hash456";
    docInfo.fileName = "hash_test.txt";
    docInfo.fileSize = 2048;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE(createResult.has_value());

    auto findResult = fix.repository_->getDocumentByHash("hash456");
    REQUIRE(findResult.has_value());
    REQUIRE(findResult.value().has_value());

    auto foundDoc = findResult.value().value();
    CHECK(foundDoc.sha256Hash == "hash456");
    CHECK(foundDoc.fileName == "hash_test.txt");
}

TEST_CASE("MetadataRepository: update document", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "hash789";
    docInfo.fileName = "original.txt";
    docInfo.fileSize = 2048;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE(createResult.has_value());
    auto docId = createResult.value();

    docInfo.id = docId;
    docInfo.fileName = "updated.txt";
    docInfo.fileSize = 4096;
    docInfo.modifiedTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

    auto updateResult = fix.repository_->updateDocument(docInfo);
    REQUIRE(updateResult.has_value());

    auto getResult = fix.repository_->getDocument(docId);
    REQUIRE(getResult.has_value());
    REQUIRE(getResult.value().has_value());

    auto updatedDoc = getResult.value().value();
    CHECK(updatedDoc.fileName == "updated.txt");
    CHECK(updatedDoc.fileSize == 4096);
}

TEST_CASE("MetadataRepository: query documents handles exact prefix and suffix",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto docA = makeDocumentWithPath(
        "/notes/todo.md", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto docB = makeDocumentWithPath(
        "/notes/log.txt", "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
    auto docC = makeDocumentWithPath(
        "/reports/summary.pdf", "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
        "application/pdf");

    REQUIRE(fix.repository_->insertDocument(docA).has_value());
    REQUIRE(fix.repository_->insertDocument(docB).has_value());
    REQUIRE(fix.repository_->insertDocument(docC).has_value());

    SECTION("Exact path lookup") {
        DocumentQueryOptions exactOpts;
        exactOpts.exactPath = "/notes/todo.md";
        auto exactRes = fix.repository_->queryDocuments(exactOpts);
        REQUIRE(exactRes.has_value());
        REQUIRE(exactRes.value().size() == 1);
        CHECK(exactRes.value().front().filePath == "/notes/todo.md");
    }

    SECTION("Directory prefix with subdirectories") {
        DocumentQueryOptions prefixOpts;
        prefixOpts.pathPrefix = "/notes";
        prefixOpts.prefixIsDirectory = true;
        prefixOpts.includeSubdirectories = true;
        auto prefixRes = fix.repository_->queryDocuments(prefixOpts);
        REQUIRE(prefixRes.has_value());
        std::vector<std::string> notePaths;
        for (const auto& d : prefixRes.value())
            notePaths.push_back(d.filePath);
        std::sort(notePaths.begin(), notePaths.end());
        REQUIRE(notePaths.size() == 2);
        CHECK(notePaths[0] == "/notes/log.txt");
        CHECK(notePaths[1] == "/notes/todo.md");
    }

    SECTION("Contains fragment via FTS") {
        DocumentQueryOptions containsOpts;
        containsOpts.containsFragment = "todo.md";
        containsOpts.containsUsesFts = true;
        auto containsRes = fix.repository_->queryDocuments(containsOpts);
        REQUIRE(containsRes.has_value());
        REQUIRE(containsRes.value().size() == 1);
        CHECK(containsRes.value().front().filePath == "/notes/todo.md");
    }

    SECTION("Extension filter") {
        DocumentQueryOptions extOpts;
        extOpts.extension = ".txt";
        auto extRes = fix.repository_->queryDocuments(extOpts);
        REQUIRE(extRes.has_value());
        REQUIRE(extRes.value().size() == 1);
        CHECK(extRes.value().front().filePath == "/notes/log.txt");
    }
}

TEST_CASE("MetadataRepository: delete document", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "hash999";
    docInfo.fileName = "delete_me.txt";
    docInfo.fileSize = 512;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE(createResult.has_value());
    auto docId = createResult.value();

    auto deleteResult = fix.repository_->deleteDocument(docId);
    REQUIRE(deleteResult.has_value());

    auto getResult = fix.repository_->getDocument(docId);
    REQUIRE(getResult.has_value());
    CHECK_FALSE(getResult.value().has_value());
}

TEST_CASE("MetadataRepository: set and get metadata", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "metadata_test";
    docInfo.fileName = "meta.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE(createResult.has_value());
    auto docId = createResult.value();

    MetadataValue value("Test Author");

    auto setResult = fix.repository_->setMetadata(docId, "author", value);
    REQUIRE(setResult.has_value());

    auto getResult = fix.repository_->getMetadata(docId, "author");
    REQUIRE(getResult.has_value());
    REQUIRE(getResult.value().has_value());

    auto retrievedValue = getResult.value().value();
    CHECK(retrievedValue.asString() == "Test Author");
}

TEST_CASE("MetadataRepository: get all metadata", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "all_metadata_test";
    docInfo.fileName = "meta_all.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE(createResult.has_value());
    auto docId = createResult.value();

    MetadataValue authorValue("John Doe");
    fix.repository_->setMetadata(docId, "author", authorValue);

    MetadataValue yearValue(int64_t(2024));
    fix.repository_->setMetadata(docId, "year", yearValue);

    MetadataValue ratingValue(4.5);
    fix.repository_->setMetadata(docId, "rating", ratingValue);

    auto getAllResult = fix.repository_->getAllMetadata(docId);
    REQUIRE(getAllResult.has_value());

    auto metadata = getAllResult.value();
    CHECK(metadata.size() == 3);

    CHECK(metadata["author"].asString() == "John Doe");
    CHECK(metadata["year"].asInteger() == 2024);
    CHECK(metadata["rating"].asReal() == Catch::Approx(4.5));
}

TEST_CASE("MetadataRepository: remove metadata", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "remove_metadata_test";
    docInfo.fileName = "remove.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE(createResult.has_value());
    auto docId = createResult.value();

    MetadataValue value("Temporary");
    fix.repository_->setMetadata(docId, "temp", value);

    auto getResult = fix.repository_->getMetadata(docId, "temp");
    REQUIRE(getResult.has_value());
    REQUIRE(getResult.value().has_value());

    auto removeResult = fix.repository_->removeMetadata(docId, "temp");
    REQUIRE(removeResult.has_value());

    getResult = fix.repository_->getMetadata(docId, "temp");
    REQUIRE(getResult.has_value());
    CHECK_FALSE(getResult.value().has_value());
}

TEST_CASE("MetadataRepository: search functionality", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "search_test";
    docInfo.fileName = "search.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE(createResult.has_value());
    auto docId = createResult.value();

    auto indexResult = fix.repository_->indexDocumentContent(
        docId, "Test Document", "This is a test document with searchable content", "text/plain");
    REQUIRE(indexResult.has_value());

    auto searchResult = fix.repository_->search("test", 10, 0);
    REQUIRE(searchResult.has_value());

    auto results = searchResult.value();
    CHECK(results.results.size() > 0);
}

TEST_CASE("MetadataRepository: fuzzy search returns content matches",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto doc = makeDocumentWithPath(
        "/notes/fuzzy_content.txt",
        "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");

    auto insertResult = fix.repository_->insertDocument(doc);
    REQUIRE(insertResult.has_value());
    auto docId = insertResult.value();

    constexpr char kRareTerm[] = "blorptastic";
    std::string contentText = std::string("The ") + kRareTerm;

    DocumentContent content;
    content.documentId = docId;
    content.contentText = contentText;
    content.contentLength = static_cast<int64_t>(contentText.length());
    content.extractionMethod = "test";
    content.language = "en";
    auto contentInsertResult = fix.repository_->insertContent(content);
    REQUIRE(contentInsertResult.has_value());

    auto indexResult =
        fix.repository_->indexDocumentContent(docId, doc.fileName, contentText, "text/plain");
    REQUIRE(indexResult.has_value());

    auto fuzzyResult = fix.repository_->fuzzySearch(kRareTerm, 0.6f, 10);
    REQUIRE(fuzzyResult.has_value());

    const auto& searchResults = fuzzyResult.value();
    REQUIRE(searchResults.results.size() == 1);
    CHECK(searchResults.results.front().document.id == docId);
}

TEST_CASE("MetadataRepository: search sanitizes snippet UTF-8", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    DocumentInfo docInfo;
    docInfo.sha256Hash = "search_bad_utf8";
    docInfo.fileName = "bad_utf8.txt";
    docInfo.fileSize = 32;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = fix.repository_->insertDocument(docInfo);
    REQUIRE(createResult.has_value());
    auto docId = createResult.value();

    std::string badContent = "alpha";
    badContent.push_back(static_cast<char>(0xFF));
    badContent += "beta";

    auto indexResult =
        fix.repository_->indexDocumentContent(docId, "Bad UTF-8", badContent, "text/plain");
    REQUIRE(indexResult.has_value());

    auto searchResult = fix.repository_->search("alpha", 10, 0);
    REQUIRE(searchResult.has_value());
    const auto& results = searchResult.value();
    REQUIRE_FALSE(results.results.empty());

    bool matchedDocument = false;
    for (const auto& item : results.results) {
        if (item.document.sha256Hash == "search_bad_utf8") {
            matchedDocument = true;
            auto sanitized = common::sanitizeUtf8(item.snippet);
            CHECK(item.snippet == sanitized);
            CHECK(item.snippet.find(static_cast<char>(0xFF)) == std::string::npos);
        }
    }

    CHECK(matchedDocument);
}

TEST_CASE("MetadataRepository: counts and modified since", "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    using clock = std::chrono::system_clock;
    auto now = std::chrono::floor<std::chrono::seconds>(clock::now());

    auto d1 = makeDocumentWithPath("/tmp/a.txt", "H-A");
    d1.extractionStatus = ExtractionStatus::Success;
    d1.contentExtracted = true;
    d1.modifiedTime = now - std::chrono::hours(2);

    auto d2 = makeDocumentWithPath("/tmp/b.txt", "H-B");
    d2.extractionStatus = ExtractionStatus::Pending;
    d2.contentExtracted = false;
    d2.modifiedTime = now - std::chrono::seconds(10);

    auto d3 = makeDocumentWithPath("/tmp/c.txt", "H-C");
    d3.extractionStatus = ExtractionStatus::Failed;
    d3.contentExtracted = true;
    d3.modifiedTime = now - std::chrono::hours(3);

    REQUIRE(fix.repository_->insertDocument(d1).has_value());
    REQUIRE(fix.repository_->insertDocument(d2).has_value());
    REQUIRE(fix.repository_->insertDocument(d3).has_value());

    auto total = fix.repository_->getDocumentCount();
    REQUIRE(total.has_value());
    CHECK(total.value() >= 3);

    auto extracted = fix.repository_->getContentExtractedDocumentCount();
    REQUIRE(extracted.has_value());
    CHECK(extracted.value() >= 2);

    auto c_pending = fix.repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Pending);
    auto c_success = fix.repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Success);
    auto c_failed = fix.repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Failed);
    REQUIRE(c_pending.has_value());
    REQUIRE(c_success.has_value());
    REQUIRE(c_failed.has_value());
    CHECK(c_pending.value() >= 1);
    CHECK(c_success.value() >= 1);
    CHECK(c_failed.value() >= 1);

    auto since = clock::now() - std::chrono::seconds(60);
    auto modifiedRes = fix.repository_->findDocumentsModifiedSince(since);
    REQUIRE(modifiedRes.has_value());
    bool foundRecent = false;
    for (const auto& doc : modifiedRes.value()) {
        if (doc.sha256Hash == d2.sha256Hash) {
            foundRecent = true;
            break;
        }
    }
    CHECK(foundRecent);
}

TEST_CASE("MetadataRepository: path tree upsert creates nodes and counts",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto docInfo = makeDocumentWithPath(
        "/src/example.txt", "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");
    auto docInsert = fix.repository_->insertDocument(docInfo);
    REQUIRE(docInsert.has_value());
    auto docId = docInsert.value();
    docInfo.id = docId;

    auto upsert = fix.repository_->upsertPathTreeForDocument(docInfo, docId, true, {});
    REQUIRE(upsert.has_value());

    auto lookup = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    REQUIRE(lookup.has_value());
    REQUIRE(lookup.value().has_value());
    auto node = lookup.value().value();
    CHECK(node.fullPath == "/src");
    CHECK(node.docCount == 1);

    auto repeat = fix.repository_->upsertPathTreeForDocument(docInfo, docId, false, {});
    REQUIRE(repeat.has_value());
    auto afterRepeat = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    REQUIRE(afterRepeat.has_value());
    REQUIRE(afterRepeat.value().has_value());
    CHECK(afterRepeat.value()->docCount == 1);

    auto fullLookup = fix.repository_->findPathTreeNodeByFullPath("/src/example.txt");
    REQUIRE(fullLookup.has_value());
    REQUIRE(fullLookup.value().has_value());
    CHECK(fullLookup.value()->fullPath == "/src/example.txt");
}

TEST_CASE("MetadataRepository: path tree centroid accumulates embeddings",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto docInfo = makeDocumentWithPath(
        "/src/lib/foo.cpp", "EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE");
    auto docInsert = fix.repository_->insertDocument(docInfo);
    REQUIRE(docInsert.has_value());
    auto docId = docInsert.value();
    docInfo.id = docId;

    REQUIRE(fix.repository_->upsertPathTreeForDocument(docInfo, docId, true, {}).has_value());

    std::vector<float> embedding{1.0F, 2.0F, 3.0F};
    REQUIRE(
        fix.repository_
            ->upsertPathTreeForDocument(docInfo, docId, false,
                                        std::span<const float>(embedding.data(), embedding.size()))
            .has_value());

    auto node = fix.repository_->findPathTreeNode(PathTreeNode::kNullParent, "src");
    REQUIRE(node.has_value());
    REQUIRE(node.value().has_value());
    CHECK(node.value()->centroidWeight == 1);
}

TEST_CASE("MetadataRepository: remove path tree decrements counts and deletes empty nodes",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto docInfo =
        makeDocumentWithPath("/project/src/lib/util.cpp",
                             "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto docInsert = fix.repository_->insertDocument(docInfo);
    REQUIRE(docInsert.has_value());
    auto docId = docInsert.value();
    docInfo.id = docId;

    std::vector<float> embedding{1.0F, 2.0F, 3.0F};
    auto upsert = fix.repository_->upsertPathTreeForDocument(
        docInfo, docId, true, std::span<const float>(embedding.data(), embedding.size()));
    REQUIRE(upsert.has_value());

    auto leafNode = fix.repository_->findPathTreeNodeByFullPath("/project/src/lib/util.cpp");
    REQUIRE(leafNode.has_value());
    REQUIRE(leafNode.value().has_value());
    CHECK(leafNode.value()->docCount == 1);
    CHECK(leafNode.value()->centroidWeight == 1);

    auto libNode = fix.repository_->findPathTreeNodeByFullPath("/project/src/lib");
    REQUIRE(libNode.has_value());
    REQUIRE(libNode.value().has_value());
    CHECK(libNode.value()->docCount == 1);

    auto remove = fix.repository_->removePathTreeForDocument(
        docInfo, docId, std::span<const float>(embedding.data(), embedding.size()));
    REQUIRE(remove.has_value());

    auto afterRemoveLeaf = fix.repository_->findPathTreeNodeByFullPath("/project/src/lib/util.cpp");
    REQUIRE(afterRemoveLeaf.has_value());
    CHECK_FALSE(afterRemoveLeaf.value().has_value());

    auto afterRemoveLib = fix.repository_->findPathTreeNodeByFullPath("/project/src/lib");
    REQUIRE(afterRemoveLib.has_value());
    CHECK_FALSE(afterRemoveLib.value().has_value());
}

TEST_CASE("MetadataRepository: remove path tree recalculates centroid",
          "[unit][metadata][repository]") {
    MetadataRepositoryFixture fix;

    auto doc1 = makeDocumentWithPath(
        "/shared/file1.txt", "1111111111111111111111111111111111111111111111111111111111111111");
    auto doc1Insert = fix.repository_->insertDocument(doc1);
    REQUIRE(doc1Insert.has_value());
    doc1.id = doc1Insert.value();

    auto doc2 = makeDocumentWithPath(
        "/shared/file2.txt", "2222222222222222222222222222222222222222222222222222222222222222");
    auto doc2Insert = fix.repository_->insertDocument(doc2);
    REQUIRE(doc2Insert.has_value());
    doc2.id = doc2Insert.value();

    std::vector<float> emb1{1.0F, 0.0F, 0.0F};
    std::vector<float> emb2{0.0F, 1.0F, 0.0F};

    auto upsert1 = fix.repository_->upsertPathTreeForDocument(
        doc1, doc1.id, true, std::span<const float>(emb1.data(), emb1.size()));
    REQUIRE(upsert1.has_value());

    auto upsert2 = fix.repository_->upsertPathTreeForDocument(
        doc2, doc2.id, true, std::span<const float>(emb2.data(), emb2.size()));
    REQUIRE(upsert2.has_value());

    auto sharedNode = fix.repository_->findPathTreeNodeByFullPath("/shared");
    REQUIRE(sharedNode.has_value());
    REQUIRE(sharedNode.value().has_value());
    CHECK(sharedNode.value()->docCount == 2);
    CHECK(sharedNode.value()->centroidWeight == 2);

    auto remove1 = fix.repository_->removePathTreeForDocument(
        doc1, doc1.id, std::span<const float>(emb1.data(), emb1.size()));
    REQUIRE(remove1.has_value());

    auto afterRemove = fix.repository_->findPathTreeNodeByFullPath("/shared");
    REQUIRE(afterRemove.has_value());
    REQUIRE(afterRemove.value().has_value());
    CHECK(afterRemove.value()->docCount == 1);
    CHECK(afterRemove.value()->centroidWeight == 1);
}
