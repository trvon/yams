#include <algorithm>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#include <yams/common/utf8_utils.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/metadata/path_utils.h>
#include <yams/search/bk_tree.h>

using namespace yams;
using namespace yams::metadata;
using namespace yams::search;

class MetadataRepositoryTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use temporary database for tests; allow override for restricted environments
        const char* t = std::getenv("YAMS_TEST_TMPDIR");
        auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
        std::error_code ec;
        std::filesystem::create_directories(base, ec);
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        dbPath_ = base / (std::string("metadata_repo_test_") + std::to_string(ts) + ".db");
        std::filesystem::remove(dbPath_);

        // Create connection pool
        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;

        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        auto initResult = pool_->initialize();
        ASSERT_TRUE(initResult.has_value());

        // Create repository
        repository_ = std::make_unique<MetadataRepository>(*pool_);
    }

    void TearDown() override {
        repository_.reset();
        pool_->shutdown();
        pool_.reset();
        std::filesystem::remove(dbPath_);
    }

    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repository_;
};

namespace {

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

} // namespace

TEST_F(MetadataRepositoryTest, InsertAndGetDocument) {
    DocumentInfo docInfo;
    docInfo.sha256Hash = "hash123";
    docInfo.fileName = "test.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto result = repository_->insertDocument(docInfo);
    ASSERT_TRUE(result.has_value());

    auto docId = result.value();
    EXPECT_GT(docId, 0);

    // Verify document was created
    auto getResult = repository_->getDocument(docId);
    ASSERT_TRUE(getResult.has_value());
    ASSERT_TRUE(getResult.value().has_value());

    auto retrievedDoc = getResult.value().value();
    EXPECT_EQ(retrievedDoc.id, docId);
    EXPECT_EQ(retrievedDoc.sha256Hash, docInfo.sha256Hash);
    EXPECT_EQ(retrievedDoc.fileName, docInfo.fileName);
    EXPECT_EQ(retrievedDoc.fileSize, docInfo.fileSize);
    EXPECT_EQ(retrievedDoc.mimeType, docInfo.mimeType);
}

TEST_F(MetadataRepositoryTest, GetDocumentNotFound) {
    auto result = repository_->getDocument(999999);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result.value().has_value());
}

TEST_F(MetadataRepositoryTest, GetDocumentByHash) {
    DocumentInfo docInfo;
    docInfo.sha256Hash = "hash456";
    docInfo.fileName = "hash_test.txt";
    docInfo.fileSize = 2048;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = repository_->insertDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());

    // Find by hash
    auto findResult = repository_->getDocumentByHash("hash456");
    ASSERT_TRUE(findResult.has_value());
    ASSERT_TRUE(findResult.value().has_value());

    auto foundDoc = findResult.value().value();
    EXPECT_EQ(foundDoc.sha256Hash, "hash456");
    EXPECT_EQ(foundDoc.fileName, "hash_test.txt");
}

TEST_F(MetadataRepositoryTest, UpdateDocument) {
    // Create document
    DocumentInfo docInfo;
    docInfo.sha256Hash = "hash789";
    docInfo.fileName = "original.txt";
    docInfo.fileSize = 2048;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = repository_->insertDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();

    // Update document
    docInfo.id = docId;
    docInfo.fileName = "updated.txt";
    docInfo.fileSize = 4096;
    docInfo.modifiedTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

    auto updateResult = repository_->updateDocument(docInfo);
    ASSERT_TRUE(updateResult.has_value());

    // Verify update
    auto getResult = repository_->getDocument(docId);
    ASSERT_TRUE(getResult.has_value());
    ASSERT_TRUE(getResult.value().has_value());

    auto updatedDoc = getResult.value().value();
    EXPECT_EQ(updatedDoc.fileName, "updated.txt");
    EXPECT_EQ(updatedDoc.fileSize, 4096);
}

TEST_F(MetadataRepositoryTest, QueryDocumentsHandlesExactPrefixAndSuffix) {
    auto docA = makeDocumentWithPath(
        "/notes/todo.md", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto docB = makeDocumentWithPath(
        "/notes/log.txt", "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
    auto docC = makeDocumentWithPath(
        "/reports/summary.pdf", "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
        "application/pdf");

    ASSERT_TRUE(repository_->insertDocument(docA).has_value());
    ASSERT_TRUE(repository_->insertDocument(docB).has_value());
    ASSERT_TRUE(repository_->insertDocument(docC).has_value());

    // Exact path via hash lookup (path hash)
    DocumentQueryOptions exactOpts;
    exactOpts.exactPath = "/notes/todo.md";
    auto exactRes = repository_->queryDocuments(exactOpts);
    ASSERT_TRUE(exactRes.has_value());
    ASSERT_EQ(exactRes.value().size(), 1u);
    EXPECT_EQ(exactRes.value().front().filePath, "/notes/todo.md");

    // Directory prefix with subdirectories
    DocumentQueryOptions prefixOpts;
    prefixOpts.pathPrefix = "/notes";
    prefixOpts.prefixIsDirectory = true;
    prefixOpts.includeSubdirectories = true;
    auto prefixRes = repository_->queryDocuments(prefixOpts);
    ASSERT_TRUE(prefixRes.has_value());
    std::vector<std::string> notePaths;
    for (const auto& d : prefixRes.value())
        notePaths.push_back(d.filePath);
    std::sort(notePaths.begin(), notePaths.end());
    ASSERT_EQ(notePaths.size(), 2u);
    EXPECT_EQ(notePaths[0], "/notes/log.txt");
    EXPECT_EQ(notePaths[1], "/notes/todo.md");

    // Suffix/contains fragment via FTS + reverse_path
    DocumentQueryOptions containsOpts;
    containsOpts.containsFragment = "todo.md";
    containsOpts.containsUsesFts = true;
    auto containsRes = repository_->queryDocuments(containsOpts);
    ASSERT_TRUE(containsRes.has_value());
    ASSERT_EQ(containsRes.value().size(), 1u);
    EXPECT_EQ(containsRes.value().front().filePath, "/notes/todo.md");

    // Extension filter restricted to `.txt`
    DocumentQueryOptions extOpts;
    extOpts.extension = ".txt";
    auto extRes = repository_->queryDocuments(extOpts);
    ASSERT_TRUE(extRes.has_value());
    ASSERT_EQ(extRes.value().size(), 1u);
    EXPECT_EQ(extRes.value().front().filePath, "/notes/log.txt");
}

TEST_F(MetadataRepositoryTest, DeleteDocument) {
    // Create document
    DocumentInfo docInfo;
    docInfo.sha256Hash = "hash999";
    docInfo.fileName = "delete_me.txt";
    docInfo.fileSize = 512;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = repository_->insertDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();

    // Delete document
    auto deleteResult = repository_->deleteDocument(docId);
    ASSERT_TRUE(deleteResult.has_value());

    // Verify deletion
    auto getResult = repository_->getDocument(docId);
    ASSERT_TRUE(getResult.has_value());
    EXPECT_FALSE(getResult.value().has_value());
}

TEST_F(MetadataRepositoryTest, SetAndGetMetadata) {
    // Create document
    DocumentInfo docInfo;
    docInfo.sha256Hash = "metadata_test";
    docInfo.fileName = "meta.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = repository_->insertDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();

    // Set metadata
    MetadataValue value("Test Author");

    auto setResult = repository_->setMetadata(docId, "author", value);
    ASSERT_TRUE(setResult.has_value());

    // Get metadata
    auto getResult = repository_->getMetadata(docId, "author");
    ASSERT_TRUE(getResult.has_value());
    ASSERT_TRUE(getResult.value().has_value());

    auto retrievedValue = getResult.value().value();
    EXPECT_EQ(retrievedValue.asString(), "Test Author");
}

TEST_F(MetadataRepositoryTest, GetAllMetadata) {
    // Create document
    DocumentInfo docInfo;
    docInfo.sha256Hash = "all_metadata_test";
    docInfo.fileName = "meta_all.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = repository_->insertDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();

    // Set multiple metadata values
    MetadataValue authorValue("John Doe");
    repository_->setMetadata(docId, "author", authorValue);

    MetadataValue yearValue(int64_t(2024));
    repository_->setMetadata(docId, "year", yearValue);

    MetadataValue ratingValue(4.5);
    repository_->setMetadata(docId, "rating", ratingValue);

    // Get all metadata
    auto getAllResult = repository_->getAllMetadata(docId);
    ASSERT_TRUE(getAllResult.has_value());

    auto metadata = getAllResult.value();
    EXPECT_EQ(metadata.size(), 3u);

    EXPECT_EQ(metadata["author"].asString(), "John Doe");
    EXPECT_EQ(metadata["year"].asInteger(), 2024);
    EXPECT_FLOAT_EQ(metadata["rating"].asReal(), 4.5);
}

TEST_F(MetadataRepositoryTest, RemoveMetadata) {
    // Create document
    DocumentInfo docInfo;
    docInfo.sha256Hash = "remove_metadata_test";
    docInfo.fileName = "remove.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = repository_->insertDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();

    // Set metadata
    MetadataValue value("Temporary");
    repository_->setMetadata(docId, "temp", value);

    // Verify it exists
    auto getResult = repository_->getMetadata(docId, "temp");
    ASSERT_TRUE(getResult.has_value());
    ASSERT_TRUE(getResult.value().has_value());

    // Remove metadata
    auto removeResult = repository_->removeMetadata(docId, "temp");
    ASSERT_TRUE(removeResult.has_value());

    // Verify removal
    getResult = repository_->getMetadata(docId, "temp");
    ASSERT_TRUE(getResult.has_value());
    EXPECT_FALSE(getResult.value().has_value());
}

TEST_F(MetadataRepositoryTest, SearchFunctionality) {
    // Create document with content
    DocumentInfo docInfo;
    docInfo.sha256Hash = "search_test";
    docInfo.fileName = "search.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = repository_->insertDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();

    // Index document content
    auto indexResult = repository_->indexDocumentContent(
        docId, "Test Document", "This is a test document with searchable content", "text/plain");
    ASSERT_TRUE(indexResult.has_value());

    // Perform search
    auto searchResult = repository_->search("test", 10, 0);
    ASSERT_TRUE(searchResult.has_value());

    auto results = searchResult.value();
    EXPECT_GT(results.results.size(), std::size_t{0});
}

TEST_F(MetadataRepositoryTest, SearchSanitizesSnippetUtf8) {
    DocumentInfo docInfo;
    docInfo.sha256Hash = "search_bad_utf8";
    docInfo.fileName = "bad_utf8.txt";
    docInfo.fileSize = 32;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = repository_->insertDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();

    std::string badContent = "alpha";
    badContent.push_back(static_cast<char>(0xFF));
    badContent += "beta";

    auto indexResult =
        repository_->indexDocumentContent(docId, "Bad UTF-8", badContent, "text/plain");
    ASSERT_TRUE(indexResult.has_value());

    auto searchResult = repository_->search("alpha", 10, 0);
    ASSERT_TRUE(searchResult.has_value());
    const auto& results = searchResult.value();
    ASSERT_FALSE(results.results.empty());

    bool matchedDocument = false;
    for (const auto& item : results.results) {
        if (item.document.sha256Hash == "search_bad_utf8") {
            matchedDocument = true;
            auto sanitized = common::sanitizeUtf8(item.snippet);
            EXPECT_EQ(item.snippet, sanitized);
            EXPECT_EQ(item.snippet.find(static_cast<char>(0xFF)), std::string::npos);
        }
    }

    EXPECT_TRUE(matchedDocument);
}

TEST_F(MetadataRepositoryTest, IndexDocumentContentSanitizesUtf8) {
    DocumentInfo docInfo;
    docInfo.sha256Hash = "fts_bad_utf8";
    docInfo.fileName = "fts_bad_utf8.txt";
    docInfo.fileSize = 64;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime =
        std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    docInfo.modifiedTime = docInfo.createdTime;

    auto createResult = repository_->insertDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();

    std::string badContent = "gamma";
    badContent.push_back(static_cast<char>(0xFF));
    badContent += "delta";

    auto indexResult =
        repository_->indexDocumentContent(docId, "Bad FTS UTF-8", badContent, "text/plain");
    ASSERT_TRUE(indexResult.has_value());

    auto fetchResult = pool_->withConnection([&](metadata::Database& db) -> Result<std::string> {
        auto stmtResult = db.prepare("SELECT content FROM documents_fts WHERE rowid = ?");
        if (!stmtResult)
            return stmtResult.error();
        metadata::Statement stmt = std::move(stmtResult).value();
        auto bindResult = stmt.bind(1, docId);
        if (!bindResult)
            return bindResult.error();
        auto stepResult = stmt.step();
        if (!stepResult)
            return stepResult.error();
        if (!stepResult.value()) {
            return Error{ErrorCode::NotFound, "FTS entry not found"};
        }
        return stmt.getString(0);
    });

    ASSERT_TRUE(fetchResult.has_value());
    const auto storedContent = fetchResult.value();
    const auto sanitized = common::sanitizeUtf8(badContent);
    EXPECT_EQ(storedContent, sanitized);
    EXPECT_EQ(storedContent.find(static_cast<char>(0xFF)), std::string::npos);
}

TEST_F(MetadataRepositoryTest, CountsAndModifiedSince) {
    using clock = std::chrono::system_clock;
    auto now = std::chrono::floor<std::chrono::seconds>(clock::now());

    // Three docs with different extraction statuses and modified times
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

    ASSERT_TRUE(repository_->insertDocument(d1).has_value());
    ASSERT_TRUE(repository_->insertDocument(d2).has_value());
    ASSERT_TRUE(repository_->insertDocument(d3).has_value());

    // Basic counts
    auto total = repository_->getDocumentCount();
    ASSERT_TRUE(total.has_value());
    EXPECT_GE(total.value(), 3); // >= because other tests may have inserted

    auto extracted = repository_->getContentExtractedDocumentCount();
    ASSERT_TRUE(extracted.has_value());
    // At least the two we inserted with contentExtracted=true
    EXPECT_GE(extracted.value(), 2);

    auto c_pending = repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Pending);
    auto c_success = repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Success);
    auto c_failed = repository_->getDocumentCountByExtractionStatus(ExtractionStatus::Failed);
    ASSERT_TRUE(c_pending.has_value());
    ASSERT_TRUE(c_success.has_value());
    ASSERT_TRUE(c_failed.has_value());
    EXPECT_GE(c_pending.value(), 1);
    EXPECT_GE(c_success.value(), 1);
    EXPECT_GE(c_failed.value(), 1);

    // Modified-since should include only d2 among our three when since=now-60s
    auto since = clock::now() - std::chrono::seconds(60);
    auto modifiedRes = repository_->findDocumentsModifiedSince(since);
    ASSERT_TRUE(modifiedRes.has_value());
    // Confirm at least one document (d2) qualifies
    bool foundRecent = false;
    for (const auto& doc : modifiedRes.value()) {
        if (doc.sha256Hash == d2.sha256Hash) {
            foundRecent = true;
            break;
        }
    }
    EXPECT_TRUE(foundRecent);
}
