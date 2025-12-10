/**
 * @file session_isolation_test.cpp
 * @brief Tests for session-isolated memory (PBI-082)
 *
 * Tests that documents tagged with session_id are properly isolated:
 * 1. Documents with session_id can be found via findDocumentsBySessionId
 * 2. Session documents can be counted via countDocumentsBySessionId
 * 3. Session tags can be removed (merge operation)
 * 4. Session documents can be deleted (discard operation)
 */

#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

using namespace yams;
using namespace yams::metadata;

class SessionIsolationTest : public ::testing::Test {
protected:
    void SetUp() override {
        const char* t = std::getenv("YAMS_TEST_TMPDIR");
        auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
        std::error_code ec;
        std::filesystem::create_directories(base, ec);
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        dbPath_ = base / (std::string("session_isolation_test_") + std::to_string(ts) + ".db");
        std::filesystem::remove(dbPath_);

        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;

        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        auto initResult = pool_->initialize();
        ASSERT_TRUE(initResult.has_value());

        repository_ = std::make_unique<MetadataRepository>(*pool_);
    }

    void TearDown() override {
        repository_.reset();
        pool_->shutdown();
        pool_.reset();
        std::filesystem::remove(dbPath_);
    }

    DocumentInfo makeDocument(const std::string& path, const std::string& hash) {
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

    int64_t addDocumentWithSession(const std::string& path, const std::string& hash,
                                   const std::string& sessionId) {
        auto doc = makeDocument(path, hash);
        auto result = repository_->insertDocument(doc);
        EXPECT_TRUE(result.has_value()) << "Failed to add document";
        int64_t docId = result.value();

        if (!sessionId.empty()) {
            auto metaResult = repository_->setMetadata(docId, "session_id", MetadataValue(sessionId));
            EXPECT_TRUE(metaResult.has_value()) << "Failed to set session_id metadata";
        }
        return docId;
    }

    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repository_;
};

TEST_F(SessionIsolationTest, FindDocumentsBySessionId) {
    // Add documents with different sessions
    addDocumentWithSession("/work/session1/file1.txt", "hash1111111111111111111111111111", "session-a");
    addDocumentWithSession("/work/session1/file2.txt", "hash2222222222222222222222222222", "session-a");
    addDocumentWithSession("/work/session2/file3.txt", "hash3333333333333333333333333333", "session-b");
    addDocumentWithSession("/work/global/file4.txt", "hash4444444444444444444444444444", "");  // No session

    // Find documents in session-a
    auto resultA = repository_->findDocumentsBySessionId("session-a");
    ASSERT_TRUE(resultA.has_value());
    EXPECT_EQ(resultA.value().size(), 2);

    // Find documents in session-b
    auto resultB = repository_->findDocumentsBySessionId("session-b");
    ASSERT_TRUE(resultB.has_value());
    EXPECT_EQ(resultB.value().size(), 1);

    // Non-existent session returns empty
    auto resultNone = repository_->findDocumentsBySessionId("nonexistent");
    ASSERT_TRUE(resultNone.has_value());
    EXPECT_EQ(resultNone.value().size(), 0);
}

TEST_F(SessionIsolationTest, CountDocumentsBySessionId) {
    addDocumentWithSession("/a/f1.txt", "hashaaaa1111111111111111111111", "test-session");
    addDocumentWithSession("/a/f2.txt", "hashaaaa2222222222222222222222", "test-session");
    addDocumentWithSession("/a/f3.txt", "hashaaaa3333333333333333333333", "test-session");
    addDocumentWithSession("/b/f1.txt", "hashbbbb1111111111111111111111", "other-session");

    auto count = repository_->countDocumentsBySessionId("test-session");
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(count.value(), 3);

    auto countOther = repository_->countDocumentsBySessionId("other-session");
    ASSERT_TRUE(countOther.has_value());
    EXPECT_EQ(countOther.value(), 1);

    auto countNone = repository_->countDocumentsBySessionId("empty");
    ASSERT_TRUE(countNone.has_value());
    EXPECT_EQ(countNone.value(), 0);
}

TEST_F(SessionIsolationTest, RemoveSessionIdFromDocuments_Merge) {
    addDocumentWithSession("/merge/f1.txt", "hashmerge111111111111111111111", "merge-session");
    addDocumentWithSession("/merge/f2.txt", "hashmerge222222222222222222222", "merge-session");
    addDocumentWithSession("/keep/f1.txt", "hashkeep1111111111111111111111", "keep-session");

    // Verify before merge
    auto countBefore = repository_->countDocumentsBySessionId("merge-session");
    ASSERT_TRUE(countBefore.has_value());
    EXPECT_EQ(countBefore.value(), 2);

    // Merge (remove session_id tags)
    auto mergeResult = repository_->removeSessionIdFromDocuments("merge-session");
    ASSERT_TRUE(mergeResult.has_value());

    // After merge, documents should no longer be in session
    auto countAfter = repository_->countDocumentsBySessionId("merge-session");
    ASSERT_TRUE(countAfter.has_value());
    EXPECT_EQ(countAfter.value(), 0);

    // Other session should be unaffected
    auto countKeep = repository_->countDocumentsBySessionId("keep-session");
    ASSERT_TRUE(countKeep.has_value());
    EXPECT_EQ(countKeep.value(), 1);
}

TEST_F(SessionIsolationTest, DeleteDocumentsBySessionId_Discard) {
    auto id1 = addDocumentWithSession("/discard/f1.txt", "hashdiscard1111111111111111111", "discard-session");
    auto id2 = addDocumentWithSession("/discard/f2.txt", "hashdiscard2222222222222222222", "discard-session");
    addDocumentWithSession("/keep/f1.txt", "hashsafe11111111111111111111111", "safe-session");

    // Verify documents exist
    auto doc1 = repository_->getDocument(id1);
    ASSERT_TRUE(doc1.has_value() && doc1.value().has_value());
    auto doc2 = repository_->getDocument(id2);
    ASSERT_TRUE(doc2.has_value() && doc2.value().has_value());

    // Discard session
    auto deleteResult = repository_->deleteDocumentsBySessionId("discard-session");
    ASSERT_TRUE(deleteResult.has_value());
    EXPECT_EQ(deleteResult.value(), 2);

    // Documents should be deleted
    auto deletedDoc1 = repository_->getDocument(id1);
    ASSERT_TRUE(deletedDoc1.has_value());
    EXPECT_FALSE(deletedDoc1.value().has_value()) << "Document should be deleted";

    auto deletedDoc2 = repository_->getDocument(id2);
    ASSERT_TRUE(deletedDoc2.has_value());
    EXPECT_FALSE(deletedDoc2.value().has_value()) << "Document should be deleted";

    // Safe session should be unaffected
    auto countSafe = repository_->countDocumentsBySessionId("safe-session");
    ASSERT_TRUE(countSafe.has_value());
    EXPECT_EQ(countSafe.value(), 1);
}

TEST_F(SessionIsolationTest, SessionDocumentsVisibleOnlyToSession) {
    // Add session documents and global documents
    addDocumentWithSession("/session/code.cpp", "hashsession11111111111111111111", "dev-session");
    addDocumentWithSession("/global/readme.md", "hashglobal111111111111111111111", "");  // No session

    // Session query should only find session document
    auto sessionDocs = repository_->findDocumentsBySessionId("dev-session");
    ASSERT_TRUE(sessionDocs.has_value());
    EXPECT_EQ(sessionDocs.value().size(), 1);
    EXPECT_EQ(sessionDocs.value()[0].fileName, "code.cpp");

    // Empty session query returns nothing (global docs have no session_id)
    auto globalDocs = repository_->findDocumentsBySessionId("");
    ASSERT_TRUE(globalDocs.has_value());
    EXPECT_EQ(globalDocs.value().size(), 0);
}

TEST_F(SessionIsolationTest, MultipleSessionsAreIsolated) {
    addDocumentWithSession("/alice/notes.txt", "hashalice1111111111111111111111", "alice-workspace");
    addDocumentWithSession("/alice/todo.txt", "hashalice2222222222222222222222", "alice-workspace");
    addDocumentWithSession("/bob/code.py", "hashbob111111111111111111111111", "bob-workspace");
    addDocumentWithSession("/shared/doc.md", "hashshared11111111111111111111", "");

    // Each session only sees its own documents
    auto aliceDocs = repository_->findDocumentsBySessionId("alice-workspace");
    ASSERT_TRUE(aliceDocs.has_value());
    EXPECT_EQ(aliceDocs.value().size(), 2);

    auto bobDocs = repository_->findDocumentsBySessionId("bob-workspace");
    ASSERT_TRUE(bobDocs.has_value());
    EXPECT_EQ(bobDocs.value().size(), 1);

    // Merging alice doesn't affect bob
    repository_->removeSessionIdFromDocuments("alice-workspace");

    auto aliceAfter = repository_->countDocumentsBySessionId("alice-workspace");
    ASSERT_TRUE(aliceAfter.has_value());
    EXPECT_EQ(aliceAfter.value(), 0);

    auto bobAfter = repository_->countDocumentsBySessionId("bob-workspace");
    ASSERT_TRUE(bobAfter.has_value());
    EXPECT_EQ(bobAfter.value(), 1);
}
