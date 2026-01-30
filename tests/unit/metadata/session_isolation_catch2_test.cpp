// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

/**
 * @file session_isolation_catch2_test.cpp
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

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

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

struct SessionIsolationFixture {
    SessionIsolationFixture() {
        dbPath_ = tempDbPath("session_isolation_catch2_test_");

        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;

        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        auto initResult = pool_->initialize();
        REQUIRE(initResult.has_value());

        repository_ = std::make_unique<MetadataRepository>(*pool_);
    }

    ~SessionIsolationFixture() {
        repository_.reset();
        pool_->shutdown();
        pool_.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath_, ec);
    }

    DocumentInfo makeDocument(const std::string& path, const std::string& hash) {
        DocumentInfo info;
        info.filePath = path;
        info.fileName = std::filesystem::path(path).filename().string();
        info.fileExtension = std::filesystem::path(path).extension().string();
        info.fileSize = 1234;
        info.sha256Hash = hash;
        info.mimeType = "text/plain";
        info.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
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
        REQUIRE(result.has_value());
        int64_t docId = result.value();

        if (!sessionId.empty()) {
            auto metaResult =
                repository_->setMetadata(docId, "session_id", MetadataValue(sessionId));
            REQUIRE(metaResult.has_value());
        }
        return docId;
    }

    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repository_;
};

} // namespace

TEST_CASE("SessionIsolation: find documents by session ID", "[unit][metadata][session]") {
    SessionIsolationFixture fix;

    fix.addDocumentWithSession("/work/session1/file1.txt", "hash1111111111111111111111111111",
                               "session-a");
    fix.addDocumentWithSession("/work/session1/file2.txt", "hash2222222222222222222222222222",
                               "session-a");
    fix.addDocumentWithSession("/work/session2/file3.txt", "hash3333333333333333333333333333",
                               "session-b");
    fix.addDocumentWithSession("/work/global/file4.txt", "hash4444444444444444444444444444",
                               ""); // No session

    auto resultA = fix.repository_->findDocumentsBySessionId("session-a");
    REQUIRE(resultA.has_value());
    CHECK(resultA.value().size() == 2);

    auto resultB = fix.repository_->findDocumentsBySessionId("session-b");
    REQUIRE(resultB.has_value());
    CHECK(resultB.value().size() == 1);

    auto resultNone = fix.repository_->findDocumentsBySessionId("nonexistent");
    REQUIRE(resultNone.has_value());
    CHECK(resultNone.value().size() == 0);
}

TEST_CASE("SessionIsolation: count documents by session ID", "[unit][metadata][session]") {
    SessionIsolationFixture fix;

    fix.addDocumentWithSession("/a/f1.txt", "hashaaaa1111111111111111111111", "test-session");
    fix.addDocumentWithSession("/a/f2.txt", "hashaaaa2222222222222222222222", "test-session");
    fix.addDocumentWithSession("/a/f3.txt", "hashaaaa3333333333333333333333", "test-session");
    fix.addDocumentWithSession("/b/f1.txt", "hashbbbb1111111111111111111111", "other-session");

    auto count = fix.repository_->countDocumentsBySessionId("test-session");
    REQUIRE(count.has_value());
    CHECK(count.value() == 3);

    auto countOther = fix.repository_->countDocumentsBySessionId("other-session");
    REQUIRE(countOther.has_value());
    CHECK(countOther.value() == 1);

    auto countNone = fix.repository_->countDocumentsBySessionId("empty");
    REQUIRE(countNone.has_value());
    CHECK(countNone.value() == 0);
}

TEST_CASE("SessionIsolation: remove session ID from documents (merge)",
          "[unit][metadata][session]") {
    SessionIsolationFixture fix;

    fix.addDocumentWithSession("/merge/f1.txt", "hashmerge111111111111111111111", "merge-session");
    fix.addDocumentWithSession("/merge/f2.txt", "hashmerge222222222222222222222", "merge-session");
    fix.addDocumentWithSession("/keep/f1.txt", "hashkeep1111111111111111111111", "keep-session");

    auto countBefore = fix.repository_->countDocumentsBySessionId("merge-session");
    REQUIRE(countBefore.has_value());
    CHECK(countBefore.value() == 2);

    auto mergeResult = fix.repository_->removeSessionIdFromDocuments("merge-session");
    REQUIRE(mergeResult.has_value());

    auto countAfter = fix.repository_->countDocumentsBySessionId("merge-session");
    REQUIRE(countAfter.has_value());
    CHECK(countAfter.value() == 0);

    auto countKeep = fix.repository_->countDocumentsBySessionId("keep-session");
    REQUIRE(countKeep.has_value());
    CHECK(countKeep.value() == 1);
}

TEST_CASE("SessionIsolation: delete documents by session ID (discard)",
          "[unit][metadata][session]") {
    SessionIsolationFixture fix;

    auto id1 = fix.addDocumentWithSession("/discard/f1.txt", "hashdiscard1111111111111111111",
                                          "discard-session");
    auto id2 = fix.addDocumentWithSession("/discard/f2.txt", "hashdiscard2222222222222222222",
                                          "discard-session");
    fix.addDocumentWithSession("/keep/f1.txt", "hashsafe11111111111111111111111", "safe-session");

    auto doc1 = fix.repository_->getDocument(id1);
    REQUIRE(doc1.has_value());
    REQUIRE(doc1.value().has_value());
    auto doc2 = fix.repository_->getDocument(id2);
    REQUIRE(doc2.has_value());
    REQUIRE(doc2.value().has_value());

    auto deleteResult = fix.repository_->deleteDocumentsBySessionId("discard-session");
    REQUIRE(deleteResult.has_value());
    CHECK(deleteResult.value() == 2);

    auto deletedDoc1 = fix.repository_->getDocument(id1);
    REQUIRE(deletedDoc1.has_value());
    CHECK_FALSE(deletedDoc1.value().has_value());

    auto deletedDoc2 = fix.repository_->getDocument(id2);
    REQUIRE(deletedDoc2.has_value());
    CHECK_FALSE(deletedDoc2.value().has_value());

    auto countSafe = fix.repository_->countDocumentsBySessionId("safe-session");
    REQUIRE(countSafe.has_value());
    CHECK(countSafe.value() == 1);
}

TEST_CASE("SessionIsolation: session documents visible only to session",
          "[unit][metadata][session]") {
    SessionIsolationFixture fix;

    fix.addDocumentWithSession("/session/code.cpp", "hashsession11111111111111111111",
                               "dev-session");
    fix.addDocumentWithSession("/global/readme.md", "hashglobal111111111111111111111", "");

    auto sessionDocs = fix.repository_->findDocumentsBySessionId("dev-session");
    REQUIRE(sessionDocs.has_value());
    CHECK(sessionDocs.value().size() == 1);
    CHECK(sessionDocs.value()[0].fileName == "code.cpp");

    auto globalDocs = fix.repository_->findDocumentsBySessionId("");
    REQUIRE(globalDocs.has_value());
    CHECK(globalDocs.value().size() == 0);
}

TEST_CASE("SessionIsolation: multiple sessions are isolated", "[unit][metadata][session]") {
    SessionIsolationFixture fix;

    fix.addDocumentWithSession("/alice/notes.txt", "hashalice1111111111111111111111",
                               "alice-workspace");
    fix.addDocumentWithSession("/alice/todo.txt", "hashalice2222222222222222222222",
                               "alice-workspace");
    fix.addDocumentWithSession("/bob/code.py", "hashbob111111111111111111111111", "bob-workspace");
    fix.addDocumentWithSession("/shared/doc.md", "hashshared11111111111111111111", "");

    auto aliceDocs = fix.repository_->findDocumentsBySessionId("alice-workspace");
    REQUIRE(aliceDocs.has_value());
    CHECK(aliceDocs.value().size() == 2);

    auto bobDocs = fix.repository_->findDocumentsBySessionId("bob-workspace");
    REQUIRE(bobDocs.has_value());
    CHECK(bobDocs.value().size() == 1);

    fix.repository_->removeSessionIdFromDocuments("alice-workspace");

    auto aliceAfter = fix.repository_->countDocumentsBySessionId("alice-workspace");
    REQUIRE(aliceAfter.has_value());
    CHECK(aliceAfter.value() == 0);

    auto bobAfter = fix.repository_->countDocumentsBySessionId("bob-workspace");
    REQUIRE(bobAfter.has_value());
    CHECK(bobAfter.value() == 1);
}
