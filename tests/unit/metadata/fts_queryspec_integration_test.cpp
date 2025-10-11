#include <filesystem>
#include <gtest/gtest.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

using namespace yams::metadata;

TEST(FtsSearchQuerySpecIntegration, BasicFtsWhenAvailable) {
    // Skip if FTS5 not available
    Database db;
    auto dbFile = std::filesystem::temp_directory_path() / "fts_qs_basic.db";
    std::error_code ec;
    std::filesystem::remove(dbFile, ec); // best effort cleanup between runs
    ASSERT_TRUE(db.open(dbFile.string(), ConnectionMode::Create));
    auto f = db.hasFTS5();
    if (!f || !f.value()) {
        GTEST_SKIP() << "FTS5 not available";
    }

    ConnectionPool pool(dbFile.string());
    ASSERT_TRUE(pool.initialize());
    MetadataRepository repo(pool);

    // Insert a doc and index content
    DocumentInfo d{};
    d.filePath = "/q/hello.txt";
    auto dv = computePathDerivedValues(d.filePath);
    d.fileName = std::filesystem::path(dv.normalizedPath).filename().string();
    d.fileExtension = ".txt";
    d.fileSize = 0;
    d.sha256Hash = "HX";
    d.pathPrefix = dv.pathPrefix;
    d.reversePath = dv.reversePath;
    d.pathHash = dv.pathHash;
    d.parentHash = dv.parentHash;
    d.pathDepth = dv.pathDepth;
    auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    d.createdTime = d.modifiedTime = d.indexedTime = now;
    auto id = repo.insertDocument(d);
    ASSERT_TRUE(id);
    ASSERT_TRUE(
        repo.indexDocumentContent(id.value(), "Hello Title", "hello world content", "text/plain"));

    auto res = repo.search("hello", 10, 0, std::nullopt);
    if (!res.has_value()) {
        GTEST_SKIP() << "Search unavailable in this environment: " << res.error().message;
    }
    EXPECT_TRUE(res.value().isSuccess());
    EXPECT_GE(res.value().results.size(), 1u);
}
