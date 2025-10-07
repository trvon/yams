// Integration exercise for queryDocumentsByPattern using a real MetadataRepository
#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/metadata/query_helpers.h>

using namespace std::chrono;
using namespace yams::metadata;

namespace {
std::filesystem::path make_temp_db() {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (std::string("qh_int_") + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}

DocumentInfo make_doc(const std::string& path, const std::string& hash,
                      const std::string& mime = "text/plain") {
    DocumentInfo d;
    d.filePath = path;
    auto derived = computePathDerivedValues(path);
    d.fileName = std::filesystem::path(derived.normalizedPath).filename().string();
    d.fileExtension = std::filesystem::path(d.fileName).extension().string();
    d.fileSize = 0;
    d.sha256Hash = hash;
    d.mimeType = mime;
    d.pathPrefix = derived.pathPrefix;
    d.reversePath = derived.reversePath;
    d.pathHash = derived.pathHash;
    d.parentHash = derived.parentHash;
    d.pathDepth = derived.pathDepth;
    auto now = floor<seconds>(system_clock::now());
    d.createdTime = now;
    d.modifiedTime = now;
    d.indexedTime = now;
    d.contentExtracted = false;
    d.extractionStatus = ExtractionStatus::Pending;
    return d;
}
} // namespace

TEST(QueryHelpersIntegration, QueryDocumentsByPatternBasic) {
    auto dbPath = make_temp_db();
    ConnectionPoolConfig cfg;
    cfg.enableWAL = false; // keep it light for tests
    cfg.minConnections = 1;
    cfg.maxConnections = 2;
    ConnectionPool pool(dbPath.string(), cfg);
    ASSERT_TRUE(pool.initialize().has_value());

    MetadataRepository repo(pool);

    // Seed three docs
    auto id1 = repo.insertDocument(make_doc("/notes/todo.md", "hashA"));
    ASSERT_TRUE(id1.has_value());
    auto id2 = repo.insertDocument(make_doc("/notes/ideas.txt", "hashB"));
    ASSERT_TRUE(id2.has_value());
    auto id3 = repo.insertDocument(make_doc("/projects/yams/README.md", "hashC"));
    ASSERT_TRUE(id3.has_value());

    // Pattern: directory prefix
    auto r1 = queryDocumentsByPattern(repo, "/notes/%");
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1.value().size(), 2u);

    // Pattern: extension
    auto r2 = queryDocumentsByPattern(repo, "%.md");
    ASSERT_TRUE(r2.has_value());
    // At least the two .md files
    EXPECT_GE(r2.value().size(), 2u);

    // Pattern: contains fragment (by name)
    auto r3 = queryDocumentsByPattern(repo, "%/README.md");
    ASSERT_TRUE(r3.has_value());
    ASSERT_EQ(r3.value().size(), 1u);
    EXPECT_EQ(r3.value().at(0).filePath, "/projects/yams/README.md");
}
