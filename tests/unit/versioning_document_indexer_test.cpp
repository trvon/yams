#include <fstream>
#include <gtest/gtest.h>
#include <tests/utils/test_helpers.h>
#include <yams/indexing/document_indexer.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>

using namespace yams;
using namespace yams::indexing;
using namespace yams::metadata;

namespace {
void writeTextFile(const std::filesystem::path& p, std::string_view text) {
    std::ofstream f(p, std::ios::binary | std::ios::trunc);
    f.write(text.data(), static_cast<std::streamsize>(text.size()));
}
} // namespace

class VersioningIndexerTest : public yams::test::YamsTest {};

TEST_F(VersioningIndexerTest, PathSeries_NewThenUpdate_CreatesVersionEdgeAndFlags) {
    // Enable feature flag (default is on, but make explicit)
#if defined(_WIN32)
    _putenv("YAMS_ENABLE_VERSIONING=1");
#else
    setenv("YAMS_ENABLE_VERSIONING", "1", 1);
#endif

    // Setup repository
    auto dbPath = (testDir / "meta.db").string();
    ConnectionPool pool(dbPath);
    ASSERT_TRUE(pool.initialize());
    auto repo = std::make_shared<MetadataRepository>(pool);

    // Create indexer
    auto indexer = createDocumentIndexer(repo);

    // Prepare file
    auto filePath = testDir / "doc.txt";
    writeTextFile(filePath, "hello v1\n");

    // Index v1
    auto r1 = indexer->indexDocument(filePath, {});
    ASSERT_TRUE(r1);
    auto res1 = r1.value();
    ASSERT_EQ(res1.status, IndexingStatus::Completed);
    auto id1 = std::stoll(res1.documentId);

    // Check metadata: version=1, is_latest=true
    auto ver1 = repo->getMetadata(id1, "version");
    ASSERT_TRUE(ver1);
    ASSERT_TRUE(ver1.value().has_value());
    EXPECT_EQ(ver1.value()->asInteger(), 1);
    auto latest1 = repo->getMetadata(id1, "is_latest");
    ASSERT_TRUE(latest1);
    ASSERT_TRUE(latest1.value().has_value());
    EXPECT_TRUE(latest1.value()->asBoolean());

    // Overwrite file with new content (same path)
    writeTextFile(filePath, "hello v2 changed\n");

    // Index v2
    auto r2 = indexer->indexDocument(filePath, {});
    ASSERT_TRUE(r2);
    auto res2 = r2.value();
    ASSERT_EQ(res2.status, IndexingStatus::Completed);
    auto id2 = std::stoll(res2.documentId);
    ASSERT_NE(id1, id2);

    // Exactly one latest=true at this path and it's id2
    auto samePathDocs = repo->findDocumentsByPath(filePath.string());
    ASSERT_TRUE(samePathDocs);
    size_t latestCount = 0;
    for (const auto& d : samePathDocs.value()) {
        auto l = repo->getMetadata(d.id, "is_latest");
        if (l && l.value().has_value() && l.value()->asBoolean()) {
            ++latestCount;
            EXPECT_EQ(d.id, id2);
        }
    }
    EXPECT_EQ(latestCount, 1u);

    // Version incremented on new doc
    auto ver2 = repo->getMetadata(id2, "version");
    ASSERT_TRUE(ver2);
    ASSERT_TRUE(ver2.value().has_value());
    EXPECT_EQ(ver2.value()->asInteger(), 2);

    // Relationship VersionOf exists: parent=id1, child=id2
    auto rels = repo->getRelationships(id2);
    ASSERT_TRUE(rels);
    bool foundVersionEdge = false;
    for (const auto& r : rels.value()) {
        if (r.relationshipType == RelationshipType::VersionOf && r.parentId == id1 &&
            r.childId == id2) {
            foundVersionEdge = true;
            break;
        }
    }
    EXPECT_TRUE(foundVersionEdge);
}
