#include <filesystem>
#include <gtest/gtest.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

using namespace yams::metadata;

namespace {
static std::filesystem::path tmpdb() {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto p = base / (std::string("tags_qs_") + std::to_string(std::rand()) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}

static DocumentInfo mkdoc(const std::string& path, const std::string& hash) {
    DocumentInfo d;
    d.filePath = path;
    auto dv = computePathDerivedValues(path);
    d.fileName = std::filesystem::path(dv.normalizedPath).filename().string();
    d.fileExtension = std::filesystem::path(d.fileName).extension().string();
    d.sha256Hash = hash;
    d.pathPrefix = dv.pathPrefix;
    d.reversePath = dv.reversePath;
    d.pathHash = dv.pathHash;
    d.parentHash = dv.parentHash;
    d.pathDepth = dv.pathDepth;
    auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    d.createdTime = d.modifiedTime = d.indexedTime = now;
    return d;
}
} // namespace

TEST(TagsQuerySpecIntegration, MatchAnyAndAll) {
    auto db = tmpdb();
    ConnectionPool pool(db.string());
    ASSERT_TRUE(pool.initialize());
    MetadataRepository repo(pool);

    auto id1 = repo.insertDocument(mkdoc("/t/a.md", "H1"));
    ASSERT_TRUE(id1);
    auto id2 = repo.insertDocument(mkdoc("/t/b.md", "H2"));
    ASSERT_TRUE(id2);

    // Store tags as metadata keys with tag: prefix
    ASSERT_TRUE(repo.setMetadata(id1.value(), "tag:alpha", MetadataValue("alpha")).has_value());
    ASSERT_TRUE(repo.setMetadata(id1.value(), "tag:beta", MetadataValue("beta")).has_value());
    ASSERT_TRUE(repo.setMetadata(id2.value(), "tag:alpha", MetadataValue("alpha")).has_value());

    // matchAny {alpha} returns both
    {
        auto res = repo.findDocumentsByTags({"alpha"}, /*matchAll=*/false);
        ASSERT_TRUE(res.has_value());
        EXPECT_EQ(res.value().size(), 2u);
    }
    // matchAll {alpha,beta} returns only id1
    {
        auto res = repo.findDocumentsByTags({"alpha", "beta"}, /*matchAll=*/true);
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(res.value().size(), 1u);
        EXPECT_EQ(res.value().at(0).sha256Hash, "H1");
    }
}
