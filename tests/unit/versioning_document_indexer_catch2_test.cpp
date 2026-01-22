// Catch2 tests for versioning document indexer
// Migrated from GTest: versioning_document_indexer_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>

#include <yams/compat/unistd.h>

#include <yams/indexing/document_indexer.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>

using namespace yams;
using namespace yams::indexing;
using namespace yams::metadata;

namespace {

std::filesystem::path make_temp_dir(std::string_view prefix = "yams_version_test_") {
    namespace fs = std::filesystem;
    const auto base = fs::temp_directory_path();
    std::uniform_int_distribution<int> dist(0, 9999);
    thread_local std::mt19937_64 rng{std::random_device{}()};
    for (int attempt = 0; attempt < 512; ++attempt) {
        const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        auto candidate =
            base / (std::string(prefix) + std::to_string(stamp) + "_" + std::to_string(dist(rng)));
        std::error_code ec;
        if (fs::create_directories(candidate, ec)) {
            return candidate;
        }
    }
    return base;
}

void writeTextFile(const std::filesystem::path& p, std::string_view text) {
    std::ofstream f(p, std::ios::binary | std::ios::trunc);
    f.write(text.data(), static_cast<std::streamsize>(text.size()));
}

struct VersioningIndexerFixture {
    VersioningIndexerFixture() { testDir = make_temp_dir(); }

    ~VersioningIndexerFixture() {
        std::error_code ec;
        std::filesystem::remove_all(testDir, ec);
    }

    std::filesystem::path testDir;
};

} // namespace

TEST_CASE_METHOD(VersioningIndexerFixture,
                 "Versioning PathSeries new then update creates version edge and flags",
                 "[versioning][indexer][catch2]") {
    // Enable feature flag (default is on, but make explicit)
#if defined(_WIN32)
    _putenv_s("YAMS_ENABLE_VERSIONING", "1");
#else
    setenv("YAMS_ENABLE_VERSIONING", "1", 1);
#endif

    // Setup repository
    auto dbPath = (testDir / "meta.db").string();
    ConnectionPool pool(dbPath);
    REQUIRE(pool.initialize());
    auto repo = std::make_shared<MetadataRepository>(pool);

    // Create indexer
    auto indexer = createDocumentIndexer(repo);

    // Prepare file
    auto filePath = testDir / "doc.txt";
    writeTextFile(filePath, "hello v1\n");

    // Index v1
    auto r1 = indexer->indexDocument(filePath, {});
    REQUIRE(r1);
    auto res1 = r1.value();
    REQUIRE(res1.status == IndexingStatus::Completed);
    auto id1 = std::stoll(res1.documentId);

    // Check metadata: version=1, is_latest=true
    auto ver1 = repo->getMetadata(id1, "version");
    REQUIRE(ver1);
    REQUIRE(ver1.value().has_value());
    CHECK(ver1.value()->asInteger() == 1);
    auto latest1 = repo->getMetadata(id1, "is_latest");
    REQUIRE(latest1);
    REQUIRE(latest1.value().has_value());
    CHECK(latest1.value()->asBoolean());

    // Overwrite file with new content (same path)
    writeTextFile(filePath, "hello v2 changed\n");

    // Index v2
    auto r2 = indexer->indexDocument(filePath, {});
    REQUIRE(r2);
    auto res2 = r2.value();
    REQUIRE(res2.status == IndexingStatus::Completed);
    auto id2 = std::stoll(res2.documentId);
    REQUIRE(id1 != id2);

    // Exactly one latest=true at this path and it's id2
    auto samePathDocs = metadata::queryDocumentsByPattern(*repo, filePath.string());
    REQUIRE(samePathDocs);
    size_t latestCount = 0;
    for (const auto& d : samePathDocs.value()) {
        auto l = repo->getMetadata(d.id, "is_latest");
        if (l && l.value().has_value() && l.value()->asBoolean()) {
            ++latestCount;
            CHECK(d.id == id2);
        }
    }
    CHECK(latestCount == 1u);

    // Version incremented on new doc
    auto ver2 = repo->getMetadata(id2, "version");
    REQUIRE(ver2);
    REQUIRE(ver2.value().has_value());
    CHECK(ver2.value()->asInteger() == 2);

    // Relationship VersionOf exists: parent=id1, child=id2
    auto rels = repo->getRelationships(id2);
    REQUIRE(rels);
    bool foundVersionEdge = false;
    for (const auto& r : rels.value()) {
        if (r.relationshipType == RelationshipType::VersionOf && r.parentId == id1 &&
            r.childId == id2) {
            foundVersionEdge = true;
            break;
        }
    }
    CHECK(foundVersionEdge);
}
