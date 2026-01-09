// Catch2 tests for embedding repair scan
// Migrated from GTest: embedding_repair_scan_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <cstdlib>
#include <filesystem>
#include <random>
#include <thread>
#include <yams/metadata/metadata_repository.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/vector_database.h>

namespace yams::repair::test {

namespace {

class InMemoryMetadata : public metadata::MetadataRepository {
public:
    InMemoryMetadata() : metadata::MetadataRepository(*getPool()) {}
    void add(const metadata::DocumentInfo& d) { docs_.push_back(d); }
    Result<std::vector<metadata::DocumentInfo>>
    queryDocuments(const metadata::DocumentQueryOptions&) override {
        return docs_;
    }
    Result<std::optional<metadata::DocumentInfo>>
    findDocumentByExactPath(const std::string& path) override {
        for (auto& d : docs_) {
            if (d.filePath == path)
                return std::optional<metadata::DocumentInfo>(d);
        }
        return std::optional<metadata::DocumentInfo>(std::nullopt);
    }
    Result<std::vector<std::string>> getDocumentTags(int64_t) override {
        return std::vector<std::string>{};
    }
    Result<std::optional<metadata::DocumentInfo>> getDocumentByHash(const std::string& h) override {
        for (auto& d : docs_)
            if (d.sha256Hash == h)
                return std::optional<metadata::DocumentInfo>(d);
        return std::optional<metadata::DocumentInfo>(std::nullopt);
    }

private:
    static std::shared_ptr<metadata::ConnectionPool> getPool() {
        static std::shared_ptr<metadata::ConnectionPool> p = [] {
            metadata::ConnectionPoolConfig cfg{};
            cfg.enableWAL = false;
            cfg.enableForeignKeys = false;
            auto pool = std::make_shared<metadata::ConnectionPool>(":memory:", cfg);
            (void)pool->initialize();
            return pool;
        }();
        return p;
    }
    std::vector<metadata::DocumentInfo> docs_;
};

} // namespace

TEST_CASE("RepairUtilScan missing embeddings list identifies missing documents",
          "[repair][embedding][catch2]") {
    if (std::getenv("YAMS_DISABLE_VECTORS") || std::getenv("YAMS_DISABLE_VECTOR_DB")) {
        SKIP("Vectors disabled via environment");
    }

    // Prepare temp dir for vector DB
    auto tmp = std::filesystem::temp_directory_path() /
               ("yams_repair_catch2_" + std::to_string(std::rand()));
    // Clean up any existing directory from previous runs
    std::error_code ec;
    std::filesystem::remove_all(tmp, ec);
    std::filesystem::create_directories(tmp);

    // Seed metadata with two docs; pre-insert one embedding
    auto repo = std::make_shared<InMemoryMetadata>();
    metadata::DocumentInfo d1{};
    d1.id = 1;
    d1.sha256Hash = "h1";
    d1.fileName = "a.txt";
    d1.mimeType = "text/plain";
    metadata::DocumentInfo d2{};
    d2.id = 2;
    d2.sha256Hash = "h2";
    d2.fileName = "b.txt";
    d2.mimeType = "text/plain";
    repo->add(d1);
    repo->add(d2);

    // Create vector DB and insert embedding for h1
    yams::vector::VectorDatabaseConfig cfg{};
    cfg.database_path = (tmp / "vectors.db").string();
    cfg.create_if_missing = true;
    cfg.embedding_dim = 384;
    auto vdb = std::make_unique<yams::vector::VectorDatabase>(cfg);
    REQUIRE(vdb->initialize());
    yams::vector::VectorRecord rec;
    rec.document_hash = "h1";
    rec.chunk_id = "c-1";
    rec.embedding = std::vector<float>(384, 0.1f);
    rec.content = "hello";
    REQUIRE(vdb->insertVector(rec));

    // Close the vector database to ensure all data is committed before scanning
    vdb.reset();

    // Call repair scan
    auto missing = getDocumentsMissingEmbeddings(repo, tmp);
    REQUIRE(missing);
    auto list = missing.value();
    // Only h2 should be missing
    REQUIRE(list.size() == 1u);
    CHECK(list[0] == std::string("h2"));

    // Cleanup
    std::filesystem::remove_all(tmp, ec);
}

} // namespace yams::repair::test
