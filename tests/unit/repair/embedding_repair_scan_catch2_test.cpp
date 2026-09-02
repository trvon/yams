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

TEST_CASE("RepairUtilScan prioritizes extractable documents ahead of skipped binaries",
          "[repair][embedding][catch2]") {
    if (std::getenv("YAMS_DISABLE_VECTORS") || std::getenv("YAMS_DISABLE_VECTOR_DB")) {
        SKIP("Vectors disabled via environment");
    }

    auto tmp = std::filesystem::temp_directory_path() /
               ("yams_repair_priority_catch2_" + std::to_string(std::rand()));
    std::error_code ec;
    std::filesystem::remove_all(tmp, ec);
    std::filesystem::create_directories(tmp);

    auto repo = std::make_shared<InMemoryMetadata>();

    metadata::DocumentInfo skipped{};
    skipped.id = 1;
    skipped.sha256Hash = "skipped-doc";
    skipped.fileName = "image.png";
    skipped.filePath = "/tmp/image.png";
    skipped.fileSize = 128;
    skipped.mimeType = "image/png";
    skipped.extractionStatus = metadata::ExtractionStatus::Skipped;

    metadata::DocumentInfo success{};
    success.id = 2;
    success.sha256Hash = "success-doc";
    success.fileName = "notes.txt";
    success.filePath = "/tmp/notes.txt";
    success.fileSize = 256;
    success.mimeType = "text/plain";
    success.contentExtracted = true;
    success.extractionStatus = metadata::ExtractionStatus::Success;

    metadata::DocumentInfo failed{};
    failed.id = 3;
    failed.sha256Hash = "failed-doc";
    failed.fileName = "empty.cpp";
    failed.filePath = "/tmp/empty.cpp";
    failed.fileSize = 0;
    failed.mimeType = "text/x-c++";
    failed.extractionStatus = metadata::ExtractionStatus::Failed;

    repo->add(skipped);
    repo->add(success);
    repo->add(failed);

    yams::vector::VectorDatabaseConfig cfg{};
    cfg.database_path = (tmp / "vectors.db").string();
    cfg.create_if_missing = true;
    cfg.embedding_dim = 384;
    auto vdb = std::make_unique<yams::vector::VectorDatabase>(cfg);
    REQUIRE(vdb->initialize());
    vdb.reset();

    auto missing = getDocumentsMissingEmbeddings(repo, tmp);
    REQUIRE(missing);

    const auto& list = missing.value();
    REQUIRE(list.size() == 3u);
    CHECK(list[0] == std::string("success-doc"));
    CHECK(list[1] == std::string("failed-doc"));
    CHECK(list[2] == std::string("skipped-doc"));

    std::filesystem::remove_all(tmp, ec);
}

TEST_CASE("RepairUtilScan returns empty list when all documents embedded",
          "[repair][embedding][catch2]") {
    if (std::getenv("YAMS_DISABLE_VECTORS") || std::getenv("YAMS_DISABLE_VECTOR_DB")) {
        SKIP("Vectors disabled via environment");
    }

    auto tmp = std::filesystem::temp_directory_path() /
               ("yams_repair_all_embedded_" + std::to_string(std::rand()));
    std::error_code ec;
    std::filesystem::remove_all(tmp, ec);
    std::filesystem::create_directories(tmp);

    auto repo = std::make_shared<InMemoryMetadata>();
    metadata::DocumentInfo doc{};
    doc.id = 1;
    doc.sha256Hash = "embedded-doc";
    doc.fileName = "readme.md";
    doc.mimeType = "text/markdown";
    doc.contentExtracted = true;
    doc.extractionStatus = metadata::ExtractionStatus::Success;
    repo->add(doc);

    yams::vector::VectorDatabaseConfig cfg{};
    cfg.database_path = (tmp / "vectors.db").string();
    cfg.create_if_missing = true;
    cfg.embedding_dim = 384;
    auto vdb = std::make_unique<yams::vector::VectorDatabase>(cfg);
    REQUIRE(vdb->initialize());
    yams::vector::VectorRecord rec;
    rec.document_hash = "embedded-doc";
    rec.chunk_id = "c-1";
    rec.embedding = std::vector<float>(384, 0.2f);
    rec.content = "content";
    REQUIRE(vdb->insertVector(rec));
    vdb.reset();

    auto missing = getDocumentsMissingEmbeddings(repo, tmp);
    REQUIRE(missing);
    CHECK(missing.value().empty());

    std::filesystem::remove_all(tmp, ec);
}

TEST_CASE("RepairUtilScan returns empty list for empty metadata", "[repair][embedding][catch2]") {
    if (std::getenv("YAMS_DISABLE_VECTORS") || std::getenv("YAMS_DISABLE_VECTOR_DB")) {
        SKIP("Vectors disabled via environment");
    }

    auto tmp = std::filesystem::temp_directory_path() /
               ("yams_repair_empty_repo_" + std::to_string(std::rand()));
    std::error_code ec;
    std::filesystem::remove_all(tmp, ec);
    std::filesystem::create_directories(tmp);

    auto repo = std::make_shared<InMemoryMetadata>();

    yams::vector::VectorDatabaseConfig cfg{};
    cfg.database_path = (tmp / "vectors.db").string();
    cfg.create_if_missing = true;
    cfg.embedding_dim = 384;
    auto vdb = std::make_unique<yams::vector::VectorDatabase>(cfg);
    REQUIRE(vdb->initialize());
    vdb.reset();

    auto missing = getDocumentsMissingEmbeddings(repo, tmp);
    REQUIRE(missing);
    CHECK(missing.value().empty());

    std::filesystem::remove_all(tmp, ec);
}

namespace {

class CandidateMetadata final : public InMemoryMetadata {
public:
    Result<std::vector<metadata::DocumentInfo>>
    queryDocuments(const metadata::DocumentQueryOptions& options) override {
        queryHadEmbeddingFilter_ = options.hasEmbedding.has_value();
        queriedEmbeddingValue_ = options.hasEmbedding.value_or(false);
        return InMemoryMetadata::queryDocuments(options);
    }

    bool queryHadEmbeddingFilter() const { return queryHadEmbeddingFilter_; }
    bool queriedEmbeddingValue() const { return queriedEmbeddingValue_; }

private:
    bool queryHadEmbeddingFilter_{false};
    bool queriedEmbeddingValue_{false};
};

metadata::DocumentInfo makeCandidate(std::string hash, std::string path, std::string mime,
                                     bool contentExtracted = false) {
    metadata::DocumentInfo doc{};
    doc.sha256Hash = std::move(hash);
    doc.filePath = std::move(path);
    doc.mimeType = std::move(mime);
    doc.contentExtracted = contentExtracted;
    return doc;
}

} // namespace

TEST_CASE("Embedding repair candidate policy preserves MIME and extracted-content eligibility",
          "[repair][embedding][catch2]") {
    auto repo = std::make_shared<CandidateMetadata>();
    repo->add(makeCandidate("text", "/docs/readme.txt", "text/plain"));
    repo->add(makeCandidate("json", "/docs/data.json", "application/json", true));
    repo->add(makeCandidate("xml", "/docs/data.xml", "application/xml"));
    repo->add(makeCandidate("x-yaml", "/docs/data.yaml", "application/x-yaml"));
    repo->add(makeCandidate("yaml", "/docs/config.yaml", "application/yaml"));
    repo->add(makeCandidate("custom-exact", "/docs/report.pdf", "application/pdf"));
    repo->add(makeCandidate("custom-prefix", "/docs/photo.png", "image/png"));
    repo->add(makeCandidate("extracted", "/docs/archive.bin", "application/octet-stream", true));
    repo->add(makeCandidate("javascript", "/docs/app.js", "application/javascript"));
    repo->add(makeCandidate("excluded", "/docs/video.mp4", "video/mp4"));

    const auto scan = selectEmbeddingRepairCandidates(
        *repo, {"application/pdf", "image/", "", "not/a-match"}, false);

    REQUIRE(scan);
    CHECK(repo->queryHadEmbeddingFilter());
    CHECK_FALSE(repo->queriedEmbeddingValue());
    CHECK(scan.value().documentsScanned == 10u);
    CHECK(scan.value().eligibleByMime == 7u);
    CHECK(scan.value().eligibleByExtractedText == 1u);
    CHECK((scan.value().documentHashes == std::vector<std::string>{"text", "json", "xml", "x-yaml",
                                                                   "yaml", "custom-exact",
                                                                   "custom-prefix", "extracted"}));
    CHECK((scan.value().excludedSamples ==
           std::vector<std::string>{"/docs/app.js mime=application/javascript extracted=0",
                                    "/docs/video.mp4 mime=video/mp4 extracted=0"}));
}

TEST_CASE("Embedding repair candidate policy force scans all documents and caps exclusions",
          "[repair][embedding][catch2]") {
    auto repo = std::make_shared<CandidateMetadata>();
    for (int i = 0; i < 10; ++i) {
        repo->add(makeCandidate("excluded-" + std::to_string(i),
                                "/docs/excluded-" + std::to_string(i), "video/mp4"));
    }

    const auto scan = selectEmbeddingRepairCandidates(*repo, {}, true);

    REQUIRE(scan);
    CHECK_FALSE(repo->queryHadEmbeddingFilter());
    CHECK(scan.value().documentsScanned == 10u);
    CHECK(scan.value().documentHashes.empty());
    CHECK(scan.value().eligibleByMime == 0u);
    CHECK(scan.value().eligibleByExtractedText == 0u);
    REQUIRE(scan.value().excludedSamples.size() == 8u);
    CHECK(scan.value().excludedSamples.front() ==
          std::string("/docs/excluded-0 mime=video/mp4 extracted=0"));
    CHECK(scan.value().excludedSamples.back() ==
          std::string("/docs/excluded-7 mime=video/mp4 extracted=0"));
}

} // namespace yams::repair::test
